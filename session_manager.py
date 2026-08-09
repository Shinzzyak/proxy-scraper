"""In-memory session→proxy mapping with per-entry TTL.

This provides the "sticky" layer for the local gateway:
each session_id maps to one upstream proxy until TTL expires
or the session is explicitly released.
"""
import threading
import time


MAX_SESSIONS = 10000  # hard cap — evict oldest when exceeded (P1-5)


class SessionManager:
    """Thread-safe in-memory session store with TTL.

    Each entry is (proxy, expiry_epoch).
    provider_fn is called only when no live mapping exists.
    """

    def __init__(self, default_ttl=300, failure_penalty_seconds=60, max_sessions=MAX_SESSIONS):
        self._default_ttl = default_ttl
        self._failure_penalty_seconds = failure_penalty_seconds
        self._max_sessions = max_sessions
        self._lock = threading.Lock()
        self._sessions = {}  # session_id -> (proxy_str, expiry_epoch)
        self._blacklist = {}  # proxy_str -> unban_epoch

    def get_or_create(self, session_id, provider_fn, ttl=None):
        """Return existing proxy for session_id, or create a new mapping.

        provider_fn: callable returning proxy string (e.g. "1.2.3.4:8080").
        ttl: seconds; defaults to self._default_ttl.

        R4-4: provider_fn runs OUTSIDE the lock — a slow SQLite pick must not
        block every other session lookup.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        now = time.time()
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry:
                proxy, expiry = entry
                if now < expiry and proxy not in self._blacklist:
                    return proxy
                # entry expired or proxy blacklisted -> fall through to re-pick
        proxy = provider_fn()
        # never hand out a blacklisted proxy; bound the retry loop so a
        # provider returning the same dead proxy cannot hang forever (P0-1)
        attempts = 0
        while proxy in self._blacklist and now < self._blacklist[proxy] and attempts < 5:
            proxy = provider_fn()
            attempts += 1
        if proxy in self._blacklist and now < self._blacklist[proxy]:
            proxy = "DIRECT"  # all candidates blacklisted — fail open
        with self._lock:
            # R4-9: never cache DIRECT — a session pinned to DIRECT stays
            # direct for the full TTL even after the pool recovers.
            if proxy != "DIRECT":
                # cap sessions — evict oldest if over limit (P1-5)
                if len(self._sessions) >= self._max_sessions:
                    oldest = min(self._sessions, key=lambda sid: self._sessions[sid][1])
                    del self._sessions[oldest]
                self._sessions[session_id] = (proxy, now + ttl)
        return proxy

    def report_failure(self, proxy):
        """Blacklist a proxy briefly after a failed request."""
        if not proxy or proxy == "DIRECT":
            return
        with self._lock:
            self._blacklist[proxy] = time.time() + self._failure_penalty_seconds

    def release(self, session_id):
        """Remove a session mapping (e.g. on explicit disconnect)."""
        with self._lock:
            self._sessions.pop(session_id, None)

    def cleanup(self):
        """Remove expired entries. Returns count removed."""
        now = time.time()
        with self._lock:
            expired = [sid for sid, (_, exp) in self._sessions.items() if now >= exp]
            for sid in expired:
                del self._sessions[sid]
            # purge expired blacklist entries
            self._blacklist = {p: t for p, t in self._blacklist.items() if t > now}
            return len(expired)

    def __len__(self):
        with self._lock:
            return len(self._sessions)
