"""In-memory session→proxy mapping with per-entry TTL.

This provides the "sticky" layer for the local gateway:
each session_id maps to one upstream proxy until TTL expires
or the session is explicitly released.
"""
import threading
import time


class SessionManager:
    """Thread-safe in-memory session store with TTL.

    Each entry is (proxy, expiry_epoch).
    provider_fn is called only when no live mapping exists.
    """

    def __init__(self, default_ttl=300):
        self._default_ttl = default_ttl
        self._lock = threading.Lock()
        self._sessions = {}  # session_id -> (proxy_str, expiry_epoch)

    def get_or_create(self, session_id, provider_fn, ttl=None):
        """Return existing proxy for session_id, or create a new mapping.

        provider_fn: callable returning proxy string (e.g. "1.2.3.4:8080").
        ttl: seconds; defaults to self._default_ttl.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        now = time.time()
        with self._lock:
            entry = self._sessions.get(session_id)
            if entry:
                proxy, expiry = entry
                if now < expiry:
                    return proxy
            proxy = provider_fn()
            self._sessions[session_id] = (proxy, now + ttl)
            return proxy

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
            return len(expired)

    def __len__(self):
        with self._lock:
            return len(self._sessions)
