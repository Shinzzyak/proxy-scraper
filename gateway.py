#!/usr/bin/env python3
"""Minimal HTTP forward proxy gateway with session-based sticky routing.

Client sends requests through this gateway with an X-Session-ID header.
The gateway maintains session→upstream-proxy mappings so that the same
session always egresses through the same upstream proxy.

This is NOT a sticky proxy in the provider sense — stickiness is enforced
by this gateway layer. Upstream proxies are public free proxies.
"""
import argparse
import json
import os
import queue
import select
import socket
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from session_manager import SessionManager


class CountryUnavailable(Exception):
    """Raised when X-Country requested but no proxy for that country."""
    def __init__(self, country):
        super().__init__(f"no proxy available for country {country}")
        self.country = country


def _raise_country(country):
    """Provider helper: raise instead of returning DIRECT (R11-6)."""
    raise CountryUnavailable(country)

# ponytail: one default TTL; when proxy pool exposes health, reduce TTL for unhealthy proxies
DEFAULT_SESSION_TTL = 300
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8080
GATEWAY_TIMEOUT = 15
UPSTREAM_PROXY_TIMEOUT = 10
ROTATE_POOL_SIZE = 100  # round-robin pool size (fresh HTTP proxies)
ROTATE_MIN_SCORE = 30   # tolerate lower score for rotation variety
MAX_FAILOVER = 3        # ProxyGate parity: try up to 3 proxies per request (B3)
AUTH_WINDOW = 300       # HMAC auth timestamp window (seconds) — replay-safe (B4)

# Cooldown mapping per failure type (farukbagci/proxy-pool — 3M records @48req/s):
# timeout/refused = transient → short; 429/403 = site-level ban → long (R5)
COOLDOWN_MAP = {
    "timed out": 60,
    "timeout": 60,
    "refused": 60,
    "429": 300,
    "too many requests": 300,
    "403": 900,
    "auth required": 300,
    "407": 300,
    "default": 120,
}


def _cooldown_for(error: str) -> int:
    """Map a failure string to a cooldown seconds (per-status, not flat 300s)."""
    err = (error or "").lower()
    for key, secs in COOLDOWN_MAP.items():
        if key in err:
            return secs
    return COOLDOWN_MAP["default"]


def _check_auth(secret: str, client_id: str, ts: str, sig: str, window: int = AUTH_WINDOW) -> bool:
    """HMAC-SHA256 auth: sig = HMAC(secret, f'{client_id}{ts}'). Replay-safe."""
    import hashlib
    import hmac as hmac_mod
    if not secret:
        return True  # auth disabled
    try:
        if abs(int(time.time()) - int(ts)) > window:
            return False  # stale/replay
    except ValueError:
        return False
    expected = hmac_mod.new(secret.encode(), f"{client_id}{ts}".encode(), hashlib.sha256).hexdigest()
    return hmac_mod.compare_digest(expected, sig)


def _auth_ok(handler, secret: str) -> bool:
    """Check Proxy-Authorization: Basic <client_id:ts:sig>. 407 on failure."""
    if not secret:
        return True
    import base64
    try:
        header = handler.headers.get("Proxy-Authorization", "")
        if not header.startswith("Basic "):
            return False
        decoded = base64.b64decode(header[6:]).decode()
        client_id, ts, sig = decoded.split(":")
        if not _check_auth(secret, client_id, ts, sig):
            return False
        return True
    except Exception:
        return False

# Async usage-log writer: batches writes so request path never blocks on SQLite.
_usage_queue = queue.Queue(maxsize=10000)  # bounded — drop-counted under abuse (P2)
_usage_stop = threading.Event()
_dropped_usage = 0


def _usage_writer():
    """Background thread: drains usage queue into usage_log in small batches."""
    batch = []
    consecutive_failures = 0
    while not _usage_stop.is_set() or not _usage_queue.empty():
        try:
            item = _usage_queue.get(timeout=0.5)
            batch.append(item)
            if len(batch) >= 50:
                if not _flush_usage(batch):
                    # failure: re-queue items, don't drop them (P1-3),
                    # then back off so a dead DB doesn't spin CPU (R4-2).
                    # put_nowait: never block the writer on a full queue (T2).
                    for it in batch:
                        try:
                            _usage_queue.put_nowait(it)
                        except queue.Full:
                            pass  # queue full — drop, DB is down anyway
                    consecutive_failures += 1
                    time.sleep(min(consecutive_failures * 0.5, 5))
                else:
                    consecutive_failures = 0
                batch = []
        except queue.Empty:
            if batch:
                if not _flush_usage(batch):
                    for it in batch:
                        try:
                            _usage_queue.put_nowait(it)
                        except queue.Full:
                            pass
                    consecutive_failures += 1
                    time.sleep(min(consecutive_failures * 0.5, 5))
                else:
                    consecutive_failures = 0
                batch = []
    if batch:
        if not _flush_usage(batch):
            for it in batch:
                try:
                    _usage_queue.put_nowait(it)
                except queue.Full:
                    pass


def _drain_usage(max_retries=3):
    """Synchronous drain (for tests): flush everything currently queued.
    Returns (flushed, failed) counts. Retry-capped so a dead DB cannot
    spin forever (T2)."""
    flushed = failed = 0
    # R13-3: jangan pakai empty() + get_nowait() — race dengan writer
    # (writer bisa ambil antara empty() dan get). Pakai blocking get
    # dengan timeout pendek; Empty = queue kosong → break.
    while True:
        batch = []
        try:
            item = _usage_queue.get(timeout=0.2)
        except queue.Empty:
            break
        batch.append(item)
        if _flush_usage(batch):
            flushed += 1
        else:
            failed += 1
            if failed < max_retries:  # R6-2: off-by-one — cap at max_retries
                _usage_queue.put(batch[0])  # bounded retries, then drop
    return flushed, failed


def _scrub_error(err: str) -> str:
    """Strip anything that looks like credentials from an error string
    (key-router pattern: secrets never logged)."""
    if not err:
        return ""
    import re as _re
    # user:pass@host or scheme://user:pass@ — redact the userinfo part
    return _re.sub(r"(://|@)[^/@\s]+@", r"\1***@", err)[:200]


def _flush_usage(batch):
    """Write batch to usage_log. Returns True on success — on failure items are
    NOT lost: caller re-queues them (P1-3)."""
    try:
        from proxy_pool import get_db
        conn = get_db()
        try:
            conn.executemany(
                "INSERT INTO usage_log (ip, port, success, response_time_ms, error) VALUES (?, ?, ?, ?, ?)",
                [(ip, port, int(ok), rt, _scrub_error(err)) for ip, port, ok, rt, err in batch],
            )
            conn.commit()
            return True
        finally:
            conn.close()
    except Exception:
        return False


def _enqueue_usage(ip, port, success, duration_ms=0, error=""):
    global _dropped_usage
    try:
        _usage_queue.put_nowait((ip, int(port), bool(success), int(duration_ms), error))
    except queue.Full:
        _dropped_usage += 1  # bounded queue — drop under burst, count it


def _pick_proxy(country="", exclude=None):
    """Select a fresh HTTP proxy from the pool.

    Returns 'host:port' string. Falls back to direct connection
    ('DIRECT') if the pool is empty. If country requested but none
    found, returns None (caller decides — do NOT silently fall back).
    """
    try:
        from proxy_pool import get_best_proxy, search_proxies
        # min_score=1: score 0 = auto-banned (R5-nit) — jangan pernah pilih
        if exclude:
            # R11-7: pick a fresh proxy NOT in exclude set (retry loop)
            rows = search_proxies(
                protocol="http", country_code=country, min_score=1,
                max_age_minutes=180, max_results=20,
            )
            for r in rows:
                candidate = f"{r['ip']}:{r['port']}"
                if candidate not in exclude:
                    return candidate
            if country:
                return None
        proxy = get_best_proxy(protocol="http", country_code=country, min_score=1, max_age_minutes=180)  # R10-1: samakan dengan rotate (7aeaff5) — 60m bikin sticky 100% DIRECT di tengah siklus 6h
        if proxy:
            return f"{proxy['ip']}:{proxy['port']}"
        if country:
            return None  # requested country unavailable — no DIRECT leak
    except Exception:
        pass
    return "DIRECT"


def _next_proxy_round_robin(pool_state, country=""):
    """Round-robin over a fresh pool snapshot, refreshed periodically.

    pool_state: dict with 'list' (list of host:port), 'index', 'lock'.
    country: optional ISO code filter (X-Country header).
    Returns next proxy or 'DIRECT' when pool empty.

    R6-3: whole read-modify-write (refresh + index++ + blacklist scan) runs
    under pool_state['lock'] — without it, concurrent requests lost index
    updates (8 threads × 300 picks → 100% duplicates) and a mid-flight
    refresh reset index to 0.
    """
    try:
        from proxy_pool import search_proxies
        lock = pool_state.setdefault("lock", threading.Lock())
        with lock:
            now = time.time()
            key = f"c:{country or '*'}"
            if pool_state.get("list") is None or pool_state.get("key") != key or now - pool_state.get("refreshed", 0) > 120:
                rows = search_proxies(
                    protocol="http", country_code=country, min_score=ROTATE_MIN_SCORE,
                    max_age_minutes=180, max_results=ROTATE_POOL_SIZE,
                )
                pool_state["list"] = [f"{r['ip']}:{r['port']}" for r in rows] or None
                pool_state["key"] = key
                pool_state["index"] = 0
                pool_state["refreshed"] = now
                # R12-8: purge expired blacklist entries at refresh — dict grows unbounded otherwise
                pool_state["blacklist"] = {p: t for p, t in pool_state.get("blacklist", {}).items() if t > now}
            if not pool_state.get("list"):
                # R11-6: country requested but none — raise, jangan DIRECT leak
                if country:
                    raise CountryUnavailable(country)
                return "DIRECT"
            # skip blacklisted proxies (P0-2: rotate used to serve dead proxies).
            # TTL 300s — recovered proxies re-enter rotation (R4-3).
            bl = pool_state.get("blacklist", {})
            now = time.time()
            for _ in range(len(pool_state["list"])):
                proxy = pool_state["list"][pool_state["index"] % len(pool_state["list"])]
                pool_state["index"] += 1
                unban_at = bl.get(proxy, 0)
                if now >= unban_at:
                    return proxy
            return "DIRECT"
    except CountryUnavailable:
        raise  # R11-6: jangan swallow — caller harus 503
    except Exception:
        return "DIRECT"


class GatewayHandler(BaseHTTPRequestHandler):
    """HTTP forward proxy that routes via session→proxy mapping."""

    def _get_session_proxy(self, country=""):
        if not country:
            country = self.headers.get("X-Country", "").upper()
        if self.server.mode == "rotate":
            return _next_proxy_round_robin(self.server.rotate_state, country)
        session_id = self.headers.get("X-Session-ID", "")
        if not session_id:
            proxy = _pick_proxy(country)
            # R10-5: country requested but unavailable — 503, jangan DIRECT leak
            if proxy is None:
                raise CountryUnavailable(country)
            return proxy or "DIRECT"
        ttl = DEFAULT_SESSION_TTL
        try:
            ttl = min(int(self.headers.get("X-Session-TTL", "")), 3600)
        except ValueError:
            pass
        # R11-7: retry dengan exclude — proxy #1 flaky jangan bikin session DIRECT
        exclude = set()  # proxies already tried in this retry loop
        for attempt in range(3):
            try:
                picked = _pick_proxy(country, exclude=exclude)
                if picked is None:
                    raise CountryUnavailable(country)
                exclude.add(picked)
                return self.server.session_manager.get_or_create(
                    session_id, lambda p=picked: p, ttl=ttl
                )
            except CountryUnavailable:
                raise
            except Exception:
                if attempt == 2:
                    raise

    def _log_usage(self, proxy, success, error="", duration_ms=0):
        """Enqueue proxy usage event (async writer batches to usage_log)."""
        try:
            if proxy and proxy != "DIRECT":
                ip, _, port = proxy.rpartition(":")
                _enqueue_usage(ip, int(port), success, duration_ms, error)
                if not success:
                    self.server.session_manager.report_failure(proxy)
                    bl = self.server.rotate_state.get("blacklist")
                    if bl is not None:
                        # T1-fix: dict API (set → AttributeError → silent skip)
                        bl[proxy] = time.time() + _cooldown_for(error)
        except Exception:
            pass

    def do_CONNECT(self):
        """HTTPS tunneling: establish CONNECT through upstream proxy, with failover."""
        if not _auth_ok(self, self.server.auth_secret):
            self.send_error(407, "Proxy Authentication Required")
            return
        # R9-1: IPv6 target "[2606:4700::1111]:443" — rpartition on ":" keeps host intact
        host, _, port = self.path.rpartition(":")
        host = host.strip("[]")
        try:
            port = int(port) if port else 443
        except ValueError:
            # R10-3: malformed CONNECT (no port / non-numeric) must 400, not crash
            self.send_error(400, "Bad CONNECT target")
            return
        # R9-2: open-relay guard — only allow standard TLS ports through CONNECT.
        # Arbitrary ports (22/25/3389/...) turn the gateway into a scanning relay.
        if port not in (80, 443, 8443):
            self.send_error(403, "CONNECT to non-standard port forbidden")
            return
        country = self.headers.get("X-Country", "").upper()
        last_err = None
        for _ in range(MAX_FAILOVER):
            upstream = None
            try:
                session_proxy = self._get_session_proxy(country=country)
            except CountryUnavailable as e:
                # R10-5: no proxy for requested country — 503, jangan DIRECT leak
                self.send_error(503, f"No proxy available for country: {e.country}")
                return
            if session_proxy == "DIRECT":
                self._connect_direct(host, port)
                return
            t0 = time.monotonic()
            try:
                upstream = socket.create_connection(session_proxy.split(":"), timeout=UPSTREAM_PROXY_TIMEOUT)
                upstream.settimeout(UPSTREAM_PROXY_TIMEOUT)  # R10-4: recv ikut timeout — silent upstream ga hang 10s×3
                upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # R16-G1: matikan Nagle — hemat 1 RTT/burst di tunnel
                # T3: sebagian proxy publik reject CONNECT tanpa UA
                upstream.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: Gateway/1.0\r\nProxy-Connection: keep-alive\r\n\r\n".encode())
                # R9-3: response can arrive in 2+ segments — loop until header end
                resp = b""
                while b"\r\n\r\n" not in resp and len(resp) < 8192:
                    chunk = upstream.recv(4096)
                    if not chunk:
                        break
                    resp += chunk
                if b"200" not in resp:
                    upstream.close()
                    self._log_usage(session_proxy, False, "upstream CONNECT rejected", int((time.monotonic() - t0) * 1000))
                    self._blacklist_proxy(session_proxy, "rejected")
                    # R15-2: release mapping sticky — kalau tidak, session tetap
                    # terkunci ke proxy mati sampai TTL habis (3x retry proxy sama)
                    session_id = self.headers.get("X-Session-ID", "")
                    if session_id:
                        self.server.session_manager.release(session_id)
                    last_err = "upstream CONNECT rejected"
                    continue
                self.send_response(200, "Connection Established")
                self.end_headers()
                self._log_usage(session_proxy, True, duration_ms=int((time.monotonic() - t0) * 1000))
                self._tunnel(self.connection, upstream)
                return
            except Exception as e:
                # R10-2: close upstream on every error path — silent upstream
                # (accept, no reply) leaked 1 fd per failover → EMFILE long-running
                try:
                    if upstream is not None:
                        upstream.close()
                except Exception:
                    pass
                self._log_usage(session_proxy, False, str(e), int((time.monotonic() - t0) * 1000))
                self._blacklist_proxy(session_proxy, str(e))
                # R15-2: release mapping sticky juga saat error (proxy mati
                # mid-CONNECT) — jangan biarkan session terkunci ke proxy mati
                session_id = self.headers.get("X-Session-ID", "")
                if session_id:
                    self.server.session_manager.release(session_id)
                last_err = str(e)
        self.send_error(502, f"Upstream error: {last_err}")

    def _blacklist_proxy(self, proxy, error=""):
        """Blacklist a failing proxy in both session manager and rotate pool."""
        try:
            self.server.session_manager.report_failure(proxy)
            bl = self.server.rotate_state.get("blacklist")
            if bl is not None:
                bl[proxy] = time.time() + _cooldown_for(error)  # per-status TTL (R5)
        except Exception:
            pass

    def _connect_direct(self, host, port):
        """CONNECT without upstream proxy (direct)."""
        try:
            remote = socket.create_connection((host, port), timeout=GATEWAY_TIMEOUT)
            remote.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # R16-G1
            self.send_response(200, "Connection Established")
            self.end_headers()
            self._tunnel(self.connection, remote)
        except Exception as e:
            self.send_error(502, f"Direct connection failed: {e}")

    def do_GET(self):
        self._forward_via_proxy("GET")

    def do_POST(self):
        self._forward_via_proxy("POST")

    def do_PUT(self):
        self._forward_via_proxy("PUT")

    def do_DELETE(self):
        self._forward_via_proxy("DELETE")

    def do_PATCH(self):
        self._forward_via_proxy("PATCH")

    def do_HEAD(self):
        self._forward_via_proxy("HEAD")

    def _forward_via_proxy(self, method):
        """Forward HTTP request through the session-assigned upstream proxy."""
        if not _auth_ok(self, self.server.auth_secret):
            self.send_error(407, "Proxy Authentication Required")
            return
        try:
            session_proxy = self._get_session_proxy()
        except CountryUnavailable as e:
            # R11-8: HTTP path — 503, bukan traceback/connection reset
            self.send_error(503, f"No proxy available for country: {e.country}")
            return
        url = self.path

        if session_proxy == "DIRECT":
            self._forward_direct(method, url)
            return

        t0 = time.monotonic()
        try:
            handler = urllib.request.ProxyHandler({
                "http": f"http://{session_proxy}",
                "https": f"http://{session_proxy}",
            })
            opener = urllib.request.build_opener(handler)
            body = None
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > 0:
                body = self.rfile.read(content_length)

            req = urllib.request.Request(url, data=body, method=method)
            for key, val in self.headers.items():
                # R7-1: NEVER forward Proxy-Authorization (HMAC client creds)
                # to a public upstream — replay within AUTH_WINDOW.
                if key.lower() not in ("proxy-connection", "host", "x-session-id", "proxy-authorization"):
                    req.add_header(key, val)

            resp = opener.open(req, timeout=UPSTREAM_PROXY_TIMEOUT)
            self._log_usage(session_proxy, True, duration_ms=int((time.monotonic() - t0) * 1000))
            self.send_response(resp.status)
            for key, val in resp.headers.items():
                if key.lower() not in ("transfer-encoding", "connection"):
                    self.send_header(key, val)
            self.end_headers()
            try:
                # R16-N4: timeout hanya berlaku sampai header; body read bisa
                # hang selamanya kalau proxy lambat — set socket timeout ulang
                fp = getattr(resp, "fp", None)
                sock = getattr(fp, "_sock", None)
                if sock is not None:
                    sock.settimeout(UPSTREAM_PROXY_TIMEOUT)
                chunk = resp.read(65536)
                while chunk:
                    self.wfile.write(chunk)
                    chunk = resp.read(65536)
            finally:
                resp.close()  # R11-4: jangan tunggu GC — socket leak transient saat burst
        except urllib.error.HTTPError as e:
            self._log_usage(session_proxy, False, f"HTTP {e.code}", int((time.monotonic() - t0) * 1000))
            self.send_response(e.code)
            self.end_headers()
            # R15-14: e.read() bisa hang kalau upstream kirim header error tapi
            # body menggantung — baca terbatas + timeout
            try:
                if e.readable():
                    fp = getattr(e, "fp", None)
                    sock = getattr(fp, "_sock", None)
                    if sock is not None:
                        sock.settimeout(UPSTREAM_PROXY_TIMEOUT)
                    self.wfile.write(e.read(8192))
            except Exception:
                pass
        except Exception as e:
            self._log_usage(session_proxy, False, str(e), int((time.monotonic() - t0) * 1000))
            self.send_error(502, f"Upstream error: {e}")

    def _forward_direct(self, method, url):
        """Forward without upstream proxy."""
        try:
            body = None
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length > 0:
                body = self.rfile.read(content_length)
            req = urllib.request.Request(url, data=body, method=method)
            for key, val in self.headers.items():
                # R7-1: never forward Proxy-Authorization to origin either
                if key.lower() not in ("proxy-connection", "x-session-id", "proxy-authorization"):
                    req.add_header(key, val)
            resp = urllib.request.urlopen(req, timeout=GATEWAY_TIMEOUT)
            self.send_response(resp.status)
            for key, val in resp.headers.items():
                if key.lower() not in ("transfer-encoding", "connection"):
                    self.send_header(key, val)
            self.end_headers()
            try:
                chunk = resp.read(65536)
                while chunk:
                    self.wfile.write(chunk)
                    chunk = resp.read(65536)
            finally:
                resp.close()  # R11-4
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
            if e.readable():
                self.wfile.write(e.read(65536))
        except Exception as e:
            self.send_error(502, f"Direct error: {e}")

    @staticmethod
    def _tunnel(client, remote):
        """Bidirectional byte relay between client and remote socket."""
        sockets = [client, remote]
        try:
            # R16-G1: TCP_NODELAY di kedua ujung — Nagle + delayed-ACK = +1 RTT
            # per burst chatty protocol
            for s in sockets:
                try:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    # R16-G4: SO_KEEPALIVE deteksi peer mati — ganti idle-kill
                    # 15s (GATEWAY_TIMEOUT) yang memutus tunnel + TLS session
                    # resumption klien. Keepalive = 60s idle, probe 5x, 10s.
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    try:
                        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
                        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
                        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 5)
                    except Exception:
                        pass
                except Exception:
                    pass
            while True:
                # R15-1: select pada fd tertutup (proxy mati mid-session) → ValueError
                # guard: buang socket yang sudah closed dari list
                sockets = [s for s in sockets if s.fileno() != -1]
                if len(sockets) < 2:
                    break
                try:
                    readable, _, errors = select.select(sockets, [], sockets, GATEWAY_TIMEOUT)
                except (OSError, ValueError):
                    break
                if errors:
                    break
                if not readable:
                    # R16-G4: jangan break saat idle — SO_KEEPALIVE yang deteksi
                    # peer mati; tunnel tetap hidup untuk TLS session resumption
                    continue
                for sock in readable:
                    try:
                        data = sock.recv(65536)
                    except (OSError, ValueError):
                        return
                    if not data:
                        return
                    target = remote if sock is client else client
                    try:
                        target.sendall(data)
                    except (OSError, ValueError):
                        return
        finally:
            try:
                remote.close()
            except Exception:
                pass

    def log_message(self, format, *args):
        # Suppress default access log noise; only log errors
        pass


def _cleanup_loop(session_manager, interval=60):
    """Background thread: purge expired sessions + auto-ban bad proxies periodically."""
    while True:
        time.sleep(interval)
        removed = session_manager.cleanup()
        if removed:
            print(f"♻️ Purged {removed} expired sessions", file=sys.stderr)
        try:
            from proxy_pool import auto_ban_bad_proxies, get_db
            banned = auto_ban_bad_proxies()
            if banned:
                print(f"🚫 Auto-banned {banned} bad proxies (usage success rate)", file=sys.stderr)
            # usage_log retention — 30 days (R4-5)
            conn = get_db()
            try:
                deleted = conn.execute(
                    "DELETE FROM usage_log WHERE julianday(timestamp) < julianday('now', '-30 days')"
                ).rowcount
                if deleted:
                    print(f"🧹 Pruned {deleted} usage_log rows (>30d)", file=sys.stderr)
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass  # auto-ban + prune are best-effort


def main():
    parser = argparse.ArgumentParser(description="Local HTTP proxy gateway with session-based sticky routing")
    parser.add_argument("--bind", default=DEFAULT_BIND)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--session-ttl", type=int, default=DEFAULT_SESSION_TTL)
    parser.add_argument("--mode", choices=["sticky", "rotate"], default="sticky",
                        help="sticky: same session → same proxy (default); rotate: round-robin per request")
    parser.add_argument("--auth-secret", default=os.environ.get("GATEWAY_AUTH_SECRET", ""),
                        help="HMAC-SHA256 auth secret (empty = auth off)")
    args = parser.parse_args()

    sm = SessionManager(default_ttl=args.session_ttl)
    server = ThreadingHTTPServer((args.bind, args.port), GatewayHandler)
    server.daemon_threads = True  # R11-5: shutdown ga nunggu tunnel idle
    server.session_manager = sm
    server.mode = args.mode
    server.rotate_state = {"list": None, "index": 0, "refreshed": 0, "blacklist": {}}
    server.auth_secret = args.auth_secret

    cleanup = threading.Thread(target=_cleanup_loop, args=(sm,), daemon=True)
    cleanup.start()
    writer = threading.Thread(target=_usage_writer, daemon=True)
    writer.start()

    print(f"🔀 Gateway listening on {args.bind}:{args.port} (session TTL {args.session_ttl}s, mode {args.mode})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Gateway stopped")
        _usage_stop.set()
        try:
            writer.join(timeout=5)  # R12-7: writer selesai dulu, baru drain — cegah race item hilang
        except Exception:
            pass
        flushed, failed = _drain_usage()
        print(f"💾 usage_log drained: {flushed} flushed, {failed} failed")
        if _dropped_usage:
            print(f"⚠️ {_dropped_usage} usage events dropped (queue full)")  # R12-7: drop ga sembunyi
        server.server_close()


if __name__ == "__main__":
    main()
