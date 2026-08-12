#!/usr/bin/env python3
"""Minimal HTTP forward proxy gateway with session-based sticky routing.

Client sends requests through this gateway with an X-Session-ID header.
The gateway maintains session→upstream-proxy mappings so that the same
session always egresses through the same upstream proxy.

This is NOT a sticky proxy in the provider sense — stickiness is enforced
by this gateway layer. Upstream proxies are public free proxies.
"""
import argparse
import base64
import json
import os
import queue
import re
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

# R31-GW5: UA default browser mobile — Python-urllib/3.x = sinyal bot gede;
# target (Z.ai, tokenharbor, ds-limit) sering filter UA proxy/datacenter.
_UA_MOBILE = ("Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/UD1A.231105.004) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.165 "
              "Mobile Safari/537.36")

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
    "http-only": 0,  # R21-GW1: HTTP-only proxy tolak CONNECT — langsung reusable (retry loop butuh coba proxy lain secepatnya)
    "mitm": 600,  # R22-GW4: MITM proxy (fake cert) — blacklist 10 menit, jangan dipakai lagi
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
    """Check Proxy-Authorization: Basic <clien...ig>. 407 on failure."""
    if not secret:
        return True
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


def _sid_from_username(headers):
    """R26-Z1 (proxy rotation): extract (sid, region, ttl) dari username
    cliproxy-style di Proxy-Authorization Basic.

    Format URL proxy:
        http://<user>-region-<REGION>-sid-<SID>-t-<TTL>:<pass>@host:port
    Contoh:
        http://bulk-region-ID-sid-a1b2c3d4-t-300:secret@127.0.0.1:8081

    Satu sid → satu egress IP (sticky via SessionManager). Ganti sid =
    session baru = IP baru. Region opsional filter negara pool. TTL default
    DEFAULT_SESSION_TTL kalau tidak ada t-<N>.

    Auth di gateway ini format '<client_id>:<ts>:<sig>' — sid ada di
    client_id (bagian sebelum ':' pertama).
    """
    auth = headers.get("Proxy-Authorization", "")
    if not auth.startswith("Basic "):
        return None, "", 0
    try:
        decoded = base64.b64decode(auth[6:]).decode("utf-8", "ignore")
        user = decoded.split(":", 1)[0]
    except Exception:
        return None, "", 0
    m = re.search(r"sid-([a-zA-Z0-9_]+)", user)
    if not m:
        return None, "", 0
    sid = m.group(1)
    region = ""
    rm = re.search(r"region-([A-Za-z]{2})", user)
    if rm:
        region = rm.group(1).upper()
    ttl = 0
    tm = re.search(r"t-(\d+)", user)
    if tm:
        try:
            ttl = min(int(tm.group(1)), 3600)
        except ValueError:
            ttl = 0
    return sid, region, ttl


def _egress_ip(proxy_str):
    """'socks5://1.2.3.4:1080' → '1.2.3.4'. 'DIRECT' → 'DIRECT'."""
    if not proxy_str or proxy_str == "DIRECT":
        return "DIRECT"
    return proxy_str.split("://")[-1].split(":")[0]


def _session_proxy_of(sm, sid):
    """IP proxy yang di-pin ke sid (kalau ada). None kalau belum."""
    try:
        sessions = getattr(sm, "_sessions", {})
        entry = sessions.get(sid)
        if entry:
            return entry[0] if isinstance(entry, tuple) else entry
    except Exception:
        pass
    return None


def _enqueue_usage(ip, port, success, duration_ms=0, error=""):
    global _dropped_usage
    try:
        _usage_queue.put_nowait((ip, int(port), bool(success), int(duration_ms), error))
    except queue.Full:
        _dropped_usage += 1  # bounded queue — drop under burst, count it


def _pick_proxy(country="", exclude=None, protocol=""):
    """Select a fresh proxy from the pool.

    Returns 'host:port' string (or 'socks5://host:port'). Falls back to
    direct connection ('DIRECT') if the pool is empty. If country requested
    but none found, returns None (caller decides — do NOT silently fall back).
    """
    try:
        from proxy_pool import get_best_proxy, search_proxies
        # min_score=1: score 0 = auto-banned (R5-nit) — jangan pernah pilih
        # R24-GW5: protocol bebas (http + socks5) — prefix socks5:// untuk routing
        if exclude:
            # R11-7: pick a fresh proxy NOT in exclude set (retry loop)
            rows = search_proxies(
                protocol=protocol, country_code=country, min_score=1,
                max_age_minutes=180, max_results=20,
            )
            for r in rows:
                candidate = f"{r['ip']}:{r['port']}"
                if r.get("protocol") == "socks5":
                    candidate = "socks5://" + candidate
                elif r.get("protocol") == "socks4":
                    # R31-GW7: SOCKS4 upstream (no header leak, sama kyk socks5)
                    candidate = "socks4://" + candidate
                if candidate not in exclude:
                    return candidate
            if country:
                return None
        proxy = get_best_proxy(protocol=protocol, country_code=country, min_score=1, max_age_minutes=0)  # max_age=0: freshness disabled — free pool volatile; failover+cooldown handles dead ones
        if proxy:
            cand = f"{proxy['ip']}:{proxy['port']}"
            if proxy.get("protocol") == "socks5":
                cand = "socks5://" + cand
            elif proxy.get("protocol") == "socks4":
                cand = "socks4://" + cand  # R31-GW7
            return cand
        if country:
            return None  # requested country unavailable — no DIRECT leak
    except Exception:
        pass
    return "DIRECT"


def _next_proxy_round_robin(pool_state, country="", exclude=None, protocol=""):
    """Round-robin over a fresh pool snapshot, refreshed periodically.

    pool_state: dict with 'list' (list of host:port), 'index', 'lock'.
    country: optional ISO code filter (X-Country header).
    protocol: optional filter ("http" / "socks5" / "" = semua).
    exclude: optional set of host:port already tried this request (R21-GW3).
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
            key = f"c:{country or '*'}:p:{protocol or '*'}"
            if pool_state.get("list") is None or pool_state.get("key") != key or now - pool_state.get("refreshed", 0) > 120:
                rows = search_proxies(
                    protocol=protocol, country_code=country, min_score=ROTATE_MIN_SCORE,
                    max_age_minutes=180, max_results=ROTATE_POOL_SIZE,
                )
                # R24-GW5: socks5 prefix untuk routing
                pool_state["list"] = [
                    (f"socks5://{r['ip']}:{r['port']}" if r.get("protocol") == "socks5" else f"{r['ip']}:{r['port']}")
                    for r in rows
                ] or None
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
            tried = exclude or set()
            for _ in range(len(pool_state["list"]) + len(tried)):
                proxy = pool_state["list"][pool_state["index"] % len(pool_state["list"])]
                pool_state["index"] += 1
                unban_at = bl.get(proxy, 0)
                if proxy in tried:
                    continue  # R21-GW3: sudah dicoba request ini — skip
                if now >= unban_at:
                    return proxy
            return "DIRECT"
    except CountryUnavailable:
        raise  # R11-6: jangan swallow — caller harus 503
    except Exception:
        return "DIRECT"


def _socks5_connect(sock, host, port):
    """R24-GW5: SOCKS5 handshake + CONNECT via upstream socket.
    Raises on failure — caller closes socket. No-auth only (free pool)."""
    import struct
    sock.sendall(b"\x05\x01\x00")  # version 5, 1 method, no-auth
    resp = sock.recv(2)
    if resp != b"\x05\x00":
        raise ConnectionError(f"SOCKS5 auth method rejected: {resp.hex()}")
    # CONNECT request: ATYP 0x03 (domain) — free pool jarang punya reverse DNS
    host_b = host.encode()
    req = b"\x05\x01\x00\x03" + bytes([len(host_b)]) + host_b + struct.pack(">H", port)
    sock.sendall(req)
    r2 = sock.recv(10)
    if len(r2) < 2 or r2[1] != 0:
        # R31-GW9: banyak SOCKS5 free GAK bisa resolve domain (ATYP 0x03
        # gagal dengan 0x04 host unreachable). Fallback: resolve lokal +
        # retry ATYP 0x01 (IPv4). Kalau masih gagal, propagate.
        try:
            import socket as _s
            ip = _s.gethostbyname(host)
            req4 = b"\x05\x01\x00\x01" + _s.inet_aton(ip) + struct.pack(">H", port)
            sock.sendall(req4)
            r2 = sock.recv(10)
            if len(r2) < 2 or r2[1] != 0:
                raise ConnectionError(f"SOCKS5 CONNECT failed: {r2.hex()}")
        except ConnectionError:
            raise
        except Exception:
            raise ConnectionError(f"SOCKS5 CONNECT failed: {r2.hex()}")
    # consume remaining BND.ADDR/BND.PORT (variable length)
    if r2[3] == 0x01:  # IPv4
        need = 4 + 2
    elif r2[3] == 0x04:  # IPv6
        need = 16 + 2
    else:  # domain
        need = r2[4] + 2 if len(r2) > 4 else 2
    while len(r2) < 4 + need:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("SOCKS5 truncated BND response")
        r2 += chunk


def _socks4_connect(sock, host, port):
    """R31-GW7: SOCKS4 CONNECT via upstream socket (no auth).

    SOCKS4 = no header leak (sama seperti SOCKS5). Pool punya 28 SOCKS4
    yang mubazir — sekarang bisa dipakai. Handshake:
      VN=0x04, CD=0x01 (CONNECT), DSTPORT, DSTIP=0.0.0.1 (domain via
      SOCKS4a: userid kosong + hostname setelah 4-byte IP).
    Response 8 bytes: VN=0, CD (0x5A=granted).
    """
    import struct
    host_b = host.encode()
    # SOCKS4a: DSTIP 0.0.0.1 + hostname setelah userid (NULL-terminated)
    req = b"\x04\x01" + struct.pack(">H", port) + b"\x00\x00\x00\x01" + b"\x00" + host_b + b"\x00"
    sock.sendall(req)
    r2 = sock.recv(8)
    if len(r2) < 8 or r2[1] != 0x5A:
        raise ConnectionError(f"SOCKS4 CONNECT failed: {r2.hex()}")
    return


class GatewayHandler(BaseHTTPRequestHandler):
    # R17-T6: slowloris header — client kirim header sebagian lalu diam →
    # rfile.readline blok selamanya, thread hang. Timeout global request-read.
    timeout = UPSTREAM_PROXY_TIMEOUT
    """HTTP forward proxy that routes via session→proxy mapping."""

    def _get_session_proxy(self, country="", exclude=None, protocol=""):
        # R26-Z1 (proxy rotation): sid dari username cliproxy-style dulu
        sid, username_region, username_ttl = _sid_from_username(self.headers)
        if not country:
            country = self.headers.get("X-Country", "").upper()
        if not country and username_region:
            country = username_region
        # R30-GW1: X-Protocol header — client pilih protocol upstream
        # ("socks5" = no header leak, aman buat bulk automation akun)
        if not protocol:
            protocol = self.headers.get("X-Protocol", "").lower()
        # R31-GW12: X-Prefer: socks — prioritaskan SOCKS5/4 di atas HTTP
        # (socks = no header leak; HTTP proxy publik sering append Via/XFF).
        # Implementasi: coba socks dulu; kalau pool kosong, fallback http.
        prefer_socks = self.headers.get("X-Prefer", "").lower() == "socks"
        if prefer_socks and not protocol:
            protocol = "socks5"
        if self.server.mode == "rotate":
            return _next_proxy_round_robin(self.server.rotate_state, country, exclude, protocol)
        session_id = sid or self.headers.get("X-Session-ID", "")
        if not session_id:
            proxy = _pick_proxy(country, exclude=exclude, protocol=protocol)
            # R10-5: country requested but unavailable — 503, jangan DIRECT leak
            if proxy is None:
                raise CountryUnavailable(country)
            return proxy or "DIRECT"
        ttl = DEFAULT_SESSION_TTL
        try:
            if username_ttl:
                ttl = min(username_ttl, 3600)
            else:
                ttl = min(int(self.headers.get("X-Session-TTL", "")), 3600)
        except ValueError:
            pass
        # R11-7: retry dengan exclude — proxy #1 flaky jangan bikin session DIRECT
        exclude = set()  # proxies already tried in this retry loop
        # R26-Z3: sid baru → proxy BEDA dari session lain (rotation beneran).
        # get_best_proxy selalu return #1 → semua sid dapat IP sama.
        # Pakai search_proxies top-N + random, exclude proxy yang sudah di-pin.
        used_proxies = set()
        try:
            from proxy_pool import search_proxies  # R26-Z3: lazy import (sama dgn _pick_proxy)
            for s, entry in getattr(self.server.session_manager, "_sessions", {}).items():
                p = entry[0] if isinstance(entry, tuple) else entry
                if p and p != "DIRECT":
                    used_proxies.add(p)
        except Exception:
            pass
        for attempt in range(3):
            try:
                if used_proxies:
                    rows = search_proxies(
                        protocol=protocol, country_code=country, min_score=1,
                        max_age_minutes=0, max_results=20,
                    )
                    picked = None
                    for r in rows:
                        cand = f"{r['ip']}:{r['port']}"
                        if r.get("protocol") == "socks5":
                            cand = "socks5://" + cand
                        elif r.get("protocol") == "socks4":
                            cand = "socks4://" + cand  # R31-GW7
                        if cand not in used_proxies and cand not in exclude:
                            picked = cand
                            break
                    if picked is None:
                        raise CountryUnavailable(country)
                else:
                    picked = _pick_proxy(country, exclude=exclude, protocol=protocol)
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
                # R24-GW5: strip socks5:// prefix — DB key = ip:port
                # R31-GW7: socks4:// juga
                if proxy.startswith("socks5://") or proxy.startswith("socks4://"):
                    proxy = proxy.split("://", 1)[-1]
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
        tried = set()  # R21-GW3: proxy yang sudah dicoba request ini
        for _ in range(MAX_FAILOVER):
            upstream = None
            try:
                session_proxy = self._get_session_proxy(country=country, exclude=tried)
            except CountryUnavailable as e:
                # R10-5: no proxy for requested country — 503, jangan DIRECT leak
                self.send_error(503, f"No proxy available for country: {e.country}")
                return
            if session_proxy == "DIRECT":
                self._connect_direct(host, port)
                return
            t0 = time.monotonic()
            try:
                # R30-GW3: strip 'socks5://' prefix sebelum split — split(':')
                # pada 'socks5://ip:port' = 3 elemen → create_connection
                # ValueError 'too many values to unpack' (502). Bug pre-existing
                # R24-GW5, baru kelihatan pas X-Protocol socks5 dipakai.
                upstream_addr = session_proxy.split("://", 1)[-1]
                # R31-GW8: connect timeout 5s (bukan 10) — free pool 58% mati;
                # request hang 30s (3×10s) = sinyal bot. Fail-fast: cepet gagal,
                # cepet pindah proxy lain. Total failover ≤15s.
                upstream = socket.create_connection(upstream_addr.split(":"), timeout=5)
                upstream.settimeout(5)
                upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # R16-G1: matikan Nagle — hemat 1 RTT/burst di tunnel
                # R24-GW5: SOCKS5 upstream — pool punya 466 socks5 yang mubazir
                # (HTTP CONNECT cuma bisa ke proxy http). Deteksi protocol dari
                # session_proxy 'socks5://' prefix atau protocol field.
                if session_proxy.startswith("socks5://"):
                    _socks5_connect(upstream, host, port)
                    # handshake sukses = tunnel siap, langsung relay
                    self.send_response(200, "Connection Established")
                    self.end_headers()
                    self._log_usage(session_proxy, True, duration_ms=int((time.monotonic() - t0) * 1000))
                    self._tunnel(self.connection, upstream)
                    return
                if session_proxy.startswith("socks4://"):
                    # R31-GW7: SOCKS4 upstream (no auth, no header leak)
                    _socks4_connect(upstream, host, port)
                    self.send_response(200, "Connection Established")
                    self.end_headers()
                    self._log_usage(session_proxy, True, duration_ms=int((time.monotonic() - t0) * 1000))
                    self._tunnel(self.connection, upstream)
                    return
                # T3: sebagian proxy publik reject CONNECT tanpa UA
                # R31-GW10: UA 'Gateway/1.0' = sinyal bot di log upstream —
                # pakai browser UA (proxy publik bisa logging/ngintip header).
                upstream.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: {_UA_MOBILE}\r\nProxy-Connection: keep-alive\r\n\r\n".encode())
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
                    tried.add(session_proxy)  # R21-GW3: jangan coba lagi request ini
                    # R21-GW1: HTTP-only proxy (balas 403/405/500 untuk CONNECT)
                    # bukan "mati" — jangan cooldown panjang. Blacklist pendek
                    # (10s) supaya retry loop coba proxy lain dalam request sama,
                    # tapi proxy bisa dipakai lagi nanti (untuk HTTP path).
                    if b"200" not in resp and resp[:12] in (b"HTTP/1.1 403", b"HTTP/1.0 403", b"HTTP/1.1 405", b"HTTP/1.0 405", b"HTTP/1.1 500", b"HTTP/1.0 500", b"HTTP/1.1 400", b"HTTP/1.0 400"):
                        self._blacklist_proxy(session_proxy, "http-only")
                    else:
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
                # R22-P2: log_usage sukses HANYA setelah probe MITM lolos —
                # sebelumnya dicatat sebelum probe → sukses palsu masuk usage_log.
                # R30-GW4: X-No-MITM: 1 → paksa probe walau --allow-mitm
                # (client verify ketat kayak tokenharbor — jangan kasih cert palsu)
                if (not self.server.allow_mitm or self.headers.get("X-No-MITM")) and self._probe_mitm(host, port, session_proxy):
                    self._log_usage(session_proxy, False, "MITM cert", int((time.monotonic() - t0) * 1000))
                    tried.add(session_proxy)
                    self._blacklist_proxy(session_proxy, "mitm")
                    try:
                        upstream.close()
                    except Exception:
                        pass
                    last_err = "MITM proxy (fake cert)"
                    continue
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
                if session_proxy:
                    tried.add(session_proxy)  # R21-GW3
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
        # R26-Z1 (proxy rotation): endpoint kontrol — /ip, /ips, /session/<sid>
        if self._do_control():
            return
        self._forward_via_proxy("GET")

    def _do_control(self):
        """GET /ip → egress IP session; GET /ips → daftar IP + status;
        GET /session/<sid> → status satu session. Dijawab langsung gateway
        (tidak forward upstream) — mirip icanhazip."""
        path = self.path.split("?", 1)[0]
        if path not in ("/ip", "/ips", "/health") and not path.startswith("/session/"):
            return False
        if not _auth_ok(self, self.server.auth_secret):
            self.send_error(407, "Proxy Authentication Required")
            return True
        sm = self.server.session_manager
        sid, region, ttl = _sid_from_username(self.headers)
        if path == "/health":
            # R31-GW6: health endpoint — uptime, session count, pool size
            try:
                import sqlite3
                db = sqlite3.connect("data/proxies.db", timeout=2)
                pool_total = db.execute("SELECT COUNT(*) FROM proxies").fetchone()[0]
                pool_fresh = db.execute(
                    "SELECT COUNT(*) FROM proxies WHERE last_seen > datetime('now','-1 hour')"
                ).fetchone()[0]
                db.close()
            except Exception:
                pool_total = pool_fresh = -1
            sessions = len(getattr(sm, "_sessions", {}))
            body = {
                "status": "ok",
                "uptime_s": int(time.time() - getattr(self.server, "started_at", time.time())),
                "sessions": sessions,
                "pool_total": pool_total,
                "pool_fresh_1h": pool_fresh,
                "mode": getattr(self.server, "mode", "sticky"),
            }
        elif path == "/ip":
            proxy = None
            if sid:
                proxy = _session_proxy_of(sm, sid)
                if proxy is None:
                    try:
                        proxy = self._get_session_proxy(exclude=set(), protocol="")
                    except Exception:
                        proxy = None
            body = {"ip": _egress_ip(proxy) if proxy else "DIRECT",
                    "sid": sid or None, "proxy": proxy or "DIRECT"}
        elif path == "/ips":
            ips = []
            seen = set()
            for s, entry in getattr(sm, "_sessions", {}).items():
                p = entry[0] if isinstance(entry, tuple) else entry
                ip = _egress_ip(p)
                if ip in seen or ip == "DIRECT":
                    continue
                seen.add(ip)
                blocked = False
                try:
                    bl = self.server.rotate_state.get("blacklist") or {}
                    blocked = bl.get(p, 0) > time.time()
                except Exception:
                    pass
                ips.append({"ip": ip, "sid": s, "status": "blocked" if blocked else "alive"})
            body = {"ips": ips}
        else:  # /session/<sid>
            req_sid = path.split("/")[-1]
            proxy = _session_proxy_of(sm, req_sid)
            body = {"sid": req_sid, "ip": _egress_ip(proxy) if proxy else None,
                    "status": "alive" if proxy else "unknown"}
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        return True

    def _do_freshen(self):
        """R30-GW2: POST /freshen — trigger freshen_pool.py on-demand.

        Spawn subprocess background (detached), guard concurrent: kalau
        freshen lagi jalan (cron/agent lain), return 409 jangan dobel.
        """
        if not _auth_ok(self, self.server.auth_secret):
            self.send_error(407, "Proxy Authentication Required")
            return
        lock = getattr(self.server, "freshen_lock", None)
        if lock is None:
            lock = self.server.freshen_lock = threading.Lock()
        if not lock.acquire(blocking=False):
            body = {"status": "busy", "detail": "freshen already running"}
            self.send_response(409)
        else:
            def _run():
                try:
                    import subprocess
                    env = dict(os.environ)
                    env.setdefault("PROXY_DB", "data/proxies.db")
                    env.setdefault("PROXY_VALIDATION_WALL_TIMEOUT", "300")
                    env.setdefault("PROXY_SOURCE_MAX_BYTES", "2000000")
                    env.setdefault("PROXY_MAX_PROXIES_PER_SOURCE", "15000")
                    subprocess.run(
                        [sys.executable, "freshen_pool.py", "--max-validate", "400"],
                        cwd=os.path.dirname(os.path.abspath(__file__)),
                        env=env, timeout=900,
                    )
                except Exception:
                    pass
                finally:
                    lock.release()
            threading.Thread(target=_run, daemon=True).start()
            # R31-GW11: include pool stats biar agent tau baseline sebelum
            # fresh (jangan query /health lagi — 1 request cukup).
            try:
                import sqlite3
                db = sqlite3.connect("data/proxies.db", timeout=2)
                before = {
                    "total": db.execute("SELECT COUNT(*) FROM proxies").fetchone()[0],
                    "fresh_1h": db.execute(
                        "SELECT COUNT(*) FROM proxies WHERE last_seen > datetime('now','-1 hour')"
                    ).fetchone()[0],
                }
                db.close()
            except Exception:
                before = {"total": -1, "fresh_1h": -1}
            body = {"status": "started", "detail": "freshen_pool.py --max-validate 400", "pool_before": before}
            self.send_response(202)
        payload = json.dumps(body).encode()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        # R30-GW2: POST /freshen → trigger freshen_pool.py on-demand (background)
        path = self.path.split("?", 1)[0]
        if path == "/freshen":
            self._do_freshen()
            return
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
        url = self.path
        body = None
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
        t0 = time.monotonic()
        last_err = None
        tried = set()  # R21-GW3
        for _ in range(MAX_FAILOVER):  # R21-GW2: retry loop HTTP path (sama seperti CONNECT)
            try:
                # R24-GW5: HTTP path tidak support socks5 (urllib ProxyHandler
                # cuma http) — minta proxy http saja; socks5 untuk CONNECT path.
                session_proxy = self._get_session_proxy(exclude=tried, protocol="http")
            except CountryUnavailable as e:
                # R11-8: HTTP path — 503, bukan traceback/connection reset
                self.send_error(503, f"No proxy available for country: {e.country}")
                return
            if session_proxy == "DIRECT":
                self._forward_direct(method, url, body)
                return
            try:
                handler = urllib.request.ProxyHandler({
                    "http": f"http://{session_proxy}",
                    "https": f"http://{session_proxy}",
                })
                opener = urllib.request.build_opener(handler)
                req = urllib.request.Request(url, data=body, method=method)
                # R31-GW5: header hygiene — jangan forward header yang bocorin
                # asal koneksi (Via/XFF/Forwarded/X-Real-IP dari client bisa
                # kontradiksi sama egress proxy → target deteksi). X-Spoof-IP
                # opsional: set XFF palsu (efektif dgn elite proxy yang gak append).
                has_ua = False
                for key, val in self.headers.items():
                    kl = key.lower()
                    # R7-1: NEVER forward Proxy-Authorization (HMAC client creds)
                    # to a public upstream — replay within AUTH_WINDOW.
                    if kl in ("proxy-connection", "host", "x-session-id", "proxy-authorization",
                              "via", "forwarded", "x-forwarded-for", "x-real-ip", "x-spoof-ip"):
                        continue
                    if kl == "user-agent":
                        has_ua = True
                    req.add_header(key, val)
                # R31-GW5: UA default browser mobile — Python-urllib/3.x = sinyal
                # bot gede; target sering filter UA proxy/datacenter.
                if not has_ua:
                    req.add_header("User-Agent", _UA_MOBILE)
                spoof = self.headers.get("X-Spoof-IP", "").strip()
                if spoof:
                    req.add_header("X-Forwarded-For", spoof)
                resp = opener.open(req, timeout=UPSTREAM_PROXY_TIMEOUT)
                self._log_usage(session_proxy, True, duration_ms=int((time.monotonic() - t0) * 1000))
                self.send_response(resp.status)
                for key, val in resp.headers.items():
                    if key.lower() not in ("transfer-encoding", "connection"):
                        self.send_header(key, val)
                self.end_headers()
                try:
                    # R16-N4: timeout hanya berlaku sampai header; body read bisa
                    # hang selamanya kalau proxy lambat — set socket timeout ulang.
                    # R17-T5: resp.fp = BufferedReader, socket di fp.raw._sock.
                    # R18-T3: jalur urllib kadang raw = SocketIO tanpa _sock —
                    # fallback ke settimeout langsung di raw.
                    fp = getattr(resp, "fp", None)
                    raw = getattr(fp, "raw", None)
                    sock = getattr(raw, "_sock", None)
                    if sock is not None:
                        sock.settimeout(UPSTREAM_PROXY_TIMEOUT)
                    elif raw is not None and hasattr(raw, "settimeout"):
                        raw.settimeout(UPSTREAM_PROXY_TIMEOUT)
                    chunk = resp.read(65536)
                    while chunk:
                        self.wfile.write(chunk)
                        chunk = resp.read(65536)
                except Exception:
                    pass  # client hang/timeout — proxy already logged
                return
            except CountryUnavailable:
                raise
            except Exception as e:
                try:
                    self._log_usage(session_proxy, False, str(e), int((time.monotonic() - t0) * 1000))
                except Exception:
                    pass
                if session_proxy:
                    tried.add(session_proxy)  # R21-GW3
                try:
                    self.server.session_manager.report_failure(session_proxy)
                except Exception:
                    pass
                last_err = str(e)
        self.send_error(502, f"Upstream error: {last_err}")

    def _forward_direct(self, method, url, body=None):
        """Forward without upstream proxy."""
        try:
            if body is None:
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

    def _probe_mitm(self, host, port, session_proxy):
        """R22-GW4: TLS probe via upstream proxy — verify cert asli target.
        Proxy MITM (SSLVPN etc) inject cert palsu → verify gagal → True (MITM).
        Cache per-proxy 300s — probe 1x per proxy, bukan per request."""
        import ssl as ssl_mod
        cache = self.server.mitm_cache
        now = time.time()
        hit = cache.get(session_proxy)
        if hit and now < hit[0] + 300:
            return hit[1]
        try:
            # R24-GW5: socks5:// prefix — probe pakai SOCKS5 handshake, bukan HTTP CONNECT
            if session_proxy.startswith("socks5://"):
                s = socket.create_connection(session_proxy[len("socks5://"):].split(":"), timeout=UPSTREAM_PROXY_TIMEOUT)
                s.settimeout(UPSTREAM_PROXY_TIMEOUT)
                _socks5_connect(s, host, port)
            else:
                ip, pport = session_proxy.split(":")
                s = socket.create_connection((ip, int(pport)), timeout=UPSTREAM_PROXY_TIMEOUT)
                s.settimeout(UPSTREAM_PROXY_TIMEOUT)
                s.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: Gateway/1.0\r\n\r\n".encode())
                resp = b""
                while b"\r\n\r\n" not in resp and len(resp) < 8192:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    resp += chunk
                if b"200" not in resp:
                    s.close()
                    cache[session_proxy] = (now, True)  # CONNECT gagal = anggap MITM/jelek
                    return True
            ctx = ssl_mod.create_default_context()
            try:
                tls = ctx.wrap_socket(s, server_hostname=host)
                cert = tls.getpeercert()
                tls.close()
            except ssl_mod.SSLCertVerificationError:
                cache[session_proxy] = (now, True)  # cert fake → MITM
                return True
            except Exception:
                cache[session_proxy] = (now, True)  # TLS handshake gagal lain — anggap MITM
                return True
            ok = bool(cert)  # cert valid & verified
            cache[session_proxy] = (now, not ok)
            return not ok
        except (socket.timeout, ConnectionRefusedError, OSError):
            # R22-P3: timeout/refused ≠ MITM — proxy jelek/mati, bukan fake cert.
            # Jangan blacklist 600s (mitm) — biarkan cooldown normal (refused 60s).
            cache[session_proxy] = (now, False)
            return False
        except Exception:
            cache[session_proxy] = (now, True)
            return True

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
    parser.add_argument("--allow-mitm", action="store_true",
                        help="R22-GW4: izinkan proxy MITM (fake cert). DEFAULT OFF = aman; "
                             "ON = semua proxy dipakai (TLS bisa dibaca proxy — jangan kirim credential)")
    args = parser.parse_args()

    sm = SessionManager(default_ttl=args.session_ttl)
    server = ThreadingHTTPServer((args.bind, args.port), GatewayHandler)
    server.daemon_threads = True  # R11-5: shutdown ga nunggu tunnel idle
    server.session_manager = sm
    server.mode = args.mode
    server.rotate_state = {"list": None, "index": 0, "refreshed": 0, "blacklist": {}}
    server.mitm_cache = {}  # R22-GW4: proxy → (ts, is_mitm) cache 300s
    server.allow_mitm = args.allow_mitm
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
