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
import select
import socket
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from session_manager import SessionManager

# ponytail: one default TTL; when proxy pool exposes health, reduce TTL for unhealthy proxies
DEFAULT_SESSION_TTL = 300
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8080
GATEWAY_TIMEOUT = 15
UPSTREAM_PROXY_TIMEOUT = 10


def _pick_proxy():
    """Select a fresh HTTP proxy from the pool.

    Returns 'host:port' string. Falls back to direct connection
    ('DIRECT') if the pool is empty.
    """
    try:
        from proxy_pool import get_best_proxy
        proxy = get_best_proxy(protocol="http", min_score=0, max_age_minutes=60)
        if proxy:
            return f"{proxy['ip']}:{proxy['port']}"
    except Exception:
        pass
    return "DIRECT"


class GatewayHandler(BaseHTTPRequestHandler):
    """HTTP forward proxy that routes via session→proxy mapping."""

    def _get_session_proxy(self):
        session_id = self.headers.get("X-Session-ID", "")
        if not session_id:
            return _pick_proxy()
        return self.server.session_manager.get_or_create(session_id, _pick_proxy)

    def do_CONNECT(self):
        """HTTPS tunneling: establish CONNECT through upstream proxy."""
        session_proxy = self._get_session_proxy()
        host, _, port = self.path.partition(":")
        port = int(port) if port else 443

        if session_proxy == "DIRECT":
            self._connect_direct(host, port)
            return

        try:
            upstream = socket.create_connection(session_proxy.split(":"), timeout=UPSTREAM_PROXY_TIMEOUT)
            upstream.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
            resp = upstream.recv(4096)
            if b"200" not in resp:
                upstream.close()
                self.send_error(502, "Upstream CONNECT failed")
                return
            self.send_response(200, "Connection Established")
            self.end_headers()
            self._tunnel(self.connection, upstream)
        except Exception as e:
            self.send_error(502, f"Upstream error: {e}")

    def _connect_direct(self, host, port):
        """CONNECT without upstream proxy (direct)."""
        try:
            remote = socket.create_connection((host, port), timeout=GATEWAY_TIMEOUT)
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
        session_proxy = self._get_session_proxy()
        url = self.path

        if session_proxy == "DIRECT":
            self._forward_direct(method, url)
            return

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
                if key.lower() not in ("proxy-connection", "host", "x-session-id"):
                    req.add_header(key, val)

            resp = opener.open(req, timeout=UPSTREAM_PROXY_TIMEOUT)
            self.send_response(resp.status)
            for key, val in resp.headers.items():
                if key.lower() not in ("transfer-encoding", "connection"):
                    self.send_header(key, val)
            self.end_headers()
            chunk = resp.read(65536)
            while chunk:
                self.wfile.write(chunk)
                chunk = resp.read(65536)
        except urllib.error.HTTPError as e:
            self.send_response(e.code)
            self.end_headers()
            if e.readable():
                self.wfile.write(e.read(65536))
        except Exception as e:
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
                if key.lower() not in ("proxy-connection", "x-session-id"):
                    req.add_header(key, val)
            resp = urllib.request.urlopen(req, timeout=GATEWAY_TIMEOUT)
            self.send_response(resp.status)
            for key, val in resp.headers.items():
                if key.lower() not in ("transfer-encoding", "connection"):
                    self.send_header(key, val)
            self.end_headers()
            chunk = resp.read(65536)
            while chunk:
                self.wfile.write(chunk)
                chunk = resp.read(65536)
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
            while True:
                readable, _, errors = select.select(sockets, [], sockets, GATEWAY_TIMEOUT)
                if errors:
                    break
                if not readable:
                    break
                for sock in readable:
                    data = sock.recv(65536)
                    if not data:
                        return
                    target = remote if sock is client else client
                    target.sendall(data)
        finally:
            remote.close()

    def log_message(self, format, *args):
        # Suppress default access log noise; only log errors
        pass


def _cleanup_loop(session_manager, interval=60):
    """Background thread: purge expired sessions periodically."""
    while True:
        time.sleep(interval)
        removed = session_manager.cleanup()
        if removed:
            print(f"♻️ Purged {removed} expired sessions", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Local HTTP proxy gateway with session-based sticky routing")
    parser.add_argument("--bind", default=DEFAULT_BIND)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--session-ttl", type=int, default=DEFAULT_SESSION_TTL)
    args = parser.parse_args()

    sm = SessionManager(default_ttl=args.session_ttl)
    server = ThreadingHTTPServer((args.bind, args.port), GatewayHandler)
    server.session_manager = sm

    cleanup = threading.Thread(target=_cleanup_loop, args=(sm,), daemon=True)
    cleanup.start()

    print(f"🔀 Gateway listening on {args.bind}:{args.port} (session TTL {args.session_ttl}s)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Gateway stopped")
        server.server_close()


if __name__ == "__main__":
    main()
