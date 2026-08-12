#!/usr/bin/env python3
"""JA3 proxy — HTTP proxy server yang emulasi TLS fingerprint Chrome.

Kenapa: web besar deteksi proxy via TLS fingerprint (JA3/JA4).
Python-urllib/curl punya JA3 beda dari browser → langsung ketahuan.
curl_cffi = libcurl dengan patch impersonate → JA3 PERSIS Chrome.

Pakai: python3 ja3_proxy.py --port 8082
Client: HTTP_PROXY=http://127.0.0.1:8082 (atau gateway dengan X-JA3: chrome)
"""
import argparse
import json
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# curl_cffi dari venv (ja3env) — fallback: system
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("curl_cffi tidak ada. Install: python3 -m venv ja3env && ja3env/bin/pip install curl_cffi")
    sys.exit(1)

IMPERSONATE = "chrome124"
UPSTREAM_TIMEOUT = 20


class Ja3Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _handle(self, method):
        try:
            url = self.path
            body = None
            cl = int(self.headers.get("Content-Length", 0))
            if cl > 0:
                body = self.rfile.read(cl)
            headers = {k: v for k, v in self.headers.items()
                       if k.lower() not in ("proxy-connection", "proxy-authorization", "connection")}
            # JA3 Chrome via curl_cffi — tanpa proxy upstream (langsung keluar
            # dari VPS, JA3 Chrome). Atau proxy upstream kalau X-Upstream-Proxies.
            upstream = self.headers.get("X-Upstream-Proxies", "").strip()
            proxies = {"http": upstream, "https": upstream} if upstream else None
            resp = cffi_requests.request(
                method, url, data=body, headers=headers,
                impersonate=IMPERSONATE, proxies=proxies, timeout=UPSTREAM_TIMEOUT,
            )
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp.content)))
            self.end_headers()
            self.wfile.write(resp.content)
        except Exception as e:
            try:
                body = json.dumps({"error": str(e)}).encode()
                self.send_response(502)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                pass

    def do_GET(self): self._handle("GET")
    def do_POST(self): self._handle("POST")
    def do_PUT(self): self._handle("PUT")
    def do_DELETE(self): self._handle("DELETE")
    def do_PATCH(self): self._handle("PATCH")
    def do_HEAD(self): self._handle("HEAD")

    def log_message(self, fmt, *args):
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8082)
    args = ap.parse_args()
    server = ThreadingHTTPServer((args.bind, args.port), Ja3Handler)
    server.daemon_threads = True
    print(f"JA3 proxy listening on {args.bind}:{args.port} (impersonate={IMPERSONATE})")
    server.serve_forever()


if __name__ == "__main__":
    main()
