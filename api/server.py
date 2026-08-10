#!/usr/bin/env python3
"""
Simple Proxy Pool API — serves best proxies as JSON.

Usage:
    python3 api/server.py              # Run on port 8080
    python3 api/server.py --port 3000  # Custom port

Endpoints:
    GET /api/proxies?protocol=http&country=ID&limit=10
    GET /api/proxies/best?protocol=http&country=ID
    GET /api/stats
    GET /api/leaderboard
    GET /api/health
"""
import json
import os
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from proxy_pool import get_db, get_pool_stats, get_usage_leaderboard, get_best_proxy


class ProxyHandler(BaseHTTPRequestHandler):
    def _handle_request(self, head_only=False):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/api/proxies":
            self._handle_proxies(params, head_only)
        elif parsed.path == "/api/proxies/best":
            self._handle_best(params, head_only)
        elif parsed.path == "/api/stats":
            self._handle_stats(head_only)
        elif parsed.path == "/api/leaderboard":
            self._handle_leaderboard(head_only)
        elif parsed.path == "/api/health":
            self._json_response({"status": "ok", "service": "proxy-pool-api"}, head_only=head_only)
        else:
            self._json_response({"error": "Not found"}, 404, head_only=head_only)

    def do_GET(self):
        self._handle_request()

    def do_HEAD(self):
        # F8-6 + R12-6: HEAD tanpa body
        self._handle_request(head_only=True)

    def do_OPTIONS(self):
        # F8-6: CORS preflight — browser cross-origin fetch sends OPTIONS first
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Session-ID")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _handle_proxies(self, params, head_only=False):
        protocol = params.get("protocol", [""])[0]  # empty = all protocols
        country = params.get("country", [""])[0]
        try:
            # Evidence round 4: cap 500 menyesatkan limit=9999 — naikkan ke 5000
            limit = max(1, min(int(params.get("limit", ["10"])[0]), 5000))
        except ValueError:
            self._json_response({"error": "limit must be an integer"}, 400)
            return
        anonymity = params.get("anonymity", [""])[0]

        conn = get_db()
        try:
            q = "SELECT * FROM proxies"
            p = []
            conds = []
            if protocol:
                conds.append("protocol = ?")
                p.append(protocol)
            if country:
                conds.append("country_code = ?")
                p.append(country.upper())
            if anonymity:
                conds.append("anonymity = ?")
                p.append(anonymity)
            if conds:
                q += " WHERE " + " AND ".join(conds)
            # F8-8 (incomplete fix): API raw query must also exclude banned zombies
            if conds:
                q += " AND last_seen != ''"
            else:
                q += " WHERE last_seen != ''"
            q += " ORDER BY score DESC, response_time_ms ASC LIMIT ?"
            p.append(limit)
            rows = conn.execute(q, p).fetchall()
            proxies = [dict(r) for r in rows]
            self._json_response({
                "count": len(proxies),
                "protocol": protocol,
                "country": country,
                "proxies": proxies,
            }, head_only=head_only)
        finally:
            conn.close()

    def _handle_best(self, params, head_only=False):
        protocol = params.get("protocol", ["http"])[0]
        country = params.get("country", [""])[0]
        proxy = get_best_proxy(protocol, country)
        if proxy:
            self._json_response(proxy, head_only=head_only)
        else:
            self._json_response({"error": "No proxy available"}, 404, head_only=head_only)

    def _handle_stats(self, head_only=False):
        stats = get_pool_stats()
        self._json_response(stats, head_only=head_only)

    def _handle_leaderboard(self, head_only=False):
        lb = get_usage_leaderboard(20)
        self._json_response({"leaderboard": lb}, head_only=head_only)

    def _json_response(self, data, status=200, head_only=False):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1",
                    help="Bind address (default 127.0.0.1 — use 0.0.0.0 only behind auth/reverse proxy)")
    args = ap.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), ProxyHandler)  # T4: was single-threaded — 1 slow query blocked /api/health
    server.daemon_threads = True  # R11-5: shutdown ga nunggu tunnel idle
    print(f"🚀 Proxy Pool API listening on {args.host}:{args.port}")
    print(f"   GET /api/proxies?protocol=http&country=ID&limit=10")
    print(f"   GET /api/proxies/best?protocol=http")
    print(f"   GET /api/stats")
    print(f"   GET /api/leaderboard")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()  # R12-10: graceful shutdown on SIGINT


if __name__ == "__main__":
    main()
