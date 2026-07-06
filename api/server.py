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
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from proxy_pool import get_db, get_pool_stats, get_usage_leaderboard, get_best_proxy


class ProxyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if parsed.path == "/api/proxies":
            self._handle_proxies(params)
        elif parsed.path == "/api/proxies/best":
            self._handle_best(params)
        elif parsed.path == "/api/stats":
            self._handle_stats()
        elif parsed.path == "/api/leaderboard":
            self._handle_leaderboard()
        elif parsed.path == "/api/health":
            self._json_response({"status": "ok", "service": "proxy-pool-api"})
        else:
            self._json_response({"error": "Not found"}, 404)

    def _handle_proxies(self, params):
        protocol = params.get("protocol", ["http"])[0]
        country = params.get("country", [""])[0]
        limit = int(params.get("limit", ["10"])[0])
        anonymity = params.get("anonymity", [""])[0]

        conn = get_db()
        try:
            q = "SELECT * FROM proxies WHERE protocol = ?"
            p = [protocol]
            if country:
                q += " AND country_code = ?"
                p.append(country.upper())
            if anonymity:
                q += " AND anonymity = ?"
                p.append(anonymity)
            q += " ORDER BY score DESC, response_time_ms ASC LIMIT ?"
            p.append(limit)
            rows = conn.execute(q, p).fetchall()
            proxies = [dict(r) for r in rows]
            self._json_response({
                "count": len(proxies),
                "protocol": protocol,
                "country": country,
                "proxies": proxies,
            })
        finally:
            conn.close()

    def _handle_best(self, params):
        protocol = params.get("protocol", ["http"])[0]
        country = params.get("country", [""])[0]
        proxy = get_best_proxy(protocol, country)
        if proxy:
            self._json_response(proxy)
        else:
            self._json_response({"error": "No proxy available"}, 404)

    def _handle_stats(self):
        stats = get_pool_stats()
        self._json_response(stats)

    def _handle_leaderboard(self):
        lb = get_usage_leaderboard(20)
        self._json_response({"leaderboard": lb})

    def _json_response(self, data, status=200):
        body = json.dumps(data, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="0.0.0.0")
    args = ap.parse_args()

    server = HTTPServer((args.host, args.port), ProxyHandler)
    print(f"🚀 Proxy Pool API listening on {args.host}:{args.port}")
    print(f"   GET /api/proxies?protocol=http&country=ID&limit=10")
    print(f"   GET /api/proxies/best?protocol=http")
    print(f"   GET /api/stats")
    print(f"   GET /api/leaderboard")
    server.serve_forever()


if __name__ == "__main__":
    main()
