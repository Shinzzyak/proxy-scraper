#!/usr/bin/env python3
"""
Integration test: prove session-sticky routing works end-to-end.

Starts the gateway, makes N sequential requests through it with the same
X-Session-ID, and verifies that ip-api.com sees the SAME egress IP
every time. No new dependencies — stdlib only.

Usage:
    1. Start a gateway:  python3 gateway.py --port 8081
    2. Run this test:    python3 test_sticky_integration.py --port 8081
    Or let it auto-start the gateway:
        python3 test_sticky_integration.py --auto

Exit 0 = all requests returned same IP (sticky works).
Exit 1 = IP changed or requests failed (sticky broken).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

GEO_TARGET = "http://ip-api.com/json/?fields=status,query,countryCode,city"


def probe(proxy_url, session_id, target=GEO_TARGET, timeout=10):
    """Make one request through the gateway proxy and return the observed egress geo."""
    handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
    opener = urllib.request.build_opener(handler)
    req = urllib.request.Request(target, headers={
        "User-Agent": "ProxyStickyTest/1.0",
        "X-Session-ID": session_id,
    })
    try:
        with opener.open(req, timeout=timeout) as resp:
            data = json.loads(resp.read(8192).decode())
        if data.get("status") != "success":
            return {"error": f"geo status: {data.get('status')}"}
        return {"ip": data["query"], "country": data.get("countryCode", ""), "city": data.get("city", "")}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def run_test(port, session_id, samples, delay):
    proxy_url = f"http://127.0.0.1:{port}"
    results = []
    for i in range(samples):
        result = probe(proxy_url, session_id)
        results.append(result)
        ip = result.get("ip", "?")
        city = result.get("city", "?")
        country = result.get("country", "?")
        err = result.get("error", "")
        if err:
            print(f"  [{i+1}/{samples}] ❌ {err}")
        else:
            print(f"  [{i+1}/{samples}] ✅ IP={ip}  city={city}  country={country}")
        if i < samples - 1:
            time.sleep(delay)

    # Analyze
    successes = [r for r in results if r.get("ip")]
    failures = [r for r in results if r.get("error")]
    unique_ips = sorted({r["ip"] for r in successes})
    unique_countries = sorted({r["country"] for r in successes})

    print(f"\n{'='*50}")
    print(f"Results: {len(successes)}/{samples} succeeded, {len(failures)} failed")
    print(f"Unique IPs observed:     {unique_ips}")
    print(f"Unique countries:        {unique_countries}")

    if len(successes) < samples:
        print("⚠️  Some requests failed — may indicate proxy died mid-session")
    if len(unique_ips) == 1 and len(successes) == samples:
        print("✅ STICKY CONFIRMED: all requests egressed from the same IP")
        return True
    elif len(unique_ips) > 1:
        print("❌ STICKY BROKEN: IP changed across requests with same session ID")
        return False
    else:
        print("❌ All requests failed — cannot determine stickiness")
        return False


def main():
    parser = argparse.ArgumentParser(description="Integration test for session-sticky gateway routing")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--session-id", default="test-sticky-001")
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--delay", type=float, default=2.0)
    parser.add_argument("--auto", action="store_true", help="Auto-start gateway before testing")
    args = parser.parse_args()

    gateway_proc = None
    if args.auto:
        print(f"🚀 Starting gateway on port {args.port}...")
        gateway_proc = subprocess.Popen(
            [sys.executable, "gateway.py", "--port", str(args.port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid,
        )
        time.sleep(2)

    try:
        print(f"🔍 Testing sticky routing: {args.samples} requests, session={args.session_id}, port={args.port}\n")
        success = run_test(args.port, args.session_id, args.samples, args.delay)
        raise SystemExit(0 if success else 1)
    finally:
        if gateway_proc:
            print("\n🛑 Stopping gateway...")
            os.killpg(os.getpgid(gateway_proc.pid), signal.SIGTERM)
            gateway_proc.wait(timeout=5)


if __name__ == "__main__":
    main()
