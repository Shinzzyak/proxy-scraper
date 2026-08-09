#!/usr/bin/env python3
"""ai_reach.py — Check whether proxies can reach AI service endpoints.

Standalone CLI (stdlib-only, no scraper imports). For each proxy, attempts a
CONNECT-style TLS reachability check against configurable AI endpoints and
reports per-target results as JSON.

This checks TLS reachability (can the proxy open a TCP+TLS path to the
endpoint), NOT whether an API key works. Label results as `tls_reachable`.

Usage:
    python3 ai_reach.py -l proxies.txt --targets chatgpt --max-proxies 200
    python3 ai_reach.py -l proxies.txt --targets all -o ai-reach.json
    cat proxies.txt | python3 ai_reach.py --targets deepseek --timeout 8

Exit codes: 0 ok, 1 no proxies, 2 wall timeout hit.
"""
import argparse
import json
import os
import socket
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

WALL_TIMEOUT = int(os.getenv("AI_REACH_WALL_TIMEOUT", "300"))

# Non-auth endpoints: stable landing/trace pages, no API key needed.
# Label: endpoint TLS-reachable ≠ AI service usable — see README caveats.
TARGETS = {
    "chatgpt":  ("chatgpt.com", 443),
    "claude":   ("claude.ai", 443),
    "gemini":   ("gemini.google.com", 443),
    "grok":     ("grok.com", 443),
    "deepseek": ("chat.deepseek.com", 443),
}


def check_proxy(proxy: str, target: str, timeout: int) -> bool:
    """True if proxy can establish TCP+TLS to the target endpoint."""
    host, port = TARGETS[target]
    try:
        if proxy == "DIRECT":
            return _tls_connect(host, port, timeout)
        phost, pport = proxy.rsplit(":", 1)
        pport = int(pport)
        # Manual CONNECT through HTTP proxy, then TLS to target.
        sock = socket.create_connection((phost, pport), timeout=timeout)
        sock.settimeout(timeout)
        sock.sendall(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode())
        # read until full header (CONNECT reply can arrive in multiple segments)
        resp = b""
        while b"\r\n\r\n" not in resp and len(resp) < 4096:
            chunk = sock.recv(4096)
            if not chunk:
                break
            resp += chunk
        if b"200" not in resp.split(b"\r\n", 1)[0]:
            sock.close()
            return False
        return _tls_connect(host, port, timeout, sock=sock)
    except Exception:
        return False


def _tls_connect(host: str, port: int, timeout: int, sock=None):
    """Wrap an optional existing socket (post-CONNECT) in TLS and handshake."""
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # reachability only; cert validity not checked
        if sock is None:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.settimeout(timeout)
        tls = ctx.wrap_socket(sock, server_hostname=host)
        tls.close()
        return True
    except Exception:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return False


def main():
    ap = argparse.ArgumentParser(description="AI-endpoint TLS reachability checker for proxies")
    ap.add_argument("-l", "--list", help="File with proxies (host:port, one per line). Default: stdin")
    ap.add_argument("-o", "--output", help="Write JSON results to file")
    ap.add_argument("--targets", default="chatgpt",
                    help="Comma-separated targets: " + ",".join(TARGETS) + " (default: chatgpt)")
    ap.add_argument("--timeout", type=int, default=8, help="Per-request timeout seconds (default 8)")
    ap.add_argument("--max-proxies", type=int, default=0, help="Limit proxies checked (0 = all)")
    ap.add_argument("--threads", type=int, default=20, help="Concurrent checks (default 20)")
    args = ap.parse_args()

    targets = [t.strip() for t in args.targets.split(",") if t.strip() in TARGETS]
    if not targets:
        print(json.dumps({"error": f"no valid targets; use: {','.join(TARGETS)}"}), file=sys.stderr)
        sys.exit(1)

    if args.list:
        with open(args.list) as f:
            proxies = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    else:
        proxies = [ln.strip() for ln in sys.stdin if ln.strip() and not ln.startswith("#")]
    if args.max_proxies > 0:
        proxies = proxies[: args.max_proxies]
    if not proxies:
        print(json.dumps({"error": "no proxies input"}), file=sys.stderr)
        sys.exit(1)

    t0 = time.time()
    results, checked = [], 0
    timed_out = False

    def work(proxy):
        return proxy, {t: check_proxy(proxy, t, args.timeout) for t in targets}

    pool = ThreadPoolExecutor(max_workers=args.threads)
    futures = [pool.submit(work, p) for p in proxies]
    try:
        while True:
            remaining = WALL_TIMEOUT - (time.time() - t0)
            if remaining <= 0:
                timed_out = True
                break
            try:
                fut = next(as_completed(futures, timeout=remaining))
            except StopIteration:
                break  # all futures done
            except TimeoutError:
                timed_out = True  # wall hit — R4-1: must cut even mid-request
                break
            proxy, res = fut.result()
            results.append({"proxy": proxy, "reachable": res})
            checked += 1
    finally:
        # do NOT wait for in-flight futures — wall timeout must be real
        pool.shutdown(wait=False, cancel_futures=True)
        if timed_out:
            # emit partial results, then hard-exit: interpreter exit waits for
            # non-daemon worker threads blocked in socket.connect
            summary = {t: sum(1 for r in results if r["reachable"].get(t)) for t in targets}
            out = {
                "checked": checked,
                "wall_timeout_s": WALL_TIMEOUT,
                "timed_out": True,
                "summary": summary,
                "results": results,
            }
            text = json.dumps(out, indent=2)
            if args.output:
                with open(args.output, "w") as f:
                    f.write(text + "\n")
            print(text)
            sys.stdout.flush()
            os._exit(2)

    summary = {t: sum(1 for r in results if r["reachable"].get(t)) for t in targets}
    out = {
        "checked": checked,
        "wall_timeout_s": WALL_TIMEOUT,
        "summary": summary,
        "results": results,
    }
    text = json.dumps(out, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
        print(f"✅ {checked} proxies → {args.output}")
        print(json.dumps({"summary": summary}, indent=2))
    else:
        print(text)
    sys.exit(2 if timed_out else 0)


if __name__ == "__main__":
    main()
