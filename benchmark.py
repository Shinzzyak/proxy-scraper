#!/usr/bin/env python3
"""
benchmark.py — Real HTTP latency benchmark through proxies.

Tests actual response time to real targets (google.com, github.com)
through each proxy. More accurate than TCP connect time.
"""
import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

TARGETS = [
    "https://httpbin.org/ip",
    "https://api.ipify.org?format=json",
    "https://icanhazip.com",
]
TIMEOUT = 10
MAX_WORKERS = 50


def benchmark_single(proxy_dict: Dict, target: str = None) -> Dict:
    """Test real HTTP latency through a proxy."""
    if target is None:
        target = TARGETS[0]

    ip = proxy_dict["ip"]
    port = proxy_dict["port"]
    protocol = proxy_dict.get("protocol", "http")

    proxy_url = f"http://{ip}:{port}"
    if protocol == "socks5":
        proxy_url = f"socks5://{ip}:{port}"

    proxy_handler = urllib.request.ProxyHandler({
        "http": proxy_url,
        "https": proxy_url,
    })
    opener = urllib.request.build_opener(proxy_handler)

    t0 = time.time()
    try:
        req = urllib.request.Request(target, headers={"User-Agent": "Mozilla/5.0"})
        resp = opener.open(req, timeout=TIMEOUT)
        body = resp.read().decode("utf-8", errors="ignore")[:500]
        latency_ms = round((time.time() - t0) * 1000)
        return {
            "ip": ip, "port": port, "protocol": protocol,
            "latency_ms": latency_ms,
            "status": "ok",
            "response_preview": body[:100],
            "target": target,
        }
    except Exception as e:
        latency_ms = round((time.time() - t0) * 1000)
        return {
            "ip": ip, "port": port, "protocol": protocol,
            "latency_ms": latency_ms,
            "status": "failed",
            "error": str(e)[:100],
            "target": target,
        }


def benchmark_batch(proxies: List[Dict], max_workers: int = MAX_WORKERS,
                    targets: List[str] = None) -> List[Dict]:
    """Benchmark multiple proxies in parallel."""
    if targets is None:
        targets = [TARGETS[0]]

    results = []
    total = len(proxies)
    print(f"\n🏎️ Benchmarking {total} proxies ({len(targets)} targets)...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {}
        for p in proxies:
            for t in targets:
                fut = pool.submit(benchmark_single, p, t)
                futs[fut] = p

        done = 0
        for fut in as_completed(futs):
            done += 1
            result = fut.result()
            results.append(result)
            status = "✅" if result["status"] == "ok" else "❌"
            print(f"  {status} {result['ip']}:{result['port']} → {result['latency_ms']}ms ({result['status']})")
            if done % 50 == 0:
                print(f"  ... {done}/{total * len(targets)} tested")

    # Aggregate by proxy (best latency across targets)
    best = {}
    for r in results:
        key = f"{r['ip']}:{r['port']}"
        if key not in best or r["latency_ms"] < best[key]["latency_ms"]:
            best[key] = r

    ranked = sorted(best.values(), key=lambda x: x["latency_ms"])
    ok_count = sum(1 for r in ranked if r["status"] == "ok")
    print(f"\n📊 Benchmark complete: {ok_count}/{len(ranked)} alive, avg {sum(r['latency_ms'] for r in ranked)//max(len(ranked),1)}ms")

    return ranked


def save_benchmark(results: List[Dict], output: str = "benchmark.json"):
    """Save benchmark results."""
    with open(output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✅ Benchmark → {output} ({len(results)} results)")


if __name__ == "__main__":
    sample = [
        {"ip": "104.25.1.63", "port": 80, "protocol": "http"},
        {"ip": "46.254.92.128", "port": 80, "protocol": "http"},
    ]
    results = benchmark_batch(sample, max_workers=5)
    save_benchmark(results, "/tmp/test_benchmark.json")
