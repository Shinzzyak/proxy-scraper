#!/usr/bin/env python3
"""
Free Proxy Scraper v2 — aggregates from 25+ sources.
Outputs: proxies.txt (host:port)
"""
import json
import re
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set

# ── Sources (25+) ──────────────────────────────────────────────────────

PROXY_SOURCES = [
    # monosans
    ("monosans-http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "host:port"),
    ("monosans-socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "host:port"),
    # mmpx12
    ("mmpx12-http", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "host:port"),
    ("mmpx12-https", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt", "host:port"),
    # hookzof
    ("hookzof-socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "host:port"),
    # roosterkid
    ("roosterkid", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt", "host:port"),
    # proxyscrape
    ("proxyscrape-http", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&timeout=10000", "host:port"),
    ("proxyscrape-socks5", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks5&timeout=10000", "host:port"),
    # free-proxy-list
    ("free-proxy-list-http", "https://www.proxy-list.download/api/v1/get?type=http", "host:port"),
    ("free-proxy-list-https", "https://www.proxy-list.download/api/v1/get?type=https", "host:port"),
    # openproxy
    ("openproxy-http", "https://openproxylist.xyz/http.txt", "host:port"),
    # komutan234
    ("komutan234-http", "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/http.txt", "host:port"),
    ("komutan234-socks4", "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/socks4.txt", "host:port"),
    # VPSLabCloud
    ("vpslab-http", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_anonymous.txt", "host:port"),
    ("vpslab-ssl", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_ssl.txt", "host:port"),
    # databay-labs
    ("databay-http", "https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/http.txt", "host:port"),
    ("databay-socks4", "https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/socks4.txt", "host:port"),
    # gfpcom
    ("gfpcom-http", "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/http.txt", "host:port"),
    ("gfpcom-socks5", "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/socks5.txt", "host:port"),
    # proxy4parsing
    ("proxy4parsing-http", "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt", "host:port"),
    # officialputuid
    ("officialputuid-socks5", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy-SOCKS5/socks5.txt", "host:port"),
    # TheSpeedX
    ("thespeedx-http", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt", "host:port"),
    ("thespeedx-socks5", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", "host:port"),
    # clarketm
    ("clarketm-http", "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "host:port"),
    # Sunny9577
    ("sunny-http", "https://raw.githubusercontent.com/Sunny9577/proxy-list/master/generated/http.txt", "host:port"),
    # zjev
    ("zjev-http", "https://raw.githubusercontent.com/zjev/proxy-list/main/http.txt", "host:port"),
    # ermaozi
    ("ermaozi-http", "https://raw.githubusercontent.com/ermaozi/get_proxy/main/alive_proxy.txt", "host:port"),
    # tsayen
    ("tsayen-http", "https://raw.githubusercontent.com/tsayen/proxy-list/refs/heads/master/http.txt", "host:port"),
    # hawspider
    ("hawspider-http", "https://raw.githubusercontent.com/hawspider/Proxy-List/refs/heads/main/http.txt", "host:port"),
]

# ── Helpers ────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*",
}

PROXY_RE = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*[:\s]\s*(\d{1,5})")


def fetch(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  ✗ {url}: {e}", file=sys.stderr)
        return ""


def extract_proxies(text: str) -> List[str]:
    proxies = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        m = PROXY_RE.search(line)
        if m:
            ip, port = m.group(1), m.group(2)
            if int(port) > 65535:
                continue
            proxies.append(f"{ip}:{port}")
    return proxies


# ── Scraper ────────────────────────────────────────────────────────────

def scrape_source(name: str, url: str, fmt: str) -> List[str]:
    print(f"  → {name}...", end=" ", flush=True)
    text = fetch(url)
    if not text:
        print("(empty/error)")
        return []
    proxies = extract_proxies(text)
    print(f"{len(proxies)}")
    return proxies


def scrape_all() -> Set[str]:
    all_proxies = set()
    print(f"Scraping {len(PROXY_SOURCES)} sources...\n")

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {
            pool.submit(scrape_source, name, url, fmt): name
            for name, url, fmt in PROXY_SOURCES
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                all_proxies.update(result)
            except Exception as e:
                print(f"  ✗ Error: {e}", file=sys.stderr)

    return all_proxies


# ── Validation ─────────────────────────────────────────────────────────

def validate_proxy(proxy: str, timeout: int = 3) -> bool:
    import socket
    host, port = proxy.split(":")
    try:
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True
    except:
        return False


def filter_valid(proxies: Set[str], max_check: int = 500) -> List[str]:
    sample = list(proxies)[:max_check]
    valid = []
    print(f"\nValidating {len(sample)} proxies (TCP connect)...")

    with ThreadPoolExecutor(max_workers=80) as pool:
        futures = {pool.submit(validate_proxy, p): p for p in sample}
        for future in as_completed(futures):
            if future.result():
                valid.append(futures[future])

    print(f"  ✓ {len(valid)}/{len(sample)} alive")
    return valid


# ── Main ───────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Free Proxy Scraper v2")
    parser.add_argument("-o", "--output", default="proxies.txt")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--max-validate", type=int, default=500)
    args = parser.parse_args()

    start = time.time()
    proxies = scrape_all()

    if not proxies:
        print("\n✗ No proxies found!", file=sys.stderr)
        sys.exit(1)

    print(f"\n📊 Total unique: {len(proxies)}")

    if args.validate:
        valid = filter_valid(proxies, args.max_validate)
        if valid:
            proxies = set(valid)

    sorted_proxies = sorted(proxies, key=lambda p: p.split(":")[0])

    with open(args.output, "w") as f:
        for p in sorted_proxies:
            f.write(p + "\n")

    elapsed = time.time() - start
    print(f"✅ Saved {len(sorted_proxies)} proxies to {args.output} ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()
