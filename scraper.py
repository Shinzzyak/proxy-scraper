#!/usr/bin/env python3
"""
Free Proxy Scraper v5 — Validation + JSON + Scoring + Geolocation + Pool + Alerts.
Outputs: proxies.txt, proxies.json, proxies-cred.txt, source-health.json, pool.json
"""
import argparse
import base64
import json
import os
import re
import socket
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from typing import List, Set, Tuple, Dict

try:
    from proxy_pool import (
        upsert_proxy, log_usage, log_source_health, log_scrape_run,
        get_pool_stats, get_usage_leaderboard, export_pool_json,
        update_fingerprints, get_best_proxy, get_quality_metrics,
    search_proxies, dedup_proxies, export_fresh_txt, export_rotate_txt,
    )
    POOL_AVAILABLE = True
except ImportError:
    POOL_AVAILABLE = False

# ── Static Sources ─────────────────────────────────────────────────────
PROXY_SOURCES = [
    ("hookzof-socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "host:port"),
    ("clarketm-http", "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "host:port"),
    ("roosterkid-socks5", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "host:port"),
    ("roosterkid-socks4", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt", "host:port"),
    ("roosterkid-https", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "host:port"),
    ("monosans-http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "host:port"),
    ("monosans-socks4", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "host:port"),
    ("monosans-socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "host:port"),
    ("speedx-http", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "host:port"),
    ("speedx-socks4", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "host:port"),
    ("speedx-socks5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "host:port"),
    ("jetkai-http", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "host:port"),
    ("jetkai-https", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt", "host:port"),
    ("jetkai-socks4", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "host:port"),
    ("jetkai-socks5", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "host:port"),
    ("proxifly-http", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", "host:port"),
    ("proxifly-https", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt", "host:port"),
    ("proxifly-socks4", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt", "host:port"),
    ("proxifly-socks5", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "host:port"),
    ("murongpig-http", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt", "host:port"),
    ("murongpig-socks5", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt", "host:port"),
    ("prxchk-http", "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt", "host:port"),
    ("prxchk-socks5", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt", "host:port"),
    ("proxylist-to-http", "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/http.txt", "host:port"),
    ("proxylist-to-socks5", "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks5.txt", "host:port"),
    ("goodproxy-raw", "https://raw.githubusercontent.com/yuceltoluyag/GoodProxy/main/raw.txt", "host:port"),
    ("anon-http", "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt", "host:port"),
    ("anon-https", "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt", "host:port"),
    ("anon-socks5", "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt", "host:port"),
    ("opsxcq-mixed", "https://raw.githubusercontent.com/opsxcq/proxy-list/master/list.txt", "host:port"),
    ("ahahaabas-st-http", "https://raw.githubusercontent.com/ahahaabas/proxies-st-http-socks/main/http.txt", "host:port"),
    ("geonode-http", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http", "geonode"),
    ("geonode-https", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=https", "geonode"),
    ("geonode-socks4", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks4", "geonode"),
    ("geonode-socks5", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5", "geonode"),
    ("geonode-p2-http", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=http", "geonode"),
    ("geonode-p2-https", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=https", "geonode"),
    ("geonode-p2-socks4", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks4", "geonode"),
    ("geonode-p2-socks5", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=2&sort_by=lastChecked&sort_type=desc&protocols=socks5", "geonode"),
    ("openproxylist-http", "https://openproxylist.xyz/http.txt", "host:port"),
    ("openproxylist-socks4", "https://openproxylist.xyz/socks4.txt", "host:port"),
    ("openproxylist-socks5", "https://openproxylist.xyz/socks5.txt", "host:port"),
    ("zaeem20-http", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/http.txt", "host:port"),
    ("zaeem20-https", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/https.txt", "host:port"),
    ("zaeem20-socks4", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks4.txt", "host:port"),
    ("zaeem20-socks5", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks5.txt", "host:port"),
    ("fatezero-http", "https://static.fatezero.org/tmp/proxy.txt", "host:port"),
    ("ab57-proxylist", "https://ab57.ru/downloads/proxylist.txt", "host:port"),
    ("ab57-proxyold", "https://ab57.ru/downloads/proxyold.txt", "host:port"),
    ("cybergw-http", "https://cyber-gateway.net/get-proxy/free-proxy/24-free-http-proxy", "host:port"),
    ("cybergw-socks5", "https://cyber-gateway.net/get-proxy/free-proxy/56-free-socks-proxy", "host:port"),
    ("speedx-http-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/http.txt", "host:port"),
    ("speedx-socks4-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks4.txt", "host:port"),
    ("speedx-socks5-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks5.txt", "host:port"),
    ("proxifly-all", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt", "host:port"),
    ("monosans-all", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt", "host:port"),
    ("vpslab-http", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt", "host:port"),
    ("proxyscrape-api-http", "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all", "host:port"),
    ("proxyscrape-api-socks4", "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks4&timeout=10000&country=all", "host:port"),
    ("proxyscrape-api-socks5", "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=10000&country=all", "host:port"),
    ("rawproxys-http", "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt", "host:port"),
    ("rawproxys-socks4", "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks4.txt", "host:port"),
    ("rawproxys-socks5", "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt", "host:port"),
    ("sunny9577-raw-http", "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt", "host:port"),
    ("ercindedeoglu-http", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt", "host:port"),
    ("ercindedeoglu-socks4", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt", "host:port"),
    ("ercindedeoglu-socks5", "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt", "host:port"),
    ("murongpig-socks4", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt", "host:port"),
    ("zevtyardt-http", "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt", "host:port"),
    ("zevtyardt-socks4", "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks4.txt", "host:port"),
    ("zevtyardt-socks5", "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/socks5.txt", "host:port"),
    ("prxchk-socks4", "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks4.txt", "host:port"),
    ("proxylist-to-socks4", "https://raw.githubusercontent.com/proxylist-to/proxy-list/main/socks4.txt", "host:port"),
    ("sunny9577-http", "https://sunny9577.github.io/proxy-scraper/generated/http_proxies.txt", "host:port"),
    ("proxyscrape-all-txt", "https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/all/data.txt", "protocolipport"),
    ("vpslab-socks5", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt", "host:port"),
    ("vpslab-socks4", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt", "host:port"),
    ("vpslab-all-elite", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_elite.txt", "host:port"),
    ("vpslab-all-ssl", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_ssl.txt", "host:port"),
    ("gfp-http", "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/http.txt", "host:port"),
    ("gfp-https", "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/https.txt", "host:port"),
    ("gfp-socks4", "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks4.txt", "host:port"),
    ("gfp-socks5", "https://raw.githubusercontent.com/wiki/gfpcom/free-proxy-list/lists/socks5.txt", "host:port"),
    ("proxifly-http-raw", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt", "host:port"),
    ("proxifly-https-raw", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/https/data.txt", "host:port"),
    ("proxifly-socks4-raw", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks4/data.txt", "host:port"),
    ("proxifly-socks5-raw", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt", "host:port"),
    ("hproxy-http", "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/http.txt", "host:port"),
    ("hproxy-socks5", "https://raw.githubusercontent.com/hproxy-com/free-proxy-list/main/socks5.txt", "host:port"),
    ("socks-proxy-net", "https://www.socks-proxy.net/", "table"),
    ("proxyscrape-v4-http", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&country=all&timeout=5000", "protocolipport"),
    ("proxyscrape-v4-https", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=https&country=all&timeout=5000", "protocolipport"),
    ("proxyscrape-v4-socks5", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks5&country=all&timeout=5000", "protocolipport"),
    ("proxyscrape-v4-socks4", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks4&country=all&timeout=5000", "protocolipport"),
]

# ── Credential proxy sources (ip:port:user:pass) ──────────────────────
CRED_SOURCES = [
    ("spys-cred", "https://spys.me/proxy.txt"),
]


# ── Regex ──────────────────────────────────────────────────────────────
PROXY_RE = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*[:\s]\s*(\d{1,5})")
CRED_RE = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*[:\s]\s*(\d{1,5})\s*[:\s]\s*(\S+)\s*[:\s]\s*(\S+)")
URL_CREDS_RE = re.compile(r"https?://([^:]+):([^@]+)@(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})")

# Bounds: some upstream lists publish 400k+ proxies. Keep cron safe on small VPS.
MAX_FETCH_BYTES = int(os.getenv("PROXY_SOURCE_MAX_BYTES", "2000000"))
MAX_PROXIES_PER_SOURCE = int(os.getenv("PROXY_MAX_PROXIES_PER_SOURCE", "15000"))
COMMON_LOW_PROXY_PORTS = {80, 81, 82, 83, 84, 85, 88, 443, 444, 808, 888, 999, 1000}


def is_valid_proxy_port(port: int) -> bool:
    """Return True for plausible proxy ports.

    Free lists often contain junk low ports like :1/:12/:41/:50. Those can pass a
    bare TCP connect and then waste validator time in protocol probes. Keep common
    low proxy ports and otherwise require >=1024.
    """
    return port in COMMON_LOW_PROXY_PORTS or 1024 <= port <= 65535

# ── User-Agent rotation (Rate Limit Protection) ───────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
]
_ua_idx = 0

def next_ua():
    global _ua_idx
    ua = USER_AGENTS[_ua_idx % len(USER_AGENTS)]
    _ua_idx += 1
    return ua


# ── Helpers ────────────────────────────────────────────────────────────

def fetch_direct(url, timeout=15):
    """Direct URL fetch with UA rotation and bounded read."""
    req = urllib.request.Request(url, headers={
        "User-Agent": next_ua(),
        "Accept": "text/html,application/json,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read(MAX_FETCH_BYTES + 1)
    return raw[:MAX_FETCH_BYTES].decode("utf-8", errors="ignore")


def fetch_via_relay(url, timeout=15):
    """Fetch through a private Vercel relay. Requires PROXY_RELAY_URL (+ optional PROXY_RELAY_TOKEN)."""
    relay_url = os.getenv("PROXY_RELAY_URL", "").rstrip("/")
    if not relay_url:
        return ""
    token = os.getenv("PROXY_RELAY_TOKEN", "")
    qs = urllib.parse.urlencode({"url": url, "timeout": str(timeout * 1000)})
    req = urllib.request.Request(
        f"{relay_url}/api/fetch?{qs}",
        headers={
            "Authorization": f"Bearer {token}" if token else "",
            "User-Agent": "ProxyScraper/4.1",
            "Accept": "application/json",
        },
    )
    data = json.loads(urllib.request.urlopen(req, timeout=timeout + 5).read().decode("utf-8", errors="ignore"))
    if not data.get("ok"):
        raise RuntimeError(data.get("error", "relay fetch failed"))
    body = data.get("body", "")
    if data.get("encoding") == "base64":
        body = base64.b64decode(body).decode("utf-8", errors="ignore")
    return body[:MAX_FETCH_BYTES]


def fetch(url, timeout=15, retries=3, backoff=1.5):
    """Fetch URL with retry + exponential backoff + UA rotation + optional relay fallback."""
    prefer_relay = os.getenv("PROXY_RELAY_FIRST", "").lower() in {"1", "true", "yes"}
    methods = [fetch_via_relay, fetch_direct] if prefer_relay else [fetch_direct, fetch_via_relay]
    last_err = None
    for attempt in range(retries):
        for method in methods:
            try:
                text = method(url, timeout=timeout)
                if text:
                    return text
            except Exception as e:
                last_err = e
                continue
        if attempt < retries - 1:
            time.sleep(backoff * (2 ** attempt))
    if last_err:
        print(f"  ✗ {url}: {last_err}", file=sys.stderr)
    return ""


def extract_proxies(text, fmt="", max_items=None):
    proxies = []
    limit = max_items or MAX_PROXIES_PER_SOURCE
    if fmt == "geonode" and text.strip().startswith("{"):
        try:
            data = json.loads(text)
            for item in data.get("data", []):
                ip, port = item.get("ip", ""), item.get("port", "")
                if ip and port and is_valid_proxy_port(int(port)):
                    proxies.append(f"{ip}:{port}")
                    if limit and len(proxies) >= limit:
                        break
        except Exception:
            pass
        return proxies
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "//")):
            continue
        if CRED_RE.search(line):
            continue
        m = PROXY_RE.search(line)
        if m and is_valid_proxy_port(int(m.group(2))):
            proxies.append(f"{m.group(1)}:{m.group(2)}")
            if limit and len(proxies) >= limit:
                break
    return proxies


def extract_creds(text):
    creds = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "//")):
            continue
        um = URL_CREDS_RE.search(line)
        if um and int(um.group(4)) <= 65535:
            creds.append(f"{um.group(3)}:{um.group(4)}:{um.group(1)}:{um.group(2)}")
            continue
        cm = CRED_RE.search(line)
        if cm and int(cm.group(2)) <= 65535:
            creds.append(f"{cm.group(1)}:{cm.group(2)}:{cm.group(3)}:{cm.group(4)}")
    return creds


# ── Source Discovery ───────────────────────────────────────────────────

def discover_new_urls():
    existing = {url for _, url, _ in PROXY_SOURCES}
    discovered = []
    print(f"\n🔍 Discovering from {len(DISCOVERY_SOURCES)} meta-sources...")
    for ds_url in DISCOVERY_SOURCES:
        text = fetch(ds_url, timeout=10)
        if not text:
            continue
        count = 0
        for line in text.splitlines():
            urls_found = re.findall(r'https?://[^\s\)\"\'<>\]]+', line)
            for url in urls_found:
                url = url.rstrip(".,;:")
                if url in existing:
                    continue
                if not any(kw in url.lower() for kw in ["proxy", "proxies", "txt", "json"]):
                    continue
                if not any(ext in url.lower() for ext in [".txt", ".json", "raw", "api", "data"]):
                    continue
                fmt = "geonode" if "geonode" in url.lower() or "json" in url.lower() else "host:port"
                discovered.append((f"disc-{count:03d}", url, fmt))
                existing.add(url)
                count += 1
        print(f"  → +{count} from {ds_url}")
    print(f"  → Discovered: {len(discovered)} new URLs")
    return discovered


# ── Source Health Scoring ──────────────────────────────────────────────

source_health = {}


def preferred_source_name(current, candidate):
    """Prefer a protocol-specific source name; tie-break deterministically."""
    if source_protocol_hint(candidate) and not source_protocol_hint(current):
        return candidate
    if source_protocol_hint(current) and not source_protocol_hint(candidate):
        return current
    return min(current, candidate)


def scrape_source(name, url, fmt):
    print(f"  → {name}...", end=" ", flush=True)
    t0 = time.time()
    text = fetch(url)
    elapsed = time.time() - t0
    if not text:
        source_health[name] = {"url": url, "alive": False, "proxies": 0, "time_s": round(elapsed, 2), "error": "empty"}
        print("(empty)")
        return []
    proxies = extract_proxies(text, fmt)
    capped = len(proxies) >= MAX_PROXIES_PER_SOURCE
    source_health[name] = {
        "url": url,
        "alive": len(proxies) > 0,
        "proxies": len(proxies),
        "time_s": round(elapsed, 2),
        "capped": capped,
        "max_fetch_bytes": MAX_FETCH_BYTES,
    }
    suffix = " capped" if capped else ""
    print(f"{len(proxies)}{suffix}")
    return proxies


def scrape_all(discover=False):
    """Scrape all sources. Returns (proxies_set, source_map).

    source_map: {"ip:port": source_name} — tracks which source contributed each proxy.
    """
    sources = list(PROXY_SOURCES)
    if discover:
        sources += discover_new_urls()
    all_proxies = set()
    source_map = {}  # proxy_str -> source_name
    print(f"\nScraping {len(sources)} sources...\n")
    with ThreadPoolExecutor(max_workers=25) as pool:
        futs = {pool.submit(scrape_source, n, u, f): n for n, u, f in sources}
        for fut in as_completed(futs):
            try:
                name = futs[fut]
                proxies = fut.result()
                all_proxies.update(proxies)
                for p in proxies:
                    source_map[p] = preferred_source_name(source_map[p], name) if p in source_map else name
            except Exception as e:
                print(f"  ✗ {e}", file=sys.stderr)
    return all_proxies, source_map


def scrape_creds():
    all_creds = set()
    print(f"\n🔑 Scraping {len(CRED_SOURCES)} credential sources...")
    for name, url in CRED_SOURCES:
        print(f"  → {name}...", end=" ", flush=True)
        text = fetch(url)
        if not text:
            print("(empty)")
            continue
        creds = extract_creds(text)
        all_creds.update(creds)
        print(f"{len(creds)}")
    return all_creds


# ── Validation Layer ───────────────────────────────────────────────────

VALIDATE_TCP_TIMEOUT = float(os.getenv("PROXY_VALIDATE_TCP_TIMEOUT", "3"))
VALIDATE_PROTOCOL_TIMEOUT = float(os.getenv("PROXY_VALIDATE_PROTOCOL_TIMEOUT", "2"))
VALIDATION_WALL_TIMEOUT = float(os.getenv("PROXY_VALIDATION_WALL_TIMEOUT", "180"))
CONFIRMED_PROTOCOLS = {"http", "socks4", "socks5"}


def is_confirmed_proxy(proxy_dict):
    return proxy_dict.get("protocol") in CONFIRMED_PROTOCOLS


def validate_tcp(proxy, timeout=VALIDATE_TCP_TIMEOUT):
    """Basic TCP connect test."""
    ip, port = proxy.split(":")
    try:
        s = socket.create_connection((ip, int(port)), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


def validate_http_connect(proxy, timeout=VALIDATE_PROTOCOL_TIMEOUT):
    """HTTP CONNECT test — check if proxy can relay HTTP traffic."""
    ip, port = proxy.split(":")
    try:
        s = socket.create_connection((ip, int(port)), timeout=timeout)
        req = (
            f"GET http://httpbin.org/ip HTTP/1.1\r\n"
            f"Host: httpbin.org\r\n"
            f"User-Agent: ProxyValidator/4.0\r\n"
            f"Connection: close\r\n\r\n"
        )
        s.sendall(req.encode())
        data = s.recv(4096)
        s.close()
        # Check for 200 OK response
        return b"200 OK" in data or b"HTTP/1" in data
    except Exception:
        return False


def validate_socks4(proxy, timeout=VALIDATE_PROTOCOL_TIMEOUT):
    """SOCKS4 CONNECT handshake test."""
    ip, port = proxy.split(":")
    try:
        with socket.create_connection((ip, int(port)), timeout=timeout) as s:
            s.settimeout(timeout)
            s.sendall(b"\x04\x01\x00\x50\x01\x01\x01\x01\x00")
            response = b""
            while len(response) < 8:
                chunk = s.recv(8 - len(response))
                if not chunk:
                    return False
                response += chunk
        return response[:2] == b"\x00\x5a"
    except (OSError, ValueError):
        return False


def validate_socks5(proxy, timeout=VALIDATE_PROTOCOL_TIMEOUT):
    """SOCKS5 handshake test."""
    ip, port = proxy.split(":")
    try:
        s = socket.create_connection((ip, int(port)), timeout=timeout)
        # SOCKS5 greeting: version 5, 1 auth method (no auth)
        s.sendall(b"\x05\x01\x00")
        resp = s.recv(2)
        s.close()
        # Response should be: version 5, method 0 (no auth)
        return len(resp) == 2 and resp[0] == 0x05 and resp[1] == 0x00
    except Exception:
        return False


def detect_anonymity(proxy, timeout=VALIDATE_PROTOCOL_TIMEOUT):
    """Detect proxy anonymity level via X-Forwarded-For detection."""
    ip, port = proxy.split(":")
    try:
        s = socket.create_connection((ip, int(port)), timeout=timeout)
        req = (
            f"GET http://httpbin.org/headers HTTP/1.1\r\n"
            f"Host: httpbin.org\r\n"
            f"X-Forwarded-For: 1.2.3.4\r\n"
            f"User-Agent: ProxyValidator/4.0\r\n"
            f"Connection: close\r\n\r\n"
        )
        s.sendall(req.encode())
        data = s.recv(8192).decode("utf-8", errors="ignore")
        s.close()
        if "1.2.3.4" in data:
            return "transparent"  # Forwarded IP visible
        return "elite"  # Forwarded IP hidden
    except Exception:
        return "unknown"


def source_protocol_hint(source_name):
    """Return a protocol hint from curated source names, or None for mixed feeds."""
    if source_name.endswith("-https"):
        return "http"
    for protocol in ("socks5", "socks4", "http"):
        if source_name.endswith(f"-{protocol}"):
            return protocol
    return None


def validate_single(proxy, do_anonymity=False, protocol_hint=None):
    """Full validation: TCP + protocol detection + response time + anonymity."""
    ip, port = proxy.split(":")
    if not is_valid_proxy_port(int(port)):
        return None
    t0 = time.time()
    if not validate_tcp(proxy):
        return None
    response_time_ms = round((time.time() - t0) * 1000)

    # Probe the source-declared protocol first, but still fall back to all protocols.
    checks = {
        "socks5": validate_socks5,
        "socks4": validate_socks4,
        "http": validate_http_connect,
    }
    order = [protocol_hint] if protocol_hint in checks else []
    order += [name for name in checks if name not in order]
    protocol = next((name for name in order if checks[name](proxy)), "unknown")

    anonymity = "unknown"
    if do_anonymity and protocol == "http":
        anonymity = detect_anonymity(proxy)

    return {
        "ip": ip,
        "port": int(port),
        "protocol": protocol,
        "response_time_ms": response_time_ms,
        "anonymity": anonymity,
        "last_seen": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ── Geolocation (batch) ───────────────────────────────────────────────

def geo_batch_lookup(ips, batch_size=100, timeout=10):
    """Batch geolocation via ip-api.com. Returns {ip: {country, country_code, city, isp}}.

    ip-api only returns the fields explicitly requested. Keep ``status`` and ``query``
    in the request because the parser needs them to map successful responses back to
    proxy IPs. Without those fields every lookup is silently discarded.
    """
    geo = {}
    ip_list = sorted(set(ip for ip in ips if ip))[:2000]  # cap at 2000
    fields = "status,message,query,country,countryCode,city,isp"
    for i in range(0, len(ip_list), batch_size):
        batch = ip_list[i:i+batch_size]
        try:
            payload = json.dumps([{"query": ip, "fields": fields} for ip in batch]).encode()
            req = urllib.request.Request(
                "http://ip-api.com/batch",
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "ProxyScraper/5.0"},
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=timeout)
            results = json.loads(resp.read().decode())
            if isinstance(results, dict):
                results = [results]
            for r in results:
                query = r.get("query")
                if r.get("status") == "success" and query:
                    geo[query] = {
                        "country": r.get("country", ""),
                        "country_code": (r.get("countryCode", "") or "").upper(),
                        "city": r.get("city", ""),
                        "isp": r.get("isp", ""),
                    }
        except Exception as e:
            print(f"  ⚠ geo batch {i//batch_size+1} failed: {e}", file=sys.stderr)
        time.sleep(1.2)  # ip-api free tier: 45 req/min → ~1.3s per batch
    return geo


# ── Proxy Scoring ─────────────────────────────────────────────────────

def compute_score(proxy_dict):
    """Composite score 0-100 based on response time, anonymity, protocol."""
    # Speed score (40% weight)
    rt = proxy_dict.get("response_time_ms", 9999)
    if rt < 500: speed = 10
    elif rt < 1000: speed = 8
    elif rt < 2000: speed = 6
    elif rt < 5000: speed = 4
    else: speed = 2

    # Anonymity score (30% weight)
    anon = proxy_dict.get("anonymity", "unknown")
    if anon == "elite": anonymity = 10
    elif anon == "transparent": anonymity = 4
    else: anonymity = 3

    # Protocol score (30% weight)
    proto = proxy_dict.get("protocol", "unknown")
    if proto == "socks5": protocol = 10
    elif proto == "http": protocol = 8
    else: protocol = 3

    score = round(speed * 4 + anonymity * 3 + protocol * 3)  # 0-100
    return score


def choose_validation_sample(proxies, max_validate=500, source_map=None):
    """Choose a deterministic, source-balanced validation sample.

    A plain list(set(...)) biases validation randomly and can starve smaller sources once
    huge feeds are added. Round-robin by source keeps health meaningful.
    """
    proxies = sorted(proxies)
    if not source_map or max_validate >= len(proxies):
        return proxies[:max_validate]

    buckets = {}
    for proxy in proxies:
        buckets.setdefault(source_map.get(proxy, "unknown") or "unknown", []).append(proxy)

    selected = []
    seen = set()
    source_names = sorted(buckets, key=lambda n: (-len(buckets[n]), n))
    while len(selected) < max_validate and source_names:
        next_sources = []
        for name in source_names:
            bucket = buckets[name]
            if not bucket:
                continue
            proxy = bucket.pop(0)
            if proxy not in seen:
                selected.append(proxy)
                seen.add(proxy)
                if len(selected) >= max_validate:
                    break
            if bucket:
                next_sources.append(name)
        source_names = next_sources
    return selected


def filter_valid(proxies, max_validate=500, do_anonymity=False, source_map=None):
    """Validate proxies in parallel, return list of valid proxy dicts.

    If source_map is provided, each valid proxy gets a 'source_name' field.
    """
    to_test = choose_validation_sample(proxies, max_validate, source_map=source_map)
    print(f"\n🔍 Validating {len(to_test)} proxies (source-balanced)...\n")
    valid = []
    timed_out = False
    pool = ThreadPoolExecutor(max_workers=200)
    futs = {
        pool.submit(
            validate_single,
            proxy,
            do_anonymity,
            source_protocol_hint((source_map or {}).get(proxy, "")),
        ): proxy
        for proxy in to_test
    }
    try:
        for fut in as_completed(futs, timeout=VALIDATION_WALL_TIMEOUT):
            result = fut.result()
            if result and is_confirmed_proxy(result):
                valid.append(result)
                print(f"  ✅ {result['ip']}:{result['port']} [{result['protocol']}] {result['response_time_ms']}ms {result['anonymity']}")
    except TimeoutError:
        timed_out = True
        pending = sum(1 for fut in futs if not fut.done())
        for fut in futs:
            if not fut.done():
                fut.cancel()
        print(f"  ⚠ validation deadline hit after {VALIDATION_WALL_TIMEOUT:.0f}s; cancelled {pending} pending checks", file=sys.stderr)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    if timed_out:
        print(f"  ⚠ proceeding with {len(valid)} completed valid proxies", file=sys.stderr)
    print(f"\n📊 {len(valid)}/{len(to_test)} alive")

    # Geolocation
    print(f"\n🌍 Looking up geolocation for {len(valid)} proxies...")
    ips = [p["ip"] for p in valid]
    geo = geo_batch_lookup(ips)
    for p in valid:
        g = geo.get(p["ip"], {})
        p["country"] = g.get("country", "")
        p["country_code"] = g.get("country_code", "")
        p["city"] = g.get("city", "")
        p["isp"] = g.get("isp", "")

    # Attach source attribution
    if source_map:
        for p in valid:
            key = f"{p['ip']}:{p['port']}"
            p["source_name"] = source_map.get(key, "")

    # Scoring
    for p in valid:
        p["score"] = compute_score(p)

    # Sort by score descending
    valid.sort(key=lambda x: x.get("score", 0), reverse=True)
    return valid


# ── JSON Output ────────────────────────────────────────────────────────

def save_json_output(valid_proxies, filename="proxies.json"):
    """Save validated proxies as JSON with metadata."""
    with open(filename, "w") as f:
        json.dump(valid_proxies, f, indent=2)
    print(f"✅ JSON output → {filename} ({len(valid_proxies)} proxies)")


def save_grouped_output(valid_proxies):
    """Save grouped outputs: by country, by protocol."""
    # By country
    by_country = {}
    for p in valid_proxies:
        cc = p.get("country_code", "XX") or "XX"
        by_country.setdefault(cc, []).append(p)
    by_country = {k: sorted(v, key=lambda x: x.get("score", 0), reverse=True) for k, v in sorted(by_country.items())}
    with open("proxies-by-country.json", "w") as f:
        json.dump(by_country, f, indent=2)
    print(f"✅ By country → proxies-by-country.json ({len(by_country)} countries)")

    # By protocol
    by_protocol = {}
    for p in valid_proxies:
        proto = p.get("protocol", "unknown")
        by_protocol.setdefault(proto, []).append(p)
    by_protocol = {k: sorted(v, key=lambda x: x.get("score", 0), reverse=True) for k, v in sorted(by_protocol.items())}
    with open("proxies-by-protocol.json", "w") as f:
        json.dump(by_protocol, f, indent=2)
    print(f"✅ By protocol → proxies-by-protocol.json ({', '.join(f'{k}:{len(v)}' for k,v in by_protocol.items())})")

    # Stats summary
    stats = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total": len(valid_proxies),
        "by_country": {k: len(v) for k, v in by_country.items()},
        "by_protocol": {k: len(v) for k, v in by_protocol.items()},
        "by_anonymity": {},
        "score_distribution": {},
        "avg_response_time_ms": round(sum(p.get("response_time_ms", 0) for p in valid_proxies) / max(len(valid_proxies), 1)),
    }
    for p in valid_proxies:
        a = p.get("anonymity", "unknown")
        stats["by_anonymity"][a] = stats["by_anonymity"].get(a, 0) + 1
        s = str(p.get("score", 0) // 10 * 10)  # bucket by 10
        stats["score_distribution"][f"{s}-{int(s)+9}"] = stats["score_distribution"].get(f"{s}-{int(s)+9}", 0) + 1
    with open("proxies-stats.json", "w") as f:
        json.dump(stats, f, indent=2)
    print(f"✅ Stats → proxies-stats.json")


def save_health_report(filename="source-health.json"):
    """Save source health scoring report."""
    # Sort by proxies count descending
    sorted_health = dict(sorted(
        source_health.items(),
        key=lambda x: x[1].get("proxies", 0),
        reverse=True
    ))
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_sources": len(sorted_health),
        "alive_sources": sum(1 for v in sorted_health.values() if v.get("alive")),
        "dead_sources": sum(1 for v in sorted_health.values() if not v.get("alive")),
        "total_proxies": sum(v.get("proxies", 0) for v in sorted_health.values()),
        "sources": sorted_health,
    }
    with open(filename, "w") as f:
        json.dump(report, f, indent=2)
    print(f"✅ Health report → {filename}")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Free Proxy Scraper v4")
    ap.add_argument("-o", "--output", default="proxies.txt")
    ap.add_argument("--cred-output", default="proxies-cred.txt")
    ap.add_argument("--discover", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--validate-full", action="store_true", help="Include anonymity detection (slower)")
    ap.add_argument("--max-validate", type=int, default=2000)
    ap.add_argument("--json", action="store_true", help="Output proxies.json with metadata")
    ap.add_argument("--grouped", action="store_true", help="Output by-country, by-protocol, stats JSON")
    ap.add_argument("--pool", action="store_true", help="Update proxy pool database")
    ap.add_argument("--health", action="store_true", help="Output source-health.json")
    ap.add_argument("--relay-url", help="Private Vercel relay base URL (sets PROXY_RELAY_URL)")
    ap.add_argument("--relay-token", help="Private Vercel relay token (sets PROXY_RELAY_TOKEN)")
    ap.add_argument("--relay-first", action="store_true", help="Try relay before direct fetch")
    args = ap.parse_args()

    if args.relay_url:
        os.environ["PROXY_RELAY_URL"] = args.relay_url
    if args.relay_token:
        os.environ["PROXY_RELAY_TOKEN"] = args.relay_token
    if args.relay_first:
        os.environ["PROXY_RELAY_FIRST"] = "1"

    t0 = time.time()
    proxies, source_map = scrape_all(discover=args.discover)
    raw = proxies  # keep raw count for logging
    if not proxies:
        print("\n✗ No proxies!", file=sys.stderr)
        sys.exit(1)
    print(f"\n📊 {len(proxies)} unique proxies ({len(set(source_map.values()))} sources)")

    # Save health report
    if args.health:
        save_health_report()

    if args.validate:
        valid = filter_valid(proxies, args.max_validate, do_anonymity=args.validate_full, source_map=source_map)
        if not valid:
            print("\n✗ No confirmed proxies; preserving existing outputs.", file=sys.stderr)
            sys.exit(1)
        if args.json and valid:
            save_json_output(valid)
        if args.grouped and valid:
            save_grouped_output(valid)
        if valid and POOL_AVAILABLE:
            print(f"\n📦 Updating proxy pool...")
            for p in valid:
                upsert_proxy(p, source=p.get("source_name", ""))
            update_fingerprints()
            print(f"✅ Pool updated with {len(valid)} proxies")
        if POOL_AVAILABLE:
            for name, health in source_health.items():
                log_source_health(name, health.get("alive", False), health.get("proxies", 0))
            log_scrape_run(
                len(raw), len(valid) if valid else 0,
                len(source_health),
                sum(1 for v in source_health.values() if v.get("alive")),
                time.time() - t0
            )
            if valid and len(valid) < 50:
                print(f"\n⚠️  ALERT: Only {len(valid)} alive proxies (threshold: 50)")
        if valid:
            proxies = set(f"{v['ip']}:{v['port']}" for v in valid)

    with open(args.output, "w") as f:
        f.write("\n".join(sorted(proxies)) + "\n")

    creds = scrape_creds()
    if creds:
        with open(args.cred_output, "w") as f:
            f.write("\n".join(sorted(creds)) + "\n")
        print(f"✅ {len(creds)} creds → {args.cred_output}")

    print(f"✅ {len(proxies)} proxies → {args.output} ({time.time()-t0:.1f}s)")


if __name__ == "__main__":
    main()
