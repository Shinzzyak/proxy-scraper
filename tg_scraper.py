#!/usr/bin/env python3
"""
tg_scraper.py — Telegram Channel Proxy Scraper

Scrapes proxy lists from Telegram channels using the public web preview (t.me/s/).
No Telegram account or API key required.

Usage:
    python3 tg_scraper.py                     # Scrape all configured channels
    python3 tg_scraper.py --channels free_proxy_list proxy_lists  # Specific channels
    python3 tg_scraper.py --add-channel mychannel  # Add a channel
    python3 tg_scraper.py --list-channels       # List known channels
    python3 tg_scraper.py --pages 5             # Scrape 5 pages of history (default: 3)
    python3 tg_scraper.py --test                # Test mode: show what would be scraped
"""
import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

# ── Config ──────────────────────────────────────────────────────────────

CONFIG_FILE = Path(__file__).parent / "data" / "tg_channels.json"

# Known channels that post proxy lists (verified working or worth trying)
KNOWN_CHANNELS = {
    # Verified/high-signal public channels from web preview/search
    "proxyscrape": {
        "description": "ProxyScrape Telegram updates; links/files for HTTP/SOCKS proxies",
        "protocols": ["http", "socks4", "socks5"],
        "priority": "high",
    },
    "whale_proxy": {
        "description": "Whale Proxy; posts HTTP/HTTPS/SOCKS files",
        "protocols": ["http", "socks4", "socks5"],
        "priority": "high",
    },
    "TGunblock": {
        "description": "Telegram unblock SOCKS5 proxy posts",
        "protocols": ["socks5"],
        "priority": "high",
    },
    # High-activity channels
    "free_proxy_list": {
        "description": "Daily proxy list updates (HTTP/SOCKS5)",
        "protocols": ["http", "socks5"],
        "priority": "high",
    },
    "proxy_lists": {
        "description": "Stats-based, low yield",
        "protocols": ["http"],
        "priority": "low",
    },
    "socks5_proxy": {
        "description": "SOCKS5-focused channel",
        "protocols": ["socks5"],
        "priority": "medium",
    },
    "free_proxy_socks5": {
        "description": "Free SOCKS5 proxies",
        "protocols": ["socks5"],
        "priority": "medium",
    },
    "proxylist2024": {
        "description": "Updated proxy lists",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "proxyspy": {
        "description": "Proxy intelligence",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "free_proxy_http": {
        "description": "HTTP proxy focused",
        "protocols": ["http"],
        "priority": "medium",
    },
    "proxy_list_http": {
        "description": "HTTP proxy list",
        "protocols": ["http"],
        "priority": "medium",
    },
    "freshproxylist": {
        "description": "Fresh proxy lists",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "proxylistfree": {
        "description": "Free proxy lists",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "proxy_pool": {
        "description": "Proxy pool updates",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "fast_proxies": {
        "description": "Fast proxy lists",
        "protocols": ["http"],
        "priority": "medium",
    },
    "socks_list": {
        "description": "SOCKS proxy lists",
        "protocols": ["socks5", "socks4"],
        "priority": "medium",
    },
    "proxylistdaily": {
        "description": "Daily proxy list",
        "protocols": ["http"],
        "priority": "medium",
    },
    "free_socks5_proxy": {
        "description": "Free SOCKS5 proxies",
        "protocols": ["socks5"],
        "priority": "medium",
    },
    # Additional channels to try
    "free_proxy_socks": {
        "description": "Free SOCKS proxies",
        "protocols": ["socks5", "socks4"],
        "priority": "medium",
    },
    "proxy_list_2025": {
        "description": "2025 proxy list",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
    "proxylistmix": {
        "description": "Mixed proxy lists",
        "protocols": ["http", "socks5"],
        "priority": "medium",
    },
}

# Proxy patterns
IP_PORT_RE = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})')
# Extended pattern: protocol://ip:port
PROTO_RE = re.compile(r'(https?|socks[45])://(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})')
# Port range filter (valid proxy ports)
MIN_PORT = 1024
MAX_PORT = 65535


def load_config() -> dict:
    """Load channel config, create if not exists, merge new defaults."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            config = json.load(f)
    else:
        config = {"channels": {}, "last_run": None, "total_proxies": 0}

    config.setdefault("channels", {})
    for name, info in KNOWN_CHANNELS.items():
        config["channels"].setdefault(name, {
            "enabled": True,
            "priority": info["priority"],
            "protocols": info["protocols"],
            "description": info["description"],
        })
    save_config(config)
    return config


def save_config(config: dict):
    """Save channel config."""
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


# ── Telegram Public Preview Scraper ─────────────────────────────────────

def fetch_tg_page(channel: str, before: int = None) -> str | None:
    """Fetch a page of Telegram channel messages via public preview."""
    url = f"https://t.me/s/{channel}"
    if before:
        url += f"?before={before}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        })
        resp = urllib.request.urlopen(req, timeout=15)
        return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"  ⚠ fetch failed for @{channel}: {e}", file=sys.stderr)
        return None


def extract_attachments_from_html(html: str, channel: str) -> list:
    """Extract Telegram document attachment metadata.

    Public t.me/s pages expose file names and message links, but not always direct CDN
    download URLs. Store metadata so a future Telegram API downloader can fetch them.
    """
    if not BS4_AVAILABLE:
        return []
    soup = BeautifulSoup(html, "html.parser")
    attachments = []
    for a in soup.find_all("a", class_=lambda c: c and "tgme_widget_message_document" in c):
        text = a.get_text(" ", strip=True)
        href = a.get("href", "")
        if not text and not href:
            continue
        attachments.append({
            "channel": channel,
            "title": text,
            "url": href,
            "looks_like_proxy_file": any(k in text.lower() for k in ["proxy", "http", "https", "socks", ".txt"]),
        })
    return attachments


def extract_proxies_from_html(html: str) -> set:
    """Extract proxy IP:port pairs from Telegram HTML."""
    if not BS4_AVAILABLE:
        # Fallback: regex only
        return set(f"{ip}:{port}" for ip, port in IP_PORT_RE.findall(html)
                   if MIN_PORT <= int(port) <= MAX_PORT)

    soup = BeautifulSoup(html, "html.parser")
    proxies = set()

    # Method 1: Message text content
    messages = soup.find_all("div", class_="tgme_widget_message_text")
    for msg in messages:
        text = msg.get_text(separator="\n")
        for ip, port in IP_PORT_RE.findall(text):
            if MIN_PORT <= int(port) <= MAX_PORT:
                proxies.add(f"{ip}:{port}")
        # Also check for protocol://ip:port format
        for proto, ip, port in PROTO_RE.findall(text):
            if MIN_PORT <= int(port) <= MAX_PORT:
                proxies.add(f"{ip}:{port}")

    # Method 2: Code blocks (many channels use <code> for proxy lists)
    for code in soup.find_all("code"):
        text = code.get_text()
        for ip, port in IP_PORT_RE.findall(text):
            if MIN_PORT <= int(port) <= MAX_PORT:
                proxies.add(f"{ip}:{port}")

    # Method 3: Pre blocks
    for pre in soup.find_all("pre"):
        text = pre.get_text()
        for ip, port in IP_PORT_RE.findall(text):
            if MIN_PORT <= int(port) <= MAX_PORT:
                proxies.add(f"{ip}:{port}")

    return proxies


def scrape_channel(channel: str, pages: int = 3) -> tuple[set, list]:
    """Scrape multiple pages from a Telegram channel. Returns (proxies, attachments)."""
    all_proxies = set()
    attachments = []

    for page in range(1, pages + 1):
        before = page * 100 if page > 1 else None
        html = fetch_tg_page(channel, before)
        if not html:
            break

        proxies = extract_proxies_from_html(html)
        new_proxies = proxies - all_proxies
        all_proxies.update(new_proxies)
        attachments.extend(extract_attachments_from_html(html, channel))

        if not new_proxies and page > 1:
            # Still fetched one historical page; enough for low-yield channels.
            break

        time.sleep(0.5)  # Rate limit

    # Deduplicate attachments by URL/title.
    seen = set()
    unique_attachments = []
    for item in attachments:
        key = (item.get("url", ""), item.get("title", ""))
        if key not in seen:
            seen.add(key)
            unique_attachments.append(item)

    return all_proxies, unique_attachments


def scrape_all_channels(channels: list = None, pages: int = 3) -> dict:
    """Scrape proxies from multiple channels. Returns {channel: proxies_set}."""
    config = load_config()
    if channels is None:
        channels = [name for name, info in config["channels"].items() if info.get("enabled", True)]

    results = {}
    attachment_results = {}
    total = 0
    total_attachments = 0

    print(f"\n📱 Scraping {len(channels)} Telegram channels ({pages} pages each)...\n")

    def _scrape_one(ch):
        proxies, attachments = scrape_channel(ch, pages)
        return ch, proxies, attachments

    with ThreadPoolExecutor(max_workers=5) as pool:
        futs = {pool.submit(_scrape_one, ch): ch for ch in channels}
        for fut in as_completed(futs):
            ch = futs[fut]
            try:
                name, proxies, attachments = fut.result()
                results[name] = proxies
                attachment_results[name] = attachments
                count = len(proxies)
                attach_count = len([a for a in attachments if a.get("looks_like_proxy_file")])
                total += count
                total_attachments += attach_count
                status = "✅" if count > 0 else ("📎" if attach_count > 0 else "⚠️")
                print(f"  {status} @{name}: {count} proxies, {attach_count} proxy-like attachments")
            except Exception as e:
                print(f"  ❌ @{ch}: {e}")
                results[ch] = set()
                attachment_results[ch] = []

    attachment_file = CONFIG_FILE.parent / "tg_attachments.json"
    with open(attachment_file, "w") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "channels": attachment_results,
        }, f, indent=2)

    print(f"\n📊 Total: {total} unique proxies, {total_attachments} proxy-like attachments from {len(channels)} channels")
    print(f"📎 Attachment metadata → {attachment_file}")
    return results


# ── Integration with proxy_pool ─────────────────────────────────────────

def add_to_pool(results: dict, db_path: str = None):
    """Add scraped proxies to the pool database."""
    sys.path.insert(0, str(Path(__file__).parent))
    from proxy_pool import get_db

    conn = get_db()
    added = 0
    all_proxies = set()

    for channel, proxies in results.items():
        for p in proxies:
            all_proxies.add(p)

    if not all_proxies:
        print("\n⚠️ No proxies to add")
        return 0

    # Validate TCP
    print(f"\n🔍 Validating {len(all_proxies)} Telegram proxies...")
    import socket
    valid = []
    for p in all_proxies:
        ip, port = p.split(":")
        try:
            s = socket.create_connection((ip, int(port)), timeout=5)
            s.close()
            valid.append(p)
        except:
            pass
    print(f"  ✅ {len(valid)}/{len(all_proxies)} alive")

    if not valid:
        return 0

    # Geo lookup
    print(f"🌍 Geo lookup for {len(valid)} proxies...")
    ips = [p.split(":")[0] for p in valid]
    geo = {}

    # ip-api batch
    ip_list = sorted(set(ips))[:2000]
    fields = "status,message,query,country,countryCode,city,isp"
    for i in range(0, len(ip_list), 100):
        batch = ip_list[i:i+100]
        try:
            payload = json.dumps([{"query": ip, "fields": fields} for ip in batch]).encode()
            req = urllib.request.Request(
                "http://ip-api.com/batch",
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "ProxyScraper/5.0"},
                method="POST",
            )
            resp = urllib.request.urlopen(req, timeout=10)
            results_geo = json.loads(resp.read().decode())
            if isinstance(results_geo, dict):
                results_geo = [results_geo]
            for r in results_geo:
                query = r.get("query")
                if r.get("status") == "success" and query:
                    geo[query] = {
                        "country": r.get("country", ""),
                        "country_code": (r.get("countryCode", "") or "").upper(),
                        "city": r.get("city", ""),
                        "isp": r.get("isp", ""),
                    }
        except Exception as e:
            print(f"  ⚠ geo batch failed: {e}", file=sys.stderr)
        time.sleep(1.2)

    # Upsert to pool
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for p in valid:
        ip, port = p.split(":")
        g = geo.get(ip, {})
        source = "telegram"
        try:
            conn.execute("""
                INSERT INTO proxies (ip, port, protocol, score, country, country_code, city, isp,
                                     last_seen, first_seen, source_name)
                VALUES (?, ?, 'http', 50, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (ip, port) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    score = MAX(proxies.score, 50),
                    country = COALESCE(NULLIF(excluded.country, ''), proxies.country),
                    country_code = COALESCE(NULLIF(excluded.country_code, ''), proxies.country_code),
                    city = COALESCE(NULLIF(excluded.city, ''), proxies.city),
                    isp = COALESCE(NULLIF(excluded.isp, ''), proxies.isp),
                    source_name = COALESCE(NULLIF(excluded.source_name, ''), proxies.source_name)
            """, (
                ip, int(port),
                g.get("country", ""), g.get("country_code", ""),
                g.get("city", ""), g.get("isp", ""),
                now, now, source,
            ))
            added += 1
        except Exception as e:
            print(f"  ⚠ DB error for {ip}:{port}: {e}", file=sys.stderr)

    conn.commit()
    conn.close()
    print(f"\n✅ Added {added} Telegram proxies to pool")
    return added


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Telegram Channel Proxy Scraper")
    ap.add_argument("--channels", nargs="*", help="Specific channels to scrape")
    ap.add_argument("--pages", type=int, default=3, help="Pages to scrape per channel (default: 3)")
    ap.add_argument("--add-to-pool", action="store_true", help="Add validated proxies to pool DB")
    ap.add_argument("--add-channel", help="Add a new channel to config")
    ap.add_argument("--remove-channel", help="Remove a channel from config")
    ap.add_argument("--list-channels", action="store_true", help="List all configured channels")
    ap.add_argument("--list-active", action="store_true", help="List only enabled channels")
    ap.add_argument("--test", action="store_true", help="Test mode: scrape but don't add to pool")
    ap.add_argument("--discover", action="store_true", help="Discover new channels via search")
    args = ap.parse_args()

    config = load_config()

    if args.list_channels:
        print("\n📱 Telegram Proxy Channels:\n")
        for name, info in sorted(config["channels"].items()):
            status = "✅" if info.get("enabled") else "❌"
            print(f"  {status} @{name} [{info.get('priority', '?')}] — {info.get('description', 'N/A')}")
            print(f"     Protocols: {', '.join(info.get('protocols', []))}")
        print(f"\nTotal: {len(config['channels'])} channels")
        return

    if args.list_active:
        active = [n for n, i in config["channels"].items() if i.get("enabled")]
        print(f"Active channels ({len(active)}): {', '.join(active)}")
        return

    if args.add_channel:
        name = args.add_channel.lstrip("@")
        if name not in config["channels"]:
            config["channels"][name] = {
                "enabled": True,
                "priority": "medium",
                "protocols": ["http", "socks5"],
                "description": "User-added channel",
            }
            save_config(config)
            print(f"✅ Added @{name} to config")
        else:
            print(f"⚠️ @{name} already in config")
        return

    if args.remove_channel:
        name = args.remove_channel.lstrip("@")
        if name in config["channels"]:
            del config["channels"][name]
            save_config(config)
            print(f"✅ Removed @{name}")
        else:
            print(f"⚠️ @{name} not found")
        return

    if args.discover:
        print("\n🔍 Discovering new Telegram proxy channels...")
        print("  (This searches for channels mentioning proxy lists)")
        # Use t.me search — limited without auth
        print("  Note: Full discovery requires Telegram API auth.")
        print("  Use --add-channel <name> to add manually.")
        return

    # Scrape
    channels = args.channels
    if channels:
        channels = [c.lstrip("@") for c in channels]

    results = scrape_all_channels(channels, pages=args.pages)

    # Save raw output
    all_proxies = set()
    for proxies in results.values():
        all_proxies.update(proxies)

    output_file = Path(__file__).parent / "data" / "tg_proxies.txt"
    with open(output_file, "w") as f:
        f.write("\n".join(sorted(all_proxies)) + "\n")
    print(f"\n📄 Raw output → {output_file} ({len(all_proxies)} proxies)")

    if args.add_to_pool and all_proxies:
        add_to_pool(results)

    # Update config last_run
    config["last_run"] = datetime.now(timezone.utc).isoformat()
    config["total_proxies"] = len(all_proxies)
    save_config(config)


if __name__ == "__main__":
    main()
