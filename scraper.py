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
    # ── ORGANIC COLLECTION (30 repos) ──
    # Tiievii
    ("tiievii-http", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/http.txt", "host:port"),
    ("tiievii-socks4", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/socks4.txt", "host:port"),
    ("tiievii-socks5", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/socks5.txt", "host:port"),
    # prxylist
    ("prxylist-http", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/http.txt", "host:port"),
    ("prxylist-socks4", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/socks4.txt", "host:port"),
    ("prxylist-socks5", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/socks5.txt", "host:port"),
    # ShiftyTrash
    ("shiftytrash-http", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/http.txt", "host:port"),
    ("shiftytrash-socks4", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/socks4.txt", "host:port"),
    ("shiftytrash-socks5", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/socks5.txt", "host:port"),
    # ImKK666
    ("imkk-http", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("imkk-socks4", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("imkk-socks5", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    # mertguvencli
    ("mertguvencli-http", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/http.txt", "host:port"),
    ("mertguvencli-socks4", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/socks4.txt", "host:port"),
    ("mertguvencli-socks5", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/socks5.txt", "host:port"),
    # Kalemulti
    ("kalemulti-http", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/http.txt", "host:port"),
    ("kalemulti-socks4", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/socks4.txt", "host:port"),
    ("kalemulti-socks5", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/socks5.txt", "host:port"),
    # TheBootstrapTutorial
    ("bootstrap-http", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/http.txt", "host:port"),
    ("bootstrap-socks4", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/socks4.txt", "host:port"),
    ("bootstrap-socks5", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/socks5.txt", "host:port"),
    # mksmg
    ("mksmg-http", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/http.txt", "host:port"),
    ("mksmg-socks4", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/socks4.txt", "host:port"),
    ("mksmg-socks5", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/socks5.txt", "host:port"),
    # uzayatsiz
    ("uzayatsiz-http", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/http.txt", "host:port"),
    ("uzayatsiz-socks4", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/socks4.txt", "host:port"),
    ("uzayatsiz-socks5", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/socks5.txt", "host:port"),
    # vvmd
    ("vvmd-http", "https://raw.githubusercontent.com/vvmd/ProxyList/main/http.txt", "host:port"),
    ("vvmd-socks4", "https://raw.githubusercontent.com/vvmd/ProxyList/main/socks4.txt", "host:port"),
    ("vvmd-socks5", "https://raw.githubusercontent.com/vvmd/ProxyList/main/socks5.txt", "host:port"),
    # Defacto2
    ("defacto2-http", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/http.txt", "host:port"),
    ("defacto2-socks4", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/socks4.txt", "host:port"),
    ("defacto2-socks5", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/socks5.txt", "host:port"),
    # Jacobbin
    ("jacobbin-http", "https://raw.githubusercontent.com/Jacobbin/Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("jacobbin-socks4", "https://raw.githubusercontent.com/Jacobbin/Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("jacobbin-socks5", "https://raw.githubusercontent.com/Jacobbin/Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    # Mahdi-Zarei
    ("mahdizarei-http", "https://raw.githubusercontent.com/Mahdi-Zarei/Free-Proxies/refs/heads/main/http.txt", "host:port"),
    ("mahdizarei-socks4", "https://raw.githubusercontent.com/Mahdi-Zarei/Free-Proxies/refs/heads/main/socks4.txt", "host:port"),
    ("mahdizarei-socks5", "https://raw.githubusercontent.com/Mahdi-Zarei/Free-Proxies/refs/heads/main/socks5.txt", "host:port"),
    # UptimerBot
    ("uptimer-http", "https://raw.githubusercontent.com/UptimerBot/proxy-list/refs/heads/main/http.txt", "host:port"),
    ("uptimer-socks4", "https://raw.githubusercontent.com/UptimerBot/proxy-list/refs/heads/main/socks4.txt", "host:port"),
    ("uptimer-socks5", "https://raw.githubusercontent.com/UptimerBot/proxy-list/refs/heads/main/socks5.txt", "host:port"),
    # ByteBlitz
    ("byteblitz-http", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("byteblitz-socks4", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("byteblitz-socks5", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    # mermald
    ("mermald-http", "https://raw.githubusercontent.com/mermald/proxy/refs/heads/main/http.txt", "host:port"),
    ("mermald-socks4", "https://raw.githubusercontent.com/mermald/proxy/refs/heads/main/socks4.txt", "host:port"),
    ("mermald-socks5", "https://raw.githubusercontent.com/mermald/proxy/refs/heads/main/socks5.txt", "host:port"),
    # Anongrp
    ("anongrp-http", "https://raw.githubusercontent.com/Anongrp/Free-Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("anongrp-socks4", "https://raw.githubusercontent.com/Anongrp/Free-Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("anongrp-socks5", "https://raw.githubusercontent.com/Anongrp/Free-Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    # sgbviper
    ("sgbviper-http", "https://raw.githubusercontent.com/sgbviper/Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("sgbviper-socks4", "https://raw.githubusercontent.com/sgbviper/Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("sgbviper-socks5", "https://raw.githubusercontent.com/sgbviper/Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    # vlfedr
    ("vlfedr-http", "https://raw.githubusercontent.com/vlfedr/proxy-list/main/http.txt", "host:port"),
    ("vlfedr-socks5", "https://raw.githubusercontent.com/vlfedr/proxy-list/main/socks5.txt", "host:port"),
    # ── CLASSIC HEAVY HITTERS ──
    # Monosans
    ("monosans-http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "host:port"),
    ("monosans-socks4", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "host:port"),
    ("monosans-socks5", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "host:port"),
    # mmpx12
    ("mmpx12-http", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "host:port"),
    ("mmpx12-https", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt", "host:port"),
    ("mmpx12-socks5", "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "host:port"),
    # hookzof
    ("hookzof-socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "host:port"),
    # roosterkid
    ("roosterkid-http", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTP_RAW.txt", "host:port"),
    ("roosterkid-socks4", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt", "host:port"),
    ("roosterkid-socks5", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "host:port"),
    # proxyscrape
    ("proxyscrape-http", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http&timeout=10000", "host:port"),
    ("proxyscrape-socks4", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks4&timeout=10000", "host:port"),
    ("proxyscrape-socks5", "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text&protocol=socks5&timeout=10000", "host:port"),
    # free-proxy-list.download
    ("fpl-http", "https://www.proxy-list.download/api/v1/get?type=http", "host:port"),
    ("fpl-https", "https://www.proxy-list.download/api/v1/get?type=https", "host:port"),
    ("fpl-socks4", "https://www.proxy-list.download/api/v1/get?type=socks4", "host:port"),
    ("fpl-socks5", "https://www.proxy-list.download/api/v1/get?type=socks5", "host:port"),
    # openproxylist.xyz
    ("openproxy-http", "https://openproxylist.xyz/http.txt", "host:port"),
    ("openproxy-socks4", "https://openproxylist.xyz/socks4.txt", "host:port"),
    ("openproxy-socks5", "https://openproxylist.xyz/socks5.txt", "host:port"),
    # komutan234
    ("komutan234-http", "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/http.txt", "host:port"),
    ("komutan234-socks4", "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/socks4.txt", "host:port"),
    ("komutan234-socks5", "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/socks5.txt", "host:port"),
    # VPSLabCloud
    ("vpslab-http", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_anonymous.txt", "host:port"),
    ("vpslab-ssl", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_ssl.txt", "host:port"),
    # databay-labs
    ("databay-http", "https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/http.txt", "host:port"),
    ("databay-socks4", "https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/socks4.txt", "host:port"),
    ("databay-socks5", "https://raw.githubusercontent.com/databay-labs/free-proxy-list/main/socks5.txt", "host:port"),
    # gfpcom
    ("gfpcom-http", "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/http.txt", "host:port"),
    ("gfpcom-socks5", "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/socks5.txt", "host:port"),
    # proxy4parsing
    ("proxy4parsing-http", "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt", "host:port"),
    # officialputuid KangProxy
    ("kangproxy-http", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy-RAW/http/http.txt", "host:port"),
    ("kangproxy-socks4", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy-RAW/socks4/socks4.txt", "host:port"),
    ("kangproxy-socks5", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy-RAW/socks5/socks5.txt", "host:port"),
    ("kangproxy-https", "https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy-RAW/https/https.txt", "host:port"),
    # Nsttt
    ("nsttt-socks5", "https://raw.githubusercontent.com/Nsttt/SOCKS5-proxy-list/main/proxy-list.txt", "host:port"),
    # TheSpeedX
    ("thespeedx-http", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt", "host:port"),
    ("thespeedx-socks5", "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt", "host:port"),
    # clarketm
    ("clarketm-http", "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "host:port"),
    ("clarketm-socks4", "https://raw.githubusercontent.com/clarketm/proxy-list/master/socks4-list-raw.txt", "host:port"),
    ("clarketm-socks5", "https://raw.githubusercontent.com/clarketm/proxy-list/master/socks5-list-raw.txt", "host:port"),
    # Sunny9577
    ("sunny-http", "https://raw.githubusercontent.com/Sunny9577/proxy-list/master/generated/http.txt", "host:port"),
    ("sunny-socks4", "https://raw.githubusercontent.com/Sunny9577/proxy-list/master/generated/socks4.txt", "host:port"),
    ("sunny-socks5", "https://raw.githubusercontent.com/Sunny9577/proxy-list/master/generated/socks5.txt", "host:port"),
    # zjev
    ("zjev-http", "https://raw.githubusercontent.com/zjev/proxy-list/main/http.txt", "host:port"),
    ("zjev-socks5", "https://raw.githubusercontent.com/zjev/proxy-list/main/socks5.txt", "host:port"),
    # ermaozi
    ("ermaozi-http", "https://raw.githubusercontent.com/ermaozi/get_proxy/main/alive_proxy.txt", "host:port"),
    # tsayen
    ("tsayen-http", "https://raw.githubusercontent.com/tsayen/proxy-list/refs/heads/master/http.txt", "host:port"),
    # hawspider
    ("hawspider-http", "https://raw.githubusercontent.com/hawspider/Proxy-List/refs/heads/main/http.txt", "host:port"),
    # jetkai
    ("jetkai-http", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "host:port"),
    ("jetkai-socks4", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "host:port"),
    ("jetkai-socks5", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "host:port"),

    # ── NEW BATCH (2026-07-05): +45 sources ──
    ("proxifly-http", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt", "host:port"),
    ("proxifly-https", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt", "host:port"),
    ("proxifly-socks4", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.txt", "host:port"),
    ("proxifly-socks5", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "host:port"),
    ("vakhov-http", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/main/http.txt", "host:port"),
    ("vakhov-https", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/main/https.txt", "host:port"),
    ("vakhov-socks4", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/main/socks4.txt", "host:port"),
    ("vakhov-socks5", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/main/socks5.txt", "host:port"),
    ("thordata-http", "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxy-list/http.txt", "host:port"),
    ("thordata-https", "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxy-list/https.txt", "host:port"),
    ("thordata-socks4", "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxy-list/socks4.txt", "host:port"),
    ("thordata-socks5", "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxy-list/socks5.txt", "host:port"),
    ("murongpig-http", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt", "host:port"),
    ("murongpig-https", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/https.txt", "host:port"),
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
    ("ahahaabas-socks5", "https://raw.githubusercontent.com/ahahaabas/socks5-proxies-free/main/proxies.txt", "host:port"),
    ("ahahaabas-verified", "https://raw.githubusercontent.com/ahahaabas/verified-proxy-list/main/proxies.txt", "host:port"),
    ("ahahaabas-mixed", "https://raw.githubusercontent.com/ahahaabas/mixed-proxy-list/main/proxies.txt", "host:port"),
    ("ahahaabas-plain", "https://raw.githubusercontent.com/ahahaabas/plain-text-proxy-list/main/proxies.txt", "host:port"),
    ("ahahaabas-http", "https://raw.githubusercontent.com/ahahaabas/proxy-list-http/main/proxies.txt", "host:port"),
    ("ahahaabas-st-http", "https://raw.githubusercontent.com/ahahaabas/proxies-st-http-socks/main/http.txt", "host:port"),
    ("ahahaabas-st-socks", "https://raw.githubusercontent.com/ahahaabas/proxies-st-http-socks/main/socks.txt", "host:port"),
    ("ahahaabas-free-socks5", "https://raw.githubusercontent.com/ahahaabas/free-socks5-proxy-list/main/proxies.txt", "host:port"),
    ("ahahaabas-bulk", "https://raw.githubusercontent.com/ahahaabas/proxies-st-bulk-list/main/proxies.txt", "host:port"),
    ("adasd223-socks4", "https://raw.githubusercontent.com/adasd223/socks4-proxy-feed/main/proxies.txt", "host:port"),
    ("adasd223-bulk", "https://raw.githubusercontent.com/adasd223/proxies-st-bulk-list/main/proxies.txt", "host:port"),
    ("pscrape-gh-http", "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/raw/http.txt", "host:port"),
    ("pscrape-gh-socks4", "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/raw/socks4.txt", "host:port"),
    ("pscrape-gh-socks5", "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/raw/socks5.txt", "host:port"),
    ("geonode-http", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%%2Chttps", "geonode"),
    ("geonode-socks5", "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5", "geonode"),
    ("monosans-https", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/https.txt", "host:port"),
    ("roosterkid-https", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "host:port"),
    ("proxy4parsing-socks5", "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/socks5.txt", "host:port"),
    ("uptimer-https", "https://raw.githubusercontent.com/UptimerBot/proxy-list/refs/heads/main/https.txt", "host:port"),
    ("byteblitz-https", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/refs/heads/main/https.txt", "host:port"),
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


def extract_proxies(text: str, fmt: str = "") -> List[str]:
    proxies = []
    if fmt == "geonode" and text.strip().startswith("{"):
        try:
            import json as _json
            data = _json.loads(text)
            for item in data.get("data", []):
                ip = item.get("ip", "")
                port = item.get("port", "")
                if ip and port and int(port) <= 65535:
                    proxies.append(f"{ip}:{port}")
        except Exception:
            pass
        return proxies
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
    proxies = extract_proxies(text, fmt)
    print(f"{len(proxies)}")
    return proxies


def scrape_all() -> Set[str]:
    all_proxies = set()
    print(f"Scraping {len(PROXY_SOURCES)} sources...\n")

    with ThreadPoolExecutor(max_workers=25) as pool:
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