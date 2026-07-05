#!/usr/bin/env python3
"""
Free Proxy Scraper v4 — Validation + JSON Output + Health Scoring + Rate Limit Protection.
Outputs: proxies.txt, proxies.json, proxies-cred.txt, source-health.json
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set, Tuple, Dict

# ── Static Sources ─────────────────────────────────────────────────────
PROXY_SOURCES = [
    # ── BATCH 1: Original organic collection ──
    ("tiievii-http", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/http.txt", "host:port"),
    ("tiievii-socks4", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/socks4.txt", "host:port"),
    ("tiievii-socks5", "https://raw.githubusercontent.com/Tiievii/proxy-list/main/socks5.txt", "host:port"),
    ("prxylist-http", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/http.txt", "host:port"),
    ("prxylist-socks4", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/socks4.txt", "host:port"),
    ("prxylist-socks5", "https://raw.githubusercontent.com/prxylist/ProxyLists/main/socks5.txt", "host:port"),
    ("shiftytrash-http", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/http.txt", "host:port"),
    ("shiftytrash-socks4", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/socks4.txt", "host:port"),
    ("shiftytrash-socks5", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/socks5.txt", "host:port"),
    ("imkk-http", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/http.txt", "host:port"),
    ("imkk-socks4", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/socks4.txt", "host:port"),
    ("imkk-socks5", "https://raw.githubusercontent.com/ImKK666/Free-Proxy-List/refs/heads/main/socks5.txt", "host:port"),
    ("mertguvencli-http", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/http.txt", "host:port"),
    ("mertguvencli-socks4", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/socks4.txt", "host:port"),
    ("mertguvencli-socks5", "https://raw.githubusercontent.com/mertguvencli/free-proxy-list/refs/heads/main/proxies/socks5.txt", "host:port"),
    ("kalemulti-http", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/http.txt", "host:port"),
    ("kalemulti-socks4", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/socks4.txt", "host:port"),
    ("kalemulti-socks5", "https://raw.githubusercontent.com/Kalemulti/Free-Proxy-List/main/proxies/socks5.txt", "host:port"),
    ("bootstrap-http", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/http.txt", "host:port"),
    ("bootstrap-socks4", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/socks4.txt", "host:port"),
    ("bootstrap-socks5", "https://raw.githubusercontent.com/TheBootstrapTutorial/ProxyLists/refs/heads/main/lists/socks5.txt", "host:port"),
    ("mksmg-http", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/http.txt", "host:port"),
    ("mksmg-socks4", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/socks4.txt", "host:port"),
    ("mksmg-socks5", "https://raw.githubusercontent.com/mksmg/Free-Proxy-List/main/socks5.txt", "host:port"),
    ("uzayatsiz-http", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/http.txt", "host:port"),
    ("uzayatsiz-socks4", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/socks4.txt", "host:port"),
    ("uzayatsiz-socks5", "https://raw.githubusercontent.com/uzayatsiz/Free-Proxies/main/socks5.txt", "host:port"),
    ("vvmd-http", "https://raw.githubusercontent.com/vvmd/ProxyList/main/http.txt", "host:port"),
    ("vvmd-socks4", "https://raw.githubusercontent.com/vvmd/ProxyList/main/socks4.txt", "host:port"),
    ("vvmd-socks5", "https://raw.githubusercontent.com/vvmd/ProxyList/main/socks5.txt", "host:port"),
    ("defacto2-http", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/http.txt", "host:port"),
    ("defacto2-socks4", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/socks4.txt", "host:port"),
    ("defacto2-socks5", "https://raw.githubusercontent.com/Defacto2/Proxy-list/refs/heads/main/socks5.txt", "host:port"),
    ("anongrp-http", "https://raw.githubusercontent.com/AnonGrp/Proxy-List/main/http.txt", "host:port"),
    ("anongrp-socks4", "https://raw.githubusercontent.com/AnonGrp/Proxy-List/main/socks4.txt", "host:port"),
    ("anongrp-socks5", "https://raw.githubusercontent.com/AnonGrp/Proxy-List/main/socks5.txt", "host:port"),
    ("byteblitz-http", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/main/http.txt", "host:port"),
    ("byteblitz-socks4", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/main/socks4.txt", "host:port"),
    ("byteblitz-socks5", "https://raw.githubusercontent.com/ByteBlitz/Proxy-List/main/socks5.txt", "host:port"),
    ("databay-http", "https://raw.githubusercontent.com/databayt/libproxy/main/http.txt", "host:port"),
    ("databay-socks4", "https://raw.githubusercontent.com/databayt/libproxy/main/socks4.txt", "host:port"),
    ("databay-socks5", "https://raw.githubusercontent.com/databayt/libproxy/main/socks5.txt", "host:port"),
    ("gfpcom-http", "https://raw.githubusercontent.com/gfpcom/Proxy-List/main/http.txt", "host:port"),
    ("gfpcom-socks5", "https://raw.githubusercontent.com/gfpcom/Proxy-List/main/socks5.txt", "host:port"),
    ("hawspider-http", "https://raw.githubusercontent.com/hawspider/Free-Proxy/main/http.txt", "host:port"),
    ("hookzof-socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "host:port"),
    ("ermaozi-http", "https://raw.githubusercontent.com/ermaozi/get_proxy/main/http.txt", "host:port"),
    ("clarketm-http", "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "host:port"),
    ("clarketm-socks4", "https://raw.githubusercontent.com/clarketm/proxy-list/master/socks4.txt", "host:port"),
    ("clarketm-socks5", "https://raw.githubusercontent.com/clarketm/proxy-list/master/socks5.txt", "host:port"),
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
    # ── BATCH 2 ──
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
    ("proxy4parsing-socks5", "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/socks5.txt", "host:port"),
    ("uptimer-https", "https://raw.githubusercontent.com/UptimerBot/proxy-list/refs/heads/main/https.txt", "host:port"),
    # ── BATCH 3: ProxyGather analysis ──
    ("openproxylist-http", "https://openproxylist.xyz/http.txt", "host:port"),
    ("openproxylist-socks4", "https://openproxylist.xyz/socks4.txt", "host:port"),
    ("openproxylist-socks5", "https://openproxylist.xyz/socks5.txt", "host:port"),
    ("hideipme-http", "https://github.com/zloi-user/hideip.me/raw/refs/heads/master/http.txt", "host:port"),
    ("hideipme-https", "https://github.com/zloi-user/hideip.me/raw/refs/heads/master/https.txt", "host:port"),
    ("hideipme-socks4", "https://github.com/zloi-user/hideip.me/raw/refs/heads/master/socks4.txt", "host:port"),
    ("hideipme-socks5", "https://github.com/zloi-user/hideip.me/raw/refs/heads/master/socks5.txt", "host:port"),
    ("ipdb-proxy", "https://raw.githubusercontent.com/ymyuuu/IPDB/refs/heads/main/BestProxy/proxy.txt", "host:port"),
    ("zaeem20-http", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/http.txt", "host:port"),
    ("zaeem20-https", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/https.txt", "host:port"),
    ("zaeem20-socks4", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks4.txt", "host:port"),
    ("zaeem20-socks5", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/socks5.txt", "host:port"),
    ("kangproxy-raw", "https://raw.githubusercontent.com/officialputuid/KangProxy/refs/heads/KangProxy/xResults/RAW.txt", "host:port"),
    ("fatezero-http", "https://static.fatezero.org/tmp/proxy.txt", "host:port"),
    ("fpu-http", "https://freeproxyupdate.com/files/txt/http.txt", "host:port"),
    ("fpu-https", "https://freeproxyupdate.com/files/txt/https-ssl.txt", "host:port"),
    ("fpu-socks4", "https://freeproxyupdate.com/files/txt/socks4.txt", "host:port"),
    ("fpu-socks5", "https://freeproxyupdate.com/files/txt/socks5.txt", "host:port"),
    ("fpu-elite", "https://freeproxyupdate.com/files/txt/elite.txt", "host:port"),
    ("proxydl-http", "https://www.proxy-list.download/api/v2/get?l=en&t=http", "host:port"),
    ("proxydl-https", "https://www.proxy-list.download/api/v2/get?l=en&t=https", "host:port"),
    ("proxydl-socks4", "https://www.proxy-list.download/api/v2/get?l=en&t=socks4", "host:port"),
    ("proxydl-socks5", "https://www.proxy-list.download/api/v2/get?l=en&t=socks5", "host:port"),
    ("proxymist-http", "https://proxymist.com/protocols/http/", "host:port"),
    ("proxymist-socks4", "https://proxymist.com/protocols/socks4/", "host:port"),
    ("proxymist-socks5", "https://proxymist.com/protocols/socks5/", "host:port"),
    ("proxymist-elite", "https://proxymist.com/anonymity/elite/", "host:port"),
    ("ab57-proxylist", "https://ab57.ru/downloads/proxylist.txt", "host:port"),
    ("ab57-proxyold", "https://ab57.ru/downloads/proxyold.txt", "host:port"),
    ("coderduck-http", "https://www.coderduck.com/free-proxy-list", "host:port"),
    ("cybergw-http", "https://cyber-gateway.net/get-proxy/free-proxy/24-free-http-proxy", "host:port"),
    ("cybergw-socks5", "https://cyber-gateway.net/get-proxy/free-proxy/56-free-socks-proxy", "host:port"),
    ("pubproxy-http", "http://pubproxy.com/api/proxy?limit=5&level=elite&last_check=10&speed=1&https=true&format=txt", "host:port"),
    ("911proxy-api", "https://www.911proxy.com/detection/proxyList?limit=500&page=1&sort_by=lastChecked&sort_type=desc", "geonode"),
    ("speedx-http-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/http.txt", "host:port"),
    ("speedx-socks4-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks4.txt", "host:port"),
    ("speedx-socks5-cdn", "https://cdn.jsdelivr.net/gh/TheSpeedX/PROXY-List@master/socks5.txt", "host:port"),
    ("proxifly-all", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt", "host:port"),
    ("monosans-all", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt", "host:port"),
    # ── BATCH 4: More repos found via topic search ──
    ("vpslab-http", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt", "host:port"),
    ("vpslab-socks4", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4.txt", "host:port"),
    ("vpslab-socks5", "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5.txt", "host:port"),
    ("mikicodes-http", "https://raw.githubusercontent.com/mikicodes1/Free-Proxy-List/main/http.txt", "host:port"),
    ("mikicodes-socks5", "https://raw.githubusercontent.com/mikicodes1/Free-Proxy-List/main/socks5.txt", "host:port"),
    ("mgozalid-http", "https://raw.githubusercontent.com/mgozalid/Proxy-List-Updated/main/http.txt", "host:port"),
    ("mgozalid-socks5", "https://raw.githubusercontent.com/mgozalid/Proxy-List-Updated/main/socks5.txt", "host:port"),
    ("aress-mirror-http", "https://raw.githubusercontent.com/aress468/Free-Proxy-List/master/http.txt", "host:port"),
    ("aress-mirror-socks5", "https://raw.githubusercontent.com/aress468/Free-Proxy-List/master/socks5.txt", "host:port"),
    ("pabloqpasin-http", "https://raw.githubusercontent.com/pabloqpasin/proxy-list/main/proxies/http.txt", "host:port"),
    ("pabloqpasin-socks5", "https://raw.githubusercontent.com/pabloqpasin/proxy-list/main/proxies/socks5.txt", "host:port"),
    ("almroot-http", "https://raw.githubusercontent.com/almroot/proxylist/master/http.txt", "host:port"),
    ("almroot-socks5", "https://raw.githubusercontent.com/almroot/proxylist/master/socks5.txt", "host:port"),
    ("sunny9577-http", "https://raw.githubusercontent.com/sunny9577/proxy-list/master/generated/http.json", "geonode"),
    ("sunny9577-socks5", "https://raw.githubusercontent.com/sunny9577/proxy-list/master/generated/socks5.json", "geonode"),
    ("lupinthegreat-http", "https://raw.githubusercontent.com/lupinthegreat/proxy-list/master/http.txt", "host:port"),
    ("lupinthegreat-socks5", "https://raw.githubusercontent.com/lupinthegreat/proxy-list/master/socks5.txt", "host:port"),
    ("databayt-all", "https://raw.githubusercontent.com/databayt/libproxy/main/checked.txt", "host:port"),
    ("hookzof-socks5-v2", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "host:port"),
    ("shiftytrash-all", "https://raw.githubusercontent.com/ShiftyTrash/free-proxy-list/master/all.txt", "host:port"),
    ("proxynova-api", "https://api.proxynova.com/get_all?http=true&https=true&limit=500", "host:port"),
    ("proxy4free-api", "https://www.proxy4free.com/list/free-proxy-list.php", "host:port"),
    ("freeproxy.world-http", "https://freeproxy.world/proxylist/http/", "host:port"),
    ("freeproxy.world-socks5", "https://freeproxy.world/proxylist/socks5/", "host:port"),
]

# ── Discovery: meta-sources that list proxy URLs ───────────────────────
DISCOVERY_SOURCES = [
    "https://raw.githubusercontent.com/Skillter/ProxyGather/refs/heads/master/sites-to-get-proxies-from.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/README.md",
]

# ── Credential proxy sources (ip:port:user:pass) ──────────────────────
CRED_SOURCES = [
    ("spys-cred", "https://spys.me/proxy.txt"),
]

# ── Regex ──────────────────────────────────────────────────────────────
PROXY_RE = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*[:\s]\s*(\d{1,5})")
CRED_RE = re.compile(r"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*[:\s]\s*(\d{1,5})\s*[:\s]\s*(\S+)\s*[:\s]\s*(\S+)")
URL_CREDS_RE = re.compile(r"https?://([^:]+):([^@]+)@(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})")

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
    """Direct URL fetch with UA rotation."""
    req = urllib.request.Request(url, headers={
        "User-Agent": next_ua(),
        "Accept": "text/html,application/json,*/*",
    })
    return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", errors="ignore")


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
        return base64.b64decode(body).decode("utf-8", errors="ignore")
    return body


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


def extract_proxies(text, fmt=""):
    proxies = []
    if fmt == "geonode" and text.strip().startswith("{"):
        try:
            data = json.loads(text)
            for item in data.get("data", []):
                ip, port = item.get("ip", ""), item.get("port", "")
                if ip and port and int(port) <= 65535:
                    proxies.append(f"{ip}:{port}")
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
        if m and int(m.group(2)) <= 65535:
            proxies.append(f"{m.group(1)}:{m.group(2)}")
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
    source_health[name] = {"url": url, "alive": len(proxies) > 0, "proxies": len(proxies), "time_s": round(elapsed, 2)}
    print(f"{len(proxies)}")
    return proxies


def scrape_all(discover=False):
    sources = list(PROXY_SOURCES)
    if discover:
        sources += discover_new_urls()
    all_proxies = set()
    print(f"\nScraping {len(sources)} sources...\n")
    with ThreadPoolExecutor(max_workers=25) as pool:
        futs = {pool.submit(scrape_source, n, u, f): n for n, u, f in sources}
        for fut in as_completed(futs):
            try:
                all_proxies.update(fut.result())
            except Exception as e:
                print(f"  ✗ {e}", file=sys.stderr)
    return all_proxies


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

def validate_tcp(proxy, timeout=5):
    """Basic TCP connect test."""
    ip, port = proxy.split(":")
    try:
        s = socket.create_connection((ip, int(port)), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False


def validate_http_connect(proxy, timeout=5):
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


def validate_socks5(proxy, timeout=5):
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


def detect_anonymity(proxy, timeout=5):
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


def validate_single(proxy, do_anonymity=False):
    """Full validation: TCP + protocol detection + response time + anonymity."""
    ip, port = proxy.split(":")
    t0 = time.time()
    if not validate_tcp(proxy):
        return None
    response_time_ms = round((time.time() - t0) * 1000)

    # Detect protocol
    protocol = "http"  # default
    if validate_socks5(proxy):
        protocol = "socks5"
    elif validate_http_connect(proxy):
        protocol = "http"
    else:
        protocol = "unknown"

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


def filter_valid(proxies, max_validate=500, do_anonymity=False):
    """Validate proxies in parallel, return list of valid proxy dicts."""
    to_test = list(proxies)[:max_validate]
    print(f"\n🔍 Validating {len(to_test)} proxies...\n")
    valid = []
    with ThreadPoolExecutor(max_workers=200) as pool:
        futs = {pool.submit(validate_single, p, do_anonymity): p for p in to_test}
        for fut in as_completed(futs):
            result = fut.result()
            if result:
                valid.append(result)
                print(f"  ✅ {result['ip']}:{result['port']} [{result['protocol']}] {result['response_time_ms']}ms {result['anonymity']}")
    print(f"\n📊 {len(valid)}/{len(to_test)} alive")
    return valid


# ── JSON Output ────────────────────────────────────────────────────────

def save_json_output(valid_proxies, filename="proxies.json"):
    """Save validated proxies as JSON with metadata."""
    with open(filename, "w") as f:
        json.dump(valid_proxies, f, indent=2)
    print(f"✅ JSON output → {filename} ({len(valid_proxies)} proxies)")


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
    ap.add_argument("--max-validate", type=int, default=500)
    ap.add_argument("--json", action="store_true", help="Output proxies.json with metadata")
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
    proxies = scrape_all(discover=args.discover)
    if not proxies:
        print("\n✗ No proxies!", file=sys.stderr)
        sys.exit(1)
    print(f"\n📊 {len(proxies)} unique proxies")

    # Save health report
    if args.health:
        save_health_report()

    if args.validate:
        valid = filter_valid(proxies, args.max_validate, do_anonymity=args.validate_full)
        if args.json and valid:
            save_json_output(valid)
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