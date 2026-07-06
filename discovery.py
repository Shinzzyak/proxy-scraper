#!/usr/bin/env python3
"""
discovery.py — Auto-discover new proxy sources from GitHub

Searches GitHub for repos with proxy lists, validates them, and adds to sources.
"""
import json
import os
import re
import time
import urllib.request
import urllib.parse
from typing import List, Dict

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
SEARCH_QUERIES = [
    "proxy list txt",
    "free proxy list",
    "proxy scraper",
    "proxy list http",
    "proxy list socks5",
    "alive proxy list",
    "proxy list 2026",
    "proxy list updated",
]

KNOWN_REPOS = set()  # loaded from sources list


def github_search(query: str, page: int = 1) -> List[Dict]:
    """Search GitHub for repos matching query."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ProxyScraper/5.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    params = urllib.parse.urlencode({"q": query, "per_page": 10, "page": page, "sort": "updated"})
    url = f"https://api.github.com/search/repositories?{params}"
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read().decode())
        return data.get("items", [])
    except Exception as e:
        print(f"  ⚠ GitHub search failed: {e}")
        return []


def find_proxy_files(owner: str, repo: str) -> List[Dict]:
    """Find proxy list files in a repo."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ProxyScraper/5.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/main?recursive=1"
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read().decode())
        tree = data.get("tree", [])
    except Exception:
        # Try master branch
        try:
            url = url.replace("main", "master")
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read().decode())
            tree = data.get("tree", [])
        except Exception:
            return []

    proxy_files = []
    patterns = [
        r"(?:http|socks[45]|proxy)[-_]?(?:list|proxy|server)?\.(?:txt|csv|json)$",
        r"(?:proxy|proxies)\.(?:txt|csv|json)$",
        r"(?:alive|valid|working)[-_]?(?:proxy|proxies)?\.(?:txt|csv|json)$",
        r"(?:raw|list)\.(?:txt|csv|json)$",
    ]
    for item in tree:
        if item["type"] != "blob":
            continue
        path = item["path"].lower()
        for pat in patterns:
            if re.search(pat, path, re.IGNORECASE):
                proxy_files.append({
                    "url": f"https://raw.githubusercontent.com/{owner}/{repo}/main/{item['path']}",
                    "path": item["path"],
                    "size": item.get("size", 0),
                })
                break
    return proxy_files


def discover_sources(max_results: int = 50) -> List[Dict]:
    """Search GitHub for new proxy sources."""
    discovered = []
    seen_repos = set()

    for query in SEARCH_QUERIES:
        print(f"🔍 Searching: {query}")
        repos = github_search(query)
        time.sleep(2)  # respect rate limit

        for repo in repos:
            full_name = repo["full_name"]
            if full_name in seen_repos:
                continue
            seen_repos.add(full_name)

            owner = repo["owner"]["login"]
            name = repo["name"]
            stars = repo.get("stargazers_count", 0)
            updated = repo.get("updated_at", "")

            # Only consider repos with some activity
            if stars < 2:
                continue

            print(f"  📦 {full_name} (⭐{stars})")
            files = find_proxy_files(owner, name)
            time.sleep(1)

            for f in files[:3]:  # max 3 files per repo
                discovered.append({
                    "name": f"{name}-{f['path'].split('/')[-1].replace('.txt', '').replace('.csv', '')}",
                    "url": f["url"],
                    "format": "host:port",
                    "repo": full_name,
                    "stars": stars,
                    "last_updated": updated,
                    "file_size": f["size"],
                })

            if len(discovered) >= max_results:
                break
        if len(discovered) >= max_results:
            break

    print(f"\n✅ Discovered {len(discovered)} new sources")
    return discovered


def test_source(url: str, timeout: int = 10) -> Dict:
    """Quick test: can we fetch this URL and does it contain proxies?"""
    import re as _re
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ProxyScraper/5.0"})
        resp = urllib.request.urlopen(req, timeout=timeout)
        text = resp.read().decode("utf-8", errors="ignore")[:5000]
        # Count proxy-like patterns
        proxies = _re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}[:\s]+\d{2,5}', text)
        return {
            "alive": True,
            "sample_proxies": len(proxies),
            "first_proxy": proxies[0] if proxies else "",
        }
    except Exception as e:
        return {"alive": False, "error": str(e)}


if __name__ == "__main__":
    sources = discover_sources(max_results=20)
    print(f"\n{'='*60}")
    for s in sources:
        print(f"  {s['name']}: {s['url']}")
        print(f"    ⭐{s['stars']} | {s['file_size']} bytes")
