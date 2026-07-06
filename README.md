# 🔄 Proxy Scraper v5

Free proxy scraper with validation, scoring, geolocation, pool management, and dashboard.

## Architecture

```
proxy-scraper/
├── scraper.py          # Main scraper (773 lines)
├── proxy_pool.py       # SQLite pool manager (408 lines)
├── discovery.py        # Source auto-discovery (171 lines)
├── api/
│   ├── server.py       # REST API server
│   └── fetch.js        # Vercel fetch relay
├── index.html          # Dashboard UI
├── data/
│   └── proxies.db      # SQLite database (gitignored)
└── .github/workflows/
    └── scrape.yml      # CI: scrape every 3 hours
```

## Outputs

| File | Description |
|------|-------------|
| `proxies.txt` | Validated alive proxies (`host:port`) |
| `proxies.json` | Metadata per proxy (score, geo, speed, anonymity) |
| `proxies-by-country.json` | Grouped by country code |
| `proxies-by-protocol.json` | Grouped by protocol (http/socks5) |
| `proxies-stats.json` | Summary statistics |
| `source-health.json` | Per-source health report |
| `pool.json` | Current pool export |
| `index.html` | Dashboard UI |
| `data/proxies.db` | SQLite database |

## Quick Start

```bash
# Full scrape + validate + pool + dashboard
python3 scraper.py --validate --max-validate 2000 --discover --health --json --grouped

# Just fetch (no validation, fast)
python3 scraper.py

# With private Vercel relay
python3 scraper.py --validate --relay-url https://your-relay.vercel.app --relay-token *** --relay-first
```

## Proxy Scoring (0-100)

| Factor | Weight | Range |
|--------|--------|-------|
| Speed | 40% | 2-10 (based on response time) |
| Anonymity | 30% | 3-10 (elite > transparent > unknown) |
| Protocol | 30% | 3-10 (socks5 > http > unknown) |

## Proxy Pool API

```bash
# Start API server
python3 api/server.py --port 8080

# Get best HTTP proxy
curl "http://localhost:8080/api/proxies/best?protocol=http"

# Get proxies by country
curl "http://localhost:8080/api/proxies?protocol=http&country=ID&limit=10"

# Get pool stats
curl "http://localhost:8080/api/stats"

# Get usage leaderboard
curl "http://localhost:8080/api/leaderboard"
```

## Source Auto-Discovery

```bash
# Discover new sources from GitHub
python3 discovery.py

# Or use in scraper
python3 scraper.py --discover-new
```

Searches GitHub for repos with proxy lists, validates them, and adds to sources.

## Dashboard

Open `index.html` in browser. Features:
- Filter by protocol, country, anonymity
- Search by IP
- Sort by score, speed, country
- Stats overview (total, countries, avg speed)

## Features

| Feature | Module | Status |
|---------|--------|--------|
| Usage Tracking | proxy_pool.py | ✅ |
| Uptime Tracking | proxy_pool.py | ✅ |
| Proxy Pool | proxy_pool.py | ✅ |
| Fingerprinting | proxy_pool.py | ✅ |
| Sticky Sessions | proxy_pool.py | ✅ |
| Quality Metrics | proxy_pool.py | ✅ |
| Source Discovery | discovery.py | ✅ |
| Alert System | scraper.py | ✅ |
| REST API | api/server.py | ✅ |
| Dashboard | index.html | ✅ |

## As a dependency

```bash
# Grab the files
curl -sO https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.txt
curl -sO https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.json
```

## GitHub Actions

- ⏰ Runs every 3 hours
- 🔍 Validates (TCP + HTTP CONNECT + SOCKS5 handshake)
- 🌍 Geolocation lookup
- 📊 Scoring + grouped output
- 📦 Pool database update
- 📝 Auto-commits updated list
- 🚀 Manual trigger via `workflow_dispatch`

## Private Vercel Relay

Optional: deploy `api/fetch.js` to Vercel for relay fetching when GitHub IPs are blocked.

```bash
vercel --prod
vercel env add RELAY_TOKEN production
```

Relay features:
- Token-gated (bearer auth)
- SSRF protection (blocks private IPs)
- Timeout + size caps
- DNS resolution check

## Sources

185+ GitHub repos + public APIs + auto-discovered sources. Health scoring tracks alive/dead status per source.

## License

Public domain. Use freely.
