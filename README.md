# 🔄 Proxy Scraper v5

Free proxy scraper with validation, scoring, geolocation, and dashboard.

## Outputs

| File | Description |
|------|-------------|
| `proxies.txt` | Validated alive proxies (`host:port`) |
| `proxies.json` | Metadata per proxy (score, geo, speed, anonymity) |
| `proxies-by-country.json` | Grouped by country code |
| `proxies-by-protocol.json` | Grouped by protocol (http/socks5) |
| `proxies-stats.json` | Summary statistics |
| `source-health.json` | Per-source health report |
| `index.html` | Dashboard UI |
| `proxies-cred.txt` | Credential proxies (when found) |

## Quick Start

```bash
# Full scrape + validate + dashboard
python3 scraper.py --validate --max-validate 500 --discover --health --json --grouped

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

## Dashboard

Open `index.html` in browser. Features:
- Filter by protocol, country, anonymity
- Search by IP
- Sort by score, speed, country
- Stats overview (total, countries, avg speed)

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

170+ GitHub repos + public APIs + auto-discovered sources. Health scoring tracks alive/dead status per source.

## License

Public domain. Use freely.
