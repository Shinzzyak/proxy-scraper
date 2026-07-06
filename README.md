# 🔄 Proxy Scraper v5

Free proxy scraper with validation, scoring, geolocation, Telegram public-preview scraping, SQLite pool management, reports, and GitHub snapshot exports.

## Agent quickstart

If you are Hermes/OpenClaw/another VPS agent, read first:

- [`AGENTS.md`](AGENTS.md) — operational rules for automation agents
- [`docs/AGENT_USAGE.md`](docs/AGENT_USAGE.md) — full playbook: consume GitHub snapshot, rebuild live DB, schedule refresh, quality tuning

### Consume GitHub snapshot

```bash
git clone https://github.com/Shinzzyak/proxy-scraper.git
cd proxy-scraper
git pull
head proxies.txt
python3 - <<'PY'
import json
print(json.load(open('proxies-stats.json')))
PY
```

### Rebuild live local DB

```bash
python3 -m pip install -r requirements.txt
PROXY_DB=data/proxies.db \
PROXY_VALIDATION_WALL_TIMEOUT=300 \
PROXY_SOURCE_MAX_BYTES=2000000 \
PROXY_MAX_PROXIES_PER_SOURCE=15000 \
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500
```

### Get best fresh proxy

```bash
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

## Architecture

```text
proxy-scraper/
├── scraper.py             # Main source scraper + validator + reports
├── freshen_pool.py        # Scheduled maintenance wrapper with lock/state
├── tg_scraper.py          # Public Telegram preview scraper
├── geo_repair.py          # ip-api.com geo repair for pool DB
├── proxy_pool.py          # SQLite pool manager + selection API
├── cli.py                 # Operator/consumer CLI
├── discovery.py           # GitHub source discovery helper
├── api/
│   ├── server.py          # REST API server
│   └── fetch.js           # Optional Vercel fetch relay
├── docs/
│   └── AGENT_USAGE.md     # Hermes/agent playbook
├── data/
│   └── proxies.db         # Local SQLite DB, gitignored
└── .github/workflows/
    └── scrape.yml         # CI snapshot refresh
```

## Outputs committed to GitHub

| File | Description |
|---|---|
| `proxies.txt` | Latest committed validated proxy snapshot (`host:port`) |
| `proxies.json` | Snapshot with metadata: score, geo, protocol, response time |
| `proxies-by-country.json` | Snapshot grouped by country code |
| `proxies-by-protocol.json` | Snapshot grouped by protocol |
| `proxies-stats.json` | Summary statistics for the committed snapshot |
| `source-health.json` | Per-source health/reach report |
| `report-weekly.md` | Human-readable report |
| `heatmap.html` | Generated heatmap/dashboard artifact when produced |

## Runtime files not committed

| File | Reason |
|---|---|
| `data/proxies.db` | Local live SQLite pool |
| `data/proxies.db-wal`, `data/proxies.db-shm` | SQLite runtime files |
| `data/freshen_pool.lock` | Local lock |
| `data/freshen_pool_state.json` | Runtime state |
| `data/tg_state.json`, `data/tg_attachments.json`, `data/tg_proxies.txt` | Runtime Telegram state |
| `logs/` | Local logs |

## Common commands

```bash
# Full scheduled maintenance path
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500

# Main scraper only, validate and update report snapshots
python3 scraper.py --validate --pool --json --grouped --health --max-validate 1500

# Fast raw source scan, no validation
python3 scraper.py --health -o /tmp/proxies_raw.txt

# Telegram public preview scrape
python3 cli.py telegram --channels freeproxyses VipProxy24 HQPROX whale_proxy --pages 2 --output /tmp/tg.txt

# Pool stats
python3 cli.py stats --json | python3 -m json.tool

# Search local live DB
python3 cli.py search --protocol http --country ID --min-score 50 --max-age-minutes 180 --limit 20 --json

# Report
python3 cli.py report
```

## Quality defaults

Recommended unattended environment:

```bash
PROXY_VALIDATION_WALL_TIMEOUT=300
PROXY_SOURCE_MAX_BYTES=2000000
PROXY_MAX_PROXIES_PER_SOURCE=15000
```

Recommended scheduled command:

```bash
PROXY_DB=data/proxies.db \
PROXY_VALIDATION_WALL_TIMEOUT=300 \
PROXY_SOURCE_MAX_BYTES=2000000 \
PROXY_MAX_PROXIES_PER_SOURCE=15000 \
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500
```

Why these defaults:
- source caps keep huge upstream feeds from dominating memory/time
- source-balanced validation avoids giant-source bias
- wall timeout prevents network tail-hangs
- freshness filters prevent stale high-score proxies from being returned

## Proxy scoring

| Factor | Weight | Notes |
|---|---:|---|
| Speed | 40% | Based on response time |
| Anonymity | 30% | elite > transparent > unknown |
| Protocol | 30% | socks5 > http > unknown |

Consumer rule:

```bash
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

## REST API

```bash
python3 api/server.py --port 8080

curl "http://localhost:8080/api/proxies/best?protocol=http"
curl "http://localhost:8080/api/proxies?protocol=http&country=ID&limit=10"
curl "http://localhost:8080/api/stats"
curl "http://localhost:8080/api/leaderboard"
```

## Telegram scraping

The scraper uses public previews only:

```text
https://t.me/s/<channel>
```

It extracts:
- raw `IP:PORT` text/code blocks
- proxy-like Telegram document attachment metadata

No Telegram auth is needed for the current workflow.

High-signal channels currently configured include:
- `freeproxyses`
- `VipProxy24`
- `HQPROX`
- `whale_proxy`
- `proxyscrape`
- `TGunblock`

## GitHub Actions

`.github/workflows/scrape.yml` refreshes snapshots on a schedule/manual dispatch.

The action uses the same bounded maintenance defaults as VPS agents, installs `requirements.txt`, runs `freshen_pool.py`, and commits updated snapshot/report files when changed.

## Private Vercel relay

Optional: deploy `api/fetch.js` to Vercel for relay fetching when GitHub/VPS IPs are blocked.

```bash
vercel --prod
vercel env add RELAY_TOKEN production
```

Relay features:
- token-gated bearer auth
- SSRF protection
- timeout + size caps
- DNS resolution check

## Source expansion rules

Prefer raw text feeds over table scraping.

Before adding a source permanently, probe it:

```bash
python3 - <<'PY'
import urllib.request, re
url='https://example.com/proxies.txt'
data=urllib.request.urlopen(url, timeout=20).read(2_000_000).decode('utf-8','ignore')
hits=set(re.findall(r'\b\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}\b', data))
print(len(hits), list(hits)[:5])
PY
```

Then verify:

```bash
python3 -m py_compile *.py api/*.py
PROXY_SOURCE_MAX_BYTES=600000 PROXY_MAX_PROXIES_PER_SOURCE=5000 python3 scraper.py --health -o /tmp/probe.txt
```

## License

Public domain. Use freely.
