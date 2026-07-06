# Agent Usage Guide — Proxy Scraper

This guide is for Hermes/OpenClaw/other VPS agents that want to use this repo without rediscovering the operational details.

## What GitHub Gives You

GitHub stores **code + committed snapshots**.

Available directly after clone/pull:

| File | Use |
|---|---|
| `proxies.txt` | Simple `host:port` snapshot. Fastest to consume. |
| `proxies.json` | Validated proxy snapshot with score, geo, protocol, speed. |
| `proxies-by-country.json` | Country-grouped proxy snapshot. |
| `proxies-by-protocol.json` | Protocol-grouped proxy snapshot. |
| `proxies-stats.json` | Snapshot metrics. |
| `source-health.json` | Source reach/health. Shows raw candidate volume. |
| `report-weekly.md` | Human-readable report. |

GitHub does **not** store:

| File | Reason |
|---|---|
| `data/proxies.db` | Local live SQLite DB; runtime state. |
| `data/proxies.db-wal`, `data/proxies.db-shm` | SQLite runtime files. |
| `data/freshen_pool.lock` | Local lock. |
| `data/tg_state.json`, `data/tg_attachments.json`, `data/tg_proxies.txt` | Runtime Telegram scrape state. |

## Mode 1 — Consume Snapshot Only

Use this when you need proxy candidates immediately.

```bash
git clone https://github.com/Shinzzyak/proxy-scraper.git
cd proxy-scraper

git pull
wc -l proxies.txt
head -20 proxies.txt
```

Python example:

```python
import json

with open('proxies.json') as f:
    proxies = json.load(f)

freshish = [p for p in proxies if p.get('score', 0) >= 50]
print(freshish[:10])
```

Shell example:

```bash
curl -fsSL https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.txt | head
```

## Mode 2 — Rebuild Live Local Pool

Use this on another VPS when the agent needs fresh local selection and scoring.

```bash
git clone https://github.com/Shinzzyak/proxy-scraper.git
cd proxy-scraper
python3 -m pip install -r requirements.txt

PROXY_DB=data/proxies.db \
PROXY_VALIDATION_WALL_TIMEOUT=300 \
PROXY_SOURCE_MAX_BYTES=2000000 \
PROXY_MAX_PROXIES_PER_SOURCE=15000 \
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500
```

Then inspect:

```bash
python3 cli.py stats --json | python3 -m json.tool
python3 cli.py sources --json | python3 -m json.tool | head
```

Get consumer proxy:

```bash
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

Search pool:

```bash
python3 cli.py search --protocol http --country ID --min-score 50 --max-age-minutes 180 --limit 20 --json
```

## Mode 3 — Scheduled VPS Maintenance

Cron example:

```cron
0 */2 * * * cd /path/to/proxy-scraper && /usr/bin/env PROXY_DB=data/proxies.db PROXY_VALIDATION_WALL_TIMEOUT=300 PROXY_SOURCE_MAX_BYTES=2000000 PROXY_MAX_PROXIES_PER_SOURCE=15000 python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500 --log >> logs/cron.log 2>&1
```

Systemd timer is also fine. Keep the lock file enabled; `freshen_pool.py` prevents overlapping runs.

### Publish refreshed VPS snapshot to GitHub

After a successful local/VPS refresh, publish only if quality gates and throttle pass:

```bash
python3 publish_snapshot.py --dry-run
python3 publish_snapshot.py
```

Recommended combined scheduled command:

```bash
PROXY_DB=data/proxies.db \
PROXY_VALIDATION_WALL_TIMEOUT=300 \
PROXY_SOURCE_MAX_BYTES=2000000 \
PROXY_MAX_PROXIES_PER_SOURCE=15000 \
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500 && \
python3 publish_snapshot.py
```

Publisher defaults:
- `--min-total 1000`
- `--min-countries 50`
- `--min-interval-hours 6`
- `--min-change-pct 5`
- `--max-drop-pct 20`

It stages only generated artifacts, not source code. Use `--force` only for an intentional baseline/reset.

## Mode 4 — GitHub Actions CI

This repo has `.github/workflows/scrape.yml`.

Expected behavior:
- syntax check with Python 3.11
- smoke-test committed snapshot files
- no scraping
- no generated snapshot commits

Why: GitHub runner network conditions produced much smaller pools than the VPS/local run. Do not let CI overwrite high-quality snapshots. If an external agent depends on GitHub snapshots, read the committed files from `main`. If the agent needs stronger freshness/quality, rebuild a local DB with the 1500-sample VPS command instead.

## Quality Rules

### Validation limits

Recommended unattended settings:

| Env/Arg | Recommended | Why |
|---|---:|---|
| `PROXY_VALIDATION_WALL_TIMEOUT` | `300` | Hard deadline; prevents network tail-hangs. |
| `PROXY_SOURCE_MAX_BYTES` | `2000000` | Bounds giant upstream feeds. |
| `PROXY_MAX_PROXIES_PER_SOURCE` | `15000` | Prevents a single huge source from dominating. |
| `--max-validate` | `1500` | Good VPS balance between freshness and runtime. |
| `--telegram-pages` | `3` | Enough to capture recent high-signal Telegram posts. |

### Consumer filter

Use both score and freshness:

```bash
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

Reason: old public proxies die fast; a stale high score is worse than no proxy.

### Low-port filter

Do not remove the plausible-port filter in `scraper.py` / `tg_scraper.py`.

Allowed low ports are common proxy ports only. Other ports below 1024 are rejected. This blocks junk like `:1`, `:12`, `:41`, `:50` that can cause validator tail-hangs.

## Telegram Notes

Telegram scraping uses public previews only:

```text
https://t.me/s/<channel>
```

No Telegram API auth is required.

The scraper can extract:
- raw `IP:PORT` text/code blocks
- document attachment metadata for proxy-like `.txt` files

Some Telegram attachments are visible as metadata only; public preview does not always expose direct file bytes.

High-signal channels currently configured include:
- `freeproxyses`
- `VipProxy24`
- `HQPROX`
- `whale_proxy`
- `proxyscrape`
- `TGunblock`

List active channels:

```bash
python3 tg_scraper.py --list-active
```

Test selected channels:

```bash
python3 cli.py telegram --channels freeproxyses VipProxy24 HQPROX whale_proxy --pages 2 --output /tmp/tg_test.txt
wc -l /tmp/tg_test.txt
```

## Source Expansion Rules

Prefer raw text feeds over HTML/table scraping.

Good source format:

```text
1.2.3.4:8080
5.6.7.8:1080
```

Before adding a new source permanently:

```bash
python3 - <<'PY'
import urllib.request, re
url='https://example.com/proxies.txt'
data=urllib.request.urlopen(url, timeout=20).read(2_000_000).decode('utf-8','ignore')
hits=set(re.findall(r'\b\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}\b', data))
print(len(hits), list(hits)[:5])
PY
```

Then run:

```bash
python3 -m py_compile *.py api/*.py
PROXY_SOURCE_MAX_BYTES=600000 PROXY_MAX_PROXIES_PER_SOURCE=5000 python3 scraper.py --health -o /tmp/probe.txt
```

## Troubleshooting

### Pool DB missing

Run:

```bash
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500
```

### No best proxy returned

Relax freshness or run freshen:

```bash
python3 cli.py best --protocol http --min-score 30 --max-age-minutes 0 --json
python3 freshen_pool.py --telegram --telegram-pages 3 --max-validate 1500
```

### Cron overlaps

`freshen_pool.py` uses `data/freshen_pool.lock`. If the process died and the lock is old, it replaces stale locks automatically after the configured max age.

### Validation hangs

Ensure this env is present:

```bash
PROXY_VALIDATION_WALL_TIMEOUT=300
```

Also keep low-port filtering enabled.

## Verification Before Commit

Agents should run:

```bash
python3 -m py_compile *.py api/*.py
python3 cli.py stats --json | python3 -m json.tool | head
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

For docs/workflow changes:

```bash
git diff --check
git status --short
```
