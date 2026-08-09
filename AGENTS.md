# AGENTS.md — Proxy Scraper Agent Rules

This repo is meant to be usable by other automation agents (Hermes, OpenClaw, VPS workers, GitHub Actions).

## Mission

Maintain a usable public proxy snapshot and, when needed, rebuild a local live SQLite pool.

## Golden Paths

### Quick consume from GitHub snapshot

Use this when you only need proxy candidates fast and do not need a local DB.

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

Important snapshot files:
- `proxies.txt` — latest committed alive proxy snapshot, `host:port`
- `proxies.json` — proxy metadata from latest run
- `proxies-by-country.json` — grouped proxy snapshot
- `proxies-by-protocol.json` — grouped proxy snapshot
- `proxies-stats.json` — summary stats for the committed snapshot
- `source-health.json` — source reach/health from latest run
- `report-weekly.md` — human-readable report

### Rebuild local live DB

Use this when the agent needs high-quality fresh proxies on its own VPS.

```bash
python3 -m pip install -r requirements.txt
PROXY_DB=data/proxies.db \
PROXY_VALIDATION_WALL_TIMEOUT=300 \
PROXY_SOURCE_MAX_BYTES=2000000 \
PROXY_MAX_PROXIES_PER_SOURCE=15000 \
python3 freshen_pool.py --telegram --telegram-pages 3 --telegram-timeout 900 --telegram-best-effort --max-validate 1500
```

### Publish high-quality snapshot from VPS/local

Use this after a successful local/VPS refresh to update GitHub artifacts without
letting GitHub Actions overwrite quality.

```bash
python3 publish_snapshot.py --dry-run
python3 publish_snapshot.py
```

Publisher safeguards:
- stages only generated artifacts (`proxies.*`, `source-health.json`, reports)
- refuses low-quality snapshots (`min_total`, `min_countries`)
- refuses large quality drops unless `--force`
- throttles commits by default (`--min-interval-hours 6`, `--min-change-pct 5`)

Then query:

```bash
python3 cli.py stats --json
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
python3 cli.py search --protocol http --min-score 50 --max-age-minutes 180 --limit 20 --json
python3 cli.py best --protocol http --limit 10   # top-N
```

### Rotating / sticky gateway + AI reachability

```bash
# Rotating gateway (new egress per request)
python3 gateway.py --mode rotate --port 8080

# Sticky gateway (X-Session-ID → same proxy, TTL 300s)
python3 gateway.py --mode sticky --port 8080

# Check pool proxies can TLS-reach AI services (standalone)
head -100 proxies.txt | python3 ai_reach.py --targets deepseek,chatgpt --timeout 6

# Config merge: config.yaml < env < CLI
python3 -c "from config_loader import merged_config; print(merged_config())"
```

## Do / Don't

Do:
- Use `freshen_pool.py` for scheduled maintenance.
- Use `cli.py best` with freshness filtering for consumers.
- Keep `PROXY_VALIDATION_WALL_TIMEOUT` set in automation.
- Prefer source-balanced validation (`scraper.py` does this internally).
- Keep Telegram scraping on public `t.me/s/<channel>` pages only unless explicitly adding authenticated Telegram support.
- Run `python3 -m py_compile *.py api/*.py` before pushing code changes.
- Expect `ai_reach.py` to exit 2 when the wall timeout fires — that is the contract, not a crash.

Don't:
- Do not commit `data/proxies.db`, `data/proxies.db-wal`, `data/proxies.db-shm`, locks, or runtime Telegram state.
- Do not let CI scrape/overwrite snapshots. Use VPS/local `publish_snapshot.py` for high-quality GitHub artifacts.
- Do not use stale high-score proxies without checking `last_seen`.
- Do not disable the low-port sanity filter; junk ports like `:1`, `:12`, `:41`, `:50` cause validator tail hangs.
- Do not scrape Telegram via auth-required APIs from this repo unless credentials and privacy rules are explicitly provided.
- Do not raise `--max-validate` blindly on small VPS nodes; validation is network-bound and can tail-hang without the wall timeout.

## Quality Defaults

Recommended defaults for unattended jobs:

```bash
PROXY_VALIDATION_WALL_TIMEOUT=300
PROXY_SOURCE_MAX_BYTES=2000000
PROXY_MAX_PROXIES_PER_SOURCE=15000
python3 freshen_pool.py --telegram --telegram-pages 3 --telegram-timeout 900 --telegram-best-effort --max-validate 1500
```

Consumer freshness defaults:

```bash
python3 cli.py best --protocol http --min-score 50 --max-age-minutes 180 --json
```

## Runtime Notes

- GitHub contains snapshots and code, not the live SQLite DB.
- `data/proxies.db` is local runtime state and intentionally gitignored.
- GitHub Actions only run CI/smoke checks and must not overwrite high-quality VPS/local snapshots.
- `source-health.json` can show raw reach far above committed validated snapshot size. That is normal: source reach != alive validated pool.
