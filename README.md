# 🔄 Proxy Scraper

Free proxy scraper with validation, JSON output, source-health scoring, auto-discovery, and optional private Vercel fetch relay.

## Outputs

- `proxies.txt` — validated alive proxies (`host:port`)
- `proxies.json` — metadata per proxy (`ip`, `port`, `protocol`, `response_time_ms`, `anonymity`, `last_seen`)
- `source-health.json` — per-source health (`alive`, `proxies`, `time_s`)
- `proxies-cred.txt` — credential proxies when found (`host:port:user:pass`)

## Usage

### As a dependency
```bash
# Add to your project
git submodule add https://github.com/Shinzzyak/proxy-scraper.git

# Or just grab the file
curl -sO https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.txt
curl -sO https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.json
```

### Local run
```bash
python3 scraper.py -o proxies.txt --validate --max-validate 500 --discover --health --json
```

### Optional: private Vercel fetch relay

This is **not** a public open proxy. It is a token-gated source fetch relay for scraper reliability when GitHub runner IPs are blocked/rate-limited by some source sites.

Deploy:
```bash
vercel --prod
vercel env add RELAY_TOKEN production
```

Run scraper through relay:
```bash
export PROXY_RELAY_URL="https://your-project.vercel.app"
export PROXY_RELAY_TOKEN="your-secret-token"
python3 scraper.py --validate --max-validate 500 --discover --health --json --relay-first
```

Or CLI args:
```bash
python3 scraper.py \
  --validate --max-validate 500 --discover --health --json \
  --relay-url "https://your-project.vercel.app" \
  --relay-token "your-secret-token" \
  --relay-first
```

Relay safety guardrails:
- Optional bearer token (`RELAY_TOKEN`)
- Blocks `localhost`, private IPs, link-local, CGNAT, `.local`
- DNS resolution checked for private targets (SSRF guard)
- `http`/`https` only
- Blocks URL credentials
- Timeout cap 25s
- Response size cap 1 MiB by default (`RELAY_MAX_BYTES`)

## Format

```txt
host:port
host:port:user:pass
```

## GitHub Actions

- ⏰ Runs every 3 hours via cron
- 🔍 Validates proxy liveness (TCP + protocol detection)
- 🧾 Writes `proxies.json` + `source-health.json`
- 📝 Auto-commits updated list
- 🚀 Manual trigger via `workflow_dispatch`

## Notes

Authenticated public proxies are rare. Most credential proxies are private provider resources (Bright Data, Oxylabs, Webshare, etc.), so `proxies-cred.txt` is usually empty unless you add a private source.
