# 🔄 Proxy Scraper

Auto-scraping free proxies from 15+ sources via GitHub Actions.  
Runs every 6 hours. Validates liveness before committing.

## Usage

### As a dependency
```bash
# Add to your project
git submodule add https://github.com/Shinzzyak/proxy-scraper.git

# Or just grab the file
curl -sO https://raw.githubusercontent.com/Shinzzyak/proxy-scraper/main/proxies.txt
```

### Local run
```bash
python3 scraper.py -o proxies.txt --validate
```

## Format
```
host:port
host:port:user:pass
```

## Sources
- proxy-list.download
- geonode.com
- proxyscrape.com
- spys.me
- openproxylist.xyz
- monosans/proxy-list (GitHub)
- roosterkid/openproxylist (GitHub)
- hookzof/socks5_list (GitHub)
- clarketm/proxy-list (GitHub)
- mmpx12/proxy-list (GitHub)
- sunny9577/proxy-list (GitHub)

## GitHub Actions
- ⏰ Runs every 6 hours via cron
- 🔍 Validates proxy liveness (TCP connect)
- 📝 Auto-commits updated list
- 🚀 Manual trigger via `workflow_dispatch`
