#!/usr/bin/env python3
"""
cli.py — One-liner CLI for proxy pool queries.

Usage:
    python3 cli.py search --country ID --protocol socks5 --min-score 70
    python3 cli.py best --protocol http
    python3 cli.py stats
    python3 cli.py top --limit 10
    python3 cli.py banned
    python3 cli.py heatmap
    python3 cli.py benchmark --limit 20
    python3 cli.py report
    python3 cli.py geo-repair
    python3 cli.py telegram --pages 3 --add-to-pool
    python3 cli.py freshen --telegram --max-validate 3000
"""
import argparse
import json
import sys
import os
import signal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def cmd_search(args):
    from proxy_pool import search_proxies
    results = search_proxies(
        protocol=args.protocol or "",
        country_code=args.country or "",
        min_score=args.min_score or 0,
        anonymity=args.anonymity or "",
        max_results=args.limit,
        max_age_minutes=args.max_age_minutes,
    )
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for p in results:
            print(f"{p['ip']}:{p['port']} [{p['protocol']}] score={p['score']} {p.get('country_code','')} {p.get('anonymity','')}")
    print(f"\n{len(results)} proxies found")


def cmd_best(args):
    from proxy_pool import get_best_proxy
    proxy = get_best_proxy(
        args.protocol or "http",
        args.country or "",
        min_score=args.min_score,
        max_age_minutes=args.max_age_minutes,
    )
    if proxy:
        if args.json:
            print(json.dumps(proxy, indent=2))
        else:
            print(f"{proxy['ip']}:{proxy['port']} [{proxy['protocol']}] score={proxy['score']} {proxy.get('country_code','')} {proxy.get('anonymity','')}")
    else:
        print("No proxy available")


def cmd_stats(args):
    from proxy_pool import get_pool_stats, get_db
    stats = get_pool_stats()
    if args.json:
        print(json.dumps(stats, indent=2))
    else:
        for k, v in stats.items():
            print(f"  {k}: {v}")


def cmd_top(args):
    from proxy_pool import get_usage_leaderboard
    lb = get_usage_leaderboard(args.limit)
    if args.json:
        print(json.dumps(lb, indent=2))
    else:
        for p in lb:
            print(f"  {p['ip']}:{p['port']} — {p['success_rate']}% success ({p['total_uses']} uses, avg {p['avg_ms']}ms)")


def cmd_banned(args):
    from reputation import get_banned_sources
    banned = get_banned_sources()
    if args.json:
        print(json.dumps(banned, indent=2))
    else:
        for b in banned:
            print(f"  🚫 {b['source_name']} — {b['ban_reason']} ({b['success_rate']:.1%})")
        print(f"\n{len(banned)} banned sources")


def cmd_heatmap(args):
    from proxy_pool import get_db
    from heatmap import generate_heatmap
    conn = get_db()
    rows = conn.execute("SELECT * FROM proxies").fetchall()
    proxies = [dict(r) for r in rows]
    conn.close()
    generate_heatmap(proxies, args.output or "heatmap.html")


def cmd_benchmark(args):
    from proxy_pool import search_proxies
    from benchmark import benchmark_batch, save_benchmark
    proxies = search_proxies(protocol=args.protocol or "http", max_results=args.limit, min_score=50, max_age_minutes=180)
    if not proxies:
        print("No proxies to benchmark")
        return
    results = benchmark_batch(proxies, max_workers=20)
    save_benchmark(results, args.output or "benchmark.json")


def cmd_report(args):
    from report import generate_weekly_report
    report = generate_weekly_report(args.output or "report-weekly.md")
    if not args.output:
        print(report)


def cmd_geo_repair(args):
    from geo_repair import repair_geo
    result = repair_geo(
        batch_size=args.batch_size,
        limit=args.limit,
        export=not args.no_export,
        json_fallback=args.json_fallback,
    )
    if args.json:
        print(json.dumps(result, indent=2))


def cmd_sources(args):
    from proxy_pool import get_db
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT source_name,
                COUNT(*) as proxies,
                SUM(CASE WHEN COALESCE(country_code,'') != '' THEN 1 ELSE 0 END) as geo_known,
                ROUND(AVG(score),1) as avg_score,
                ROUND(AVG(response_time_ms),0) as avg_rt
            FROM proxies
            WHERE source_name != ''
            GROUP BY source_name
            ORDER BY proxies DESC
        """).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], indent=2))
        else:
            print(f"{'Source':<30} {'Proxies':>8} {'Geo%':>6} {'Score':>6} {'RT':>6}")
            print("-" * 60)
            for r in rows:
                geo_pct = round(100.0 * r['geo_known'] / max(r['proxies'], 1), 0)
                print(f"{r['source_name']:<30} {r['proxies']:>8} {geo_pct:>5.0f}% {r['avg_score']:>5} {r['avg_rt']:>5}ms")
    finally:
        conn.close()


def cmd_telegram(args):
    import tg_scraper
    if args.list_channels:
        config = tg_scraper.load_config()
        print("\n📱 Telegram Proxy Channels:\n")
        for name, info in sorted(config["channels"].items()):
            status = "✅" if info.get("enabled") else "❌"
            print(f"  {status} @{name} [{info.get('priority', '?')}] — {info.get('description', 'N/A')}")
            print(f"     Protocols: {', '.join(info.get('protocols', []))}")
        print(f"\nTotal: {len(config['channels'])} channels")
        return
    channels = [c.lstrip('@') for c in (args.channels or [])] or None
    results = tg_scraper.scrape_all_channels(channels, pages=args.pages)
    all_proxies = set()
    for proxies in results.values():
        all_proxies.update(proxies)
    output = args.output or os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "tg_proxies.txt")
    with open(output, "w") as f:
        f.write("\n".join(sorted(all_proxies)) + "\n")
    print(f"\n📄 Telegram proxies → {output} ({len(all_proxies)})")
    if args.add_to_pool:
        tg_scraper.add_to_pool(results)


def cmd_freshen(args):
    import freshen_pool
    argv = ["freshen_pool.py"]
    argv += ["--max-validate", str(args.max_validate)]
    if args.telegram:
        argv.append("--telegram")
        argv += ["--telegram-pages", str(args.telegram_pages)]
    if args.geo_only:
        argv.append("--geo-only")
    if args.scrape_only:
        argv.append("--scrape-only")
    if args.no_lock:
        argv.append("--no-lock")
    old_argv = sys.argv
    try:
        sys.argv = argv
        freshen_pool.main()
    finally:
        sys.argv = old_argv


def cmd_source_health(args):
    from proxy_pool import get_db
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT source_name,
                COUNT(*) as runs,
                SUM(CASE WHEN alive=1 THEN 1 ELSE 0 END) as alive_runs,
                MAX(timestamp) as last_seen
            FROM source_history
            GROUP BY source_name
            ORDER BY last_seen DESC
        """).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], indent=2))
        else:
            print(f"{'Source':<30} {'Runs':>6} {'Alive':>6} {'Uptime':>7} {'Last Seen':>20}")
            print("-" * 75)
            for r in rows:
                uptime = round(100.0 * r['alive_runs'] / max(r['runs'], 1), 1)
                print(f"{r['source_name']:<30} {r['runs']:>6} {r['alive_runs']:>6} {uptime:>6.1f}% {r['last_seen'][:19] if r['last_seen'] else 'never':>20}")
    finally:
        conn.close()


def main():
    # Avoid noisy BrokenPipeError tracebacks when piping large JSON output to tools like `head`.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    ap = argparse.ArgumentParser(description="Proxy Pool CLI")
    sub = ap.add_subparsers(dest="command")

    # search
    s = sub.add_parser("search")
    s.add_argument("--country", "-c", default="")
    s.add_argument("--protocol", "-p", default="")
    s.add_argument("--min-score", type=int, default=0)
    s.add_argument("--anonymity", "-a", default="")
    s.add_argument("--limit", "-l", type=int, default=20)
    s.add_argument("--max-age-minutes", type=int, default=0, help="0 disables freshness filter")
    s.add_argument("--json", "-j", action="store_true")

    # best
    b = sub.add_parser("best")
    b.add_argument("--protocol", "-p", default="http")
    b.add_argument("--country", "-c", default="")
    b.add_argument("--min-score", type=int, default=50)
    b.add_argument("--max-age-minutes", type=int, default=180, help="0 disables freshness filter")
    b.add_argument("--json", "-j", action="store_true")

    # stats
    st = sub.add_parser("stats")
    st.add_argument("--json", "-j", action="store_true")

    # top
    t = sub.add_parser("top")
    t.add_argument("--limit", "-l", type=int, default=10)
    t.add_argument("--json", "-j", action="store_true")

    # banned
    bd = sub.add_parser("banned")
    bd.add_argument("--json", "-j", action="store_true")

    # heatmap
    hm = sub.add_parser("heatmap")
    hm.add_argument("--output", "-o", default="heatmap.html")

    # benchmark
    bm = sub.add_parser("benchmark")
    bm.add_argument("--protocol", "-p", default="http")
    bm.add_argument("--limit", "-l", type=int, default=20)
    bm.add_argument("--output", "-o", default="benchmark.json")

    # report
    rp = sub.add_parser("report")
    rp.add_argument("--output", "-o", default="")

    # geo repair
    gr = sub.add_parser("geo-repair")
    gr.add_argument("--batch-size", type=int, default=100)
    gr.add_argument("--limit", "-l", type=int, default=0, help="0 = all missing proxies")
    gr.add_argument("--no-export", action="store_true")
    gr.add_argument("--json-fallback", default="proxies.json")
    gr.add_argument("--json", "-j", action="store_true")

    # sources
    sr = sub.add_parser("sources")
    sr.add_argument("--json", "-j", action="store_true")

    # source-health
    sh = sub.add_parser("source-health")
    sh.add_argument("--json", "-j", action="store_true")

    # telegram
    tg = sub.add_parser("telegram", help="Scrape proxy lists from Telegram public channels")
    tg.add_argument("--channels", nargs="*", default=None)
    tg.add_argument("--pages", type=int, default=3)
    tg.add_argument("--add-to-pool", action="store_true")
    tg.add_argument("--output", "-o", default="")
    tg.add_argument("--list-channels", action="store_true")

    # freshen
    fr = sub.add_parser("freshen", help="Run scheduled scrape + geo repair + optional Telegram refresh")
    fr.add_argument("--max-validate", type=int, default=3000)
    fr.add_argument("--telegram", action="store_true")
    fr.add_argument("--telegram-pages", type=int, default=3)
    fr.add_argument("--geo-only", action="store_true")
    fr.add_argument("--scrape-only", action="store_true")
    fr.add_argument("--no-lock", action="store_true")

    args = ap.parse_args()
    if not args.command:
        ap.print_help()
        return

    cmds = {
        "search": cmd_search, "best": cmd_best, "stats": cmd_stats,
        "top": cmd_top, "banned": cmd_banned, "heatmap": cmd_heatmap,
        "benchmark": cmd_benchmark, "report": cmd_report,
        "geo-repair": cmd_geo_repair,
        "sources": cmd_sources, "source-health": cmd_source_health,
        "telegram": cmd_telegram, "freshen": cmd_freshen,
    }
    cmds[args.command](args)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        try:
            sys.stdout.close()
        finally:
            raise SystemExit(0)
