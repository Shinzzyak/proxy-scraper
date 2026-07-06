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
"""
import argparse
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def cmd_search(args):
    from proxy_pool import search_proxies
    results = search_proxies(
        protocol=args.protocol or "",
        country_code=args.country or "",
        min_score=args.min_score or 0,
        anonymity=args.anonymity or "",
        max_results=args.limit,
    )
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for p in results:
            print(f"{p['ip']}:{p['port']} [{p['protocol']}] score={p['score']} {p.get('country_code','')} {p.get('anonymity','')}")
    print(f"\n{len(results)} proxies found")


def cmd_best(args):
    from proxy_pool import get_best_proxy
    proxy = get_best_proxy(args.protocol or "http", args.country or "")
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
    proxies = search_proxies(protocol=args.protocol or "http", max_results=args.limit)
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
    ap = argparse.ArgumentParser(description="Proxy Pool CLI")
    sub = ap.add_subparsers(dest="command")

    # search
    s = sub.add_parser("search")
    s.add_argument("--country", "-c", default="")
    s.add_argument("--protocol", "-p", default="")
    s.add_argument("--min-score", type=int, default=0)
    s.add_argument("--anonymity", "-a", default="")
    s.add_argument("--limit", "-l", type=int, default=20)
    s.add_argument("--json", "-j", action="store_true")

    # best
    b = sub.add_parser("best")
    b.add_argument("--protocol", "-p", default="http")
    b.add_argument("--country", "-c", default="")
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
    }
    cmds[args.command](args)


if __name__ == "__main__":
    main()
