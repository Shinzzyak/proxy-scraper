#!/usr/bin/env python3
"""
report.py — Weekly/Monthly Markdown report generator.

Generates summary reports for cron job output.
"""
import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from proxy_pool import get_db


def generate_weekly_report(output: str = "report-weekly.md") -> str:
    """Generate weekly summary report as Markdown."""
    conn = get_db()
    now = datetime.now(timezone.utc)
    week_ago = (now - timedelta(days=7)).isoformat()

    # Scrape runs this week
    runs = conn.execute(
        "SELECT * FROM scrape_runs WHERE timestamp > ? ORDER BY timestamp",
        (week_ago,)
    ).fetchall()

    # Source history this week
    sources = conn.execute(
        "SELECT source_name, alive, proxy_count, timestamp FROM source_history WHERE timestamp > ? ORDER BY timestamp",
        (week_ago,)
    ).fetchall()

    # Current pool
    pool = conn.execute("""
        SELECT COUNT(*) as total,
            SUM(CASE WHEN protocol='http' THEN 1 ELSE 0 END) as http_count,
            SUM(CASE WHEN protocol='socks5' THEN 1 ELSE 0 END) as socks5_count,
            ROUND(AVG(score), 1) as avg_score,
            COUNT(DISTINCT country_code) as countries
        FROM proxies
    """).fetchone()

    # Usage stats
    usage = conn.execute("""
        SELECT COUNT(*) as total_uses,
            SUM(success) as successes,
            ROUND(AVG(response_time_ms), 0) as avg_ms
        FROM usage_log WHERE timestamp > ?
    """, (week_ago,)).fetchone()

    # Reputation
    try:
        banned = conn.execute("SELECT COUNT(*) as c FROM source_reputation WHERE is_banned = 1").fetchone()["c"]
        top_sources = conn.execute(
            "SELECT source_name, success_rate FROM source_reputation WHERE total_submitted >= 10 ORDER BY success_rate DESC LIMIT 5"
        ).fetchall()
    except Exception:
        banned = 0
        top_sources = []

    conn.close()

    # Build report
    total_runs = len(runs)
    avg_alive = sum(r["total_alive"] for r in runs) // max(total_runs, 1)
    avg_duration = sum(r["duration_s"] for r in runs) / max(total_runs, 1)
    alive_sources = len(set(s["source_name"] for s in sources if s["alive"]))
    dead_sources = len(set(s["source_name"] for s in sources if not s["alive"]))
    total_raw = sum(r["total_raw"] for r in runs)

    pool_total = pool["total"] if pool else 0
    pool_http = pool["http_count"] if pool else 0
    pool_socks5 = pool["socks5_count"] if pool else 0
    pool_score = pool["avg_score"] if pool else 0
    pool_countries = pool["countries"] if pool else 0

    usage_total = (usage["total_uses"] if usage else 0) or 0
    usage_success = (usage["successes"] if usage else 0) or 0
    usage_rate = round(100 * usage_success / max(usage_total, 1), 1)
    usage_avg_ms = (usage["avg_ms"] if usage else 0) or 0

    report = f"""# 📊 Weekly Proxy Report

**Period:** {(now - timedelta(days=7)).strftime('%Y-%m-%d')} → {now.strftime('%Y-%m-%d')}

## Scrape Summary
- **Total runs:** {total_runs}
- **Avg alive per run:** {avg_alive}
- **Avg duration:** {avg_duration:.1f}s
- **Total raw proxies scraped:** {total_raw:,}

## Current Pool
- **Total proxies:** {pool_total}
- **HTTP:** {pool_http} | **SOCKS5:** {pool_socks5}
- **Avg score:** {pool_score}/100
- **Countries:** {pool_countries}

## Source Health
- **Alive sources:** {alive_sources}
- **Dead sources:** {dead_sources}
- **Banned sources:** {banned}

## Top Sources
"""
    for s in top_sources:
        rate = round(s["success_rate"] * 100, 1)
        report += f"- **{s['source_name']}:** {rate}% success\n"

    report += f"""
## Usage (this week)
- **Total requests:** {usage_total}
- **Success rate:** {usage_rate}%
- **Avg response time:** {usage_avg_ms}ms

## Recommendations
"""
    if pool_total < 100:
        report += "- ⚠️ Pool is low (<100 proxies). Consider increasing max-validate.\n"
    if dead_sources > alive_sources:
        report += "- ⚠️ More dead sources than alive. Run --discover-new to find replacements.\n"
    if banned > 5:
        report += f"- ⚠️ {banned} sources banned. Review ban criteria.\n"
    if usage_rate < 80:
        report += "- ⚠️ Usage success rate below 80%. Pool quality may be degraded.\n"
    if not report.endswith("\n\n## Recommendations\n\n"):
        report += "- ✅ All metrics healthy.\n"

    with open(output, "w") as f:
        f.write(report)
    print(f"✅ Weekly report → {output}")
    return report


if __name__ == "__main__":
    report = generate_weekly_report("/tmp/test_report.md")
    print(report[:500])
