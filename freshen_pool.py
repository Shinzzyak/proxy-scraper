#!/usr/bin/env python3
"""
freshen_pool.py — Scheduled Proxy Pool Refresh

Runs the maintenance cycle:
1. Scrape configured sources
2. Validate a bounded sample
3. Update pool DB + source health
4. Repair missing geolocation
5. Scrape Telegram channels and merge alive proxies
6. Export fresh JSON/TXT reports

Designed for cron/systemd/OpenClaw cron jobs.
Uses a lock file to prevent overlapping runs.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
LOCK_FILE = DATA_DIR / "freshen_pool.lock"
STATE_FILE = DATA_DIR / "freshen_pool_state.json"
PYTHON = sys.executable or "python3"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_state(state: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def acquire_lock(max_age_minutes: int = 180) -> bool:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_FILE.exists():
        try:
            data = json.loads(LOCK_FILE.read_text())
            started = data.get("started_at_epoch", 0)
            age = time.time() - float(started)
            if age < max_age_minutes * 60:
                print(f"⚠️ Freshen already running (lock age {age/60:.1f}m): {LOCK_FILE}")
                return False
            print(f"⚠️ Stale lock found ({age/60:.1f}m), replacing")
        except Exception:
            print("⚠️ Broken lock file, replacing")
    LOCK_FILE.write_text(json.dumps({
        "pid": os.getpid(),
        "started_at": utc_now(),
        "started_at_epoch": time.time(),
    }, indent=2))
    return True


def release_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def run_cmd(cmd: list, timeout: int = 1800) -> tuple[int, str]:
    """Run a command, stream output, return (code, combined_output)."""
    print(f"\n$ {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "PROXY_DB": str(DATA_DIR / "proxies.db")},
    )
    output = []
    started = time.time()
    try:
        for line in proc.stdout:
            print(line, end="")
            output.append(line)
            if time.time() - started > timeout:
                proc.kill()
                output.append("\n[TIMEOUT]\n")
                return 124, "".join(output)
        code = proc.wait(timeout=10)
        return code, "".join(output)
    except subprocess.TimeoutExpired:
        proc.kill()
        output.append("\n[TIMEOUT]\n")
        return 124, "".join(output)


def get_pool_stats() -> dict:
    sys.path.insert(0, str(ROOT))
    from proxy_pool import get_db
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT NULLIF(country_code, '')) as countries,
                COUNT(DISTINCT NULLIF(source_name, '')) as sources,
                SUM(CASE WHEN COALESCE(country_code, '') = '' THEN 1 ELSE 0 END) as missing_geo,
                ROUND(AVG(score), 1) as avg_score,
                ROUND(AVG(response_time_ms), 0) as avg_rt
            FROM proxies
        """).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def export_pool_snapshots(max_age_minutes: int = 1440) -> int:
    """Export GitHub-friendly snapshots from the local DB.

    `scraper.py` exports the validator batch before Telegram is merged. Running this
    at the end ensures `proxies.txt/json` reflect the final pool state seen by
    consumers and other agents.
    """
    sys.path.insert(0, str(ROOT))
    from proxy_pool import search_proxies

    proxies = search_proxies(
        min_score=0,
        max_results=200000,
        max_age_minutes=max_age_minutes,
    )
    if not proxies and max_age_minutes:
        proxies = search_proxies(min_score=0, max_results=200000, max_age_minutes=0)

    proxies = sorted(
        proxies,
        key=lambda p: (-int(p.get("score") or 0), int(p.get("response_time_ms") or 999999), p.get("ip", "")),
    )

    (ROOT / "proxies.txt").write_text("\n".join(f"{p['ip']}:{p['port']}" for p in proxies) + ("\n" if proxies else ""))
    (ROOT / "proxies.json").write_text(json.dumps(proxies, indent=2))

    by_country = {}
    by_protocol = {}
    by_anonymity = {}
    for p in proxies:
        by_country.setdefault(p.get("country_code") or "UNKNOWN", []).append(p)
        by_protocol.setdefault(p.get("protocol") or "unknown", []).append(p)
        by_anonymity[p.get("anonymity") or "unknown"] = by_anonymity.get(p.get("anonymity") or "unknown", 0) + 1

    (ROOT / "proxies-by-country.json").write_text(json.dumps(by_country, indent=2))
    (ROOT / "proxies-by-protocol.json").write_text(json.dumps(by_protocol, indent=2))

    stats = {
        "generated_at": utc_now(),
        "source": "pool-db",
        "max_age_minutes": max_age_minutes,
        "total": len(proxies),
        "by_country": {k: len(v) for k, v in sorted(by_country.items())},
        "by_protocol": {k: len(v) for k, v in sorted(by_protocol.items())},
        "by_anonymity": by_anonymity,
        "avg_response_time_ms": round(sum(int(p.get("response_time_ms") or 0) for p in proxies) / len(proxies), 1) if proxies else 0,
        "avg_score": round(sum(int(p.get("score") or 0) for p in proxies) / len(proxies), 1) if proxies else 0,
    }
    (ROOT / "proxies-stats.json").write_text(json.dumps(stats, indent=2))
    print(f"✅ Final pool snapshots exported: {len(proxies)} proxies")
    return len(proxies)


def main():
    ap = argparse.ArgumentParser(description="Scheduled proxy pool refresh")
    ap.add_argument("--max-validate", type=int, default=3000, help="Max proxies to validate per scrape")
    ap.add_argument("--telegram", action="store_true", help="Also scrape Telegram channels")
    ap.add_argument("--telegram-pages", type=int, default=3, help="Telegram pages per channel")
    ap.add_argument("--geo-only", action="store_true", help="Only repair missing geolocation")
    ap.add_argument("--scrape-only", action="store_true", help="Only run scraper, no Telegram")
    ap.add_argument("--export-max-age-minutes", type=int, default=1440, help="Freshness window for final DB snapshot export; 0 means no freshness filter; -1 disables export")
    ap.add_argument("--no-lock", action="store_true", help="Disable lock file")
    ap.add_argument("--log", action="store_true", help="Write log file under logs/")
    args = ap.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()
    start = time.time()
    started_at = utc_now()

    if not args.no_lock and not acquire_lock():
        sys.exit(2)

    status = "success"
    errors = []
    before = {}
    after = {}

    try:
        before = get_pool_stats()
        print(f"\n🚀 Freshen started: {started_at}")
        print(f"Before: {before}")

        if args.geo_only:
            code, out = run_cmd([PYTHON, "geo_repair.py", "--limit", "5000"], timeout=1200)
            if code != 0:
                status = "failed"
                errors.append(f"geo_repair exited {code}")
        else:
            # 1) Main scraper with validation + pool update + reports
            cmd = [
                PYTHON, "scraper.py",
                "--validate", "--pool", "--json", "--grouped", "--health",
                "--max-validate", str(args.max_validate),
            ]
            code, out = run_cmd(cmd, timeout=2400)
            if code != 0:
                status = "partial"
                errors.append(f"scraper exited {code}")

            # 2) Geo repair any missing geolocation after scrape
            code, out = run_cmd([PYTHON, "geo_repair.py", "--limit", "5000"], timeout=1200)
            if code != 0:
                status = "partial"
                errors.append(f"geo_repair exited {code}")

            # 3) Telegram scrape (optional)
            if args.telegram and not args.scrape_only:
                code, out = run_cmd([
                    PYTHON, "tg_scraper.py",
                    "--pages", str(args.telegram_pages),
                    "--add-to-pool",
                ], timeout=1800)
                if code != 0:
                    status = "partial"
                    errors.append(f"tg_scraper exited {code}")

            # 4) Export final snapshots from DB after all sources (including Telegram) merged.
            if args.export_max_age_minutes >= 0:
                export_pool_snapshots(max_age_minutes=args.export_max_age_minutes)

        after = get_pool_stats()
        duration = round(time.time() - start, 1)
        print(f"\n✅ Freshen finished: {utc_now()} ({duration}s)")
        print(f"After: {after}")

        state.update({
            "last_run_started_at": started_at,
            "last_run_finished_at": utc_now(),
            "last_status": status,
            "last_duration_s": duration,
            "last_errors": errors,
            "before": before,
            "after": after,
        })
        save_state(state)

        if errors:
            print("\n⚠️ Errors:")
            for e in errors:
                print(f"  - {e}")
        sys.exit(0 if status == "success" else 1)

    finally:
        if not args.no_lock:
            release_lock()


if __name__ == "__main__":
    main()
