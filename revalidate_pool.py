#!/usr/bin/env python3
"""Revalidate pool — probe ulang proxy existing tanpa nunggu freshen 6 jam.

Kenapa: free proxy list churn tinggi. 81% pool (3-6 jam) kemungkinan mati
sebelum freshen berikutnya. Revalidate = probe ulang stale-first dengan
budget incremental (bukan semua 2744 sekaligus).

Alur:
1. Pilih proxy stale-first: last_seen paling lama + score tinggi dulu
   (proxy bagus yang tua > proxy jelek yang muda).
2. Probe via validate_single (TCP + protocol detect + response time).
3. Update DB: hidup → last_seen=now, score=min(100, score+5); mati →
   last_seen='' (zombie — tidak ter-pick gateway/search) atau hapus.
4. Usage-based: proxy yang sering dipakai gateway (usage_log) dapat
   prioritas revalidate — gateway butuh yang fresh.

Budget default 300 per run — VPS 3GB, 200 workers parallel, ~30-60s.
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from proxy_pool import get_db
from scraper import validate_single, is_confirmed_proxy

REVALIDATE_WALL_TIMEOUT = 120  # detik


def pick_stale_first(budget: int, min_score: int = 1) -> list:
    """Pilih proxy stale-first: last_seen lama dulu, score tinggi prioritas.
    Skip zombie (last_seen='') — sudah dianggap mati."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT ip, port, protocol, score FROM proxies
               WHERE last_seen != '' AND score >= ?
               ORDER BY julianday(last_seen) ASC, score DESC, response_time_ms ASC
               LIMIT ?""",
            (min_score, budget),
        ).fetchall()
        return [(r["ip"], r["port"], r["protocol"]) for r in rows]
    finally:
        conn.close()


def pick_usage_priority(budget: int) -> list:
    """Proxy yang sering dipakai gateway (usage_log success) — fresh dulu."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT p.ip, p.port, p.protocol FROM proxies p
               JOIN (SELECT ip, port, COUNT(*) as cnt FROM usage_log
                     WHERE success = 1 GROUP BY ip, port ORDER BY cnt DESC LIMIT ?) u
                 ON u.ip = p.ip AND u.port = p.port
               WHERE p.last_seen != ''
               ORDER BY u.cnt DESC LIMIT ?""",
            (budget, budget),
        ).fetchall()
        return [(r["ip"], r["port"], r["protocol"]) for r in rows]
    finally:
        conn.close()


def revalidate(budget: int = 300, mode: str = "stale", do_anonymity: bool = False) -> dict:
    t0 = time.time()
    if mode == "usage":
        targets = pick_usage_priority(budget)
    else:
        targets = pick_stale_first(budget)
    if not targets:
        print("No proxies to revalidate (pool empty?)")
        return {"tested": 0, "alive": 0, "seconds": 0}

    print(f"🔍 Revalidating {len(targets)} proxies ({mode}-first, budget {budget})...")
    alive = []
    dead = []
    pool = ThreadPoolExecutor(max_workers=200)
    futs = {
        pool.submit(validate_single, f"{ip}:{port}", do_anonymity, proto): (ip, port)
        for ip, port, proto in targets
    }
    try:
        for fut in as_completed(futs, timeout=REVALIDATE_WALL_TIMEOUT):
            result = fut.result()
            ip, port = futs[fut]
            if result and is_confirmed_proxy(result):
                alive.append((ip, port))
            else:
                dead.append((ip, port))
    except TimeoutError:
        pending = sum(1 for f in futs if not f.done())
        for f in futs:
            if not f.done():
                f.cancel()
        print(f"  ⚠ deadline {REVALIDATE_WALL_TIMEOUT}s; {pending} pending cancelled", file=sys.stderr)
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    # Update DB
    conn = get_db()
    try:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for ip, port in alive:
            conn.execute(
                """UPDATE proxies SET last_seen = ?, score = MIN(100, score + 5),
                   response_time_ms = CASE WHEN response_time_ms = 0 THEN 100 ELSE response_time_ms END
                   WHERE ip = ? AND port = ?""",
                (now, ip, port),
            )
        for ip, port in dead:
            # zombie: last_seen='' → tidak ter-pick gateway/search (F8-8)
            conn.execute(
                "UPDATE proxies SET last_seen = '' WHERE ip = ? AND port = ?",
                (ip, port),
            )
        conn.commit()
    finally:
        conn.close()

    dt = time.time() - t0
    print(f"📊 {len(alive)}/{len(targets)} alive ({len(alive)/max(len(targets),1):.0%}) | {dt:.1f}s")
    return {"tested": len(targets), "alive": len(alive), "dead": len(dead), "seconds": dt}


def main():
    p = argparse.ArgumentParser(description="Revalidate pool proxies (stale-first)")
    p.add_argument("--budget", type=int, default=300, help="max proxies per run")
    p.add_argument("--mode", choices=["stale", "usage"], default="stale",
                   help="stale: last_seen lama dulu; usage: proxy favorit gateway dulu")
    p.add_argument("--min-score", type=int, default=1)
    p.add_argument("--loop", action="store_true", help="jalankan terus dengan interval")
    p.add_argument("--interval", type=int, default=300, help="detik antar run (dengan --loop)")
    args = p.parse_args()

    if args.loop:
        while True:
            revalidate(args.budget, args.mode)
            print(f"💤 sleep {args.interval}s...")
            time.sleep(args.interval)
    else:
        revalidate(args.budget, args.mode)


if __name__ == "__main__":
    main()
