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
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from proxy_pool import get_db
from scraper import validate_single, is_confirmed_proxy

REVALIDATE_WALL_TIMEOUT = 120  # detik
DATA_DIR = Path(__file__).parent / "data"
LOCK_FILE = DATA_DIR / "revalidate_pool.lock"


def acquire_lock() -> bool:
    """P0-2: lock file sendiri (bukan lock freshen) — PID + stale replacement.
    Mencegah dua revalidate tick jalan bareng (double probe + double write)."""
    DATA_DIR.mkdir(exist_ok=True)
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, f"{os.getpid()} {int(time.time())}".encode())
        os.close(fd)
        return True
    except FileExistsError:
        # stale lock (PID mati / >30 menit) → replace
        try:
            pid, ts = map(int, open(LOCK_FILE).read().split())
            if not os.path.exists(f"/proc/{pid}") or time.time() - ts > 1800:
                os.remove(LOCK_FILE)
                return acquire_lock()
        except (ValueError, OSError):
            os.remove(LOCK_FILE)
            return acquire_lock()
        return False


def release_lock():
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def pick_stale_first(budget: int, min_score: int = 1) -> list:
    """Pilih proxy stale-first: last_seen lama dulu, score tinggi prioritas.
    Skip zombie (last_seen='') — sudah dianggap mati.
    R24-P2-1: exclude proxy fresh <30m — baru di-validasi freshen,
    revalidate budget tidak terbuang padanya."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT ip, port, protocol, score FROM proxies
               WHERE last_seen != ''
                 AND source_name != 'manual-light'  -- R43-FIX: proxy user private, JANGAN di-prune
                 AND julianday(last_seen) < julianday('now', '-30 minutes')
                 AND score >= ?
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


def pick_failed_usage(budget: int) -> list:
    """P2-1: proxy yang baru gagal di gateway (fail>=2, 3 hari) — probe ulang.
    Revalidate 'kapan dibutuhkan' bukan 'kapan terjadwal' — gratis dari usage_log."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT p.ip, p.port, p.protocol FROM proxies p
               JOIN (SELECT ip, port, COUNT(*) as fails FROM usage_log
                     WHERE success = 0 AND timestamp >= datetime('now', '-3 days')
                     GROUP BY ip, port HAVING fails >= 2) u
                 ON u.ip = p.ip AND u.port = p.port
               WHERE p.last_seen != '' AND p.score > 0
               ORDER BY u.fails DESC LIMIT ?""",
            (budget,),
        ).fetchall()
        return [(r["ip"], r["port"], r["protocol"]) for r in rows]
    finally:
        conn.close()


def pick_zombies(budget: int) -> list:
    """P2-2: revive zombie (last_seen='') — satu-satunya cara proxy ke-ban balik."""
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT ip, port, protocol FROM proxies
               WHERE last_seen = ''
                 AND source_name != 'manual-light'  -- R43-FIX: proxy user private, JANGAN di-revive/prune
               ORDER BY score DESC LIMIT ?""",
            (budget,),
        ).fetchall()
        return [(r["ip"], r["port"], r["protocol"]) for r in rows]
    finally:
        conn.close()


def revalidate(budget: int = 300, mode: str = "mixed", do_anonymity: bool = False) -> dict:
    """P0-3/P2: probe + ban gagal + revive zombie + usage-fail priority.
    mode: mixed (stale + failed-usage + zombie), stale, usage."""
    t0 = time.time()
    if mode == "stale":
        targets = pick_stale_first(budget)
    elif mode == "usage":
        targets = pick_usage_priority(budget)
    elif mode == "zombie":
        targets = pick_zombies(budget)
    else:  # mixed (default)
        targets = []
        # 1. failed di gateway (P2-1) — 'kapan dibutuhkan'
        targets += pick_failed_usage(budget // 4)
        # 2. zombie (P2-2) — biar bisa balik
        targets += pick_zombies(budget // 4)
        # 3. sisa: stale-first (P1-1)
        targets += pick_stale_first(budget - len(targets))
        # dedupe
        seen = set()
        targets = [t for t in targets if not (t[0], t[1]) in seen and not seen.add((t[0], t[1]))]
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

    # Update DB — satu commit per batch (P1-1: jangan 1 commit/proxy,
    # block request path gateway). P0-3: ban yang gagal (score=0 +
    # last_seen='' → tak ter-pick), bukan cuma bump yang hidup.
    conn = get_db()
    try:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.executemany(
            """UPDATE proxies SET last_seen = ?, score = MIN(100, score + 5)
               WHERE ip = ? AND port = ?""",
            [(now, ip, port) for ip, port in alive],
        )
        conn.executemany(
            """UPDATE proxies SET last_seen = '', score = 0 WHERE ip = ? AND port = ?""",
            [(ip, port) for ip, port in dead],
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
    p.add_argument("--mode", choices=["stale", "usage", "zombie", "mixed"], default="mixed",
                   help="stale: last_seen lama dulu; usage: proxy favorit gateway dulu; "
                        "zombie: revive proxy ke-ban; mixed (default): failed-usage + zombie + stale")
    p.add_argument("--min-score", type=int, default=1)
    p.add_argument("--no-lock", action="store_true", help="skip lock file (debug)")
    p.add_argument("--loop", action="store_true", help="jalankan terus dengan interval")
    p.add_argument("--interval", type=int, default=300, help="detik antar run (dengan --loop)")
    args = p.parse_args()

    if not args.no_lock and not acquire_lock():
        print("⚠ Revalidate sudah jalan (lock), skip", file=sys.stderr)
        sys.exit(2)
    try:
        if args.loop:
            while True:
                revalidate(args.budget, args.mode)
                print(f"💤 sleep {args.interval}s...")
                time.sleep(args.interval)
        else:
            revalidate(args.budget, args.mode)
    finally:
        release_lock()


if __name__ == "__main__":
    main()
