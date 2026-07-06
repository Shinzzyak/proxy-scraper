#!/usr/bin/env python3
"""
proxy_pool.py — Proxy Pool Manager with SQLite backend

Features:
- Usage Tracking: log every proxy use, success/fail, response time
- Uptime Tracking: historical source health, predict source death
- Proxy Pool: best proxy by country/protocol/score
- Fingerprinting: datacenter vs residential detection
- Sticky Sessions: group by ASN/ISP
- Quality Metrics: p50/p95/p99 response times, success rates
"""
import json
import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple

DB_PATH = os.environ.get("PROXY_DB", str(Path(__file__).parent / "data" / "proxies.db"))


def get_db() -> sqlite3.Connection:
    """Get database connection, create tables if needed."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _create_tables(conn)
    return conn


def _create_tables(conn: sqlite3.Connection):
    """Create schema if not exists."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS proxies (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            protocol TEXT DEFAULT 'http',
            score INTEGER DEFAULT 0,
            anonymity TEXT DEFAULT 'unknown',
            country TEXT DEFAULT '',
            country_code TEXT DEFAULT '',
            city TEXT DEFAULT '',
            isp TEXT DEFAULT '',
            response_time_ms INTEGER DEFAULT 0,
            last_seen TEXT DEFAULT '',
            first_seen TEXT DEFAULT '',
            is_datacenter INTEGER DEFAULT -1,
            PRIMARY KEY (ip, port)
        );

        CREATE TABLE IF NOT EXISTS usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            success INTEGER NOT NULL,
            response_time_ms INTEGER DEFAULT 0,
            error TEXT DEFAULT '',
            timestamp TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS source_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            alive INTEGER NOT NULL,
            proxy_count INTEGER DEFAULT 0,
            timestamp TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS scrape_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_raw INTEGER DEFAULT 0,
            total_alive INTEGER DEFAULT 0,
            total_sources INTEGER DEFAULT 0,
            alive_sources INTEGER DEFAULT 0,
            duration_s REAL DEFAULT 0,
            timestamp TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_usage_ip ON usage_log(ip, port);
        CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage_log(timestamp);
        CREATE INDEX IF NOT EXISTS idx_source_history_name ON source_history(source_name, timestamp);
    """)
    conn.commit()


# ── Usage Tracking ──────────────────────────────────────────────────────

def log_usage(ip: str, port: int, success: bool, response_time_ms: int = 0, error: str = ""):
    """Log a proxy usage event."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO usage_log (ip, port, success, response_time_ms, error) VALUES (?, ?, ?, ?, ?)",
            (ip, port, int(success), response_time_ms, error)
        )
        conn.commit()
    finally:
        conn.close()


def get_proxy_stats(ip: str, port: int) -> Dict:
    """Get usage statistics for a specific proxy."""
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) as total_uses,
                SUM(success) as successes,
                ROUND(100.0 * SUM(success) / MAX(COUNT(*), 1), 1) as success_rate,
                ROUND(AVG(response_time_ms), 0) as avg_response_ms,
                MIN(response_time_ms) as min_response_ms,
                MAX(response_time_ms) as max_response_ms
            FROM usage_log WHERE ip = ? AND port = ?
        """, (ip, port)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_usage_leaderboard(limit: int = 20) -> List[Dict]:
    """Top proxies by usage and success rate."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT
                ip, port,
                COUNT(*) as total_uses,
                SUM(success) as successes,
                ROUND(100.0 * SUM(success) / MAX(COUNT(*), 1), 1) as success_rate,
                ROUND(AVG(response_time_ms), 0) as avg_ms
            FROM usage_log
            GROUP BY ip, port
            HAVING total_uses >= 3
            ORDER BY success_rate DESC, total_uses DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Uptime Tracking ─────────────────────────────────────────────────────

def log_source_health(source_name: str, alive: bool, proxy_count: int):
    """Record source health snapshot."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO source_history (source_name, alive, proxy_count) VALUES (?, ?, ?)",
            (source_name, int(alive), proxy_count)
        )
        conn.commit()
    finally:
        conn.close()


def get_source_uptime(source_name: str, hours: int = 72) -> Dict:
    """Get source uptime stats for last N hours."""
    conn = get_db()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        row = conn.execute("""
            SELECT
                COUNT(*) as total_checks,
                SUM(alive) as alive_checks,
                ROUND(100.0 * SUM(alive) / MAX(COUNT(*), 1), 1) as uptime_pct,
                ROUND(AVG(proxy_count), 0) as avg_proxies
            FROM source_history
            WHERE source_name = ? AND timestamp > ?
        """, (source_name, cutoff)).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def predict_source_death(source_name: str, window_hours: int = 72) -> Optional[Dict]:
    """Predict when source will die based on trend."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT alive, timestamp FROM source_history
            WHERE source_name = ?
            ORDER BY timestamp DESC LIMIT 12
        """, (source_name,)).fetchall()
        if len(rows) < 3:
            return None

        # Trend: consecutive failures
        failures = 0
        for r in rows:
            if not r["alive"]:
                failures += 1
            else:
                break

        total = len(rows)
        alive_rate = sum(1 for r in rows if r["alive"]) / total

        # Predict: if trend is downward and alive_rate < 50%, "at risk"
        at_risk = alive_rate < 0.5 and failures >= 2
        estimated_hours_left = None
        if at_risk:
            # Simple: failures / total * 72h window
            estimated_hours_left = round((1 - alive_rate) * window_hours, 1) if alive_rate > 0 else 0

        return {
            "source": source_name,
            "alive_rate": round(alive_rate * 100, 1),
            "recent_failures": failures,
            "at_risk": at_risk,
            "estimated_hours_left": estimated_hours_left,
        }
    finally:
        conn.close()


# ── Proxy Pool ──────────────────────────────────────────────────────────

def upsert_proxy(proxy_dict: Dict, source: str = ""):
    """Insert or update proxy in pool. Optional source attribution."""
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO proxies (ip, port, protocol, score, anonymity, country, country_code, city, isp, response_time_ms, last_seen, first_seen, source_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (ip, port) DO UPDATE SET
                protocol=excluded.protocol, score=excluded.score, anonymity=excluded.anonymity,
                country=excluded.country, country_code=excluded.country_code, city=excluded.city,
                isp=excluded.isp, response_time_ms=excluded.response_time_ms, last_seen=excluded.last_seen,
                source_name = CASE WHEN excluded.source_name != '' THEN excluded.source_name ELSE proxies.source_name END
        """, (
            proxy_dict["ip"], proxy_dict["port"],
            proxy_dict.get("protocol", "http"),
            proxy_dict.get("score", 0),
            proxy_dict.get("anonymity", "unknown"),
            proxy_dict.get("country", ""),
            proxy_dict.get("country_code", ""),
            proxy_dict.get("city", ""),
            proxy_dict.get("isp", ""),
            proxy_dict.get("response_time_ms", 0),
            proxy_dict.get("last_seen", ""),
            proxy_dict.get("last_seen", ""),
            source or proxy_dict.get("source_name", ""),
        ))
        conn.commit()
    finally:
        conn.close()


def get_best_proxy(protocol: str = "http", country_code: str = "") -> Optional[Dict]:
    """Get best available proxy by criteria."""
    conn = get_db()
    try:
        q = "SELECT * FROM proxies WHERE protocol = ?"
        params = [protocol]
        if country_code:
            q += " AND country_code = ?"
            params.append(country_code)
        q += " ORDER BY score DESC, response_time_ms ASC LIMIT 1"
        row = conn.execute(q, params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_sticky_group(isp: str = "", country_code: str = "") -> List[Dict]:
    """Get proxies grouped by ISP/ASN for sticky sessions."""
    conn = get_db()
    try:
        q = "SELECT * FROM proxies WHERE isp != ''"
        params = []
        if isp:
            q += " AND isp = ?"
            params.append(isp)
        if country_code:
            q += " AND country_code = ?"
            params.append(country_code)
        q += " ORDER BY score DESC LIMIT 20"
        rows = conn.execute(q, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ── Fingerprinting ──────────────────────────────────────────────────────

DATACENTER_ASNS = {
    "Amazon", "AWS", "Google", "Microsoft", "Azure", "Cloudflare",
    "DigitalOcean", "Vultr", "OVH", "Hetzner", "Linode", "Alibaba",
    "Tencent", "Oracle", "IBM", "Rackspace", "Equinix", "CoreSite",
}

def detect_fingerprint(isp: str) -> Dict:
    """Detect proxy fingerprint: datacenter vs residential."""
    is_datacenter = False
    for asn in DATACENTER_ASNS:
        if asn.lower() in isp.lower():
            is_datacenter = True
            break
    return {
        "is_datacenter": is_datacenter,
        "is_residential": not is_datacenter and isp != "",
        "confidence": "high" if isp else "low",
    }


def update_fingerprints():
    """Batch update fingerprints for all proxies."""
    conn = get_db()
    try:
        rows = conn.execute("SELECT ip, port, isp FROM proxies").fetchall()
        for r in rows:
            fp = detect_fingerprint(r["isp"])
            conn.execute(
                "UPDATE proxies SET is_datacenter = ? WHERE ip = ? AND port = ?",
                (int(fp["is_datacenter"]), r["ip"], r["port"])
            )
        conn.commit()
        print(f"✅ Updated fingerprints for {len(rows)} proxies")
    finally:
        conn.close()


# ── Quality Metrics ─────────────────────────────────────────────────────

def get_quality_metrics(ip: str, port: int) -> Dict:
    """Get quality metrics: p50/p95/p99 response times."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT response_time_ms FROM usage_log
            WHERE ip = ? AND port = ? AND success = 1 AND response_time_ms > 0
            ORDER BY response_time_ms
        """, (ip, port)).fetchall()
        times = [r["response_time_ms"] for r in rows]
        if not times:
            return {"p50": 0, "p95": 0, "p99": 0, "samples": 0}

        n = len(times)
        return {
            "p50": times[n // 2],
            "p95": times[int(n * 0.95)] if n > 1 else times[0],
            "p99": times[int(n * 0.99)] if n > 1 else times[0],
            "samples": n,
        }
    finally:
        conn.close()


def log_scrape_run(total_raw: int, total_alive: int, total_sources: int, alive_sources: int, duration_s: float):
    """Log a scrape run for historical tracking."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO scrape_runs (total_raw, total_alive, total_sources, alive_sources, duration_s) VALUES (?, ?, ?, ?, ?)",
            (total_raw, total_alive, total_sources, alive_sources, round(duration_s, 2))
        )
        conn.commit()
    finally:
        conn.close()


# ── Export ──────────────────────────────────────────────────────────────

def export_pool_json(output: str = "pool.json"):
    """Export current pool to JSON."""
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM proxies ORDER BY score DESC").fetchall()
        pool = [dict(r) for r in rows]
        with open(output, "w") as f:
            json.dump(pool, f, indent=2)
        print(f"✅ Pool export → {output} ({len(pool)} proxies)")
    finally:
        conn.close()


def get_pool_stats() -> Dict:
    """Get overall pool statistics."""
    conn = get_db()
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN protocol='http' THEN 1 ELSE 0 END) as http_count,
                SUM(CASE WHEN protocol='socks5' THEN 1 ELSE 0 END) as socks5_count,
                ROUND(AVG(score), 1) as avg_score,
                ROUND(AVG(response_time_ms), 0) as avg_rt,
                COUNT(DISTINCT NULLIF(country_code, '')) as countries,
                COUNT(DISTINCT NULLIF(isp, '')) as isps,
                SUM(CASE WHEN COALESCE(country_code, '') = '' THEN 1 ELSE 0 END) as unknown_country_count,
                SUM(CASE WHEN COALESCE(country_code, '') != '' THEN 1 ELSE 0 END) as known_geo_count,
                ROUND(100.0 * SUM(CASE WHEN COALESCE(country_code, '') != '' THEN 1 ELSE 0 END) / MAX(COUNT(*), 1), 1) as geo_coverage_pct,
                SUM(CASE WHEN is_datacenter=1 THEN 1 ELSE 0 END) as datacenter_count,
                SUM(CASE WHEN is_datacenter=0 THEN 1 ELSE 0 END) as residential_count
            FROM proxies
        """).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def search_proxies(protocol: str = "", country_code: str = "", min_score: int = 0,
                    anonymity: str = "", max_results: int = 50) -> List[Dict]:
    """Search proxies by criteria."""
    conn = get_db()
    try:
        q = "SELECT * FROM proxies WHERE 1=1"
        params = []
        if protocol:
            q += " AND protocol = ?"
            params.append(protocol)
        if country_code:
            q += " AND country_code = ?"
            params.append(country_code.upper())
        if min_score > 0:
            q += " AND score >= ?"
            params.append(min_score)
        if anonymity:
            q += " AND anonymity = ?"
            params.append(anonymity)
        q += " ORDER BY score DESC, response_time_ms ASC LIMIT ?"
        params.append(max_results)
        rows = conn.execute(q, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def dedup_proxies(proxies: List[Dict]) -> List[Dict]:
    """Remove duplicates by IP:port, keep highest score."""
    seen = {}
    for p in proxies:
        key = f"{p['ip']}:{p['port']}"
        if key not in seen or p.get("score", 0) > seen[key].get("score", 0):
            seen[key] = p
    return sorted(seen.values(), key=lambda x: x.get("score", 0), reverse=True)


def export_fresh_txt(proxies: List[Dict], max_age_minutes: int = 30, output: str = "proxies-fresh.txt"):
    """Export only proxies seen within max_age_minutes."""
    import datetime
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=max_age_minutes)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    fresh = [p for p in proxies if p.get("last_seen", "") >= cutoff_str]
    lines = sorted(set(f"{p['ip']}:{p['port']}" for p in fresh))
    with open(output, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ Fresh proxies → {output} ({len(fresh)} proxies, <{max_age_minutes}min)")
    return fresh


def export_rotate_txt(proxies: List[Dict], output: str = "proxy-rotate.txt"):
    """Export best proxy per protocol for auto-rotation."""
    best = {}
    for p in proxies:
        proto = p.get("protocol", "http")
        if proto not in best or p.get("score", 0) > best[proto].get("score", 0):
            best[proto] = p
    lines = []
    for proto, p in sorted(best.items()):
        lines.append(f"{p['ip']}:{p['port']}")
    with open(output, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"✅ Rotate proxies → {output} ({len(lines)} protocols)")
    return best


if __name__ == "__main__":
    # Quick test
    db = get_db()
    print(f"DB: {DB_PATH}")
    stats = get_pool_stats()
    print(f"Pool: {json.dumps(stats, indent=2)}")
    lb = get_usage_leaderboard(5)
    print(f"Leaderboard: {json.dumps(lb, indent=2)}")
