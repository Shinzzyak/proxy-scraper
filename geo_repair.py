#!/usr/bin/env python3
"""
geo_repair.py — Repair missing geolocation data in the proxy pool.

Uses scraper.geo_batch_lookup() so the same provider/format powers both live
scrapes and after-the-fact DB enrichment.
"""
import argparse
import json
import time
from typing import Dict, List

from proxy_pool import get_db, update_fingerprints
from scraper import geo_batch_lookup


def _load_json_proxies(path: str) -> List[Dict]:
    try:
        with open(path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
    except FileNotFoundError:
        return []
    except Exception as exc:
        print(f"⚠️ Could not read {path}: {exc}")
    return []


def _export_pool_artifacts() -> None:
    """Refresh JSON/TXT/grouped outputs from current DB state."""
    from scraper import save_json_output, save_grouped_output

    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM proxies ORDER BY score DESC, response_time_ms ASC").fetchall()
        proxies = [dict(r) for r in rows]
    finally:
        conn.close()

    save_json_output(proxies, "proxies.json")
    save_grouped_output(proxies)
    with open("proxies.txt", "w") as f:
        f.write("\n".join(sorted({f"{p['ip']}:{p['port']}" for p in proxies})) + "\n")
    print(f"✅ TXT output → proxies.txt ({len(proxies)} proxies)")


def repair_geo(batch_size: int = 100, limit: int = 0, export: bool = True, json_fallback: str = "proxies.json") -> Dict:
    """Fill missing country/city/ISP for existing DB proxies."""
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT ip, port FROM proxies
            WHERE COALESCE(country_code, '') = '' OR COALESCE(isp, '') = ''
            ORDER BY score DESC, response_time_ms ASC
            """
        ).fetchall()
        missing = [dict(r) for r in rows]

        if limit and limit > 0:
            missing = missing[:limit]

        ips = [r["ip"] for r in missing]
        print(f"🌍 Repairing geo for {len(ips)} proxies...")
        geo = geo_batch_lookup(ips, batch_size=batch_size)

        updated = 0
        for r in missing:
            g = geo.get(r["ip"])
            if not g or not g.get("country_code"):
                continue
            conn.execute(
                """
                UPDATE proxies
                SET country = ?, country_code = ?, city = ?, isp = ?
                WHERE ip = ? AND port = ?
                """,
                (
                    g.get("country", ""),
                    g.get("country_code", ""),
                    g.get("city", ""),
                    g.get("isp", ""),
                    r["ip"],
                    r["port"],
                ),
            )
            updated += 1
        conn.commit()

        # Optional fallback: preserve any geo already present in proxies.json.
        fallback_updates = 0
        if json_fallback:
            json_proxies = _load_json_proxies(json_fallback)
            by_key = {
                (p.get("ip"), int(p.get("port", 0))): p
                for p in json_proxies
                if p.get("ip") and p.get("port") and p.get("country_code")
            }
            for r in missing:
                p = by_key.get((r["ip"], int(r["port"])))
                if not p:
                    continue
                conn.execute(
                    """
                    UPDATE proxies
                    SET country = ?, country_code = ?, city = ?, isp = ?
                    WHERE ip = ? AND port = ? AND COALESCE(country_code, '') = ''
                    """,
                    (
                        p.get("country", ""),
                        (p.get("country_code", "") or "").upper(),
                        p.get("city", ""),
                        p.get("isp", ""),
                        r["ip"],
                        r["port"],
                    ),
                )
                fallback_updates += conn.total_changes
            conn.commit()

        stats = conn.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN COALESCE(country_code, '') != '' THEN 1 ELSE 0 END) as known_geo,
                SUM(CASE WHEN COALESCE(country_code, '') = '' THEN 1 ELSE 0 END) as unknown_geo,
                COUNT(DISTINCT NULLIF(country_code, '')) as countries
            FROM proxies
            """
        ).fetchone()
        result = dict(stats)
        result.update({
            "geo_provider_hits": len(geo),
            "db_updates": updated,
            "json_fallback_updates": fallback_updates,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    finally:
        conn.close()

    update_fingerprints()
    if export:
        _export_pool_artifacts()

    print(
        "✅ Geo repair complete: "
        f"{result['known_geo']}/{result['total']} known geo, "
        f"{result['countries']} countries, {result['unknown_geo']} unknown"
    )
    return result


def main():
    ap = argparse.ArgumentParser(description="Repair missing proxy geolocation in DB")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--limit", type=int, default=0, help="0 = all missing proxies")
    ap.add_argument("--no-export", action="store_true", help="Do not refresh proxies.json/grouped/txt")
    ap.add_argument("--json-fallback", default="proxies.json", help="Use existing JSON as fallback geo source")
    args = ap.parse_args()
    repair_geo(
        batch_size=args.batch_size,
        limit=args.limit,
        export=not args.no_export,
        json_fallback=args.json_fallback,
    )


if __name__ == "__main__":
    main()
