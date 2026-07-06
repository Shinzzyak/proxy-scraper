#!/usr/bin/env python3
"""
reputation.py — Source reputation tracking and auto-ban.

Tracks: success rate, proxy quality, reliability per source.
Auto-bans sources with >30% invalid proxies.
"""
import json
import os
import sqlite3
import time
from typing import Dict, List, Optional
from proxy_pool import get_db

BAN_THRESHOLD = float(os.environ.get("REPUTATION_BAN_THRESHOLD", "0.30"))
WARN_THRESHOLD = float(os.environ.get("REPUTATION_WARN_THRESHOLD", "0.50"))


def init_reputation_table():
    """Create reputation table."""
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS source_reputation (
                source_name TEXT PRIMARY KEY,
                total_submitted INTEGER DEFAULT 0,
                valid_proxies INTEGER DEFAULT 0,
                invalid_proxies INTEGER DEFAULT 0,
                success_rate REAL DEFAULT 1.0,
                avg_score REAL DEFAULT 0.0,
                is_banned INTEGER DEFAULT 0,
                ban_reason TEXT DEFAULT '',
                last_updated TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    finally:
        conn.close()


def update_reputation(source_name: str, submitted: int, valid: int):
    """Update source reputation after a scrape run."""
    init_reputation_table()
    conn = get_db()
    try:
        invalid = submitted - valid
        success_rate = valid / max(submitted, 1)

        # Upsert
        existing = conn.execute(
            "SELECT total_submitted, valid_proxies FROM source_reputation WHERE source_name = ?",
            (source_name,)
        ).fetchone()

        if existing:
            total_sub = existing["total_submitted"] + submitted
            total_valid = existing["valid_proxies"] + valid
            new_rate = total_valid / max(total_sub, 1)
            conn.execute("""
                UPDATE source_reputation SET
                    total_submitted = ?, valid_proxies = ?, invalid_proxies = invalid_proxies + ?,
                    success_rate = ?, last_updated = datetime('now')
                WHERE source_name = ?
            """, (total_sub, total_valid, invalid, new_rate, source_name))
            final_rate = new_rate
        else:
            conn.execute("""
                INSERT INTO source_reputation (source_name, total_submitted, valid_proxies, invalid_proxies, success_rate)
                VALUES (?, ?, ?, ?, ?)
            """, (source_name, submitted, valid, invalid, success_rate))
            final_rate = success_rate

        # Check ban threshold
        if final_rate < BAN_THRESHOLD:
            conn.execute(
                "UPDATE source_reputation SET is_banned = 1, ban_reason = ? WHERE source_name = ?",
                (f"Success rate {final_rate:.1%} < {BAN_THRESHOLD:.0%}", source_name)
            )
            print(f"🚫 BANNED: {source_name} (success rate: {final_rate:.1%})")
        elif final_rate < WARN_THRESHOLD:
            print(f"⚠️  WARNING: {source_name} (success rate: {final_rate:.1%})")

        conn.commit()
    finally:
        conn.close()


def get_reputation(source_name: str) -> Optional[Dict]:
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT * FROM source_reputation WHERE source_name = ?",
            (source_name,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_all_reputations() -> List[Dict]:
    conn = get_db()
    try:
        rows = conn.execute("SELECT * FROM source_reputation ORDER BY success_rate DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def is_banned(source_name: str) -> bool:
    rep = get_reputation(source_name)
    return rep["is_banned"] if rep else False


def get_banned_sources() -> List[Dict]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT * FROM source_reputation WHERE is_banned = 1 ORDER BY success_rate"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_top_sources(limit: int = 10) -> List[Dict]:
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT * FROM source_reputation
            WHERE total_submitted >= 10
            ORDER BY success_rate DESC, avg_score DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def export_reputation_report(output: str = "reputation.json"):
    reps = get_all_reputations()
    banned = get_banned_sources()
    top = get_top_sources(10)
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_sources": len(reps),
        "banned_sources": len(banned),
        "top_sources": top,
        "banned": banned,
        "all": reps,
    }
    with open(output, "w") as f:
        json.dump(report, f, indent=2)
    print(f"✅ Reputation report → {output} ({len(reps)} sources, {len(banned)} banned)")
