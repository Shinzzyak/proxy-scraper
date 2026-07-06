#!/usr/bin/env python3
"""
session_manager.py — Sticky session with auto-rotation.

Tracks proxy assignments per client, auto-rotates after TTL.
"""
import json
import os
import time
from typing import Dict, List, Optional
from proxy_pool import get_db

SESSION_TTL_MINUTES = int(os.environ.get("SESSION_TTL", "10"))
MAX_SESSIONS = int(os.environ.get("MAX_SESSIONS", "100"))


def get_sticky_proxy(client_id: str, protocol: str = "http",
                     country_code: str = "", ttl_minutes: int = 0) -> Optional[Dict]:
    """Get a sticky proxy for client. Rotates after TTL."""
    if ttl_minutes <= 0:
        ttl_minutes = SESSION_TTL_MINUTES

    conn = get_db()
    try:
        # Check existing session
        row = conn.execute("""
            SELECT ip, port, assigned_at FROM sessions
            WHERE client_id = ? AND protocol = ?
            ORDER BY assigned_at DESC LIMIT 1
        """, (client_id, protocol)).fetchone()

        if row:
            assigned = time.mktime(time.strptime(row["assigned_at"], "%Y-%m-%dT%H:%M:%SZ"))
            age_minutes = (time.time() - assigned) / 60
            if age_minutes < ttl_minutes:
                # Still valid — return same proxy
                proxy = conn.execute(
                    "SELECT * FROM proxies WHERE ip = ? AND port = ?",
                    (row["ip"], row["port"])
                ).fetchone()
                if proxy:
                    return dict(proxy)

        # Get new proxy
        q = "SELECT * FROM proxies WHERE protocol = ?"
        params = [protocol]
        if country_code:
            q += " AND country_code = ?"
            params.append(country_code)
        q += " ORDER BY score DESC, response_time_ms ASC LIMIT 1"
        proxy = conn.execute(q, params).fetchone()
        if not proxy:
            return None

        # Record session
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute("""
            INSERT INTO sessions (client_id, ip, port, protocol, assigned_at)
            VALUES (?, ?, ?, ?, ?)
        """, (client_id, proxy["ip"], proxy["port"], protocol, now))
        conn.commit()
        return dict(proxy)
    finally:
        conn.close()


def cleanup_expired_sessions():
    """Remove sessions older than 24h."""
    conn = get_db()
    try:
        conn.execute("""
            DELETE FROM sessions WHERE assigned_at < datetime('now', '-24 hours')
        """)
        conn.commit()
    finally:
        conn.close()


def get_active_sessions() -> List[Dict]:
    """List active sessions."""
    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT client_id, ip, port, protocol, assigned_at FROM sessions
            WHERE assigned_at > datetime('now', '-24 hours')
            ORDER BY assigned_at DESC
        """).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def init_sessions_table():
    """Create sessions table if not exists."""
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id TEXT NOT NULL,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                protocol TEXT DEFAULT 'http',
                assigned_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_sessions_table()
    p = get_sticky_proxy("test-client", "http")
    print(f"Sticky proxy: {p}")
    sessions = get_active_sessions()
    print(f"Active sessions: {len(sessions)}")
