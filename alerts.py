#!/usr/bin/env python3
"""
alerts.py — Telegram alerts for proxy scraper.

Sends notifications when:
- Alive proxies drop below threshold
- Scrape completes (optional)
- Source dies (optional)
"""
import json
import os
import urllib.request
import urllib.parse
from typing import Optional


def send_telegram(message: str, bot_token: str = "", chat_id: str = "") -> bool:
    """Send message via Telegram Bot API."""
    if not bot_token:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not chat_id:
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        print(f"⚠️  Telegram not configured. Message: {message}")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    data = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode()

    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        result = json.loads(resp.read().decode())
        return result.get("ok", False)
    except Exception as e:
        print(f"⚠️  Telegram send failed: {e}")
        return False


def alert_low_proxies(count: int, threshold: int = 50, top_proxies: str = "") -> bool:
    """Alert when alive proxies drop below threshold."""
    msg = f"⚠️ <b>Proxy Alert</b>\n\nOnly <b>{count}</b> alive proxies (threshold: {threshold})\n\n"
    if top_proxies:
        msg += f"Top proxies:\n{top_proxies}\n\n"
    msg += "Action: Check source health or increase max-validate."
    return send_telegram(msg)


def alert_scrape_complete(total: int, alive: int, sources: int, duration: float, top_proxies: str = "") -> bool:
    """Alert on scrape completion."""
    msg = f"✅ <b>Scrape Complete</b>\n\n"
    msg += f"Raw: {total:,}\nAlive: {alive}\nSources: {sources}\nDuration: {duration:.1f}s\n\n"
    if top_proxies:
        msg += f"Top proxies:\n{top_proxies}"
    return send_telegram(msg)


def alert_source_death(source: str, alive_rate: float) -> bool:
    """Alert when a source is dying."""
    msg = f"💀 <b>Source Dying</b>\n\nSource: <code>{source}</code>\nAlive rate: {alive_rate:.1f}%\n\n"
    msg += "Action: Consider removing or investigating this source."
    return send_telegram(msg)


if __name__ == "__main__":
    # Test
    send_telegram("🧪 Proxy Scraper alert test — ignore this message.")
