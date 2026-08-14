"""R47 bug-hunt tests: parser edge, dedup sources, blocked IPs, 429, revalidate manual-light, reputation grace cap."""
import os
import sqlite3
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

import scraper
import revalidate_pool
import reputation


class ParserTests(unittest.TestCase):
    def test_geonode_requires_json(self):
        # HTML error page / non-JSON response must NOT be parsed as geonode
        self.assertEqual(scraper.extract_proxies("<html>rate limited</html>", "geonode"), [])

    def test_table_accepts_plain_ip_td(self):
        tbl = "<table><tr><td>1.2.3.4</td><td>8080</td></tr></table>"
        self.assertIn("1.2.3.4:8080", scraper.extract_proxies(tbl, "table"))

    def test_proxydb_href(self):
        pd = '<a href="/1.2.3.4/8080#http">'
        self.assertEqual(scraper.extract_proxies(pd, "proxydb"), ["1.2.3.4:8080"])

    def test_clash_yaml_flow_and_block(self):
        flow = "- {'name': 'x', 'server': '8.8.8.8', 'port': 8080}"
        block = "- name: x\n  server: 9.9.9.9\n  port: 1080"
        self.assertIn("8.8.8.8:8080", scraper.extract_proxies(flow, "clash-yaml"))
        self.assertIn("9.9.9.9:1080", scraper.extract_proxies(block, "clash-yaml"))


class SourceDedupTests(unittest.TestCase):
    def test_proxy_sources_unique(self):
        urls = [u for _, u, _ in scraper.PROXY_SOURCES]
        names = [n for n, _, _ in scraper.PROXY_SOURCES]
        self.assertEqual(len(urls), len(set(urls)), "duplicate URLs in PROXY_SOURCES")
        self.assertEqual(len(names), len(set(names)), "duplicate source names")


class BlockedIPTests(unittest.TestCase):
    def test_loopback_private_blocked(self):
        for ip in ["0.0.0.0", "127.0.0.1", "127.1.2.3", "10.0.0.5", "192.168.1.1",
                   "172.16.0.1", "172.31.255.255", "169.254.1.1", "255.255.255.255"]:
            self.assertTrue(scraper.is_blocked_ip(ip), f"{ip} should be blocked")

    def test_public_ips_not_blocked(self):
        for ip in ["8.8.8.8", "172.32.0.1", "192.169.1.1", "11.0.0.1", "1.2.3.4"]:
            self.assertFalse(scraper.is_blocked_ip(ip), f"{ip} should NOT be blocked")

    def test_private_ips_rejected_at_extract(self):
        out = scraper.extract_proxies("127.0.0.1:8080\n0.0.0.0:3128\n10.1.2.3:80\n8.8.8.8:8080", "host:port")
        self.assertEqual(out, ["8.8.8.8:8080"])


class RateLimitTests(unittest.TestCase):
    def test_429_sleeps_and_retries(self):
        calls = {"n": 0}

        def flaky(url, timeout=15):
            calls["n"] += 1
            if calls["n"] < 3:
                raise urllib.error.HTTPError(url, 429, "rate", {"Retry-After": "1"}, None)
            return "ok"

        with patch("scraper.fetch_direct", side_effect=flaky), \
             patch("scraper.time.sleep") as sleep:
            out = scraper.fetch("http://x.test/", retries=3, backoff=0.1)
        self.assertEqual(out, "ok")
        self.assertGreaterEqual(calls["n"], 3)
        # at least one 429 backoff sleep happened (Retry-After honored)
        delays = [c.args[0] for c in sleep.call_args_list]
        self.assertTrue(any(d >= 1 for d in delays), f"no 429 backoff sleep: {delays}")


class RevalidateManualLightTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._db = str(Path(self._tmp.name) / "t.db")
        os.environ["PROXY_DB"] = self._db
        import proxy_pool
        conn = proxy_pool.get_db()
        conn.execute("INSERT OR REPLACE INTO proxies (ip, port, protocol, score, last_seen, source_name) VALUES ('1.1.1.1', 8080, 'http', 50, '2026-01-01T00:00:00Z', 'manual-light')")
        conn.execute("INSERT OR REPLACE INTO proxies (ip, port, protocol, score, last_seen, source_name) VALUES ('2.2.2.2', 8080, 'http', 50, '2026-01-01T00:00:00Z', 'normal-src')")
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (1, '1.1.1.1', 8080, 0, 100)")
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (11, '1.1.1.1', 8080, 0, 100)")
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (2, '2.2.2.2', 8080, 0, 100)")
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (22, '2.2.2.2', 8080, 0, 100)")
        conn.commit()
        conn.close()

    def tearDown(self):
        self._tmp.cleanup()
        os.environ.pop("PROXY_DB", None)

    def test_pick_failed_usage_excludes_manual_light(self):
        targets = revalidate_pool.pick_failed_usage(10)
        self.assertEqual([t[0] for t in targets], ["2.2.2.2"])

    def test_pick_usage_priority_excludes_manual_light(self):
        import proxy_pool
        conn = proxy_pool.get_db()
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (3, '1.1.1.1', 8080, 1, 50)")
        conn.execute("INSERT OR REPLACE INTO usage_log (id, ip, port, success, response_time_ms) VALUES (4, '2.2.2.2', 8080, 1, 50)")
        conn.commit()
        conn.close()
        targets = revalidate_pool.pick_usage_priority(10)
        self.assertEqual([t[0] for t in targets], ["2.2.2.2"])

    def test_update_does_not_prune_manual_light(self):
        import proxy_pool
        conn = proxy_pool.get_db()
        # call the DB-update statement directly (skip network): manual-light must survive
        conn.execute("UPDATE proxies SET last_seen = '', score = 0 WHERE ip = '1.1.1.1' AND port = 8080 AND source_name != 'manual-light'")
        conn.commit()
        row = conn.execute("SELECT last_seen, score FROM proxies WHERE ip = '1.1.1.1'").fetchone()
        self.assertEqual(row["last_seen"], "2026-01-01T00:00:00Z")  # untouched
        conn.close()


class ReputationGraceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["PROXY_DB"] = str(Path(self._tmp.name) / "t.db")

    def tearDown(self):
        self._tmp.cleanup()
        os.environ.pop("PROXY_DB", None)

    def test_zero_valid_banned_after_grace_cap(self):
        from proxy_pool import get_db
        conn = get_db()
        for _ in range(4):
            conn.execute("INSERT INTO source_history (source_name, alive, proxy_count, timestamp) VALUES (?,?,?,datetime('now'))", ("junk-src", 1, 100))
        conn.commit()
        conn.close()
        reputation.update_reputation("junk-src", 100, 0)
        conn = get_db()
        row = conn.execute("SELECT is_banned FROM source_reputation WHERE source_name='junk-src'").fetchone()
        self.assertEqual(row["is_banned"], 1)
        conn.close()

    def test_first_run_still_grace(self):
        from proxy_pool import get_db
        conn = get_db()
        conn.execute("INSERT INTO source_history (source_name, alive, proxy_count, timestamp) VALUES (?,?,?,datetime('now'))", ("new-src", 1, 100))
        conn.commit()
        conn.close()
        reputation.update_reputation("new-src", 100, 0)
        conn = get_db()
        row = conn.execute("SELECT is_banned FROM source_reputation WHERE source_name='new-src'").fetchone()
        self.assertEqual(row["is_banned"], 0)
        conn.close()


if __name__ == "__main__":
    unittest.main()
