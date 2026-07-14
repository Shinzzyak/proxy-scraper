import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import freshen_pool


class ExportSnapshotsTests(unittest.TestCase):
    def test_does_not_replace_last_snapshot_with_stale_database_entries(self):
        stale = [{"ip": "1.2.3.4", "port": 1080, "protocol": "socks5", "score": 99}]
        with tempfile.TemporaryDirectory() as directory, patch.object(
            freshen_pool, "ROOT", Path(directory)
        ), patch("proxy_pool.search_proxies", side_effect=[[], stale]):
            output = Path(directory) / "proxies.txt"
            output.write_text("last-known-good:1080\n")
            self.assertEqual(freshen_pool.export_pool_snapshots(max_age_minutes=60), 0)
            self.assertEqual(output.read_text(), "last-known-good:1080\n")

    def test_excludes_unknown_protocol_from_fresh_snapshot(self):
        fresh = [
            {"ip": "1.2.3.4", "port": 1080, "protocol": "socks5", "score": 90},
            {"ip": "5.6.7.8", "port": 8080, "protocol": "unknown", "score": 100},
        ]
        with tempfile.TemporaryDirectory() as directory, patch.object(
            freshen_pool, "ROOT", Path(directory)
        ), patch("proxy_pool.search_proxies", return_value=fresh):
            self.assertEqual(freshen_pool.export_pool_snapshots(max_age_minutes=60), 1)
            self.assertEqual((Path(directory) / "proxies.txt").read_text(), "1.2.3.4:1080\n")


if __name__ == "__main__":
    unittest.main()
