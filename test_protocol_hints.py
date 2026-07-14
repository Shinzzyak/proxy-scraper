import unittest
from unittest.mock import patch

import scraper


class SourceCoverageTests(unittest.TestCase):
    def test_geonode_has_protocol_specific_shards(self):
        names = {name for name, _, _ in scraper.PROXY_SOURCES}
        self.assertTrue({
            "geonode-http", "geonode-https", "geonode-socks4", "geonode-socks5",
            "geonode-p2-http", "geonode-p2-https", "geonode-p2-socks4", "geonode-p2-socks5",
        } <= names)


class ProtocolHintTests(unittest.TestCase):
    def test_keeps_protocol_hint_when_proxy_is_in_multiple_sources(self):
        self.assertEqual(
            scraper.preferred_source_name("opsxcq-mixed", "monosans-socks5"),
            "monosans-socks5",
        )
        self.assertEqual(
            scraper.preferred_source_name("z-socks5", "a-socks5"),
            "a-socks5",
        )

    def test_extracts_known_protocol_from_source_name(self):
        self.assertEqual(scraper.source_protocol_hint("proxyscrape-v4-socks5"), "socks5")
        self.assertEqual(scraper.source_protocol_hint("monosans-socks4"), "socks4")
        self.assertEqual(scraper.source_protocol_hint("clarketm-http"), "http")
        self.assertEqual(scraper.source_protocol_hint("geonode-https"), "http")
        self.assertIsNone(scraper.source_protocol_hint("opsxcq-mixed"))

    def test_tries_source_protocol_before_other_probes(self):
        calls = []
        with patch("scraper.validate_tcp", return_value=True), patch(
            "scraper.validate_socks5", side_effect=lambda _: calls.append("socks5") or True
        ), patch("scraper.validate_socks4", side_effect=lambda _: calls.append("socks4") or False), patch(
            "scraper.validate_http_connect", side_effect=lambda _: calls.append("http") or False
        ):
            result = scraper.validate_single("1.2.3.4:1080", protocol_hint="socks5")
        self.assertEqual(result["protocol"], "socks5")
        self.assertEqual(calls, ["socks5"])


if __name__ == "__main__":
    unittest.main()
