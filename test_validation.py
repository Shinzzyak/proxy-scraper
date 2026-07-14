import unittest
from unittest.mock import patch

import scraper


class FakeSocket:
    def __init__(self, response):
        self.response = response
        self.sent = b""
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def settimeout(self, timeout):
        pass

    def sendall(self, data):
        self.sent += data

    def recv(self, size):
        if isinstance(self.response, list):
            return self.response.pop(0)
        return self.response

    def close(self):
        self.closed = True


class ConfirmationTests(unittest.TestCase):
    def test_only_protocol_confirmed_results_are_publishable(self):
        self.assertTrue(scraper.is_confirmed_proxy({"protocol": "http"}))
        self.assertTrue(scraper.is_confirmed_proxy({"protocol": "socks4"}))
        self.assertTrue(scraper.is_confirmed_proxy({"protocol": "socks5"}))
        self.assertFalse(scraper.is_confirmed_proxy({"protocol": "unknown"}))


class Socks4ValidationTests(unittest.TestCase):
    def test_accepts_fragmented_socks4_connect_reply(self):
        sock = FakeSocket([b"\x00\x5a", b"\x00\x00\x00\x00\x00\x00"])
        with patch("socket.create_connection", return_value=sock):
            self.assertTrue(scraper.validate_socks4("1.2.3.4:1080"))
        self.assertEqual(sock.sent, b"\x04\x01\x00\x50\x01\x01\x01\x01\x00")
        self.assertTrue(sock.closed)

    def test_classifies_socks4_as_confirmed_protocol(self):
        with patch("scraper.validate_tcp", return_value=True), patch(
            "scraper.validate_socks5", return_value=False
        ), patch("scraper.validate_socks4", return_value=True):
            result = scraper.validate_single("1.2.3.4:1080")
        self.assertEqual(result["protocol"], "socks4")


if __name__ == "__main__":
    unittest.main()
