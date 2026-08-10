import unittest
from unittest.mock import Mock, patch

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


class ConnectProbeTests(unittest.TestCase):
    """R17-T8: regression guard untuk CONNECT probe (R16-N1 + R17-T1)."""

    def test_probe_connect_accepts_proxy_with_200(self):
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"HTTP/1.1 200 Connection established\r\n\r\n"
            mc.return_value = fake
            self.assertTrue(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_probe_connect_accepts_proxy_with_403(self):
        """R20: 4xx = proxy HIDUP (HTTP-only yang tolak CONNECT) — harus True."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"HTTP/1.1 403 Forbidden\r\n\r\n"
            mc.return_value = fake
            self.assertTrue(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_probe_connect_accepts_404_with_200_in_body(self):
        """R20: 404 status line + body '200' — proxy hidup (HTTP-only), True.
        R18-T2 lama salah ekspektasi: 4xx = mati; padahal 4xx = proxy hidup."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"HTTP/1.1 404 Not Found\r\n\r\nstatus 200 OK page"
            mc.return_value = fake
            self.assertTrue(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_probe_connect_accepts_200_then_silence(self):
        """R18-T1: proxy balas 200 lalu diam (tunnel mode) — harus True."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.side_effect = [b"HTTP/1.1 200 Connection established\r\n\r\n", b""]
            mc.return_value = fake
            self.assertTrue(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_probe_connect_rejects_3xx(self):
        """R20: 3xx = redirect aneh, bukan proxy hidup — harus False."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"HTTP/1.1 302 Found\r\n\r\n"
            mc.return_value = fake
            self.assertFalse(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_probe_connect_rejects_non_http(self):
        """R20: non-HTTP response = mati — harus False."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"garbage not http\r\n\r\n"
            mc.return_value = fake
            self.assertFalse(scraper._probe_connect("1.2.3.4", 8080, 5))

    def test_jsonlines_extract(self):
        """R19: fmt jsonlines (fate0/proxylist) — host/port per baris JSON."""
        txt = ('{"anonymity": "high", "host": "1.2.3.4", "port": 8080}\n'
               '{"host": "5.6.7.8", "port": 3128}\nnot json\n')
        p = scraper.extract_proxies(txt, "jsonlines", 10)
        self.assertEqual(p, ["1.2.3.4:8080", "5.6.7.8:3128"])

    def test_jsonlines_rejects_float_port(self):
        """R19-P1: port float ("8080.9" → 8080.9) harus diskip, bukan korup."""
        txt = ('{"host": "1.2.3.4", "port": 8080.9}\n'
               '{"host": "1.2.3.4", "port": "8080"}\n'
               '{"host": "1.2.3.4", "port": true}\n')
        p = scraper.extract_proxies(txt, "jsonlines", 10)
        self.assertEqual(p, ["1.2.3.4:8080"])

    def test_probe_connect_uses_fresh_socket(self):
        """R17-T1: probe harus socket BARU — socket lama sudah close."""
        with patch("socket.create_connection") as mc:
            fake = Mock()
            fake.recv.return_value = b"HTTP/1.1 200 Connection established\r\n\r\n"
            mc.return_value = fake
            scraper._probe_connect("1.2.3.4", 8080, 5)
            mc.assert_called_once()  # create_connection dipanggil (socket baru)


if __name__ == "__main__":
    unittest.main()
