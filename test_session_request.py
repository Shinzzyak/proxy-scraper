import json
import unittest
from unittest.mock import MagicMock, patch

import session_quality


class SessionRequestTests(unittest.TestCase):
    def test_sends_same_session_header_and_returns_egress_geo(self):
        response = MagicMock()
        response.read.return_value = json.dumps({"status": "success", "query": "203.0.113.7", "countryCode": "us"}).encode()
        response.__enter__.return_value = response
        opener = MagicMock()
        opener.open.return_value = response
        with patch("urllib.request.build_opener", return_value=opener):
            result = session_quality.request_egress("1.2.3.4:8080", "test-session", timeout=2)
        self.assertEqual(result, {"ip": "203.0.113.7", "country_code": "US"})
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_header("X-session-id"), "test-session")
        self.assertEqual(opener.open.call_args.kwargs["timeout"], 2)


if __name__ == "__main__":
    unittest.main()
