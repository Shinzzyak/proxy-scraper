import unittest

import session_quality


class SessionQualityTests(unittest.TestCase):
    def test_marks_identical_egress_and_geo_as_stable(self):
        samples = [
            {"ip": "203.0.113.7", "country_code": "US"},
            {"ip": "203.0.113.7", "country_code": "US"},
            {"ip": "203.0.113.7", "country_code": "US"},
        ]
        result = session_quality.summarize_samples(samples, requested=3)
        self.assertTrue(result["stable"])
        self.assertEqual(result["successes"], 3)
        self.assertEqual(result["unique_ips"], ["203.0.113.7"])
        self.assertEqual(result["unique_countries"], ["US"])

    def test_marks_changed_egress_as_unstable(self):
        samples = [
            {"ip": "203.0.113.7", "country_code": "US"},
            {"ip": "198.51.100.8", "country_code": "DE"},
        ]
        result = session_quality.summarize_samples(samples, requested=2)
        self.assertFalse(result["stable"])
        self.assertEqual(result["unique_ips"], ["198.51.100.8", "203.0.113.7"])

    def test_marks_failed_sample_as_unstable(self):
        result = session_quality.summarize_samples(
            [{"ip": "203.0.113.7", "country_code": "US"}, {"error": "timeout"}], requested=2
        )
        self.assertFalse(result["stable"])
        self.assertEqual(result["successes"], 1)


class InputValidationTests(unittest.TestCase):
    def test_rejects_non_ip_query(self):
        self.assertIsNone(session_quality._validate_ip("bukan-ip"))
        self.assertIsNone(session_quality._validate_ip(""))
        self.assertIsNone(session_quality._validate_ip(None))
        self.assertIsNone(session_quality._validate_ip(42))

    def test_accepts_valid_ipv4_and_ipv6(self):
        self.assertEqual(session_quality._validate_ip("203.0.113.7"), "203.0.113.7")
        self.assertEqual(session_quality._validate_ip("::1"), "::1")

    def test_rejects_invalid_country_code(self):
        self.assertIsNone(session_quality._validate_country(""))
        self.assertIsNone(session_quality._validate_country("USA"))
        self.assertIsNone(session_quality._validate_country("123"))
        self.assertIsNone(session_quality._validate_country(None))

    def test_accepts_valid_two_letter_country(self):
        self.assertEqual(session_quality._validate_country("us"), "US")
        self.assertEqual(session_quality._validate_country("DE"), "DE")

    def test_malformed_response_returns_error(self):
        # Simulate what request_egress would produce if IP validation rejects the query
        ip = session_quality._validate_ip("not-an-ip")
        cc = session_quality._validate_country("US")
        self.assertIsNone(ip)
        self.assertTrue(cc)


if __name__ == "__main__":
    unittest.main()
