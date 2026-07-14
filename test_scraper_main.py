import io
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

import scraper


class ValidatedOutputTests(unittest.TestCase):
    def test_no_confirmed_proxy_preserves_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "proxies.txt"
            output.write_text("last-known-good:1080\n")
            stderr = io.StringIO()
            with patch("scraper.scrape_all", return_value=({"1.2.3.4:1080"}, {})), patch(
                "scraper.filter_valid", return_value=[]
            ), patch("scraper.scrape_creds", return_value=[]), patch(
                "sys.argv", ["scraper.py", "--validate", "-o", str(output)]
            ), redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as error:
                    scraper.main()
            self.assertEqual(error.exception.code, 1)
            self.assertEqual(output.read_text(), "last-known-good:1080\n")
            self.assertIn("No confirmed proxies", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
