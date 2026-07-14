import unittest
import time

from session_manager import SessionManager


class SessionManagerTests(unittest.TestCase):
    def test_returns_same_proxy_for_same_session(self):
        sm = SessionManager(default_ttl=60)
        proxy = sm.get_or_create("abc", lambda: "1.2.3.4:8080")
        self.assertEqual(proxy, "1.2.3.4:8080")
        self.assertEqual(sm.get_or_create("abc", lambda: "5.6.7.8:8080"), "1.2.3.4:8080")

    def test_returns_different_proxy_for_different_sessions(self):
        sm = SessionManager(default_ttl=60)
        a = sm.get_or_create("a", lambda: "1.1.1.1:80")
        b = sm.get_or_create("b", lambda: "2.2.2.2:80")
        self.assertNotEqual(a, b)

    def test_expired_session_gets_new_proxy(self):
        sm = SessionManager(default_ttl=0)
        sm.get_or_create("abc", lambda: "old:80")
        proxy = sm.get_or_create("abc", lambda: "new:80")
        self.assertEqual(proxy, "new:80")

    def test_release_removes_session(self):
        sm = SessionManager(default_ttl=60)
        sm.get_or_create("abc", lambda: "1.2.3.4:80")
        sm.release("abc")
        proxy = sm.get_or_create("abc", lambda: "5.6.7.8:80")
        self.assertEqual(proxy, "5.6.7.8:80")

    def test_cleanup_removes_expired_sessions(self):
        sm = SessionManager(default_ttl=0)
        sm.get_or_create("a", lambda: "1:80")
        sm.get_or_create("b", lambda: "2:80")
        removed = sm.cleanup()
        self.assertEqual(removed, 2)
        self.assertEqual(len(sm), 0)

    def test_cleanup_keeps_alive_sessions(self):
        sm = SessionManager(default_ttl=600)
        sm.get_or_create("a", lambda: "1:80")
        sm.get_or_create("b", lambda: "2:80")
        removed = sm.cleanup()
        self.assertEqual(removed, 0)
        self.assertEqual(len(sm), 2)

    def test_release_nonexistent_session_is_safe(self):
        sm = SessionManager(default_ttl=60)
        sm.release("nope")


if __name__ == "__main__":
    unittest.main()
