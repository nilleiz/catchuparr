import unittest
from unittest.mock import Mock

from catchuparr.views import _session_limit_allows


class SessionLimitTests(unittest.TestCase):
    def test_bounded_user_is_denied_when_redis_ping_fails(self):
        redis = Mock()
        redis.ping.side_effect = ConnectionError("Redis unavailable")
        count_connections = Mock(return_value=[])

        self.assertFalse(_session_limit_allows(1, 0, redis, count_connections))
        count_connections.assert_not_called()

    def test_bounded_user_is_denied_when_active_connection_query_fails(self):
        redis = Mock()
        count_connections = Mock(side_effect=RuntimeError("connection query failed"))

        self.assertFalse(_session_limit_allows(1, 0, redis, count_connections))
        self.assertEqual(redis.ping.call_count, 1)

    def test_bounded_user_is_denied_when_redis_fails_after_connection_query(self):
        redis = Mock()
        redis.ping.side_effect = [True, ConnectionError("Redis unavailable")]

        self.assertFalse(_session_limit_allows(1, 0, redis, lambda: []))
        self.assertEqual(redis.ping.call_count, 2)

    def test_unbounded_user_does_not_depend_on_redis(self):
        redis = Mock()
        redis.ping.side_effect = ConnectionError("Redis unavailable")

        self.assertTrue(_session_limit_allows(0, 10, redis, lambda: []))
        redis.ping.assert_not_called()

    def test_session_limit_combines_dispatcharr_and_plugin_sessions(self):
        redis = Mock()

        self.assertFalse(_session_limit_allows(2, 1, redis, lambda: ["active"]))
        self.assertEqual(redis.ping.call_count, 2)


if __name__ == "__main__":
    unittest.main()
