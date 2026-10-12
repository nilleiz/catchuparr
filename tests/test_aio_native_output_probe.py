import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import quote, unquote, urlsplit

from scripts.aio_native_output_probe import (
    _native_archive_failure_message,
    _resolve_wsgi_path,
)


class NativeOutputProbePathTests(unittest.TestCase):
    def _request_factory(self, url, params):
        # RequestFactory/WSGI supplies decoded PATH_INFO while keeping query
        # values on their own decoding path.
        return SimpleNamespace(
            path_info=unquote(urlsplit(url).path, encoding="utf-8", errors="strict"),
            query=params,
        )

    def test_resolver_receives_decoded_profile_path_exactly_once(self):
        resolver = Mock(return_value="match")
        for profile in (
            "Synthetic profile with spaces",
            "Synthetic 50% profile",
            "Synthetic λ profile",
        ):
            with self.subTest(profile=profile):
                resolver.reset_mock()
                url = "/output/m3u/" + quote(profile, safe="")
                result = _resolve_wsgi_path(url, resolver, self._request_factory)
                self.assertEqual(result, "match")
                resolver.assert_called_once_with("/output/m3u/" + profile)

    def test_query_data_is_not_path_decoded_or_rewritten(self):
        resolver = Mock(return_value="match")
        url = "/output/m3u/Synthetic%20profile?query_probe=literal%252Fvalue"
        seen = []

        def request_factory(path, params):
            seen.append((path, params))
            return self._request_factory(path, params)

        _resolve_wsgi_path(url, resolver, request_factory)
        self.assertEqual(seen, [(url, {})])
        resolver.assert_called_once_with("/output/m3u/Synthetic profile")

    def test_archive_failure_diagnostic_reports_only_safe_boolean_categories(self):
        response = SimpleNamespace(status_code=403, content=b"stream limit exceeded")
        request = object()
        capability = "synthetic-private-capability"
        user = SimpleNamespace(id=17)
        views = SimpleNamespace(_native_playback_context=Mock(return_value=object()))

        with patch(
            "catchuparr.xc_runtime.active_ts_session_count", return_value=1,
        ):
            message = _native_archive_failure_message(
                response, request, "synthetic-channel", capability,
                "/synthetic/archive", user, views,
            )

        self.assertIn('"context_available": true', message)
        self.assertIn('"stream_limit_category": true', message)
        self.assertIn('"existing_ts_session": true', message)
        self.assertNotIn(capability, message)
        self.assertNotIn("synthetic-channel", message)
        self.assertNotIn("stream limit exceeded", message)
        views._native_playback_context.assert_called_once_with(
            request, capability, "synthetic-channel",
        )


if __name__ == "__main__":
    unittest.main()
