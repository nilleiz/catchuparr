import types
import unittest

from catchuparr.adapters.xc import (
    SUPPORTED_DISPATCHARR_VERSION,
    XCCallbacks,
    install_xc_hooks,
)


class Response:
    def __init__(self, content="", status=200):
        self.content = content
        self.status_code = status
        self.headers = {}

    def __getitem__(self, key):
        return self.headers[key]

    def __setitem__(self, key, value):
        self.headers[key] = value


class Request:
    def __init__(self, stream_id="8"):
        self.GET = {"stream_id": stream_id}


class Channel:
    id = 8


def modules():
    calls = {"entry": 0, "epg": 0, "serve": 0}

    def _xc_channel_entry(channel, channel_num_map, _get_default_group_id,
                          _logo_url_prefix, _logo_url_suffix, *, catchup_allowed=True):
        calls["entry"] += 1
        return {"stream_id": channel.id, "tv_archive": 0, "tv_archive_duration": 0}

    def xc_get_epg(request, user, short=False):
        calls["epg"] += 1
        return {"epg_listings": [{
            "start": "2026-01-01 10:00:00",
            "end": "2026-01-01 11:00:00",
            "has_archive": 0,
        }]}

    def _serve_catchup(request, user, channel, timestamp, client_duration_hint=None):
        calls["serve"] += 1
        return Response("provider")

    output = types.SimpleNamespace(
        _xc_channel_entry=_xc_channel_entry,
        xc_get_epg=xc_get_epg,
        is_catchup_enabled=lambda user: user.get("catchup", True),
        HttpResponse=Response,
        HttpResponseForbidden=lambda message: Response(message, 403),
    )
    timeshift = types.SimpleNamespace(
        _serve_catchup=_serve_catchup,
        is_catchup_enabled=lambda user: user.get("catchup", True),
        parse_catchup_timestamp=lambda value: value if value else None,
        HttpResponse=Response,
        HttpResponseForbidden=lambda message: Response(message, 403),
    )
    return output, timeshift, calls


class XCHookInstallTests(unittest.TestCase):
    def test_rejects_unsupported_version_without_mutating_modules(self):
        output, timeshift, _ = modules()
        original = output._xc_channel_entry
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version="0.32.0", callbacks=XCCallbacks(),
        )
        self.assertFalse(result.installed)
        self.assertIs(output._xc_channel_entry, original)

    def test_rejects_signature_mismatch_without_partial_patch(self):
        output, timeshift, _ = modules()
        original = output.xc_get_epg
        output.xc_get_epg = lambda request, user: {}
        entry = output._xc_channel_entry
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=XCCallbacks(),
        )
        self.assertFalse(result.installed)
        self.assertIs(output._xc_channel_entry, entry)
        self.assertIsNot(output.xc_get_epg, original)

    def test_installs_idempotently(self):
        output, timeshift, _ = modules()
        callbacks = XCCallbacks()
        first = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        wrapper = output._xc_channel_entry
        second = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        self.assertTrue(first.installed)
        self.assertTrue(second.installed)
        self.assertIs(output._xc_channel_entry, wrapper)


class XCHookBehaviorTests(unittest.TestCase):
    def install(self, callbacks):
        output, timeshift, calls = modules()
        result = install_xc_hooks(
            output, timeshift, dispatcharr_version=SUPPORTED_DISPATCHARR_VERSION,
            callbacks=callbacks,
        )
        self.assertTrue(result.installed)
        return output, timeshift, calls

    def test_channel_entry_exposes_local_retention_only_when_allowed(self):
        output, _, calls = self.install(XCCallbacks(channel_archive_days=lambda channel: 9))
        args = (Channel(), {}, lambda: 1, "prefix", "suffix")
        enabled = output._xc_channel_entry(*args, catchup_allowed=True)
        disabled = output._xc_channel_entry(*args, catchup_allowed=False)
        self.assertEqual(enabled["tv_archive"], 1)
        self.assertEqual(enabled["tv_archive_duration"], 9)
        self.assertEqual(disabled["tv_archive"], 0)
        self.assertEqual(calls["entry"], 2)

    def test_epg_only_marks_fully_covered_local_programmes(self):
        output, _, calls = self.install(XCCallbacks(
            program_available=lambda channel_id, start, end, user: channel_id == "8" and end.endswith("11:00:00"),
        ))
        result = output.xc_get_epg(Request(), {"catchup": True})
        self.assertEqual(result["epg_listings"][0]["has_archive"], 1)
        self.assertEqual(calls["epg"], 1)
        result = output.xc_get_epg(Request(), {"catchup": False})
        self.assertEqual(result["epg_listings"][0]["has_archive"], 0)

    def test_provider_path_is_preserved_when_local_programme_missing(self):
        _, timeshift, calls = self.install(XCCallbacks(playback_available=lambda *args: False))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)

    def test_local_coverage_without_security_contract_fails_closed(self):
        _, timeshift, calls = self.install(XCCallbacks(playback_available=lambda *args: True))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(calls["serve"], 0)

    def test_invalid_timestamp_stays_on_dispatcharr_validation_path(self):
        _, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: True,
            serve_local_playback=lambda *args: Response("local"),
        ))
        timeshift.parse_catchup_timestamp = lambda value: None
        response = timeshift._serve_catchup(
            Request(), {"catchup": True}, Channel(), "bad-timestamp",
        )
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)

    def test_local_playback_requires_explicit_authorization(self):
        served = []
        callbacks = XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: False,
            serve_local_playback=lambda *args: served.append(True) or Response("local"),
        )
        _, timeshift, calls = self.install(callbacks)
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(served, [])
        self.assertEqual(calls["serve"], 0)

    def test_authorized_local_playback_calls_archive_callback(self):
        callbacks = XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: True,
            serve_local_playback=lambda *args: Response("local"),
        )
        _, timeshift, calls = self.install(callbacks)
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.content, "local")
        self.assertEqual(calls["serve"], 0)

    def test_coverage_and_authorization_errors_fall_back_or_fail_closed(self):
        output, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: (_ for _ in ()).throw(RuntimeError("index down")),
        ))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.content, "provider")
        self.assertEqual(calls["serve"], 1)
        # A local authorization failure cannot flow to provider or unprotected local output.
        output, timeshift, calls = self.install(XCCallbacks(
            playback_available=lambda *args: True,
            authorize_local_playback=lambda *args: (_ for _ in ()).throw(RuntimeError("policy down")),
            serve_local_playback=lambda *args: Response("local"),
        ))
        response = timeshift._serve_catchup(Request(), {"catchup": True}, Channel(), "2026-01-01:10-00")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(calls["serve"], 0)


if __name__ == "__main__":
    unittest.main()
