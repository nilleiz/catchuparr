import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from catchuparr import native_output


def _endpoint(request, profile_name=None, user=None):
    return request, profile_name, user


class NativeOutputHookTests(unittest.TestCase):
    def setUp(self):
        native_output.uninstall_output_hooks()

    def tearDown(self):
        native_output.uninstall_output_hooks()

    def _modules(self, m3u_callback=_endpoint, epg_callback=_endpoint):
        apps = types.ModuleType("apps")
        output = types.ModuleType("apps.output")
        urls = types.ModuleType("apps.output.urls")
        urls.urlpatterns = [
            SimpleNamespace(name="m3u_endpoint", callback=m3u_callback),
            SimpleNamespace(name="epg_endpoint", callback=epg_callback),
        ]
        apps.output = output
        output.urls = urls
        return {"apps": apps, "apps.output": output, "apps.output.urls": urls}, urls

    def test_install_is_idempotent_and_uninstall_restores_both_callbacks(self):
        modules, urls = self._modules()
        original = tuple(route.callback for route in urls.urlpatterns)
        with patch.dict(sys.modules, modules):
            self.assertTrue(native_output.install_output_hooks())
            installed = tuple(route.callback for route in urls.urlpatterns)
            self.assertTrue(all(callback is not old for callback, old in zip(installed, original)))
            self.assertTrue(native_output.install_output_hooks())
            self.assertEqual(installed, tuple(route.callback for route in urls.urlpatterns))
            native_output.uninstall_output_hooks()
        self.assertEqual(original, tuple(route.callback for route in urls.urlpatterns))

    def test_invalid_second_signature_does_not_partially_install_first(self):
        def wrong_endpoint(request, *args, **kwargs):
            return request

        modules, urls = self._modules(epg_callback=wrong_endpoint)
        original = tuple(route.callback for route in urls.urlpatterns)
        with patch.dict(sys.modules, modules):
            self.assertFalse(native_output.install_output_hooks())
        self.assertEqual(original, tuple(route.callback for route in urls.urlpatterns))

    def test_missing_endpoint_fails_closed_without_mutation(self):
        modules, urls = self._modules()
        urls.urlpatterns.pop()
        original = tuple(route.callback for route in urls.urlpatterns)
        with patch.dict(sys.modules, modules):
            self.assertFalse(native_output.install_output_hooks())
        self.assertEqual(original, tuple(route.callback for route in urls.urlpatterns))

    def test_epg_mapping_uses_the_renderer_user_before_admission_filtering(self):
        from catchuparr.native_output import _augment_native_epg

        selected = "00000000-0000-0000-0000-000000000001"
        m3u = (
            f'#EXTINF:-1 tvg-id="shared",Selected\n'
            f'http://dispatcharr/proxy/ts/stream/{selected}?output_profile=7\n'
            '#EXTINF:-1 tvg-id="shared",Other\n'
            'https://provider.example/direct/stream\n'
        )
        m3u_response = SimpleNamespace(status_code=200, content=m3u.encode())
        generate_m3u = unittest.mock.Mock(return_value=m3u_response)
        apps = types.ModuleType("apps")
        output = types.ModuleType("apps.output")
        output_views = types.ModuleType("apps.output.views")
        output_views.generate_m3u = generate_m3u
        apps.output = output
        output.views = output_views
        request = SimpleNamespace()
        context = SimpleNamespace(
            request=request,
            renderer_user=None,
            profile_id="7",
            config=SimpleNamespace(
                channel_uuids={selected}, archive_root="/tmp/synthetic-native-archive"
            ),
        )
        response = SimpleNamespace(streaming_content=iter((b"<tv></tv>",)))
        with (
            patch.dict(sys.modules, {
                "apps": apps,
                "apps.output": output,
                "apps.output.views": output_views,
            }),
            patch("catchuparr.views._core_request", return_value=request),
            patch("catchuparr.engine.store.ArchiveStore"),
        ):
            result = _augment_native_epg(response, context, "Synthetic profile")
        self.assertIs(result, response)
        self.assertIsNone(generate_m3u.call_args.kwargs["user"])

    def test_direct_m3u_preserves_provider_and_covered_proxy_fallback_rows(self):
        playlist = (
            '#EXTM3U x-tvg-url="https://dispatcharr.example/output/epg/profile" '
            'url-tvg="https://dispatcharr.example/output/epg/profile"\n'
            '#EXTINF:-1 tvg-id="provider" catchup="xc" catchup-days="7",Provider\n'
            'https://provider.example/synthetic.ts\n'
            '#EXTINF:-1 tvg-id="covered",Covered fallback\n'
            'https://dispatcharr.example/proxy/ts/stream/'
            '00000000-0000-0000-0000-000000000001?output_profile=73&output_format=hls\n'
        )
        response = SimpleNamespace(content=playlist.encode("utf-8"))
        request = SimpleNamespace(
            method="GET",
            GET={"direct": "TrUe"},
        )

        def direct_endpoint(request, profile_name=None, user=None):
            return response

        modules, urls = self._modules(m3u_callback=direct_endpoint)
        native_context = Mock()
        with (
            patch.dict(sys.modules, modules),
            patch.object(native_output, "_native_export_context", native_context),
        ):
            self.assertTrue(native_output.install_output_hooks())
            route = next(route for route in urls.urlpatterns if route.name == "m3u_endpoint")
            result = route.callback(request, "Synthetic profile", None)

        native_context.assert_not_called()
        self.assertIs(result, response)
        self.assertEqual(response.content, playlist.encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
