import importlib
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    module.__path__ = []
    return module


class RecorderProxyRouteTests(unittest.TestCase):
    def _route_modules(self):
        root_urls = _module("dispatcharr.urls", urlpatterns=[])
        dispatcharr = _module("dispatcharr", urls=root_urls)
        django_urls = _module(
            "django.urls",
            clear_url_caches=lambda: None,
            path=lambda route, callback, name: SimpleNamespace(
                route=route, callback=callback, name=name
            ),
        )
        django = _module("django", urls=django_urls)
        return {
            "dispatcharr": dispatcharr,
            "dispatcharr.urls": root_urls,
            "django": django,
            "django.urls": django_urls,
        }, root_urls

    def test_route_install_is_idempotent_and_installs_process_guard_each_time(self):
        from catchuparr import views
        from catchuparr.adapters import recorder_proxy as adapter

        modules, root_urls = self._route_modules()
        installed_guards = []
        with patch.dict(sys.modules, modules), patch.object(
            adapter,
            "install_proxyserver_cleanup_hook",
            side_effect=lambda: installed_guards.append(True) or True,
        ):
            importlib.reload(views)
            views.install_routes()
            views.install_routes()

        route_names = [route.name for route in root_urls.urlpatterns]
        self.assertEqual(1, route_names.count("catchuparr-recorder"))
        self.assertEqual(1, route_names.count("catchuparr-sharing-borrow"))
        self.assertEqual(1, route_names.count("catchuparr-m3u"))
        self.assertEqual(2, len(installed_guards))

    def test_uninstall_removes_route_then_stops_workers_then_unhooks(self):
        from catchuparr import views
        from catchuparr.adapters import recorder_proxy as adapter

        modules, root_urls = self._route_modules()
        events = []
        with patch.dict(sys.modules, modules), patch.object(
            adapter,
            "install_proxyserver_cleanup_hook",
            return_value=True,
        ), patch.object(
            adapter,
            "stop_managed_workers",
            side_effect=lambda: events.append(("stop", not root_urls.urlpatterns)) or True,
        ), patch.object(
            adapter,
            "uninstall_proxyserver_cleanup_hook",
            side_effect=lambda: events.append(("unhook", not root_urls.urlpatterns)) or True,
        ):
            importlib.reload(views)
            views.install_routes()
            views.uninstall_routes()

        self.assertEqual([("stop", True), ("unhook", True)], events)
        self.assertEqual([], root_urls.urlpatterns)

    def test_signed_borrow_endpoint_streams_only_after_capability_admission(self):
        from catchuparr import views
        from catchuparr.adapters.sharing_borrow_route import (
            encode_borrow_capability,
            sign_borrow_capability,
        )

        class Response:
            def __init__(self, content="", status=200, content_type=None):
                self.content = content
                self.status_code = status
                self.headers = {}
                self.streaming_content = None
                self.content_type = content_type

            def __setitem__(self, key, value):
                self.headers[key] = value

        class StreamingResponse(Response):
            def __init__(self, stream, **kwargs):
                super().__init__(**kwargs)
                self.streaming_content = stream

        django_http = _module(
            "django.http", HttpResponse=Response,
            StreamingHttpResponse=StreamingResponse,
        )
        django = _module("django", http=django_http)
        request = SimpleNamespace(method="GET", headers={})
        session = SimpleNamespace(iter_chunks=lambda: iter((b"synthetic-ts",)))
        identity = (
            "channel-route-synthetic", "worker-route-synthetic", "owner-route-synthetic",
            "owner-route-synthetic", "stream-route-synthetic", "account-route-synthetic",
            "m3u-route-synthetic", "input-route-synthetic",
        )
        signed = sign_borrow_capability(
            secret=b"synthetic-secret-with-at-least-32-bytes",
            channel_uuid=identity[0], episode_id="episode-route-synthetic",
            user_id="user-route-synthetic", configuration_generation="config-route-synthetic",
            catalog_generation="catalog-route-synthetic", control_generation=3,
            recorder_fence=9, intent_epoch=7, connection_identity=identity,
            episode_version=1, expires_at=200, nonce="n" * 32,
        )
        token = encode_borrow_capability(signed)
        from catchuparr.adapters.sharing_borrow_route import decode_borrow_capability

        self.assertEqual(decode_borrow_capability(token), signed)
        runtime_module = _module(
            "catchuparr.sharing_runtime",
            open_borrow_session=lambda _capability: session,
        )
        calls = []

        def open_session(_capability):
            calls.append(True)
            return session

        runtime_module.open_borrow_session = open_session
        modules = {
            "django": django,
            "django.http": django_http,
            "catchuparr.sharing_runtime": runtime_module,
        }
        request.headers["X-Catchuparr-Borrow"] = token
        with patch.dict(sys.modules, modules):
            response = views.sharing_borrow_view(request, identity[0])
        self.assertEqual(calls, [True])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, "video/mp2t")
        self.assertEqual(list(response.streaming_content), [b"synthetic-ts"])
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")

    def test_signed_borrow_endpoint_denies_invalid_capability_without_echoing_it(self):
        from catchuparr import views

        class Response:
            def __init__(self, content="", status=200):
                self.content = content
                self.status_code = status
                self.headers = {}

            def __setitem__(self, key, value):
                self.headers[key] = value

        django_http = _module(
            "django.http", HttpResponse=Response,
            StreamingHttpResponse=Response,
        )
        django = _module("django", http=django_http)
        request = SimpleNamespace(method="GET", headers={})
        bearer = "opaque-synthetic-secret-capability"
        request.headers["X-Catchuparr-Borrow"] = bearer
        with patch.dict(sys.modules, {"django": django, "django.http": django_http}), patch(
            "catchuparr.adapters.sharing_borrow_route.decode_borrow_capability",
            side_effect=ValueError("invalid"),
        ):
            response = views.sharing_borrow_view(request, "channel-route-synthetic")
        self.assertEqual(response.status_code, 404)
        self.assertNotIn(bearer, response.content)
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")


if __name__ == "__main__":
    unittest.main()
