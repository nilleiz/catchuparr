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


if __name__ == "__main__":
    unittest.main()
