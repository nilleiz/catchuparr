import logging
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from catchuparr.adapters import recorder_proxy


def _module(name, **attributes):
    module = types.ModuleType(name)
    module.__dict__.update(attributes)
    module.__path__ = []
    return module


def _dispatcharr_api_modules():
    def reserve_profile_slot(profile, redis_client):
        return True, 1, None

    def release_profile_slot(profile_id, redis_client):
        return None

    def stream(request, channel_id):
        return ("native-stream", channel_id)

    def create_stream_generator(
        channel_id,
        client_id,
        client_ip,
        client_user_agent,
        channel_initializing,
        user,
        buffer,
        channel_name,
    ):
        return None

    def resolve_url(stream, m3u_account, m3u_profile):
        return None

    def channel_setup_needed(proxy_server, channel_id):
        return False, None, False

    def get_client_ip(request):
        return "synthetic-client-ip"

    def get_alternate_streams(channel_id):
        return ["native-alternate"]

    def channel_metadata_for_key(channel_id):
        return f"metadata:{channel_id}"

    def channel_owner_for_key(channel_id):
        return f"owner:{channel_id}"

    class ChannelStream:
        objects = object()

    class ChannelMetadataField:
        STREAM_ID = "stream_id"
        M3U_PROFILE = "m3u_profile"
        STREAM_PROFILE = "stream_profile"

    class RedisKeys:
        channel_metadata = staticmethod(channel_metadata_for_key)
        channel_owner = staticmethod(channel_owner_for_key)

    class ProxyServer:
        redis_client = None

        def _release_stream_resources(self, channel_id):
            return ("released", channel_id)

        def try_acquire_ownership(self, channel_id, ttl):
            return True

        def _get_channel_init_lock(self, channel_id):
            return None

        def _finish_channel_init_lock(self, channel_id, lock):
            return None

        def get_buffer(self, channel_id, profile):
            return None

        def initialize_channel(self, url, channel_id, user_agent, transcode, stream_id):
            return True

    class ChannelService:
        logger = logging.getLogger("synthetic.channel-service")

        @staticmethod
        def initialize_channel(
            channel_id,
            stream_url,
            user_agent,
            transcode,
            stream_profile_value,
            stream_id,
            m3u_profile_id,
            channel_name,
            stream_name,
        ):
            return True

        @staticmethod
        def is_channel_unavailable_for_new_clients(channel_id):
            return False

        @staticmethod
        def stop_channel(channel_id):
            return None

    core = _module("apps")
    channels = _module("apps.channels")
    channel_models = _module("apps.channels.models", ChannelStream=ChannelStream)
    m3u = _module("apps.m3u")
    pool = _module(
        "apps.m3u.connection_pool",
        reserve_profile_slot=reserve_profile_slot,
        release_profile_slot=release_profile_slot,
    )
    proxy = _module("apps.proxy")
    live_proxy = _module("apps.proxy.live_proxy")
    live_urls = _module(
        "apps.proxy.live_proxy.urls",
        urlpatterns=[SimpleNamespace(name="stream", callback=stream)],
    )
    constants = _module(
        "apps.proxy.live_proxy.constants", ChannelMetadataField=ChannelMetadataField
    )
    input_module = _module("apps.proxy.live_proxy.input")
    input_manager = _module(
        "apps.proxy.live_proxy.input.manager",
        get_alternate_streams=get_alternate_streams,
        logger=logging.getLogger("synthetic.input-manager"),
    )
    input_module.manager = input_manager
    output = _module("apps.proxy.live_proxy.output")
    output_ts = _module("apps.proxy.live_proxy.output.ts")
    generator = _module(
        "apps.proxy.live_proxy.output.ts.generator",
        create_stream_generator=create_stream_generator,
    )
    redis_keys = _module("apps.proxy.live_proxy.redis_keys", RedisKeys=RedisKeys)
    server = _module(
        "apps.proxy.live_proxy.server",
        ProxyServer=ProxyServer,
        logger=logging.getLogger("synthetic.proxy-server"),
    )
    services = _module("apps.proxy.live_proxy.services")
    channel_service = _module(
        "apps.proxy.live_proxy.services.channel_service",
        ChannelService=ChannelService,
        logger=ChannelService.logger,
    )
    url_utils = _module(
        "apps.proxy.live_proxy.url_utils",
        _resolve_live_stream_url=resolve_url,
        logger=logging.getLogger("synthetic.url-utils"),
    )
    proxy_views = _module(
        "apps.proxy.live_proxy.views", _channel_setup_needed=channel_setup_needed
    )
    dispatcharr = _module("dispatcharr")
    dispatcharr_utils = _module("dispatcharr.utils", get_client_ip=get_client_ip)

    class RegistryRedis:
        def ping(self):
            return True

        def scan(self, cursor, **_kwargs):
            return 0, []

    class RedisClient:
        @staticmethod
        def get_client():
            return RegistryRedis()

    core_utils = _module("core.utils", RedisClient=RedisClient)
    core_package = _module("core")
    django = _module("django")

    class HttpResponseNotFound:
        def __init__(self):
            self.status_code = 404

    django_http = _module("django.http", HttpResponseNotFound=HttpResponseNotFound)

    modules = {
        "apps": core,
        "apps.channels": channels,
        "apps.channels.models": channel_models,
        "apps.m3u": m3u,
        "apps.m3u.connection_pool": pool,
        "apps.proxy": proxy,
        "apps.proxy.live_proxy": live_proxy,
        "apps.proxy.live_proxy.urls": live_urls,
        "apps.proxy.live_proxy.constants": constants,
        "apps.proxy.live_proxy.input": input_module,
        "apps.proxy.live_proxy.input.manager": input_manager,
        "apps.proxy.live_proxy.output": output,
        "apps.proxy.live_proxy.output.ts": output_ts,
        "apps.proxy.live_proxy.output.ts.generator": generator,
        "apps.proxy.live_proxy.redis_keys": redis_keys,
        "apps.proxy.live_proxy.server": server,
        "apps.proxy.live_proxy.services": services,
        "apps.proxy.live_proxy.services.channel_service": channel_service,
        "apps.proxy.live_proxy.url_utils": url_utils,
        "apps.proxy.live_proxy.views": proxy_views,
        "dispatcharr": dispatcharr,
        "dispatcharr.utils": dispatcharr_utils,
        "core": core_package,
        "core.utils": core_utils,
        "django": django,
        "django.http": django_http,
    }
    for name, module in modules.items():
        if "." in name:
            parent, child = name.rsplit(".", 1)
            setattr(modules[parent], child, module)
    return modules, ProxyServer, input_manager, live_urls


class RecorderProxyHookTests(unittest.TestCase):
    def test_hook_install_twice_and_shutdown_restore_originals(self):
        modules, proxy_server, input_manager, live_urls = _dispatcharr_api_modules()
        original_release = proxy_server._release_stream_resources
        original_alternates = input_manager.get_alternate_streams
        original_route = live_urls.urlpatterns[0].callback

        with patch.dict(sys.modules, modules):
            self.assertEqual((), recorder_proxy._core_api_compatibility_issues())
            self.assertTrue(recorder_proxy.install_proxyserver_cleanup_hook())
            guarded_release = proxy_server._release_stream_resources
            guarded_alternates = input_manager.get_alternate_streams
            guarded_route = live_urls.urlpatterns[0].callback

            self.assertTrue(recorder_proxy.install_proxyserver_cleanup_hook())
            self.assertIs(proxy_server._release_stream_resources, guarded_release)
            self.assertIs(input_manager.get_alternate_streams, guarded_alternates)
            self.assertIs(live_urls.urlpatterns[0].callback, guarded_route)
            self.assertEqual(404, guarded_route(None, "catchuparr-r" + "a" * 40).status_code)
            self.assertEqual(("native-stream", "ordinary-id"), guarded_route(None, "ordinary-id"))
            self.assertEqual([], guarded_alternates("catchuparr-r" + "a" * 40))
            self.assertEqual(["native-alternate"], guarded_alternates("ordinary-id"))

            self.assertTrue(recorder_proxy.uninstall_proxyserver_cleanup_hook())
            self.assertIs(proxy_server._release_stream_resources, original_release)
            self.assertIs(input_manager.get_alternate_streams, original_alternates)
            self.assertIs(live_urls.urlpatterns[0].callback, original_route)
            self.assertFalse(any(
                getattr(filter_object, "_catchuparr_url_redactor", False)
                for logger_object in (
                    recorder_proxy.logger,
                    input_manager.logger,
                    modules["apps.proxy.live_proxy.server"].logger,
                    modules["apps.proxy.live_proxy.services.channel_service"].logger,
                    modules["apps.proxy.live_proxy.url_utils"].logger,
                )
                for filter_object in logger_object.filters
            ))

    def test_compatibility_diagnostic_names_rejected_parameter_only(self):
        modules, proxy_server, _input_manager, _live_urls = _dispatcharr_api_modules()

        def incompatible_initialize(self, url, channel_id, user_agent, transcode):
            return None

        with patch.dict(sys.modules, modules):
            proxy_server.initialize_channel = incompatible_initialize
            issues = recorder_proxy._core_api_compatibility_issues()

        self.assertEqual(
            ("parameters:ProxyServer.initialize_channel:stream_id",),
            tuple(issue for issue in issues if issue.startswith("parameters:ProxyServer.initialize_channel:")),
        )


if __name__ == "__main__":
    unittest.main()
