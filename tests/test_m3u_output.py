import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from catchuparr.adapters import m3u
from catchuparr.adapters.m3u import (
    annotate_m3u,
    annotate_native_m3u,
    build_catchup_source,
    filter_xmltv,
    merge_xmltv_snapshots,
    set_xmltv_playlist_urls,
    xmltv_channel_map_from_m3u,
)


class M3UOutputTests(unittest.TestCase):
  def test_annotate_m3u_preserves_live_id_url_and_unselected_entries(self):
    playlist = (
        '#EXTM3U\n'
        '#EXTINF:-1 tvg-id="dispatcharr.1" tvg-name="News" group-title="Local",News HD\n'
        'http://dispatcharr/live/dispatcharr.1\n'
        '#EXTINF:-1 tvg-id="other" catchup="append",Other\n'
        'http://dispatcharr/live/other\n'
    )

    result = annotate_m3u(
        playlist,
        {"dispatcharr.1": "archive channel/1"},
        "https://tv.example/plugin/archive",
        "opaque/token+value",
        catchup_days=2,
    )

    self.assertIn('#EXTINF:-1 tvg-id="dispatcharr.1"', result)
    self.assertIn('catchup="default"', result)
    self.assertIn('catchup-days="2"', result)
    self.assertIn('catchup-timezone="UTC"', result)
    self.assertIn("channel_id=archive%20channel%2F1", result)
    self.assertIn("access_token=opaque%2Ftoken%2Bvalue", result)
    self.assertNotIn("credential_id=", result)
    self.assertIn("utc={utc}&duration={duration}", result)
    self.assertIn("http://dispatcharr/live/dispatcharr.1", result)
    self.assertIn('#EXTINF:-1 tvg-id="other" catchup="append",Other', result)
    self.assertNotIn('catchup-timezone="UTC"', result.split('tvg-id="other"', 1)[1])
    self.assertTrue(result.endswith("http://dispatcharr/live/other\n"))


  def test_annotate_replaces_existing_local_catchup_values_without_duplicates(self):
    source = '#EXTINF:-1 tvg-id="one" catchup="append" catchup-days="7",One\n'
    result = annotate_m3u(
        source,
        {"one": "one"},
        "https://tv.example/catchup",
        "opaque-token",
    )
    self.assertEqual(result.count('catchup="'), 1)
    self.assertEqual(result.count('catchup-days="'), 1)
    self.assertIn('catchup="default"', result)
    self.assertIn('catchup-days="1"', result)
    self.assertEqual(result.count('catchup-timezone="UTC"'), 1)

  def test_xmltv_header_update_preserves_other_metadata_and_existing_queries(self):
    playlist = (
        '\ufeff#EXTM3U x-tvg-url="https://provider.example/guide?days=3&region=x" '
        'url-tvg="https://provider.example/legacy" cache="2"\r\n'
        '#EXTINF:-1,Channel\r\nhttps://provider.example/live\r\n'
    )
    result = set_xmltv_playlist_urls(
        playlist, 'https://plugin.example/xmltv?access_token=opaque%22token'
    )

    header, remainder = result.split("\r\n", 1)
    self.assertTrue(header.startswith('\ufeff#EXTM3U '))
    self.assertIn('cache="2"', header)
    self.assertEqual(header.count("x-tvg-url="), 1)
    self.assertEqual(header.count("url-tvg="), 1)
    self.assertIn('x-tvg-url="https://plugin.example/xmltv?access_token=opaque%22token"', header)
    self.assertIn('url-tvg="https://plugin.example/xmltv?access_token=opaque%22token"', header)
    self.assertEqual(remainder, '#EXTINF:-1,Channel\r\nhttps://provider.example/live\r\n')

  def test_xmltv_header_url_is_added_without_rewriting_non_m3u_content(self):
    self.assertEqual(
        set_xmltv_playlist_urls("#EXTM3U\n#EXTINF:-1,Channel\n", "https://guide.example/xmltv"),
        '#EXTM3U x-tvg-url="https://guide.example/xmltv" '
        'url-tvg="https://guide.example/xmltv"\n#EXTINF:-1,Channel\n',
    )
    plain = "#EXTINF:-1,Channel\nhttps://provider.example/live\n"
    self.assertEqual(set_xmltv_playlist_urls(plain, "https://guide.example/xmltv"), plain)

  def test_catchup_attributes_insert_after_quoted_commas(self):
    playlist = (
        '#EXTM3U\n'
        '#EXTINF:-1 tvg-id="local" group-title="News, World",Channel, HD\n'
        'https://provider.example/live/local\n'
    )
    result = annotate_m3u(
        playlist,
        {"local": "archive-local"},
        "https://plugin.example/archive",
        "opaque-token",
    )
    extinf = result.splitlines()[1]
    self.assertIn('group-title="News, World"', extinf)
    self.assertIn('catchup-source="https://plugin.example/archive?', extinf)
    self.assertLess(extinf.index('catchup="default"'), extinf.index(",Channel, HD"))
    self.assertTrue(result.endswith("https://provider.example/live/local\n"))

  def test_native_annotation_can_preserve_provider_catchup_and_live_url(self):
    playlist = (
        '#EXTM3U x-tvg-url="https://provider.example/guide?days=3"\n'
        '#EXTINF:-1 tvg-id="local" catchup="xc" catchup-days="5",Channel\n'
        'https://dispatcharr.example/proxy/ts/stream/local?output_profile=7&output_format=mpegts\n'
    )
    result = annotate_m3u(
        playlist,
        {"local": "local"},
        "https://plugin.example/archive",
        "opaque-token",
        preserve_existing_catchup=True,
    )
    self.assertEqual(playlist, result)

  def test_xmltv_channel_map_uses_effective_output_profile_id_and_eligible_streams(self):
    playlist = (
        '#EXTM3U\n'
        '#EXTINF:-1 tvg-id="profile.override" group-title="News, World",Channel\n'
        'https://dispatcharr.example/proxy/ts/stream/channel-1?profile=profile-7\n'
        '#EXTINF:-1 tvg-id="ambiguous" group-title="Local",First\n'
        'https://dispatcharr.example/proxy/ts/stream/channel-1\n'
        '#EXTINF:-1 tvg-id="ambiguous" group-title="Local",Second\n'
        'https://dispatcharr.example/proxy/ts/stream/channel-2\n'
        '#EXTINF:-1 tvg-id="not-eligible",Other\n'
        'https://dispatcharr.example/proxy/ts/stream/provider-channel\n'
    )
    self.assertEqual(
        xmltv_channel_map_from_m3u(playlist, {"channel-1", "channel-2"}),
        {"profile.override": "channel-1"},
    )


  def test_xmltv_shared_id_with_unselected_emitted_channel_is_ambiguous(self):
    playlist = (
        '#EXTINF:-1 tvg-id="shared",Selected\n'
        'https://dispatcharr.example/proxy/ts/stream/selected\n'
        '#EXTINF:-1 tvg-id="shared",Unselected\n'
        'https://dispatcharr.example/proxy/ts/stream/unselected\n'
    )
    self.assertEqual({}, xmltv_channel_map_from_m3u(playlist, {"selected"}))

  def test_xmltv_shared_id_with_direct_entry_is_ambiguous(self):
    playlist = (
        '#EXTINF:-1 tvg-id="shared",Selected\n'
        'https://dispatcharr.example/proxy/ts/stream/selected\n'
        '#EXTINF:-1 tvg-id="shared",Direct\n'
        'https://provider.example/direct/other\n'
    )
    self.assertEqual({}, xmltv_channel_map_from_m3u(playlist, {"selected"}))

  def test_unselected_proxy_uuid_does_not_fall_through_to_selected_tvg_id(self):
    playlist = (
        '#EXTINF:-1 tvg-id="selected",Unselected proxy\n'
        'https://dispatcharr.example/proxy/ts/stream/unselected\n'
    )
    result = annotate_m3u(
        playlist,
        {"selected": "selected"},
        "https://dispatcharr.example/archive",
        "opaque-token",
    )
    self.assertEqual(playlist, result)

  def test_native_annotation_only_adds_signed_caps_to_proxy_rows(self):
    playlist = (
        '#EXTM3U x-tvg-url="https://guide.example/epg"\n'
        '#EXTINF:-1 tvg-id="selected",Selected\n'
        'https://dispatcharr.example/proxy/ts/stream/selected?output_profile=7&output_format=mpegts\n'
        '#EXTINF:-1 tvg-id="direct",Direct\n'
        'https://provider.example/live/direct\n'
        '#EXTINF:-1 tvg-id="xc" catchup="xc" catchup-days="5",Provider XC\n'
        'https://dispatcharr.example/proxy/ts/stream/xc\n'
    )
    result = annotate_native_m3u(
        playlist,
        {"selected": "signed capability"},
        "https://dispatcharr.example/catchuparr/native/archive",
        catchup_days=3,
    )
    self.assertIn("output_profile=7&output_format=mpegts", result)
    self.assertIn('x-tvg-url="https://guide.example/epg"', result)
    self.assertIn("token=signed%20capability&utc={utc}&lutc={lutc}&playback=archive", result)
    self.assertNotIn("{duration}", result)
    self.assertIn('#EXTINF:-1 tvg-id="direct",Direct', result)
    self.assertIn('#EXTINF:-1 tvg-id="xc" catchup="xc" catchup-days="5",Provider XC', result)
    self.assertEqual(1, result.count('catchup="default"'))

  def test_url_template_rejects_embedded_secrets_and_keeps_placeholders(self):
    with self.assertRaisesRegex(ValueError, "embedded credentials"):
        build_catchup_source("https://user:secret@tv.example/archive", "ch", "opaque")
    template = build_catchup_source("https://tv.example/archive", "ch", "opaque")
    self.assertIn("access_token=opaque", template)
    self.assertIn("utc={utc}&duration={duration}", template)

  def test_tivimate_template_keeps_utc_and_duration_in_seconds(self):
    source = '#EXTINF:-1 tvg-id="local",Local\n'
    result = annotate_m3u(source, {"local": "local"}, "https://tv.example/archive", "token")
    self.assertIn('catchup-timezone="UTC"', result)
    self.assertIn("utc={utc}&duration={duration}", result)

  def test_provider_radio_and_other_entries_keep_their_metadata(self):
    playlist = (
        '#EXTINF:-1 tvg-id="local" radio="false" group-title="Local",Local TV\n'
        'http://dispatcharr/live/local\n'
        '#EXTINF:-1 tvg-id="provider" radio="true" catchup="append" catchup-days="14" group-title="Music",Provider Radio\n'
        'http://provider.example/radio\n'
        '#EXTINF:-1 tvg-id="other" tvg-name="Other",Other TV\n'
        'http://provider.example/other\n'
    )
    result = annotate_m3u(
        playlist, {"local": "archive-local"}, "https://tv.example/archive", "token"
    )
    self.assertIn('tvg-id="local" radio="false" group-title="Local"', result)
    self.assertIn('tvg-id="provider" radio="true" catchup="append" catchup-days="14" group-title="Music",Provider Radio', result)
    self.assertIn('#EXTINF:-1 tvg-id="other" tvg-name="Other",Other TV', result)
    self.assertEqual(result.count('catchup-timezone="UTC"'), 1)

  def test_live_proxy_uuid_can_match_when_effective_tvg_id_differs(self):
    proxy_id = "3e9e9aca-01ab-43de-9f14-323835d1ef25"
    playlist = (
        '#EXTINF:-1 tvg-id="profile.override",Channel\n'
        f"https://dispatcharr.example/proxy/ts/stream/{proxy_id}\n"
    )
    result = annotate_m3u(
        playlist,
        {proxy_id: "archive-channel"},
        "https://dispatcharr.example/archive",
        "opaque-token",
    )
    self.assertIn('catchup="default"', result)
    self.assertIn('tvg-id="profile.override"', result)
    self.assertIn(f"stream/{proxy_id}", result)

  def test_live_proxy_uuid_takes_precedence_over_conflicting_tvg_id(self):
    playlist = (
        '#EXTINF:-1 tvg-id="profile.shared",Channel\n'
        'https://dispatcharr.example/proxy/ts/stream/channel-live\n'
    )
    result = annotate_m3u(
        playlist,
        {"profile.shared": "wrong-channel", "channel-live": "channel-live"},
        "https://dispatcharr.example/archive",
        "opaque-token",
    )
    self.assertIn("channel_id=channel-live", result)
    self.assertNotIn("channel_id=wrong-channel", result)


  def test_xmltv_keeps_covered_past_and_all_current_future_entries(self):
    xml = '''<?xml version="1.0"?>
    <tv>
      <channel id="news"><display-name>News</display-name></channel>
      <programme channel="news" start="20261004190000 +0000" stop="20261004200000 +0000"><title>Covered</title></programme>
      <programme channel="news" start="20261004200000 +0000" stop="20261004210000 +0000"><title>Gap</title></programme>
      <programme channel="news" start="20261005200000 +0000" stop="20261005210000 +0000"><title>Future</title></programme>
    </tv>'''
    calls = []

    def covered(channel, start, stop):
        calls.append((channel, start, stop))
        return channel == "news" and start.hour == 19

    result = filter_xmltv(
        xml,
        covered,
        now=datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
    )

    self.assertIn("Covered", result)
    self.assertNotIn("Gap", result)
    self.assertIn("Future", result)
    self.assertIn("display-name", result)
    self.assertEqual(len(calls), 2)
    self.assertEqual(calls[0][1].tzinfo, timezone.utc)

  def test_xmltv_keeps_unselected_provider_history(self):
    xml = '''<tv>
      <programme channel="local" start="20261004190000 +0000" stop="20261004200000 +0000"><title>Local gap</title></programme>
      <programme channel="provider" start="20261004190000 +0000" stop="20261004200000 +0000"><title>Provider archive</title></programme>
    </tv>'''
    result = filter_xmltv(
        xml, lambda *_: False,
        now=datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
        local_channel_ids={"local"},
    )
    self.assertNotIn("Local gap", result)
    self.assertIn("Provider archive", result)
    disabled = filter_xmltv(
        xml, lambda *_: False,
        now=datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
        local_channel_ids=set(),
    )
    self.assertIn("Local gap", disabled)
    self.assertIn("Provider archive", disabled)


  def test_xmltv_retains_history_with_missing_or_invalid_bounds(self):
    xml = '''<tv>
      <programme channel="news" start="bad" stop="bad"><title>Unknown time</title></programme>
      <programme channel="news" start="20261004190000 +0000"><title>No stop</title></programme>
    </tv>'''
    result = filter_xmltv(
        xml,
        lambda *_: False,
        now=datetime(2026, 10, 5, 12, tzinfo=timezone.utc),
    )
    self.assertIn("Unknown time", result)
    self.assertIn("No stop", result)

  def test_xmltv_rejects_input_over_documented_size_limit(self):
    with patch.object(m3u, "MAX_XMLTV_BYTES", 4):
      with self.assertRaisesRegex(ValueError, "byte limit"):
        filter_xmltv("<tv />", lambda *_: True)

  def test_xmltv_restores_only_covered_missing_schedule(self):
    from datetime import timedelta

    now = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
    start = now - timedelta(hours=2)
    stop = now - timedelta(hours=1)
    program = {"start_utc": start, "end_utc": stop, "title": "Earlier news", "payload": {"description": "Summary"}}
    xml = '<tv><channel id="news" /></tv>'
    restored = merge_xmltv_snapshots(
        xml, {"news": [program], "other": [program]},
        lambda channel, *_: channel == "news", now=now,
    )
    self.assertIn("Earlier news", restored)
    self.assertIn("Summary", restored)
    self.assertEqual(restored.count("<programme"), 1)
    self.assertEqual(
        merge_xmltv_snapshots(restored, {"news": [program]}, lambda *_: True, now=now).count("<programme"),
        1,
    )

  def test_xmltv_snapshot_dedupe_compares_utc_instants_not_text(self):
    now = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
    xml = (
        '<tv><programme channel="news" start="20261004200000 +0000" '
        'stop="20261004210000 +0000"><title>Existing</title></programme></tv>'
    )
    snapshot = {
        "start_utc": datetime(2026, 10, 4, 22, tzinfo=timezone(timedelta(hours=2))),
        "end_utc": datetime(2026, 10, 4, 23, tzinfo=timezone(timedelta(hours=2))),
        "title": "Equivalent snapshot",
    }
    merged = merge_xmltv_snapshots(
        xml, {"news": [snapshot]}, lambda *_: True, now=now
    )
    self.assertEqual(merged.count("<programme"), 1)
    self.assertIn("Existing", merged)
    self.assertNotIn("Equivalent snapshot", merged)

  def test_xmltv_existing_offset_program_dedupes_against_utc_snapshot(self):
    now = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
    xml = (
        '<tv><programme channel="news" start="20261004220000 +0200" '
        'stop="20261004230000 +0200"><title>Existing offset</title></programme></tv>'
    )
    snapshot = {
        "start_utc": datetime(2026, 10, 4, 20, tzinfo=timezone.utc),
        "end_utc": datetime(2026, 10, 4, 21, tzinfo=timezone.utc),
        "title": "Equivalent UTC snapshot",
    }
    merged = merge_xmltv_snapshots(
        xml, {"news": [snapshot]}, lambda *_: True, now=now
    )
    self.assertEqual(merged.count("<programme"), 1)
    self.assertIn("Existing offset", merged)
    self.assertNotIn("Equivalent UTC snapshot", merged)


if __name__ == "__main__":
    unittest.main()
