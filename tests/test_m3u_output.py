import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from catchuparr.adapters import m3u
from catchuparr.adapters.m3u import (
    annotate_m3u,
    build_catchup_source,
    filter_xmltv,
    merge_xmltv_snapshots,
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
    self.assertIn("channel_id=archive%20channel%2F1", result)
    self.assertIn("access_token=opaque%2Ftoken%2Bvalue", result)
    self.assertNotIn("credential_id=", result)
    self.assertIn("utc={utc}&duration={duration}", result)
    self.assertIn("http://dispatcharr/live/dispatcharr.1", result)
    self.assertIn('#EXTINF:-1 tvg-id="other" catchup="append",Other', result)
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


  def test_url_template_rejects_embedded_secrets_and_keeps_placeholders(self):
    with self.assertRaisesRegex(ValueError, "embedded credentials"):
        build_catchup_source("https://user:secret@tv.example/archive", "ch", "opaque")
    template = build_catchup_source("https://tv.example/archive", "ch", "opaque")
    self.assertIn("access_token=opaque", template)
    self.assertIn("utc={utc}&duration={duration}", template)

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


if __name__ == "__main__":
    unittest.main()
