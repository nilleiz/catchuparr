"""Actual XC route acceptance for a bounded, independently muxed programme."""
from __future__ import annotations

import json
import subprocess
from datetime import timedelta
from urllib.parse import parse_qs, urlsplit


def probe_xc_timeline(*, root, store, channel, start, timeshift, request, xc_params, time_key,
                      views, hls_params, close_hls_session):
    source = root / "synthetic-timeline-source.ts"
    subprocess.run([
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
        "-t", "6", "-c:v", "mpeg2video", "-bf", "2", "-c:a", "mp2",
        "-f", "mpegts", str(source),
    ], check=True, capture_output=True, timeout=30)
    # Native XC admission requires the first six seconds to be covered.
    # Keep that guard exercised before the later intentional archive gap.
    # Stay within the fixture's one-hour retention for both native routes.
    selected_start = start - timedelta(minutes=30)
    for index, offset in enumerate((0, 12)):
        segment = root / f"synthetic-timeline-part-{index}.ts"
        segment.write_bytes(source.read_bytes())
        store.add_segment(
            str(channel.uuid), segment,
            selected_start + timedelta(seconds=offset),
            selected_start + timedelta(seconds=offset + 6), discontinuity=bool(index),
        )
    params = dict(xc_params, stream=str(channel.id), duration="1",
                  **{time_key: str(int(selected_start.timestamp()))})

    def response(range_header=None):
        meta = {"HTTP_USER_AGENT": "Synthetic programme timeline client",
                "REMOTE_ADDR": "127.0.0.70"}
        if range_header is not None:
            meta["HTTP_RANGE"] = range_header
        return timeshift.timeshift_proxy_query(request("/streaming/timeshift.php", params, **meta))

    full = response()
    if (full.status_code != 200 or full.get("X-Catchuparr-Archive-Timeline") != "available-media"
            or full.get("X-Catchuparr-Archive-Partial") != "true"):
        raise RuntimeError("Native XC programme clock mapping did not accept archived source resets")
    try:
        body = b"".join(full.streaming_content)
    finally:
        full.close()
    if len(body) != 2 * source.stat().st_size:
        raise RuntimeError("Native XC stopped after its first archive part")
    ranged = response("bytes=191-700")
    if ranged.status_code != 206:
        raise RuntimeError("Native XC mapped range did not preserve partial response status")
    try:
        if b"".join(ranged.streaming_content) != body[191:701]:
            raise RuntimeError("Native XC byte range differs from its programme resource")
    finally:
        ranged.close()
    output = root / "synthetic-timeline-programme.ts"
    output.write_bytes(body)
    duration = subprocess.run([
        "ffprobe", "-v", "error", "-show_entries", "format=duration", "-of",
        "default=nw=1:nk=1", str(output),
    ], check=True, capture_output=True, text=True, timeout=30)
    if abs(float(duration.stdout) - 12.1) > 0.15:
        raise RuntimeError("Native XC did not skip the missing archive interval at the source reset")
    def verify_decode(path):
        decoded = subprocess.run([
            "ffmpeg", "-v", "error", "-xerror", "-protocol_whitelist", "file,crypto,data",
            "-i", str(path), "-map", "0", "-vsync", "0", "-f", "framemd5", "-",
        ], check=False, capture_output=True, text=True, timeout=30)
        if decoded.returncode:
            # This diagnostic reads only generated synthetic fixture files.
            detail = decoded.stderr.replace(str(root), "synthetic")[-2000:]
            raise RuntimeError(f"Synthetic transition decode failed ({decoded.returncode}): {detail}")
        counts = {}
        for line in decoded.stdout.splitlines():
            if line and not line.startswith("#"):
                stream = line.split(",", 1)[0].strip()
                counts[stream] = counts.get(stream, 0) + 1
        if counts.get("0") != 300 or counts.get("1", 0) <= 300:
            raise RuntimeError("Native playback lost audio/video across its source reset")
        return counts

    counts = verify_decode(output)
    hls = views.archive_view(request(
        "/catchuparr/archive",
        dict(hls_params, utc=str(int(selected_start.timestamp())), duration="30"),
    ))
    if hls.status_code != 200:
        raise RuntimeError("Native HLS did not expose the synthetic archive transition")
    try:
        text = hls.content.decode()
        urls = [line for line in text.splitlines() if line and not line.startswith("#")]
        if len(urls) != 2 or text.count("#EXT-X-DISCONTINUITY") != 1:
            raise RuntimeError("Native HLS did not mark the exact synthetic source transition")
        hls_body = []
        for index, url in enumerate(urls):
            parts = urlsplit(url)
            query = {key: values[0] for key, values in parse_qs(parts.query).items()}
            path_parts = parts.path.rstrip("/").split("/")
            segment = views.segment_view(
                request(parts.path, query), path_parts[-2], path_parts[-1],
            )
            if segment.status_code != 200:
                raise RuntimeError("Native HLS failed to serve synthetic transition media")
            local = root / f"synthetic-hls-{index}.ts"
            local.write_bytes(segment.content)
            hls_body.append(segment.content)
            text = text.replace(url, local.name)
        if b"".join(hls_body) != body:
            raise RuntimeError("Native HLS and XC mapped different synthetic media timelines")
        local_playlist = root / "synthetic-hls.m3u8"
        local_playlist.write_text(text, encoding="utf-8")
        if verify_decode(local_playlist) != counts:
            raise RuntimeError("Native HLS and XC decoded different synthetic frame counts")
    finally:
        close_hls_session(hls, channel)
    print(json.dumps({"xc_programme_timeline": "pass", "media_seconds": 12.1,
                      "archive_span_seconds": 18, "missing_interval_skipped": True,
                      "decoded_video_frames": counts["0"], "decoded_audio_frames": counts["1"],
                      "hls_matches_xc": True, "byte_range_identical": True}))
