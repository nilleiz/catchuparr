"""Actual XC route acceptance for a bounded, independently muxed programme."""
from __future__ import annotations

import json
import subprocess
from datetime import timedelta


def probe_xc_timeline(*, root, store, channel, start, timeshift, request, xc_params, time_key):
    source = root / "synthetic-timeline-source.ts"
    subprocess.run([
        "ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=64x64:rate=25",
        "-t", "2", "-c:v", "mpeg2video", "-bf", "2", "-f", "mpegts", str(source),
    ], check=True, capture_output=True, timeout=30)
    selected_start = start - timedelta(hours=2)
    for index, offset in enumerate((0, 12)):
        segment = root / f"synthetic-timeline-part-{index}.ts"
        segment.write_bytes(source.read_bytes())
        store.add_segment(
            str(channel.uuid), segment,
            selected_start + timedelta(seconds=offset),
            selected_start + timedelta(seconds=offset + 2), discontinuity=bool(index),
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
    if (full.status_code != 200 or full.get("X-Catchuparr-Archive-Timeline") != "programme"
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
    if abs(float(duration.stdout) - 14) > 0.15:
        raise RuntimeError("Native XC exposed a segment duration instead of its archive programme span")
    decoded = subprocess.run([
        "ffmpeg", "-v", "error", "-xerror", "-i", str(output), "-map", "0:v:0",
        "-vsync", "0", "-f", "framemd5", "-",
    ], check=True, capture_output=True, text=True, timeout=30)
    count = sum(1 for line in decoded.stdout.splitlines() if line and not line.startswith("#"))
    if count != 100:
        raise RuntimeError("Native XC lost decodable frames across its source reset")
    print(json.dumps({"xc_programme_timeline": "pass", "programme_seconds": 14,
                      "decoded_frames": count, "byte_range_identical": True}))
