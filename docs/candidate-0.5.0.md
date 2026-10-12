# 0.5.0 native output archive integration

Version 0.5.0 adds optional Catchuparr archive catch-up metadata to Dispatcharr's
native M3U and XMLTV output. The setting is disabled by default. Implementation
and user acceptance are complete; publication awaits the required release
workflow.

## Enable native output

1. Set **User ID for new M3U token** to an active Dispatcharr user who is
   allowed to watch the channels and use catch-up.
2. In the same plugin settings form, enable **Add Catchuparr catch-up to native
   output**.
3. Choose **Validate configuration**, review the complete preview, then choose
   **Apply configuration** once. Saving the draft alone does not enable the
   feature. No container restart is required by the feature.
4. Refresh the native M3U and XMLTV exports after applying. Existing
   token-authenticated Catchuparr M3U/XMLTV links continue to work independently.

The switch affects Dispatcharr's native output endpoints. Catchuparr evaluates
the current applied channel selection, native output profile, configured
playback user, channel permissions, archive coverage and current network rules
before it adds metadata. The native playlist remains the source of truth for
which channels and live stream URLs it emits. Query parameters used by native
live stream URLs, including output-profile choices, remain on those URLs.

For ordinary native M3U requests, Catchuparr adds catch-up attributes only to
eligible proxy-stream entries that do not already have provider catch-up
attributes. A native M3U request with `direct=true` remains byte-for-byte as
Dispatcharr rendered it, including its header, provider metadata and any proxy
fallback rows; Catchuparr adds no archive attributes to that export. The
`direct` parameter is an M3U rendering option and does not change the separate
native XMLTV augmentation policy. XMLTV history is added only when an emitted
guide ID maps to one unambiguous eligible channel. Ambiguous IDs are skipped;
existing provider programmes are kept, with duplicate instants compared in
UTC.

Archive playback serves the stored MPEG-TS segments. A native live
`output_profile` does not transcode or otherwise transform archive playback.
This path has not been shown to provide frame-accurate seeking or automatic
continuation across every player's programme boundaries.

## Access and limits

Dispatcharr's native M3U/XMLTV endpoints retain their own network checks. For
the anonymous native renderer, Catchuparr uses the configured playback user as
the admission identity; the player importing the generated playlist does not
need a browser cookie or a Catchuparr token. The user must remain active, have
catch-up enabled, be allowed to view the current channel in the selected native
profile, and be permitted by both the `M3U_EPG` and `STREAMS` network rules.
Catchuparr checks those conditions again when an archive request arrives.

Each eligible channel receives a signed capability scoped to the applied
playback user, channel and native output profile. It is separate from the
token-authenticated Catchuparr export credentials and does not grant access to
other Catchuparr routes. Current user, profile, network, channel selection,
catch-up, toggle and signing-key state are checked on playback. Dispatcharr's
current stream/session limits still apply. Session identity includes the export
capability and request network/client metadata; it is not a physical-device
identity, so copied playlists can share an identity when their request metadata
matches.

The signing key is stored in the private plugin state directory, outside the
replaceable plugin ZIP. Applying the feature after it was disabled rotates the
key and invalidates old native capabilities. Refresh native output after
re-enabling it or changing the configured playback user or archive location.

## Disable and rollback

Disable **Add Catchuparr catch-up to native output** and Apply the complete
settings once to remove Catchuparr additions from subsequent native exports.
Existing signed capabilities are rejected after the toggle is disabled or its
key is rotated. This does not delete archive media or change Dispatcharr's
provider assignments.

For an application/configuration rollback, preserve the matching private
Catchuparr active-settings and native-keyring state files together with the
Dispatcharr configuration backup. They are outside the plugin ZIP and archive
directory. If a matching keyring is unavailable after restore, old capabilities
fail closed; apply the settings again and refresh native outputs. Keep no more
than two verified task-owned application/configuration backups. Do not include
the archive, recordings or archive database in these backups.

## Implementation and acceptance evidence

The earlier implementation passed 433 tests, Ruff, Python compilation and
package validation. The 30-entry package had SHA-256
`e710fb265a70ed4c12b585ff750e02c4912a31bab417593d2f258c386996434a`.

The full synthetic AIO probe passed on Dispatcharr 0.31.0
(`f81924fa3dbfeb463b3908be7e086bf58aacbd2ba56062bfb555ee3a471acf8f`) and
0.32.0 (`b7d695c5cc98b9abd64c74a94539c1ebffc23d965021613c820c67e25dea41a`).
Each run passed 32 AIO tests and the integration gates for native M3U/XMLTV
output, signed archive access, byte ranges, current permission and session
checks, Stats Stop and cleanup. The runs completed separately; they do not
establish concurrent-run behavior. No container data, recordings or credentials
were retained as artifacts.

The final parity correction makes standard and add-on playback routes share the
same archive manifest selection for current, completed and gapped archives. It
passed four focused native 0.32.0 cases, including authorization and HEAD
cleanup. The complete historical suite and 0.31.0 probes were not rerun after
that correction. The final correction received independent review.

User acceptance is complete. The server-side evidence does not establish every
player's compatibility, archive output-profile transcoding or frame-accurate
seeking. Required release CI remains the publication gate.
