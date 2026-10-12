# 0.5.0 candidate: native output archive integration

This candidate optionally adds Catchuparr archive catch-up metadata to
Dispatcharr's native M3U and XMLTV output. The setting is disabled by default.
The local suite and package checks pass. The first corrected-profile full AIO
runs on both pinned versions reached the native DIRECT-output comparison and
failed because archive metadata changed proxy fallback rows. Catchuparr now
leaves the complete native M3U response untouched when `direct=true`; both
full AIO reruns and independent review of this correction remain pending.
Native integration and player behavior are not yet accepted.

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

## Candidate evidence

The implementation passes the 431-test local suite, Ruff, compile checks and
package validation. The scripts-only AIO probe exercises real native endpoint
resolution, archive routes, permissions, byte ranges, session limits and Stats
Stop against the actual ORM and Redis. Full probes were attempted on both
pinned AIO images but stopped at the DIRECT-output equality gate before the
remaining playback checks. The direct-mode correction now preserves the whole
native response; reruns are required before native server acceptance. This
guide does not claim player-specific behavior.
