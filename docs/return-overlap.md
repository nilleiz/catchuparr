# Return overlap: returning from a delayed viewer stream without a gap

Status: implemented in 0.7.9.

## Problem

While a viewer watches a channel, the recorder borrows the viewer's native
connection. When the last viewer has left and `last_viewer_return_seconds` has
passed, the borrowed reader is cleaned up and the recorder returns to its
dedicated source.

Two providers carrying the same channel can differ in latency by a minute or
more. When the viewer's source is behind the dedicated source, the archive
ends at programme time `T - d` while the dedicated source starts at `T`. Today
the return therefore leaves a gap of about `d` seconds.

0.7.8 solved the opposite direction (a delayed dedicated source handing over
to a faster viewer stream): the old input keeps recording while the new one is
staged, and the audio comparison (`engine/offset.py`) finds the seam. The
return cannot reuse that directly:

1. The dedicated source is authorized only by the RETURN token, which exists
   only after the borrowed reader's exact cleanup was acknowledged.
2. Cleaning up the borrowed reader closes its native stream; it cannot keep
   reading after cleanup.
3. Any additional reader on the viewer channel must be a classified private
   client. An ordinary client would count as a viewer and start a new borrow;
   an unclassified client blocks attach (`native-client-classification-unknown`).

## Approach: a bounded private tail reader

A second private reader, the *tail reader*, is opened on the viewer channel
while the borrow waits for its return (`wait_return`), shortly before the
return deadline. The native borrow check rejects every publication of the
borrowed reader from the return deadline on (`return-deadline-reached`), so
the tail must already be reading by then; opening it at the deadline would be
too late. After the deadline the tail joins the frozen archive tail by the
existing packet-hash splice (same source, identical packets), takes over
archive publication, and keeps recording the delayed stream after the borrowed
reader's cleanup. The RETURN token is issued as today. The dedicated source is
then opened as a staged input, and the audio comparison looks for the seam
exactly as in `_try_synchronized_handover`. At the seam the tail freezes and
the dedicated input publishes from `preset_skip_until` with `frozen = True`.

```
publisher:  borrow ──┤join├── tail ─────────────────┤seam├── dedicated
borrow:     wait_return ──┤deadline├── cleanup ── RETURN token
tail:       open ─ registered ─ joined ─ publishing ───── frozen ── closing ── closed
dedicated:                          staged (under token) ── publishing
```

The tail is a bounded deviation from today's return: every failure, timeout or
stop falls back to the existing return path, never to a longer one.

## Eligibility

The runtime asks the planner while the borrow is in `wait_return`. A request
more than `TAIL_LEAD_SECONDS` (20 s, three default segments plus margin)
before the return deadline is answered with no action (`not_yet`); the runtime
asks again later. Otherwise the tail is planned only when all of these hold;
if not, the return runs exactly as today (`plain_return`):

- `synchronize_before_switch` is enabled;
- `handover_max_overlap_seconds` is an integer from 10 to 120;
- the borrowed connection's account is one of `handover_delayed_account_ids`;
- the first dedicated candidate's account is known and is not delayed;
- a fresh tail id is supplied;
- at least `MIN_LEAD_SECONDS` (8 s) remain until the return deadline, so the
  tail holds a complete segment that overlaps the borrow's last one.

## State machine

The planner is pure (`catchuparr/sharing_return_tail.py`): it compares events
and timestamps and returns the next state plus the actions the runtime must
perform. It never touches Dispatcharr, Redis, processes or the archive.

### Phases

| Phase | Meaning | Archive publisher |
| --- | --- | --- |
| `idle` | No tail planned | borrow |
| `opening` | Tail ledger entry written, reader requested, not registered | borrow |
| `joining` | Tail registered as private client, staged behind the borrow; joins after the return deadline | borrow |
| `tailing` | Tail publishes; borrow cleanup and RETURN token pending; dedicated may be requested | tail |
| `seeking` | Dedicated staged under the RETURN token; audio seam search | tail |
| `closing` | Tail close requested | dedicated or none |
| `cleanup_pending` | Tail close not yet acknowledged; retried | dedicated or none |
| `closed` | Terminal | dedicated or none |

### Transitions

Events carry the tail id; an event for another tail id is ignored. Events in
`closed` are ignored. Repeated events are idempotent.

| From | Event | To | Actions | Outcome |
| --- | --- | --- | --- | --- |
| (none) | request before the lead window | `idle` | | `not_yet` |
| (none) | request, ineligible | `idle` | `plain_return` | `ineligible` |
| (none) | request, eligible | `opening` | `open_tail` | |
| `opening` | tail registered (`catchuparr-client-` id) | `joining` | | |
| `opening` | tail registered, other id | `closing` | `close_tail`, `plain_return` | `invalid_client` |
| `opening`, `joining` | viewer returned (episode `active` again), before the return deadline | `closing` | `close_tail` | `viewer_returned` |
| `opening`, `joining` | return deadline reached | unchanged | | |
| `joining` | tail joined, after the return deadline | `tailing` | `cleanup_borrow` | |
| `opening`, `joining` | join failed, tail failed, or join deadline | `closing` | `close_tail`, `plain_return` | `join_failed` |
| `tailing` | borrow cleaned | `tailing` | | |
| `tailing` | RETURN token, borrow cleaned | `tailing` | `stage_dedicated` | |
| `tailing` | dedicated staged | `seeking` | | |
| `tailing` | tail failed, or hard deadline | `closing` | `close_tail` | `tail_failed` / `deadline` |
| `tailing`, `seeking` | dedicated failed | `closing` | `close_tail` | `dedicated_failed` |
| `seeking` | seam found (tail publication frozen) | `closing` | `switch_to_dedicated`, `close_tail` | `seam` |
| `seeking` | tail failed, or seam deadline | `closing` | `fallback_to_dedicated`, `close_tail` | `tail_failed` / `deadline` |
| any active | stop | `closing` | `close_tail` (+ `close_dedicated_staged` in `seeking`) | `stopped` |
| `closing`, `cleanup_pending` | tail closed | `closed` | | |
| `closing` | tail close failed | `cleanup_pending` | | |
| `cleanup_pending` | tick | `cleanup_pending` | `close_tail` | |
| `closing`, `cleanup_pending` | tick after cleanup deadline | `cleanup_pending` | `close_tail`, `stop_recorder` | |

"Active" means `opening`, `joining`, `tailing` or `seeking`. A RETURN token
before the borrow cleanup was acknowledged, and a join before the return
deadline, are ignored. After `viewer_returned` the borrow simply continues; a
later `wait_return` may plan a new tail with a new tail id. A viewer arriving
after the return deadline does not interrupt the tail (see below). `fallback_to_dedicated`
is the existing unsynchronized return; `plain_return` is today's return path
for a borrow that still publishes.

### Deadlines

All deadlines are absolute and fixed when they start; no event extends them.
Every event first applies any deadline that has passed at its time, so no
active phase survives its hard deadline, whatever event arrives.

- Join deadline: `return_deadline + JOIN_TIMEOUT_SECONDS` (12 s, as
  `SYNCHRONIZED_HANDOVER_TIMEOUT_SECONDS`).
- Hard deadline: `return_deadline + max_overlap_seconds + TAIL_ALLOWANCE_SECONDS`
  (60 s for join, cleanup and token latency). It bounds how long the viewer's
  provider connection is held beyond today's return and is persisted.
- Seam deadline: `min(hard deadline, staged_at + max_overlap_seconds + SEAM_MARGIN_SECONDS)`
  (20 s for the minimum audio overlap and segment closing).
- Cleanup deadline: `closing_at + TAIL_CLEANUP_SECONDS` (30 s). After it the
  recorder stops, as with an unacknowledged borrow cleanup.

### Invariants

1. At most one input publishes archive segments at any time (`publisher`).
2. The borrowed reader is cleaned up only after the tail took over publication
   or the tail was abandoned (`plain_return`). Its cleanup and the RETURN token
   protocol are unchanged.
3. The dedicated source is opened only under the RETURN token, after the
   acknowledged borrow cleanup.
4. The tail never outlives its hard deadline plus the cleanup deadline without
   the recorder stopping.
5. A tail is never resumed after a restart: any persisted tail that is not
   `closed` is recovered into `closing` with `close_tail`; a snapshot that
   cannot be validated is recovered into `cleanup_pending` with `close_tail`,
   and the runtime closes every tail client of that channel.
6. Every path ends in `closed` or in `stop_recorder`.

### Return acknowledgement

The RETURN is acknowledged (`acknowledge_return_media`) with the first segment
the dedicated input publishes, so with a tail only after `switch_to_dedicated`
or `fallback_to_dedicated`; no planner or coordinator timer treats the later
acknowledgement as a failure. While the RETURN is pending, the supervisor
suppresses new ATTACHs; this is the bounded wait of a viewer arriving during
the tail. The acknowledgement revalidates the dedicated attempt's sharing
operation and authority revision, and a non-retryable mismatch stops the
recorder. Therefore tail registration, heartbeat and cleanup must never change
the sharing authority revision or the operation pointer; the native CI must
show an acknowledgement after a seam succeeding.

### Persistence

`TailState.to_snapshot()` holds the tail id, channel, recorder fence, phase,
client id, `opened_at`, the return, join and hard deadlines, `closing_at` and
the outcome. It
never holds a URL, token or capability. Restoring requires the same channel.

## Implementation

- Planner: `catchuparr/sharing_return_tail.py` (pure).
- Runtime: `catchuparr/sharing_tail.py`. `NativeTailReader` registers a
  task-local client inside the recorder process with
  `BorrowedClientRegistration`, marks it with the running recorder's private
  Stats marker before it is registered, and reads the native buffer with
  `ReadOnlyTsBufferReader`. Each read is proven by a read-only Lua check of the
  borrowed connection's owner, metadata, worker heartbeat, recorder lease and
  stopping marker until the hard deadline; `NativeClientActivityGuard`
  refreshes `last_active` after every chunk written to FFmpeg.
- Input: `PipedFFmpegRecorder` feeds those chunks to FFmpeg's stdin; a failing
  reader closes stdin, so FFmpeg ends like a closed input.
- Join: while the tail is staged, the borrowed input discards its final partial
  segment (`final_segment_min_seconds`), so the archive ends at a segment
  boundary; `join_staged_after_archive_tail` then continues it at the exact
  packet splice of `prepare_synchronized_return`.
- Seam: `ReturnTail.hand_over` offers the dedicated recorder to the running
  tail through `RecorderAttemptSwitch`; `_try_synchronized_handover` finds the
  audio seam as in 0.7.8 and returns the frozen dedicated input, which then
  publishes and acknowledges the RETURN.
- Recovery: a Redis record `catchuparr:sharing:tail:<channel>` with the exact
  client fields and a TTL is written after registration. A recorder start
  removes such a client exactly before any sharing work; a tail is never
  resumed. Redis replaces the SQLite ledger of the first draft because the
  native client itself only exists in Redis.
- Wiring: `tasks._start_return_tail_watch` polls the episode projection every
  0.5 s during the borrow; the dedicated attempt calls `hand_over` instead of
  `prepare_synchronized_return`. Line pins in `scripts/aio_recorder_failover.py`
  follow `tasks.py`.

Native evidence (synthetic AIO, Dispatcharr 0.33.0,
`scripts/aio_return_tail_probe.py` inside the media probe's borrow return):

- A second task-local client marked with the running recorder's private Stats
  marker (`catchuparr:stats:recorder-client:*`, bound to the recorder lease, not
  to the borrow episode) is classified private by the Python and Lua checks:
  the borrow stays in `wait_return` and returns at its native deadline.
- Registering it changes neither the sharing authority nor the operation
  pointer, and the RETURN is acknowledged while it is present.
- After the borrowed client's exact cleanup the native channel stays up for
  it alone, and its buffer keeps advancing with readable chunks.
- Dispatcharr removes a client that reports no activity as a ghost after
  about 50 s, and then stops the channel. The tail reader must therefore
  refresh `last_active` of its existing client hash at least once per second,
  as the borrowed reader does. The tail classification can reuse the
  recorder marker; no Lua change is needed for it.

Not yet covered: Dispatcharr 0.32, a tail alone for the full
`max_overlap_seconds`, and the packet join and audio seam on real archive
segments.

## Rejected alternatives

- Opening the dedicated source before the borrow cleanup: it would run without
  a RETURN token and break the authority protocol.
- An ordinary extra client on the viewer channel: it counts as a viewer.
- Keeping the borrowed reader beyond its cleanup: cleanup closes its stream.

## Tests

`tests/test_sharing_return_tail.py` covers eligibility, every transition in the
table, deadlines, stale and duplicate events, the single-publisher invariant
over random event sequences, snapshot round trip and recovery.
