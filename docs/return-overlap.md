# Return overlap: returning from a delayed viewer stream without a gap

Status: specification, not implemented. Target: 0.7.9.

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

## Integration (after the planner is accepted)

- Ledger: a tail record next to the borrow episode ledger, written before the
  reader opens (`opening`), so a crash leaves a closable record.
- Native classification: `sharing_native._private_client` must classify the
  tail client as private through its own ledger key; it is no longer bound to
  the closed borrow episode. The native borrow check and viewer observation
  ignore it; a viewer arriving after the return deadline waits for the
  bounded tail to end.
- Trigger: the runtime asks the planner while the episode is `wait_return`;
  the tail opens as staged input of the borrowed recorder and joins with the
  archive-tail splice of `prepare_synchronized_return`. Its staging limit must
  cover the lead time plus the join.
- Join: borrow → tail uses the existing staged-input splice
  (`matching_completed_segments` / `_prepare_cut`).
- Seam: tail → dedicated reuses the audio path of `_try_synchronized_handover`
  with the dedicated input as staged input (`max_seconds` already covers
  `handover_overlap_seconds`).
- Authority: the tail must not hold the sharing operation pointer, so
  `issue_recorder_attempt` still admits the dedicated attempt. Verify in the
  native CI that the authority revision bump at borrow cleanup does not
  invalidate the tail's reader proof.
- `scripts/aio_recorder_failover.py` pins line numbers in `tasks.py`; update
  them with every change above them.

Open questions to settle in the native CI (Dispatcharr 0.32 and 0.33): whether
the native channel stays up for the tail after the borrowed client leaves, and
whether a private client opened while the borrow is active survives the
borrow's cleanup unchanged.

## Rejected alternatives

- Opening the dedicated source before the borrow cleanup: it would run without
  a RETURN token and break the authority protocol.
- An ordinary extra client on the viewer channel: it counts as a viewer.
- Keeping the borrowed reader beyond its cleanup: cleanup closes its stream.

## Tests

`tests/test_sharing_return_tail.py` covers eligibility, every transition in the
table, deadlines, stale and duplicate events, the single-publisher invariant
over random event sequences, snapshot round trip and recovery.
