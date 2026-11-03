# Show rules (0.8.0 specification, neighbours since 0.8.2)

A rule selects either whole channels (`channels:`) or programmes (`shows:`);
using both in one rule is a validation error. A `shows` rule records every
EPG programme that matches, plus optional padding, on the channels it covers.

```yaml
rules:
  - shows:
      name: Evening news            # optional label for logs and the preview
      match:
        title:
          any:
            - starts_with: "News"
            - regex: '^Late( show)?$'
          not: { contains: "Weather" }
        description: { contains: "report" }   # optional, fields combine with AND
        min_minutes: 5                        # optional programme length bounds
      channels:
        include: { groups: [Example group] }  # optional, default: all channels with EPG
        exclude: { names: [Example channel] }
      padding: { before: 2, after: 10 }       # minutes, 0-240
      include_previous: { min: 10, max: 60 }  # optional, since 0.8.2
      include_next: true                      # optional, since 0.8.2
    m3u: { include: [Example provider] }      # nested provider filter
    schedule: { weekdays: [{ start: "18:00", end: "23:00" }] }
```

- Text operators: `equals`, `starts_with`, `ends_with`, `contains` (alias
  `includes`), `wildcard` (`*`, `?`) and `regex` (Python `re`, searched, at most
  200 characters, nested repetition rejected). Each takes one text or a list
  (any of); `any`, `all` and `not` combine conditions. Matching ignores case and
  is Unicode-normalized unless `case_sensitive: true`.
- Neighbouring programmes (0.8.2): `include_previous` and `include_next` also
  record the programme directly before or after a match on the same channel.
  `true` takes the whole neighbour; a mapping takes optional `min` and `max`
  minutes (1-1440, `min` not above `max`):
  - `min`: if the neighbour is shorter than `min`, exactly one more programme
    beyond it is included as well (never a third one).
  - `max`: caps the whole extra time before the start or after the end of the
    match, also when `min` added a second programme.
  - A neighbour must touch the match (gap up to 60 s); without one, only the
    padding applies.
  - `padding.before` applies to the start of the included programmes before the
    match, `padding.after` to the end of those after it.
  - Overviews still name the matched programme only; the validation preview adds
    the recording span, for example `20:00–20:15 Example (records 19:28–20:47)`.
- Fields: `title`, `sub_title`, `description`. In `match`, next to the
  fields, `case_sensitive: true` makes all text operators case-sensitive, and
  `min_minutes` / `max_minutes` (1-1440) bound the programme length.
- Case-insensitive matching uses Unicode casefolding, so `strasse` also matches
  `Straße`; `case_sensitive: true` compares the text as written. Combining
  characters, no-break spaces and repeated white space are normalized first.
- Cost: every rule is evaluated against every programme of every channel it
  covers, about 20 microseconds each. A few rules over hundreds of channels take
  seconds per planner run; 256 rules over 300 channels take about 20 seconds.
  Scope rules to the channels they need.
- Overlapping or touching windows (gap up to 60 s) are recorded as one
  uninterrupted window, across rules as well. A running window keeps its start
  and is never shortened by an EPG refresh.
- `schedule` limits which programmes count (by start time); without it every
  match counts and the global schedule is not inherited.
- `m3u:` nests `include`, `exclude` and `priority` of the provider filter. The
  flat keys on the rule level stay valid; mixing both in one rule is an error.
- Retention remains the global setting.
- A channel that a `channels` rule also selects keeps that rule's schedule and
  source filter; the shows windows only add recording time. A `m3u` filter on
  the shows rule does not apply to such a channel and is reported as a warning.
- Several shows rules on the same channel must use identical `m3u` filters.
- Which shows are planned:
  - **Validate** pushes the next matching programmes per channel (five per
    channel in the toast, twenty in the `show_matches` result) with the
    programme title and time, and says when the EPG could not be read.
  - **Today's schedule** lists the planned shows of the day (`Show · 17:00–17:15
    Synthetic Programme › Synthetic Channel`) next to the weekly windows, and `shows` in each
    channel row carries title, rule and time.
  - **Current recordings** names the programme a running show window records for
    (`Synthetic Channel - Source [ok] · Show: Synthetic Programme`); the row's `show` field holds title,
    rule, times and `phase` (`upcoming` while the padding before it runs,
    `airing`, `ended` during the padding after it).
- The planner runs every minute and stores the windows in
  `.catchuparr-show-windows.json` in the archive directory. Each window carries
  its own end, so a stopped planner or a Redis restart only keeps already
  planned shows from being cut off; no new shows are planned until it runs
  again.
