# Show rules (0.8.0 specification)

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
    m3u: { include: [Example provider] }      # nested provider filter
    schedule: { weekdays: [{ start: "18:00", end: "23:00" }] }
```

- Text operators: `equals`, `starts_with`, `ends_with`, `contains` (alias
  `includes`), `wildcard` (`*`, `?`) and `regex` (Python `re`, searched, at most
  200 characters, nested repetition rejected). Each takes one text or a list
  (any of); `any`, `all` and `not` combine conditions. Matching ignores case and
  is Unicode-normalized unless `case_sensitive: true`.
- Fields: `title`, `sub_title`, `description`.
- Overlapping or touching windows (gap up to 60 s) are recorded as one
  uninterrupted window, across rules as well. A running window keeps its start
  and is never shortened by an EPG refresh.
- `schedule` limits which programmes count (by start time); without it every
  match counts and the global schedule is not inherited.
- `m3u:` nests `include`, `exclude` and `priority` of the provider filter. The
  flat keys on the rule level stay valid; mixing both in one rule is an error.
- Retention remains the global setting.
