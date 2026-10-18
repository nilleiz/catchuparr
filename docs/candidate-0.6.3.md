# Catchuparr 0.6.3 prerelease candidate

This candidate adds two read-only plugin actions: Current recordings and
Today's recording schedule. The schedule view reads the applied configuration,
uses its timezone, and lists today's channel windows with past, current, or
upcoming status. Global recorder pause is shown alongside each window. Empty
days are reported explicitly. Overnight windows are clipped at midnight.

Both overview actions avoid runtime bootstrap, configuration recovery, control
initialization, and recorder or archive mutations. An unavailable control
snapshot is reported as unknown. Draft configuration does not affect either
view.

No configuration migration is required. The candidate remains unpublished;
CI and release acceptance are pending.
