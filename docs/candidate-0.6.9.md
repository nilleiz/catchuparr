# Catchuparr 0.6.9 prerelease candidate

A restart can leave an active borrow episode in SQLite after its native Redis
projection and private registration have disappeared. A new recorder then has
valid authority but cannot open another borrow while that old episode remains
unacknowledged.

Startup now recovers that exact superseded episode before consuming another
ATTACH claim. Recovery requires the current recorder lease, applied control and
configuration, catalog authority, an older episode fence, and confirmed absence
of the old projection and every exact private-client registration resource.
An atomic permanent tombstone fences delayed old registration; SQLite cleanup
uses the episode revision comparison. Viewers and native source ownership are
not stopped or reset. Missing or conflicting evidence blocks new openings.

Current episodes, uncertain native resources and changed configuration are not
force-cleaned. This is scoped recovery, not a global startup reset. It does not
remove intent claims, flush Redis or reset archive/configuration state.

Focused regressions use real SQLite and isolated Redis/Lua. They verify fresh
registration after recovery, preserved ordinary viewers, current/native binding
checks and safe failure paths. Required GitHub CI and Dispatcharr 0.32.0 native
acceptance remain pending. The minimum supported Dispatcharr version is 0.32.0.
