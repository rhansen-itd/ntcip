# Config abstraction

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 496-535). Read before changing config_manager.py or the intersections/ layout. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L496-535 -->

## Config abstraction

`video_engine/config_manager.py` already implements the provider pattern:
`ConfigProvider` (ABC, `get_intersection_config()` / `list_intersection_ids()`),
with `JsonFileConfigProvider` (edge) and `SqliteCentralConfigProvider` (central)
as concrete implementations. `system_runner.py` defaults to the JSON provider.
When adding intersection-level config needs, extend `ConfigProvider`'s
interface and both implementations together — don't special-case one
deployment path with a dict lookup that bypasses the abstraction.

**One file per intersection, in a directory (2026-08-03, ROADMAP 2 —
load-bearing).** `JsonFileConfigProvider` accepts **either** a single JSON file
holding one or more blocks (unchanged, and the natural shape for a central
server) **or a directory**, whose `*.json` files — non-recursive, sorted — are
merged into one namespace. The repo ships
`video_engine/intersections/{201,701}.json` and that is the convention:

- It mirrors `SqliteCentralConfigProvider`'s one-row-per-intersection table on
  the filesystem, instead of leaving the JSON path uniquely "one blob holds
  every site".
- An edge box ships only its own site's file. A merged file would put every
  site's SNMP community string and camera credentials on every box.
- `_load` validates every block eagerly and raises on the first bad one, so a
  malformed 701 block in a shared file would stop the 201 box from starting.

Four properties are load-bearing. An intersection defined in **two files
raises**, naming both — never a silent last-file-wins, which is how a box ends
up on the wrong controller IP. **Nothing is published until every file parses
and validates**, so a bad edit during commissioning leaves a running provider's
previous config intact rather than emptying it. The scan is **not recursive**,
so a backup or data folder underneath is not silently loaded. And an **empty
directory raises** — a provider that quietly knows about no intersections is a
worse failure than a loud one. `source_path(iid)` reports which file a block
came from; it is deliberately **not** on the `ConfigProvider` ABC, because it
is a property of a file-backed store and the SQLite provider has no answer for
it — the one place the "extend both implementations together" rule doesn't
apply. The two deploy-time tools re-implement the merge in
`sync_ui_config.load_intersections` rather than importing the provider: they
belong to neither package, and a half-authored config that fails validation is
exactly when you still want the tool that helps you finish it to run.
