# Roadmap

Forward-looking work, ordered by priority. CLAUDE.md documents what the code
currently does; this file documents what still needs deciding or building.

## 1. Resolve the video buffer RAM-vs-drift tradeoff (highest priority)

Three implementations of the disk-writing path exist, representing different
points on the same tradeoff. None is a clean solution yet:

| File | FPS strategy | Peak RAM per clip | Drift |
|---|---|---|---|
| `video_engine/_old_video_buffer.py` (kept on disk, not wired into `system_runner.py`) | fixed/reported FPS at writer-open time | bounded — incremental write | unclear — see provenance note below |
| `video_engine/_edge_video_buffer.py` (kept on disk, not wired into `system_runner.py`) | derived once from a 3s warmup window, then incremental write | bounded — ~80–180MB peak at 720p/1080p | acceptable for clips under ~30s; grows for longer clips because a single FPS value is assumed constant for the whole recording |
| `video_engine/video_buffer.py` (current production, wired into `system_runner.py`) | exact, computed from total frames / total elapsed time over the **entire** clip | unbounded — holds every raw frame of the full clip (pre-roll + live) in RAM until the stop trigger arrives | none |

**Why this matters:** `video_buffer.py` is only viable today because it's
being run from a powerful machine over the network, not from the J1900-class
edge box the project targets. A 5-minute 1080p clip at 20fps held entirely as
raw `ndarray` frames is tens of GB — that will not run on the actual edge
hardware. This directly violates the "RAM pre-roll only, then route live
frames straight to disk" constraint in CLAUDE.md.

**Provenance note — unresolved, don't trust either memory or code "polish"
alone:** which of `_old_` / `_edge_` was actually run and verified to work
acceptably for short clips is genuinely unclear — the human memory on this
has flipped once already. Code archaeology gives circumstantial-only signal:
both files share an identical module docstring (same lineage, diverged disk-
writer logic). `_old_`'s `DiskWriter.push()` has an ad-hoc patch comment
(`# NEW CODE: Prevent duplicate frames`) suggesting a real bug was observed
and band-aided during actual use. `_edge_`'s docstring explicitly argues
against both a full-buffer approach (citing the same ~35GB math that
describes `video_buffer.py`'s problem) and against trusting "metadata claims"
for FPS (which is what `_old_` does) — reading like a later, more deliberate
iteration written in response to both predecessors' shortcomings, and it has
no duplicate-frame patch. That's evidence about which was written *more
recently and more deliberately*, not evidence about which was actually
*tested and confirmed working* — an LLM can write a well-justified docstring
for code that was never run against a real stream. Resolve this empirically,
not by further guessing: run both `_old_` and `_edge_` against the same real
or recorded RTSP stream for a few short (~15–30s) clips and a few long
(~3–5min) clips, and watch for (a) duplicate/frozen frames, (b) measured
clip duration vs. wall-clock elapsed time. Whichever holds up becomes the
documented ground truth for this tradeoff — until then, treat both as
unverified candidates rather than picking a "winner" from memory.

**Options to evaluate** (not yet decided — needs a deliberate choice, not a
silent pick):
- Adopt `_edge_video_buffer.py`'s warmup-window approach for edge deployments,
  and accept/bound the drift by capping `max_duration_sec` tighter for edge
  configs (drift is admitted to be fine under ~30s).
- Periodically re-derive FPS mid-recording (e.g., every N seconds, close and
  reopen a new file segment, or recalculate and only apply going forward) to
  bound drift without bounding clip length as hard.
- Switch from `cv2.VideoWriter` (which assumes constant frame rate) to a
  muxing approach that can stamp true per-frame presentation timestamps
  (e.g., via PyAV/FFmpeg) — this is the "real" fix for RTSP jitter, since CFR
  assumptions are the root cause of drift, but it's a bigger lift and a new
  dependency.
- Keep two code paths intentionally: `video_buffer.py` for the central/server
  deployment (ample RAM, wants zero drift) and `_edge_video_buffer.py` (or its
  successor) for true edge boxes — `system_runner.py` would need a config
  flag to choose which `VideoBufferManager` implementation to load.

Until this is resolved, treat `video_buffer.py` as **not edge-ready** despite
being the current default in `system_runner.py`.

## 2. Merge or finalize the second intersection config

`video_engine/701_intersection.json` (intersection 701, US-95/Whitley Dr) is
real in-progress config, separate from intersection 201 in
`video_engine/intersections.json`. `JsonFileConfigProvider` expects one file
keyed by intersection ID — decide whether to merge 701 into `intersections.json`
or keep per-intersection files and adjust the config provider to support that
shape.

## 3. Commit or finalize `ring_monitor.py`

`ntcip_monitor/monitors/ring_monitor.py` is new and uncommitted. Confirm it's
ready (or still WIP) and commit once settled.

## 4. Jules code-review backlog

Findings from Jules's ongoing automated review, triaged against current code
(none were obsolete — all still apply as described). Grouped by how they
should be tackled.

### 4a. Batch the per-OID SNMP polling loops (one focused session)

`output_monitor.py` and `detector_monitor.py` both call
`self.snmp_client.get(oid)` once per item inside a loop, instead of
`self.snmp_client.get(*oids)` once — which `phase_monitor.py` already does
correctly (`reds, yellows, greens = self.snmp_client.get(reds_oid, ...)`).

- `ntcip_monitor/monitors/output_monitor.py:54` — loops over up to 16 outputs.
- `ntcip_monitor/monitors/detector_monitor.py:66` — loops over up to 8
  detector groups. Independently flagged by Jules too (matches what I'd
  already found by inspection) — same fix applies to both.

Traced through `EconoliteSNMPClient.get()`
(`ntcip_monitor/core/snmp_client.py:33`): it already internally re-chunks any
multi-OID call into `CHUNK_SIZE=1` single-OID requests and preserves
ordering, so batching the *call* doesn't change the wire behavior or risk
mis-pairing values to OIDs — it only removes redundant lock acquisitions (16→1
per output poll) and future-proofs for if `CHUNK_SIZE` is ever raised above 1
(see CLAUDE.md — don't actually change `CHUNK_SIZE` itself). Low risk, but
note: `self.stats['reads']` currently increments once per OID; after batching
it increments once per `get()` call, which changes what that stat means if
anything depends on it (e.g. the web UI `/api/stats` endpoint).

### 4b. Unused-import cleanup (one mechanical sweep, zero behavior risk)

All confirmed still unused (grep shows zero other references besides the
import line). Safe to remove in a single pass:

- `ntcip_monitor/core/snmp_client.py:1` — `Counter32`, `Unsigned32`, `Gauge32`
  (keep `Integer32`, used in `set()`).
- `ntcip_monitor/monitors/phase_monitor.py:7` — `PhaseStatus`.
- `examples.py:11` — `DetectorState`, `OutputState` (keep `SignalState`).
- `video_engine/routine_scheduler.py:106` — `timezone` (keep `date`, `datetime`).
- `ntcip_monitor/main.py:6` — `sys`.
- `ntcip_monitor/utils/config_loader.py:8` — `datetime`.
- `ntcip_monitor/utils/controller_control.py:12` — `Counter32`, `Integer32`.
- `ntcip_monitor/ui/web_ui.py:12` — `SignalState`, `DetectorState`, `OutputState`.

### 4c. False positives / non-issues to skip — don't action these

- `from __future__ import annotations` flagged as unused in
  `video_engine/routine_scheduler.py:97` and `video_engine/config_manager.py:143`.
  Known false-positive pattern for import linters: it's a PEP 563 compiler
  directive with no runtime reference by design, so naive "is this name
  referenced anywhere" checks always flag it. Used consistently across all 7
  files in `video_engine/` plus `ring_monitor.py` as a deliberate convention —
  removing it from just these two files would make them inconsistent for zero
  benefit.
- `ConfigLoader`/`ControllerControl` "unused" in `ntcip_monitor/utils/__init__.py:3`,
  and `NTCIPMonitorApp` "unused" in `ntcip_monitor/__init__.py:5`. Both false
  positives — this is the standard `__init__.py` re-export idiom
  (`from ntcip_monitor import NTCIPMonitorApp` is literally how the README
  tells users to import the package; `utils/__init__.py` lists both names in
  `__all__`). Jules's unused-import check doesn't appear to special-case
  `__init__.py` re-exports.
- "Uncached repeated configuration reads" in `video_engine/discrepancy_engine.py:444`.
  Checked both call sites (`__init__` and the explicit `reload()` method,
  `discrepancy_engine.py:402` and `:466`) — neither is in the hot evaluator
  loop, and `JsonFileConfigProvider` already parses the JSON file once at
  construction and caches it in memory; each `get_intersection_config()` call
  is just an in-memory deep-copy, not disk I/O. The hot-loop pattern the
  rationale describes doesn't actually exist here.

### 4d. Seed a test suite (no `tests/` directory exists yet)

Six "lacks test coverage" findings that are genuinely valid and all
pure/deterministic — no mocking needed — making a reasonable first
`tests/` directory:

- `ntcip_monitor/core/oid_definitions.py:109` — `get_phase_oids(group)`.
- `ntcip_monitor/core/oid_definitions.py:127` — `get_detector_oid(detector_num)`.
- `ntcip_monitor/core/oid_definitions.py:146` — `get_output_oid(output_num)`.
- `ntcip_monitor/core/data_models.py:179` — `parse_signal_state(red_bit, yellow_bit, green_bit)`.
- `video_engine/config_manager.py:262` — `ConfigProviderError` (tiny exception
  subclass; test just instantiates it and checks `args`/`__cause__`).
- `video_engine/discrepancy_engine.py:141` — `_resolve_pytz(tz_name, log)`.
  (Jules's finding cites this as `get_safe_timezone` at line 194 — that name/
  line is stale; it's the same function, just renamed/moved since. Still
  genuinely untested: feed it an invalid IANA name and assert it falls back
  to `pytz.utc` and logs a warning — no mocking needed, `pytest`'s `caplog`
  fixture covers the log assertion.)

This is the first test coverage in the repo — worth deciding on a test
runner/layout (e.g. `tests/` at root, `pytest`) as part of this session rather
than ad hoc.

### 4e. Broader test backlog — needs mocking/fixtures, defer until after 4d

The remaining "lacks test coverage" findings all need a mocked `snmp_client`,
a Flask test client, an in-memory SQLite DB, or similar fixtures — a much
bigger lift than 4d's pure functions. Worth a real test-strategy session once
4d's `pytest` scaffolding exists, not ad hoc: `DetectorMonitor`, `OutputMonitor`,
`PhaseMonitor`, `ControllerControl`, `NTCIPMonitorApp`, `WebUI`,
`DiscrepancyEngine.__init__` error handling, `RoutineScheduler`,
`ConfigProvider`/`JsonFileConfigProvider`/`SqliteCentralConfigProvider`,
`VideoBufferServer`.

### 4f. Harden the web UI (security — one session)

Two real findings that belong together since they're both about
`ntcip_monitor/ui/web_ui.py` network exposure:

- **`web_ui.py:22`** — `WebUI.__init__` defaults `host='0.0.0.0'`, and nothing
  in `run.py` exposes a way to override it (`WebUI(app, port=args.web_port)` —
  no `host=` passed). Today there's no way to bind to localhost-only without
  editing source. Fix: default to `127.0.0.1`, add a `--web-host` CLI flag /
  config key for anyone who deliberately wants LAN access.
- **`web_ui.py:103`** — `/api/control/*` endpoints (sync time, place vehicle
  calls, toggle outputs — i.e. actions that touch real traffic signal
  hardware) have no authentication. `ARCHITECTURE.md` already documents this
  as a known gap ("Web UI has no authentication — add reverse proxy with auth
  if exposed"). Minimal viable fix matching the project's stated style
  (resist over-engineering): a shared-secret header check via config, not a
  full session/JWT system — full auth can come later if the deployment story
  changes.

### 4g. Won't-fix — hardware constraint

`ntcip_monitor/core/snmp_client.py:58` — SNMPv1 with cleartext community
strings, flagged as insecure. Correct observation, but not a code bug:
Econolite Cobalt/EOS controllers require SNMPv1 (already documented in
CLAUDE.md/ARCHITECTURE.md as a hardware constraint, alongside port 501 and
`CHUNK_SIZE=1`). SNMPv3 isn't available on this hardware to upgrade to. The
real mitigation is deployment-level: keep controller traffic on an isolated/
segmented network, not a code change.

### 4h. Lower priority — readability, do after 4d exists

- **`_evaluate_pair`, `video_engine/discrepancy_engine.py:584`** (244 lines)
  and **`_fire_trigger`, `video_engine/discrepancy_engine.py:924`** (11 args)
  — both legitimate, but both sit inside the discrepancy engine's
  carefully-worked-out state machine (the cooldown/active-trigger-id
  interaction documented in the module's own docstring — see CLAUDE.md).
  Refactoring either without tests in place first risks silently breaking
  behavior that took real effort to get right. Do this *after* 4d (and
  ideally after extending 4d's tests to cover `_evaluate_pair`'s rules
  directly), not before. `_fire_trigger`'s fix is probably a small
  `TriggerSpec`-like dataclass to replace the 11 positional args.
- **`SystemRunner.__init__`, `video_engine/system_runner.py:162`** (7 args) —
  valid nitpick, low value: it's a top-level orchestrator constructor called
  from exactly one place (`main()`) with sensible defaults. Skip unless doing
  a broader `system_runner.py` pass anyway.
