# Roadmap

Forward-looking work, ordered by priority. [[CLAUDE.md]] documents what the
code currently *does*; this file documents what still needs deciding or
building; [[DESIGN_HISTORY.md]] records *why* past decisions were made.

Work is broken into **named, numbered items**:

- The number is a **stable ID**, assigned once when an item is added and never
  reused or renumbered — not an execution order. An item keeps its number if
  items above/below it are finished and removed, or if the list is reordered.
- **File order is priority order** (with prerequisites noted inline), read top
  to bottom. Reordering the file doesn't change the numbers.
- Each item carries a **Target** model and a **Suggested prompt**. Default
  Target is **Opus, end-to-end in one session** (plan + implement + tests +
  the DESIGN_HISTORY entry and check-offs, no cross-model hand-off). Note a
  different Target only when an item is a whole small mechanical unit or a
  narrowed debugging escalation.
- To run an item, tell the agent "do Item N of ROADMAP.md" (or reference it by
  name). **On completion:** check off the item's boxes here, append a dated
  entry to [[DESIGN_HISTORY.md]]'s Decisions log, update [[CLAUDE.md]] if a
  convention changed, and move fully-finished items out of this file into
  DESIGN_HISTORY.md so ROADMAP stays forward-looking.

When scoping a new batch of requests, **group them into session-sized items**
(merge small related requests; split only what's too big for one session)
rather than one item per bullet, and assign new stable IDs continuing from the
highest used so far.

---

## 1 — Edge-viable video buffer: remux/stream-copy rewrite (Target: Opus builds, Fable verifies) — highest priority

**Decision (2026-07-14): go remux.** Replace the CFR `cv2.VideoWriter` path
with a PyAV **remux / stream-copy** buffer — demux packets, RAM-bounded
pre-roll of encoded packets, copy to disk using the camera's own timestamps.
Accurate length by construction, near-zero edge CPU, RAM bounded by a time
window not clip length. Full design, component specs, timestamp handling, the
decoded-path seam, and the Opus/Fable task split are in
**[VIDEO_BUFFER_REMUX_PLAN.md](VIDEO_BUFFER_REMUX_PLAN.md)** — read it before
starting. The scope/prompt below is the summary; the plan is authoritative.

### Background — the three CFR variants (now superseded)

Three implementations of the disk-writing path exist, representing different
points on the same tradeoff. All three assume a single FPS and therefore drift
under RTSP jitter, so the remux decision above supersedes the earlier "pick
one of these empirically" plan (the `_old_`/`_edge_` provenance question is now
moot for production; kept here as background):

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

Until the remux backend lands, treat `video_buffer.py` as **not edge-ready**
despite being the current default in `system_runner.py`. It stays as the
`full` (central/server) backend; the remux path becomes the edge default.

### Scope

Full detail in [VIDEO_BUFFER_REMUX_PLAN.md](VIDEO_BUFFER_REMUX_PLAN.md) §8.

**Opus — build + blind self-test (no camera needed):**
- [x] `video_engine/remux_video_buffer.py`: `PacketStreamBuffer` (PyAV demux,
      packet pre-roll ring, keyframe tracking, wall-clock receive stamps),
      `ClipRemuxer` (keyframe-seek pre-roll, per-clip timestamp rebase,
      incremental mux, clean finalize), `VideoBufferManager` (reuse the
      existing Hot Folder poll / semaphore / disk-check).
- [x] `VideoBufferConfig` `backend` switch (`remux` | `full`) wired in
      `system_runner.py`; keep the decoded-path seam clean (plan §6).
- [x] `__replay_verify.py` + ffmpeg-synthesized CFR/jitter/B-frame streams;
      assert clip length ≈ requested window ≈ source PTS span, first frame
      decodes, RSS flat across a 4-min clip. Add `av` to `requirements.txt`.
- [x] DESIGN_HISTORY.md entry; check off these boxes.

**Fable — verify against a real capture (after the owner records one):**
- [ ] Run `video_engine/tools/__replay_verify.py` on the owner's real capture
      (`video_engine/tests/fixtures/sample.ts`); confirm length accuracy under
      real jitter + RAM-boundedness.
- [ ] Adversarially probe the plan §4 edge cases (B-frame/DTS monotonicity,
      keyframe-seek first-frame decode, mid-clip PTS discontinuity, concurrent
      triggers, RTSP drop/reconnect). Debug residual drift/dup/freeze; log the
      outcome in DESIGN_HISTORY.md.

Suggested prompt:
> [Opus] In the ntcip project, do Item 1 of ROADMAP.md following
> VIDEO_BUFFER_REMUX_PLAN.md: build the remux/stream-copy video buffer in a new
> `video_engine/remux_video_buffer.py` (PyAV demux, RAM-bounded packet
> pre-roll, keyframe-seek pre-roll, per-clip timestamp rebase, incremental mux,
> clean finalize), keep the `VideoBufferManager` Hot Folder/semaphore/disk-check
> surface so `system_runner.py` selects it via a `backend` config flag, and
> preserve a clean seam for a future decoded backend. Self-test blind with
> `__replay_verify.py` against ffmpeg-synthesized CFR/jitter/B-frame streams
> (assert clip length ≈ source PTS span, first frame decodes, RSS flat over a
> 4-min clip). Add PyAV to requirements. Land the DESIGN_HISTORY.md entry and
> leave the real-stream verification boxes for the Fable pass.

---

## 2 — Merge or finalize the second intersection config (Target: Opus)

`video_engine/701_intersection.json` (intersection 701, US-95/Whitley Dr) is
real in-progress config, separate from intersection 201 in
`video_engine/intersections.json`. `JsonFileConfigProvider` expects one file
keyed by intersection ID — decide whether to merge 701 into `intersections.json`
or keep per-intersection files and adjust the config provider to support that
shape.

Suggested prompt:
> [Opus] In the ntcip project, do Item 2 of ROADMAP.md: decide and implement
> how the second intersection config (`video_engine/701_intersection.json`,
> intersection 701) is stored relative to intersection 201 in
> `intersections.json` — either merge into the single keyed file or extend
> `ConfigProvider`/`JsonFileConfigProvider` (and the central provider, per the
> config-abstraction rule) to support per-intersection files. Land a
> DESIGN_HISTORY.md entry recording the choice and why.

---

## 3 — Commit or finalize `ring_monitor.py` (Target: Opus)

`ntcip_monitor/monitors/ring_monitor.py` is new and uncommitted. Confirm it's
ready (or still WIP) and commit once settled.

Suggested prompt:
> [Opus] In the ntcip project, do Item 3 of ROADMAP.md: review
> `ntcip_monitor/monitors/ring_monitor.py`, confirm it's production-ready
> (or note what's WIP), and commit it once settled. Note anything decided in
> DESIGN_HISTORY.md.

---

## 4 — Jules code-review backlog (Target: Opus; each sub-item is one session)

Findings from Jules's ongoing automated review, triaged against current code
(none were obsolete — all still apply as described). Grouped by how they
should be tackled. Each lettered sub-item below is a session-sized unit; take
them in roughly the listed order (4d before 4a/4h, since those want tests
first).

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

Suggested prompt:
> [Opus] In the ntcip project, do Item 4a of ROADMAP.md: batch the per-OID
> polling loops in `output_monitor.py:54` and `detector_monitor.py:66` into a
> single `get(*oids)` call each, matching `phase_monitor.py`. Do not change
> `CHUNK_SIZE`. Check whether anything (e.g. `/api/stats`) depends on
> `stats['reads']` counting per-OID before changing its meaning. DESIGN_HISTORY
> entry + check off.

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

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 4b of ROADMAP.md: remove the confirmed
> unused imports listed there in one mechanical pass (keep the named
> exceptions). Do NOT touch the false positives called out in 4c. DESIGN_HISTORY
> one-liner + check off.

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

Suggested prompt:
> [Opus] In the ntcip project, do Item 4d of ROADMAP.md: stand up the first
> `tests/` directory (pytest, root-level) and cover the six pure/deterministic
> functions listed there. Decide and document the test runner/layout in
> DESIGN_HISTORY.md — this is the scaffolding 4a/4e/4h build on.

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

Suggested prompt:
> [Opus] In the ntcip project, do Item 4f of ROADMAP.md: harden
> `ntcip_monitor/ui/web_ui.py` — default the bind host to `127.0.0.1` with a
> `--web-host`/config override, and gate the `/api/control/*` hardware-touching
> endpoints behind a config-driven shared-secret header check (not full
> auth). DESIGN_HISTORY entry noting the deliberately-minimal scope.

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

---

## Future (not yet scoped)

Items below need a planning pass before they're actionable (no Target/Scope/
prompt yet). Promote to a numbered item above — with the next unused stable
ID — when scoped.

- *(none yet — add as they come up)*
