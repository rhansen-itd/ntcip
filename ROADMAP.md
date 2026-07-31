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

> **Status at a glance (2026-07-31).**
>
> **Waiting on the owner + a controller — one round trip closes both:**
> restart the monitor with the new `snmp_chunk_size: 8`, rerun
> `__capture_ntcip.py` (~10 min) with a matching datZ pull, and run the engine
> ≥ 2 h with a matching ATSPM export. That single capture session feeds **4a
> steps 3–4** (verify the sweep got faster) and **9C** (the accuracy
> re-baseline). Don't capture twice.
>
> **Ready to start now, no hardware:** **4d / 4b / 5** (Sonnet — mechanical,
> precedents set), **6** (Opus, fold into 5), **11c** (Opus — `sample.ts` can
> stand in for a camera), **11d** (Sonnet), **2**, **3**, **10**.
>
> **Suggested order:** 4d → 4b/5 → 6, with the owner's round trip
> (4a 3–4, then 9C) slotted in whenever the controller is available.
> Item **11** runs on its own track, 11a → 11b → 11c → 11d, one per session.
>
> Items **8** (remux manager thread-safety + the single-camera assumption) and
> **4f** (web UI: loopback default + shared-secret control endpoints) landed
> 2026-07-31 — see DESIGN_HISTORY.  Item **11** was scoped 2026-07-31 into
> four sub-items; **11a** and **11b** landed the same day, so `/overlay` now
> renders live shapes over a still image; **11c** (live MJPEG) is next.
>
> Model routing follows the Fable-era principle: the *thinking* for the
> remaining items is pre-done in the item text, so the Target line says who
> executes. Deferred by design: **4e** (needs a fixture strategy session,
> after 4d), **4h** (refactors, after 4d). Don't-action lists: **4c**, **4g**.

---

## 9 — Post-4a accuracy re-baseline (Target: owner + any Claude session)

Design and code in [[SCOPE_sampling_floor.md]]; A and B are **done**:

- [x] **A — runtime sweep-time self-measurement** (2026-07-30). `BaseMonitor`
  EMA of `_poll()` + sleep, `effective_cycle_sec()`, `get_stats()`,
  rate-limited slow-sweep INFO. See DESIGN_HISTORY 2026-07-30.
- [x] **B — sampling-floor gating** (2026-07-30). `system_runner` injects the
  floor via `DiscrepancyMonitor.set_sampling_floor()` (startup from
  `sampling_floor_sec`, then every 60 s from the measured cycle); Rule 2
  refuses pulses below `min_pulse_floor_multiple × floor`; per-pair high-duty
  advisory WARNING with opt-in `suppress_high_duty_pairs`.
- [ ] **C — post-4a re-baseline protocol.** Owner-run; steps and pass/fail
  numbers are in [[SCOPE_sampling_floor.md]] §"Item C". **Prerequisite:** 4a's
  controller round trip (probe → `snmp_chunk_size` → restart → recapture).

**Read before running C:** at the default 1.6 s floor the Rule 2 gate (3.2 s)
exceeds a typical 2.0 s `lag_threshold_sec`, so **Rule 2 is effectively off
until the sweep gets faster** — a re-baseline run before 4a lands will show
zero Rule 2 triggers *by design*, not a regression. Set `sampling_floor_sec`
per the probe verdict (or let the runtime measurement do it) before judging
precision/recall.

Suggested prompt (after the owner's 4a round trip + a ≥2 h engine run):
> In the ntcip project, do Item 9C of ROADMAP.md: run the re-baseline
> protocol in SCOPE_sampling_floor.md §"Item C" against the new capture/datZ
> and the ATSPM export, and report each pass/fail number. Categorize residual
> misses/FPs before touching any rule code. DESIGN_HISTORY entry + check off.

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
when logged. Grouped by how they should be tackled; each lettered sub-item is
a session-sized unit. Remaining order: **4d** (tests) → **4b** (mechanical),
with **4e**/**4h** after 4d and **4a** waiting only on the owner's capture.
**4c** and **4g** are don't-action lists. **4f** (security) landed 2026-07-31 —
see DESIGN_HISTORY.

### 4a. SNMP sweep speed — verify the round trip (nearly done)

The original finding (per-OID poll loops) and the measurement that made it the
top accuracy item (`CHUNK_SIZE=1` → a 1.0–1.5 s sampling cycle, 7–42 % edge
capture) are recorded in DESIGN_HISTORY (2026-07-19). **The software half and
the config flip are both done:**

- [x] **Software** (2026-07-19) — call sites batched into one `get(*oids)`,
  `EconoliteSNMPClient(chunk_size=...)`, `detector_range` derived from the
  config's detectors, `snmp_chunk_size` / `controller.chunk_size` config keys,
  `stats['reads']` now counting poll cycles. Tests:
  `ntcip_monitor/tests/test_snmp_batching.py`.
- [x] **Probe** (owner, 2026-07-20; `snmp_batch_probe_20260720_073926.json`)
  — **chunk 8 is clean** on the detector groups: 25/25, order + byte ranges
  ok, median sweep **547 ms → 94 ms** (5.8×); production 6-group shape 93 ms.
  So the Cobalt's "Too Big" history really was a dense-table effect, not a
  multi-OID-PDU limit. (The dirty-verdict fallback — concurrent per-group
  clients — is therefore **moot**; its design is preserved in DESIGN_HISTORY
  2026-07-19 if a different controller ever needs it.)
- [x] **Config** (2026-07-31) — `"snmp_chunk_size": 8` in
  `_intersections.json`, `intersections.json`, `video_engine/intersections.json`
  (all intersection 201 = the probed controller 10.37.23.200). Deliberately
  **not** set for intersection 701 (10.70.10.51) or the standalone
  `config.json` (10.37.2.68): different controllers, no probe evidence. Probe
  each before raising it there.

What remains — **the same round trip Item 9C needs, so do one capture, not
two**:

3. [ ] **[Owner, controller machine]** restart the monitor (nothing takes
   effect until then), then rerun `__capture_ntcip.py` ~10 min with a matching
   datZ pull; push capture + datZ.
4. [ ] **[Any Claude session, any model]** verify with
   `__correlate_channels.py` and the edge-capture-ratio check that sweep time
   and edge capture improved (baseline 2026-07-19: median sweep 1.53 s, 7–42 %
   of edges seen; expected now ~0.1 s sweep and ≥ 90 % capture). Then move
   this item to DESIGN_HISTORY.

Note the outputs OIDs failed this probe for an unrelated reason — see Item 10.

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
- `ntcip_monitor/ui/web_ui.py:35` — `SignalState`, `DetectorState`, `OutputState`.

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

### 4d. Cover the remaining untested pure functions

Six "lacks test coverage" findings that are genuinely valid and all
pure/deterministic — no mocking needed. Both `tests/` directories now exist
(`video_engine/tests/`, `ntcip_monitor/tests/`), so this is breadth over
established scaffolding, not new layout work:

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
  to `pytz.utc` and logs a warning — no mocking needed,
  `unittest.TestCase.assertLogs` covers the log assertion.)

**Sequencing (2026-07-19):** Item 7 has landed and established the layout
precedent: per-package tests (`video_engine/tests/test_discrepancy_rules.py`),
**stdlib `unittest`** (pytest is not installed in the deployment env; the
tests run via `python3 video_engine/tests/test_discrepancy_rules.py` or
unittest discovery, and remain pytest-compatible if it's ever added). Mirror
that: put the ntcip_monitor cases in `ntcip_monitor/tests/`, use `unittest`,
and for the `_resolve_pytz` log assertion use
`unittest.TestCase.assertLogs` instead of pytest's `caplog`. With the layout
decision removed, 4d is pure mechanical breadth over deterministic functions —
hence the Sonnet target below, not Opus.

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 4d of ROADMAP.md: cover the six
> pure/deterministic functions listed there with stdlib `unittest` (pytest is
> not installed). Do NOT invent a new
> test layout — follow the `tests/` layout Item 7 already established (check
> `video_engine/tests/` and the DESIGN_HISTORY entry from 7) and place the
> `ntcip_monitor` tests in the matching per-package location. No mocking needed.
> DESIGN_HISTORY one-liner + check off. (4e and 4h build on this.)

### 4e. Broader test backlog — needs mocking/fixtures, defer until after 4d

The remaining "lacks test coverage" findings all need a mocked `snmp_client`,
a Flask test client, an in-memory SQLite DB, or similar fixtures — a much
bigger lift than 4d's pure functions. Worth a real test-strategy session once
4d's `unittest` scaffolding exists, not ad hoc: `DetectorMonitor`, `OutputMonitor`,
`PhaseMonitor`, `ControllerControl`, `NTCIPMonitorApp`, `WebUI`,
`DiscrepancyEngine.__init__` error handling, `RoutineScheduler`,
`ConfigProvider`/`JsonFileConfigProvider`/`SqliteCentralConfigProvider`,
`VideoBufferServer`.

### 4f. Harden the web UI — **done 2026-07-31**

Bind host now defaults to `127.0.0.1` (`--web-host` / `web_ui.host` to
override) and `/api/control/*` is gated by the `X-NTCIP-Control-Token` shared
secret, refused outright on a non-loopback bind with no token. Rationale in
DESIGN_HISTORY (2026-07-31). A Flask-test-client regression test for it is
part of **4e** (which already lists `WebUI`) — Flask isn't installed in this
environment, so the policy is currently covered only by a stubbed scratch
check.

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

## 5 — Retire the superseded CFR buffers (Target: Sonnet, mechanical)

Item 1 (remux backend) is complete and real-stream verified (see
DESIGN_HISTORY.md 2026-07-15), which unblocks the follow-up cleanup the plan
deferred: `video_engine/_old_video_buffer.py` and
`video_engine/_edge_video_buffer.py` are superseded interim CFR attempts,
imported by nothing. Move them to a `legacy/` folder or delete them (they're
recoverable from git history either way), and update CLAUDE.md's "Known repo
clutter" note when done. Do **not** touch `video_buffer.py` — it remains the
supported `full` (central/server) backend.

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 5 of ROADMAP.md: remove (or move to
> `video_engine/legacy/`) the superseded `_old_video_buffer.py` and
> `_edge_video_buffer.py`, confirm nothing imports them, update CLAUDE.md's
> clutter note, and add a DESIGN_HISTORY.md one-liner.

---

## 6 — Retire the `full` (CFR) video backend (Target: Opus)

`video_engine/video_buffer.py` is the legacy `full`/CFR `cv2.VideoWriter`
backend. As of 2026-07-15 it is **unused** (no intersection config sets
`video_backend`, so every deployment defaults to `remux`), **strictly worse**
than `remux` on the three edge constraints (clip-length accuracy, CPU, RAM), and
carries the documented **RAM-unboundedness bug** (`DiskWriter._write_loop`
buffers a whole clip in memory — tens of GB for a multi-minute 1080p clip).
Nothing consumes decoded pixels today, and the flagged *future* decoded path is
explicitly a new bounded branch, not this file — so `full` is not the seam for
it either.

Decide and implement one of:
- **(a, recommended)** delete `video_buffer.py` and collapse
  `system_runner._build_video_manager` to remux-only (drop the backend switch
  and the `full` config docs); or
- **(b)** if a central re-encode/decoded need turns out to be real, keep the
  *switch* but rebuild that need as a **RAM-bounded decoded** backend — do not
  ship the CFR one.

Coordinate with **Item 5**: that item removes `_old_`/`_edge_` and currently says
"do not touch `video_buffer.py` — it remains the supported `full` backend"; this
item reopens exactly that decision (it's the last CFR file). Do Item 5 first or
fold both into one sweep. On completion, update CLAUDE.md's hardware-constraints
/ "two backends" section and the `config_manager.py` config docs.

Suggested prompt:
> [Opus] In the ntcip project, do Item 6 of ROADMAP.md: retire the `full` (CFR)
> video backend. Confirm nothing selects it, then remove `video_buffer.py` and
> simplify `system_runner._build_video_manager` to remux-only (or, if a central
> decoded need is confirmed, replace it with a RAM-bounded decoded backend — not
> the CFR one). Update CLAUDE.md's backends section and `config_manager.py` docs,
> and land a DESIGN_HISTORY.md entry.

---

## 10 — `specialFunctionOutputState` OIDs rejected by the Cobalt (Target: Opus)

Found in the 2026-07-20 probe (`snmp_batch_probe_20260720_073926.json`,
analysed 2026-07-31). `OUTPUT_OIDS` = `...1206.4.2.1.3.14.1.2.{1..16}` returns
SNMPv1 **`noSuchName` at index 1** on controller 10.37.23.200 — 0/25
successes at chunk 1 *and* chunk 16. Identical failure at chunk 1 means this
is **not** a batching problem: the agent does not implement that object at
that OID (wrong column/index for Econolite's table, or the feature is
unsupported/unlicensed on this box).

Not urgent — nothing polls outputs today (`config.json` has
`monitors.outputs.enabled: false`, and `system_runner` only ever builds a
`DetectorMonitor`). The reason it's worth an item anyway:

- **It fails silently.** `output_monitor._poll` catches `SNMPError` with a
  bare `pass` (`ntcip_monitor/monitors/output_monitor.py`), so enabling
  outputs against this controller yields a monitor that polls forever, emits
  nothing, and logs nothing. At minimum that `pass` should log (rate-limited)
  — compare `detector_monitor._poll`, which at least prints a diagnostic.
- **The OID needs verifying against the MIB**, not guessing: walk the
  `1.3.6.1.4.1.1206.4.2.1.3` subtree on the controller (or check the vendor
  MIBs in `MIBs/`) to find what the Cobalt actually exposes, and fix
  `oid_definitions.OUTPUT_BASE` if the column is wrong.
- **Related, general:** an SNMPv1 GET is all-or-nothing — one unsupported
  varbind aborts the whole PDU. That is harmless for detector groups (all 8
  valid, which is why chunk 8 is clean) but means any future multi-OID batch
  mixing supported and unsupported objects loses the good values too. Worth a
  sentence in CLAUDE.md's SNMP section when this is fixed.

Suggested prompt:
> [Opus] In the ntcip project, do Item 10 of ROADMAP.md: determine the correct
> Econolite OID for the special-function outputs (check `MIBs/` and, if the
> controller is reachable, a subtree walk), fix `OUTPUT_BASE`/`OUTPUT_OIDS` if
> wrong, and replace `output_monitor._poll`'s silent `except SNMPError: pass`
> with rate-limited structured logging. DESIGN_HISTORY entry + check off.

---

## 11 — Live Video Overlay (Target: Opus; scoped 2026-07-31 into 11a–11d)

**Vision:** A real-time version of pyatspm's video overlay.  Embed a live
RTSP camera feed (or a still image from file) in the ntcip web UI, with
detector loops and stopbars drawn on top and recolored by the live
SNMP-polled phase/detector/overlap state — the same visual output as
pyatspm's `atspm video-overlay` command, but driven by the live monitor
data instead of a recorded event database.

**Planning pass complete (2026-07-31).**  Design questions A–E are settled
(see *Decisions* below) and the work is split into four session-sized
sub-items, **11a → 11b → 11c → 11d**, in that order.  11a is pure and
dependency-free; 11a+11b together give a working overlay against a still
image; 11c adds live video; 11d removes the manual config copying.
Rationale for each decision is in DESIGN_HISTORY (2026-07-31).

### What pyatspm does (the reference implementation)

pyatspm's video overlay is a **post-hoc** pipeline:  recorded video +
recorded events → rendered video with overlaid shapes.  The pieces:

1. **Shape config data model** — `ShapeConfig` class in
   [`pyatspm/src/atspm/data/video.py`](file:///home/hansrkid/pyatspm/src/atspm/data/video.py)
   (L95-234).  A list of shape dicts, each with `type` (`"loop"` or
   `"stopbar"`), `points` (pixel coords), `color`, `input` (detector
   channel for loops), `phase` (int or `"OLA"`-`"OLP"` overlap code for
   stopbars), `name`.  Resolution (`video_width`/`video_height`) recorded
   once.  Round-trips to a 2-section CSV (metadata header + per-shape
   rows).  Example in
   [`~/vid_cfg720.csv`](file:///home/hansrkid/vid_cfg720.csv).

2. **Interactive calibration UI** —
   [`pyatspm/src/atspm/video/calibrate.py`](file:///home/hansrkid/pyatspm/src/atspm/video/calibrate.py)
   `calibrate_shapes()` (L112-492).  OpenCV window over first frame +
   Tkinter dialogs.  Mouse-driven: click 4 points → loop, click 2 points →
   stopbar.  Edit mode with point/body dragging, snapping, copy, undo.
   Keys: `l`/`s` mode switch, `c` color, `i` input, `p` phase, `e` edit,
   `g` snap, `u` undo, `w` save, `q` quit.

3. **Overlay renderer** —
   [`pyatspm/src/atspm/video/overlay.py`](file:///home/hansrkid/pyatspm/src/atspm/video/overlay.py)
   (entire file, 100 lines).  Three functions:
   - `draw_loop_overlay(frame, shape, is_on)` — outlined when off, filled
     (alpha-blended) when on.
   - `draw_stopbar_overlay(frame, shape, status)` — line colored by G/Y/R/na.
   - `draw_shape_overlay(frame, shape, status)` — dispatcher.

4. **Status lookup** —
   [`pyatspm/src/atspm/analysis/video.py`](file:///home/hansrkid/pyatspm/src/atspm/analysis/video.py)
   (471 lines).  Pure functions: `phase_status_at_timestamps()`,
   `overlap_status_at_timestamps()`, `detector_status_at_timestamps()`.
   These are vectorised batch lookups against a DataFrame of recorded
   events.  **Not needed for live** — the ntcip monitors already expose
   point-in-time state directly.

5. **Video processor** —
   [`pyatspm/src/atspm/video/processor.py`](file:///home/hansrkid/pyatspm/src/atspm/video/processor.py)
   `render_overlay()` (L62-186).  Orchestrates: open video, fetch events
   from SQLite, chunk frames, vectorise status lookups, draw shapes, write
   output.  **Not needed for live** — the equivalent is reading an RTSP
   frame, querying the live monitors, and drawing.

6. **CLI** —
   [`pyatspm/src/atspm/cli.py`](file:///home/hansrkid/pyatspm/src/atspm/cli.py)
   `handle_video_calibrate_shapes` (L1573-1605),
   `handle_video_overlay` (L1608-1678),
   `_video_shape_path` (L1541-1551).  Shape CSVs live at
   `intersections/<folder>/video/<camera>_shapes.csv`.

### What ntcip already has (the live data sources)

- **Phase state** —
  [`ntcip_monitor/monitors/phase_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/phase_monitor.py)
  `PhaseMonitor._last_phases` (dict `{phase_num: SignalState}`),
  `_last_overlaps` (dict `{overlap_num: SignalState}`).  Exposed via
  `get_all_phases()` / `get_all_overlaps()` (L219-225).  `SignalState` enum:
  `DARK=0, RED=1, YELLOW=2, GREEN=3`
  ([`data_models.py:11-16`](file:///home/hansrkid/ntcip/ntcip_monitor/core/data_models.py#L11-L16)).

- **Detector state** —
  [`ntcip_monitor/monitors/detector_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/detector_monitor.py)
  `DetectorMonitor._last_detectors` (dict `{det_num: DetectorState}`).
  Exposed via `get_all_detectors()` (L122-124).  `DetectorState` enum:
  `INACTIVE=0, ACTIVE=1`
  ([`data_models.py:19-22`](file:///home/hansrkid/ntcip/ntcip_monitor/core/data_models.py#L19-L22)).

- **Web UI** —
  [`ntcip_monitor/ui/web_ui.py`](file:///home/hansrkid/ntcip/ntcip_monitor/ui/web_ui.py)
  Flask app with `/api/status` (L159-203) returning JSON with
  `phases`, `overlaps`, `detectors` keyed by number → state name.  Runs
  in a background thread (L260-298).  Note `/api/control/*` is token-gated
  as of 4f; the read-only status/stats routes are not.

- **Camera config** —
  [`video_engine/intersections.json`](file:///home/hansrkid/ntcip/video_engine/intersections.json)
  (L12-19) has `cameras.fisheye.url` (RTSP URL) per intersection.
  Detectors (L20-100) have `phase`, `paired_detector_id`, `camera_id`.

### Verified during the planning pass — don't re-derive these

- **The camera stream is 720×720 h264 @ 10 fps**
  (`video_engine/tests/fixtures/sample.ts`), matching `~/vid_cfg720.csv`'s
  recorded `720,720`.  The existing calibration targets this exact view —
  no dewarp mismatch, no rescaling needed.  `sample.ts` also supplies a
  real frame from this camera, so the overlay is testable with no camera
  reachable.
- **Shape `input` values (17, 24, 26, 33, 38, 46, …) match intersection
  201's detector IDs exactly.**  Same numbering space; no channel
  remapping.  (Consistent with the 2026-07-19 `__correlate_channels.py`
  verification — see CLAUDE.md's NTCIP rules.)
- **`~/vid_cfg720.csv` is in pyatspm's *legacy* CSV format** — per-row
  `video_width`/`video_height`, a `direction` column, no `name`.  Current
  `ShapeConfig.load` expects the two-section layout and crashes on it
  (`int("stopbar")`).  The reader must accept both.
- **CSV colors are BGR, not RGB** (pyatspm writes OpenCV order).
  `"255,0,0"` is *blue*, `"0,0,255"` is *red*.  Anything rendering these
  in a browser must reverse the triple — the bug looks plausible on
  screen, so it needs an explicit test.
- **`/api/status` already returns phases, overlaps, and detectors** as
  state names; no new state plumbing into the monitors is required.
- **PyAV encodes MJPEG natively**
  (`av.CodecContext.create('mjpeg','w')` verified) — the live path needs
  no new dependency.  `av` is already in `requirements.txt`.
- **This environment lacks `cv2`, `flask`, `numpy`, and `atspm`** (`av`
  and `pytz` are present).  11a is deliberately dependency-free so it is
  fully testable here; 11b+ need `pip install -r requirements.txt`.

### Decisions (settled 2026-07-31 — A–E from the original planning pass)

| Question | Decision |
|---|---|
| **B.** Rendering | Client-side `<canvas>` over a background image.  Server resolves shape→status; JS only recolors. |
| Background source | Pluggable: `file` (static image) **and** `live` (MJPEG from RTSP).  Both feed the same canvas. |
| **D.** Host + config | `ntcip_monitor/ui/`, driven by a new `overlay` section in `config.json`.  **No `video_engine` import.** |
| Config drift | Offline, deploy-time sync script — not a runtime dependency. |
| **A.** pyatspm | **Vendor** the CSV reader (~80 lines, stdlib).  pyatspm's calibrator stays the authoring tool. |
| **E.** Calibration | Use pyatspm's existing `atspm video-calibrate-shapes` externally; a web calibrator is a separate future item. |
| **C.** Mapping | As originally sketched; the status table is in 11a below. |

Two constraints drove these and should not be quietly reversed:

- **The UI runs on a remote host, not the J1900 edge box.**  CLAUDE.md's
  hardware constraints govern the video *buffering* path only.  Decode
  cost is not a concern for this feature, which is why live MJPEG is in
  scope at all.
- **ntcip and pyatspm must be independently distributable**, and
  `ntcip_monitor` and `video_engine` must stay independent of each other.
  That rules out importing `atspm` and rules out a runtime read of
  `video_engine/intersections.json`.  Vendoring ~80 lines beats extracting
  a shared third package at this size.

### 11a. Vendored shape loader + status resolution (Target: Opus)

Pure, stdlib-only, fully testable in this environment.  No Flask, no
camera, no monitor imports.

**New:** `ntcip_monitor/ui/overlay/{__init__,shapes,status}.py`,
`ntcip_monitor/tests/test_overlay_shapes.py`

`shapes.py` — vendored from `pyatspm/src/atspm/data/video.py` L50-234:
`OVERLAP_LETTER_MAP` (`OLA`–`OLP` → 1–16), `resolve_stopbar_target()`, and
a `ShapeConfig` with `.shapes` / `.video_width` / `.video_height` /
`.load()`.  Two deliberate deviations from pyatspm:

- **Tolerant loader** — sniff row 1.  `video_width,video_height` →
  two-section format; `type,points,...` → legacy format (width/height from
  the first data row, ignore `direction`).  Reads both.
- **Drop `validate_resolution()`** — it exists to refuse rescaling a
  recorded video.  The canvas scales safely (11b), so it doesn't apply.
  Keep the metadata itself.

`status.py` — pure functions mapping the `/api/status` payload shape onto
shapes, returning a list parallel to `config.shapes`:

| Shape | Source | Result |
|---|---|---|
| `loop` | `detectors[input]` | `"ACTIVE"` → on, else off |
| `stopbar`, int phase | `phases[n]` | `GREEN`→`G`, `YELLOW`→`Y`, `RED`→`R`, `DARK`/missing→`na` |
| `stopbar`, `OLx` | `overlaps[n]` | same mapping |

**Edge case to handle explicitly:** `PhaseMonitor` monitors overlaps 1–8,
but the format permits `OLA`–`OLP` (1–16).  `vid_cfg720.csv` uses
`OLB`/`OLC`/`OLD`/`OLF`, all in range, but a shape referencing `OLI`+ would
render permanently grey with no explanation.  Resolve to `na` at request
time (not an error), and log a one-time WARNING at load naming the
offending shapes.

Tests — stdlib `unittest`, following `ntcip_monitor/tests/test_snmp_batching.py`:
both CSV formats produce identical shape lists; malformed points/colors;
overlap letter mapping incl. out-of-range; phase bounds; every row of the
status table; missing detector → off; missing phase → `na`; the overlap >8
warning fires once.

- [x] `shapes.py` reads both CSV formats
- [x] `status.py` resolution table
- [x] tests green: `python3 ntcip_monitor/tests/test_overlay_shapes.py` (41 cases)

Suggested prompt:
> [Opus] In the ntcip project, do Item 11a of ROADMAP.md: vendor the
> pyatspm shape-config reader into `ntcip_monitor/ui/overlay/` and add the
> pure status-resolution functions, with stdlib `unittest` coverage
> (pytest/cv2/flask/numpy are NOT installed — 11a must stay
> dependency-free).  Read item 11's "Verified during the planning pass"
> list first and don't re-derive it.  Follow the `tests/` layout Item 7
> established.  Then verify against the real file:
> `ShapeConfig.load('/home/hansrkid/vid_cfg720.csv')` must yield
> `720 720` and 37 shapes.  DESIGN_HISTORY one-liner + check off.
> *(Done 2026-07-31.  The count in this prompt originally read 38; the real
> file has 37 — 28 loops + 9 stopbars.)*

### 11b. Overlay page, shape/state routes, file background (Target: Opus)

First end-to-end visible result.  Depends on 11a.

**Modified:** `ntcip_monitor/ui/web_ui.py`, `config.json`, `run.py`,
`ntcip_monitor/ui/templates/dashboard.html` (link only)
**New:** `ntcip_monitor/ui/templates/overlay.html`,
`ntcip_monitor/ui/overlay/source.py`

A **dedicated `/overlay` page**, not a dashboard panel — `dashboard.html`
is already 464 lines with six sections, and the video wants the viewport.
Link to it from the dashboard header.

Routes:

- `GET /overlay` → the page
- `GET /api/overlay/shapes` → `{video_width, video_height, shapes:[...]}`, fetched once at load
- `GET /api/overlay/state` → statuses parallel to `shapes`, polled at the existing 250 ms `UPDATE_INTERVAL`
- `GET /api/overlay/background` → single JPEG
- `GET /api/overlay/stream` → MJPEG (11c; 404 while the source is `file`)

Splitting shapes (static) from state (polled) keeps the hot payload to one
small array and puts every mapping decision in Python where 11a's tests
cover it.

`source.py` — `BackgroundSource` ABC + `FileImageSource` (reads bytes from
disk, re-reads on mtime change so swapping the calibration still doesn't
need a restart).

**Canvas sizing:** set `canvas.width/height` to the config's
`video_width`/`video_height`, draw in native shape coordinates, and stack
canvas + background in a container where both are `width:100%; height:auto`.
The browser then scales both identically — no coordinate math anywhere.
JS mirrors `pyatspm/src/atspm/video/overlay.py` L46-99: loop off → 1px
stroke in the shape color; loop on → filled at alpha 0.2 plus a white 1px
outline; stopbar → 3px line in G/Y/R/grey.  **Reverse the BGR triple** (see
the verified-facts list).

```json
"overlay": {
  "enabled": true,
  "shapes_csv": "overlay/201_fisheye_shapes.csv",
  "background": "file",
  "image_path": "overlay/201_fisheye.jpg",
  "camera_url": "rtsp://.../axis-media/media.amp?camera=1&videocodec=h264",
  "stream_fps": 5
}
```

**Access policy — a deliberate departure from 4f.**  4f left read-only
routes open and gated only `/api/control/*`.  Proxied camera video is a
different category from "phase 3 is green": it's a live view of a public
roadway, and an operator who runs `--web-host 0.0.0.0` would not expect to
have published it.  Apply the existing interlock to the **two video routes
only** — reuse `_is_loopback_host()` (`web_ui.py:46`) and refuse with 403
on a non-loopback bind with no token, exactly as `_check_control_access()`
(`web_ui.py:107`) already does.  `/api/overlay/shapes` and
`/api/overlay/state` stay open like `/api/status`.

- [x] routes + `/overlay` page
- [x] `FileImageSource` with mtime reload
- [x] BGR→RGB reversal, verified visually (a `255,0,0` shape renders blue) —
  done in Python (`shapes.bgr_to_rgb`, on the way out to
  `/api/overlay/shapes`) rather than in JS, so a unit test pins it; the page
  consumes RGB directly
- [x] video routes refused on non-loopback bind with no token
- [x] CLAUDE.md updated (this is where the feature becomes load-bearing)

Suggested prompt:
> [Opus] In the ntcip project, do Item 11b of ROADMAP.md: add the
> `/overlay` page, the four `/api/overlay/*` routes, and the file-image
> background source, on top of 11a's loader.  Read item 11's verified-facts
> list first — especially the BGR color order and the 720×720 canvas
> sizing.  Note the deliberate access-policy departure from 4f for the two
> video routes.  Verify by extracting a frame from
> `video_engine/tests/fixtures/sample.ts` as the background and confirming
> the loops land on the pavement.  DESIGN_HISTORY entry + CLAUDE.md update
> + check off.
> *(Done 2026-07-31.  The still and the shape CSV were committed to
> `overlay/` so the shipped `config.json` works out of the box.  Note
> `overlay.camera_url` is left empty — 11d's sync script authors it from
> `video_engine/intersections.json` rather than hand-copying a credential.)*

### 11c. Live MJPEG source (Target: Opus)

Depends on 11b.  **Modified:** `ntcip_monitor/ui/overlay/source.py`

`RtspMjpegSource` using PyAV — no new dependency.  11b left the seams:
subclass `BackgroundSource`, override `supports_stream()` → True and
`mjpeg_frames()` (yield raw JPEG bytes; `web_ui.py` owns the multipart
framing), and make `create_background_source()` return it for
`background: "live"` instead of raising `NotImplementedError`.  The route,
the access gate, the `?token=` fallback for `<img>`, and the page's
`SOURCE_KIND === 'live'` branch are already in place.

- **One shared decoder thread per URL**, not one per client.  Ref-counted
  subscribers, opened lazily on first subscriber, torn down after the last
  leaves plus a short grace period — an idle page costs no RTSP session,
  and N viewers cost one stream off the intersection rather than N.
- Decode all frames, encode and publish at `stream_fps` (default 5, from a
  10 fps source) to bound bandwidth.
- Subscriber bookkeeping follows the discipline CLAUDE.md documents for
  `remux_video_buffer.VideoBufferManager`: **under the lock, collect what
  to act on; release; then act.**  Never hold the lock across a decode, an
  encode, or a socket write.
- Reconnect with backoff on stream drop; serve the last good frame
  meanwhile rather than breaking the `<img>`.
- `/api/overlay/background` returns the latest decoded frame when the
  source is `live`, so the still endpoint works for both source types.

- [ ] `RtspMjpegSource` with ref-counted shared decoder
- [ ] two tabs open → one RTSP session; both closed → teardown
- [ ] reconnect/backoff on drop

Suggested prompt:
> [Opus] In the ntcip project, do Item 11c of ROADMAP.md: add the live
> MJPEG background source using PyAV (already a dependency; the mjpeg
> encoder is verified available).  One shared decoder thread per URL with
> ref-counted subscribers — not one session per client.  Follow the lock
> discipline CLAUDE.md documents for the remux manager.  DESIGN_HISTORY
> entry + check off.

### 11d. Config sync + calibration workflow (Target: Sonnet, mechanical)

**New:** `tools/sync_ui_config.py`, `tools/grab_calibration_still.py`
(repo root — a deploy-time layer belonging to neither package, the same
role `video_engine/system_runner.py` plays at runtime)

`sync_ui_config.py` reads `video_engine/intersections.json` for a given
intersection ID and writes `controller.*` and `overlay.camera_url` into
`config.json`.  `--dry-run` by default.  This is the de-duplication
mechanism: both packages keep their own config file and stay independently
deployable, but the shared values get one authoring source instead of being
hand-copied.

`grab_calibration_still.py` pulls one frame from an RTSP URL to a JPEG.

**Calibration workflow** — documented, not built: grab a still → run
pyatspm's `atspm video-calibrate-shapes` against it → drop the CSV at
`overlay.shapes_csv`.  No new code; 11a's tolerant reader accepts either
format pyatspm produces.  A browser-based calibrator would remove the
pyatspm and Tkinter dependency from the workflow entirely, but it's a
substantial build — promote it to its own item if it's wanted.

- [ ] `sync_ui_config.py` with `--dry-run`
- [ ] `grab_calibration_still.py`
- [ ] workflow documented in CLAUDE.md

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 11d of ROADMAP.md: add
> `tools/sync_ui_config.py` (offline, deploy-time, `--dry-run` by default)
> and `tools/grab_calibration_still.py` at the repo root, and document the
> calibration workflow in CLAUDE.md.  These are deploy-time scripts, NOT
> runtime imports — `ntcip_monitor` and `video_engine` must stay
> independent.  DESIGN_HISTORY one-liner + check off.

### Key files to read (per sub-item)

| Concern | File | Lines |
|---------|------|-------|
| **11a** — vendor source | [`pyatspm/.../data/video.py`](file:///home/hansrkid/pyatspm/src/atspm/data/video.py) | L50-234 |
| **11a** — test layout precedent | [`ntcip/.../test_snmp_batching.py`](file:///home/hansrkid/ntcip/ntcip_monitor/tests/test_snmp_batching.py) | all |
| **11b** — drawing semantics to port to JS | [`pyatspm/.../video/overlay.py`](file:///home/hansrkid/pyatspm/src/atspm/video/overlay.py) | L46-99 |
| **11b** — routes + access policy to extend | [`ntcip/.../web_ui.py`](file:///home/hansrkid/ntcip/ntcip_monitor/ui/web_ui.py) | L46, L107-145, L147-203 |
| **11c** — lock discipline to mirror | [`ntcip/.../remux_video_buffer.py`](file:///home/hansrkid/ntcip/video_engine/remux_video_buffer.py) | manager bookkeeping |
| **11d** — camera URL source of truth | [`ntcip/.../intersections.json`](file:///home/hansrkid/ntcip/video_engine/intersections.json) | L12-19 |
| Shape data model + CSV format | [`pyatspm/.../data/video.py`](file:///home/hansrkid/pyatspm/src/atspm/data/video.py) | L95-234 (ShapeConfig class) |
| Overlay drawing functions | [`pyatspm/.../video/overlay.py`](file:///home/hansrkid/pyatspm/src/atspm/video/overlay.py) | L46-99 (all 3 draw functions) |
| Calibration UI | [`pyatspm/.../video/calibrate.py`](file:///home/hansrkid/pyatspm/src/atspm/video/calibrate.py) | L112-492 (calibrate_shapes) |
| Live phase/overlap state | [`ntcip/.../phase_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/phase_monitor.py) | L54-56, L219-225 (state dicts) |
| Live detector state | [`ntcip/.../detector_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/detector_monitor.py) | L48, L122-124 (state dict) |
| Web UI / Flask routes | [`ntcip/.../web_ui.py`](file:///home/hansrkid/ntcip/ntcip_monitor/ui/web_ui.py) | L147-203 (routes, /api/status) |
| Camera RTSP URL config | [`ntcip/.../intersections.json`](file:///home/hansrkid/ntcip/video_engine/intersections.json) | L12-19 (cameras.fisheye.url) |
| SignalState / DetectorState enums | [`ntcip/.../data_models.py`](file:///home/hansrkid/ntcip/ntcip_monitor/core/data_models.py) | L11-22 |
| Example shape CSV | [`~/vid_cfg720.csv`](file:///home/hansrkid/vid_cfg720.csv) | all 38 lines (1 header + 37 shapes) |
| pyatspm CLI workflow | [`pyatspm/.../cli.py`](file:///home/hansrkid/pyatspm/src/atspm/cli.py) | L1573-1678 |

### Risks carried into implementation

- **Untestable surface.**  ~~Flask routes and the MJPEG path can't be tested
  in this environment.~~  Superseded 2026-07-31: `flask` and `pysnmp` were
  installed during 11b, and the routes were verified with a Flask test
  client + stub monitors (38 checks) — but only from a scratch script, so
  the in-repo route test is still **4e**'s work.  11a/11b logic that can be
  tested without Flask is in `test_overlay_shapes.py` (66 cases).
- **Calibration staleness.**  `vid_cfg720.csv` matches the fixture's
  720×720, but the fixture is from 2026-07-15.  If the camera has been
  re-aimed or reconfigured since, shapes will be subtly misplaced.  The
  file-background check in 11b is the cheap way to catch this before
  trusting the overlay.
- **Sampling floor.**  The effective NTCIP detector sampling cycle is
  ~1.0–1.5 s (CLAUDE.md), and 4a raised `snmp_chunk_size` to 8 for
  intersection 201 without a re-baseline (Item 9C).  A 250 ms UI poll will
  therefore show detector state changing in steps considerably coarser than
  the video.  That's honest — it shows the monitor's real resolution — but
  the page should label it rather than imply frame-accurate detection.

---

## Future (not yet scoped)

Items below need a planning pass before they're actionable (no Target/Scope/
prompt yet). Promote to a numbered item above — with the next unused stable
ID — when scoped.

- *(none yet — add as they come up)*
