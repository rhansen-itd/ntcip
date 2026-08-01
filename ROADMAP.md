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

> **Status at a glance (2026-08-01).**
>
> **Waiting on the owner + a controller — one round trip:** **9C2** needs a
> ≥ 2 h engine run at *peak* (16:00–18:00) with a matching datZ pull. The
> 2026-07-31 round trip already closed **4a steps 3–4** (sweep verified: cycle
> 1.53 s → ~0.33 s, edge capture 26 % → 94 %) and produced 9C's first
> measurement, but off-peak — max detector duty 32.8 % against the 80–94 %
> that motivated the scope, so the false-trigger storm condition is still
> untested.
>
> **Ready to start now, no hardware:** **5** (Sonnet — mechanical, precedents
> set), **6** (Opus, fold into 5), **2**, **3**, **10**, and **4e / 4h**
> (unblocked by 4d).
>
> **Suggested order:** 5 → 6, with the owner's peak-hour run (9C2) slotted
> in whenever the controller is available.
>
> Items **8** (remux manager thread-safety + the single-camera assumption) and
> **4f** (web UI: loopback default + shared-secret control endpoints) landed
> 2026-07-31 — see DESIGN_HISTORY.  Item **11** (Live Video Overlay) was
> scoped and finished the same day, 11a→11d: `/overlay` renders live shapes
> over a still image *or* a live MJPEG feed, and `tools/` holds the
> deploy-time config sync and calibration-still grabber.  **9C1** (the engine
> decision log) landed 2026-08-01, so the next run's recall is measurable for
> the first time, and **4b** (unused-import sweep) closed the same day.
>
> Model routing follows the Fable-era principle: the *thinking* for the
> remaining items is pre-done in the item text, so the Target line says who
> executes. **4e** (fixture strategy session) and **4h** (refactors) were
> gated on 4d and are now open. Don't-action lists: **4c**, **4g**.

---

## 9 — Post-4a accuracy re-baseline (Target: owner + any Claude session)

Design and code in [[SCOPE_sampling_floor.md]]; A and B are **done**, C is
**partially run** (2026-07-31) and blocked on C1/C2 below:

- [x] **A — runtime sweep-time self-measurement** (2026-07-30). `BaseMonitor`
  EMA of `_poll()` + sleep, `effective_cycle_sec()`, `get_stats()`,
  rate-limited slow-sweep INFO. See DESIGN_HISTORY 2026-07-30.
- [x] **B — sampling-floor gating** (2026-07-30). `system_runner` injects the
  floor via `DiscrepancyMonitor.set_sampling_floor()` (startup from
  `sampling_floor_sec`, then every 60 s from the measured cycle); Rule 2
  refuses pulses below `min_pulse_floor_multiple × floor`; per-pair high-duty
  advisory WARNING with opt-in `suppress_high_duty_pairs`.
- [~] **C — post-4a re-baseline protocol.** **Run 2026-07-31: 3 of 4 criteria
  pass.** Rule 2 precision **93.9 %** (≥ 80 % ✓), **zero** stale-refire phantoms
  (✓), **no** zero-correspondence pair (✓), adjusted recall **59.9 %** (≥ 70 %
  ✗). Overall precision 36.5 % → **89.4 %**. Full measurement, the miss/FP
  categorization and the caveats are in DESIGN_HISTORY (2026-07-31). Reproduce
  with `__decode_datz.py` → `__make_gt_export.py` → `__accuracy_report.py
  --poll 0.33`; the export is committed as
  `gt_anomalies_20260731_1830-2130.csv`.

**C1 has landed; C2 is now the only thing between here and a clean pass, and
it needs a controller. Neither is a rule change, and §Item C says don't touch
rule code until they're settled.**

- [x] **C1 — log engine decisions separately from recordings** (2026-08-01).
  `discrepancies_log.csv` is written by `remux_video_buffer._handle_start`
  *after* `_writer_semaphore.acquire()` succeeds, so a trigger the
  `max_concurrent_writers=2` cap drops leaves no row. The cap was saturated
  11.6 % of the run's wall clock but accounts for **43 % of the 108 "misses"**
  (vs 13 % of non-missed GT events — a 3.3× enrichment that controls for
  traffic clustering), which is why recall was not fairly measurable from that
  artifact. The engine now appends `engine_decisions.csv` itself
  (`_log_decision`, path injected by `system_runner`), with exact Unix event
  windows; `__accuracy_report.py` auto-detects either format and takes
  `--recording-log` to count decisions that never became clips. See
  DESIGN_HISTORY 2026-08-01. **The 59.9 % still stands unrevised** — it was
  measured on an artifact that predates this log, and only a fresh engine run
  produces a decision log to re-score.
- [ ] **C2 — re-score on a peak-hour run.** The 2026-07-31 sample is off-peak:
  max detector duty **32.8 %**, against the **80–94 %** that motivated this
  scope. The high-duty advisory never fired and the false-trigger storm
  condition is untested. Needs another ≥ 2 h engine run at peak (16:00–18:00)
  with a matching datZ pull — the datZ side of that window already exists, the
  engine side does not. That run now also produces the first real
  `engine_decisions.csv`, so score it with
  `__accuracy_report.py engine_decisions.csv <gt> --recording-log
  discrepancies_log.csv`: the recall number will be the first one not
  depressed by writer-cap drops, and the DELIVERY section says how big that
  correction was.

**Two rule-level findings recorded, deliberately not acted on:**

- **Rule 2's floor gate is asymmetric.** It gates the *orphan's* duration but
  says nothing about whether the *partner* is resolvable. Pair 26:33 produced 6
  of the 7 rule 2 FPs: det 33 has a median pulse of 0.70 s with **49 % under
  0.65 s**, so GT's chatter exception sees a 0.1–0.6 s partner blip the engine's
  0.325 s sampling cannot. A partner-side gate is the indicated fix.
- **Rule 1's threshold has no hysteresis.** All **12 of 12** rule 1 FPs measured
  5.03–5.06 s against the 5.0 s threshold. Requiring `threshold + one sampling
  cycle` would remove every one, at some cost to recall.

Suggested prompt (needs a peak-hour engine run + matching datZ in hand):
> In the ntcip project, continue Item 9C of ROADMAP.md: run C2 and re-score
> with `__decode_datz.py` → `__make_gt_export.py` → `__accuracy_report.py`,
> passing the run's `engine_decisions.csv` (not `discrepancies_log.csv`) plus
> `--recording-log`. Report each pass/fail number against
> SCOPE_sampling_floor.md §"Item C".

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
a session-sized unit. Remaining: **4e** and **4h**, both unblocked by 4d.
**4c** and **4g** are don't-action lists. **4f** (security) and **4a** (SNMP
sweep speed) landed 2026-07-31; **4d** (tests) and **4b** (unused imports)
2026-08-01 — see DESIGN_HISTORY.

### 4a. SNMP sweep speed — **done 2026-07-31**

Batching landed 2026-07-19, `snmp_chunk_size: 8` was adopted on the probe
verdict 2026-07-31, and the controller round trip verified it the same day:
**effective sampling cycle 1.53 s → ~0.33 s, edge capture 26 % → 94 %** (97 %
of ON pulses), channel map re-confirmed with every active channel self-matching.
Full measurement, including why the number came from the engine's own Rule 2
pulse quantization rather than from the capture, is in DESIGN_HISTORY
(2026-07-31). Two tool fixes landed with it: `__capture_ntcip.py` now issues one
batched `get(*group_oids)` per sweep (it had stayed per-group, so it never
exercised the batched path) and reports median/p95 sweep time, and
`video_engine/tools/__decode_datz.py` decodes datZ → event CSV via pyatspm's own
helpers.

**Carried forward:** `banks_events_20260719_1730.csv` is **1.0 s early** — the
ad-hoc extraction that produced it dropped the datZ header's sub-minute offset.
Re-decode from the committed datZ with `__decode_datz.py` rather than reusing
it, and read the 2026-07-19 entry's "+1.08 s skew" as +429 ms.

Note the outputs OIDs failed the 2026-07-20 probe for an unrelated reason — see
Item 10.

### 4b. Unused-import cleanup — **done 2026-08-01**

All eight sites swept in one pass, one line each, no behavior change:
`Counter32`/`Unsigned32`/`Gauge32` (`core/snmp_client.py`, `Integer32` kept),
`PhaseStatus` (`phase_monitor.py`), `DetectorState`/`OutputState`
(`examples.py`, `SignalState` kept), `datetime.timezone`
(`video_engine/routine_scheduler.py`), `sys` (`main.py`), `datetime`
(`utils/config_loader.py`), the whole `pysnmp.hlapi` line
(`utils/controller_control.py`), and the whole `data_models` line
(`ui/web_ui.py`). Each name re-confirmed unreferenced before removal and
un-re-imported after; all six suites green (234 cases) and every touched module
imports cleanly at runtime. 4c's false positives untouched. See DESIGN_HISTORY
2026-08-01 for the two things deliberately left in place (`set()`'s docstring
mention of `Counter32`, and `main.py`'s commented-out `sys.exit(1)`).

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

### 4d. Cover the remaining untested pure functions — **done 2026-08-01**

All six now covered, 51 new cases across three files, no mocking anywhere:

| Function | Home | Cases |
|---|---|---|
| `get_phase_oids` / `get_detector_oid` / `get_output_oid` (`oid_definitions.py`), `parse_signal_state` (`data_models.py`) | **new** `ntcip_monitor/tests/test_oid_helpers.py` | 33 |
| `ConfigProviderError` (`config_manager.py`) | **new** `video_engine/tests/test_config_manager.py` | 9 |
| `_resolve_pytz` (`discrepancy_engine.py`) | `video_engine/tests/test_discrepancy_rules.py` | 8 |

Jules's finding cited the last one as `get_safe_timezone` at line 194 — stale
name and line, same function. The test layout follows Item 7's precedent
unchanged (per-package `tests/`, stdlib `unittest`, `assertLogs` for the
warning assertion). See DESIGN_HISTORY 2026-08-01 for the two judgement calls:
importing `ntcip_monitor/core`'s leaf modules rather than the package, and
pinning `get_output_oid` to the OID the code emits today rather than the one
the controller accepts (ROADMAP 10 moves it).

This unblocks **4e** and **4h**.

### 4e. Broader test backlog — needs mocking/fixtures (4d has landed; open)

The remaining "lacks test coverage" findings all need a mocked `snmp_client`,
a Flask test client, an in-memory SQLite DB, or similar fixtures — a much
bigger lift than 4d's pure functions. Worth a real test-strategy session, not
ad hoc. Note two stubbing precedents now exist to copy rather than reinvent:
`test_snmp_batching.py` injects a fake `pysnmp.hlapi` into `sys.modules`, and
`test_overlay_shapes.py` overrides the live source's three PyAV seams.
Subjects: `DetectorMonitor`, `OutputMonitor`,
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

### 4h. Lower priority — readability (4d has landed; open)

- **`_evaluate_pair`** (244 lines) and **`_fire_trigger`** (**12 args** since
  9C1 added `event_window`; line numbers in the original finding are stale)
  — both legitimate, but both sit inside the discrepancy engine's
  carefully-worked-out state machine (the cooldown/active-trigger-id
  interaction documented in the module's own docstring — see CLAUDE.md).
  Refactoring either without tests in place first risks silently breaking
  behavior that took real effort to get right. Item 7 + 9C1 now pin
  `_evaluate_pair` through 20-odd integration and decision-log cases, so the
  safety net exists; extending it further before refactoring is still the
  cheaper order. `_fire_trigger`'s fix is a small `TriggerSpec`-like dataclass
  to replace the positional args — 9C1's `event_window` was passed as a single
  tuple specifically so it collapses into one field when that happens.
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

## 11 — Live Video Overlay — **done 2026-07-31 (11a–11d)**

`GET /overlay` draws pyatspm-calibrated detector loops and stopbars on a
`<canvas>` over the camera image, recolored from the live SNMP-polled
phase/detector/overlap state — pyatspm's `atspm video-overlay` output, driven
by live monitor data instead of a recorded event database.  Scoped and built
2026-07-31; conventions are in CLAUDE.md ("Live video overlay") and the
rationale for every decision is in DESIGN_HISTORY (five entries, 2026-07-31).

| Sub-item | Shipped |
|---|---|
| **11a** | `ntcip_monitor/ui/overlay/{shapes,status}.py` — pyatspm's shape-config reader vendored (tolerant of both CSV formats), pure status resolution |
| **11b** | `/overlay` page, four `/api/overlay/*` routes, `source.py` + `FileImageSource`, `overlay` config section, `overlay/` deployment data |
| **11c** | `RtspMjpegSource` — one shared PyAV decoder per camera, ref-counted subscribers, `stream_fps` publishing, reconnect/backoff |
| **11d** | `tools/sync_ui_config.py` + `tools/grab_calibration_still.py` (deploy-time, dry-run by default), calibration workflow documented in CLAUDE.md |

Tests: `python3 ntcip_monitor/tests/test_overlay_shapes.py` (86 stdlib
`unittest` cases; the live source's PyAV seams are stubbed so the suite still
runs on a bare interpreter).  An in-repo *route* test is still **4e**'s work.

### Reference map (kept — useful for any future overlay work)

| Concern | File | Lines |
|---------|------|-------|
| Shape data model + CSV format (vendor source) | [`pyatspm/.../data/video.py`](file:///home/hansrkid/pyatspm/src/atspm/data/video.py) | L95-234 |
| Drawing semantics ported to JS | [`pyatspm/.../video/overlay.py`](file:///home/hansrkid/pyatspm/src/atspm/video/overlay.py) | L46-99 |
| Calibration UI (the authoring tool) | [`pyatspm/.../video/calibrate.py`](file:///home/hansrkid/pyatspm/src/atspm/video/calibrate.py) | L112-492 |
| pyatspm CLI workflow | [`pyatspm/.../cli.py`](file:///home/hansrkid/pyatspm/src/atspm/cli.py) | L1573-1678 |
| Live phase/overlap state | [`ntcip/.../phase_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/phase_monitor.py) | L54-56, L219-225 |
| Live detector state | [`ntcip/.../detector_monitor.py`](file:///home/hansrkid/ntcip/ntcip_monitor/monitors/detector_monitor.py) | L48, L122-124 |
| Camera RTSP URL source of truth | [`ntcip/.../intersections.json`](file:///home/hansrkid/ntcip/video_engine/intersections.json) | L12-19 |

**Verified facts that outlive the item — don't re-derive:** the camera is
720×720 h264 @ 10 fps and `~/vid_cfg720.csv` records `720,720`, so the existing
calibration targets this exact view; shape `input` values match intersection
201's detector IDs with no remapping; that CSV is pyatspm's *legacy* per-row
format, which the vendored reader accepts alongside the two-section one; CSV
colors are **BGR** (`"255,0,0"` is blue), reversed exactly once in
`shapes.bgr_to_rgb()`; and FFmpeg's mjpeg encoder ignores `-q:v`/`qscale` —
quality comes from `qmin`/`qmax`.

### Live risks carried forward

- **Calibration staleness.**  `vid_cfg720.csv` matches the fixture's 720×720,
  but the fixture is from 2026-07-15.  If the camera has been re-aimed since,
  shapes will be subtly misplaced — check the file background before trusting
  the overlay.  (Checked once on 2026-07-31: the loops land on the pavement.)
- **Sampling floor.**  The effective NTCIP detector sampling cycle is
  ~1.0–1.5 s (CLAUDE.md), and 4a raised `snmp_chunk_size` to 8 without a
  re-baseline (Item 9C).  The 250 ms UI poll therefore shows detector state
  changing in steps much coarser than the video.  The page labels this; keep
  the caveat if you touch the template.

---

## Future (not yet scoped)

Items below need a planning pass before they're actionable (no Target/Scope/
prompt yet). Promote to a numbered item above — with the next unused stable
ID — when scoped.

- **Browser-based shape calibrator.**  Would remove pyatspm (and Tkinter, and
  cv2) from the overlay's authoring workflow entirely: draw loops and stopbars
  on the `/overlay` canvas and write the CSV server-side.  Item 11 deliberately
  kept `atspm video-calibrate-shapes` as the authoring tool because this is a
  substantial build — the drawing/edit/undo/snap interaction in
  [`calibrate.py`](file:///home/hansrkid/pyatspm/src/atspm/video/calibrate.py)
  L112-492 is ~380 lines of OpenCV/Tkinter event handling to reimplement.
  11a's reader already accepts both CSV formats, so the writer is the only new
  data-layer piece.
