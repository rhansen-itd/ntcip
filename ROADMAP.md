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

> **Status at a glance (2026-08-03).**
>
> **Nothing is waiting on hardware, and no further engine run is needed to
> proceed.** The 2026-08-02 Sunday run (11.9 h, 1553 starts — 3× the 08-01
> sample) is measured and committed; it answered all three of the re-measurement
> questions the previous status block was holding open, and surfaced a
> measurement defect that now gates the rule work.
>
> **Item 13 landed 2026-08-03**, and it moved every precision figure.
> `__accuracy_report.py` had matched a trigger to ground truth on GT *start*
> alone, scoring a rule 1 trigger that picks a disagreement up mid-event as a
> false positive. Adding a containment pass recovered **44 of the 08-02 run's
> 135 FPs** and **2 of 08-01's**: precision is now **94.1 %** (08-02) and
> **96.9 %** (08-01). The old bias was volume-dependent, so **any precision
> number quoted from before 2026-08-03 is a floor and is not comparable across
> runs of different length** — re-score rather than cite.
>
> **Suggested order:** **12** (re-derive both sub-items against the corrected
> numbers; 12A's sizing and 12B's "probably don't" verdict were both computed
> on the uncorrected data) → **14** (the dedup-window gap) → **2** or **3**
> (self-contained).
>
> **Also ready, no hardware:** **10**, and **4e / 4h** (unblocked by 4d).
>
> **The three re-measurements, answered.** *Clock skew* **drifts within a run**
> and is not a per-run constant: −0.30 s → +2.2 s → +1.2 s across 08-02, ~2.5 s
> peak-to-peak, no step (best scalar +0.75 s, still inside tolerance — but do
> not assume that holds). *Writer-cap decision loss* fell **33.6 % → 20.0 %**,
> so 9C4 recovered ~40 % of it. *Cleanup sweep* removed **190 of 877 clips
> (21.7 %)** versus the predicted 6.6 % residual — the prediction was sized on a
> 3.75 h run; 73.2 % of removals are different-group (only the sweep can catch
> those, and the population grows with run length), 15.8 % same-pair cooldown
> re-fires, and **11.1 % same-group leaks that 9C4 should have caught** (Item
> 14).
>
> **Automated duplicate-clip cleanup** landed 2026-08-01 as a same-session
> follow-on to 9C4 (`video_engine/video_cleanup.py`; see DESIGN_HISTORY) — a
> clip wholly contained in another from the same camera is deleted and its
> `discrepancies_log.csv` rows repointed at the survivor.
>
> **Item 9 is closed in full** — A, B, C and C1–C4 all landed between
> 2026-07-30 and 2026-08-01 (see DESIGN_HISTORY), and **9C4** removed the
> cross-pair duplicate storm that was burning writer slots on second copies of
> the same moment.
>
> Items **8** (remux manager thread-safety + the single-camera assumption) and
> **4f** (web UI: loopback default + shared-secret control endpoints) landed
> 2026-07-31 — see DESIGN_HISTORY.  Item **11** (Live Video Overlay) was
> scoped and finished the same day, 11a→11d: `/overlay` renders live shapes
> over a still image *or* a live MJPEG feed, and `tools/` holds the
> deploy-time config sync and calibration-still grabber.  **9C1** (the engine
> decision log) landed 2026-08-01, so the next run's recall is measurable for
> the first time; **4b** (unused-import sweep) and **5 + 6** (all three CFR
> video buffers deleted — `remux_video_buffer.py` is now the only backend)
> closed the same day, as did **9C3** (suppression log) and **9C4**
> (cross-pair duplicate rejection, which also taught `paired_detector_id` to
> accept a list).
>
> Model routing follows the Fable-era principle: the *thinking* for the
> remaining items is pre-done in the item text, so the Target line says who
> executes. **4e** (fixture strategy session) and **4h** (refactors) were
> gated on 4d and are now open. Don't-action lists: **4c**, **4g**.

---

## 13 — Accuracy matcher scored mid-event triggers as FPs — **done 2026-08-03**

`_match` compared only the trigger's event start against the GT anomaly's
start (±`--tolerance`). Rule 1 does not always observe a disagreement from its
beginning — after a cooldown, or picking one up part-way, `event_start_ts`
lands mid-event while ground truth records one long `extended_disagreement` —
so the trigger scored as a phantom despite the engine having caught the event.
A second containment pass now matches a trigger whose event start falls inside
`[gt.start − tol, gt.end + tol]`.

| Run | FPs before | Recovered | Precision |
|---|---|---|---|
| 2026-08-02 | 135 | **44** | 91.3 % → **94.1 %** |
| 2026-08-01 | 18 | 2 | 96.5 % → **96.9 %** |

Median engine-start lag past GT start on 08-02: **38 s** (p90 58 s, max 65 s).
The bias is volume-dependent (2.8 points over 11.9 h vs 0.4 over 3.75 h), so
pre-fix figures are floors and are not comparable across runs of different
length. Rule 1 precision on 08-02 went 90.0 % → 95.3 %; adjusted recall
87.6 % → 88.3 %. The 2026-07-31 legacy-format artifacts score **identically**
(89.4 %), preserving that guarantee; the 08-01 legacy path moved 96.8 % →
97.1 %.

Two things worth knowing if this is revisited. **Type scoping was kept** — a
rule 2 orphan claim landing inside a rule 1 disagreement window is a different
claim, not a match; an early count of 62 that ignored this was wrong, and 44 is
the type-respecting figure. **Pass 2 allows many-to-one** (pass 1 stays
one-to-one): a long disagreement the engine re-fires inside genuinely
corresponds to several triggers. On both committed runs that allowance was
never exercised — all 44 and all 2 landed on distinct GT events — so it is a
theoretical generosity today, and it is reported rather than silent.

---

## 14 — `dedup_window_sec` (1.0 s) is far shorter than a clip (24 s median) (Target: Opus)

9C4 suppresses a same-group `start` fired within `dedup_window_sec` (default
**1.0 s**) of the group's last emitted start for the same cameras. But the
median recorded clip on the 2026-08-02 run is **24.4 s**, so two same-group
starts a few seconds apart both fire, both record, and one clip ends up wholly
inside the other. Measured on that run: of 190 sweep deletions, **21 (11.1 %)
are same-group/different-pair** — precisely the population 9C4 exists to
prevent. (The other 169 are out of its reach by construction: 139 different-group,
30 same-pair cooldown re-fires.)

The disk-side sweep does catch these, so this is a cost question, not a
correctness one — a duplicate clip burns one of `max_concurrent_writers`
(default 2) for its whole length, and the writer cap still drops 20.0 % of
decisions. Raising the window is not obviously right: it must not swallow a
genuinely separate event on a busy group, and 9C4's held-pair AND-gated stop
already couples the folded pair's resolution to the owner's.

- [ ] Re-derive the window from the committed 08-02 artifacts: for same-group
  start pairs, plot inter-start gap against whether the resulting clips were
  contained. The 21 deletions carry both spans in `video_cleanup_log.csv`.
- [ ] Decide between a larger fixed default and deriving the window from the
  trigger's own `pre_roll + post_roll` (which is what actually determines
  overlap).
- [ ] Whatever lands, re-check it against the 26.2 % duplicate rate 9C4 was
  built for — the goal is fewer contained clips, not a broader suppression that
  starts eating real events.

---

## 12 — Two rule-level accuracy findings from the 9C re-baseline (Target: Opus)

**Unblocked — Item 13 landed 2026-08-03.** Both sub-items were sized on
precision figures the old matcher understated, and 44 of the 08-02 FP
population they reason about turned out to be mis-scored matches — all of them
rule 1, which is exactly what 12B argues about. Re-derive both against the
corrected numbers before implementing either; the shape of each argument may
survive, but the sizing will not. Current baselines to beat: 08-02 overall
94.1 % (rule 1 95.3 %, rule 2 92.8 %), 08-01 overall 96.9 %.

Both were surfaced by Item 9's measurement work and re-measured on the
2026-08-01 high-duty run; Item 9 itself is closed (see DESIGN_HISTORY,
2026-07-30 → 2026-08-01). §Item C's "don't touch rule code until the
measurement is settled" gate is open — these are the rule changes it was
holding back. They are independent of each other.

**What the 2026-08-02 run changed about each** (see DESIGN_HISTORY 2026-08-03):
rule 1 looked like the larger raw FP source (83 vs 52), inverting 08-01's
ordering — but **44 of those 83 were Item 13's mis-scoring**, leaving rule 1
with 39 real FPs against rule 2's 52, so 08-01's ordering stands after all and
**12A is once again the higher-value item**. Pair
26:33 was **50.0 % precision on 08-01** (7/14) and *improved* to 62.2 % on
08-02, the day its phase gained the most volume, which weakens "high duty
drives its FPs". Volume change vs precision change across all 17 pairs is
r = +0.19. And 7 of 17 pairs carried under 15 triggers on 08-01 — five reading
"100 %" on 1–9 triggers — so per-pair 08-01 figures are thin evidence.

- [ ] **A — Rule 2's floor gate is asymmetric, and it is the single largest
  FP source.** The gate bounds the *orphan's* duration but says nothing about
  whether the *partner* is resolvable. Pair 26:33 produced **6 of the 9 rule 2
  FPs** on the 2026-08-01 run (and 7 of all 18), as 1.32–1.94 s orphan pulses
  on det 26 — the same pair as 2026-07-31, opposite detector. It is also the
  only pair whose precision is poor (7 matched / 14 triggers) while having
  **zero** true misses. A partner-side gate is the indicated fix: fixing 26:33
  alone would take overall precision from 96.5 % to ~97.8 %.

- [ ] **B — Rule 1 has no hysteresis, but the obvious evidence for adding it
  is not diagnostic, and the fix currently looks net-negative.** All 9 rule 1
  FPs measured 5.033–5.081 s against the 5.0 s threshold — but **all 281 rule 1
  triggers measure ≤ 5.1 s**, because the engine fires the instant the
  disagreement crosses the threshold, so `disagreement_sec` is the duration *at
  fire time*, not the event's true length. "The FPs cluster at threshold"
  describes every rule 1 trigger and separates nothing. Judge it against
  ground-truth durations instead: the 15 true-missed `extended_disagreement`
  events run 5.1–8.0 s (median 5.4, **9 of 15 ≤ 5.5 s**), so firing at
  `threshold + one cycle` (5.33 s) would push a meaningful share of genuine
  events below the bar to remove 9 FPs out of 518 triggers. **Don't implement
  this without re-deriving it from GT durations first** — the answer may well
  be "no".

Both can be re-derived from the **committed** 2026-08-01 and 2026-08-02
artifacts — no new engine run is required. **The controller clock skew must be
re-measured per run** and now also *within* one: it drifted −0.30 s → +2.2 s
→ +1.2 s across 08-02 (best scalar +0.75 s; +4.49 s on 08-01, ~0 s on
07-31). Measure it by cross-correlating engine-observed detector edges
(`engine_suppressions.csv`, rule-2 rows of `engine_decisions.csv`) against the
controller's 82/81 codes — nearest-neighbour matching aliases once the offset
approaches the ~3.2 s median inter-edge gap.

Suggested prompt:
> [Opus] In the ntcip project, do Item 12 of ROADMAP.md, after Item 13 has
> landed: re-derive 12A's partner-side gate and 12B's hysteresis argument
> against the corrected precision figures, using the committed 2026-08-01 and
> 2026-08-02 artifacts, then implement whichever survives.

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
`VideoBufferServer`. **Note:** `VideoBufferServer` names no class in this repo
and never did (checked 2026-08-01 against the CFR files Items 5/6 deleted, at
commit `0c2e11b`) — treat it as a stale finding and cover
`remux_video_buffer.VideoBufferManager` instead, which
`test_remux_manager.py` already partly does.

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
