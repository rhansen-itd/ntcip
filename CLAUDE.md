# NTCIP Traffic Monitor + Video Engine

Edge/central system for traffic signal monitoring: an NTCIP SNMP monitor detects
controller events (phases, detectors, outputs), and an independent video engine
buffers RTSP/HTTP camera streams in RAM and saves clips when two co-located
detection technologies (e.g., Radar vs. Video, or Radar vs. Loop) disagree about
the same physical zone. Target deployment is per-intersection edge boxes (e.g.,
Intel J1900 — weak CPU, constrained disk) as well as a central multi-intersection
server.

This file documents conventions already established in the code — verified
against the implementation, not just aspirational. Treat anything stated as
"implemented" as load-bearing: don't casually change it without understanding
why it's built that way.

## Workflow & documentation

Three docs divide the labor; keep them in sync:

- **CLAUDE.md** (this file) — what the code currently *does*. The "now"
  snapshot of conventions, verified against the implementation.
- **[ROADMAP.md](ROADMAP.md)** — what still needs *deciding or building*.
  Priority-ordered, stable-ID numbered items, each with a Target model and a
  Suggested prompt. Read it first; sessions are scoped there.
- **[DESIGN_HISTORY.md](DESIGN_HISTORY.md)** — *why* past decisions were made.
  Build history plus an append-only, dated Decisions log.

**At the end of a session:** check off completed boxes in ROADMAP.md, append a
dated entry to DESIGN_HISTORY.md's Decisions log capturing the decision and its
rationale (not just the diff), update this file if a load-bearing convention
changed, and move fully-finished ROADMAP items into DESIGN_HISTORY.md. Default
model routing is **Opus, one item end-to-end per session**; see ROADMAP.md's
intro for the item conventions.

## Module boundaries — read this before touching either package

There are two independent top-level packages and they must stay that way:

- `ntcip_monitor/` — SNMP polling, event emission (phase/detector/output state
  changes). Entry point: `run.py` → `ntcip_monitor.main.NTCIPMonitorApp`.
- `video_engine/` — RTSP/HTTP capture, RAM pre-roll, disk recording, and the
  discrepancy-detection "brain". Entry point: `video_engine/system_runner.py`.

**Neither package imports the other.** `video_engine/discrepancy_engine.py`
subscribes to `ntcip_monitor` detector events in-process (via `system_runner.py`,
which wires both packages together), but the moment a discrepancy is confirmed,
the *only* way it reaches the video buffer is by writing a trigger file to a
spool directory (the "Hot Folder"). Never add a direct import from
`discrepancy_engine.py`/`remux_video_buffer.py` into `ntcip_monitor`, or vice versa —
this decoupling is intentional so the two halves can be deployed, tested, and
swapped independently (e.g., a future non-NTCIP discrepancy source should be
able to drive the same video engine with zero video_engine changes).

### Hot Folder pattern (the bridge)

Implemented in `discrepancy_engine.py` (writer) and `remux_video_buffer.py`
(reader — a `Path.glob("trigger_*.json")` oldest-first poll loop).

- Filename: `trigger_{iso8601}_{uuid4_short}.json`
- Writer: write full JSON to `*.tmp`, then atomic `os.rename()` to `*.json`.
  Never write the final filename directly — a reader could see a partial file.
- Reader: poll the directory (current interval ~2–5s region, see
  `remux_video_buffer.py` poll loop), sorted oldest-first, never with a sleep
  inside the frame-capture loop itself.

Trigger file schema (enforced in `remux_video_buffer.py`; the canonical, field-by-field
reference is `config_manager.py`'s module docstring — a real section as of
2026-07-31, previously only claimed to exist):

```json
{
  "trigger_id": "uuid4-hex-string",
  "action": "start",               // "start", "stop", or "extend"
  "event_timestamp": 1738923456.7, // Unix timestamp when discrepancy DETECTED
  "reason": "detector_disagreement", // what the engine always writes today
  "intersection_id": "1234_main",
  "timezone": "America/Boise",      // for local times in the CSV log
  "cameras": ["cam1", "cam2"],      // specific IDs or ["all"]
  "pre_roll_sec": 10,
  "post_roll_sec": 20,
  "max_duration_sec": 300,
  "metadata": {"det1": "radar", "det2": "loop", "lag": 2.5}
}
```

**Single-camera assumption (load-bearing).** `_handle_start`
resolves `cameras` against the configured streams and records only
`target_cams[0]` — one writer per trigger. A multi-camera trigger logs a WARNING
with `cameras_requested`/`cameras_recorded` and is otherwise honored for the
first camera. This is deliberate (no second camera exists to test against), not
an oversight; don't "fix" it by adding per-camera writers until one is deployed.
Note a pair whose two detectors name different `camera_id`s does produce a
two-camera trigger, so the warning is reachable in real config.

Don't add fields casually — both sides (writer in `discrepancy_engine.py`,
reader in `remux_video_buffer.py`) need to agree, and `config_manager.py`'s
docstring is the canonical schema reference.

### Discrepancy rules (the "brain")

`video_engine/discrepancy_engine.py`'s module docstring is the authoritative
spec for the three rules (Extended Holdover, Orphan Pulse, Chatter Exception)
and the Rule 1 active-resolution state machine. Read it before modifying
trigger-firing logic — it's dense but precise, including the cooldown/active-
trigger-id interaction that prevents double-firing. Don't re-derive this from
first principles; the docstring already encodes the corner cases that were
worked out by hand.

**Detector groups and cross-pair duplicate rejection (2026-08-01, ROADMAP 9C4
— load-bearing).** `paired_detector_id` accepts a **scalar or a list**; pairs
are the union of all normalized links, and **groups** are the connected
components of the resulting pair graph (`_build_groups`). A 3-way group can
therefore be authored explicitly (A `[B,C]`, B `[A,C]`, C `[A,B]`) or as a ring
of scalars (A→B, B→C, C→A) — for n=3 both give the identical 3 pairs. From n=4
they diverge (ring 4 edges, list 6) and **both are legitimate**: a group is a
**dedup scope only**, never an instruction to evaluate every internal pair, or
a 4-ring silently grows comparisons nobody asked for. Pair generation stays
link-driven. (Unrelated to NTCIP's 16-channel "detector groups" in
`system_runner`'s poll planning — same word, different thing.)

Within a group, a `start` fired less than one **dedup window** after the
group's last emitted `start` **for the same
cameras** is not written to the Hot Folder: with triangles, one event where B
disagrees with both A and C fires on `A:B` and `B:C` on the same tick, two
clips of one moment burning both writer slots (137 of 523 starts, 26.2 %, on
the 2026-08-01 run). Four properties are load-bearing: the window anchors on
**emitted** starts only (a suppressed row never anchors, or a storm rolls the
window forever); cameras are part of the key; a `stop` is never suppressed and
never anchors; and a suppressed Rule 1 `start` **must not** set
`active_trigger_id` — it engages the pair cooldown instead, because a later
`stop` reusing that ID would reference a recording the buffer never started.

**Dropping the duplicate `start` is safe; dropping its `stop` is not — the
stop is an AND** (2026-08-01, the same item). A clip stands for every
disagreement folded into it, so if the owner pair resolves at t+4 while the
folded pair keeps disagreeing to t+30, stopping on the owner alone ends the
footage before the event it was suppressed for is over. A suppressed duplicate
registers on the owner's `held_pair_keys`; the owner's resolution state machine
treats the disagreement as resolved only when its own detectors agree **and**
every held pair's do, and a re-divergence on any of them restarts the post-roll
countdown. A held pair runs **no rules at all** while held (guard 0 in
`_evaluate_pair`, ahead of the cooldown guard because the callback path can
clear a cooldown early), and is released into a **fresh cooldown** when the
stop goes out so it doesn't re-fire on the tail of the footage just recorded.
Two asymmetries fall out and both are deliberate: **a Rule 1 start is never
folded into a Rule 2 recording** (a Rule 2 clip's length is fixed at fire time
and never gets a stop, so it can't be held open — measured cost, 2 of 137
duplicates on the 2026-08-01 run), and **a Rule 2 duplicate never holds**
anything open (its pulse is complete before it is even evaluated). A
derived group spanning more than one `phase` logs a WARNING (transitive
over-grouping from one stray link); the derived groups are logged at startup
next to `_pairs`. **The schema lives in three places that must agree** —
`_build_structures`, `config_manager.py`'s docstring, and
`__make_gt_export.py:_load_pairs` — since an export covering fewer pairs than
the run scores every trigger on a missing pair as a false positive.

**The window is per rule, and the Rule 2 half is guarded (2026-08-03, ROADMAP
14 — load-bearing).** One number can't serve both rules, because the guarantee
a fold rests on differs: `dedup_window_rule1_sec` (new key, default **10.0** ≈
`pre_roll + post_roll` here) covers a Rule 1 candidate folding into a Rule 1
owner, safe at **any** width thanks to the AND-stop above;
`dedup_window_sec` (**raised 1.0 → 3.0**) covers a Rule 2 candidate, which has
no lever to hold a clip open and so must pass `_owner_covers_event`. Each key's
`0` disables **its own path only**. The guard compares in **event
coordinates** — a clip is `[event_start − pre_roll, that + max_duration_sec]`,
i.e. what the candidate's own clip would have been — so it asks whether the
owner's footage reaches at least as far in *both* directions. A Rule 2 owner's
span is fixed at fire time and rides on `_GroupFire` (`span_start`/`span_end`);
a Rule 1 owner is judged by **liveness** (`active_trigger_id` still set), and
one that already stopped is refused (unreachable at the defaults; it exists so
raising the window in config can't silently lose footage). Both widths are
measured on clip **containment**, not the fire-time clustering that sized the
original 1.0 s: median preventable gap 1.62 s, 29 of 38 within 3 s, 35 within
10 s, three outliers ≥ 38 s left to the disk sweep. Replaying both committed
decision logs **through the real monitor** (it reproduces the 08-02 run's own
457 suppression marks 457/457 at the shipped settings, and 135 on 08-01):
08-02 → **545 of 1553 starts (35.1 %)**, preventing **17 of the 38** contained
same-group clips; 08-01 → **164 of 523**. The scope predicted 543/170 — the
08-01 gap is the guard's **start-side** check, which the scope's audit omitted
and which is load-bearing: even at the old 1.0 s window it refuses 5 folds on
08-02 and 3 on 08-01 that the shipped runs performed with the pulse partly
outside the clip.

Two accuracy-critical Rule 2 mechanics (added 2026-07-19, see DESIGN_HISTORY):
the partner-overlap test runs against `_DetectorState.on_intervals` — a
bounded deque of completed `(on_ts, off_ts)` ON intervals appended on the
falling edge under the per-detector lock, pruned only by the evaluator thread
— **not** a most-recent-edge scalar (a scalar cannot represent an interval;
that shape caused both false negatives and leaked Rule 3 overlaps). And a
Rule 2 verdict older than `_ORPHAN_DECISION_GRACE_SEC` past its window close
is discarded, never fired late (the pre-roll footage is gone by then).

**Sampling-floor gating (added 2026-07-30, ROADMAP 9 A+B — load-bearing).**
The engine must not evaluate evidence finer than its own sampling resolution.
The floor is **injected, never imported**: `system_runner` calls
`DiscrepancyMonitor.set_sampling_floor()` at startup from the config's
`sampling_floor_sec` (default 1.6 = the *pre-4a* NTCIP reality) and every 60 s
thereafter from `DetectorMonitor.effective_cycle_sec()` — do not "simplify"
this by importing `ntcip_monitor` into the engine. Rule 2 refuses orphan
pulses shorter than `min_pulse_floor_multiple × floor` (default 2.0×),
counting them in the per-pair `below_floor_suppressed` and recording each one
in `engine_suppressions.csv` (below). **The runtime
measurement, not the 1.6 default, is what governs in production** — since 4a
landed, intersection 201 measures ~0.33 s, so the Rule 2 gate is ~0.65 s and
the rule is fully live (114 of 180 triggers in the 2026-07-31 run). Before 4a
the same default put the gate at 3.2 s, above a typical 2.0 s
`lag_threshold_sec`, which disabled Rule 2 in practice; if you read that
statement anywhere else, it is pre-2026-07-31. **Rule 2's precision at the new
floor is now validated: 96.3 % on the 2026-08-01 high-duty run (ROADMAP 9C2).**
The gate suppressed 710 distinct pulses over that run (998 rows, one per
affected pair), median duration 0.34 s — i.e. sub-cycle blips, not lost
signal. A rolling 120 s ON-duty fraction per pair
drives a rate-limited WARNING; `suppress_high_duty_pairs` (default false) can
disable Rules 1+2 for such pairs. Because the duty computation reads the same
`on_intervals` deque, its retention horizon is now
`max(3 × threshold + grace, 120 s)` — keep the two consistent if either
changes.

**Partner sub-floor-activity gate (added 2026-08-03, ROADMAP 12A —
load-bearing).** The floor gate bounds the *orphan's* side of Rule 2; this one
bounds the **partner's**, from the same principle. Rule 2's evidence is that
the partner was completely OFF — worthless when that partner keeps producing
0.1–0.4 s pulses a ~0.33 s sampler cannot see. That is the dominant rule-2 FP
mechanism in ground truth (the orphan was real in 61 of 61 FPs checked; the
partner *did* respond, sub-floor, in 6/9 and 28/52 of the two runs' rule-2
FPs, against ~1 % of TPs). The signal is invisible at event time, so the gate
is **statistical**: each `_DetectorState` keeps `below_floor_pulses`, a deque
of the pulse windows *its own* candidates were declined at by the floor gate,
and a Rule 2 candidate whose **partner** has ≥ `partner_blip_max` (config,
default **5**) entries inside the trailing `partner_blip_window_sec` (default
**300**, `0` on either disables) is declined — per-pair
`partner_blip_suppressed`, plus an `engine_suppressions.csv` row with reason
`partner_below_floor_activity` carrying `partner_blip_count` and the horizon.
Four things are load-bearing: the gate sits **strictly after** the floor gate
(a below-floor pulse is always `below_sampling_floor`, so the two populations
stay disjoint); the deque counts **distinct pulses, not evaluations** (a
triangle declines one physical pulse once per pair — entries are deduped
against the deque's tail); it is the **one `_DetectorState` field not guarded
by the lock** (written and read only on the evaluator thread); and the
parameters are measured, not guessed — replayed over both committed runs, ≥5
in 300 s kills 6 FP + 5 TP on 08-01 (→ **98.0 %** overall / 98.7 % rule 2) and
15 FP + 10 TP on 08-02 (→ **95.0 %** / 94.7 %), while N=3 triples the TP cost
for the same FPs and 600 s horizons are strictly worse. **Those two figures
are replay projections, not measured runs** — the table further down still
reports the last measured run. Kills concentrate on 26:33, whose det 33 is the
#1 below-floor producer on both runs by ~2.4× and probably needs physical
service; the gate is rolling precisely so it recovers on its own if that
happens. Rule 1 hysteresis was evaluated on the same evidence and
**rejected** (ROADMAP 12B — 4–9 FPs prevented against 22–53 genuine events
demoted); the arithmetic lives in `discrepancy_engine.py`'s Rule 1 docstring
section, and there is deliberately no config key for it.

The rule functions are pinned by `video_engine/tests/test_discrepancy_rules.py`
(154 stdlib-`unittest` cases, incl. the stale-refire guard, the floor gate, the
partner gate, the
decision log, the suppression log, group derivation in both config forms,
cross-pair duplicate rejection and its AND-gated stop, and `_resolve_pytz`) —
run it after any
engine change:
`python3 video_engine/tests/test_discrepancy_rules.py`.
Accuracy vs. an ATSPM ground-truth export is measured with
`video_engine/tools/__accuracy_report.py` (correspondence-based
precision/recall; models cooldown + poll aliasing), not by comparing raw
counts. Build the export with `__decode_datz.py` → `__make_gt_export.py`, and
pass the *same* intersection config the engine ran with — the three
intersection JSONs disagree on pairs, and scoring against the wrong set
invents misses — the cheap way to tell which config a run used is the set of
`pair_key` values in its decision log (the 2026-08-02 run used the root
`_intersections.json`, 17 pairs, **not** `video_engine/intersections.json`,
which defines 5).

**The matcher matches on start alignment *and* containment (2026-08-03,
ROADMAP 13 — load-bearing).** `_match` originally compared only the trigger's
event start against the GT anomaly's **start** (±`--tolerance`, 3.0 s). Rule 1
does not always observe a disagreement from its beginning: after a cooldown,
or picking one up part-way, `event_start_ts` lands mid-event while ground
truth records the whole thing as one long `extended_disagreement` — so the
trigger scored as a phantom despite the engine having caught the event, with
the two durations agreeing exactly. A second pass now matches a trigger whose
event start falls inside `[gt.start − tol, gt.end + tol]`. On the 2026-08-02
run that recovered **44 of 135 apparent FPs**, the engine's start sitting a
median **38 s** past the GT start. The bias is **volume-dependent** — 2.8
points over 11.9 h against 0.4 over 3.75 h — so pre-2026-08-03 precision
figures are floors and are **not** comparable across runs of different length.

Pass 1 (start-aligned) stays one-to-one; pass 2 (containment) allows
many-to-one, because a long disagreement the engine re-fires inside really does
correspond to several triggers. That allowance is reported, not hidden — and on
both committed runs it was never exercised (all 44 and all 2 landed on distinct
GT events), so it is currently a theoretical generosity, not a live one.

Last measured **2026-08-02** (11.9 h, 1553 starts, 3× the prior sample):
overall precision **94.1 %**, rule 1 95.3 %, rule 2 92.8 %, adjusted recall
88.3 %, writer-cap delivery loss 20.0 % (down from 33.6 %).
2026-08-01 (ROADMAP 9C2, high-duty, 3.75 h): **96.9 %**, rule 1 97.5 %, rule 2
96.3 %, adjusted recall 86.3 %, zero stale-refire phantoms — all four §Item C
criteria passed. (Both figures pre-13 were 91.3 % and 96.5 %.) Artifacts for
both runs are committed
(`engine_decisions_*`, `engine_suppressions_*`, `discrepancies_log_*`,
`banks_events_*`, `gt_anomalies_*`, plus `video_cleanup_log_20260802.csv`).
The superseded 2026-07-31 figures (89.4 % / 59.9 %) were read off the
*recording* log and were a floor for a different reason.

**Two traps when comparing runs**, both hit on 2026-08-03 and both ruled out
before the matcher was found: per-pair figures from the 08-01 run are thin
(7 of 17 pairs under 15 triggers, five reading "100 %" on 1–9), and traffic
composition shifts between days (ph6 gained 9.6 points of share on the Sunday
run) — but re-weighting one run's per-pair precision onto the other's trigger
mix moves it only ~0.7 points, so mix is *not* an explanation for a precision
gap. Neither is dedup (duplicates scored 91.9 % vs non-duplicates' 91.1 %).

**Controller clock skew is real, must be measured per run, and drifts *within*
a run (2026-08-01, revised 2026-08-03 — load-bearing).** The engine stamps
events with the monitoring machine's clock; the ground truth is stamped by the
Econolite controller. Nothing keeps them in sync. Measured values so far: ~0 s
(2026-07-31), **+4.49 s** (2026-08-01), and on 2026-08-02 a *drift* from
−0.30 s at 09:39 to **+2.2 s** by 18:15 and back to +1.2 s — ~2.5 s
peak-to-peak with no step, even though the clock had been synced shortly
before that run. `--clock-offset` takes a single scalar, which was still safe
there (best fit +0.75 s, max residual ~1.45 s, inside the 3.0 s tolerance);
on a run that wanders further it would not be, and the run would need scoring
in segments.

Uncorrected, a skew larger than `--tolerance` drags overall precision to
**11.6 %** — a collapse that looks like a catastrophic engine regression and is
not one. The tell: every candidate false positive reports nearly the *same*
`nearest GT Δ`, while the per-pair table still shows healthy trigger and GT
counts on the same pairs. (Contrast the ROADMAP 13 matcher defect, fixed
2026-08-03, whose FPs showed *scattered* deltas — median 117.9 s on 08-02,
only 1 of 135 inside 5 s.)

Measure the skew from engine-observed detector edges (`engine_suppressions.csv`
and rule-2 rows of `engine_decisions.csv` carry exact Unix ON/OFF windows)
against the controller's 82/81 codes. **Use cross-correlation, not
nearest-neighbour matching** — scan candidate offsets and take the peak match
count; nearest-neighbour aliases onto the wrong pulse once the offset
approaches the ~3.2 s median inter-edge gap, and reports a falsely small skew.
The result is otherwise insensitive to the exact value (3.5–5.5 s scored
identically on 08-01, since the offset only has to land inside the tolerance)
— what matters is not leaving it at zero.

Unrelated but adjacent: the monitoring machine here runs **PDT** while the site
is **MDT**, so `datetime.fromtimestamp()` in an ad-hoc script prints an hour
behind the site-local times `__accuracy_report.py` and the datZ filenames use.

**Three logs, and they mean different things (2026-08-01, ROADMAP 9C1 + 9C3 —
load-bearing for anyone measuring accuracy).** All land in `output_dir`:

- **`engine_decisions.csv`** — written by `discrepancy_engine._log_decision`,
  one row per trigger the engine emitted, appended right after the Hot Folder
  rename succeeds and before any post-write state management. Nothing
  downstream can suppress a row. **Score accuracy against this file.** The
  path is injected by `system_runner` (`decision_log_path=output_dir /
  "engine_decisions.csv"`); `None` disables it, which is the default for any
  other construction path. Writing is best-effort — a failed append logs an
  ERROR and is swallowed, because a full disk must never stop a recording.
  Rows carry the underlying event's `event_start_ts` / `event_end_ts` as exact
  Unix floats (either blank where the rule doesn't define it: a Rule 1 `start`
  has no end yet, a `stop` has neither), so no consumer has to recover timing
  from a 1-second local stamp plus a regex. `_DECISION_LOG_FIELDS` is
  **append-only** — an existing log is never rewritten, so a new column
  inserted mid-list desynchronizes a resumed file from its header. Rows also
  carry `dedup_group` / `suppressed_as_duplicate` / `duplicate_of_trigger_id`
  (9C4): a trigger rejected as a cross-pair duplicate is **marked here, not
  dropped and not moved to the suppression log**, because ground truth
  contains the same event on both pairs of the group — a consumer that never
  saw the row would score the sibling pair's event as a miss. The event
  window reaches `_fire_trigger` as one optional `event_window` tuple and is
  deliberately **not** added to the trigger payload (the video buffer has no
  use for it, and the Hot Folder schema is intentionally hard to grow).
- **`discrepancies_log.csv`** — written by the video-buffer backend, one row
  per clip actually *recorded*. `remux_video_buffer._handle_start` calls
  `_log_discrepancy_to_csv` only after `_writer_semaphore.acquire()` succeeds,
  so a trigger dropped by the `max_concurrent_writers` cap leaves no row.
  Measured on the 2026-07-31 run, the cap was saturated 11.6 % of wall clock
  yet accounted for 43 % of the apparent misses. **Recall read off this file
  is a floor, not an estimate.** It is also the one log that is ever
  *rewritten*: the duplicate-clip sweep (below) repoints `Video_Filename` at a
  surviving clip. Rows are never added or removed by that, so anything scored
  from timestamps is unaffected.
- **`engine_suppressions.csv`** — written by
  `discrepancy_engine._log_suppression`, one row per candidate the engine
  deliberately **declined** to act on, tagged with a `reason` column. Two
  reasons today: `below_sampling_floor` (the Rule 2 floor gate) and
  `partner_below_floor_activity` (the 12A partner gate; its rows carry
  `partner_blip_count` / `partner_blip_window_sec`, blank on the other
  reason). Same injected
  path (`suppression_log_path`, `None` disables) and the same best-effort
  contract as the decision log; both share `_append_csv_row`, so the
  never-re-header-a-resumed-file behavior cannot drift between them.
  `_SUPPRESSION_LOG_FIELDS` is **append-only** for that reason.
  `sampling_floor_sec` and `min_pulse_floor_multiple` are stored as separate
  columns, not just their product, so a consumer can recompute the gate at
  other multiples and recover the counterfactual from a finished run.
  **A suppressed row is not a would-have-fired trigger** — the gate sits at
  candidate registration, ahead of Rule 2's partner-overlap test, so recall
  attributed to it is an upper bound. `reason` is a plain string precisely so
  new populations can land here as new values, with no schema change and no
  fourth file — the partner gate was the first to take that path, and the ones
  `__accuracy_report.py` still *models* (cooldown, grace expiry, high-duty)
  can follow it. The cross-pair duplicate deliberately did **not**
  land here — see the decision log above.

`__accuracy_report.py` auto-detects which format it was handed (on the
presence of an `event_timestamp` column) and says so in its first line; the
legacy path is preserved so the committed 2026-07-31 artifacts still score
identically. Pass `--recording-log` alongside a decision log to get a DELIVERY
section counting decisions that never became clips. Rows marked
`suppressed_as_duplicate` are **scored like any other trigger** and excluded
only from DELIVERY (they have no clip by design, not by back-pressure);
verified by re-scoring the 2026-08-01 log with duplicates marked — precision
and recall come out identical.

### Duplicate-clip cleanup — the disk-side half of dedup (2026-08-01, load-bearing)

`video_engine/video_cleanup.py` deletes a clip when **another clip from the
same camera covers its whole wall-clock span**, and repoints every log
reference at the survivor. It is the counterpart to 9C4, not a replacement:
9C4 stops the *engine* firing twice **within a detector group**, and by
construction cannot touch a Rule 2 orphan clip nested inside a Rule 1 clip
(it explicitly refuses to fold those), two unrelated pairs disagreeing about
the same approach, or a hand-dropped trigger over live footage. Sized against
the committed 2026-08-01 artifacts (retrospectively, before 9C4 was live):
**91 of 348 recorded clips (26.1 %) were wholly contained in another**; 68 of
those were the population 9C4 now rejects upstream, predicting a **6.6 %**
residual.

**Measured for real on 2026-08-02, the first run with both live: 190 of 877
clips (21.7 %, 93 min, 371 MB), not 6.6 %.** The prediction was sized on a
3.75 h run and the dominant population grows with run length. Breakdown
(corrected 2026-08-03 — the first published split, 139/30/21, was joined
through the *rewritten* recording log, where every kept file appears in ≥ 2
rows and aliases deleted clips onto their survivors; classify by the
trigger-ID prefix in the clip filename instead, which maps 190/190 uniquely):
**152 (80 %) different-group** — unrelated pairs covering the same approach,
which only this sweep can catch; **38 (20 %) same-group/different-pair, which
9C4 should have caught** — its single `dedup_window_sec` was 1.0 s while the
median clip is 24.4 s and the sibling pair typically crosses threshold
1.0–2.3 s later, so same-group starts both record and one ends up nested. The
per-rule windows that landed 2026-08-03 (ROADMAP 14, above) prevent **17 of
those 38** upstream; the rest are Rule 1 folded into a Rule 2 owner (refused by
design) and three gap outliers ≥ 38 s. And
**zero same-pair** — the 60 s cooldown spaces same-pair clips further apart
than a 24.4 s median clip can contain.

Four things are load-bearing:

- **A clip's span is recovered, not recorded.** `end_ts` = the file's **mtime**
  (`ClipRemuxer._finalize` closes the container as its last act), `duration` =
  the container's own duration via PyAV (exact — clip length equals the source
  PTS span by construction, there is no FPS to guess), `start_ts` = the
  difference. That is cross-checked against the **dispatch epoch in the
  filename** (`{trigger8}_{camera}_{int(time.time())}{ext}`): a clip whose
  mtime and name disagree by more than 5 s is **skipped, never deleted** (the
  likely cause is a copy that didn't preserve mtime). A file whose name doesn't
  parse as a clip is not a candidate at all, so the CSV logs and any hand-named
  export in `output_dir` are safe by construction.
- **`plan_removals` is one pass over `(start asc, end desc, name)` against a
  running list of survivors.** Three properties fall out: a keeper is never
  itself deleted (so no rewrite can point at a file a later step removes, and
  no chain resolution is needed), mutual containment resolves deterministically,
  and it is **conservative** — a clip starting slightly *before* a much longer
  one is kept, because it isn't contained. Keeping an extra file is a cost;
  losing unique footage is a defect. The `tolerance_sec` (default 0.5) exists
  only so two clips of the *same* moment that differ by poll latency still
  compare as duplicates; at 0.0 the same run yields 31 removals instead of 91.
- **Logs are rewritten first, the file is deleted second.** The reverse order
  would leave a row naming a file that is gone; this order, if the delete
  fails, leaves a row naming a clip that exists and still contains the event.
  If a rewrite raises, **nothing is deleted that sweep**. Which logs get
  rewritten is the single table `REFERENCE_COLUMNS` (`discrepancies_log.csv` /
  `Video_Filename` today) — that is the whole extension point; the engine's two
  logs are written before any clip exists and carry no filename.
- **Two independent guards keep an in-flight recording off the list**: the
  manager's live view of its active + draining writers (`_protected_clip_paths`,
  authoritative in-process) and `cleanup_min_age_sec` (mtime-based, which also
  covers clips left by a crashed run). The sweep runs on its own daemon thread
  and shares the manager's `_csv_lock` with `_log_discrepancy_to_csv`, so a
  rewrite can't interleave with an append.

Every deletion is audited in **`video_cleanup_log.csv`** (`output_dir`,
`_CLEANUP_LOG_FIELDS` append-only like the engine's logs) carrying *both*
spans — deleting footage is the one irreversible thing this system does, and
the row has to be enough to re-check the decision after the evidence is gone.
Config is the intersection's optional `video_cleanup` block (`enabled` default
**true**, `interval_sec` 300, `tolerance_sec` 0.5, `min_age_sec` 60); the
canonical reference is `config_manager.py`'s docstring. Manual front end:
`python3 video_engine/tools/cleanup_clips.py --output-dir <dir>` — **dry run
until `--apply`**. `video_cleanup.py` imports neither `ntcip_monitor` nor
`remux_video_buffer` (the manager imports *it*), and PyAV is imported lazily
inside `probe_duration_sec` so the module and its tests load on a bare
interpreter.

## Config abstraction

`video_engine/config_manager.py` already implements the provider pattern:
`ConfigProvider` (ABC, `get_intersection_config()` / `list_intersection_ids()`),
with `JsonFileConfigProvider` (edge) and `SqliteCentralConfigProvider` (central)
as concrete implementations. `system_runner.py` defaults to the JSON provider.
When adding intersection-level config needs, extend `ConfigProvider`'s
interface and both implementations together — don't special-case one
deployment path with a dict lookup that bypasses the abstraction.

## Hardware constraints (edge = J1900-class CPU)

There is **one video-buffer backend**: `video_engine/remux_video_buffer.py`
(PyAV stream-copy — demux to encoded packets, RAM-bounded time-windowed packet
pre-roll, copy to disk using the source's own timestamps, no decode/encode).
It meets every constraint below.

The `full` CFR `cv2.VideoWriter` backend (`video_engine/video_buffer.py`) was
**retired 2026-08-01** (ROADMAP Item 6) — no deployment ever selected it, it
lost to `remux` on all three edge constraints, and its `DiskWriter._write_loop`
collected every raw frame of a clip into an in-memory list before writing (to
compute an exact FPS from total frames / total elapsed), making it
RAM-unbounded: tens of GB for a multi-minute 1080p clip. `_build_video_manager`
still *reads* `video_backend` purely to WARN that a stale value is being
ignored; it is no longer a switch, and there is nothing to switch to. **If a
central decoded/re-encode need ever appears, build it as a new RAM-bounded
branch** (`ClipRemuxer`'s lifecycle is deliberately separable from its `_mux`
write step for exactly this) — do not restore the CFR file from history.

Constraint status:

- **Zero-drift capture**: the stream-read loop has no `time.sleep()` — it
  iterates `container.demux()`, which blocks on I/O naturally. ✅
- **RAM pre-roll**: `collections.deque` of *encoded packets* bounded by a
  **time window** (`pre_roll_sec + keyframe_margin_sec`), independent of clip
  length. ✅
- **Concurrent-recording cap**: `threading.Semaphore(max_concurrent_writers)`,
  default 2. ✅
- **Disk check**: free space checked before a recording starts, aborts + logs
  below `min_free_disk_mb`. ✅
- **"Dump pre-roll, then route live frames directly to disk"**: ✅ `ClipRemuxer`
  muxes packets to disk incrementally (pre-roll then live), never accumulating
  the clip in RAM. Verified: RSS flat (~1 MB growth) across a genuine 240s clip
  in `__replay_verify.py`. (This was the constraint the CFR path violated.)

**Manager thread-safety in `remux` (2026-07-31, ROADMAP 8 — load-bearing).**
`VideoBufferManager`'s writer bookkeeping (`_active_writers`, `_stop_timers`,
`_draining`) is touched by the poll loop, by `threading.Timer` callbacks
(`_auto_stop`), and by the main thread's `stop()`, and is guarded by a single
`_state_lock`. The discipline is **under the lock, pop/collect what to act on;
release; then act** — never hold it across `finish()`, `join()`, a semaphore
acquire, `buf.subscribe`/`unsubscribe`, or any I/O (`_auto_stop` re-enters
`_stop_trigger` from a Timer thread, so a join under the lock deadlocks the reap
path). `_stop_timers` maps `trigger_id -> (generation, timer)`; the generation
lets a timer whose `cancel()` lost a race against `extend` detect that it has
been superseded and do nothing. Tests:
`python3 video_engine/tests/test_remux_manager.py` (22 stubbed-remuxer cases).

Clip length in `remux` is accurate **by construction** (= source PTS span = true
elapsed), so there is no FPS to guess and nothing drifts under RTSP jitter — the
defect the three CFR variants all shared. See [[DESIGN_HISTORY.md]] (2026-07-14
Item 1 entries) and
[VIDEO_BUFFER_REMUX_PLAN.md](video_engine/VIDEO_BUFFER_REMUX_PLAN.md).
**Real-stream Fable verification passed 2026-07-15** against the owner's
capture (`tests/fixtures/sample.ts`): exact length fidelity under real jitter,
RSS flat, and all plan-§4 adversarial probes green (B-frames, backward-jump
clamp, concurrent triggers, drop/reconnect). One documented behavior: mid-clip
**forward** PTS gaps are deliberately preserved (no frames arrived = real
elapsed time), while backward jumps are clamped — see the module docstring and
the 2026-07-15 DESIGN_HISTORY entry.

## NTCIP / SNMP rules

- All discrepancy timestamps come from the monitoring machine's own clock
  (`time.time()` / `datetime.now()`), never from camera or controller-reported
  time — sub-second comparisons depend on this.
- Event callbacks (`on_detector_on`/`on_detector_off` etc.) must return in
  microseconds — they only mutate a few scalar fields under a lock. Don't add
  I/O, file writes, or blocking calls inside a callback; do that work on the
  background evaluator thread instead.
- `EconoliteSNMPClient` sends `chunk_size` OIDs per PDU (constructor param,
  **default 1** — the verified-safe Cobalt/EOS setting that avoids "Too Big"
  errors). **Do not raise the default**; raise it per-deployment only via the
  intersection config's `snmp_chunk_size` key (standalone app:
  `controller.chunk_size`) after a green `__probe_snmp_batch.py` run on that
  controller. Monitor poll loops are batched into one `get(*oids)` call per
  sweep (order-preserving; wire behavior at chunk 1 is identical to the old
  per-OID loops), and `system_runner` polls only the detector groups the
  config's detectors occupy. `stats['reads']` counts poll cycles, not OIDs.
  Tests: `ntcip_monitor/tests/test_snmp_batching.py` (stubbed pysnmp).
- **Measured 2026-07-31, post-4a (load-bearing):** with `snmp_chunk_size: 8`
  the whole detector sweep is one PDU, and on intersection 201 the **effective
  sampling cycle is ~0.33 s** (~0.125 s sweep + the 0.2 s `poll_interval`
  sleep), catching **~94 % of true detector edges** (97 % of ON pulses).
  Baseline before the flip, for contrast: 8 sequential round trips, a
  1.0–1.5 s cycle, and only ~26 % of edges — which is why the pre-2026-07-31
  guidance treated every high-duty-channel trigger as unreliable. The
  per-channel *mapping* in `_intersections.json` is verified correct against
  controller high-res data (`__correlate_channels.py`, twice: 2026-07-19 and
  again post-flip) — never "fix" accuracy problems by remapping channels.
  **Neither number transfers to another controller**: 8 is set only for 201,
  and `poll_interval` still bounds the cycle from below. Trust
  `effective_cycle_sec()` (below) over either figure.
- **The monitor measures its own cycle** (2026-07-30, ROADMAP 9A):
  `BaseMonitor` folds each `_poll()`-plus-sleep into an EMA (α=0.1) exposed as
  `effective_cycle_sec()` and in a new `get_stats()`, and logs a rate-limited
  (5 min) structured INFO when it exceeds `2 × poll_interval`.
  **`effective_cycle_sec()` is the number to trust for sampling resolution;
  `poll_interval` is only a lower bound on it.** `0.0` means "no cycle
  completed yet" — callers must fall back to a configured default, never treat
  it as a fast sweep. Tests: `ntcip_monitor/tests/test_snmp_batching.py`
  (17 cases).
- Poll interval is configurable per-intersection; a warning is logged if it
  drops below 0.5s (`config_manager.py`) — note this warning understates
  reality given the sweep-time floor above.
- Econolite Cobalt specifics baked into the code: SNMP **v1** (not v2c), port
  **501** (not 161), community string = controller username, Phase 1 = bit 0.

## Web UI exposure (2026-07-31, ROADMAP 4f — load-bearing)

`ntcip_monitor/ui/web_ui.py` is an operator tool, not a service, and its
`/api/control/*` routes drive real signal hardware (time sync, vehicle calls,
output toggles). Two rules, both implemented:

- **Bind host defaults to `127.0.0.1`.** Override with `--web-host` (run.py) or
  `web_ui.host` in config — CLI beats config beats the default; `web_ui.port`
  resolves the same way. Don't restore a `0.0.0.0` default.
- **Control endpoints are gated by a shared secret** in the
  `X-NTCIP-Control-Token` header (`hmac.compare_digest`, compared as bytes),
  read from `$NTCIP_WEB_CONTROL_TOKEN` then `web_ui.control_token`. Policy:
  token set → header must match (401); no token + loopback bind → allowed;
  no token + non-loopback bind → **403, control disabled** plus a startup
  warning. The two rules interlock on purpose — exposing hardware control to
  the network takes both a host change and a secret. `/api/status` and
  `/api/stats` stay open (read-only, polled every 250 ms by the dashboard).

Both rules are implemented in one place: `_check_shared_secret()`, which
`_check_control_access()` and the overlay's `_check_video_access()` both call.

Deliberately not a session/user/JWT system — a reverse proxy owns real auth if
the deployment story changes. There's still no in-repo route test (a
Flask-test-client case is ROADMAP 4e), though `flask` and `pysnmp` were
installed here during 11b and the routes were verified from a scratch harness.

## Live video overlay (2026-07-31, ROADMAP 11a–11c — load-bearing)

`GET /overlay` draws pyatspm-calibrated detector loops and stopbars on a
`<canvas>` over a camera image, recolored from the live monitor state. Config
lives in `config.json`'s `overlay` section (`enabled`, `shapes_csv`,
`background`, `image_path`, `camera_url`, `stream_fps`, plus the optional
`stream_quality` and `rtsp_transport`); absent or `enabled: false` means every
overlay route answers 404. Deployment data for intersection 201 is in
`overlay/` at the repo root. The shipped config uses `background: "file"`
because `camera_url` is empty until ROADMAP 11d authors it.

- **`ntcip_monitor/ui/overlay/` imports nothing heavy.** `shapes.py` (vendored
  from pyatspm — see the module docstring for the four deliberate deviations),
  `status.py`, and `source.py` are stdlib-only apart from one guarded
  `import av` in `source.py` (`try/except ImportError`, touched only on the
  live path): no Flask, no cv2, no `atspm`, no `video_engine`, no monitor
  imports. That is what keeps 86 unit tests runnable on a bare interpreter
  (`python3 ntcip_monitor/tests/test_overlay_shapes.py`) — the live source's
  three PyAV seams (`_open_container` / `_decode` / `_encode_jpeg`) are
  overridable precisely so its threading is testable without a camera. Flask
  lives only in `web_ui.py`, including the MJPEG multipart framing.
- **The live source shares one decoder per camera** (`RtspMjpegSource`,
  `background: "live"`). Viewers are **ref-counted subscribers** — a stream
  generator for its lifetime, a `/api/overlay/background` request for one
  frame — and the decoder thread opens on the first and retires
  `idle_grace_sec` (10 s) after the last. N tabs cost the intersection one
  RTSP session, an idle page costs none. Bookkeeping follows the same lock
  discipline as the remux manager (decide/collect under the lock, act after
  releasing; never hold it across a connect, decode, encode, or socket write),
  and each decoder thread carries a `_DecoderSession` liveness token so a
  retiring thread can never stop its successor. Frames are decoded at the
  source rate but encoded only at `stream_fps` — encoding is the expensive
  half. JPEG quality comes from the encoder's `qmin`/`qmax`
  (`overlay.stream_quality`, 1 best–31 worst, default 12); FFmpeg's
  `-q:v`/`qscale` options are ignored by this encoder (verified, don't retry
  them).
- **Shape CSV colors are BGR** (OpenCV order, as pyatspm authors them):
  `"255,0,0"` is *blue*. `shapes.bgr_to_rgb()` reverses the triple exactly
  once, inside `shapes_payload()` on the way to `/api/overlay/shapes`; the
  loaded shapes keep the authored order. Don't reverse again in the page.
- **Two routes are open, two are gated.** `/api/overlay/shapes` (static,
  fetched once) and `/api/overlay/state` (polled at 250 ms) are open like
  `/api/status`. `/api/overlay/background` and `/api/overlay/stream` carry the
  **same interlock as `/api/control/*`** — a deliberate departure from 4f,
  because proxied camera video is a live view of a public roadway and
  `--web-host 0.0.0.0` shouldn't publish it by accident. The video routes also
  accept `?token=` (an `<img>` can't set a header); control is header-only.
- **The canvas does all the scaling.** `canvas.width/height` = the config's
  `video_width/video_height`; shapes are drawn in native calibration
  coordinates; canvas and background are stacked at `width:100%`. No
  coordinate math in the page — don't add any.
- **Every failure degrades to a 503 on one route**, never a crash: a missing
  CSV, an unreadable image, or an unreachable camera leaves the dashboard and
  the rest of the page working. `FileImageSource` re-reads on mtime/size
  change, so swapping the calibration still needs no restart; the live source
  reconnects with 1 s→30 s backoff and keeps re-sending the last good frame
  every 2 s so a viewer's `<img>` doesn't break mid-outage.
- The page **labels its own resolution** — SNMP sampling is ~1–1.5 s effective
  (see the NTCIP rules above), far coarser than the video. Keep that caveat if
  you touch the template.

### Deploy-time tooling and the calibration workflow (ROADMAP 11d)

`tools/` at the repo root holds **deploy-time** scripts that belong to neither
package — the same role `video_engine/system_runner.py` plays at runtime.
They may import `ntcip_monitor`; they are never imported by it, and nothing in
them relaxes the rule that the two packages don't import each other.

- **`tools/sync_ui_config.py`** is the de-duplication mechanism for values that
  live in both config files. `video_engine/intersections.json` is the
  authoring source; the script writes `controller.ip/port/community/chunk_size`
  and `overlay.camera_url` into `config.json`. **Dry run by default** (`--apply`
  to write), credentials masked in its output, atomic replace, idempotent.
  Poll intervals, timezone and `web_ui.*` are deliberately *not* synced — the
  monitor tunes four monitors separately, and bind host/port/token are
  properties of the host you run the UI on, not of the intersection.
- **`tools/grab_calibration_still.py`** saves one frame as a JPEG, resolving
  the URL from a `--intersection`/`--camera` pair or taking it directly. It
  grabs through the overlay's own `RtspMjpegSource`, so a successful grab is
  also proof the live overlay path can reach that camera.
- **Calibration workflow** (no ntcip code involved in step 2): grab a still →
  run pyatspm's `atspm video-calibrate-shapes --camera <name> --video <still>`
  against it (only the first frame is used; record a short clip with
  `video_engine/tools/__capture_rtsp.py` if OpenCV won't open the JPEG) → copy
  the CSV it writes to `overlay.shapes_csv`. 11a's reader accepts either format
  pyatspm produces. A browser-based calibrator would drop the pyatspm/Tkinter
  dependency entirely; it's parked in ROADMAP's Future section.

## Tests

Seven suites, all **stdlib `unittest`** (pytest is not installed here), one file
per subject, each runnable directly from any working directory via its own
`sys.path` bootstrap. 379 cases total as of 2026-08-03:

| Suite | Cases | Subject |
|---|---|---|
| `video_engine/tests/test_discrepancy_rules.py` | 168 | rule functions, `_evaluate_pair` integration, decision log, suppression log, sampling-floor + partner sub-floor-activity gates, detector groups + cross-pair duplicate rejection + AND-gated stop + per-rule dedup windows and the Rule 2 coverage guard, `_resolve_pytz` |
| `video_engine/tests/test_video_cleanup.py` | 44 | clip-name parsing, containment + tolerance, `plan_removals` invariants, log rewrite, scan/sweep (stubbed duration probe) |
| `video_engine/tests/test_remux_manager.py` | 22 | manager writer/timer bookkeeping (stubbed remuxer) |
| `video_engine/tests/test_config_manager.py` | 9 | `ConfigProviderError` |
| `ntcip_monitor/tests/test_overlay_shapes.py` | 86 | shape reader, status resolution, live source (stubbed PyAV) |
| `ntcip_monitor/tests/test_oid_helpers.py` | 33 | OID math + `parse_signal_state` |
| `ntcip_monitor/tests/test_snmp_batching.py` | 17 | chunking, batched poll loops, cycle EMA (stubbed pysnmp) |

Suites import the module under test as directly as possible — `test_oid_helpers`
puts `ntcip_monitor/core/` on `sys.path` and imports the leaf modules rather
than the package, because `core/__init__.py` re-exports `snmp_client` and would
drag in pysnmp. Keeping every suite runnable on a bare interpreter is
deliberate; preserve it when adding cases.

## Style conventions already in use

- **Logging**: structured JSON-lines via a shared `_JsonFormatter` pattern
  (see `remux_video_buffer.py`, `system_runner.py`). Use `logging`, not `print()`,
  for anything in the monitor/discrepancy/buffer business logic. (`print()` is
  fine in the standalone manual tools under `video_engine/tools/` like
  `record_clip.py`, `drop_trigger.py`, `simulate_playback.py` — those are debug
  tools, not production modules.)
- **Docstrings**: Google-style throughout (Args/Returns/Raises). Match this in
  new code.
- **No unsolicited files**: don't generate README/requirements/deployment
  manifests unless explicitly asked. Don't rewrite existing classes unless
  asked to refactor/optimize — provide the requested module/change only.

## Known repo clutter

As of this writing:

- `video_engine/archive/` and the `* - Copy.py` backup files across both
  packages have been removed (none were imported anywhere). The superseded
  drafts that were never committed are preserved at commit `1f48bfa` if ever
  needed again; the rest are recoverable from their normal file history.
- **All three CFR video buffers are gone** (deleted 2026-08-01, ROADMAP #5 and
  #6): `_edge_video_buffer.py` and `_old_video_buffer.py` (interim RAM-bounded
  CFR attempts, superseded by the 2026-07-14 remux decision and imported by
  nothing), and `video_buffer.py` (the `full` central/server backend — see the
  hardware-constraints section). All three are recoverable from git history;
  they last exist at commit `0c2e11b`. `remux_video_buffer.py` is the only
  buffer. Don't restore any of them — a future decoded backend is a new
  RAM-bounded branch, not a revival.
- `ntcip_monitor/monitors/ring_monitor.py` — new, not yet committed to git.
- `tools/` (repo root) is **not** clutter and is distinct from
  `video_engine/tools/`: it holds the deploy-time scripts described above
  (`sync_ui_config.py`, `grab_calibration_still.py`), which belong to neither
  package. Package-specific debug tools still go under `video_engine/tools/`.
- `overlay/` (repo root) is **not** clutter: it's the overlay's per-deployment
  data for intersection 201 — `201_fisheye_shapes.csv` (a copy of the owner's
  `~/vid_cfg720.csv` calibration) and `201_fisheye.jpg` (a still extracted
  from `video_engine/tests/fixtures/sample.ts`). `config.json` points at both.
- `video_engine/701_intersection.json` — real in-progress config for a second
  intersection (701, US-95/Whitley Dr), distinct from intersection 201 in
  `video_engine/intersections.json`. See [ROADMAP.md](ROADMAP.md) #2.
- `video_engine/tools/` holds the standalone debug/manual scripts. Two clean
  CLIs cover manual recording: **`record_clip.py`** (one-shot clip, or `--serve`
  to keep the buffer running while you drop triggers; replaced `__record.py`) and
  **`drop_trigger.py`** (writes a Hot Folder trigger; replaced `__trigger.py`).
  **`cleanup_clips.py`** is the third clean CLI: the manual front end to the
  duplicate-clip sweep, dry-run until `--apply`.
  The rest are `__`-prefixed dev/verification tools: `__capture_rtsp.py`,
  `__replay_verify.py`, `__probe_adversarial.py`, `__accuracy_report.py`
  (engine-log vs ATSPM-export precision/recall report), `__capture_ntcip.py`
  (raw NTCIP detector-edge capture, all 64 channels, ATSPM 82/81 event codes —
  for channel-mapping audits against the pyatspm DB; reuses the production
  SNMP client/OID math **including one batched `get(*group_oids)` per sweep**,
  so its reported median/p95 sweep time represents the monitor's — pass
  `--chunk-size` or a `--config` carrying `snmp_chunk_size` to match
  production, and `--simulate` for offline smoke tests),
  `__decode_datz.py` (controller `.datZ`/`.zip` → `timestamp,event_code,
  parameter` CSV — the ground truth the next two tools eat; calls **pyatspm's
  own** decoder helpers by file path, and applies the datZ header's sub-minute
  offset, which an ad-hoc extraction once dropped: see the 2026-07-31
  DESIGN_HISTORY entry and note `banks_events_20260719_1730.csv` is 1 s early),
  `__make_gt_export.py` (those events → the ATSPM anomaly export
  `__accuracy_report.py` scores against, via pyatspm's own
  `analyze_discrepancies()`, with pairs and `lag_threshold_sec` read from the
  intersection config so they can't drift from the engine run — **run it under
  pyatspm's interpreter**, it needs pandas/numpy which this repo deliberately
  doesn't depend on),
  `__correlate_channels.py` (MCC waveform correlation of a capture against a
  controller high-res export — verifies the channel map; see the 2026-07-19
  and 2026-07-31 DESIGN_HISTORY entries), plus `simulate_playback.py`.
  `video_engine/tests/` holds the unit tests
  (`test_discrepancy_rules.py`, `test_remux_manager.py`,
  `test_config_manager.py`, `test_video_cleanup.py`; stdlib `unittest`) and
  `video_engine/tests/fixtures/` the captured test data
  (`sample.ts` + its `.packets.jsonl` profile). The five tools that import
  `video_engine/` modules (`record_clip`, `cleanup_clips`, `__replay_verify`,
  `__probe_adversarial`, `simulate_playback`) add a `sys.path` bootstrap
  (`.../tools/` → parent) so they run from any working directory; the others
  (`__capture_rtsp`, `drop_trigger`, `__accuracy_report`, `__decode_datz`,
  `__make_gt_export`) don't import them and are location-independent
  (`__accuracy_report` needs `pytz`; the two datZ-chain tools resolve the
  sibling pyatspm checkout themselves, overridable with `--pyatspm`).

See [ROADMAP.md](ROADMAP.md) for open architectural decisions and planned work.

## Environment

- `requirements.txt` covers both packages (pysnmp/flask/pyasn1/pycryptodomex
  for `ntcip_monitor`; opencv-python/pytz for `video_engine`). Installed on
  this machine as of 2026-07-31: `flask`, `pysnmp` 5.1.0 + `pyasn1` 0.6.0 (the
  pinned pair — pysnmp 7 drops `hlapi.getCmd`), `av`, `pytz`, `PIL`. **Not**
  installed: `cv2`, `numpy`, `atspm`, `pytest` — tests are stdlib `unittest`.
- `video_engine/tools/simulate_playback.py` expects a sibling project at
  `../pyatspm` (present on this machine at `/home/hansrkid/pyatspm`) for
  reading historical detector events out of a pyatspm SQLite DB. It's not a
  pip dependency — `simulate_playback.py` adds it to `sys.path` directly. Note
  that path is resolved from the **current working directory** (`os.getcwd()`),
  not the script's location, so run it from the repo root as before.
