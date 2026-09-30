# Discrepancy engine rules

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 99-251). Read before changing trigger firing, groups/dedup, or the Rule 2 gates. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L99-251 -->

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
