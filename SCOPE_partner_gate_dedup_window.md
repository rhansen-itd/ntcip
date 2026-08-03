# Scope: Partner-blip gate (12A), hysteresis decision (12B), dedup window re-derivation (14)

**Status (2026-08-03):** analysis complete (Fable). **Item A implemented and
Item B checked off (2026-08-03, Opus)** — the partner gate's offline
counterfactual reproduced the expected kills exactly (08-01 6 FP + 5 TP,
08-02 15 FP + 10 TP), as did every alternative row of its parameter table.
**Item C (ROADMAP 14) is the open implementation work**; Item D is optional and
awaits owner sign-off.
This document re-derives ROADMAP Items 12 and 14 against the corrected
post-Item-13 baselines (08-02 overall 94.1 %, rule 1 95.3 %, rule 2 92.8 %;
08-01 overall 96.9 %) and prescribes exactly what to build. Items A and C are
independent implementation sessions; Item B is a decided-**no** with a
documentation-only check-off.

**Target model:** Opus — the thinking below is done; execution is mechanical.
**Prerequisite:** none open (Item 13 landed 2026-08-03). No new engine run is
required for any expected-effect figure; every number reproduces from the
committed 2026-08-01 / 2026-08-02 artifacts alone.

**Reproduce the baselines first** (both must come out exactly as shown before
trusting any counterfactual below):

```
python3 video_engine/tools/__accuracy_report.py engine_decisions_20260801.csv \
  gt_anomalies_20260801_1300-1645.csv --recording-log discrepancies_log_20260801.csv \
  --poll 0.33 --clock-offset 4.49        # → overall 502/518 = 96.9 %
python3 video_engine/tools/__accuracy_report.py engine_decisions_20260802.csv \
  gt_anomalies_20260802_0930-2229.csv --recording-log discrepancies_log_20260802.csv \
  --poll 0.33 --clock-offset 0.75        # → overall 1462/1553 = 94.1 %
```

## Background — re-derived facts this scope rests on (2026-08-03, Fable)

Method: reproduce the committed baselines with the patched
`__accuracy_report.py`, then cross-reference every FP and TP against the
controller's own 82/81 edges (`banks_events_*`, shifted onto the engine clock
by the per-run offsets above) and against the engine's own suppression,
decision, and cleanup logs. "Controller-truth" below means derived from those
edges, not from the engine's sampled view.

### Fact 1 — the rule-2 FP population decomposes into two disjoint mechanisms

Every rule-2 FP was classified by (a) whether the partner detector had any
controller-truth ON interval intersecting the orphan's ±5 s observation
window ("partner-active"), and (b) whether the orphan pulse measured inside
one sampling floor of the 5.0 s threshold, i.e. > 4.67 s ("boundary"). The
two populations do not overlap on either run:

| Run | rule-2 FPs | partner-active | boundary (>4.67 s) | other |
|---|---|---|---|---|
| 08-01 | 9 | **6** (all 26:33) | 2 | 1 |
| 08-02 | 52 | **28** (26:33: 14) | **21** (42:8: 12, 31:8: 9) | 3 |

**Mechanism 1 — sub-floor partner response** (the 12A target). The orphan
pulse is real in controller truth (9/9 and 52/52 checked), and the partner
*did* respond — with 0.1–0.4 s blips (median 0.1–0.2 s), below what a
~0.33 s sampling cycle can see. The engine's "partner completely OFF during
the window" evidence is structurally blind to them; ground truth (which sees
the blip) refuses the anomaly. Among rule-2 **TPs** the partner was active in
the window only 1 % of the time (2/232 and 4/668) — the separation is
essentially perfect, but the separating variable is *invisible to the engine
at event time* (an engine-visible-pulse gate kills 0–2 FPs; that check
already exists in the rule). Only a *statistical* partner signal can work.

**Mechanism 2 — threshold-boundary type flip** (not an engine defect). All 21
boundary FPs on 08-02 sit on the det-8 triangle (42:8, 31:8): the engine
measured the pulse at 4.3–4.96 s (< 5.0 → rule 2), the controller measured
≥ 5.0, so pyatspm classifies the same physical event as
`extended_disagreement` — 14 of the 21 land inside a same-pair GT
`extended_disagreement` row of ~5.1 s that itself scores as a **true miss**
(double penalty for one caught event; the footage was recorded). The 08-02
true-miss `extended_disagreement` population (n=37, median 5.2 s, 23 < 5.33 s)
is largely the mirror of these FPs. **Suppressing the boundary zone in the
engine is net-negative and must not be done**: 47 rule-2 TPs also live at
pulse > 4.67 s on 08-02 against the 21 FPs. This is a scoring/classification
artifact; the only action offered is the optional report-only diagnostic in
Item D.

**Is 26:33 still the dominant source?** By raw count, no — 42:8 (17) edges
26:33 (14) on 08-02. By mechanism, **yes**: 26:33 contributes 14 of the 28
mechanism-1 FPs (6 of 6 on 08-01), and 100 % of 26:33's rule-2 FPs are
mechanism 1. The 42:8/31:8 counts are inflated by mechanism 2, which no
engine gate should touch. Det 33 is also the #1 producer of below-floor
orphan candidates on both runs (304 distinct pulses on 08-01, 731 on 08-02,
~2.4× the runner-up) — flag to the owner that the physical detector likely
needs service; the gate below is the software mitigation, not the fix.

### Fact 2 — rule-1 FPs are chatter-aliasing, not marginal durations

Three measurements, same story on both runs:

- **Controller-truth continuous-XOR run at the event start**: 08-02 FP median
  **1.8 s** (31 of 39 < 5.0 s; 8 show no XOR at all at the probe point);
  08-01 median 4.5 s. The engine's "continuous ≥ 5 s disagreement" was
  stitched across true agreements its sampling never saw — sub-floor partner
  blips again, this time breaking Rule 1's continuity instead of Rule 2's
  silence. XOR fraction over the fire window: FP median 0.84 vs TP 0.98.
- **Engine-observed episode duration** (stop − post-roll − event start): FP
  median **7.2 s** (08-02) / 7.6 s (08-01). Only **4 of 34** FPs with stops on
  08-02 (3 of 7 on 08-01) observed less than 5.33 s total — i.e. a threshold
  bump to `threshold + one cycle` (5.33 s) would have prevented **~4–9 of 39**
  FPs (the 5 held starts without stops are unknowable); the rest re-fire
  anyway because the engine's disagreement image persists far past the bump.
- **The recall side**: matched GT `extended_disagreement` events with duration
  < 5.33 s: **53** on 08-02 (of 794), **20** on 08-01 (of 270);
  engine-observed TP episodes < 5.33 s: 22+ and 10. The old "9 of 15 ≤ 5.5 s"
  true-missed population reproduces unchanged on 08-01 post-Item-13 (n=15,
  median 5.4 s, 9 ≤ 5.5 s) — a bump grows exactly that population.

Cost 22–53 TPs to remove 4–9 FPs, on a rule already at 95.3 %: **hysteresis
is net-negative by 3–6×. Decision: do not implement** (Item B).

### Fact 3 — ROADMAP 14's deletion breakdown was mis-joined; corrected here

Mapping every `video_cleanup_log_20260802.csv` row through the **trigger-ID
prefix in the clip filename** (`{trigger8}_{camera}_{epoch}.ts` →
`trigger_id[:8]`, unique across all 1553 starts, 190/190 rows mapped) gives:

| population | recorded in ROADMAP 14 / CLAUDE.md | actual |
|---|---|---|
| different-group | 139 (73.2 %) | **152** |
| same-group / different-pair | 21 (11.1 %) | **38** |
| same-pair cooldown re-fire | 30 (15.8 %) | **0** |

The earlier split was joined through the *rewritten* recording log — after the
sweep repoints `Video_Filename` at survivors, every kept file appears in ≥ 2
rows and a filename join aliases deleted clips onto their survivors. Zero
same-pair is also what first principles predict: the 60 s cooldown spaces
same-pair clips further apart than a 24.4 s median clip can contain.
**The 9C4-reachable population is 38/190 (20 %), nearly double the recorded
figure** — Item C is worth more than the ROADMAP text suggests.

Structure of the 38 (each row carries both spans in the cleanup log):
inter-start gap median **1.62 s**, 23 ≤ 2 s, 29 ≤ 3 s, 33 ≤ 5 s, 35 ≤ 10 s,
and 3 outliers (37.9 s, 38.9 s, 242.9 s) beyond any sane window. Most gaps sit
at **1.0–2.3 s — just past the shipped 1.0 s window**: the sibling pair
observes the same physical event through a different technology and crosses
its threshold one to two seconds later. The 08-01 "curve flattens after ~1 s"
observation that sized the window counted *fire-time clustering*; clip
*containment* keeps accruing well past 1 s. Five of the 38 are a rule-1 clip
deleted against a rule-2 survivor — unpreventable by design (a rule-1 start is
never folded into a rule-2 recording) and correctly left to the sweep.

A dedup replay simulator (9C4 anchor semantics: last *emitted* start per
(group, cameras), rule-1-into-rule-2 refusal, `abs()` comparison) validates
exactly before being trusted: at W=1.0 it reproduces the 08-02 run's own 457
suppression marks 457/457, and 135/523 on the 08-01 log (the documented replay
figure). Window sweep on 08-02, with a coverage audit of every *extra*
suppression (rule-2 candidate's `[pulse_start − pre_roll, pulse_end +
post_roll]` vs the owner's guaranteed recording span; rule-1 extras are held
open by the 9C4 AND-stop and covered by construction):

| window | extra suppressions | contained deletions prevented (of 38) | rule-2 windows NOT covered |
|---|---|---|---|
| 1.0 (shipped) | 0 | 0 | 0 |
| 2.0 | 62 | 11 | 1 |
| 3.0 | 82 | 13 | 1 |
| 5.0 | 97 | 15 | 1 |
| 10.0 (= pre+post) | 168 | 17 | **32** |
| hybrid: rule1 10.0 / rule2 3.0 + coverage guard | 91 | **17** | **0** |

A blanket `pre_roll + post_roll` window (10 s here) is **unsafe for rule 2**
— 32 orphan pulse windows would be suppressed without their footage being
inside the owner's recording. The same width is **safe at any value for
rule-1→rule-1 folds** because the held-pair AND-stop keeps the owner
recording until the folded pair also resolves (the 9C4 argument, width-
independent). That asymmetry, not a single number, is the answer to Item 14's
question.

---

## Item A — 12A: partner sub-floor-activity gate for Rule 2 (**implement**)

**Principle** (extends the sampling-floor principle of SCOPE_sampling_floor):
the engine must not treat *partner silence* as evidence when the partner's
recent behavior shows it produces pulses below the engine's own resolution.
The floor gate bounds the orphan's side; this gate bounds the partner's.

### Design

The engine already *sees* the statistical signal it needs: every below-floor
orphan candidate it declines (`below_sampling_floor`) is a sub-floor blip on a
known detector. Keep a short history of those per detector; refuse a rule-2
orphan whose **partner** has produced too many of them recently.

- **State**: `_DetectorState` gains `below_floor_pulses` — a deque of
  `(on_ts, off_ts)` tuples. Append in `_maybe_register_orphan` at the moment
  the floor gate declines a candidate, **deduped per detector**: a detector in
  two pairs (triangles) has the same pulse evaluated twice, so append only if
  the window differs from the deque's last entry. All access is on the
  evaluator thread (registration and evaluation both happen there) — no new
  locking. Prune to the horizon below wherever `on_intervals` is pruned.
- **Gate**: in `_maybe_register_orphan`, **after** the existing floor gate
  (ordering is load-bearing: a below-floor pulse must be counted and declined
  as `below_sampling_floor`, never reach this gate), count the partner's
  `below_floor_pulses` entries with `off_ts > now − partner_blip_window_sec`.
  If the count ≥ `partner_blip_max`, decline the candidate: bump a new
  per-pair `partner_blip_suppressed` counter (alongside
  `below_floor_suppressed`, surfaced in the same stats), log at DEBUG, and
  write a suppression row.
- **Config** (intersection level, read where `min_pulse_floor_multiple` is
  read): `partner_blip_window_sec` default **300.0**, `partner_blip_max`
  default **5** (`0` disables the gate). Update `config_manager.py`'s
  docstring — the canonical schema reference.
- **Suppression log**: new `reason` value **`partner_below_floor_activity`**
  (exactly the extension path the `reason` column was designed for — no
  schema change needed for the reason itself). Append two columns at the
  **end** of `_SUPPRESSION_LOG_FIELDS`: `partner_blip_count` and
  `partner_blip_window_sec`, so the counterfactual at other thresholds is
  recoverable from a finished run (the C3 precedent: store the inputs, not
  just the verdict). Append-only discipline: end of the tuple, never
  mid-list; existing `pulse_*` columns keep describing the declined orphan
  candidate itself. For `below_sampling_floor` rows the two new columns stay
  blank.

Parameter choice is measured, not guessed. Counting **distinct pulses** (the
log's one-row-per-pair duplication removed) over candidate (N, horizon)
combinations against both runs' FP/TP populations:

| gate | 08-01 FP/TP killed | 08-02 FP/TP killed | 08-02 overall → | 08-02 rule 2 → |
|---|---|---|---|---|
| ≥3 in 300 s | 6 / 22 | 15 / 32 | 95.0 % | 94.5 % |
| **≥5 in 300 s** | **6 / 5** | **15 / 10** | **95.0 %** | **94.7 %** |
| ≥8 in 300 s | 4 / 0 | 12 / 3 | 94.9 % | 94.3 % |

600 s and 1800 s horizons are strictly worse (same FP kill, 2–5× the TP
kill). **N=5, horizon 300 s** is the default: it takes 08-01 to **98.0 %
overall / 98.7 % rule 2** and 08-02 to **95.0 % / 94.7 %**, at a TP cost of
2.2 % / 1.5 % of rule-2 TPs — and those TPs are precisely events whose
partner blips sub-floor nearby, i.e. the population whose partner-silence
evidence is least trustworthy. The kills concentrate where they should:
26:33 supplies 6/6 (08-01) and 14–18 of the kills (08-02).

### Explicitly not in this item

- No gating on controller-side data (unavailable at runtime) and no static
  "disable rule 2 on 26:33" config (kills recovery if the detector is fixed;
  the rolling gate adapts on its own).
- No engine action on mechanism 2 (boundary zone) — measured net-negative,
  see Fact 1. Item D is the only boundary-zone action offered.
- No change to the partner-overlap interval test, the floor gate, or
  `_ORPHAN_DECISION_GRACE_SEC`.

### Tests (extend `video_engine/tests/test_discrepancy_rules.py`)

1. Partner with 5 distinct below-floor pulses inside 300 s → orphan declined,
   suppression row has `reason=partner_below_floor_activity`,
   `partner_blip_count=5`, and the pair counter increments.
2. Partner with 4 → candidate registers normally.
3. Blips older than the horizon are ignored (and pruned).
4. Triangle dedupe: one physical below-floor pulse on a shared detector,
   evaluated via two pairs, appends **one** deque entry.
5. `partner_blip_max: 0` disables the gate entirely.
6. Ordering: a below-floor candidate on a blip-heavy pair is logged as
   `below_sampling_floor`, not `partner_below_floor_activity`.
7. The two new columns are blank on `below_sampling_floor` rows and populated
   on `partner_below_floor_activity` rows.
8. Config plumb-through from the intersection dict, and defaults when absent.

### Verification

Offline, before any new run: re-derive the FP/TP kill counts from the
committed artifacts (parse both runs through `__accuracy_report`'s own
`_parse_engine_csv`/`_parse_gt_csv`/`_match` with the offsets above; count
distinct below-floor pulses per detector from `engine_suppressions_*.csv`
deduped on `(orphan_det, event_start_ts)`; a rule-2 trigger is killed if its
partner's trailing-300 s count ≥ 5 at `event_timestamp`). Expected exactly:
08-01 kills 6 FP + 5 TP; 08-02 kills 15 FP + 10 TP. On the next owner run,
expect rule-2 precision to move toward the projected figures and
`partner_below_floor_activity` rows to appear at roughly 15–25/day pace —
dominated by pairs whose partner is det 33.

---

## Item B — 12B: Rule 1 hysteresis — **decided NO; do not implement**

The re-derivation (Fact 2) settles the question ROADMAP 12B left open, in the
direction it suspected. From GT durations and the engine's own stop rows: a
threshold bump to 5.33 s removes ~4–9 of 39 FPs on 08-02 (3 of 7 on 08-01)
while pushing 22–53 genuine events below the bar; the FP mechanism is
sub-floor chatter breaking true continuity (controller-truth XOR run median
1.8 s at the event start), which a one-cycle bump does not address because the
engine's stale disagreement image persists a median 7.2 s. Any
consecutive-sample or agreement-confirmation variant is the same trade under
a different name: it delays or discards short *true* events (median true-miss
duration is already 5.2–5.4 s) to filter FPs that mostly re-fire anyway.

**Action for Opus (documentation only, no code, no config):**

- Add one paragraph to `discrepancy_engine.py`'s module docstring, in the
  Rule 1 section, recording that a fire-threshold bump / hysteresis was
  evaluated against ground truth on 2026-08-03 and rejected, with the 4–9 FP
  vs 22–53 TP arithmetic and a pointer to this scope (mirrors the
  "document, don't code" precedent of SCOPE_sampling_floor Item B.2).
- Check 12B off in ROADMAP as decided-no and record the decision + rationale
  in DESIGN_HISTORY's Decisions log.
- The genuine fix for the FP mechanism, if it is ever wanted, is finer
  sampling or per-pair suppression of chatter-prone channels (the existing
  `suppress_high_duty_pairs` family) — a deployment decision, out of scope.

---

## Item C — 14: per-rule dedup windows + rule-2 coverage guard (**implement**)

**Decision:** neither a single larger fixed window nor a blanket
`pre_roll + post_roll` derivation. The safe width differs by rule because the
coverage guarantee differs by rule (Fact 3): a rule-1→rule-1 fold is
footage-safe at any width (AND-stop), a rule-2 suppression is only safe while
the owner's recording provably covers the pulse window.

### Design

Two windows plus a guard, all in `_duplicate_within_group` (and
`_apply_dedup_config` for the config side):

- **`dedup_window_rule1_sec`** (new key, default **10.0**, `0` disables the
  rule-1 path): a **rule-1 candidate** folds into a **rule-1 owner** emitted
  within this window. 10.0 ≈ `pre_roll + post_roll` here and sits just above
  the p90 preventable gap (9.0 s); the AND-stop makes any width safe, and the
  three 38 s+ outliers are deliberately left to the sweep (a 4-minute window
  would fold genuinely distinct events into one giant clip).
- **`dedup_window_sec`** (existing key, default raised **1.0 → 3.0**): a
  **rule-2 candidate** folds into an owner of either rule emitted within this
  window — 3.0 is the knee of the gap histogram (29 of 38 within; beyond it,
  extra suppressions grow with no additional deletions prevented at the
  hybrid setting). The rule-1-into-rule-2 refusal and the never-suppress-stop
  rule are unchanged, as are the anchor semantics (emitted starts only,
  cameras in the key, suppressed starts never anchor).
- **Coverage guard on the rule-2 path**: suppress only if the candidate's
  needed window `[event_start_ts − pre_roll, event_end_ts + post_roll]` is
  inside the owner's *guaranteed* span —
  - owner is **rule 2**: its clip span is fixed and known at fire time
    (`[owner_event_start − pre_roll, owner_pulse_end + post_roll +
    threshold]`); compare directly. `_GroupFire` must grow an optional
    `span_end` field (rule-2 owners only) to carry it.
  - owner is **rule 1**: if its recording is still active
    (`active_trigger_id` still set on the owner's pair), it is covered by
    construction — the pulse is already over, and the recording runs at least
    `post_roll` past any future resolution. If the owner has already stopped
    (possible only if the config sets `dedup_window_sec` above
    `lag_threshold_sec + post_roll`), refuse to suppress.
  At the defaults the guard never fires on the rule-1-owner branch (a rule-1
  owner cannot stop within 3 s of starting); it exists so raising the window
  in config cannot silently create footage loss. Keeping duplicate detection
  conservative-by-construction is the same posture as `plan_removals`.
- Rewrite the sizing comment on the window constant: the "curve flattens
  after ~1 s" observation counted fire-time clustering on 08-01; clip
  containment (Fact 3) is the number that matters, and it keeps accruing to
  ~3 s (rule 2) / ~10 s (rule 1).
- No decision-log schema change (the 9C4 columns already record suppressions);
  no Hot Folder change; `config_manager.py` docstring updated with both keys.

### Expected effect (validated by replay, not projected)

On the 08-02 log: **+91 suppressions** (543 total, 35.0 % of 1553 starts),
preventing **17 of the 38** contained same-group clips with **zero** rule-2
pulse windows left uncovered; ~91 fewer clips ≈ 37 min less writer occupancy
(median clip 24.4 s) against the 20.0 % delivery loss. On the 08-01 log: 170
suppressed (32.5 %) vs the shipped 135. The remaining 21 same-group deletions
(rule-1-into-rule-2 by design, gap outliers, anchor-chain cases) plus all 152
different-group deletions stay with the cleanup sweep — this item narrows the
sweep's diet, it does not replace it (re-checking the original 9C4 goal: more
suppression *of the same moment*, no broadening onto real events — the guard
is what enforces that).

### Tests (extend `test_discrepancy_rules.py`; the existing 9C4 cases pin the
unchanged semantics)

1. Rule-1 candidate at gap 8 s after rule-1 owner → suppressed + held
   (new window applies).
2. Rule-1 candidate at gap 8 s after **rule-2** owner → emitted (refusal
   unchanged, and `dedup_window_rule1_sec` does not override it).
3. Rule-2 candidate at gap 2.5 s, owner rule-2, pulse window inside the
   owner's fixed span → suppressed.
4. Same, pulse window ending past the owner's span → **emitted** (guard).
5. Rule-2 candidate at gap 2.5 s, rule-1 owner still active → suppressed;
   same with the owner already stopped (window raised in config) → emitted.
6. Defaults: `dedup_window_sec` absent → 3.0; `dedup_window_rule1_sec`
   absent → 10.0; each `0` disables its own path only.
7. Replay constants: the W=1.0 semantics still reproduce 135 suppressions on
   a synthetic sequence derived from the documented anchors (or assert the
   simulator invariants: suppressed starts never anchor, cameras in the key).
8. Cross-item: a suppressed rule-1 start still engages cooldown and never
   sets `active_trigger_id` (existing tests should already cover; extend to
   the wider window).

### Verification

Offline: the replay simulator (validated 457/457 on 08-02 marks and 135 on
08-01 before use) run at the new defaults must give exactly 543 total
suppressions on 08-02 and 170 on 08-01, with the coverage audit reporting 0
uncovered rule-2 windows. After the next run with both items live: the
sweep's same-group/different-pair deletion count (by trigger-prefix
classification, **not** a filename join) should drop by roughly half, and
`video_cleanup_log.csv` should show the residual dominated by different-group
containment.

---

## Item D — optional, needs owner sign-off: boundary-zone diagnostic in `__accuracy_report.py`

Report-only; no matching change (Item 13's type-scoping decision stands). Add
a PRECISION-section line counting rule-2 FPs whose pulse exceeds
`threshold − floor` (new args `--lag-threshold`, default 5.0, and `--floor`,
default 0.33) and, of those, how many sit inside a same-pair GT
`extended_disagreement` window — with the mirrored count of true-miss
`extended_disagreement` events ≤ `threshold + floor`. On 08-02 this labels 21
FPs and ~23 misses as one classification artifact instead of 44 defects; on a
legacy-format log the line is skipped (no exact windows). Verbose mode lists
the rows. The committed artifacts must still score identically (the line is
additive). Skip this item entirely if the owner prefers the report untouched.

---

## Out of scope

- Rule 1 threshold, hysteresis, or resolution state-machine changes (Item B).
- Hot Folder / trigger schema changes; writer-cap changes.
- Matcher/type-scoping changes beyond the report-only Item D.
- The channel mapping (verified; leave it alone).
- Hardware follow-up on det 33 (flagged to the owner in Fact 1; not software).
- Cooldown retuning; the cleanup sweep's algorithm (its diet changes, not it).

## Bookkeeping for the executing session(s)

- ROADMAP: move 12A and 14 into DESIGN_HISTORY when done; check 12B off as
  decided-no (Item B's entry covers it). Fix the 21/30/139 breakdown wherever
  it appears (ROADMAP 14 intro, CLAUDE.md cleanup section) to 38/0/152 with a
  one-line note on the join defect — that correction stands even if
  implementation slips.
- DESIGN_HISTORY: dated Decisions entries for whichever items land, plus the
  two corrections from this analysis (deletion-breakdown mis-join; "26:33
  dominant" holding by mechanism but not by raw count).
- CLAUDE.md: the discrepancy-rules section gains the partner gate (config
  keys, suppression reason, the counting-distinct-pulses subtlety) and the
  split dedup windows + coverage guard; the accuracy figures stay as they are
  until a new run is measured.
- Commit per ROADMAP item, per the standing convention.
