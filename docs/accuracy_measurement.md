# Measuring engine accuracy

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 253-346). Read before scoring a run against ATSPM ground truth or comparing runs. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L253-346 -->

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
pass the *same* intersection config the engine ran with — scoring against
the wrong pair set invents misses — and the cheap way to tell which config a
run used is the set of `pair_key` values in its decision log. Since ROADMAP 2
(2026-08-03) there is **one** 201 config, `video_engine/intersections/201.json`
(17 pairs), which is the file the committed 08-01 and 08-02 runs ran on,
byte-identical to the `_intersections.json` they name; the 5-pair
`video_engine/intersections.json` that used to sit beside it is retired.
`--config` on `__make_gt_export.py` takes the directory or a single file.

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
