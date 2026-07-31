# Scope: Sampling-floor awareness + post-4a accuracy re-baseline (ROADMAP #9)

**Status (2026-07-30):** items **A and B are implemented** as specified — see
the DESIGN_HISTORY entry of that date for what landed and the two consequences
worth knowing (Rule 2 is effectively off at the default 1.6 s floor; the
`on_intervals` retention horizon now covers the duty window). **Item C below
is the remaining work** and is owner-run.

**Target model:** Opus (the design below is decided; execution is one session)
**Prerequisite:** ROADMAP 4a's controller round trip (probe run, `snmp_chunk_size`
set or not, monitor restarted). Item B here is worth doing even before that.

## Background — measured facts this scope rests on (2026-07-19, Fable)

- The NTCIP detector sweep is RTT-bound: **median 1.53 s** at chunk 1 × 8
  groups. NTCIP saw only **7–42 %** of true detector edges vs the
  controller's high-res log. After 4a: 6 groups (~1.15 s) if the probe fails,
  ~0.15–0.3 s if `snmp_chunk_size: 8` is clean.
- The channel *mapping* in `_intersections.json` is **verified correct**
  (`__correlate_channels.py` vs datZ). Do not revisit it.
- Phases 2/6/7 detectors run at **80–94 % ON-duty** with frequent sub-second
  gaps; the discrepancy engine's false-trigger storms come from sampling
  aliasing on those channels, not from the rules being wrong.
- Engine-vs-ATSPM baseline (`__accuracy_report.py`, 2026-07-19 sample):
  precision 36.5 % (42.7 % projected after the stale-refire fix), adjusted
  recall 36.5 %.

The principle: **the engine must not evaluate evidence finer than its own
sampling resolution.** Today it happily fires Rule 2 on a "pulse" whose true
duration is below what the poll can resolve, and runs pairs whose duty cycle
makes NTCIP data structurally unreliable.

## Item A — runtime sweep-time self-measurement

`DetectorMonitor` should know its own effective cycle time:

- In `BaseMonitor`/`DetectorMonitor`, record the wall-clock duration of each
  `_poll()` plus the sleep, as an EMA (`alpha≈0.1`); expose
  `effective_cycle_sec()` and include it in `get_stats()`.
- Log (structured, INFO, once per ~5 min) when the EMA exceeds
  `2 × poll_interval` — the operator's signal that the sweep, not the config,
  sets the sampling rate.

## Item B — sampling-floor gating in the discrepancy engine

**Boundary constraint:** `discrepancy_engine` must not import
`ntcip_monitor`. The floor reaches the engine via the composition root:
`system_runner` (which already wires both) calls a new
`DiscrepancyMonitor.set_sampling_floor(sec: float)` — at startup from config
(`sampling_floor_sec`, default 1.6 = today's measured reality) and thereafter
on a slow cadence (~60 s) from `detector_monitor.effective_cycle_sec()`.
A float assignment is atomic under the GIL; same pattern as
`cooldown_active`.

Gating rules (keep the pure functions pure — pass the floor in as an arg):

1. **Rule 2:** an orphan candidate is trusted only if its pulse duration
   ≥ `k × floor` (config `min_pulse_floor_multiple`, default 2.0). Shorter →
   don't register the candidate (in `_maybe_register_orphan`), increment a
   per-pair `below_floor_suppressed` counter, and log at DEBUG. Note the
   asymmetry this heals: today a *seen* short pulse fires while the partner's
   equally short response pulse is *unseen* — the exact aliasing false
   positive. After a green chunk-8 probe the floor drops to ~0.2 s and the
   gate ≈ 0.4 s, which suppresses almost nothing real (GT pulse minimum in
   the sample was 0.1 s but poll-aliased ones are expected-miss anyway).
2. **Rule 1:** no change to the threshold path (5 s ≫ any floor). But in the
   resolution state machine, an *agreement* shorter than the floor is not
   reliable evidence of resolution — acceptable as-is because post-roll
   restarts on re-divergence; document, don't code.
3. **High-duty advisory (not suppression by default):** the engine already
   holds `on_intervals` per detector; compute a rolling ON-duty fraction per
   detector on the evaluator thread (reuse the pruned deque + `is_on`; window
   ≈ 120 s). When a pair's min duty > `high_duty_warn_fraction` (default
   0.8), log a structured WARNING (rate-limited, once per ~10 min per pair):
   "pair operates above the NTCIP sampling-reliability regime". Add config
   `suppress_high_duty_pairs` (default **false**) that fully disables Rules
   1+2 for such pairs — a deployment decision, off until the owner opts in.

**Tests** (extend `video_engine/tests/test_discrepancy_rules.py`): below-floor
pulse not registered; at/above-floor registered; floor update takes effect;
duty computation from intervals; high-duty warning path (assertLogs).

## Item C — post-4a re-baseline protocol (how to know it worked)

1. Controller machine: `__capture_ntcip.py` ~10 min + matching datZ; push.
2. Any session: `__correlate_channels.py` + the edge-ratio check.
   **Pass:** median sweep ≤ 0.3 s and edge capture ≥ 90 % (chunk 8) — or
   sweep ≈ 1.1 s / capture ≈ 30–50 % (probe failed, groups-only): then the
   4a fallback (per-group threading, designed in ROADMAP 4a) is the next
   step before this re-baseline can pass.
3. Run the engine live ≥ 2 h; export the matching ATSPM window from pyatspm;
   run `__accuracy_report.py`.
   **Pass:** rule-2 precision ≥ 80 % with zero stale-refire phantoms and no
   pair showing the "zero correspondence" signature; adjusted recall ≥ 70 %
   on pulses ≥ 2× the new floor. Below that, categorize the residual
   true-misses/FPs before touching any rule code — the report's lists say
   whether it's floor, cooldown, or a genuine rule gap.

## Out of scope
- Cooldown/threshold retuning (product decision; revisit only after C).
- Hot Folder / trigger schema changes. Rule 1 state machine changes.
- The channel mapping (verified; leave it alone).
