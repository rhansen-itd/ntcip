# Scope: Discrepancy-engine accuracy — Rule 2 partner-history + validation harness

> **STATUS: IMPLEMENTED 2026-07-19** (ROADMAP Item 7). See the 2026-07-19
> entry in DESIGN_HISTORY.md for what landed and the measured numbers.
> Kept as the detailed scope record.

**Target model:** Fable (one item end-to-end, per ROADMAP conventions)
**Context:** `video_engine/discrepancy_engine.py` is the authoritative "brain."
Read its module docstring first; it encodes the three rules and the Rule 1
state machine by hand. Do **not** re-derive those from scratch.

## Background — how this scope arose

Owner compared the engine's live output (`discrepancies_log.csv`, NTCIP-polled
at 0.2 s with poll lag) against an ATSPM ground truth derived from internal
controller data at 0.1 s (`Discrepancies_20260719_140000_20260719_160000.csv`,
produced by the sibling `../pyatspm` project). They looked "way off."

A review established three causes:

1. **Rule 2 stale-refire (FIXED, 2026-07-19).** Orphan pulses re-fired once per
   ~60 s cooldown on unchanged detector state (15 of 76 orphan rows in the
   sample were phantom duplicates at 60/120 s spacing). Fixed by adding
   `last_handled_pulse_on_a/b` to `_PairRuntimeState` and a monotonic guard in
   `_maybe_register_orphan`. **This scope does not need to revisit it.**

2. **Rule 2 partner-history is a single scalar (THIS SCOPE, item A).**

3. **Methodology mismatch (THIS SCOPE, item B — build a harness, don't "fix").**
   Cooldown + poll coarseness make raw counts diverge by design. Needs a
   correspondence-based validation tool, not a code change to the rules.

## Item A — Rule 2 partner-overlap correctness

`_check_rule2_orphan` answers "was the partner detector OFF for the entire
window `[pulse_on − threshold, pulse_off + threshold]`?" using only the
partner's `other_last_on` scalar (its single most-recent rising edge). A scalar
cannot represent an interval, producing:

- **False negatives:** when the partner actuates *after* `window_end`
  (`other_last_on > window_end`), neither branch matches and the function
  returns `False`, silently dropping a legitimate orphan. Reproduce against the
  sample: any orphan candidate immediately followed by a partner actuation.
- **Missed mid-window overlaps:** a partner ON that fell inside the window but
  was later overwritten by a newer ON is invisible, so Rule 3 (chatter)
  protection leaks.

**Work:**
- Replace the scalar partner history with a bounded, time-windowed record of
  the partner's recent ON *intervals* (a `collections.deque` of
  `(on_ts, off_ts)`, pruned to the max window the engine ever inspects
  ≈ `2*threshold + margin`). Populate it from the detector callbacks; keep the
  microsecond callback contract (only append under the existing per-detector
  lock; no I/O). Prune on the evaluator thread.
- Rewrite the overlap test as true interval intersection: fire only if **no**
  partner interval intersects `[window_start, window_end]` **and** the partner
  is not currently ON.
- Keep `_check_rule2_orphan` a pure function (it is unit-testable by design) —
  pass the interval list in rather than reading state inside it.
- Preserve the thread-safety contract documented at the top of the module and
  the Hot Folder / trigger schema (no new trigger fields).

**Tests:** there is currently **no** unit-test file for the pure functions
(`_check_rule1_continuous`, `_check_rule2_orphan`, `_maybe_register_orphan`).
Add one (`video_engine/tests/test_discrepancy_rules.py`) covering: partner-ON
after window (must fire), partner-ON inside window (must suppress), chatter
straddling the window, the stale-refire guard from fix #1, and the Rule 1
threshold boundary. This is the regression net the inline fix lacked.

## Item B — accuracy validation harness (the real answer to "are they off?")

Counts will never match because of the 60 s per-pair cooldown and 0.2 s poll.
Build a comparison tool (belongs under `video_engine/tools/`, `__`-prefixed dev
tool, `print()` allowed) that measures **correspondence**, not counts:

- Parse both CSVs; canonicalize pairs as sorted tuples; restrict to the windows
  the engine actually covered (split engine rows on gaps > 600 s — the sample
  has two blocks: 13:59–14:19 and 15:50–16:00).
- **Precision:** for each engine trigger, is there a ground-truth event of the
  corresponding type (`rule1↔extended_disagreement`, `rule2↔isolated_pulse`) on
  the same pair within a tolerance (≈ poll_interval + lag, start ~2–3 s)? Report
  unmatched engine triggers (candidate false positives).
- **Recall:** for each ground-truth event that *should survive* a 0.2 s + lag
  resampling (i.e. long enough not to be poll-aliased — extended_disagreements,
  and isolated pulses ≳ 2× poll), is there a matching engine trigger, allowing
  for cooldown suppression (a GT event inside another pair's active cooldown
  window is expected-miss, not a defect)? Report true misses.
- Model cooldown explicitly so expected suppression is separated from real
  misses. Output precision/recall plus the categorized lists.

This turns "way off" into an actionable number and tells the owner which
residual gaps are engine defects vs. inherent to NTCIP sampling.

## Out of scope
- Changing cooldown/poll defaults (product decision, not correctness).
- Any change to the Hot Folder pattern, trigger schema, or Rule 1 state machine.
- The `full` vs `remux` video-buffer backends.
