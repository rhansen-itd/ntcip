# The three output logs

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 348-414). Read before touching a log writer or reading a log to measure anything. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L348-414 -->

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
