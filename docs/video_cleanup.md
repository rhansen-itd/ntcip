# Duplicate-clip cleanup

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 416-494). Read before changing video_cleanup.py or anything that deletes clips. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L416-494 -->

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
