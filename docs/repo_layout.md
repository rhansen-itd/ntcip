# Repo layout and tool inventory

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 843-916). Read when looking for a tool, or wondering if a file is clutter. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L843-916 -->

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
- `video_engine/intersections/` is **not** clutter: it is the intersection
  config directory (`201.json`, `701.json` — US-95/Whitley Dr). It replaced
  three scattered files on 2026-08-03 (ROADMAP 2): the root
  `_intersections.json`, `video_engine/intersections.json`, and
  `video_engine/701_intersection.json`. All three are recoverable from git
  history at commit `ff8244a`. An untracked `intersections.json` may still sit
  at the repo root on this machine — that is a stale copy of the retired
  5-pair 201 config, not a config the code reads any more.
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
