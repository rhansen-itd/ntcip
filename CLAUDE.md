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

## Where the detail lives: `docs/`

The detailed, load-bearing reference for every other subsystem was moved
**verbatim** out of this file into `docs/` on 2026-09-30 (it cost ~15k tokens
per session). Each section below is only a digest of the rules. **Read the
linked doc before changing that subsystem.** It holds the rationale, the
measurements, and corner cases the digest leaves out. When a convention
changes, update both the doc and its digest here.

| Doc | Read it when |
|---|---|
| [docs/engine_rules.md](docs/engine_rules.md) | changing trigger firing, groups/dedup, or the Rule 2 gates |
| [docs/engine_logs.md](docs/engine_logs.md) | touching a log writer, or reading a log to measure anything |
| [docs/accuracy_measurement.md](docs/accuracy_measurement.md) | scoring a run against ATSPM ground truth or comparing runs (includes the last measured figures) |
| [docs/video_cleanup.md](docs/video_cleanup.md) | changing `video_cleanup.py` or anything that deletes clips |
| [docs/config_provider.md](docs/config_provider.md) | changing `config_manager.py` or the `intersections/` layout |
| [docs/video_buffer.md](docs/video_buffer.md) | changing `remux_video_buffer.py` |
| [docs/ntcip_snmp.md](docs/ntcip_snmp.md) | changing the SNMP client, poll loops, or chunk sizes |
| [docs/web_ui.md](docs/web_ui.md) | changing `web_ui.py` routes, auth, or `ui/events.py` (SSE) |
| [docs/overlay.md](docs/overlay.md) | changing `ui/overlay/`, repo-root `tools/`, or calibration |
| [docs/repo_layout.md](docs/repo_layout.md) | looking for a tool, or wondering whether a file is clutter |

### Discrepancy rules (digest of engine_rules.md)

- `discrepancy_engine.py`'s module docstring is the authoritative spec for
  Rules 1–3 and the Rule 1 resolution state machine. Don't re-derive it.
- `paired_detector_id` is a scalar or a list. Groups are the connected
  components of the pair graph. A group is a **dedup scope only**; pair
  generation stays link-driven. The pair schema lives in three places that must
  agree: `_build_structures`, `config_manager.py`'s docstring, and
  `__make_gt_export.py:_load_pairs`.
- Cross-pair dedup: a `start` within one dedup window of the group's last
  **emitted** start, for the same cameras, is marked and not written to the
  Hot Folder. A suppressed start never anchors the window. A `stop` is never
  suppressed. A suppressed Rule 1 start must **not** set `active_trigger_id`;
  it engages the pair cooldown instead.
- The stop is an **AND**: a folded duplicate registers on the owner's
  `held_pair_keys`, and the owner resolves only when it and every held pair
  agree. A held pair runs no rules (guard 0, ahead of the cooldown guard) and
  is released into a fresh cooldown. A Rule 1 start is never folded into a
  Rule 2 recording, and a Rule 2 duplicate never holds anything open.
- Windows are per rule: `dedup_window_rule1_sec` (10.0) and
  `dedup_window_sec` (3.0, Rule 2). The Rule 2 fold must also pass
  `_owner_covers_event`, which compares in event coordinates. `0` disables
  its own path only.
- Rule 2 overlap runs against the `_DetectorState.on_intervals` deque, never
  a most-recent-edge scalar. A verdict older than
  `_ORPHAN_DECISION_GRACE_SEC` is discarded, never fired late.
- The sampling floor is **injected** (`set_sampling_floor()` from
  `system_runner`, from `effective_cycle_sec()`). Never import
  `ntcip_monitor` into the engine. Rule 2 refuses pulses shorter than
  `min_pulse_floor_multiple × floor`. The duty-fraction horizon and the
  `on_intervals` retention must stay consistent.
- The partner sub-floor-activity gate (`partner_blip_max` 5 in
  `partner_blip_window_sec` 300) sits **strictly after** the floor gate and
  counts distinct pulses. `below_floor_pulses` is the one `_DetectorState`
  field without the lock (evaluator thread only). Rule 1 hysteresis was
  rejected and deliberately has no config key.
- Run `python3 video_engine/tests/test_discrepancy_rules.py` after any engine
  change.

### Logs and accuracy (digest of engine_logs.md, accuracy_measurement.md)

- `engine_decisions.csv`: one row per emitted trigger. **Score accuracy
  against this file.** Cross-pair duplicates are marked here
  (`suppressed_as_duplicate`), not dropped. `discrepancies_log.csv`: one row
  per clip actually recorded, so recall read from it is a floor. It is the
  one log the cleanup sweep rewrites. `engine_suppressions.csv`: candidates
  the engine declined, tagged by a `reason` string. New populations become
  new reason values, not new files.
- Log paths are injected by `system_runner` (`None` disables). Writes are
  best-effort and never stop a recording. `_DECISION_LOG_FIELDS`,
  `_SUPPRESSION_LOG_FIELDS` and `_CLEANUP_LOG_FIELDS` are **append-only**.
  The event window is not part of the trigger payload.
- Score with `video_engine/tools/__accuracy_report.py` against a
  `__decode_datz.py` → `__make_gt_export.py` export (run the latter under
  pyatspm's interpreter), using the **same** intersection config the run
  used. The run's `pair_key` values tell you which config that was.
- **Measure controller clock skew per run** by cross-correlation (not
  nearest-neighbour) and pass `--clock-offset`. The tell for an uncorrected
  skew: every candidate FP shows nearly the same `nearest GT Δ`. The
  monitoring machine runs PDT while the site is MDT.
- The matcher matches on start alignment, then containment. Precision figures
  from before 2026-08-03 are floors and aren't comparable across run lengths.

### Duplicate-clip cleanup (digest of video_cleanup.md)

`video_engine/video_cleanup.py` deletes a clip only when another clip from the
same camera covers its whole span, and repoints the log references to the
survivor. The span comes from mtime plus the PyAV duration and is
cross-checked against the filename epoch; a mismatch over 5 s is skipped,
never deleted. `plan_removals` is conservative and never deletes a keeper.
**Logs are rewritten first, the file is deleted second**, and if a rewrite
raises, nothing is deleted that sweep. In-flight clips are protected twice
(`_protected_clip_paths` and `cleanup_min_age_sec`). Every deletion is audited
in `video_cleanup_log.csv`. The manual CLI `cleanup_clips.py` is a dry run
until `--apply`. The module imports neither package, and PyAV is lazy.

### Config (digest of config_provider.md)

- `ConfigProvider` (ABC) has `JsonFileConfigProvider` (edge) and
  `SqliteCentralConfigProvider` (central). Extend the interface and **both**
  implementations together, never a bypassing dict lookup. `source_path()`
  is the one deliberate exception (file-backed only).
- The JSON provider takes a file or a directory. The convention is **one file
  per intersection** in `video_engine/intersections/`. An ID defined in two
  files raises. Nothing is published until every file validates. The scan is
  not recursive. An empty directory raises.

### Video buffer (digest of video_buffer.md)

- `remux_video_buffer.py` (PyAV stream copy) is the **only** backend. Don't
  restore the retired CFR buffers. A future decoded need is a new RAM-bounded
  branch.
- Constraints: no `time.sleep()` in the read loop. The pre-roll deque is
  bounded by time. Writers are capped by a semaphore (default 2). Free disk is
  checked before a recording starts. Clips are muxed to disk incrementally.
- Manager state is guarded by `_state_lock`: **pop/collect under the lock,
  release, then act**. Never hold it across `finish()`, `join()`, a semaphore
  acquire, subscribe/unsubscribe, or I/O. Timers carry a generation.
- Forward PTS gaps are preserved; backward jumps are clamped.

### NTCIP / SNMP (digest of ntcip_snmp.md)

- All timestamps come from the monitoring machine's clock, never the
  camera's or the controller's.
- Event callbacks return in microseconds: a few scalar writes under a lock,
  no I/O. Heavy work goes on the evaluator thread.
- `EconoliteSNMPClient` `chunk_size` **defaults to 1. Don't raise the
  default.** Raise it per deployment (`snmp_chunk_size`) only after a green
  `__probe_snmp_batch.py` run. Intersection 201 uses 8.
- `effective_cycle_sec()` is the sampling resolution to trust;
  `poll_interval` is only a lower bound. `0.0` means no cycle yet, so fall
  back to the configured default.
- Never "fix" accuracy by remapping channels (201's map is verified).
- Cobalt: SNMP **v1**, port **501**, community = controller username,
  Phase 1 = bit 0.

### Web UI (digest of web_ui.md, overlay.md)

- It binds to `127.0.0.1` by default. Control routes require
  `X-NTCIP-Control-Token` when a token is set. With no token they are allowed
  on loopback and get 403 on a non-loopback bind. Both checks live in
  `_check_shared_secret()`. The overlay's video routes carry the same
  interlock (and accept `?token=`).
- SSE (`/api/events`, `/api/overlay/events`): the dev server must stay
  `threaded=True`. Callbacks only enqueue. On overflow the queue is dropped
  and a snapshot sent. Subscriptions are never detached. Overlay status is
  resolved server-side. The 250 ms poll is kept as a fallback.
- `ui/overlay/` and `ui/events.py` stay stdlib-only (one guarded `import av`).
  One ref-counted decoder is shared per camera. Shape CSV colors are **BGR**,
  reversed exactly once in `shapes_payload()`. The canvas does all the
  scaling. Every failure degrades to a 503 on one route.
- Repo-root `tools/` holds deploy-time scripts. They may import
  `ntcip_monitor` but are never imported by it. `sync_ui_config.py` is a dry
  run until `--apply`.

## Tests

Eight suites, all **stdlib `unittest`** (pytest is not installed here), one file
per subject, each runnable directly from any working directory via its own
`sys.path` bootstrap. 446 cases total as of 2026-08-03:

| Suite | Cases | Subject |
|---|---|---|
| `video_engine/tests/test_discrepancy_rules.py` | 168 | rule functions, `_evaluate_pair` integration, decision log, suppression log, sampling-floor + partner sub-floor-activity gates, detector groups + cross-pair duplicate rejection + AND-gated stop + per-rule dedup windows and the Rule 2 coverage guard, `_resolve_pytz` |
| `video_engine/tests/test_video_cleanup.py` | 44 | clip-name parsing, containment + tolerance, `plan_removals` invariants, log rewrite, scan/sweep (stubbed duration probe) |
| `video_engine/tests/test_remux_manager.py` | 22 | manager writer/timer bookkeeping (stubbed remuxer) |
| `video_engine/tests/test_config_manager.py` | 29 | `ConfigProviderError`, `JsonFileConfigProvider` file-or-directory loading (merge, duplicate-ID refusal, non-recursion, atomic reload, `source_path`) + the shipped `intersections/` directory |
| `ntcip_monitor/tests/test_overlay_shapes.py` | 86 | shape reader, status resolution, live source (stubbed PyAV) |
| `ntcip_monitor/tests/test_oid_helpers.py` | 33 | OID math + `parse_signal_state` |
| `ntcip_monitor/tests/test_snmp_batching.py` | 17 | chunking, batched poll loops, cycle EMA (stubbed pysnmp) |
| `ntcip_monitor/tests/test_ui_events.py` | 47 | SSE delta coalescing, overflow-and-resync, close/wake, attach-once fan-out (stubbed monitors) |

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
