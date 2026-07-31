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
`discrepancy_engine.py`/`video_buffer.py` into `ntcip_monitor`, or vice versa —
this decoupling is intentional so the two halves can be deployed, tested, and
swapped independently (e.g., a future non-NTCIP discrepancy source should be
able to drive the same video engine with zero video_engine changes).

### Hot Folder pattern (the bridge)

Implemented in `discrepancy_engine.py` (writer) and both video-buffer backends
as readers (`video_buffer.py` and `remux_video_buffer.py`, identical
`Path.glob("trigger_*.json")` oldest-first poll loop).

- Filename: `trigger_{iso8601}_{uuid4_short}.json`
- Writer: write full JSON to `*.tmp`, then atomic `os.rename()` to `*.json`.
  Never write the final filename directly — a reader could see a partial file.
- Reader: poll the directory (current interval ~2–5s region, see
  `video_buffer.py` poll loop), sorted oldest-first, never with a sleep inside
  the frame-capture loop itself.

Trigger file schema (enforced in `video_buffer.py`; the canonical, field-by-field
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

**Single-camera assumption (both backends, load-bearing).** `_handle_start`
resolves `cameras` against the configured streams and records only
`target_cams[0]` — one writer per trigger. A multi-camera trigger logs a WARNING
with `cameras_requested`/`cameras_recorded` and is otherwise honored for the
first camera. This is deliberate (no second camera exists to test against), not
an oversight; don't "fix" it by adding per-camera writers until one is deployed.
Note a pair whose two detectors name different `camera_id`s does produce a
two-camera trigger, so the warning is reachable in real config.

Don't add fields casually — both sides (writer in `discrepancy_engine.py`,
reader in `video_buffer.py`) need to agree, and `config_manager.py`'s docstring
is the canonical schema reference.

### Discrepancy rules (the "brain")

`video_engine/discrepancy_engine.py`'s module docstring is the authoritative
spec for the three rules (Extended Holdover, Orphan Pulse, Chatter Exception)
and the Rule 1 active-resolution state machine. Read it before modifying
trigger-firing logic — it's dense but precise, including the cooldown/active-
trigger-id interaction that prevents double-firing. Don't re-derive this from
first principles; the docstring already encodes the corner cases that were
worked out by hand.

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
`sampling_floor_sec` (default 1.6 = the measured NTCIP reality) and every 60 s
thereafter from `DetectorMonitor.effective_cycle_sec()` — do not "simplify"
this by importing `ntcip_monitor` into the engine. Rule 2 refuses orphan
pulses shorter than `min_pulse_floor_multiple × floor` (default 2.0×),
counting them in the per-pair `below_floor_suppressed`; **at the default floor
that gate is 3.2 s, above a typical 2.0 s `lag_threshold_sec`, so Rule 2 is
effectively disabled until the sweep gets faster** (intended — see the
2026-07-30 DESIGN_HISTORY entry). A rolling 120 s ON-duty fraction per pair
drives a rate-limited WARNING; `suppress_high_duty_pairs` (default false) can
disable Rules 1+2 for such pairs. Because the duty computation reads the same
`on_intervals` deque, its retention horizon is now
`max(3 × threshold + grace, 120 s)` — keep the two consistent if either
changes.

The rule functions are pinned by `video_engine/tests/test_discrepancy_rules.py`
(50 stdlib-`unittest` cases, incl. the stale-refire guard and the floor gate)
— run it after any engine change:
`python3 video_engine/tests/test_discrepancy_rules.py`.
Accuracy vs. an ATSPM ground-truth export is measured with
`video_engine/tools/__accuracy_report.py` (correspondence-based
precision/recall; models cooldown + poll aliasing), not by comparing raw
counts.

## Config abstraction

`video_engine/config_manager.py` already implements the provider pattern:
`ConfigProvider` (ABC, `get_intersection_config()` / `list_intersection_ids()`),
with `JsonFileConfigProvider` (edge) and `SqliteCentralConfigProvider` (central)
as concrete implementations. `system_runner.py` defaults to the JSON provider.
When adding intersection-level config needs, extend `ConfigProvider`'s
interface and both implementations together — don't special-case one
deployment path with a dict lookup that bypasses the abstraction.

## Hardware constraints (edge = J1900-class CPU)

There are now **two video-buffer backends**, selected by the intersection
config's `video_backend` key (thin import switch in
`SystemRunner._build_video_manager`; both expose the same `VideoBufferConfig` /
`VideoBufferManager` surface):

- **`remux` (default, edge)** — `video_engine/remux_video_buffer.py`. PyAV
  stream-copy: demux to encoded packets, RAM-bounded time-windowed packet
  pre-roll, copy to disk using the source's own timestamps (no decode/encode).
  Meets all the constraints below, including the previously-violated one.
- **`full` (central/server)** — `video_engine/video_buffer.py`. The legacy CFR
  `cv2.VideoWriter` path; RAM-unbounded (see below), viable only on ample-RAM
  hosts.

Constraint status:

- **Zero-drift capture**: the stream-read loop has no `time.sleep()`. ✅ (both;
  `remux` iterates `container.demux()` which blocks on I/O naturally).
- **RAM pre-roll**: `collections.deque`. ✅ `remux` holds *encoded packets*
  bounded by a **time window** (`pre_roll_sec + keyframe_margin_sec`),
  independent of clip length; `full` holds decoded frames sized to the window.
- **Concurrent-recording cap**: `threading.Semaphore(max_concurrent_writers)`,
  default 2. ✅ (both).
- **Disk check**: free space checked before a recording starts, aborts + logs
  below `min_free_disk_mb`. ✅ (both).
- **"Dump pre-roll, then route live frames directly to disk"**:
  - `remux`: ✅ **satisfied.** `ClipRemuxer` muxes packets to disk incrementally
    (pre-roll then live), never accumulating the clip in RAM. Verified: RSS flat
    (~1 MB growth) across a genuine 240s clip in `__replay_verify.py`.
  - `full`: ❌ **still violated.** `DiskWriter._write_loop` in `video_buffer.py`
    collects every raw frame of the *entire* clip into an in-memory list and only
    writes on stop (to compute an exact FPS from total frames / total elapsed
    time) — RAM-unbounded, a multi-minute 1080p clip is tens of GB. This is why
    `full` is central/server-only, never an edge default.

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
been superseded and do nothing. The `full` backend has no Timer-driven
bookkeeping and is unchanged. Tests:
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
- **Measured 2026-07-19 (load-bearing):** because of `CHUNK_SIZE = 1`, one
  detector sweep = 8 sequential SNMP round trips, so the **effective sampling
  cycle is ~1.0–1.5 s wall-clock** regardless of `poll_interval` (which is
  only the inter-sweep sleep). NTCIP therefore catches only ~7–42 % of true
  detector edges on fast-cycling channels; discrepancy rules on high-duty
  presence zones (intersection 201 phases 2/6/7) operate below this sampling
  floor and mass-produce false triggers. The per-channel *mapping* in
  `_intersections.json` was verified correct against controller high-res data
  (`__correlate_channels.py`) — do not "fix" accuracy problems by remapping
  channels; fix the sweep speed (ROADMAP 4a) or gate the rules.
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

Deliberately not a session/user/JWT system — a reverse proxy owns real auth if
the deployment story changes. Flask/Jinja2 aren't installed in this
environment, so there's no in-repo test yet; a Flask-test-client case is part
of ROADMAP 4e.

## Style conventions already in use

- **Logging**: structured JSON-lines via a shared `_JsonFormatter` pattern
  (see `video_buffer.py`, `system_runner.py`). Use `logging`, not `print()`,
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
- `video_engine/_edge_video_buffer.py` and `video_engine/_old_video_buffer.py`
  are **superseded** as of the 2026-07-14 remux decision (ROADMAP #1): both are
  interim RAM-bounded CFR attempts, and the remux backend
  (`remux_video_buffer.py`) replaces the whole CFR-for-edge approach, making the
  old "which one was verified" provenance question moot for production. They're
  still on disk (nothing imports them); with Item 1 now complete and verified
  (2026-07-15), retiring them is ROADMAP #5 — don't wire either into
  `system_runner.py`.
- `ntcip_monitor/monitors/ring_monitor.py` — new, not yet committed to git.
- `video_engine/701_intersection.json` — real in-progress config for a second
  intersection (701, US-95/Whitley Dr), distinct from intersection 201 in
  `video_engine/intersections.json`. See [ROADMAP.md](ROADMAP.md) #2.
- `video_engine/tools/` holds the standalone debug/manual scripts. Two clean
  CLIs cover manual recording: **`record_clip.py`** (one-shot clip, or `--serve`
  to keep the buffer running while you drop triggers; replaced `__record.py`) and
  **`drop_trigger.py`** (writes a Hot Folder trigger; replaced `__trigger.py`).
  The rest are `__`-prefixed dev/verification tools: `__capture_rtsp.py`,
  `__replay_verify.py`, `__probe_adversarial.py`, `__accuracy_report.py`
  (engine-log vs ATSPM-export precision/recall report), `__capture_ntcip.py`
  (raw NTCIP detector-edge capture, all 64 channels, ATSPM 82/81 event codes —
  for channel-mapping audits against the pyatspm DB; reuses the production
  SNMP client/OID math, `--simulate` for offline smoke tests),
  `__correlate_channels.py` (MCC waveform correlation of a capture against a
  controller high-res export — verifies the channel map; see the 2026-07-19
  DESIGN_HISTORY entries), plus `simulate_playback.py`. `video_engine/tests/` holds the unit tests
  (`test_discrepancy_rules.py` and `test_remux_manager.py`, stdlib `unittest` —
  the layout precedent for ROADMAP 4d) and `video_engine/tests/fixtures/` the
  captured test data
  (`sample.ts` + its `.packets.jsonl` profile). The four tools that import
  `video_engine/` modules (`record_clip`, `__replay_verify`,
  `__probe_adversarial`, `simulate_playback`) add a `sys.path` bootstrap
  (`.../tools/` → parent) so they run from any working directory; the others
  (`__capture_rtsp`, `drop_trigger`, `__accuracy_report`) don't import them
  and are location-independent (`__accuracy_report` needs `pytz`).

See [ROADMAP.md](ROADMAP.md) for open architectural decisions and planned work.

## Environment

- `requirements.txt` covers both packages (pysnmp/flask/pyasn1/pycryptodomex
  for `ntcip_monitor`; opencv-python/pytz for `video_engine`).
- `video_engine/tools/simulate_playback.py` expects a sibling project at
  `../pyatspm` (present on this machine at `/home/hansrkid/pyatspm`) for
  reading historical detector events out of a pyatspm SQLite DB. It's not a
  pip dependency — `simulate_playback.py` adds it to `sys.path` directly. Note
  that path is resolved from the **current working directory** (`os.getcwd()`),
  not the script's location, so run it from the repo root as before.
