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

Trigger file schema (enforced in `video_buffer.py`, documented in
`config_manager.py`):

```json
{
  "trigger_id": "uuid4-hex-string",
  "action": "start",               // "start", "stop", or "extend"
  "event_timestamp": 1738923456.7, // Unix timestamp when discrepancy DETECTED
  "reason": "detector_lag",        // "detector_lag", "no_actuation", "phase_mismatch"
  "intersection_id": "1234_main",
  "cameras": ["cam1", "cam2"],      // specific IDs or ["all"]
  "pre_roll_sec": 10,
  "post_roll_sec": 20,
  "max_duration_sec": 300,
  "metadata": {"det1": "radar", "det2": "loop", "lag": 2.5}
}
```

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
is discarded, never fired late (the pre-roll footage is gone by then). The
rule functions are pinned by `video_engine/tests/test_discrepancy_rules.py`
(26 stdlib-`unittest` cases, incl. the stale-refire guard) — run it after any
engine change: `python3 video_engine/tests/test_discrepancy_rules.py`.
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
- `EconoliteSNMPClient` uses `CHUNK_SIZE = 1` (`ntcip_monitor/core/snmp_client.py`)
  to avoid "Too Big" SNMP errors on Econolite Cobalt/EOS dense tables. **Do not
  change this** without confirming against real hardware — it looks
  inefficient but is a deliberate workaround.
- Poll interval is configurable per-intersection; a warning is logged if it
  drops below 0.5s (`config_manager.py`).
- Econolite Cobalt specifics baked into the code: SNMP **v1** (not v2c), port
  **501** (not 161), community string = controller username, Phase 1 = bit 0.

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
  SNMP client/OID math, `--simulate` for offline smoke tests), plus
  `simulate_playback.py`. `video_engine/tests/` holds the unit tests
  (`test_discrepancy_rules.py`, stdlib `unittest` — the layout precedent for
  ROADMAP 4d) and `video_engine/tests/fixtures/` the captured test data
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
