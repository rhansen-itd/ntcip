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
`sampling_floor_sec` (default 1.6 = the *pre-4a* NTCIP reality) and every 60 s
thereafter from `DetectorMonitor.effective_cycle_sec()` — do not "simplify"
this by importing `ntcip_monitor` into the engine. Rule 2 refuses orphan
pulses shorter than `min_pulse_floor_multiple × floor` (default 2.0×),
counting them in the per-pair `below_floor_suppressed` and recording each one
in `engine_suppressions.csv` (below). **The runtime
measurement, not the 1.6 default, is what governs in production** — since 4a
landed, intersection 201 measures ~0.33 s, so the Rule 2 gate is ~0.65 s and
the rule is fully live (114 of 180 triggers in the 2026-07-31 run). Before 4a
the same default put the gate at 3.2 s, above a typical 2.0 s
`lag_threshold_sec`, which disabled Rule 2 in practice; if you read that
statement anywhere else, it is pre-2026-07-31. **Rule 2's precision at the new
floor is now validated: 96.3 % on the 2026-08-01 high-duty run (ROADMAP 9C2).**
The gate suppressed 710 distinct pulses over that run (998 rows, one per
affected pair), median duration 0.34 s — i.e. sub-cycle blips, not lost
signal. A rolling 120 s ON-duty fraction per pair
drives a rate-limited WARNING; `suppress_high_duty_pairs` (default false) can
disable Rules 1+2 for such pairs. Because the duty computation reads the same
`on_intervals` deque, its retention horizon is now
`max(3 × threshold + grace, 120 s)` — keep the two consistent if either
changes.

The rule functions are pinned by `video_engine/tests/test_discrepancy_rules.py`
(88 stdlib-`unittest` cases, incl. the stale-refire guard, the floor gate, the
decision log, the suppression log, and `_resolve_pytz`) — run it after any
engine change:
`python3 video_engine/tests/test_discrepancy_rules.py`.
Accuracy vs. an ATSPM ground-truth export is measured with
`video_engine/tools/__accuracy_report.py` (correspondence-based
precision/recall; models cooldown + poll aliasing), not by comparing raw
counts. Build the export with `__decode_datz.py` → `__make_gt_export.py`, and
pass the *same* intersection config the engine ran with — the three
intersection JSONs disagree on pairs, and scoring against the wrong set
invents misses. **Last measured 2026-08-01 (ROADMAP 9C2, the high-duty run):
overall precision 96.5 %, rule 2 precision 96.3 %, adjusted recall 86.2 %,
zero stale-refire phantoms, no zero-correspondence pair — all four §Item C
criteria pass.** Artifacts are committed as `engine_decisions_20260801.csv`,
`engine_suppressions_20260801.csv`, `discrepancies_log_20260801.csv`,
`banks_events_20260801_1300-1645.csv` and
`gt_anomalies_20260801_1300-1645.csv`. The superseded 2026-07-31 figures
(89.4 % / 59.9 %) were read off the *recording* log and were a floor.

**Controller clock skew is real and must be measured per run (2026-08-01 —
load-bearing).** The engine stamps events with the monitoring machine's clock;
the ground truth is stamped by the Econolite controller. Nothing keeps them in
sync, and on the 2026-08-01 run the controller ran **+4.49 s ahead** (vs ~0 s
on 2026-07-31). Uncorrected, that is larger than `--tolerance` (3.0 s) and
drags overall precision from 96.5 % to **11.6 %** — a collapse that looks like
a catastrophic engine regression and is not one. The tell: every candidate
false positive reports nearly the *same* `nearest GT Δ`, while the per-pair
table still shows healthy trigger and GT counts on the same pairs. Measure the
skew by comparing engine-observed detector edges (`engine_suppressions.csv`
and rule-2 rows of `engine_decisions.csv` carry exact Unix ON/OFF windows)
against the controller's 82/81 codes, then pass `--clock-offset` to
`__accuracy_report.py`. The result is insensitive to the exact value (3.5–5.5 s
all score identically, since the offset only has to land inside the tolerance)
— what matters is not leaving it at zero.

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
  inserted mid-list desynchronizes a resumed file from its header. The event
  window reaches `_fire_trigger` as one optional `event_window` tuple and is
  deliberately **not** added to the trigger payload (the video buffer has no
  use for it, and the Hot Folder schema is intentionally hard to grow).
- **`discrepancies_log.csv`** — written by the video-buffer backend, one row
  per clip actually *recorded*. `remux_video_buffer._handle_start` calls
  `_log_discrepancy_to_csv` only after `_writer_semaphore.acquire()` succeeds,
  so a trigger dropped by the `max_concurrent_writers` cap leaves no row.
  Measured on the 2026-07-31 run, the cap was saturated 11.6 % of wall clock
  yet accounted for 43 % of the apparent misses. **Recall read off this file
  is a floor, not an estimate.**
- **`engine_suppressions.csv`** — written by
  `discrepancy_engine._log_suppression`, one row per candidate the engine
  deliberately **declined** to act on, tagged with a `reason` column. Today
  the only reason is `below_sampling_floor` (the Rule 2 gate). Same injected
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
  the other populations `__accuracy_report.py` currently *models* (cooldown,
  grace expiry, high-duty, and ROADMAP 9C4's cross-pair duplicate) can land
  here later as new values, with no schema change and no fourth file.

`__accuracy_report.py` auto-detects which format it was handed (on the
presence of an `event_timestamp` column) and says so in its first line; the
legacy path is preserved so the committed 2026-07-31 artifacts still score
identically. Pass `--recording-log` alongside a decision log to get a DELIVERY
section counting decisions that never became clips.

## Config abstraction

`video_engine/config_manager.py` already implements the provider pattern:
`ConfigProvider` (ABC, `get_intersection_config()` / `list_intersection_ids()`),
with `JsonFileConfigProvider` (edge) and `SqliteCentralConfigProvider` (central)
as concrete implementations. `system_runner.py` defaults to the JSON provider.
When adding intersection-level config needs, extend `ConfigProvider`'s
interface and both implementations together — don't special-case one
deployment path with a dict lookup that bypasses the abstraction.

## Hardware constraints (edge = J1900-class CPU)

There is **one video-buffer backend**: `video_engine/remux_video_buffer.py`
(PyAV stream-copy — demux to encoded packets, RAM-bounded time-windowed packet
pre-roll, copy to disk using the source's own timestamps, no decode/encode).
It meets every constraint below.

The `full` CFR `cv2.VideoWriter` backend (`video_engine/video_buffer.py`) was
**retired 2026-08-01** (ROADMAP Item 6) — no deployment ever selected it, it
lost to `remux` on all three edge constraints, and its `DiskWriter._write_loop`
collected every raw frame of a clip into an in-memory list before writing (to
compute an exact FPS from total frames / total elapsed), making it
RAM-unbounded: tens of GB for a multi-minute 1080p clip. `_build_video_manager`
still *reads* `video_backend` purely to WARN that a stale value is being
ignored; it is no longer a switch, and there is nothing to switch to. **If a
central decoded/re-encode need ever appears, build it as a new RAM-bounded
branch** (`ClipRemuxer`'s lifecycle is deliberately separable from its `_mux`
write step for exactly this) — do not restore the CFR file from history.

Constraint status:

- **Zero-drift capture**: the stream-read loop has no `time.sleep()` — it
  iterates `container.demux()`, which blocks on I/O naturally. ✅
- **RAM pre-roll**: `collections.deque` of *encoded packets* bounded by a
  **time window** (`pre_roll_sec + keyframe_margin_sec`), independent of clip
  length. ✅
- **Concurrent-recording cap**: `threading.Semaphore(max_concurrent_writers)`,
  default 2. ✅
- **Disk check**: free space checked before a recording starts, aborts + logs
  below `min_free_disk_mb`. ✅
- **"Dump pre-roll, then route live frames directly to disk"**: ✅ `ClipRemuxer`
  muxes packets to disk incrementally (pre-roll then live), never accumulating
  the clip in RAM. Verified: RSS flat (~1 MB growth) across a genuine 240s clip
  in `__replay_verify.py`. (This was the constraint the CFR path violated.)

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
been superseded and do nothing. Tests:
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
- **Measured 2026-07-31, post-4a (load-bearing):** with `snmp_chunk_size: 8`
  the whole detector sweep is one PDU, and on intersection 201 the **effective
  sampling cycle is ~0.33 s** (~0.125 s sweep + the 0.2 s `poll_interval`
  sleep), catching **~94 % of true detector edges** (97 % of ON pulses).
  Baseline before the flip, for contrast: 8 sequential round trips, a
  1.0–1.5 s cycle, and only ~26 % of edges — which is why the pre-2026-07-31
  guidance treated every high-duty-channel trigger as unreliable. The
  per-channel *mapping* in `_intersections.json` is verified correct against
  controller high-res data (`__correlate_channels.py`, twice: 2026-07-19 and
  again post-flip) — never "fix" accuracy problems by remapping channels.
  **Neither number transfers to another controller**: 8 is set only for 201,
  and `poll_interval` still bounds the cycle from below. Trust
  `effective_cycle_sec()` (below) over either figure.
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

Both rules are implemented in one place: `_check_shared_secret()`, which
`_check_control_access()` and the overlay's `_check_video_access()` both call.

Deliberately not a session/user/JWT system — a reverse proxy owns real auth if
the deployment story changes. There's still no in-repo route test (a
Flask-test-client case is ROADMAP 4e), though `flask` and `pysnmp` were
installed here during 11b and the routes were verified from a scratch harness.

## Live video overlay (2026-07-31, ROADMAP 11a–11c — load-bearing)

`GET /overlay` draws pyatspm-calibrated detector loops and stopbars on a
`<canvas>` over a camera image, recolored from the live monitor state. Config
lives in `config.json`'s `overlay` section (`enabled`, `shapes_csv`,
`background`, `image_path`, `camera_url`, `stream_fps`, plus the optional
`stream_quality` and `rtsp_transport`); absent or `enabled: false` means every
overlay route answers 404. Deployment data for intersection 201 is in
`overlay/` at the repo root. The shipped config uses `background: "file"`
because `camera_url` is empty until ROADMAP 11d authors it.

- **`ntcip_monitor/ui/overlay/` imports nothing heavy.** `shapes.py` (vendored
  from pyatspm — see the module docstring for the four deliberate deviations),
  `status.py`, and `source.py` are stdlib-only apart from one guarded
  `import av` in `source.py` (`try/except ImportError`, touched only on the
  live path): no Flask, no cv2, no `atspm`, no `video_engine`, no monitor
  imports. That is what keeps 86 unit tests runnable on a bare interpreter
  (`python3 ntcip_monitor/tests/test_overlay_shapes.py`) — the live source's
  three PyAV seams (`_open_container` / `_decode` / `_encode_jpeg`) are
  overridable precisely so its threading is testable without a camera. Flask
  lives only in `web_ui.py`, including the MJPEG multipart framing.
- **The live source shares one decoder per camera** (`RtspMjpegSource`,
  `background: "live"`). Viewers are **ref-counted subscribers** — a stream
  generator for its lifetime, a `/api/overlay/background` request for one
  frame — and the decoder thread opens on the first and retires
  `idle_grace_sec` (10 s) after the last. N tabs cost the intersection one
  RTSP session, an idle page costs none. Bookkeeping follows the same lock
  discipline as the remux manager (decide/collect under the lock, act after
  releasing; never hold it across a connect, decode, encode, or socket write),
  and each decoder thread carries a `_DecoderSession` liveness token so a
  retiring thread can never stop its successor. Frames are decoded at the
  source rate but encoded only at `stream_fps` — encoding is the expensive
  half. JPEG quality comes from the encoder's `qmin`/`qmax`
  (`overlay.stream_quality`, 1 best–31 worst, default 12); FFmpeg's
  `-q:v`/`qscale` options are ignored by this encoder (verified, don't retry
  them).
- **Shape CSV colors are BGR** (OpenCV order, as pyatspm authors them):
  `"255,0,0"` is *blue*. `shapes.bgr_to_rgb()` reverses the triple exactly
  once, inside `shapes_payload()` on the way to `/api/overlay/shapes`; the
  loaded shapes keep the authored order. Don't reverse again in the page.
- **Two routes are open, two are gated.** `/api/overlay/shapes` (static,
  fetched once) and `/api/overlay/state` (polled at 250 ms) are open like
  `/api/status`. `/api/overlay/background` and `/api/overlay/stream` carry the
  **same interlock as `/api/control/*`** — a deliberate departure from 4f,
  because proxied camera video is a live view of a public roadway and
  `--web-host 0.0.0.0` shouldn't publish it by accident. The video routes also
  accept `?token=` (an `<img>` can't set a header); control is header-only.
- **The canvas does all the scaling.** `canvas.width/height` = the config's
  `video_width/video_height`; shapes are drawn in native calibration
  coordinates; canvas and background are stacked at `width:100%`. No
  coordinate math in the page — don't add any.
- **Every failure degrades to a 503 on one route**, never a crash: a missing
  CSV, an unreadable image, or an unreachable camera leaves the dashboard and
  the rest of the page working. `FileImageSource` re-reads on mtime/size
  change, so swapping the calibration still needs no restart; the live source
  reconnects with 1 s→30 s backoff and keeps re-sending the last good frame
  every 2 s so a viewer's `<img>` doesn't break mid-outage.
- The page **labels its own resolution** — SNMP sampling is ~1–1.5 s effective
  (see the NTCIP rules above), far coarser than the video. Keep that caveat if
  you touch the template.

### Deploy-time tooling and the calibration workflow (ROADMAP 11d)

`tools/` at the repo root holds **deploy-time** scripts that belong to neither
package — the same role `video_engine/system_runner.py` plays at runtime.
They may import `ntcip_monitor`; they are never imported by it, and nothing in
them relaxes the rule that the two packages don't import each other.

- **`tools/sync_ui_config.py`** is the de-duplication mechanism for values that
  live in both config files. `video_engine/intersections.json` is the
  authoring source; the script writes `controller.ip/port/community/chunk_size`
  and `overlay.camera_url` into `config.json`. **Dry run by default** (`--apply`
  to write), credentials masked in its output, atomic replace, idempotent.
  Poll intervals, timezone and `web_ui.*` are deliberately *not* synced — the
  monitor tunes four monitors separately, and bind host/port/token are
  properties of the host you run the UI on, not of the intersection.
- **`tools/grab_calibration_still.py`** saves one frame as a JPEG, resolving
  the URL from a `--intersection`/`--camera` pair or taking it directly. It
  grabs through the overlay's own `RtspMjpegSource`, so a successful grab is
  also proof the live overlay path can reach that camera.
- **Calibration workflow** (no ntcip code involved in step 2): grab a still →
  run pyatspm's `atspm video-calibrate-shapes --camera <name> --video <still>`
  against it (only the first frame is used; record a short clip with
  `video_engine/tools/__capture_rtsp.py` if OpenCV won't open the JPEG) → copy
  the CSV it writes to `overlay.shapes_csv`. 11a's reader accepts either format
  pyatspm produces. A browser-based calibrator would drop the pyatspm/Tkinter
  dependency entirely; it's parked in ROADMAP's Future section.

## Tests

Six suites, all **stdlib `unittest`** (pytest is not installed here), one file
per subject, each runnable directly from any working directory via its own
`sys.path` bootstrap. 255 cases total as of 2026-08-01:

| Suite | Cases | Subject |
|---|---|---|
| `video_engine/tests/test_discrepancy_rules.py` | 88 | rule functions, `_evaluate_pair` integration, decision log, suppression log, `_resolve_pytz` |
| `video_engine/tests/test_remux_manager.py` | 22 | manager writer/timer bookkeeping (stubbed remuxer) |
| `video_engine/tests/test_config_manager.py` | 9 | `ConfigProviderError` |
| `ntcip_monitor/tests/test_overlay_shapes.py` | 86 | shape reader, status resolution, live source (stubbed PyAV) |
| `ntcip_monitor/tests/test_oid_helpers.py` | 33 | OID math + `parse_signal_state` |
| `ntcip_monitor/tests/test_snmp_batching.py` | 17 | chunking, batched poll loops, cycle EMA (stubbed pysnmp) |

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
  `test_config_manager.py`; stdlib `unittest`) and
  `video_engine/tests/fixtures/` the captured test data
  (`sample.ts` + its `.packets.jsonl` profile). The four tools that import
  `video_engine/` modules (`record_clip`, `__replay_verify`,
  `__probe_adversarial`, `simulate_playback`) add a `sys.path` bootstrap
  (`.../tools/` → parent) so they run from any working directory; the others
  (`__capture_rtsp`, `drop_trigger`, `__accuracy_report`, `__decode_datz`,
  `__make_gt_export`) don't import them and are location-independent
  (`__accuracy_report` needs `pytz`; the two datZ-chain tools resolve the
  sibling pyatspm checkout themselves, overridable with `--pyatspm`).

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
