# Video Buffer Remux Plan — ROADMAP Item 1

Design/decision document for the edge-viable video buffer. This is the plan
Opus implements against; the verification tasks it leaves for Fable are called
out explicitly in [§8](#8-division-of-labor--opus-vs-fable). Land the outcome
as a dated [[DESIGN_HISTORY.md]] entry when the implementation session
completes.

Status: **Complete.** Opus implementation + blind self-test landed 2026-07-14;
real-stream Fable verification (§8) passed 2026-07-15 against the owner's
capture (`video_engine/tests/fixtures/sample.ts`) — all standard checks and
adversarial probes green, no code defects; outcome logged in DESIGN_HISTORY.md.

---

## 1. Decision

Replace the current constant-frame-rate (CFR) `cv2.VideoWriter` path with a
**remux / stream-copy** path built on **PyAV** (FFmpeg bindings): demux the
RTSP stream into encoded packets, keep a **RAM-bounded** rolling pre-roll of
packets, and on a trigger copy packets straight to disk **using the camera's
own presentation timestamps**. No decode, no re-encode.

This is the "second-to-last option" from ROADMAP Item 1 (per-frame PTS muxing),
chosen over the three CFR variants (`video_buffer.py`, `_edge_video_buffer.py`,
`_old_video_buffer.py`), and it makes the old provenance question — which of
`_old_`/`_edge_` was really verified — **moot for production**: all three
assume a single FPS and therefore drift under RTSP jitter. Remux takes clip
timing from the source, so there is no FPS to guess and nothing to tune
against a live feed.

Why remux is the right call for this project specifically:

- **Accurate length by construction.** Clip duration equals the span of the
  source packets' timestamps — the true wall-clock elapsed time — regardless
  of jitter. This is the exact defect Item 1 exists to fix.
- **Edge-CPU-friendly.** Stream-copy does no decode and no encode. On a
  J1900-class box this is near-zero CPU — strictly better than any CFR
  re-encode path.
- **RAM-bounded.** The pre-roll holds *encoded* packets sized to a time
  window (bitrate × window), ~100× smaller than decoded frames and
  **independent of clip length** — directly resolving the CLAUDE.md
  "dump pre-roll, then route live frames straight to disk" violation.
- **Develops well blind.** Correctness comes from source timestamps, not
  tuned parameters, so it can be built and self-tested without a live camera
  (see [§7](#7-verification)).

**Scope guard — the video engine does not currently need decoded pixels.**
Discrepancy detection is NTCIP-driven, not vision-driven, so nothing consumes
decoded frames today. The owner has flagged a possible **future decoded
option** (e.g. a vision-based discrepancy source or burned-in overlays); this
plan therefore keeps a clean packet-vs-frame seam so that path is an *added
branch*, not a rewrite (see [§6](#6-future-decoded-path-leave-room-do-not-build)).

---

## 2. What stays unchanged (contracts we must not break)

- **Hot Folder trigger contract** — trigger-file schema, `start`/`stop`/
  `extend` actions, atomic `*.tmp`→`*.json`, oldest-first polling, no sleep in
  the capture loop. The new buffer is driven by the same triggers. See
  CLAUDE.md "Hot Folder pattern".
- **`VideoBufferManager` public surface** — `system_runner.py` does
  `from video_buffer import VideoBufferConfig, VideoBufferManager`, builds a
  `VideoBufferConfig`, constructs `VideoBufferManager(cfg)`, and calls
  `.start()` / `.stop()`. The remux backend must expose the *same* surface so
  the swap is a config choice, not a `system_runner` rewrite.
- **Concurrent-recording cap** — `threading.Semaphore(max_concurrent_writers)`
  gates simultaneous clips.
- **Disk-free check** — abort + log a clip start when free space is below
  `min_free_disk_mb`.
- **Wall-clock timestamps** — trigger windows are anchored on the monitoring
  machine's clock (`event_timestamp`), never camera-reported time. Remux uses
  wall-clock only for the coarse *window boundaries* and source PTS for
  *intra-clip* timing (see [§4](#4-timestamp-handling-the-core-of-the-design)).
- **Structured JSON-lines logging** via the shared `_JsonFormatter`.

---

## 3. Architecture

New module **`video_engine/remux_video_buffer.py`**, mirroring `video_buffer.py`'s
public names (`VideoBufferConfig`, `VideoBufferManager`) so `system_runner.py`
selects it by config. Internals map onto the three existing roles:

| Existing (`video_buffer.py`) | New (`remux_video_buffer.py`) | Change |
|---|---|---|
| `StreamBuffer` — cv2 read → **decoded frame** deque | **`PacketStreamBuffer`** — PyAV demux → **encoded packet** deque | store packets, not frames; track keyframes; stamp receive wall-time |
| `DiskWriter` — accumulate every frame, write on stop (RAM-unbounded) | **`ClipRemuxer`** — open container on start, **mux packets incrementally**, rebase timestamps, finalize on stop | RAM-bounded; source-timestamped |
| `VideoBufferManager` — Hot Folder poll, semaphore, disk check | **`VideoBufferManager`** — same orchestration, drives the new source/recorder | minimal change; reuse the poll/semaphore/disk logic |

### 3.1 `PacketStreamBuffer` (capture)

- Open with `av.open(url, options={"rtsp_transport": "tcp"}, timeout=…)`;
  iterate `container.demux(video_stream)`. **No `.decode()` call.**
- For each packet, record a lightweight `PacketRecord`:
  `bytes(packet)`/the packet handle, `pts`, `dts`, `time_base`, `is_keyframe`
  (`packet.is_keyframe`), and **`recv_wall = time.time()`** stamped at arrival
  (this is the wall-clock↔packet bridge; per CLAUDE.md the clock is the
  monitoring machine's).
- **Zero-drift preserved:** the loop has no `time.sleep()`; `demux()` blocks on
  I/O naturally.
- **Pre-roll ring:** a `collections.deque` evicting records older than
  `now − (pre_roll_sec + keyframe_margin_sec)`, so the buffer always retains at
  least one keyframe *before* the pre-roll horizon. Bounded by **time**, not
  clip length. Also keep the demuxed stream's codec parameters (for
  `add_stream(template=…)` on the writer side).
- Also expose the source `time_base` and codec context needed to open output
  streams.

### 3.2 `ClipRemuxer` (per active trigger)

- Created on a `start` trigger, under the existing semaphore, after the
  disk-free check passes.
- **Clip start = keyframe seek.** Compute `clip_start_wall =
  event_timestamp − pre_roll_sec`. Select the **last keyframe packet with
  `recv_wall ≤ clip_start_wall`**; if the ring doesn't reach that far back,
  start at the oldest retained keyframe and log a `preroll_truncated` warning.
  Starting on a keyframe is mandatory — a clip that begins mid-GOP won't
  decode.
- **Open output** with `av.open(path, "w")`; add one output stream via
  `out.add_stream(template=source_stream)` (copies codec parameters — no
  encoder is created).
- **Timestamp rebase (see [§4](#4-timestamp-handling-the-core-of-the-design)).**
- **Live phase:** as `PacketStreamBuffer` yields new packets, mux them straight
  to disk (`out.mux(packet)`), incrementally. RAM does not grow with clip
  length.
- **Stop conditions:** a matching `stop` trigger, `event_timestamp +
  post_roll_sec` reached, or `max_duration_sec` cap — whichever first. `extend`
  pushes the deadline out. On stop, flush and **close the container cleanly**
  (finalize).
- **Container format:** default **`.mkv`** or **`.ts`** (both VFR-safe and
  survive an abrupt close). Avoid `.mp4` for recordings that may be killed
  mid-write (trailing `moov` atom). Decide one in implementation; note it in
  DESIGN_HISTORY.

### 3.3 `VideoBufferManager` (orchestration — mostly reused)

- Keep `_poll_loop` / `_scan_trigger_dir` / `_handle_start` / `_handle_stop`
  / `_auto_stop` / `_check_disk_space` / `_writer_semaphore` essentially as-is;
  they are backend-agnostic. `_handle_start` constructs a `ClipRemuxer` instead
  of a `DiskWriter`; `_feed_active_writers` becomes "route each new packet to
  active remuxers" (or the remuxers subscribe to the shared `PacketStreamBuffer`
  directly — implementer's call, keep it lock-clean).

---

## 4. Timestamp handling (the core of the design)

The whole correctness story lives here — this is the primary Fable review
target.

- **Two clocks, two jobs.** *Wall-clock* `recv_wall` decides only the coarse
  clip **boundaries** (which packet starts/ends the window) — accurate to
  ±one packet, which is all a boundary needs. *Source PTS/DTS* decides all
  **intra-clip** timing, giving jitter-exact frame spacing and therefore true
  clip length.
- **Per-clip rebase.** Let the first written packet have `pts0`/`dts0` (source
  time_base). Write every packet with `pts −= pts0`, `dts −= dts0`, and
  reassign `packet.stream = out_stream` so PyAV rescales from source to output
  `time_base`. Result: clip starts at t=0 and its length equals the source PTS
  span of the written packets = true elapsed time.
- **DTS monotonicity / B-frames.** Muxers require non-decreasing DTS. If the
  stream carries B-frames (DTS < PTS, packets in DTS order), the rebase must
  preserve DTS order and the first packet must be an I-frame (guaranteed by the
  keyframe seek). Many surveillance encoders are baseline IPPP (no B-frames),
  which sidesteps this — **but do not assume it**; handle and test both.
- **Discontinuities / wraparound.** RTSP PTS can jump or wrap (camera reboot,
  RTCP resync). Per-clip rebase contains most of this; a *mid-clip*
  discontinuity needs a decision (clamp the gap vs. split the clip). Pick one,
  document it, and test against a long capture.

---

## 5. Configuration & backend selection

- Extend `VideoBufferConfig` with: `backend: str = "remux"` (values `remux` |
  `full`), `keyframe_margin_sec: float` (pre-roll keyframe safety, default ~2×
  expected GOP seconds), and `container_ext: str` (default `.mkv`/`.ts`).
- `system_runner.py` chooses the implementation from `backend`: `remux` →
  `remux_video_buffer.VideoBufferManager` (edge default); `full` →
  the existing `video_buffer.VideoBufferManager` (central/server, ample RAM,
  wants exact-FPS zero-drift with decoded frames available). This is a thin
  import switch, not a rewrite — the surfaces match ([§2](#2-what-stays-unchanged-contracts-we-must-not-break)).
- `_old_video_buffer.py` / `_edge_video_buffer.py` are **superseded** by this
  decision (both are interim CFR attempts). Recommend moving them to a
  `legacy/` folder or removing them once remux is verified; that cleanup is a
  follow-up, not part of this item, and CLAUDE.md's "kept intentionally" note
  should be updated when it lands.

---

## 6. Future decoded path (leave room — do NOT build)

The owner may later want a decoded option (vision-based discrepancy source,
burned-in timestamps/overlays). Structure the remux code so that path is an
added stage, not a rewrite:

- Keep a clean unit boundary: `PacketStreamBuffer` deals in **packets**; a
  future `FrameStreamBuffer` would deal in **decoded frames** behind the same
  interface the manager consumes.
- Keep `ClipRemuxer`'s lifecycle (keyframe seek, trigger-driven start/stop/
  extend, semaphore, disk check, finalize) separable from its *write*
  strategy, so a `ClipEncoder` (decode → process → encode) can reuse the
  lifecycle and swap only the write step.
- Do not hardcode "packets only" assumptions into `VideoBufferManager`; it
  should orchestrate units generically. Note the seam in the module docstring.

Selecting decoded would be a *third* `backend` value later — not in scope now.

---

## 7. Verification

Two independent axes; only the second needs a real camera.

### 7.1 Blind self-test (Opus, this/next session — no camera needed)

Generate synthetic streams locally with ffmpeg and assert on written clip
length and structure:

```bash
# Known duration + GOP, constant rate:
ffmpeg -f lavfi -i testsrc=size=1280x720:rate=20:duration=120 \
  -c:v libx264 -g 40 -bf 0 -pix_fmt yuv420p synthetic_cfr.ts
# Jittered variant (variable PTS) to stress the length math:
ffmpeg -f lavfi -i testsrc=size=1280x720:rate=20:duration=120 \
  -vf "setpts=PTS*(1+0.1*sin(N/10))" -c:v libx264 -g 40 synthetic_jitter.ts
```

Opus writes **`video_engine/__replay_verify.py`** — a debug tool (`__`-prefixed,
print() allowed) that:
- replays a file (or a live URL) through the remux buffer as a **real-time-paced
  source**, standing in for live RTSP;
- fires `start`/`stop`/`extend` triggers via the real Hot Folder;
- cuts both a **short (~20s)** and a **long (~4min)** clip;
- **asserts** written-container duration ≈ requested window and ≈ the source
  PTS span (tolerance ~1 frame), the first frame decodes (no leading gray),
  and **RSS stays flat** across the long clip (RAM-boundedness — host-
  independent, so this proves the fix even off-edge).

These pin the length/keyframe/RAM logic without a camera. B-frame handling
gets a synthetic case too (`-bf 2`).

### 7.2 Real-stream verification (Fable, after a capture exists)

The owner records a real feed with `video_engine/tools/__capture_rtsp.py` on a
camera-capable box (does **not** need to be a J1900 — clip faithfulness comes
from the camera's timestamps, not the capture host). Fable then runs
`video_engine/tools/__replay_verify.py` against that `sample.ts`
(`video_engine/tests/fixtures/sample.ts`) and the profiled jitter/GOP, and
adversarially probes the [§4](#4-timestamp-handling-the-core-of-the-design)
edge cases on real data. Details in [§8](#8-division-of-labor--opus-vs-fable).

Absolute CPU/RAM/disk headroom on an actual J1900 is a later, low-risk
deployment check (remux does no decode/encode), not a blocker for this item.

---

## 8. Division of labor — Opus vs. Fable

**Opus (design + implement + blind self-test), one session end-to-end:**
- [x] `remux_video_buffer.py`: `PacketStreamBuffer`, `ClipRemuxer`,
      `VideoBufferManager` (reusing the poll/semaphore/disk logic).
- [x] Per-clip timestamp rebase + keyframe-seek pre-roll ([§4](#4-timestamp-handling-the-core-of-the-design)).
- [x] `VideoBufferConfig` extensions + `system_runner.py` backend switch
      ([§5](#5-configuration--backend-selection)).
- [x] Keep the decoded-path seam clean ([§6](#6-future-decoded-path-leave-room-do-not-build)).
- [x] `__replay_verify.py` + synthetic-stream self-tests
      ([§7.1](#71-blind-self-test-opus-thisnext-session--no-camera-needed));
      green on CFR, jittered, and B-frame synthetics.
- [x] Add `av` (PyAV) to `requirements.txt`; note the container-format choice.
- [x] DESIGN_HISTORY.md entry; check off the Opus boxes in ROADMAP Item 1.

**Fable (verification + debug escalation), after the owner's real capture:**
- [x] Run `__replay_verify.py` against the real `sample.ts`; confirm
      written-length accuracy within tolerance under **real** jitter, and
      RAM-boundedness across a long clip. *(2026-07-15: exact — 0.0000s error
      over 180.005s / 1801 packets; RSS growth 3.0 MB.)*
- [x] Adversarial probes on real data: B-frame/DTS-monotonicity, first-frame
      decodes from the keyframe seek, a mid-clip PTS discontinuity behaves per
      the [§4](#4-timestamp-handling-the-core-of-the-design) decision,
      concurrent triggers under the semaphore both produce correct clips,
      RTSP drop/reconnect mid-recording finalizes cleanly. *(All pass; forward
      PTS gaps are preserved by design — documented in the module docstring.)*
- [x] Debug any drift/dup/freeze the synthetic self-tests missed; record the
      outcome and any fix in DESIGN_HISTORY.md. *(None found; see the
      2026-07-15 DESIGN_HISTORY entry.)*

This matches Fable's defined role (correctness-critical + debugging
escalation) and keeps Opus's blind implementation honest by having a second
capable model verify the timestamp core against real jitter.

---

## 9. Acceptance criteria

1. A remux backend runs from `system_runner.py` via `backend: "remux"`,
   driven by the unchanged Hot Folder triggers.
2. On synthetic CFR, jittered, and B-frame streams: written clip length
   matches the requested window (and source PTS span) within ~1 frame; clips
   decode from frame 0.
3. RSS stays flat across a 4-minute clip (no per-clip RAM growth) —
   the CLAUDE.md violation is gone.
4. The existing `full` backend still selectable and unchanged.
5. Real-stream Fable pass green (or its findings triaged and fixed), logged in
   DESIGN_HISTORY.md.
