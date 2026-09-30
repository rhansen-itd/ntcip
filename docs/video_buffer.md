# Video buffer and edge constraints

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 537-596). Read before changing remux_video_buffer.py. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L537-596 -->

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
