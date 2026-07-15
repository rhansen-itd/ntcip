# Design History — NTCIP Traffic Monitor + Video Engine

This file is the running record of *why* the system is built the way it is:
the build history to date and an append-only, dated **Decisions log** for
every design decision as it lands. It is the companion to two other docs:

- **[[CLAUDE.md]]** documents what the code *currently does* — conventions
  verified against the implementation. It is the "now" snapshot.
- **[[ROADMAP.md]]** documents what still needs *deciding or building* —
  priority-ordered, stable-ID items.
- **This file** documents *how we got here and why* — the trail of decisions
  behind CLAUDE.md's conventions, so a decision's rationale outlives the
  person who made it.

## How this file is maintained (the workflow)

Work is scoped as numbered items in [[ROADMAP.md]], each sized to roughly one
focused working session. When an item is finished:

1. Check off its boxes in [[ROADMAP.md]].
2. Append a **dated entry to the Decisions log below** capturing what changed
   and — more importantly — *why*, including corner cases and rejected
   alternatives. Future readers should not have to re-derive the reasoning
   from the diff.
3. If a load-bearing convention changed, update [[CLAUDE.md]] so the "now"
   snapshot stays accurate, and cross-reference the decisions-log entry.
4. Move fully-completed ROADMAP items out of ROADMAP.md and summarize them
   here, so ROADMAP stays a forward-looking list.

Entries are **append-only** and dated (`YYYY-MM-DD`). Don't rewrite past
entries to match later decisions — supersede them with a new dated entry that
says what changed and why, so the evolution stays legible. Link related
entries and docs with `[[wiki-links]]`.

The canonical spec for any given subsystem still lives in its code (e.g.
`video_engine/discrepancy_engine.py`'s module docstring for the discrepancy
rules, `video_engine/config_manager.py`'s docstring for the trigger-file
schema). This file records the *decisions and their rationale*; it points at
the code, it does not duplicate it.

---

## Build history (pre-workflow, reconstructed from git)

This section back-fills the history that predates the design-history workflow
(started 2026-07-14), reconstructed from the commit log. It's coarse — commit
granularity, not session granularity — because the detailed reasoning wasn't
being logged yet. Everything from here forward should be finer-grained.

### Phase 0 — NTCIP monitor foundation (2026-02-08 → 2026-02-09)

- `f1914fa` Initial commit; `8a27925` initial commit of modules — the
  `ntcip_monitor/` SNMP polling core (phase/detector/output monitors, SNMP
  client, OID definitions, data models).
- `161a91f` / `408b91d` / `19f66fd` — pedestrian/ring status monitoring added
  and debugged against real controller behavior.

### Phase 1 — Video engine added (2026-02-24)

- `5ae31a4` "Added video engine. Added ring status oids." — introduces the
  independent `video_engine/` package (RTSP/HTTP capture, RAM pre-roll, disk
  recording, discrepancy engine) and ring-status OIDs on the monitor side.
  This is where the two-package architecture (see the 2026-07-14 decisions-log
  entry) originates.

### Phase 2 — Cleanup & conventions documented (2026-06-29)

- `222e630` Updates; `0b71942` remove compiled code files.
- `1f48bfa` / `4a6e786` — superseded `video_engine` drafts snapshotted (so
  they remain recoverable) and then removed from the tree.
- `c18f066` "Document project conventions…" — [[CLAUDE.md]] written, verifying
  established conventions against the implementation; `ring_monitor.py` and
  video-engine tooling added. The conventions codified here are the source for
  the back-filled decisions logged on 2026-07-14 below.

### Phase 3 — Design-history workflow adopted (2026-07-14)

- [[ROADMAP.md]] reorganized to the stable-ID, priority-ordered item format
  with Target + Suggested-prompt per item; this DESIGN_HISTORY.md started, and
  the existing load-bearing decisions back-filled into the log below. Modeled
  on the workflow used in the sibling `econ_itd_tools`/`iprj_designer` project.

---

## Decisions log

*(append as made — dated, append-only; newest at the bottom of each date)*

The 2026-07-14 entries are **back-filled** from conventions already verified
in [[CLAUDE.md]] at the time the workflow started. They capture decisions made
earlier (during Phases 0–2 above) whose rationale is worth preserving; the
dates reflect when they were *logged here*, not when they were originally
decided. Entries after this point are logged as the decision lands.

- 2026-07-14 — **Design-history workflow started.** Adopted the
  ROADMAP-item → check-off → decisions-log-entry loop from the sibling
  `iprj_designer` project (see [[ROADMAP.md]] intro for the item conventions).
  Reason: the project had accumulated real, hard-won design decisions
  (SNMP quirks, the Hot Folder decoupling, the video-buffer RAM tradeoff)
  documented only as "this is how it is" in [[CLAUDE.md]], with the *why*
  living in people's heads. This file gives that reasoning a durable home.

- 2026-07-14 — **Two independent top-level packages; neither imports the
  other** (back-filled). `ntcip_monitor/` (SNMP polling, event emission) and
  `video_engine/` (capture, buffering, discrepancy brain) are decoupled on
  purpose. `discrepancy_engine.py` subscribes to monitor detector events
  in-process (wired by `system_runner.py`), but a confirmed discrepancy
  reaches the video buffer *only* by writing a trigger file to a spool
  directory. **Why:** the two halves must deploy, test, and swap
  independently — in particular, a future non-NTCIP discrepancy source should
  be able to drive the same video engine with zero `video_engine` changes.
  Enforcement: never add a direct import across the boundary. See
  [[CLAUDE.md]] "Module boundaries".

- 2026-07-14 — **Hot Folder as the only bridge between the halves**
  (back-filled). Writer (`discrepancy_engine.py`) writes full JSON to `*.tmp`
  then does an atomic `os.rename()` to `trigger_{iso8601}_{uuid4_short}.json`;
  reader (`video_buffer.py`) polls the directory oldest-first
  (`glob("trigger_*.json")`), never with a sleep inside the frame-capture
  loop. **Why the tmp+rename:** a reader must never observe a partially
  written trigger. **Why polling, not a callback:** it keeps the decoupling
  above absolute — the video side has no code path into the monitor side.
  Canonical trigger-file schema lives in `config_manager.py`'s docstring;
  don't add fields without updating both sides and that docstring.

- 2026-07-14 — **All discrepancy timestamps come from the monitoring
  machine's own clock** (`time.time()`/`datetime.now()`), never from camera-
  or controller-reported time (back-filled). **Why:** the discrepancy rules
  compare events sub-second; mixing clock sources (camera NTP drift,
  controller clock skew) would make those comparisons meaningless. This is a
  hard invariant, not a preference.

- 2026-07-14 — **SNMP event callbacks must return in microseconds**
  (back-filled). `on_detector_on`/`on_detector_off`/etc. only mutate a few
  scalar fields under a lock; all real work (evaluation, file writes, I/O)
  happens on the background evaluator thread. **Why:** callbacks run on the
  polling path; any blocking call there stalls polling and corrupts the timing
  the discrepancy rules depend on.

- 2026-07-14 — **Econolite Cobalt/EOS hardware quirks are deliberate, not
  bugs** (back-filled). Baked into the code and confirmed against real
  hardware: SNMP **v1** (not v2c), port **501** (not 161), community string =
  controller username, Phase 1 = bit 0. Separately,
  `EconoliteSNMPClient.CHUNK_SIZE = 1` forces one OID per request. **Why
  CHUNK_SIZE=1:** dense Cobalt/EOS tables return "Too Big" SNMP errors on
  multi-OID gets; single-OID requests are the confirmed workaround. It looks
  inefficient but must not change without re-confirming against hardware. See
  [[CLAUDE.md]] "NTCIP / SNMP rules" and [[ROADMAP.md]] Item 4a (batching the
  *call* is safe precisely because `get()` re-chunks to 1 internally).

- 2026-07-14 — **Config access goes through the `ConfigProvider` ABC**
  (back-filled). `JsonFileConfigProvider` (edge) and
  `SqliteCentralConfigProvider` (central) are the two concrete
  implementations; `system_runner.py` defaults to JSON. **Why:** edge boxes
  and the central multi-intersection server have genuinely different config
  storage, and both deployment paths must stay first-class — so new
  intersection-level config needs extend the ABC and *both* implementations
  together, never a dict lookup that bypasses one path.

- 2026-07-14 — **Video-buffer RAM-vs-drift tradeoff is unresolved; three
  implementations kept on purpose** (back-filled — this is an *open* decision,
  tracked as [[ROADMAP.md]] Item 1). Production `video_buffer.py` holds every
  raw frame of the whole clip in RAM to compute an exact FPS at stop time —
  correct but RAM-unbounded, viable only because it currently runs on a
  powerful non-edge machine. `_edge_video_buffer.py` and `_old_video_buffer.py`
  are RAM-bounded alternatives kept as live candidates. **Why logged here:**
  which of the two bounded variants was actually verified working is genuinely
  unresolved (human memory has flipped once); the decision must be made
  *empirically* against a real stream, not by picking a winner from memory or
  docstring polish. Do not wire a buffer implementation into `system_runner.py`
  or edit any of the three files before reading ROADMAP Item 1's provenance
  note. This entry will be superseded by a dated resolution when Item 1 lands.

- 2026-07-14 — **Item 1 direction decided: remux/stream-copy** (supersedes the
  "video-buffer tradeoff unresolved" entry above for the *production edge
  path*). Rather than pick between the two RAM-bounded CFR variants
  (`_old_`/`_edge_`), we're replacing the CFR `cv2.VideoWriter` approach
  entirely with a PyAV **remux** buffer: demux RTSP into encoded packets, keep
  a RAM-bounded (time-windowed) packet pre-roll, and copy packets to disk using
  the camera's own presentation timestamps — no decode, no encode. **Why:**
  (1) accurate clip length *by construction* (length = source PTS span = true
  elapsed), which is the actual defect Item 1 exists to fix and which no
  single-FPS approach can guarantee under RTSP jitter; (2) near-zero CPU on the
  J1900 (stream-copy); (3) RAM bounded by a time window, not clip length,
  resolving the CLAUDE.md "dump pre-roll then stream to disk" violation; (4) it
  develops well *blind* — correctness comes from source timestamps, not tuned
  parameters. This makes the `_old_`/`_edge_` provenance question moot for
  production (both are superseded; `video_buffer.py` stays as the `full`
  central/server backend behind a config flag). Accepted cost: a new dependency
  (PyAV) and keyframe/DTS handling. Owner flagged a possible **future decoded
  option** (vision source / burned-in overlays), so the design keeps a clean
  packet-vs-frame seam so that's an added branch, not a rewrite. Full plan +
  Opus/Fable split: [VIDEO_BUFFER_REMUX_PLAN.md](video_engine/VIDEO_BUFFER_REMUX_PLAN.md).
  Implementation is a following session; this entry will be followed by a
  dated "implemented"/"verified" entry when it lands. See [[ROADMAP.md]]
  Item 1.

- 2026-07-14 — **Item 1 implemented (Opus pass): remux backend + blind
  self-test landed.** New module `video_engine/remux_video_buffer.py` implements
  the PyAV stream-copy buffer decided above, mirroring `video_buffer.py`'s public
  surface (`VideoBufferConfig`, `VideoBufferManager`) so `system_runner.py`
  selects it via a config flag. Three roles: `PacketStreamBuffer` (demux →
  time-windowed encoded-packet ring, no `time.sleep()` in the read loop, keyframe
  retained behind the pre-roll horizon), `ClipRemuxer` (keyframe-seek pre-roll,
  per-clip timestamp rebase, incremental mux, clean finalize), and a
  `VideoBufferManager` that reuses the Hot Folder poll / semaphore / disk-check.
  Backend selection is a thin import switch in `SystemRunner._build_video_manager`
  keyed on the intersection config's `video_backend` (`"remux"` default edge /
  `"full"` legacy CFR central). PyAV (`av>=12`) added to `requirements.txt`.

  **Decisions made during implementation (not pre-specified in the plan):**

  - **Packets are detached from their source container, not held as `av.Packet`.**
    The first cut stored live `av.Packet` objects in the ring and used
    `add_stream_from_template(packet.stream)`. This **segfaulted**: pre-roll
    packets (and packets buffered across an RTSP reconnect) outlive the container
    they were demuxed from, and an `av.Packet.stream` / template reference into a
    closed container is a use-after-free. Fix: at demux time copy the encoded
    payload to `bytes` plus plain pts/dts/duration/time_base/keyframe into a
    `PacketRecord`, and capture codec params once per stream-open into a
    `StreamTemplate` (codec name, w/h, pix_fmt, extradata, time_base). The output
    stream is built with `add_stream(codec_name)` + params — no live input-stream
    reference, no encoder. This is load-bearing for both the ring and reconnect
    survival, not just the test. (PyAV 18's template call is also renamed
    `add_stream_from_template`; moot now that we don't use it.)
  - **Timestamp rebase uses a single offset = first packet's DTS, subtracted from
    both PTS and DTS.** The first attempt subtracted `pts0` from PTS and `dts0`
    from DTS separately; on B-frame streams `pts0 != dts0`, which inverts
    `pts >= dts` on the first following packet and the muxer rejects it
    (`EINVAL`). One offset preserves the PTS/DTS relationship. The clip's length
    is `max_pts − min_pts`, which is offset-invariant, so length fidelity holds
    regardless (verified exact to 0.000s on CFR, jitter, and B-frame synthetics).
  - **Mid-clip DTS discontinuity → clamp, don't split** (plan §4 left this open).
    On a backward jump / wraparound / stall (`out_dts <= last_out_dts`) the
    remuxer re-anchors the offset so output DTS advances one frame past the last
    and continues — keeping every frame with a one-frame gap rather than splitting
    the clip. Simplest correct choice for the edge; Fable probes it on real data.
  - **Container format = `.ts` (MPEG-TS), default `container_ext`.** MPEG-TS
    survives an abrupt process kill / power loss with no trailer to finalize
    (unlike `.mp4`'s trailing `moov` atom), needs no repair to be playable, and
    matches `__capture_rtsp.py`'s `sample.ts`. Overridable via config.
  - **Packet routing is by subscription, not a per-tick flush.** A started
    `ClipRemuxer` `subscribe()`s to its camera's `PacketStreamBuffer` under the
    capture lock, which atomically hands it the pre-roll snapshot and registers it
    for live packets — so there is no gap/dup at the seam and no `_feed_active_writers`
    polling. Each remuxer owns a writer thread + queue, keeping disk I/O off the
    zero-drift capture loop (mirrors the old `DiskWriter` threading model).
  - **`extend` now genuinely extends** (reschedules the max-duration timer) in the
    remux backend, where the legacy `full` backend treated `extend` as `stop`.
    The discrepancy engine doesn't currently emit `extend`, so this is forward-
    looking, not a behavior change to an exercised path.

  **Blind self-test — `video_engine/__replay_verify.py`** (debug tool, `print()`
  allowed). Replays ffmpeg-synthesized streams through the *real*
  `VideoBufferManager` and Hot Folder via a real-time-paced `PacketStreamBuffer`
  subclass injected through a new `stream_buffer_factory` seam on the manager
  (the same seam a future decoded `FrameStreamBuffer` would use — plan §6). All
  green: length fidelity exact (0.000s) on CFR / jitter / B-frame; first frame
  decodes and is a keyframe; a real-time windowed clip tracks
  `pre_roll + (stop − start)` (keyframe-aligned); and **RSS stayed flat across a
  genuine 240s / 4800-packet clip (≈0.8 MB growth)** — the CLAUDE.md
  RAM-unboundedness violation is gone by construction. Real-stream verification
  (§8) is left for the Fable pass against an owner capture. See [[ROADMAP.md]]
  Item 1 and [VIDEO_BUFFER_REMUX_PLAN.md](video_engine/VIDEO_BUFFER_REMUX_PLAN.md).

- 2026-07-14 — Added `video_engine/__capture_rtsp.py` (ffmpeg/ffprobe-based,
  stdlib-only) to record a faithful stream-copy sample + a jitter/GOP profile
  from a real camera, for the later Fable verification pass. Capture host is
  irrelevant to fidelity (timestamps come from the camera), so it can be run
  from any camera-capable box, not a J1900.

- 2026-07-14 — **Structured JSON-lines logging, not `print()`** (back-filled).
  Business logic in the monitor/discrepancy/buffer packages logs via the
  shared `_JsonFormatter` pattern (`logging` module). `print()` is reserved
  for the standalone manual test scripts (`__trigger.py`, `__record.py`,
  `simulate_playback.py`). **Why:** edge/central deployments need parseable
  logs; ad-hoc prints don't survive to the log aggregator.

- 2026-07-15 — **Reorganized `video_engine/` dev tooling and fixtures.** Moved
  the five standalone debug/manual scripts (`__capture_rtsp.py`, `__trigger.py`,
  `__record.py`, `__replay_verify.py`, `simulate_playback.py`) into
  `video_engine/tools/`, and the captured verification fixture (`sample.ts` +
  its `.packets.jsonl` profile) into `video_engine/tests/fixtures/`. The three
  tools that import sibling `video_engine/` modules gained a one-line
  `sys.path.insert(0, <parent>)` bootstrap so they run from any cwd.
  **Why:** separate throwaway dev/debug scripts and test data from the
  production package surface, following conventional project layout; the
  production modules (`remux_video_buffer.py`, `video_buffer.py`,
  `discrepancy_engine.py`, `config_manager.py`, `system_runner.py`) stay flat
  in `video_engine/`. No production code moved — the two packages' import
  boundary is unchanged.

- 2026-07-15 — **Fixed two `__capture_rtsp.py` bugs found during the first real
  capture.** (1) The RTSP demuxer rejects ffmpeg's `-rw_timeout`; switched to
  `-timeout` for `rtsp://` URLs (kept `-rw_timeout` for http/file). (2) The
  `url` positional was required even with `--profile-only`; made it optional and
  moved the requirement check into the capture path. **Why:** both only surface
  against a live camera (the dev box can't reach one), so they slipped past the
  synthetic self-tests; the owner hit them capturing the first `sample.ts`.

- 2026-07-15 — **ROADMAP Item 1 complete: remux backend verified against the
  owner's real capture (Fable pass) — all standard checks and adversarial
  probes green, zero code defects found.** The real fixture
  (`video_engine/tests/fixtures/sample.ts`: h264 720×720 10fps, 180s, 1801
  packets, genuine jitter — PTS deltas 0.011ms–150ms, σ≈11ms — long 6.2s GOPs,
  no B-frames, monotonic DTS) was run through
  `video_engine/tools/__replay_verify.py`: **length fidelity exact** (out span
  180.0050s = source span 180.0050s, 0.0000s error, all 1801 packets) and
  **RSS flat** (3.0 MB growth across the full-length clip). Adversarial probes
  (plan §4 edge cases, run via `video_engine/tools/__probe_adversarial.py` —
  committed so they can be re-run; it reuses `__replay_verify.py`'s
  `Harness`/`PacedReplayStreamBuffer`):
  - *Windowed clip @1x on the real 6.2s-GOP stream* — 13.0s for a 10.0s
    request, inside the keyframe-aligned bound (start ≤ one GOP early, never
    unboundedly off); first frame decodes and is a keyframe.
  - *Mid-clip backward PTS/DTS jump* (self-concatenated TS, PTS restarting at
    1.4s mid-stream) — re-anchor clamp worked exactly as designed: output span
    360.010s ≈ 2× source, all 3602 packets kept, output DTS strictly
    monotonic, max residual gap one frame (0.15s), decodes from frame 0.
  - *Mid-clip 64s forward PTS gap* (spliced 0–60s + 120–180s cuts, `-copyts`)
    — **behavior finding, judged correct, now documented**: forward gaps pass
    through unclamped (out span 180.0s with the 64s hole preserved). Rationale:
    a forward jump means no frames arrived, so preserving it keeps clip span =
    true elapsed time — the design's core promise; clamping would hide a real
    camera outage from the reviewer. Documented in the module docstring with a
    revisit note should real hardware ever produce an absurd (hours) forward
    resync jump. No code change beyond the docstring.
  - *B-frames on real content* (re-encode of the capture with `-bf 2`; 1801
    pts≠dts packets) — exact fidelity (180.1000s = source), DTS strictly
    monotonic, `pts ≥ dts` invariant held, all 1801 frames decode.
  - *Concurrent triggers under the semaphore* — two simultaneous clips on one
    camera both correct (8.90s each, keyframe starts, monotonic); a third
    trigger at the cap=2 was dropped with the expected warning.
  - *Source drop + reconnect mid-recording* (looping replay buffer with
    `reconnect_on_eof=True`) — the clip spans the reconnect (261.2s,
    2614 packets), the PTS restart was clamped to one frame, output DTS
    monotonic, the whole clip decodes, clean finalize.
  Also confirmed in passing: the fidelity test exercises the
  `preroll_truncated` fallback (ring shorter than the requested window → falls
  back to the earliest keyframe) on every run, and single-camera-per-trigger
  (`target_cams[0]`) is pre-existing parity with the `full` backend, not a
  remux regression. **Why this closes Item 1:** plan §9 acceptance criteria
  1–4 were met by the 2026-07-14 Opus session; criterion 5 (real-stream Fable
  pass green) is met by this entry. Item 1 is removed from [[ROADMAP.md]]; its
  full background (CFR-variant comparison, `_old_`/`_edge_` provenance
  question — moot for production since remux takes timing from source PTS)
  stays preserved in
  [VIDEO_BUFFER_REMUX_PLAN.md](video_engine/VIDEO_BUFFER_REMUX_PLAN.md) and the
  2026-07-14 entries above. The follow-up cleanup (retire the superseded
  `_old_`/`_edge_` CFR buffers) is now ROADMAP Item 5.

- 2026-07-15 — **Clean manual-recording CLIs; retired the `__record`/`__trigger`
  harnesses.** Added `video_engine/tools/record_clip.py` (one-shot clip with an
  optional `--at HH:MM[:SS]` scheduled start and a `--serve` long-running mode)
  and `video_engine/tools/drop_trigger.py` (start/stop/extend Hot Folder
  triggers), and removed the superseded `__record.py` (hardcoded URL, wired to
  the legacy `full` backend, blocked forever) and `__trigger.py`. **Why:** the
  video engine only records in response to Hot Folder triggers, so manual
  recording always needs "run the manager + drop triggers." `__record`/`__trigger`
  were a crufty two-terminal dev harness for that; `record_clip.py`
  (fire-and-forget or `--serve`) + `drop_trigger.py` cover the same ground
  cleanly. Scheduling is tool-side, not in the trigger: the trigger's
  `event_timestamp` is a *retrospective* anchor into the ~`pre_roll +
  keyframe_margin` (~14 s default) pre-roll ring, never a forward scheduler, so
  `--at` holds the trigger until the target wall-clock time. Both new tools are
  `remux`-only by design.

- 2026-07-15 — **Scoped ROADMAP Item 6: retire the `full` (CFR) backend.**
  `video_buffer.py` is unused (nothing sets `video_backend`; all deployments
  default to `remux`), strictly worse than remux on the edge constraints, and
  carries the known RAM-unboundedness bug. Filed Item 6 to delete it (recommended)
  or, if a central decoded need is confirmed, replace it with a RAM-bounded
  decoded backend — explicitly *not* the CFR one. **Why:** keeping a broken,
  unused backend as a co-equal option is misleading; the future decoded path
  (plan §6) is a new bounded branch, not this file. Item 6 reopens Item 5's
  "keep `video_buffer.py`" note — the two should be coordinated.
