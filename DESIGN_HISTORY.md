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

- 2026-07-19 — **Discrepancy-engine accuracy: stale-refire fix, Rule 2
  interval-based partner history, and the accuracy validation harness
  (ROADMAP Item 7, Fable).** Trigger: the owner compared the engine's live
  output (`discrepancies_log.csv`, NTCIP-polled at 0.2 s) against an ATSPM
  ground truth from the sibling `pyatspm` project (controller data at 0.1 s)
  and the two looked "way off". Scope was worked out in
  `SCOPE_discrepancy_accuracy.md` (repo root); three changes landed:
  1. **Rule 2 stale-refire guard** (inline fix earlier the same day, this
     session pinned it with tests): orphan pulses re-fired once per ~60 s
     cooldown expiry on *unchanged* detector state — 15 of the 76 rule 2 rows
     in the 2-hour sample were phantom duplicates with identical pulse
     windows. Fixed by `last_handled_pulse_on_a/b` in `_PairRuntimeState` and
     a monotonic ON-edge guard in `_maybe_register_orphan`: a pulse is armed
     at most once, ever.
  2. **Rule 2 partner history is now an interval record, not a scalar.**
     `_check_rule2_orphan` used to decide "was the partner OFF for the whole
     observation window?" from `other_last_on` alone — a scalar cannot
     represent an interval, so (a) a partner actuating *after* the window
     matched no branch and silently suppressed a legitimate orphan (false
     negative), (b) a mid-window partner ON overwritten by a newer edge became
     invisible (leaked Rule 3 overlap → false positive), and (c) a partner ON
     straddling the window start looked like "last ON before window" and fired
     falsely. Now each `_DetectorState` keeps `on_intervals`, a bounded deque
     of completed `(on_ts, off_ts)` intervals appended O(1) on the falling
     edge under the existing per-detector lock (callback microsecond contract
     preserved); the evaluator thread prunes it during its snapshot (time
     window ≈ 3×threshold; `maxlen=128` as a RAM backstop) and
     `_check_rule2_orphan` — still a pure function — does true interval
     intersection against the window. **New behavior decision:** a verdict
     rendered more than `_ORPHAN_DECISION_GRACE_SEC` (2 s) after the window
     closed is discarded instead of fired. Rationale: the evaluator only
     reaches a candidate that late when the pair sat in cooldown or inside an
     active Rule 1 recording, and by then the RAM pre-roll for the event is
     gone — the old code could fire a trigger whose clip showed the wrong
     minute entirely. Trigger schema and Hot Folder untouched.
  3. **Accuracy validation harness** (`video_engine/tools/__accuracy_report.py`,
     stdlib+pytz dev tool): measures *correspondence*, not raw counts — counts
     diverge by design (per-pair 60 s cooldown, 0.2 s poll). It reconstructs
     precise engine event times from the rule 2 descriptions' embedded Unix
     windows (with a clock self-check against `Local_Timestamp`), restricts to
     the engine's coverage blocks (gap-split) ∩ the GT export window, does
     global nearest-start one-to-one matching, and separates expected misses
     (poll-aliased pulses < 2×poll; export-boundary-clipped events; simulated
     cooldown/active-recording suppression — an upper bound, since the early
     cooldown reset can shorten real cooldowns) from **true misses**, plus a
     per-pair breakdown. On the 2026-07-19 sample: precision 38/104 = 36.5 %
     (42.7 % projected post-fix — the 15 phantoms are tagged individually),
     adjusted recall 36.5 %, and the per-pair table shows the residual gap is
     strongly bimodal: pairs 30:41 (8/8), 22:39 (3/3), 29:43 (9/10 real) match
     well, while 17:2, 26:33, 24:7, 38:7 have *zero* correspondence in either
     direction with nearest-candidate deltas of hundreds of seconds —
     i.e. the engine config and the pyatspm export likely disagree about
     those pairings/phase mappings. That config audit is the recommended next
     accuracy step, not further rule changes.
  Also seeded the repo's first unit-test file:
  `video_engine/tests/test_discrepancy_rules.py` — 26 stdlib-`unittest` cases
  (pytest is not installed on the target env) covering
  `_check_rule1_continuous` boundaries, `_check_rule2_orphan` interval
  semantics (incl. the three scalar-era bug shapes above and the staleness
  grace), `_maybe_register_orphan` incl. the stale-refire guard, and
  integration tests driving real callbacks + `_evaluate_pair` against a stub
  `ConfigProvider` and a temp Hot Folder. This layout (per-package `tests/`,
  `unittest`, `sys.path` bootstrap like `tools/`) is the precedent Item 4d
  should mirror. Item 7 is removed from [[ROADMAP.md]];
  `SCOPE_discrepancy_accuracy.md` remains on disk as the detailed scope
  record.

- 2026-07-19 — **Channel-mapping investigation + raw NTCIP capture tool
  (`__capture_ntcip.py`).** Follow-up to the Item 7 accuracy harness's
  bimodal per-pair result. Findings: (1) the pairwise-triangle expansion of
  3-detector phases is **not** the problem — pyatspm's `int_cfg.csv` `Det: P*
  Pairs` table (6/1/2026 epoch) lists the exact same triangles the engine
  builds from `_intersections.json`, and both sides expand them identically;
  (2) it is **not a label swap** either — dead-pair engine triggers align in
  time with *no* GT anomaly on *any* pair (nearest 10–90 s away), ruling out
  "engine pair X is really pyatspm pair Y"; (3) the signature is
  **channel-level**: on phases 2, 6, 7 (+ Currux det 4), both engine channels
  pulse but essentially never overlap, while controller-internal data shows
  those zones agreeing — and dead vs. healthy channels interleave within the
  same SNMP detector groups (2,4,7 dead / 3,8 healthy in group 1), ruling out
  a systematic bit/group off-by-one in the monitor. Likeliest cause:
  per-channel assignments in `_intersections.json` (which NTCIP channel each
  physical Evo/ITSPlus/Currux unit drives) are wrong for those phases.
  Also found: `video_engine/intersections.json` — what `system_runner` loads
  by default — is a **stale 10-detector config** with placeholder pairs
  (11↔51 "phase 11", 13↔52); the live run that produced the sample log used
  the 19-detector triangle config at repo root (`_intersections.json`).
  Promoting it is ROADMAP #2 territory. To resolve the channel question
  empirically, added `video_engine/tools/__capture_ntcip.py`: polls all 64
  detector channels via the production `EconoliteSNMPClient` + the same
  `DETECTOR_GROUPS`/LSB-first bit math as `DetectorMonitor` (so it sees
  exactly what the engine sees), and writes every ON/OFF edge as CSV using
  ATSPM's own 82/81 event codes for direct correlation against the pyatspm
  SQLite raw log. `--config`/`--intersection` pulls the controller address
  from an intersection JSON; `--simulate` provides an offline fake controller
  (verified: 10/s achieved rate, alternating edges, init-state rows).
  `oid_definitions` is loaded by file path because the `ntcip_monitor`
  package `__init__` eagerly imports pysnmp, which `--simulate` must not
  require. Next step: capture a few minutes live and correlate per-channel
  edge streams against pyatspm channels (argmax similarity → true map).

- 2026-07-19 — **Channel mapping CONFIRMED; root cause of the phase-2/6/7
  discrepancy storms is RTT-bound SNMP sampling, not the config.** Correlated
  the 10-min raw NTCIP capture against the controller's own high-res log
  (datZ `ECON_10.37.23.200_2026_07_19_1730.datZ`, decoded with pyatspm's
  `parse_datz_bytes`; 489 s overlap) using the new
  `video_engine/tools/__correlate_channels.py` — Matthews-correlation scoring
  of per-channel ON/OFF waveforms (Jaccard was first tried and rejected:
  high-duty channels cross-match by chance), two-pass clock-skew alignment
  (+1.08 s NTCIP-behind-controller measured), margin-aware verdicts so
  co-located partners (which co-actuate by design) aren't misread as remaps.
  **Result: every verifiable channel in `_intersections.json` matches its own
  number.** The real defect, measured from the same capture: with
  `CHUNK_SIZE=1` the 8 detector-group reads are sequential round trips, so
  the **effective detector sweep is 1.0–1.5 s wall-clock** (median 1.53 s;
  `poll_interval` is only the inter-sweep sleep), and NTCIP catches only
  **~7–42 % of true detector edges** (ch 26: 6 of 64 edges in the overlap;
  ch 38: 3 of 43; ch 33: 29 of 153). Phases 2/6/7 are high-duty (80–94 % ON)
  presence zones with frequent sub-second gaps — aliased differently per
  technology, they *look* like constant disagreement to the engine while the
  controller's 0.1 s data shows agreement; phases 1/3/8 have sparse
  multi-second pulses that survive the sampling, which is why they matched
  ground truth. **Implications:** (a) ROADMAP 4a (batch the per-OID SNMP
  reads — test whether 8 small group OIDs fit one PDU despite the Cobalt
  "Too Big" history) is now the highest-leverage accuracy fix (~8× sweep
  speedup if it works); (b) until then, discrepancy rules on high-duty
  channels operate below the sampling floor and their triggers should be
  treated as unreliable; (c) `config_manager`'s "<0.5 s poll" warning is
  misleading — the sweep itself already exceeds it. Committed evidence:
  the capture CSV, the datZ, and its extracted event CSV.

- 2026-07-19 — **4a software half landed probe-ready (final Fable session,
  chosen as highest-leverage remaining work).** With the controller round
  trip impossible before Fable access ended, the code was restructured so
  applying the eventual probe verdict is a config flip, not a model session:
  (1) `EconoliteSNMPClient` now takes `chunk_size` (default 1 — the
  verified-safe Cobalt wire behavior; the old hardcoded `CHUNK_SIZE=1` is the
  class default). (2) `detector_monitor._poll` and `output_monitor._poll`
  batch their per-OID loops into one `get(*oids)` each (the client re-chunks
  internally and preserves order, so wire behavior at chunk 1 is byte-
  identical to before; `stats['reads']` now counts poll cycles, not OIDs —
  commented for `/api/stats`). (3) `system_runner._build_ntcip_monitor`
  derives `detector_range` from the intersection's configured detectors
  instead of hardcoding (1, 65) — at 201 that's groups 1–6 instead of 8, a
  guaranteed ~25 % sweep cut with zero risk — and passes a new
  `snmp_chunk_size` intersection-config key (default 1) through to the
  client; the standalone app gained `controller.chunk_size`. (4) First
  ntcip_monitor tests: `ntcip_monitor/tests/test_snmp_batching.py`, 10 cases
  against a stubbed `pysnmp.hlapi` whose fake `getCmd` logs per-PDU OID
  counts — pinning chunk splitting/ordering, scalar single-OID return,
  batched polls emitting correct edges, and range→groups derivation.
  **Why:** the 2026-07-19 correlation work proved the RTT-bound sweep
  (median 1.53 s, 7–42 % edge capture) is the dominant accuracy defect; this
  change banks the risk-free part now and reduces the remaining hardware
  work to: run `__probe_snmp_batch.py`, set `snmp_chunk_size` per its
  verdict, re-measure with `__capture_ntcip.py`/`__correlate_channels.py`
  (protocol in ROADMAP 4a). Runner-up considered and deferred to Opus:
  Item 8 (remux manager lock — real but rare-trigger hazard).

- 2026-07-19 — **Closing Fable handoff: pre-decided designs for the
  post-Fable roadmap.** Spent the final Fable budget on planning artifacts
  rather than execution, so cheaper models inherit decided designs instead of
  open questions: (1) **SCOPE_sampling_floor.md / ROADMAP #9** — the engine
  must not evaluate evidence finer than its sampling resolution: runtime
  sweep-time self-measurement in DetectorMonitor, floor injection into the
  discrepancy engine via system_runner (`set_sampling_floor`, preserving the
  no-cross-import boundary), Rule 2 refusal of below-2×-floor pulses,
  per-pair high-duty advisory (suppression opt-in), and a concrete
  pass/fail re-baseline protocol. (2) **4a fallback design** (in ROADMAP 4a)
  — if the Cobalt rejects multi-OID PDUs: N independent clients/threads for
  fixed group subsets writing latest-bitmask slots, single emitter thread
  preserved; probe to be extended with a `--concurrency` phase first since a
  weak controller CPU may serialize concurrent UDP. (3) **Item 8 retargeted
  Fable→Opus** with execution guidance: one lock, pure-bookkeeping critical
  sections, never stop()/join()/I-O under the lock (Timer-thread re-entry
  deadlock), snapshot `_draining` under lock, multi-camera warn-only, stubbed
  ClipRemuxer test. Post-Fable routing note added to the ROADMAP intro
  (order: 4a round trip → 8 → 9 → 4d → 4f → 4b/5).

- 2026-07-30 — **Sampling-floor awareness landed (ROADMAP 9, items A+B of
  [[SCOPE_sampling_floor.md]]; item C remains an owner-run protocol).** The
  governing principle is now enforced in code: *the engine must not evaluate
  evidence finer than its own sampling resolution.* (A) `BaseMonitor` records
  each `_poll()`-plus-sleep as an EMA (α=0.1), exposes `effective_cycle_sec()`
  and a new `get_stats()`, and logs a rate-limited (5 min) structured INFO
  when the EMA exceeds `2 × poll_interval` — the operator's signal that SNMP
  round trips, not `poll_interval`, set the sampling rate. `0.0` is the
  documented "not measured yet" sentinel. (B) The floor reaches the engine by
  **injection, not import**: `system_runner` calls the new
  `DiscrepancyMonitor.set_sampling_floor()` once at startup from the config's
  `sampling_floor_sec` (default 1.6 = the 2026-07-19 measured median) and
  thereafter every 60 s from `effective_cycle_sec()` on a daemon thread that
  waits on the shutdown event — the package boundary is untouched, and a
  single float assignment is atomic under the GIL (same pattern as
  `cooldown_active`). Rule 2 now refuses orphan candidates whose pulse is
  shorter than `min_pulse_floor_multiple × floor` (default 2.0×), counting
  them in a per-pair `below_floor_suppressed` and logging at DEBUG; rejected
  pulses are marked handled so the counter tracks pulses, not 0.1 s ticks. A
  per-pair rolling ON-duty fraction (new pure `_compute_on_duty_fraction`,
  120 s window, recomputed at most every 5 s on the evaluator thread) drives
  a rate-limited (10 min) structured WARNING when a pair's *minimum* duty
  exceeds `high_duty_warn_fraction` (default 0.8), with full Rule 1+2
  suppression available but **off by default** (`suppress_high_duty_pairs`)
  — a deployment decision, not an engine default.
  **Why this shape:** the 2026-07-19 measurements showed the false-trigger
  storms were aliasing, not rule errors — a *seen* sub-floor pulse fires
  while the partner's equally short response pulse is simply *unseen*. Gating
  on pulse length heals that asymmetry at its source; the pure rule functions
  stay pure (the floor is passed in as an argument), and the floor is
  measured rather than assumed so the gate self-corrects when 4a's probe
  lands. **Two consequences worth stating plainly:** (1) at the default 1.6 s
  floor the Rule 2 gate is 3.2 s, which *exceeds* a typical
  `lag_threshold_sec` of 2.0 s — so **Rule 2 is effectively disabled until
  the sweep gets faster**. That is the honest reading of the measurement, not
  an accident; after a green `snmp_chunk_size: 8` probe the floor drops to
  ~0.2 s and the gate to ~0.4 s, suppressing almost nothing real. (2) the
  `on_intervals` retention horizon is now `max(3 × threshold + grace, 120 s)`
  because the duty computation reads the same deque, so
  `_PARTNER_INTERVAL_MAXLEN` rose 128 → 512 (~8 KB/detector) to keep the
  time-based prune the real bound. **Documented, not coded** (per the scope):
  in the Rule 1 resolution state machine an *agreement* shorter than the
  floor is not reliable evidence of resolution — acceptable because a
  re-divergence restarts the post-roll countdown, so it can only delay a stop
  trigger, never truncate a clip. Tests: `test_discrepancy_rules.py` 26 → 50
  cases (floor gate incl. once-per-pulse counting, floor-update-takes-effect
  end-to-end through `_evaluate_pair`, 9 duty-fraction cases, high-duty
  warning + rate limit + opt-in suppression, which clears the Rule 1 timer as
  it goes so a pair resuming after a quiet period times afresh);
  `test_snmp_batching.py` 10 → 17
  (EMA seeding/blending, `get_stats`, slow-sweep log + rate limit, real
  `_run_loop` measurement). Note the pre-existing integration tests now
  declare `set_sampling_floor(0.01)` — at the production default their 50 ms
  pulses are correctly refused, which is itself a check that the gate works.

- 2026-07-31 — **`snmp_chunk_size: 8` adopted for intersection 201 on a green
  probe (ROADMAP 4a step 2).** The owner's 2026-07-20 probe run
  (`snmp_batch_probe_20260720_073926.json`, committed 2026-07-31) came back
  clean at every chunk size tried on the detector groups: 25/25 successes with
  correct ordering and byte ranges at chunks 1/2/4/8, median sweep **547 ms →
  94 ms** (5.8×), production 6-group shape 93 ms. So the Cobalt's historical
  "Too Big" failures really were a dense-table phenomenon, not a
  multi-OID-PDU limit — the hypothesis 4a was written to test. Set in
  `_intersections.json`, `intersections.json`, and
  `video_engine/intersections.json`. **Why only those:** all three are
  intersection 201 on controller 10.37.23.200, the probed box.
  `701_intersection.json` (10.70.10.51) and the standalone `config.json`
  (10.37.2.68) are different controllers with no probe evidence and stay at
  the default 1 — per the CLAUDE.md rule that chunk size is raised
  per-deployment only on a green probe for *that* controller.
  **Knock-on for ROADMAP 9:** the detector sweep drops ~1.5 s → ~0.1 s, so the
  measured sampling floor lands near 0.3 s (sweep + the 0.2 s inter-sweep
  sleep) and the Rule 2 gate falls from 3.2 s to ~0.6 s — i.e. **Rule 2 stops
  being effectively disabled** once the monitor restarts. No config change is
  needed for that: `system_runner` overwrites the 1.6 s startup assumption
  with the measured cycle within 60 s.
  **Caveat recorded, not fixed:** the probe's output phases failed 0/25 at
  chunk 1 *and* chunk 16 with `noSuchName at index 1` — identical at chunk 1,
  so it is an OID/support problem on `specialFunctionOutputState`
  (`...4.2.1.3.14.1.2.x`), not a chunking limit. Harmless today (outputs are
  `enabled: false` in `config.json` and `system_runner` never builds an
  `OutputMonitor`), but `output_monitor._poll` swallows `SNMPError` with a
  bare `pass`, so if outputs are ever enabled against this controller they
  fail silently forever. Logged as ROADMAP 10.

- 2026-07-31 — **ROADMAP tidied after the 9A/B + 4a-config landings.** Item 8
  moved above Item 9 (file order is priority order, and 9's only remaining
  part is owner-blocked while 8 is the top actionable item — the sole known
  bug in code that runs). The Fable-era routing note was replaced with a
  status-at-a-glance block, which also flags that 4a steps 3–4 and 9C need
  **one** controller capture between them, not two. 4a compressed to what
  remains: its narrative history now lives here, and the dirty-probe fallback
  (concurrent per-group clients) was dropped from the forward-looking list as
  moot given the green verdict — the design stays in the 2026-07-19 entry
  should another controller ever need it. Fixed stale text: 4d no longer
  claims no `tests/` directory exists (two do), and the sub-item ordering note
  no longer sequences work behind 4a.

- 2026-07-31 — **Remux `VideoBufferManager` bookkeeping put under one lock;
  the single-camera assumption made explicit (ROADMAP Item 8, done).**
  `_active_writers`, `_stop_timers`, and `_draining` were mutated from three
  thread contexts — the poll loop, `threading.Timer` callbacks (`_auto_stop`),
  and the main thread (`stop()`) — with no lock. All three are now guarded by
  a single `_state_lock`, with a strict discipline stated in the class
  docstring: **under the lock, pop/collect what to act on; release; then act.**
  Nothing blocking (`writer.finish()`, `join()`, `timer.cancel()`, semaphore
  acquires, disk checks, `buf.subscribe`/`unsubscribe`) happens while the lock
  is held — `_auto_stop` runs on a Timer thread and re-enters `_stop_trigger`,
  so a join under the lock would deadlock the reap path. Concretely:
  `_reap_finished` now selects *and* removes in one locked step and joins
  afterwards; `stop()` snapshots timers/trigger-ids under the lock and drains
  `_draining` by repeated locked `pop(0)` instead of snapshot-then-`clear()`;
  `_stop_trigger` does the whole pop-writer/pop-timer/append-draining hand-off
  atomically. **Why the shape matters and not just "add a lock":** the old
  `_reap_finished` walked a *snapshot*, joined, then `remove()`d — while
  `stop()` joined its own snapshot and `clear()`ed the list, so a reaper parked
  in `join()` came back to `ValueError: list.remove(x): x not in list` and the
  writer was joined twice. That exact interleaving is now a regression test
  (`test_reap_parked_in_join_is_not_clobbered_by_stop`), verified to fail on
  the pre-fix file and pass on the new one — the other concurrency tests pass
  either way, which is worth knowing about their strength.
  Two smaller correctness fixes came out of the same reading. (1) **Timers now
  carry a generation** (`_stop_timers: {trigger_id: (generation, timer)}`): a
  timer that fires just as `extend` supersedes it — i.e. whose `cancel()` lost
  the race — used to stop a clip it no longer owned, truncating footage the
  extend was asking to keep. `_auto_stop` now returns unless its generation is
  still the registered one. (2) **The writer and its timer are registered
  before the timer is armed**; previously `_stop_timers[tid]` was assigned
  *after* `timer.start()`, so a very short `max_duration_sec` could fire
  against bookkeeping that did not exist yet and leave a stale entry behind.
  **Single-camera assumption (latent, both backends):** `_handle_start` takes
  `target_cams[0]` and creates exactly one writer, so a `["all"]` or two-camera
  trigger silently recorded only the first. Deliberately *not* fixed with
  per-camera writers — there is no second camera deployed to test against.
  Instead both backends log a WARNING with `cameras_requested` /
  `cameras_recorded` when a trigger resolves to more than one, remux logs the
  same pair on every start, and the assumption is written down in
  `config_manager.py`. This is reachable config, not a schema formality: a pair
  whose two detectors name different `camera_id`s makes
  `_cameras_for_pair` return two cameras. The camera provenance went to the
  **structured log, not the CSV** — `discrepancies_log.csv` is appended with a
  header written only when the file is absent, so widening the row would
  silently misalign columns in every existing log.
  While there, `config_manager.py` gained the Hot Folder **trigger-schema
  section** that CLAUDE.md has always claimed lived there but didn't (it was
  only in CLAUDE.md itself), corrected against what the engine actually writes:
  `reason` is always `detector_disagreement` today, and there is a `timezone`
  field the CLAUDE.md copy omits.
  Tests: new `video_engine/tests/test_remux_manager.py`, 22 stdlib-`unittest`
  cases with `ClipRemuxer`/`PacketStreamBuffer` stubbed (no PyAV, no streams,
  no disk) per the item's "test economically" guidance — start/stop/reap
  bookkeeping, semaphore accounting across four sequential clips, the
  generation guard, threaded stop/reap races, and the camera warnings.
  `test_discrepancy_rules.py` (50) and `test_snmp_batching.py` (17) still pass.

- 2026-07-31 — **Web UI hardened: loopback by default, shared-secret header on
  `/api/control/*` (ROADMAP Item 4f, done).** `WebUI.__init__` defaulted to
  `host='0.0.0.0'` with no way to override it from `run.py`, and the three
  `/api/control/*` routes — sync controller time, place a vehicle call, toggle
  an output — took unauthenticated POSTs. Anything that could reach the port
  could drive real signal hardware. Two changes, deliberately small.
  **(1) Bind host.** Default is now `127.0.0.1`, with `--web-host` on `run.py`
  and `web_ui.host` in config (CLI > config > default); `web_ui.port` is now
  read from config too, since the key already existed and was silently ignored.
  `config.json`'s live `"host": "0.0.0.0"` was flipped to `127.0.0.1` — the key
  was dead before this session (run.py never passed it), so leaving it would
  have quietly preserved the exposure the item is about.
  **(2) Control auth.** A shared secret in the `X-NTCIP-Control-Token` header,
  compared with `hmac.compare_digest` (as bytes — `compare_digest` raises
  TypeError on non-ASCII `str`, and header values can be non-ASCII), sourced
  from `$NTCIP_WEB_CONTROL_TOKEN` first and `web_ui.control_token` second so
  the secret can stay out of a config file. The policy has three cases, and
  the third is the point of it: token set → header must match (401 otherwise);
  no token + **loopback** bind → allowed, which is exactly today's behavior for
  a local operator, so nobody's workflow breaks; no token + **non-loopback**
  bind → control refused with 403 plus a startup warning. That last case means
  the two halves of the fix reinforce each other — you cannot expose hardware
  control to the network by flipping one setting, you have to flip the host
  *and* set a secret. Loopback is decided with `ipaddress.is_loopback` (so all
  of `127.0.0.0/8` and `::1` count) plus the `localhost` names; anything
  unresolvable is treated as exposed.
  **Why a header and not real auth:** the deployment is one operator on an
  edge box, and the item explicitly scoped this as "not a full session/JWT
  system". Sessions, users, CSRF, and TLS all belong to a reverse proxy if the
  deployment story ever changes; ARCHITECTURE.md's Security Considerations now
  says so instead of the old blanket "Web UI has no authentication".
  `/api/status` and `/api/stats` stay open — read-only, and the dashboard
  polls `/api/status` every 250 ms, so gating them would have meant putting the
  secret in a page that has no auth in front of it anyway.
  **Dashboard:** the template only renders a token field when a token is
  configured (`control_token_required` is passed to `render_template`), stores
  what's typed in `localStorage`, and sends it as the header — so the existing
  Sync Controller Time button keeps working in all three cases without the
  server ever embedding the secret in the page.
  **Not tested in-repo:** Flask and Jinja2 aren't installed in this
  environment, so the policy was verified against a stubbed `flask` module
  (24 checks: loopback classification incl. `127.5.5.5`/`[::1]`/unresolvable
  names, all three policy cases, prefix-mismatch, non-ASCII header, and the
  whitespace-only token being treated as unset). A real Flask-test-client case
  belongs to ROADMAP 4e, which already lists `WebUI` and owns the fixture
  strategy; the template render is unverified by machine here.

- 2026-07-31 — **Live Video Overlay (ROADMAP 11) scoped into 11a–11d; five
  design questions settled.** Planning pass only — no code. The item asked for
  a real-time version of pyatspm's `atspm video-overlay`: a camera view in the
  ntcip web UI with detector loops and stopbars recolored by live SNMP state.
  **Two owner-supplied constraints reframed the whole thing**, and both are
  worth recording because the code doesn't show them.
  **(1) The UI runs on a remote host, not the J1900.** CLAUDE.md's hardware
  constraints are written as if the whole system shares the edge box's CPU
  budget, and the first cut of this plan was built around avoiding server-side
  decode for that reason. The owner clarified that those constraints govern the
  *video buffering* path only — the GUI is always remote. That put live MJPEG
  back in scope; the remaining cost is intersection uplink bandwidth, not CPU,
  which is why 11c uses one shared decoder per URL with ref-counted subscribers
  rather than a session per viewer. The requested feature is live MJPEG **plus**
  a static-image-from-file source, and the file source isn't just a fallback:
  shape configs are calibrated pixel-exact against one still, so loading that
  still is how you verify a calibration before trusting it against live video —
  and it makes the whole feature demoable and testable with no camera reachable.
  **(2) ntcip and pyatspm must be independently distributable.** The owner's
  intent is that either can be handed to someone else standing alone, so
  importing `atspm` is out. A shared third package was considered and rejected
  as disproportionate for ~80 lines of CSV parsing. Decision: **vendor** the
  reader (`ShapeConfig.load` + the `OLA`–`OLP` overlap map), keeping the file
  format byte-compatible so pyatspm's existing OpenCV/Tkinter calibrator stays
  the authoring tool. What makes the vendoring stay small is the rendering
  choice: **client-side `<canvas>`**, with the server resolving shape→status and
  JS only recoloring. pyatspm's drawing code is then a spec to port, not a
  dependency, and none of its genuinely complex parts (the calibration state
  machine, the vectorised status lookups against recorded events) are involved —
  those exist to reconstruct state at arbitrary past timestamps, which the live
  monitors make unnecessary.
  **Module boundary held.** The web UI lives in `ntcip_monitor` but camera URLs
  live in `video_engine/intersections.json`, and CLAUDE.md forbids the two
  packages importing each other. Rather than breach it, the overlay reads a new
  `overlay` section in the UI's own `config.json` — which already duplicates the
  controller IP/port/community, so this follows an existing pattern rather than
  setting a new precedent. The owner asked for de-duplication where possible, so
  11d adds `tools/sync_ui_config.py` at the **repo root**: an offline,
  deploy-time script that derives the UI config's shared fields from
  `intersections.json`. Root placement is deliberate — it belongs to neither
  package, the same role `system_runner.py` plays as the runtime wiring layer.
  One authoring source, zero runtime coupling.
  **Facts verified during the pass, recorded so no future session re-derives
  them:** the camera stream is 720×720 h264 @ 10 fps (from
  `tests/fixtures/sample.ts`), matching `~/vid_cfg720.csv`'s recorded
  resolution — so the existing calibration targets this exact view and no
  rescaling is needed; the CSV's detector inputs (17, 24, 26, 33, 38, 46, …)
  are the same numbering space as intersection 201's detector IDs, so there is
  no channel remapping; `~/vid_cfg720.csv` is in pyatspm's **legacy** CSV format
  (per-row width/height, a `direction` column, no `name`) which current
  `ShapeConfig.load` crashes on, so the vendored reader must sniff and accept
  both; CSV colors are **BGR** (OpenCV order), so `"255,0,0"` is blue and a
  browser must reverse the triple — a bug that looks plausible on screen, hence
  an explicit test; `/api/status` already returns everything needed, so no new
  monitor plumbing; and PyAV encodes MJPEG natively, so the live path adds no
  dependency.
  **One deliberate departure from 4f.** 4f gated `/api/control/*` and left
  read-only routes open. The two camera-video routes will *not* follow that:
  proxied video is a live view of a public roadway, categorically different from
  "phase 3 is green", and an operator flipping `--web-host 0.0.0.0` would not
  expect to have published it. They reuse 4f's existing interlock —
  `_is_loopback_host()` plus the token check — while `/api/overlay/shapes` and
  `/api/overlay/state` stay open like `/api/status`.
  **Sequencing:** 11a (pure loader + status resolution, stdlib only, fully
  testable in this environment) → 11b (page, routes, file background — first
  visible result) → 11c (live MJPEG) → 11d (sync tool + calibration workflow).
  Split this way on the owner's standing preference for session-sized items:
  11a is dependency-free and green here, while everything after it needs
  `flask`/`cv2` installed, so the split also falls on the testability boundary.
  A browser-based calibrator was scoped and deferred — it would remove pyatspm
  and Tkinter from the workflow entirely, but it's a substantial build and
  belongs in its own item if wanted.

- 2026-07-31 — **ROADMAP 11a landed: pyatspm's shape reader vendored into
  `ntcip_monitor/ui/overlay/`, plus pure status resolution.** New
  `shapes.py` (`OVERLAP_LETTER_MAP`, `resolve_stopbar_target()`,
  `ShapeConfig.load()`) and `status.py` (`resolve_shape_status()` /
  `resolve_all()`), pinned by 41 stdlib-`unittest` cases in
  `ntcip_monitor/tests/test_overlay_shapes.py`. Both modules are stdlib-only
  and import nothing from the monitors, so they are fully green in this
  environment (no flask/cv2/numpy/atspm installed) — the reason the item was
  carved out this way in the first place.
  **Four deliberate deviations from the pyatspm original**, recorded so a
  future session doesn't "sync" them away. (1) The loader **sniffs row 1** and
  reads both CSV layouts — two-section (pyatspm's current writer) and legacy
  (per-row width/height, a `direction` column, no `name`), the format every
  real field calibration is in and the one pyatspm's own loader crashes on.
  Both paths funnel through one `_row_to_shape()`, so points/colors parse in
  exactly one place. (2) `validate_resolution()` **dropped** — it refuses
  rescaling a recorded video, and 11b's canvas scales safely; the
  width/height metadata is still kept, because the page sizes the canvas from
  it. (3) `save()` and the `relevant_*()` helpers dropped — pyatspm stays the
  authoring tool and nothing consumes them yet. (4) **Malformed rows are
  skipped, not fatal:** `load()` raises only `FileNotFoundError`, and a row
  with unparseable points/color/input is dropped and reported in a single
  structured WARNING naming the offending 1-based line numbers. **Why:** the
  overlay is a monitoring aid, and an operator-edited CSV losing one loop
  beats an `/overlay` page that won't render at all — the same "degrade, don't
  fail" posture the item already took for out-of-range overlaps.
  **`MAX_MONITORED_OVERLAP = 8`** lives in `shapes.py`, documented as *what
  `PhaseMonitor` actually polls, not a format limit* (the CSV permits
  `OLA`–`OLP` = 1–16). `load()` emits one WARNING naming every stopbar bound
  above it; `status.py` then reports them as `"na"` rather than raising, so
  they no longer render permanently grey with no explanation.
  **Colors stay BGR in the loader.** `"255,0,0"` is blue, as pyatspm authored
  it; reversing the triple is the renderer's job in 11b. A real-file test pins
  the as-authored order specifically so that reversal can't be "fixed"
  upstream and silently double-applied.
  **`status.py` compares state-name strings, not enums** — it accepts the
  exact `phases`/`overlaps`/`detectors` dicts `/api/status` builds, with
  **either int or str keys** (the monitors hand over int-keyed dicts
  in-process; the same payload through `jsonify` has string keys). That keeps
  it free of any monitor import and testable with plain dicts. Nothing in the
  module raises — a single bad shape must never break a 250 ms poll.
  **Two package `__init__.py` files went lazy (PEP 562):** `ntcip_monitor/`
  and `ntcip_monitor/ui/` now resolve `NTCIPMonitorApp` / `WebUI` through
  `__getattr__` instead of importing them at package-import time. Without
  this, `import ntcip_monitor.ui.overlay.shapes` drags in `pysnmp` and
  `flask`, which defeats the whole point of a dependency-free 11a. Existing
  `from ntcip_monitor import NTCIPMonitorApp` / `from ntcip_monitor.ui import
  WebUI` (run.py, examples.py) are unaffected.
  **Count correction:** the planning pass recorded `~/vid_cfg720.csv` as 38
  shapes / 39 lines. It is **37** (28 loops + 9 stopbars) across 38 lines
  (1 header + 37 rows); ROADMAP updated. Verified end-to-end: the real file
  loads to `720 720` / 37 shapes, and a hand-authored two-section equivalent
  loads to an identical shape list — proving the tolerant sniff before 11b
  depends on it.

- 2026-07-31 — **ROADMAP 11b landed: the `/overlay` page, four
  `/api/overlay/*` routes, and the file background source — first visible
  overlay.** New `ntcip_monitor/ui/overlay/source.py` (`BackgroundSource` ABC
  + `FileImageSource`) and `ntcip_monitor/ui/templates/overlay.html`;
  `web_ui.py` gained the routes, `config.json`/`run.py` the `overlay` section,
  and the dashboard header a link. A dedicated page, not a dashboard panel:
  `dashboard.html` is already 464 lines and the video wants the viewport.
  **Shapes (static) and state (polled) are split routes**, so the 250 ms hot
  payload is one array of 37 strings and every mapping decision stays in 11a's
  tested Python. `/api/overlay/state` and `/api/status` now share one
  `_build_status()` so the overlay can never drift from the dashboard.
  **The BGR→RGB reversal moved into Python** (`shapes.bgr_to_rgb()`, applied
  by the new `shapes_payload()` on the way to the wire) rather than into the
  page's JavaScript as the plan sketched. **Why:** the mistake looks entirely
  plausible on screen — red and blue are both believable loop colors — so it
  needs a unit test, and only Python has one here. The loader still stores the
  authored BGR, so the reversal happens exactly once, at the boundary.
  **Access policy: a deliberate departure from 4f.** 4f left every read-only
  route open and gated only `/api/control/*`. The two *video* routes
  (`background`, `stream`) now carry the same interlock — token set → header
  must match (401); no token + loopback → allowed; no token + non-loopback →
  403 — while `shapes` and `state` stay open like `/api/status`. **Why:**
  proxied camera imagery is a different category from "phase 3 is green" —
  it's a live view of a public roadway, and an operator who passes
  `--web-host 0.0.0.0` to reach the dashboard has not consented to publishing
  the camera. Both paths now run through one `_check_shared_secret()` so the
  policy has a single implementation. The video routes *additionally* accept
  `?token=`, because the MJPEG `<img>` of 11c cannot set a request header;
  control stays header-only, where the token can't land in an access log.
  **Canvas sizing does the scaling.** `canvas.width/height` are the config's
  `video_width/video_height` (720×720), shapes are drawn in native
  calibration coordinates, and canvas + background are stacked at
  `width:100%`. The browser scales both by the same factor, so there is no
  coordinate math anywhere in the page — and no rescale-refusal check to
  vendor (11a's dropped `validate_resolution()`).
  **Degrade, don't fail** — the same posture 11a took for bad CSV rows. A
  missing shapes CSV, an unreadable image, or `background: "live"` (11c) is
  logged and turned into a 503 on that route only; the dashboard, and the rest
  of the overlay page, still work. `FileImageSource` re-reads on mtime/size
  change (swap the calibration still without a restart), keeps serving the
  last good bytes when the file vanishes mid-copy, and ignores a zero-byte
  read — a copy in progress must not blank the operator's page.
  **The page labels its own resolution.** SNMP sampling is ~1–1.5 s effective,
  far coarser than the video, so the page says so rather than implying
  frame-accurate detection.
  **Verified three ways** (`flask` and `pysnmp` 5.1.0 were installed into this
  environment to do it, so CLAUDE.md's "Flask isn't installed" note is now
  stale and was corrected): 66 stdlib-`unittest` cases (41 from 11a + 25 new
  covering `shapes_payload`, the reversal, and `FileImageSource`); 38
  Flask-test-client checks against stub monitors covering all four routes ×
  the three access states plus every degraded config; and the shipped page
  JavaScript run under Node against the live app with a recording canvas
  context, confirming 37 strokes, the 0.2-alpha fill + white outline for an
  ON loop, 3 px G/Y/R stopbars, and `rgb(0, 0, 255)` for a `"255,0,0"`
  authored shape. Geometry was checked against a real frame extracted from
  `video_engine/tests/fixtures/sample.ts`: **the loops land on the pavement**,
  so the 2026-07-15 calibration still matches the camera's aim.
  The route-level test stays out of the repo — it belongs with **4e**'s Flask
  test-client work; the scratch harness was verification, not a deliverable.
  **`overlay/` (repo root) holds the deployment data** the shipped
  `config.json` points at: `201_fisheye_shapes.csv` (a copy of
  `~/vid_cfg720.csv`) and `201_fisheye.jpg` (the extracted still).
  `overlay.camera_url` is left **empty on purpose** — 11d's sync script
  authors it from `video_engine/intersections.json` rather than hand-copying
  a credential into a second file.
  **Seams left for 11c:** `BackgroundSource.mjpeg_frames()` is declared (yield
  raw JPEGs; `web_ui.py` owns the multipart framing, so `source.py` needs no
  Flask), `supports_stream()` gates the route, `create_background_source()`
  raises `NotImplementedError` for `"live"`, and the page already branches on
  `SOURCE_KIND`.

- 2026-07-31 — **ROADMAP 11c landed: `RtspMjpegSource` — one shared decoder
  per camera, MJPEG to every viewer.** `ntcip_monitor/ui/overlay/source.py`
  gained the live source 11b left seams for; `create_background_source()` now
  builds it for `background: "live"` instead of raising. No new dependency:
  PyAV decodes h264 and re-encodes MJPEG (`av.CodecContext.create('mjpeg','w')`),
  and the import is guarded by `try/except ImportError` so the overlay package
  still loads — and its tests still run — on an interpreter without `av`.
  **One decoder thread per source, ref-counted, not one per client.** Viewers
  attach as subscribers: an `/api/overlay/stream` generator for its lifetime, an
  `/api/overlay/background` request for the length of one frame wait. The thread
  opens lazily on the first subscriber and retires `idle_grace_sec` (10 s) after
  the last one leaves. **Why:** an idle page must cost the intersection no RTSP
  session at all, N tabs must cost one stream rather than N, and the grace
  period keeps a page reload from tearing the camera session down and rebuilding
  it. The same property makes `/api/overlay/background` work on a live source —
  it returns the latest decoded frame, so the still endpoint serves both kinds.
  **The start/stop race is closed with a per-thread liveness token**
  (`_DecoderSession`), the same shape as the generation counter on the remux
  manager's stop timers: the retiring thread commits `session.running = False`
  and clears `self._session` *under the lock*, so a subscriber arriving before
  the commit keeps the thread alive and one arriving after it starts a fresh
  thread — and a thread that is shutting down can never stop its successor.
  Lock discipline follows CLAUDE.md's remux rule: the condition is held only to
  publish a finished JPEG (a reference assignment plus `notify_all`) or to read
  bookkeeping, never across a connect, a decode, an encode, or a socket write.
  **Decode every frame, encode at `stream_fps`** (default 5, from a 10 fps
  source). Encoding is the expensive half and the shapes' own resolution is the
  ~1–1.5 s SNMP sweep, so publishing faster would buy nothing. JPEG quality is
  set through the encoder's quantiser bounds (`qmin`/`qmax`, new optional
  `overlay.stream_quality`, default 12 ≈ 56 KB/frame at 720×720) — FFmpeg's
  `-q:v`/`qscale` options are silently ignored by this encoder, which was
  verified, not assumed.
  **A stalled stream re-sends the last good frame** every `keepalive_sec` (2 s)
  rather than letting the viewer's `<img>` break, and reconnects with 1 s→30 s
  exponential backoff on both a failed open and a mid-stream drop; a clean EOF
  is treated as a drop. The page additionally retries the request itself after
  3 s on an `<img>` error, which covers the monitor restarting underneath it.
  **Verified** with 86 stdlib-`unittest` cases (66 from 11a/11b + 20 new): the
  PyAV seams (`_open_container`/`_decode`/`_encode_jpeg`) are stubbed so
  subscriber counting, shared sessions, idle teardown, re-open after teardown,
  fps capping, keepalive re-send, backoff on failed open, mid-stream reconnect,
  and `close()` are all covered on a bare interpreter, plus two real-PyAV tests
  that skip when `av` or the fixture is absent. End-to-end through the actual
  Flask routes with `video_engine/tests/fixtures/sample.ts` standing in for the
  camera (a data file — no `video_engine` import): two concurrent MJPEG clients
  received **byte-identical frames from a single container open** at ~5 fps,
  `/api/overlay/background` returned a valid 720×720 JPEG of the intersection,
  the decoder retired ~10 s after both clients left, and an unreachable
  `rtsp://` URL degraded to a 503 on that one route while `/overlay`,
  `/api/overlay/shapes` and `/api/overlay/state` stayed 200. The 11b access
  interlock still applies unchanged: on a `0.0.0.0` bind with no token, both
  video routes answer 403 and `state` answers 200.
  **`config.json` stays on `background: "file"`** — `camera_url` is still empty
  by design (11d authors it from `video_engine/intersections.json`), so
  switching to live is a two-key edit once that lands.

- 2026-07-31 — **ROADMAP 11d landed: `tools/` at the repo root — deploy-time
  config sync and a calibration-still grabber. Item 11 is complete.** New
  `tools/sync_ui_config.py` and `tools/grab_calibration_still.py`; both are
  standalone CLIs that run at deploy time and are imported by nothing.
  **Why a third top-level directory.** The values that drift — the controller's
  SNMP endpoint and the camera URL — are one fact about one intersection stored
  in two config files, because `ntcip_monitor` reads `config.json` and
  `video_engine` reads `intersections.json` and neither may import the other.
  Merging the files would break that boundary; a runtime read of the engine's
  config from the monitor would break it too. A deploy-time script that treats
  `intersections.json` as the authoring source removes the hand-copying without
  touching the runtime independence at all — the same role `system_runner.py`
  plays for the two packages at runtime, which is why it sits beside them
  rather than inside either.
  **`sync_ui_config.py` syncs five keys and refuses to guess at the rest**:
  `controller.ip/port/community/chunk_size` and `overlay.camera_url`. Poll
  intervals are **not** synced (the monitor tunes four monitors separately
  under `monitors.*`; one engine-side `poll_interval_sec` doesn't map onto
  them), nor is `timezone` (the engine's CSV log has no monitor equivalent),
  nor anything under `web_ui` (bind host, port and control token are properties
  of the machine you run the UI on, not of the intersection). **Dry run by
  default** — `--apply` writes, atomically, and re-running is a no-op. Camera
  credentials are **masked** in the printed diff (`rtsp://root:***@...`,
  `--show-secrets` to override): a deploy step that echoes a password into a
  terminal scrollback or a CI log is a small leak with no upside. Missing keys
  are reported as `(unset)` and created; the file is re-serialised at two-space
  indent, which is the trade `--apply` being opt-in pays for.
  **`grab_calibration_still.py` grabs through `RtspMjpegSource`** rather than
  its own PyAV plumbing. **Why:** JPEG encoding then lives in exactly one place,
  and a successful grab doubles as proof that the *live overlay path* can reach
  that camera, decode it, and encode from it — one fewer thing to debug when
  someone later flips `background` to `"live"`. `--settle` (default 1 s) takes a
  later frame so auto-exposure has stabilised, `--quality` defaults to 3 (sharp;
  the overlay's own default is 12) because a calibration still is drawn on, and
  `--intersection`/`--camera` resolve the URL from the same authoring source the
  sync script uses. `RtspMjpegSource.stats()` gained `resolution` to report it.
  **The calibration workflow is documented, not built** (CLAUDE.md): grab a
  still → `atspm video-calibrate-shapes --camera <name> --video <still>` →
  copy the CSV to `overlay.shapes_csv`. Note pyatspm's calibrator takes
  `--video` and uses its first frame; OpenCV normally opens a JPEG as a
  single-frame video, and `video_engine/tools/__capture_rtsp.py` records a short
  clip if it doesn't. A **browser-based calibrator** would remove pyatspm,
  Tkinter and cv2 from the workflow entirely — parked in ROADMAP's Future
  section rather than built, because `calibrate.py`'s draw/edit/undo/snap
  interaction is ~380 lines of event handling to reimplement.
  **Verified** against both real config files: 201 (one camera, auto-selected)
  and 701 (four cameras — correctly demands `--camera`); `--apply` on a copy
  produced the three expected changes, left every other section intact, and
  re-ran clean; unknown ID, missing file and missing camera URL all fail with a
  one-line message instead of a traceback. The grabber wrote a sharp 720×720
  still of the intersection from `sample.ts`, refused to overwrite without
  `--force`, and reported the unreachable real camera as a timeout, not a crash.
  **ROADMAP Item 11 was replaced by a done-stub** per the workflow rule for
  finished items, keeping the pyatspm reference map, the verified-facts list and
  the two live risks (calibration staleness, sampling floor) that outlive it.

- 2026-07-31 — **ROADMAP 4a closed: the batched SNMP sweep is verified on the
  controller. Effective sampling cycle 1.53 s → ~0.33 s; edge capture 26 % →
  94 %.** The owner pushed the post-flip round trip (10-min
  `ntcip_capture_20260731_181108.csv`, the 16:00–21:30 datZ set, and a 2 h 46 m
  engine run in `discrepancies_log.csv`); this session decoded and verified it.
  **Channel map re-confirmed** — all 27 active channels self-match under
  `__correlate_channels.py` (best score = own number, every configured detector
  `ok ✓`), so the 2026-07-19 mapping verdict still holds after the chunk-size
  change. **The sweep-speed number did not come from the capture**, and that is
  the methodological finding worth keeping: `__capture_ntcip.py` still read one
  group OID per `client.get()` in a per-group loop, so `snmp_chunk_size` never
  touched it — its cycle only drifted 1.08 s → 0.83 s on ambient RTT, and an
  edge-capture ratio computed from it (67 % edges / 72 % pulses) measures *the
  tool*, not production. The production monitor's cycle was instead recovered
  from the engine's own output: Rule 2 orphan-pulse durations in
  `discrepancies_log.csv` are quantized by the sampling cycle, and the 114
  logged pulses land on clean multiples of **~0.325 s** (0.65, 0.975, 1.30,
  1.625, …). With `poll_interval_sec: 0.2` that implies a **~0.125 s sweep**,
  matching the 2026-07-20 probe's 93 ms prediction for the 6-group production
  shape (~10× the ~1.3 s baseline sweep). The floor gate corroborates it
  independently: the shortest pulse Rule 2 accepted was 0.645 s, and the gate
  is `2.0 × floor`, so the injected floor was ≤ 0.323 s. **Edge capture at the
  production cycle is 94 % (97 % of ON pulses)**, from a phase-averaged
  resampling of the controller's own 0.1 s waveforms at 0.325 s; the same model
  predicts 71 %/79 % at the capture tool's 0.834 s cycle against 67 %/72 %
  measured, which is what validates it. Baseline for the same computation on
  the 2026-07-19 pair: 26 % edges / 27 % pulses. Target (≥ 90 %) met.
  **Two fixes landed rather than leaving the gap.** (1) `__capture_ntcip.py`
  now issues one batched `client.get(*group_oids)` per sweep — the same call
  shape as `detector_monitor._poll` — takes `--chunk-size` (defaulting to the
  config's `snmp_chunk_size`), and prints median/p95 sweep time plus the
  implied sampling cycle in its summary, so the next capture measures the
  production path directly instead of needing this inference. The simulated
  client's `get()` was widened to `*oids` to match. (2) New
  `video_engine/tools/__decode_datz.py` — datZ → `timestamp,event_code,
  parameter` CSV, expanding `.zip` bundles, with `--detectors-only` and
  `--start/--end` window filters. It calls **pyatspm's own** decoder helpers by
  file path (`_extract_header_fields` + `_parse_binary_payload`), bypassing
  `parse_datz_bytes` only because that returns a pandas DataFrame and pandas
  isn't installed here. It exists because the ad-hoc 2026-07-19 extraction
  **omitted the header's sub-minute offset**: binary offsets are measured from
  the `Controller Data Log Beginning` instant, 0.0–1.0 s past the filename's
  clock boundary, and `banks_events_20260719_1730.csv` is consequently **1.0 s
  early** throughout. That inflated the era's headline skew — the
  "+1.08 s NTCIP-behind-controller" figure in the 2026-07-19 entry re-measures
  as **+429 ms** once the offset is applied, which is the physically sensible
  answer (≈ half of that capture's 1.08 s cycle) and lines up with the +437 ms
  measured on 07-31 at a 0.83 s cycle. The committed 07-19 CSV is left as-is as
  a historical artifact; treat its timestamps as 1 s early, or re-decode from
  the datZ, which is also committed. Correct decode of the new window is
  committed as `banks_events_20260731_1811.csv`.
  **Consequence for ROADMAP 9 that outranks 4a itself:** at a ~0.33 s measured
  floor the Rule 2 gate is ~0.65 s, not the 3.2 s the 1.6 s default implied, so
  **Rule 2 is no longer effectively disabled** — it fired 114 of the 180
  triggers in the pushed run, versus zero by design under the old floor. The
  runtime floor injection did this on its own with no config change, which is
  the 9B design working as intended. 9C's prerequisite is now met and its input
  data (2 h 46 m of engine triggers + overlapping datZ) is in the repo; the
  precision/recall re-baseline is the next session, and the 114 Rule 2 triggers
  are unvalidated until it runs.

- 2026-07-31 — **ROADMAP 9C run: 3 of 4 pass criteria met. Precision 36.5 % →
  89.4 %; adjusted recall 36.5 % → 59.9 %, short of the 70 % bar — and the
  categorization says the shortfall is mostly a *logging* artifact, not a rule
  defect.** Scored the 2 h 46 m engine run (`discrepancies_log.csv`, 18:36–21:22
  on 2026-07-31, 180 triggers) against ground truth built from the same day's
  datZ. Chain: `__decode_datz.py` → `__make_gt_export.py` (new, below) → 451
  anomalies over the 17 pairs of `_intersections.json` at
  `lag_threshold_sec=5.0` → `__accuracy_report.py --poll 0.33` (the measured
  cycle). The engine ran on `_intersections.json`, **not** the other two
  intersection JSONs — its 16 triggering pairs are a subset of that file's 17
  and of neither other file's 5; scoring against the wrong pair set would have
  invented misses wholesale.

  **Against SCOPE_sampling_floor.md §Item C step 3:** rule 2 precision
  **93.9 %** (≥ 80 % ✓); **zero** stale-refire phantoms (✓); **no pair with the
  zero-correspondence signature** — all 16 triggering pairs matched at least one
  GT event (✓); adjusted recall **59.9 %** (≥ 70 % ✗ — rule 1 65.1 %, rule 2
  57.5 %). Precision is insensitive to the modelled poll (89.4 % at 0.2/0.33/0.5)
  so it is a robust number; recall is not (50.8 %/59.9 %/69.4 %), but **fails
  even at the most generous 0.5 s**, so the verdict does not hinge on that knob.

  **The 108 true misses are not the sampling floor.** The report already
  excludes 138 sub-0.7 s pulses as poll-aliased, and resampling the controller's
  0.1 s waveforms at 0.325 s shows all 108 remaining events were resolvable —
  the evidence was visible. What explains them is that
  **`discrepancies_log.csv` is a log of recordings, not of engine decisions**:
  `remux_video_buffer._handle_start` calls `_log_discrepancy_to_csv` only after
  `_writer_semaphore.acquire()` succeeds, so a trigger dropped by the
  `max_concurrent_writers=2` cap leaves no row at all. Reconstructing writer
  occupancy from the logged clips: the cap was saturated **11.6 %** of the wall
  clock, yet **46 of 108 misses (43 %)** start inside a saturated stretch,
  against **13 %** of the 315 non-missed in-coverage GT events — the control
  that rules out "misses just cluster when traffic does". A 3.3× enrichment.
  Recall measured from this artifact therefore conflates "the rule did not fire"
  with "the buffer refused the clip", and **cannot be fairly scored until the
  engine logs its own decisions separately from the recordings.** That is the
  blocking item, and it is a logging change, not a rule change.

  **All 19 false positives are boundary artifacts — not one is a detection of a
  non-event.** Three mechanisms, cleanly separated: (1) **12 of 12 rule 1 FPs**
  have durations of **5.03–5.06 s** against the 5.0 s threshold — every one
  within a fifth of a sampling cycle of it. Disagreements truly just under 5 s
  measure just over, because both edges carry up to ±0.33 s of sampling error.
  (2) **6 of 7 rule 2 FPs are on pair 26:33**, and the mechanism is exact:
  det 33 (Phase 6 Evo Radar) has a **median pulse of 0.70 s with 49 % of its
  pulses under 0.65 s**, so when GT sees a 0.1–0.6 s partner blip inside the
  ±5 s window and suppresses the anomaly by the chatter exception, the engine's
  0.325 s sampling never saw that blip, read the partner as "completely OFF",
  and fired. **This is the aliasing asymmetry Item B was written to heal, and
  the gate as built does not reach it** — it tests the *orphan's* duration
  (1.1–2.6 s here, comfortably above the 0.65 s gate) and says nothing about
  whether the *partner* is resolvable at all. A partner-side gate is the
  indicated fix; it is not written, because §Item C says categorize before
  touching rule code. (3) The remaining rule 2 FP is a rule-boundary crossing:
  a true 5.8 s pulse on det 8 that pyatspm bins as `extended_disagreement` and
  the engine, measuring 4.04 s, bins as an orphan — a mislabel of a real event.

  **Caveat that limits what this run proves:** it is an off-peak sample. Peak
  detector duty here is **32.8 %** (det 17), nowhere near the **80–94 %** that
  motivated this whole scope, so the high-duty advisory never fired and the
  false-trigger storm condition is simply **not exercised**. The 89.4 %
  precision is real but earned on an easier sample than the 2026-07-19 baseline
  (a 14:00–16:00 window), and the two are not strictly comparable. The datZ set
  does cover 16:00–18:00, but no engine ran then — closing this needs another
  ≥ 2 h engine run at peak, which is what 9C should be re-scored on.

  **Tooling:** new `video_engine/tools/__make_gt_export.py` fills the one gap in
  the chain — `__accuracy_report.py` documented that its ground truth comes
  "from the sibling pyatspm project" but no script produced it, and the step was
  being hand-rolled each time. It calls pyatspm's own `analyze_discrepancies()`
  (same three rules), reads the pairs and `lag_threshold_sec` from the
  intersection config so they cannot silently drift from the engine run, and
  refuses to guess when the config's thresholds disagree. It must run under
  pyatspm's interpreter (pandas/numpy are deliberately not dependencies here)
  and says so with the exact command when the import fails. Export committed as
  `gt_anomalies_20260731_1830-2130.csv`; regenerate with the two-command chain
  above. **9C stays open** pending the decision log and the peak-hour run.

- 2026-08-01 — **Item 9C1: the engine logs its own decisions, in a file the
  video buffer cannot suppress.** `discrepancies_log.csv` is written by
  `remux_video_buffer._handle_start` *after* `_writer_semaphore.acquire()`
  succeeds, so a trigger dropped by the `max_concurrent_writers=2` cap leaves
  no row anywhere; the 2026-07-31 measurement showed that population is not
  small or random (cap saturated 11.6 % of wall clock, but 43 % of the 108
  apparent misses started inside a saturated stretch, a 3.3× enrichment over
  the 13 % base rate). Recall computed from that artifact charges the engine
  for the buffer's back-pressure. `DiscrepancyMonitor` now appends
  **`engine_decisions.csv`** from `_log_decision`, called at the end of
  `_fire_trigger` — after the atomic Hot Folder rename, before any post-write
  state management. **Why there:** a row then means exactly "this trigger
  reached the spool directory", which is the true boundary of engine
  responsibility; logging before the rename would credit deliveries that
  failed, and logging inside the state machine would tangle measurement with
  the cooldown/active-trigger interaction the module docstring warns against
  touching.

  **The path is injected, not discovered.** `system_runner` passes
  `decision_log_path=output_dir / "engine_decisions.csv"`, defaulting to
  `None` (disabled) everywhere else — the same composition-root discipline as
  the sampling floor, and it keeps the two artifacts side by side in one
  directory so the accuracy chain has nothing to hunt for. No new config key:
  the value is fully determined by `output_dir`, and CLAUDE.md's rule is to
  add config surface only when a deployment could reasonably want it
  different.

  **Rows carry exact Unix event windows.** `event_start_ts` / `event_end_ts`
  are written as floats, so a consumer never reconstructs timing from a
  1-second local timestamp plus a regex over a human-readable description —
  which is what `__accuracy_report.py` had to do, and why it needed a `--tz`
  it could silently get wrong. Either column may be blank where the rule does
  not define it: a Rule 1 `start` knows when the disagreement began but not
  when it ends, and a `stop` describes neither. The window reaches
  `_fire_trigger` as a single optional `event_window` tuple rather than two
  scalars, because ROADMAP 4h already wants that signature *shorter*; when 4h
  builds its `TriggerSpec` dataclass this folds in as one field. It is
  deliberately **not** added to the trigger payload — the video buffer has no
  use for it, and the Hot Folder schema is intentionally expensive to grow
  (both sides plus `config_manager.py`'s canonical docstring must agree).

  **Failure is contained by contract.** A failed append logs an ERROR and is
  swallowed: measurement degrading is acceptable, a full or read-only disk
  stopping a recording is not. `_DECISION_LOG_FIELDS` is append-only for the
  same reason the header is written only to an empty file — the log survives
  restarts, so a column inserted mid-list would desynchronize a resumed file
  from its own header. The engine is the single writer (every `_fire_trigger`
  call site is on the evaluator thread), so the append needs no lock.

  **`__accuracy_report.py` takes either format**, auto-detected on the
  presence of an `event_timestamp` column and announced in its first line of
  output, with the legacy reconstruction path kept intact — re-running the
  committed 2026-07-31 artifacts reproduces that run exactly (precision
  89.4 %, rule 2 93.9 %, adjusted recall 59.9 %, zero phantoms), which is what
  makes the refactor safe to trust. `stop` rows are skipped by the decision
  parser; counting them would double every Rule 1 event. New `--recording-log`
  joins the two artifacts on `Trigger_ID` and prints a DELIVERY section: how
  many decisions never became clips, split by rule, and how many of those were
  ground-truth-matched (real events with no reviewable footage — an
  operational finding, not an accuracy one).

  **What this does *not* do:** it does not revise the 59.9 %. That number was
  measured on an artifact predating this log, and only a fresh engine run
  produces a decision log to score. It also does not record decisions *not* to
  fire — cooldown- and floor-suppressed candidates are still only counters and
  DEBUG lines, so `__accuracy_report.py` keeps *simulating* suppression
  windows and its cooldown category stays an upper bound. Logging real
  suppressions would mean adding a decision point inside `_evaluate_pair`,
  which §Item C rules out until C2 is settled. Tests: nine new cases in
  `video_engine/tests/test_discrepancy_rules.py` (59 total, all green),
  covering the exact rule 2 pulse window, Rule 1's open window, the `stop`
  row's shared trigger ID, header-once-across-restarts, the Trigger_ID join
  with the Hot Folder payload, and that an unwritable log path still delivers
  the trigger.

- 2026-08-01 — **Item 4d: the six untested pure functions are covered.** 51 new
  stdlib-`unittest` cases, no mocking anywhere (the whole point of the
  selection — every one of these is deterministic). Layout follows Item 7's
  precedent unmodified: per-package `tests/`, one file per subject, a
  `sys.path` bootstrap so each runs from any working directory. Three
  judgement calls are worth recording.

  **Where each function's tests live.** `_resolve_pytz` went into the existing
  `test_discrepancy_rules.py` rather than a new file — it is a
  `discrepancy_engine` function and that suite is already "tests for the
  discrepancy engine". `ConfigProviderError` got a **new**
  `video_engine/tests/test_config_manager.py` even though it is nine cases
  for a nine-line class: 4e's backlog lists `ConfigProvider`,
  `JsonFileConfigProvider`, and `SqliteCentralConfigProvider`, and they belong
  in that file, so creating it now means 4e adds cases instead of relitigating
  placement. The four `ntcip_monitor` functions share
  `ntcip_monitor/tests/test_oid_helpers.py` — they are all "compute an OID or
  a state from a number", one subject.

  **`test_oid_helpers.py` imports the leaf modules, not the package.** It puts
  `ntcip_monitor/core/` on `sys.path` and imports `oid_definitions` and
  `data_models` directly, because `core/__init__.py` re-exports `snmp_client`
  and would drag pysnmp into a suite that has no need of it. pysnmp *is*
  installed on this machine, so this buys nothing today — it buys that the
  suite keeps running on a bare interpreter, which is the property
  `test_overlay_shapes.py` was built around and which makes these tests usable
  as a smoke check on an edge box that has not had its dependencies installed
  yet. The alternative (stubbing pysnmp the way `test_snmp_batching.py` does)
  is more machinery for less isolation when the module under test genuinely
  has no SNMP dependency.

  **`get_output_oid`'s expectations pin what the code emits, not what the
  controller accepts.** ROADMAP 10 records that `OUTPUT_BASE`
  (`…1206.4.2.1.3.14.1.2.{1..16}`) returns SNMPv1 `noSuchName` on the Cobalt
  at 10.37.23.200 — the OID is very likely wrong. Writing tests that assert
  the current value creates a small trap: a future reader can mistake a green
  suite for evidence the OID is correct. Rather than skip the function or
  encode a guess at the right column, the test class docstring says plainly
  that these are change-detectors and that Item 10 will move them. Coverage
  and correctness are different claims; the tests should only make the one
  they can.

  **One observation, deliberately not acted on:** `_resolve_pytz` documents
  "never raises", and that holds for every string plus `None` (pytz raises
  `UnknownTimeZoneError` for both, which the function catches), but a
  non-string non-`None` `timezone` value in config — expressible in JSON —
  reaches `pytz.timezone` and raises `AttributeError`. It is contained: every
  call site is under `_evaluator_loop`'s broad `except Exception`, so the
  effect is a logged exception per tick, not a dead thread. Tightening the
  guard is a robustness change, not test coverage, so 4d left it. Tests now
  total 234 across six suites; the inventory is in CLAUDE.md's new "Tests"
  section.

- 2026-08-01 — **Item 4b: the eight confirmed unused imports are gone.** One
  mechanical pass, one line touched per site, no behavior change: `Counter32`/
  `Unsigned32`/`Gauge32` from `core/snmp_client.py`, `PhaseStatus` from
  `phase_monitor.py`, `DetectorState`/`OutputState` from `examples.py`,
  `datetime.timezone` from `video_engine/routine_scheduler.py`, `sys` from
  `main.py`, `datetime` from `utils/config_loader.py`, `Counter32`/`Integer32`
  (the whole `pysnmp.hlapi` line) from `utils/controller_control.py`, and the
  whole `data_models` line from `ui/web_ui.py`. Verified before removal that
  each name has zero references outside its import line, and after removal that
  nothing re-imports it *from* these modules — `utils/__init__.py`,
  `core/__init__.py` and `monitors/__init__.py` re-export only names that stay.
  All six suites green (234 cases), and every touched module imports cleanly at
  runtime, not just under `py_compile`.

  **Two things left deliberately in place.** `snmp_client.set()`'s docstring
  still names `Counter32` as an example `asn_type` — that is correct and stays
  correct: the caller supplies the class, so the parameter's contract does not
  depend on this module importing it. And `main.py` line 77 keeps a
  commented-out `sys.exit(1)` inside a commented-out connection test; the
  import is dead today, and reviving that block is a decision that should
  restore its own import rather than a reason to carry one. `ROADMAP 4c`'s
  three false positives (`from __future__ import annotations` in two
  `video_engine` files, the `__init__.py` re-export idiom, the "uncached
  config reads" finding) were not touched, as that item directs.

- 2026-08-01 — **Items 5 + 6 folded into one sweep: every CFR video buffer is
  deleted.** ROADMAP 6 explicitly said to do 5 first or fold both, because 5's
  standing instruction ("do not touch `video_buffer.py` — it remains the
  supported `full` backend") is exactly what 6 reopens. Doing them separately
  would have meant writing that instruction into CLAUDE.md's clutter note and
  deleting it hours later. Three files gone —
  `_old_video_buffer.py` (31 KB), `_edge_video_buffer.py` (42 KB), and
  `video_buffer.py` (547 lines) — leaving `remux_video_buffer.py` as the only
  backend. All three are recoverable from git history at commit `0c2e11b`,
  which is recorded in CLAUDE.md rather than left for someone to bisect for.

  **Deleted, not moved to `legacy/`.** ROADMAP 5 offered either. A `legacy/`
  folder inside `video_engine/` renames the clutter rather than removing it,
  and adds a directory that looks importable; the repo already set the opposite
  precedent when `video_engine/archive/` and the `* - Copy.py` backups were
  deleted outright on the same reasoning. Git history is the archive.

  **Item 6 took option (a), on evidence rather than on the recommendation.**
  Checked before deleting: no config anywhere sets `video_backend` (all four
  intersection/config JSONs, plus the two untracked ones — the only repo hit is
  a stale permission string in `.claude/settings.local.json`); nothing imports
  `video_buffer` except the deferred switch itself; and all four buffer-using
  tools (`record_clip`, `__replay_verify`, `__probe_adversarial`,
  `simulate_playback`) import `remux_video_buffer`. Option (b) — keep the switch
  for a central decoded/re-encode need — was rejected because that need is not
  real today: nothing consumes decoded pixels, and the one live decoder in the
  repo is the overlay's (`ntcip_monitor/ui/overlay/source.py`), which is in the
  *other* package and would not be served by a `video_engine` backend. The seam
  survives anyway: `ClipRemuxer`'s lifecycle is deliberately separable from its
  `_mux` write step, so a future bounded `ClipEncoder` swaps the write step
  rather than reviving CFR.

  **`_build_video_manager` still reads `video_backend`, but only to warn.** The
  obvious collapse deletes the key entirely, which would make a deployed
  `"video_backend": "full"` silently produce remux. An operator who set that
  value chose it for a reason (exact-FPS output) and should be told it is gone,
  so the key is read, ignored, and WARNed about, and recording continues. That
  is a one-branch remnant, not a switch — there is nothing to switch to.

  **Doc surgery was the bulk of the work,** because `video_buffer.py` was cited
  as the *normative* reference in four places it had no business being the
  reference for: the Hot Folder reader, the trigger-schema enforcer, the
  `_JsonFormatter` logging exemplar, and the module named in the
  "never import across packages" rule. All four now name
  `remux_video_buffer.py`. CLAUDE.md's "two backends" section became one
  backend with the RAM-unboundedness bug rewritten as the *reason for the
  retirement* rather than a live caveat; `config_manager.py`'s canonical schema
  docstring lost its "both backends" phrasing; and
  `VIDEO_BUFFER_REMUX_PLAN.md` got a dated historical note instead of an edit,
  since a completed plan document should keep describing the state it was
  written against.

  **One stale name found in passing, not acted on:** ROADMAP 4e's test backlog
  lists `VideoBufferServer`, a class that exists in no file and never existed in
  the two deleted drafts either (checked at `0c2e11b`). Left for 4e to resolve,
  since that item owns the list. Three generic "the `video_buffer` layer/package"
  phrases in `routine_scheduler.py` and `discrepancy_engine.py` docstrings were
  also left alone — they name the video-buffering half of the system, not the
  module, and editing the discrepancy engine for a docstring word is not worth
  the diff. All 234 tests green throughout.

- 2026-08-01 — **Item 9C3: the engine logs what it *declined* to do, with a
  reason column, because the accuracy report was modelling that population
  instead of measuring it.** `engine_suppressions.csv` — one row per candidate
  the engine deliberately withheld, path injected by `system_runner`
  (`suppression_log_path`, `None` disables), same best-effort contract as the
  decision log.

  **The motivating defect is a silent one.** `__accuracy_report.py:485`
  computes `aliasing_floor = 2.0 × poll` and drops every GT isolated pulse
  shorter than that from scoring — 138 events on the 2026-07-31 run, which is
  much of what "adjusted" means in adjusted recall. That floor and the engine's
  actual Rule 2 gate (`min_pulse_floor_multiple × sampling_floor`) land at
  nearly the same number today (0.66 s vs 0.65 s), which makes it easy to
  assume they select the same events. They do not: the report measures true
  durations from the controller's 0.1 s waveform, while the gate measures the
  engine's own quantized observation, which carries up to ±1 sampling cycle on
  each edge. A true 0.9 s pulse the report keeps can be gated; a true 0.5 s
  pulse the report drops can pass. So the excluded population was an
  *assertion*, unverifiable from the artifacts. Now it is a file.

  **The gate's two factors are stored separately, not just their product.**
  `sampling_floor_sec` and `min_pulse_floor_multiple` each get a column, so a
  consumer can recompute the gate at 1.0×/1.5×/3.0× and recover what the run
  *would* have evaluated — a tuning curve for `min_pulse_floor_multiple` out
  of a finished run, with no second controller session. Verified end-to-end
  against det 33's real profile (median 0.70 s, 49 % under 0.65 s): three
  suppressed pulses, sweep reporting 2/3 kept at 1.0× and 1/3 at 1.5×.

  **Recall attributed to the gate is an upper bound, and the code says so.**
  The gate sits at candidate *registration*, ahead of Rule 2's partner-overlap
  test, so a suppressed pulse never reaches that test and may well have failed
  it. Making the number exact would mean arming below-floor pulses and gating
  at fire time — a real behavior change, since `orphan_watch_a/b` hold one
  candidate each and arming junk would evict live candidates. Not done; §Item C
  says categorize before touching rule code, and this item deliberately stayed
  on the logging side of that line, as 9C1 did.

  **Three implementation choices worth recording.** (1)
  `_maybe_register_orphan` **stays a pure static method** and now *returns* the
  pulse it suppressed rather than writing anything: the 13 existing tests call
  it unbound, and I/O belongs to the caller that has the instance. Every
  non-suppression path returns `None`, including the early returns, so the
  caller cannot mistake "no candidate" for "suppressed" — pinned by a test.
  (2) The floor and multiple are captured **once in `_evaluate_pair`**, next to
  the existing "read the floor once so both slots are judged against the same
  value" comment, so the row records the gate actually applied rather than
  whatever the 60 s floor updater has moved on to by log time. (3) The
  header-only-on-a-new-file logic was extracted into a shared
  `_append_csv_row`, used by both logs — the restart-safety behavior that makes
  an append-only column list matter is now impossible to fix in one file and
  not the other.

  **`reason` is a plain string on purpose.** `__accuracy_report.py` also models
  cooldown suppression, Rule 2 verdicts discarded past
  `_ORPHAN_DECISION_GRACE_SEC`, and `suppress_high_duty_pairs`; each becomes a
  new value in this column, not a new file. So does 9C4's cross-pair duplicate
  rejection, scoped in ROADMAP the same day.

  21 new cases (67 → 88 in `test_discrepancy_rules.py`, 234 → 255 total), all
  six suites green. The consumer half — `--suppression-log` in
  `__accuracy_report.py` — is deliberately *not* built yet: it scores an
  artifact that does not exist until the C2 peak-hour run produces one, and
  writing a reader against imagined data is how the ground-truth chain grew its
  last two mismatches.

- 2026-08-01 — **Item 9C4 scoped (not built): cross-pair duplicate triggers.**
  The owner's 3-way comparisons are expressed as a *ring* of single
  `paired_detector_id` links (46→17, 17→2, 2→46), which `_build_pairs`
  normalizes into the 3 edges of a triangle — 5 such triangles plus 2 plain
  pairs make up `_intersections.json`'s 17. The engine has no notion of the
  group, so one physical event where B disagrees with both A and C fires on AB
  *and* BC, often in the same evaluator tick (all pairs are evaluated
  sequentially within a tick; the deciseconds the owner observed are the cases
  that straddle two 0.1 s ticks). Each duplicate burns one of only 2 writer
  slots — the same back-pressure that corrupted the 2026-07-31 recall
  measurement — so dedup plausibly *raises* measurable recall.

  Design recorded in ROADMAP: connected components in `_build_pairs`, a
  per-group `last_fire_ts` checked before the tmp-write in `_fire_trigger`, and
  on a hit skip the trigger file but still call `_log_decision` with a marker.
  That "log it but don't record it" shape is only expressible because 9C1
  split the engine's record from the buffer's; before it, the only log was
  written downstream of the writer semaphore.

  **Owner requirement added the same day: a group must be configurable both
  ways** — explicitly, with `paired_detector_id` as a list (A: `[B,C]`,
  B: `[A,C]`, C: `[A,B]`), and implicitly, as today's ring of scalars summed
  into a group. The two need no separate code path: pairs are the union of
  normalized links (the loop just has to accept a list as well as a scalar),
  groups are connected components over the resulting graph, and for n=3 both
  forms yield the identical 3 pairs. Three consequences recorded rather than
  assumed: (1) that equivalence is an artifact of n=3 — a 4-ring gives 4 edges
  where an explicit all-pairs list gives 6 — so a group is defined as a **dedup
  scope only**, never as an instruction to evaluate all internal pairs, or a
  4-ring config silently grows comparisons nobody configured; (2) connected
  components can over-group transitively, one stray link merging two intended
  groups with no error, so the derived groups get logged at startup and a group
  spanning more than one `phase` gets a WARNING — checked against
  `_intersections.json`, all 7 current groups are phase-coherent, so the guard
  is quiet today and fires only on a real mistake; (3) the list form has to
  land in `__make_gt_export.py:_load_pairs` (still scalar-only at `:72`) at the
  same time as in `_build_pairs`, because that tool reads pairs from the
  intersection config specifically so ground truth cannot drift from the engine
  run — teaching one side list form and not the other makes the export cover
  fewer pairs than the engine ran, scoring every trigger on a missing pair as a
  false positive. Same failure mode CLAUDE.md already warns about, reached by a
  new route.

  **Sequenced after C2 for a measurement reason, not a risk one.**
  `__make_gt_export.py` runs pyatspm's `analyze_discrepancies()` per pair, so
  ground truth contains the identical AB/BC duplication. Suppressing one side
  without simultaneously teaching `__accuracy_report.py` to credit the
  suppressed row scores it as a **miss** — corrupting the exact number C2
  exists to establish, and changing the instrument and the subject in the same
  run with no baseline for either. Running C2 first also *measures* the
  duplicate population (same-group rows in the decision log within N seconds),
  so the dedup window comes from data rather than a guess.

- 2026-08-01 — **ROADMAP 9C2 run and scored: Item 9C passes all four criteria,
  and controller clock skew nearly hid it.** The owner delivered a 3.3 h engine
  run (13:27–16:47, 523 `start` decisions) plus a matching 15-file datZ pull.
  Scored per the §Item C protocol: **overall precision 96.5 %** (500/518),
  **rule 1 precision 96.8 %** (268/277), **rule 2 precision 96.3 %** (232/241,
  criterion ≥ 80 % ✓), **adjusted recall 86.2 %** (criterion ≥ 70 % ✓), **zero
  stale-refire phantoms** (✓), and **no zero-correspondence pair** (✓ — the
  worst is 26:33 at 7 matched / 14 triggers). This supersedes the 2026-07-31
  off-peak run's 89.4 % / 59.9 %.

  **The high-duty condition was genuinely exercised this time**, which was the
  entire point of C2. Reconstructing ON intervals from the controller's own
  0.1 s record: **14 of 17 pairs peak above the 0.80 advisory threshold over a
  rolling 120 s window, max 0.966**, against 32.8 % on 2026-07-31 and the
  80–94 % that motivated the scope. Note precision went **up** under load, not
  down — the false-trigger storm the sampling-floor work was defending against
  did not materialize at the post-4a ~0.33 s cycle. Caveat recorded: the run's
  engine *log* was not in the bundle, so the literal acceptance signal
  (`grep "sampling-reliability regime"`) could not be run; the duty numbers
  come from the controller's record, which is authoritative for the physical
  condition but is not proof the engine's own advisory fired. Next bundle
  should include the log.

  **The finding that matters most for anyone repeating this: the two sides are
  stamped by different clocks and nothing syncs them.** The engine uses the
  monitoring machine's `time.time()`; the ground truth is stamped by the
  Econolite controller. On this run the controller ran **+4.49 s ahead** — it
  was ~0 s on 2026-07-31, so this is not a constant of the deployment. Scored
  as-is, that exceeds `__accuracy_report.py`'s 3.0 s match tolerance and the
  report says **overall precision 11.6 %, rule 1 precision 0.4 %** — which
  reads exactly like a catastrophic regression and is nothing of the kind. It
  was caught from the shape of the output rather than the magnitude: every
  candidate false positive reported nearly the *same* `nearest GT Δ` (4.0–5.2 s),
  while the per-pair table still showed healthy trigger and GT counts on the
  same pairs. A real accuracy collapse does not produce a constant offset.

  The skew was then measured independently of any rule semantics, by comparing
  **engine-observed detector edges against the controller's raw 82/81 codes** —
  `engine_suppressions.csv` and the rule-2 rows of `engine_decisions.csv` carry
  exact Unix ON/OFF windows, which is a use for the C1/C3 logs nobody
  anticipated when they were built. A global-offset scan peaks sharply at
  **+4.45 s** (52.9 % of engine edges within 0.25 s of a controller edge, vs
  6.5 % at zero offset), the per-detector medians agree across all 12 detectors
  (4.42–4.59 s, sd ≈ 0.25), and there is no monotonic drift across the run
  (±0.35 s wander). Refined estimate **+4.49 s**.
  `__accuracy_report.py` gained a `--clock-offset` flag and a docstring section
  on the signature. **The corrected result is insensitive to the exact value**
  — offsets of 3.5 through 5.5 s all score identically, since the offset only
  has to land inside the tolerance — so the flag is a coarse correction, not a
  calibration; what matters is not leaving it at zero. Verified the committed
  2026-07-31 artifacts still score 89.4 % / 93.9 % / 59.9 % unchanged.

  **Residual populations, categorized before touching any rule code** (per
  §Item C's instruction). 18 candidate FPs: 9 rule 1, 9 rule 2, and **7 of the
  18 are pair 26:33** — the same pair that dominated 2026-07-31, now as
  1.32–1.94 s orphan pulses on det 26 rather than det 33. It is also the only
  pair with poor precision and *zero* true misses, which confirms the
  asymmetric-floor-gate diagnosis: the gate bounds the orphan's duration but
  says nothing about whether the partner is resolvable. A partner-side gate is
  now the highest-value rule change available (~96.5 % → ~97.8 % overall).
  80 true misses: 65 `isolated_pulse` (45 of them ≤ 1.0 s, i.e. within ~3
  sampling cycles) and 15 `extended_disagreement`.

  **A previously recorded finding was corrected here.** The 2026-07-31 note
  "all 12 of 12 rule 1 FPs measured 5.03–5.06 s against the 5.0 s threshold,
  so `threshold + one cycle` hysteresis would remove every one" is not
  diagnostic. All 9 rule 1 FPs again measured 5.033–5.081 s — but **all 281
  rule 1 triggers measure ≤ 5.1 s**, because the engine fires the instant the
  disagreement crosses the threshold, so `disagreement_sec` is the duration
  *at fire time*, not the event's true length. The statistic describes every
  rule 1 trigger and separates FPs from nothing. Judged properly against
  ground-truth durations, the 15 true-missed `extended_disagreement` events run
  5.1–8.0 s (median 5.4, **9 of 15 ≤ 5.5 s**), so firing at 5.33 s would push a
  meaningful share of genuine events under the bar to remove 9 FPs out of 518
  triggers. Recorded as net-negative pending a proper GT-duration derivation.

  **C4's duplicate population is now sized rather than guessed**, which was the
  stated reason for sequencing it after C2. Connected components over
  `_intersections.json`[201] derive **exactly the 7 groups the design
  predicted** (5 triangles + `26:33` + `29:43`), all phase-coherent, so the
  over-grouping WARN is clean on today's config. Of 523 `start` decisions,
  **103 land on the same group at the identical 0.1 s tick** and **137 (26.2 %)
  fall within a 1.0 s window** (rule 1: 84, rule 2: 53); 2.0 s catches 162 and
  5.0 s catches 181, so the curve flattens after ~1 s and **1.0 s is the window
  to implement**. The payoff is concrete: the `max_concurrent_writers` cap
  dropped **174 decisions (33.6 %)**, 167 of which were ground-truth-matched
  real events with no reviewable clip, so removing ~137 duplicate starts
  recovers most of that contention.

  Floor-gate cost, from the first real `engine_suppressions.csv`: 998 rows over
  **710 distinct pulses**, median duration **0.34 s** — sub-cycle blips at a
  ~0.33 s sampling floor, not lost signal. The C3 decision to store
  `sampling_floor_sec` and `min_pulse_floor_multiple` as separate columns paid
  off immediately: the counterfactual is recoverable without another controller
  session, and at `min_pulse_floor_multiple=1.5` **368** of those rows would
  have passed the gate (669 at 1.0). Also confirmed the floor injection works
  in production — 30 rows carry the 1.6 s startup default and every row
  thereafter carries the measured cycle (median **0.3289 s**, p95 0.3467 s),
  matching the ~0.33 s figure 4a established.

  Artifacts committed for reproducibility, following the 2026-07-31 precedent:
  `engine_decisions_20260801.csv`, `engine_suppressions_20260801.csv`,
  `discrepancies_log_20260801.csv`, `banks_events_20260801_1300-1645.csv`,
  `gt_anomalies_20260801_1300-1645.csv`. Reproduce with
  `__accuracy_report.py engine_decisions_20260801.csv
  gt_anomalies_20260801_1300-1645.csv --recording-log
  discrepancies_log_20260801.csv --poll 0.33 --clock-offset 4.49`.

- 2026-08-01 — **Item 9C4: cross-pair duplicate triggers are rejected within a
  derived detector group.** With 3-way comparisons, one physical event in which
  B disagrees with both A and C fires on pair `A:B` *and* pair `B:C`, usually
  on the same 0.1 s evaluator tick — two clips of the same moment, each burning
  one of only `max_concurrent_writers` (default 2) writer slots. Sized from the
  2026-08-01 decision log rather than guessed (that sequencing was the point of
  putting C4 after C2): **137 of 523 start decisions, 26.2 %**, while the
  writer cap dropped 174 decisions (33.6 %).

  **Groups are derived, not configured.** `_build_structures` now computes the
  connected components of the pair graph (`_build_groups`), so a group is every
  detector reachable from another through `paired_detector_id` links. The two
  authoring styles the owner asked for unify with no separate code path:
  `paired_detector_id` accepts a **scalar or a list**, pairs are the union of
  all normalized links, and groups are the components over the result. For
  n = 3 an explicit list (A `[B,C]`, B `[A,C]`, C `[A,B]`) and today's ring of
  scalars (A→B, B→C, C→A) produce the *identical* 3 pairs. **Why the two forms
  needed unifying rather than choosing between:** the list says "compare all of
  these", the ring says "this cycle", and from n = 4 they genuinely differ (4
  edges vs 6). Both are legitimate, so a group is a **dedup scope only** and
  never an instruction to evaluate every internal pair — pair generation stays
  link-driven, or a 4-ring config would silently grow two comparisons nobody
  asked for. Pinned by two tests that assert exactly that (4-ring → 4 pairs,
  4-list → 6 pairs).

  **Verified against `_intersections.json`[201]:** the derivation returns
  exactly the 7 groups predicted — the 5 triangles `[2,17,46] [3,30,41]
  [4,22,39] [7,24,38] [8,31,42]` plus `[26,33]` and `[29,43]` — all
  phase-coherent. Replaying the implemented algorithm over the run's decision
  log reproduces the sizing exactly (137 / 26.2 %, rule 1: 84, rule 2: 53, and
  162 at 2.0 s / 181 at 5.0 s), which is the check that the shipped anchor
  semantics match the ones the window was chosen under.

  **Four properties are load-bearing, and each exists for a failure it
  prevents:**

  1. *The window is anchored on emitted starts only.* A suppressed row never
     updates the group's last-fire stamp. **Why:** anchoring on suppressions
     would roll the window forward through a storm and suppress unboundedly.
  2. *Cameras are part of the dedup key.* **Why:** two pairs in one group that
     resolve to different `camera_id`s cover different footage and are not
     duplicates of each other.
  3. *A `stop` is never suppressed and never anchors.* **Why:** suppressing it
     strands a recording until the `max_duration_sec` cap; letting it anchor
     would suppress the next genuine group event.
  4. *A suppressed Rule 1 `start` must not arm `active_trigger_id`.* This was
     the fiddly part flagged in the item text: the resolution state machine
     later sends a `stop` reusing that ID, so arming it for a trigger the
     buffer never received would send a stop for a recording that does not
     exist. A suppressed start engages the pair cooldown instead — exactly what
     a Rule 2 start does — which also stops the pair re-firing on the same
     physical event one threshold later. Note this is *less* suppressive than
     the pre-9C4 behavior, not more: a duplicate Rule 1 start used to hold its
     pair through resolution + post-roll + cooldown, so no recall is lost by
     the change.

  **The suppressed decision is logged, not dropped**, with three append-only
  columns (`dedup_group`, `suppressed_as_duplicate`,
  `duplicate_of_trigger_id`). **Why it went in the decision log rather than
  `engine_suppressions.csv`**, whose `reason` column was designed to absorb
  exactly this kind of population: a duplicate is a fully-formed trigger with
  its own ID that the engine decided about *after* the rules fired, not a
  candidate it declined to evaluate — and ground truth contains the same event
  on both pairs, because `analyze_discrepancies()` is per-pair too. A consumer
  that never saw the row would score the sibling pair's GT event as a **miss**,
  corrupting the very recall number C2 established. `__accuracy_report.py`
  therefore scores these rows like any other trigger and excludes them only
  from the DELIVERY section (a duplicate has no clip by design, not by
  back-pressure). Confirmed end-to-end: re-scoring the 2026-08-01 log with the
  duplicates marked returns **identical** precision 96.5 % and adjusted recall
  86.2 %, with 136 rows moved out of DELIVERY's undelivered count.

  **The schema change landed in all three places** it had to, per the item's
  warning that missing one breaks measurement silently: `_build_structures`,
  `config_manager.py`'s canonical schema docstring, and
  `__make_gt_export.py:_load_pairs`. If the engine had learned list form and
  the export had not, every trigger on a pair missing from the export would
  score as a false positive — the "scoring against the wrong pair set" failure
  arriving by a new route.

  **Over-grouping guard:** one stray link transitively merges two intended
  groups with no other symptom, so a derived group spanning more than one
  `phase` logs a WARNING, and the derived groups are logged at startup the way
  `_pairs` already were. Clean on today's config; it fires only on a genuine
  mistake.

  Window default `dedup_window_sec: 1.0` (config, `0` disables); a malformed
  value falls back to the default rather than disabling, so a typo cannot
  silently restore the duplicate storm. Tests: 32 new cases in
  `test_discrepancy_rules.py` (88 → 120), covering both config forms, the
  n = 3 coincidence and the n = 4 divergence, the over-grouping WARN, all four
  load-bearing properties, and one end-to-end triangle event that produces two
  decision rows and one clip.

  **Not measured, and deliberately so:** the actual recovery in delivered
  clips. Marking the historical log is a scoring simulation, not a
  counterfactual — a suppressed start also engages cooldown, which changes
  what the engine does next. The real number needs a fresh run.

  **Item 9 is closed in full** with this sub-item: A (runtime sweep-time
  self-measurement, 2026-07-30), B (sampling-floor gating, 2026-07-30), C (the
  post-4a re-baseline, passed 4 of 4 on 2026-08-01), C1 (decision log), C2
  (high-duty re-score), C3 (suppression log) and C4 (this entry) all landed —
  see their own entries above. The two rule-level findings the re-baseline
  surfaced (Rule 2's asymmetric floor gate, Rule 1's absent hysteresis) are
  *not* part of it: they are rule changes, were explicitly gated behind the
  measurement work, and carry forward as ROADMAP Item 12.

- 2026-08-01 — **9C4 follow-up: the duplicate's `stop` is an AND, not a drop.**
  Raised by the owner immediately after 9C4 landed, and correct: dropping a
  duplicate *start* is safe because the sibling pair is already recording that
  instant, but the *stop* is not symmetric. If pair `A:B` owns the recording and
  resolves at t+4 while the folded pair `B:C` keeps disagreeing until t+30, the
  clip ends before the event it was suppressed for is over — the pair that
  happened to fire first would decide, arbitrarily, when the footage ends. The
  original 9C4 implementation had exactly that hole: `held` pairs went into
  cooldown and the owner's resolution state machine tested only its own two
  detectors.

  **Fix:** a suppressed duplicate registers on the owner's `held_pair_keys`
  (and records its owner in `held_by_pair_key`), and the owner's resolution
  test becomes `own detectors agree AND every held pair agrees`. A
  re-divergence on *any* participant restarts the post-roll countdown, exactly
  as one on the owner already did. Three supporting behaviors, each for a
  concrete failure:

  * A held pair runs **no rules at all** while held — a new guard 0 in
    `_evaluate_pair`, placed *ahead* of the cooldown guard because
    `_maybe_reset_cooldown_early` can clear a cooldown from the callback thread
    and a held pair must stay quiet regardless. It also clears
    `disagreement_start` as it goes, for the same reason the high-duty
    suppression path does: a stale timer would fire the instant the hold lifts.
  * On `stop`, held pairs are **released into a fresh cooldown**, not straight
    back into service — otherwise each one re-fires on the tail of the footage
    that was just recorded, recreating the duplicate this mechanism removes.
  * `_held_pairs_agree` prunes held pairs whose detectors vanished in a reload:
    a pair that cannot resolve would otherwise hold the clip to the
    `max_duration_sec` cap.

  **Two asymmetries fell out of the fix, and the run data decided both.**
  Cross-tabulating the 137 duplicates by (owner rule → duplicate rule) gives
  82 rule1→rule1, 51 rule2→rule2, 2 rule1→rule2 and 2 rule2→rule1:

  1. **A Rule 1 start is never folded into a Rule 2 recording.** A Rule 2
     clip's length is fixed at fire time and no `stop` is ever sent for it, so
     it cannot be held open; folding an open-ended Rule 1 event into one would
     truncate it to a length chosen for a brief pulse. It fires its own
     recording, which it can close itself. Measured cost: **2 of 137**. The
     alternative — transferring ownership via the Hot Folder's `extend` action
     — was rejected as a new mode (a pair's `active_trigger_id` referring to
     another pair's trigger, plus a guessed new duration) for 0.4 % of cases.
  2. **A Rule 2 duplicate never holds anything open.** An orphan pulse is
     complete before Rule 2 even evaluates it, so there is nothing to wait for;
     holding on one would extend a clip for a disagreement already over.

  **Restated sizing:** the duplicate *population* in a 1.0 s group window is
  still 137 of 523 starts (26.2 %) — that is what 9C2 measured and what picked
  the window — but the shipped code suppresses **135 (25.8 %)**, letting the 2
  rule2→rule1 cases through. Anyone replaying the 2026-08-01 log against this
  implementation should expect 135; the figures are not in conflict.

  **What this deliberately does not fix:** a detector stuck ON already holds
  its own Rule 1 recording open indefinitely, and the AND extends that stall to
  the held pairs. The exposure is widened, not created, and the *footage* stays
  bounded because the buffer auto-stops at `max_duration_sec` — what stalls is
  pair-level state, not disk. A time-bounded forced stop would change Rule 1's
  existing behavior and is out of scope here.

  Tests: 11 more cases (120 → 131) in a new `TestHeldPairResolution` class that
  drives real detector callbacks through a triangle — including the property in
  time (the stop's timestamp is after the *held* pair resolved, not the
  owner's), the re-divergence reset, the no-rules-while-held guard proven with
  the cooldown forcibly cleared, and both vanishing-detector paths. Two 9C4
  tests were rewritten: they had asserted the "suppressed Rule 1 start doesn't
  arm the state machine" property using a rule2→rule1 setup that is no longer a
  suppression at all.

- 2026-08-01 — **Automated duplicate-clip cleanup: a clip wholly contained in
  another is deleted and its log rows repointed** (`video_engine/video_cleanup.py`,
  new; scoped and shipped in one session as a follow-on to 9C4). 9C4 stops the
  *engine* firing twice for one event **within a detector group**. It cannot
  stop two clips of the same moment reaching disk when the overlap is not a
  group duplicate — and by its own deliberate asymmetries it never will: a
  Rule 1 start is explicitly not folded into a Rule 2 recording, a Rule 2
  duplicate holds nothing open, and neither mechanism sees two *unrelated*
  pairs disagreeing about the same approach seconds apart or a hand-dropped
  trigger over live footage. This is the disk-side half of the same idea.

  **Sized against the committed 2026-08-01 artifacts before writing it, not
  after.** Reconstructing clip spans from `engine_decisions_20260801.csv`
  (start `event_timestamp` − 5 s pre-roll → the matching stop, or +
  `max_duration_sec` for the 152 rule 2 clips that never get one) for the 348
  triggers that actually recorded: **91 clips (26.1 %) are wholly contained in
  another, ~41 min of duplicate footage.** Attributing them, 68 are the same
  population 9C4 now rejects upstream (same derived group, starts < 1.0 s
  apart), leaving **23 (6.6 % of recorded clips, ~11 min)** that only this
  sweep can catch. That residual — not the headline 26 % — is the honest
  justification, and it is the number to re-measure on the first post-9C4 run.

  **A clip's span is recovered from the finished file, not recorded during
  writing.** Nothing in the recording path persists start/end wall-clock times,
  and adding that would have meant either a fourth per-clip artifact or a new
  column in a log written at *start*, before the end is known. Instead:
  `end_ts` = the file's **mtime** (`ClipRemuxer._finalize` closes the container
  as its last act), `duration` = the container's own duration via PyAV — exact,
  because clip length equals the source PTS span by construction, the one thing
  the remux design guarantees and the CFR path never could — and `start_ts` is
  the difference. The failure mode this admits is a rewritten mtime, so the
  span is cross-checked against the **dispatch epoch already encoded in the
  filename**; a disagreement over 5 s skips the clip rather than judging it.
  Files whose names don't parse as clips are not candidates at all, which is
  what makes the three CSV logs and any hand-named export in `output_dir` safe
  by construction rather than by an exclusion list.

  **`plan_removals` is a single pass over `(start asc, end desc, name)` against
  a running list of survivors** — deliberately not a fixpoint over all pairs.
  The pass buys three properties outright: a keeper is never itself deleted
  (so no rewrite can name a file a later step removes, and chains need no
  resolution — in a 3-deep nest both inner clips repoint to the *outermost*),
  mutual containment resolves deterministically, and it is conservative. It
  will keep a clip that starts 2 s before a much longer one even though the
  longer clip nearly covers it, because it is not *contained*. A tolerance
  (default 0.5 s) is applied to both bounds for the one case that needs it —
  two clips of the same moment differing by poll latency; at 0.0 the same run
  yields 31 removals instead of 91, at 1.0 only 95, so the curve is flat past
  0.5 and the slack is buying real duplicates, not eroding the rule.

  **Logs are rewritten before the file is deleted**, and if a rewrite raises,
  nothing is deleted that sweep. The reverse order leaves a row naming a file
  that is gone; this order at worst leaves a row naming a clip that exists and
  still contains the event, and the next sweep retries with the rewrite already
  idempotent. Which logs get rewritten is one table (`REFERENCE_COLUMNS`) —
  today `discrepancies_log.csv` / `Video_Filename`, the only artifact in the
  tree naming a clip file; `engine_decisions.csv` and `engine_suppressions.csv`
  are written before a clip exists and carry no filename. The rewrite is a
  read-modify-`os.replace`, the same write-then-rename discipline as the Hot
  Folder, under a `_csv_lock` now shared with `_log_discrepancy_to_csv` so a
  concurrent append can't be lost.

  **Deletion is audited, because it is the one irreversible thing this system
  does.** `video_cleanup_log.csv` joins the other three in `output_dir`, one
  row per deletion carrying *both* spans, the reclaimed bytes and the rows
  repointed — enough to re-check the containment decision after the evidence is
  gone. `_CLEANUP_LOG_FIELDS` is append-only for the same reason as the
  engine's two logs. Note this makes CLAUDE.md's "three logs" four, but not a
  fourth *accuracy* log: it records deletions, never decisions, and
  `__accuracy_report.py` neither reads it nor is affected by the rewrite (rows
  are repointed, never added or removed, and scoring keys on timestamps).

  **Enabled by default** (`video_cleanup` block, `interval_sec` 300,
  `tolerance_sec` 0.5, `min_age_sec` 60). Automation nobody has to turn on was
  the point of the request; the safety is in the conservatism above plus two
  independent in-flight guards — the manager's live view of active + draining
  writers (`_protected_clip_paths`, authoritative in-process) and the mtime age
  check (which also covers clips left behind by a crashed run). The manual CLI
  (`tools/cleanup_clips.py`) inverts the default and is **dry-run until
  `--apply`**, following `tools/sync_ui_config.py`'s precedent.

  Ownership sits with the video buffer, which knows what is still being written
  and owns `discrepancies_log.csv`; the sweep runs on its own daemon thread and
  is stopped before the writers finalize on shutdown. `video_cleanup.py` imports
  neither `ntcip_monitor` nor `remux_video_buffer` — the manager imports *it*,
  one direction — and PyAV is imported lazily inside `probe_duration_sec`, so
  the module and its tests load on a bare interpreter.

  Tests: new `video_engine/tests/test_video_cleanup.py`, **44 stdlib
  `unittest` cases** (298 → 342 across seven suites), with the duration probe
  stubbed so the suite needs neither PyAV nor real video — clips are ordinary
  files with a clip-shaped name and an `os.utime`'d mtime, which is exactly the
  span model. Covers the keeper-never-deleted invariant, the chain case, the
  dispatch cross-check, the rewrite-fails-so-nothing-is-deleted path, and that
  the sweep takes the shared CSV lock. Separately verified end-to-end against
  real MPEG-TS written by PyAV (a 180 s clip and a 30 s clip cut from
  `tests/fixtures/sample.ts`, plus a same-window clip on a second camera):
  dry run reported the plan, `--apply` deleted only the contained clip,
  repointed its `discrepancies_log.csv` row, left the other camera alone, and a
  second run was a no-op.

- 2026-08-03 — **The 2026-08-02 (Sunday) full-day run measured. Headline
  finding: `__accuracy_report.py` matches on GT *start time* only, so a
  trigger that fires mid-event is scored as a false positive — this, not an
  engine regression, is most of the apparent 96.5 % → 91.3 % precision drop.**
  First run with 9C4 and the cleanup sweep both live: 11.9 h (09:39–21:35
  site), 1553 starts, 877 clips, 2808 floor suppressions, 190 sweep deletions
  — 3× the 2026-08-01 sample. Scored with `_intersections.json` (17 pairs, the
  config the run actually used — `video_engine/intersections.json` defines only
  5 and would have invented misses; the decision log's `pair_key` set is the
  cheap way to tell which config a run used).

  **The matcher defect.** `_match` is "one-to-one nearest-*start* matching per
  (pair, type)" with `diff <= tolerance` (3.0 s). The engine's rule 1 does not
  necessarily fire at the disagreement's start: after a cooldown, or when it
  picks the disagreement up mid-run, it fires deep inside an event that ground
  truth records as a single long anomaly. On this run **44 of the 135 listed
  FPs fall inside a GT anomaly window on the same pair**, the engine's own
  `event_start_ts` sitting a median 38 s past the GT start, p90 58 s, max 65 s
  — and the durations match
  exactly (engine measures a 62.9 s mismatch; the GT row is a 62.9 s
  `extended_disagreement` starting 53 s earlier). Counting an event start inside
  `[gt_start − tol, gt_end + tol]` as a match gives **91.3 % → 94.1 %** on
  08-02 and **96.5 % → 96.9 %** on 08-01. *(Figures corrected later the same
  day, when Item 13 was implemented and the patched tool reported the real
  numbers: this paragraph first read 62 / 46 % / 95.3 % / 97.5 %, computed
  without respecting the matcher's `(pair, type)` scoping — it counted rule 2
  orphan triggers landing inside rule 1 disagreement windows, which is a
  different claim, not a match. 44 is the type-respecting figure. The
  correction is recorded rather than silently applied because the wrong numbers
  were committed and pushed in `57a1359`.)* The artifact is volume-dependent —
  it costs 2.8 points on a 12 h Sunday against 0.4 on a 3.75 h Saturday,
  because long disagreements are what it mis-scores — so it is *not* a
  constant that cancels between runs, and every precision figure recorded
  before today is a floor. Fixing the matcher is ROADMAP 13, done the same day.

  **Ruled out, in order, before landing on the matcher** (recorded so nobody
  re-runs them): clock skew — the FP nearest-GT deltas are median 117.9 s with
  only 1 of 135 inside 5 s, the opposite of the skew signature; time-of-day and
  duration — restricting 08-02 to 08-01's window gives 91.4 %, versus 91.3 %
  for the whole run; **pair mix** — re-weighting 08-02's per-pair precision
  onto 08-01's trigger mix gives 92.0 %, and the reverse 96.6 %, so the
  directional shift is worth ~0.7 points; **9C4** — duplicates score 91.9 %
  against non-duplicates' 91.1 %; and detector health — per-detector sub-floor
  blip rates move ≤ 2.8 points either way.

  **Controller clock skew drifts *within* a run; it is not a per-run
  constant.** Two independent estimators (nearest-neighbour, and
  cross-correlation over ±20 s at 0.05 s steps, which does not alias when the
  offset approaches the ~3.2 s median inter-edge gap) agree: −0.30 s at
  09:39, +2.2 s by 18:15, +1.2 s at 21:35 — ~2.5 s peak-to-peak, no step.
  Best single scalar +0.75 s, max residual ~1.45 s, inside the 3.0 s
  tolerance, so one `--clock-offset` is still safe *here*; on a run that
  wanders further it would not be. The 24-minute hole at 11:12–11:35 is a VPN
  outage, correctly handled as two coverage blocks. Note the monitoring
  machine runs **PDT** while the site is **MDT** — `datetime.fromtimestamp()`
  in an ad-hoc script prints an hour behind the site-local times the report
  and the datZ filenames use.

  **The traffic genuinely differed, as the owner predicted, and it explains
  the trigger distribution but not the precision delta.** Share of all
  detector activations: ph6 (SB) 22.1 % → 31.7 % (**+9.6 pts**) and ph7 (WB)
  12.1 % → 12.9 % were the only phases to gain; ph8 −3.3, ph2 −2.8, ph3 −2.3,
  ph1 −1.9. Pair-level volume change vs precision change is r = +0.19 — no
  relationship. Two corrections to earlier readings fell out: 26:33 (the 12A
  poster child) was **50.0 % on 08-01** (7/14 — CLAUDE.md's own figure) and
  *improved* to 62.2 % on the day its phase gained the most volume; and 7 of
  17 pairs carried under 15 triggers on 08-01, five of them reading "100 %"
  on 1–9 triggers, so that baseline was thinner per-pair than its headline
  suggests.

  **Delivery and cleanup, the other two things the status block wanted
  re-measured.** The writer-cap decision loss fell **33.6 % → 20.0 %** (219 of
  1553), so 9C4 recovered roughly 40 % of it. The cleanup sweep removed **190
  of 877 clips (21.7 %, 93 min, 371 MB)** against a predicted 6.6 % residual;
  the prediction was wrong because it was sized on a 3.75 h run. Breakdown:
  **139 (73.2 %) different-group** — the population only the sweep can catch,
  which grows with run length as unrelated pairs overlap the same approach;
  30 (15.8 %) same-pair cooldown re-fires, never a 9C4 case; and **21 (11.1 %)
  same-group, different pair, which 9C4 should have caught**. The last is a
  real gap: `dedup_window_sec` defaults to **1.0 s** but the median clip is
  **24.4 s**, so two same-group starts a few seconds apart escape dedup and
  still produce a wholly-contained clip. That is ROADMAP 14.

  Artifacts committed: `banks_events_20260802_0930-2229.csv`,
  `gt_anomalies_20260802_0930-2229.csv`, `engine_decisions_20260802.csv`,
  `engine_suppressions_20260802.csv`, `discrepancies_log_20260802.csv`, and
  `video_cleanup_log_20260802.csv` (the first committed cleanup log).

- 2026-08-03 — **ROADMAP 13 landed: `__accuracy_report.py` matches on start
  alignment *and* window containment.** `_match` gains a second pass — a
  trigger whose `event_start_ts` falls inside `[gt.start − tol, gt.end + tol]`
  on the same `(pair, type)` now counts as a match. Recovered **44 of the
  2026-08-02 run's 135 false positives** and 2 of 2026-08-01's: overall
  precision **91.3 % → 94.1 %** and **96.5 % → 96.9 %**, rule 1 90.0 % →
  95.3 %, adjusted recall 87.6 % → 88.3 %.

  **Pass 1 is untouched**, deliberately: start-aligned candidates are still
  collected globally and assigned smallest-difference-first, one-to-one, so
  every row that matched before still matches the same event. Containment runs
  only over what pass 1 left unmatched. The 2026-07-31 legacy-format artifacts
  consequently score **identically** (89.4 %), preserving the guarantee
  CLAUDE.md makes about them; the 08-01 legacy path moved 96.8 % → 97.1 %,
  which is the fix working rather than drift.

  **Two judgement calls, both recorded because they are easy to get wrong.**
  *Type scoping was kept.* A rule 2 orphan claim landing inside a rule 1
  disagreement window is a different assertion about the world, not a late
  match, so containment is scoped to `(pair, gt_type)` exactly as start
  alignment is. A first count of 62 that ignored this was wrong and was
  committed in `57a1359` before the patch produced the real number; 44 is the
  type-respecting figure and the earlier entry has been corrected in place with
  a note. *Pass 2 allows many-to-one where pass 1 does not.* A 120 s
  disagreement the engine re-fires inside genuinely corresponds to several
  triggers, each of which observed a real disagreement — whereas two triggers
  aligned on the same GT *start* are competing to describe one instant, where
  one is a redundant detection. The generosity is bounded and visible: the
  count is returned from `_match`, printed in the PRECISION block alongside how
  many distinct GT events the contained matches landed on, and on **both**
  committed runs every contained match landed on its own event (44 of 44, 2 of
  2), so the many-to-one path has never actually fired. Redundant clips are
  measured elsewhere anyway (9C4, the cleanup sweep, the DELIVERY section).

  **What this does to Item 12.** All 44 recovered triggers are rule 1. Before
  the fix rule 1 looked like the larger FP source on 08-02 (83 vs 52),
  inverting the 08-01 ordering that made 12A the priority; after it, rule 1 has
  39 real FPs against rule 2's 52, so **12A is once again the higher-value
  item** and 12B's evidence base shrank by more than half. Both sub-items still
  need re-deriving against 94.1 % / 96.9 % baselines before anything is
  implemented — that was the point of gating them.

  Verified against an oracle computed independently of the tool, from the raw
  decision log and GT export, before the patch was written: it predicted 44 and
  94.1 % / 96.9 %, and the patched tool reported exactly that. All output paths
  (`--verbose` FP listing, per-pair table, DELIVERY cross-reference) exercised
  on the 08-02 artifacts.

- 2026-08-03 — **Items 12 and 14 re-derived against the corrected baselines;
  designs decided and scoped in `SCOPE_partner_gate_dedup_window.md`
  (analysis only — no engine code changed).** Everything below reproduces
  from the committed 08-01/08-02 artifacts alone, cross-referenced against
  the controller's own 82/81 edges shifted by the per-run offsets.

  **12A: the rule-2 FP population splits into two disjoint mechanisms**, and
  the split settles the "is 26:33 still dominant" question both ways. (1)
  *Sub-floor partner response* — the orphan pulse is real, and the partner
  responded with 0.1–0.4 s blips below the ~0.33 s sampling floor, so the
  engine's "partner completely OFF" evidence is structurally blind: 6/9 of
  08-01's and 28/52 of 08-02's rule-2 FPs, vs 1 % of TPs — near-perfect
  separation on a variable the engine cannot observe at event time. 26:33
  dominates this mechanism (6/6, 14/28); det 33 is the #1 sub-floor-blip
  producer on both runs (304 and 731 distinct pulses) and probably needs
  physical service. (2) *Threshold-boundary type flips* — 21 of 08-02's FPs
  (all on the det-8 triangle) measured 4.3–4.96 s pulses the controller saw
  as ≥ 5.0 s, so GT typed the same event `extended_disagreement`; 14 sit
  inside such a GT row that itself scores as a true miss. A scoring artifact,
  not an engine defect — suppressing the boundary zone would cost 47 TPs to
  kill 21 FPs and must not be done. The prescribed fix for (1) is a rolling
  partner-blip gate fed by the engine's own floor-gate declines (≥ 5 distinct
  below-floor partner pulses in 300 s declines the orphan; suppression reason
  `partner_below_floor_activity`): measured offline, it kills 6 FP/5 TP on
  08-01 and 15 FP/10 TP on 08-02 → 96.9 % → 98.0 % and 94.1 % → 95.0 %
  overall. 600/1800 s horizons and N=3 are strictly worse; N=8 is cheaper
  but kills a third fewer FPs.

  **12B: hysteresis re-derived from GT durations and decided NO.** The rule-1
  FPs are not marginal-duration events: their controller-truth XOR run at the
  event start is a median 1.8 s (08-02) — the "continuous ≥ 5 s" image was
  stitched across true agreements by the same sub-floor blips — and the
  engine's *observed* FP episodes run a median 7.2 s, so a bump to
  `threshold + one cycle` (5.33 s) prevents only ~4–9 of 39 FPs while
  demoting 22–53 genuine matched events (< 5.33 s GT duration). Net-negative
  by 3–6× on a rule already at 95.3 %. The 08-01 "9 of 15 ≤ 5.5 s"
  true-missed population reproduces unchanged post-Item-13.

  **14: the recorded 139/30/21 deletion breakdown was a join defect.**
  Joining through the recording log after the sweep's rewrite aliases every
  deleted clip onto its survivor; classifying by the trigger-ID prefix in the
  clip filename (unique, 190/190 mapped) gives **152 different-group / 38
  same-group-different-pair / 0 same-pair** — zero same-pair is what the 60 s
  cooldown vs 24.4 s median clip predicts, and the 9C4-reachable population
  is 20 %, nearly double the recorded figure. CLAUDE.md and ROADMAP corrected.
  Gap structure of the 38: median 1.62 s (the sibling pair crosses threshold
  1–2.3 s after the owner — just past the 1.0 s window), 29 ≤ 3 s, 35 ≤ 10 s,
  3 outliers ≥ 38 s; 5 are rule-1-deleted-into-rule-2-kept, unpreventable by
  design. A replay simulator (validated exactly: 457/457 against 08-02's own
  suppression marks, 135 on the 08-01 log) shows a blanket
  `pre_roll + post_roll` window (10 s) would strand 32 rule-2 pulse windows
  outside their owner's recording — so the decision is **per-rule windows**:
  `dedup_window_rule1_sec` 10.0 (any width is footage-safe via the AND-stop)
  and `dedup_window_sec` 1.0 → 3.0 for rule-2 candidates plus a coverage
  guard (suppress only if the pulse window sits inside the owner's guaranteed
  span; a rule-1 owner still active always qualifies at sane windows).
  Expected from replay: +91 suppressions on 08-02 (543 total, 35.0 %),
  17 of the 38 contained clips prevented, zero uncovered windows; 170
  (32.5 %) on 08-01. The sweep keeps the rest (152 different-group by
  construction, plus outliers) — its diet narrows, it is not replaced.

- 2026-08-03 — **ROADMAP 12A implemented: Rule 2 gains a partner
  sub-floor-activity gate; 12B closed as decided-no (documentation only).**
  Built exactly to `SCOPE_partner_gate_dedup_window.md` Item A. The floor gate
  (ROADMAP 9) bounds the *orphan's* side of Rule 2; this bounds the
  **partner's**, from the same principle — the engine must not treat partner
  silence as evidence when that partner has been producing pulses below the
  engine's own resolution. **Why a statistical signal rather than a check at
  event time:** the separating variable (a 0.1–0.4 s partner blip) is by
  definition invisible to the sampler at the moment of the event; the
  engine-visible-pulse test already exists in the rule and kills 0–2 of these
  FPs. What *is* visible is the partner's recent history of blips — and the
  engine already collects it, once per candidate the floor gate declines.

  Mechanics: `_DetectorState` gains `below_floor_pulses`, a deque of the
  `(on_ts, off_ts)` windows that detector's own pulses were declined at,
  appended by the caller of `_maybe_register_orphan` (deduped against the
  deque's tail, because a triangle declines one physical pulse once per pair)
  and pruned to `partner_blip_window_sec` next to `on_intervals`. It is the
  **one field on `_DetectorState` not guarded by its lock** — both the write
  and the read are on the evaluator thread, and the docstring says so
  explicitly rather than leaving the class invariant quietly violated. The
  gate itself sits in `_maybe_register_orphan`, **strictly after** the floor
  gate: a below-floor pulse must always be counted and reported as
  `below_sampling_floor`, so the two populations stay disjoint and both counts
  keep meaning something. Config (intersection level, read where
  `min_pulse_floor_multiple` is): `partner_blip_window_sec` 300.0,
  `partner_blip_max` 5, `0` on either disabling the gate; a malformed or
  negative value falls back to the default, the same posture as
  `dedup_window_sec` — a typo must not silently restore an FP population.

  The helper's return grew from a bare pulse tuple to an `_OrphanSuppression`
  NamedTuple (reason + pulse + count) because two gates now report through the
  same seam and the caller must not have to *infer* which one fired. New
  suppression reason `partner_below_floor_activity` — the extension path the
  `reason` column was designed for, and the first to use it — plus
  `partner_blip_count` / `partner_blip_window_sec` appended (end of the tuple,
  never mid-list) so a finished run can be re-scored at other thresholds
  without another controller session, exactly the reasoning that put
  `sampling_floor_sec` and `min_pulse_floor_multiple` in as separate columns.
  Blank on `below_sampling_floor` rows.

  **Verification was offline and reproduced the scope's numbers exactly**, on
  both committed runs, before anything shipped: replaying the gate over
  `engine_decisions_*` + `engine_suppressions_*` (distinct pulses deduped on
  `(orphan_det, event_start_ts)`, counted at each rule-2 trigger's
  `event_timestamp`) kills **6 FP + 5 TP on 08-01** (96.9 % → 98.0 % overall,
  96.3 % → 98.7 % rule 2) and **15 FP + 10 TP on 08-02** (94.1 % → 95.0 %,
  92.8 % → 94.7 %). The whole parameter table reproduced too — N=3 keeps the
  same FP kill at 3–4× the TP cost, N=8 loses a third of the FP kill, a 600 s
  horizon costs 4× the TPs — so N=5/300 s is confirmed, not assumed. Kills
  concentrate where the mechanism predicts: 26:33 supplies 6 of 6 (08-01) and
  14 of 15 FP kills (08-02). **These are projections from replay; the next
  owner run is what measures the gate**, and `partner_below_floor_activity`
  rows should appear at roughly 15–25/day, dominated by pairs whose partner is
  det 33. The gate is the software mitigation for a detector that likely needs
  physical service; being rolling rather than a static "disable rule 2 on
  26:33" is what lets it recover on its own once it is fixed.

  **12B (Rule 1 hysteresis) is closed as decided-no with no code and no config
  key** — the 2026-08-03 re-derivation above settles it (4–9 FPs prevented
  against 22–53 genuine events demoted; the FP mechanism is sub-floor chatter
  stitching, which a one-cycle bump does not address). Recorded as a paragraph
  in `discrepancy_engine.py`'s Rule 1 docstring section, mirroring the
  "document, don't code" precedent of SCOPE_sampling_floor Item B.2, so the
  next person to have the idea finds the measurement before writing the patch.

  Tests: +23 cases in `test_discrepancy_rules.py` (154 total, 365 across the
  seven suites) covering the gate's arithmetic and horizon edge, the ordering
  against the floor gate, once-per-pulse counting, the triangle dedupe, both
  disable paths, the row's columns in both reasons, and config
  plumb-through/defaults/garbage/reload. Two existing cases were updated for
  the richer return type. ROADMAP 14 (SCOPE Item C) is unaffected and remains
  the next item.

- 2026-08-03 — **ROADMAP 14 implemented: the cross-pair dedup window is now
  per rule, and the Rule 2 half carries a coverage guard.** Built to
  `SCOPE_partner_gate_dedup_window.md` Item C. 9C4 shipped one window
  (`dedup_window_sec`, 1.0 s) for both rules; the 2026-08-02 run showed the
  median recorded clip is 24.4 s and that same-group clips the disk sweep
  deleted as *contained* in a sibling's typically start **1.0–2.3 s** after
  their survivor — just past the window. **Why the answer is two windows and
  not one bigger number:** the property a fold rests on is not the same for
  the two rules. A Rule 1 candidate folded into a Rule 1 owner is safe at any
  width, because 9C4's AND-gated stop holds the owner's recording open until
  the folded pair's own disagreement resolves. A Rule 2 candidate has no such
  lever — its pulse is over before it is even evaluated — so widening its
  window trades footage for writer slots unless something checks that the
  footage exists. A blanket `pre_roll + post_roll` window would have left 32
  orphan pulse windows outside the clip they were folded into.

  So: `dedup_window_rule1_sec` (new, default **10.0** ≈ this deployment's
  `pre_roll + post_roll`, just above the p90 preventable gap of 9.0 s) for
  Rule 1 → Rule 1, and `dedup_window_sec` (**1.0 → 3.0**, the knee of the
  containment-gap histogram: 29 of the 38 preventable deletions sit within
  3 s) for Rule 2, gated by the new `_owner_covers_event`. Each key's `0` now
  disables **its own path only**. The guard compares in **event coordinates**
  — a clip is `[event_start − pre_roll, that + max_duration_sec]`, which is
  what the candidate's own clip would have been, so the question it answers is
  "does the owner's footage reach at least as far, both ways, as the clip this
  candidate would have bought". A Rule 2 owner's span is fixed at fire time
  and is carried on `_GroupFire` (`span_start` / `span_end`); a Rule 1 owner
  is judged by **liveness** (`active_trigger_id` still set), since an
  open-ended recording that is still running necessarily contains a pulse that
  is already over. An owner that has already stopped is refused — unreachable
  at the defaults, and there precisely so that raising the window in config
  cannot silently create footage loss, the same conservative-by-construction
  posture as `video_cleanup.plan_removals`. The sizing comment on the window
  constant was rewritten: the old "curve flattens after ~1 s" observation
  counted *fire-time clustering*, and clip *containment* is the number that
  actually matters.

  **Verification replayed both committed decision logs through the real
  `DiscrepancyMonitor`** — every logged trigger re-offered to `_fire_trigger`
  in order, so what is measured is the shipped code path, not a model of it.
  At the shipped 9C4 settings the replay reproduces the 2026-08-02 run's own
  457 suppression marks exactly (457/457) and the documented 135 on 2026-08-01,
  which is what makes the new numbers comparable. At the new defaults: 08-02
  goes to **545 of 1553 starts (35.1 %)**, +93 newly suppressed, and prevents
  **17 of the 38** same-group clips the sweep had to delete — matching the
  scope's prediction on the number that was the point of the item. 08-01 goes
  to **164 of 523 (31.4 %)**, from 135. ~93 fewer clips is ~37 min of writer
  occupancy returned against a 20.0 % delivery loss.

  Two measured corrections to the scope's projections, both from the guard's
  **start-side** check. The scope predicted 543 / 170; the shipped code gives
  545 / 164, and the 08-01 gap is the six folds whose pulse *began before the
  owner's event* — the scope's audit checked only the end of the window, so
  its "zero uncovered" claim holds only with the start check included. That
  check is therefore load-bearing, not decoration: at the **old** 1.0 s window
  it already refuses 5 folds on 08-02 and 3 on 08-01 that the shipped runs
  performed, i.e. the guard corrects a small pre-existing footage-loss
  population as well as bounding the new one. The sweep-log classification was
  re-derived by trigger-ID prefix (never a filename join) and reproduces
  **152 different-group / 38 same-group / 0 same-pair** exactly.

  The remaining same-group deletions stay with the disk sweep by design: Rule
  1 folded into a Rule 2 owner is still refused (a fixed-length clip cannot be
  held open), and the three 38 s–243 s gap outliers are left alone because a
  window that wide would fold genuinely distinct events into one clip. This
  item narrows the sweep's diet; it does not replace it — all 152
  different-group deletions are reachable by nothing else.

  Tests: +14 cases in `test_discrepancy_rules.py` (168 total, 379 across the
  seven suites) covering both windows, the rule-1-into-rule-2 refusal at the
  wider width, both guard bounds against a Rule 2 owner, both liveness
  outcomes against a Rule 1 owner, each key's independent `0`, defaults,
  garbage-value fallback and reload. Four existing 9C4 cases had their
  timestamps or expected default widened — the semantics they pin are
  unchanged.
