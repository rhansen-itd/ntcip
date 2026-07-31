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
