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
