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
