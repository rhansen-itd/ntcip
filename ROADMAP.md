# Roadmap

Forward-looking work, ordered by priority. [[CLAUDE.md]] documents what the
code currently *does*; this file documents what still needs deciding or
building; [[DESIGN_HISTORY.md]] records *why* past decisions were made.

Work is broken into **named, numbered items**:

- The number is a **stable ID**, assigned once when an item is added and never
  reused or renumbered — not an execution order. An item keeps its number if
  items above/below it are finished and removed, or if the list is reordered.
- **File order is priority order** (with prerequisites noted inline), read top
  to bottom. Reordering the file doesn't change the numbers.
- Each item carries a **Target** model and a **Suggested prompt**. Default
  Target is **Opus, end-to-end in one session** (plan + implement + tests +
  the DESIGN_HISTORY entry and check-offs, no cross-model hand-off). Note a
  different Target only when an item is a whole small mechanical unit or a
  narrowed debugging escalation.
- To run an item, tell the agent "do Item N of ROADMAP.md" (or reference it by
  name). **On completion:** check off the item's boxes here, append a dated
  entry to [[DESIGN_HISTORY.md]]'s Decisions log, update [[CLAUDE.md]] if a
  convention changed, and move fully-finished items out of this file into
  DESIGN_HISTORY.md so ROADMAP stays forward-looking.

When scoping a new batch of requests, **group them into session-sized items**
(merge small related requests; split only what's too big for one session)
rather than one item per bullet, and assign new stable IDs continuing from the
highest used so far.

---

> **Post-Fable routing (2026-07-19, final Fable session).** Fable access has
> ended; the remaining items were deliberately left in a state where their
> *thinking* is pre-done and cheaper models execute:
> **8 → Opus** (race analysis + the deadlock trap are written in the item);
> **9 → Opus** (full design in [[SCOPE_sampling_floor.md]]);
> **4a remainder → no model** (owner runs the probe, flips a config key);
> **4a fallback (if probe fails) → Opus** (threading design decided inline
> below); **4f → Opus**; **4d / 4b / 5 → Sonnet** (mechanical, precedents
> set). Suggested order: 4a round trip → 8 → 9 → 4d → 4f → 4b/5.

---

## 9 — Sampling-floor awareness + post-4a accuracy re-baseline (Target: Opus)

Full decided design in [[SCOPE_sampling_floor.md]] (written 2026-07-19 from
the measured 1.53 s sweep / 7–42 % edge-capture findings). Summary: the
engine must not evaluate evidence finer than its own sampling resolution.
(A) `DetectorMonitor` self-measures its effective cycle (EMA, exposed via
stats); (B) `system_runner` injects that floor into `DiscrepancyMonitor.
set_sampling_floor()` (keeps the package boundary clean); Rule 2 refuses
orphan pulses shorter than 2× floor; per-pair high-duty advisory warning
(suppression opt-in only); (C) the pass/fail re-baseline protocol with
concrete numbers. Items A+B are useful even before 4a's round trip.

Suggested prompt:
> [Opus] In the ntcip project, do Item 9 of ROADMAP.md by implementing
> SCOPE_sampling_floor.md items A and B exactly as specified (C is an
> owner-run protocol, just leave it documented). Extend
> video_engine/tests/test_discrepancy_rules.py and
> ntcip_monitor/tests/test_snmp_batching.py per the scope's test list.
> DESIGN_HISTORY entry + check off.

---

## 8 — remux VideoBufferManager thread-safety + multi-camera assumption (Target: Opus)

Found 2026-07-19 while reviewing `video_engine/remux_video_buffer.py`.

- **Unlocked shared state across three thread contexts (real bug).**
  `_active_writers`, `_stop_timers`, and `_draining` are mutated from the poll
  loop (`_scan_trigger_dir`/`_handle_start`/`_stop_trigger`/`_reap_finished`),
  from `threading.Timer` callbacks (`_auto_stop` → `_stop_trigger`), and from
  the main thread (`stop()`), with **no lock**. Compound sequences (pop timer →
  cancel → pop writer → append `_draining`) can interleave, and `_draining` is
  iterated-and-removed by `_reap_finished`/`stop()` while a Timer thread may be
  appending — a concurrent-list-mutation hazard (intermittent `RuntimeError` /
  missed join / semaphore accounting drift). The `full` backend's poll-only
  model doesn't hit this because it has no Timer callbacks; the remux backend's
  `_auto_stop` timers introduced the exposure. Fix: guard the manager's
  writer/timer/draining bookkeeping with a single `threading.Lock` (short
  critical sections; no I/O under the lock).
- **Single-camera assumption (latent, both backends).** `_handle_start` does
  `cam_id = target_cams[0]` and creates exactly one writer, so a `["all"]` or
  two-camera trigger silently records only the first camera. Present in
  `video_buffer.py` too, so it's a design assumption, not a remux regression —
  harmless today (every intersection uses the single `fisheye` camera) but the
  trigger schema advertises multi-camera. Recommended for now: warn when a
  trigger resolves to >1 camera and document the single-camera assumption in the
  schema; defer true per-camera writers until a second camera is deployed.

**Execution guidance (2026-07-19, so Opus doesn't rediscover it):**
- ONE lock for all three fields; critical sections are pure bookkeeping
  (dict/list pop/append, timer cancel). **Never call `writer.stop()`,
  `thread.join()`, semaphore acquire, or any I/O while holding the lock** —
  `_auto_stop` runs on a Timer thread and `_stop_trigger` can be re-entered
  from it; holding the lock across a join deadlocks the reap path. Pattern:
  under the lock, pop/collect what to act on; release; then act.
- `stop()` and `_reap_finished` iterate `_draining` — snapshot the list under
  the lock (`list(...)`), mutate only under the lock.
- Multi-camera: log one WARNING when `target_cams` resolves to >1 camera and
  record `cameras_recorded: [cam_id]` vs `cameras_requested` in the CSV/log;
  document the single-camera assumption in `config_manager.py`'s trigger
  schema docstring. Do NOT implement per-camera writers (no second camera
  exists to test against).
- Test economically: stub `ClipRemuxer` (no PyAV, no streams) and drive
  start/auto-stop/stop/reap sequences from two threads; follow
  `video_engine/tests/test_discrepancy_rules.py`'s unittest layout. Skip
  real-stream replay verification.

Suggested prompt:
> [Opus] In the ntcip project, do Item 8 of ROADMAP.md following the item's
> execution guidance exactly: single lock around the manager's
> writer/timer/draining bookkeeping (no I/O or joins under the lock),
> multi-camera warn + schema docstring note, and a stubbed-remuxer
> bookkeeping test in video_engine/tests/. DESIGN_HISTORY entry + check off.

---

## 2 — Merge or finalize the second intersection config (Target: Opus)

`video_engine/701_intersection.json` (intersection 701, US-95/Whitley Dr) is
real in-progress config, separate from intersection 201 in
`video_engine/intersections.json`. `JsonFileConfigProvider` expects one file
keyed by intersection ID — decide whether to merge 701 into `intersections.json`
or keep per-intersection files and adjust the config provider to support that
shape.

Suggested prompt:
> [Opus] In the ntcip project, do Item 2 of ROADMAP.md: decide and implement
> how the second intersection config (`video_engine/701_intersection.json`,
> intersection 701) is stored relative to intersection 201 in
> `intersections.json` — either merge into the single keyed file or extend
> `ConfigProvider`/`JsonFileConfigProvider` (and the central provider, per the
> config-abstraction rule) to support per-intersection files. Land a
> DESIGN_HISTORY.md entry recording the choice and why.

---

## 3 — Commit or finalize `ring_monitor.py` (Target: Opus)

`ntcip_monitor/monitors/ring_monitor.py` is new and uncommitted. Confirm it's
ready (or still WIP) and commit once settled.

Suggested prompt:
> [Opus] In the ntcip project, do Item 3 of ROADMAP.md: review
> `ntcip_monitor/monitors/ring_monitor.py`, confirm it's production-ready
> (or note what's WIP), and commit it once settled. Note anything decided in
> DESIGN_HISTORY.md.

---

## 4 — Jules code-review backlog (Target: Opus; each sub-item is one session)

Findings from Jules's ongoing automated review, triaged against current code
(none were obsolete — all still apply as described). Grouped by how they
should be tackled. Each lettered sub-item below is a session-sized unit; take
them in roughly the listed order (4d before 4a/4h, since those want tests
first).

### 4a. Batch the per-OID SNMP polling loops (one focused session)

`output_monitor.py` and `detector_monitor.py` both call
`self.snmp_client.get(oid)` once per item inside a loop, instead of
`self.snmp_client.get(*oids)` once — which `phase_monitor.py` already does
correctly (`reds, yellows, greens = self.snmp_client.get(reds_oid, ...)`).

- `ntcip_monitor/monitors/output_monitor.py:54` — loops over up to 16 outputs.
- `ntcip_monitor/monitors/detector_monitor.py:66` — loops over up to 8
  detector groups. Independently flagged by Jules too (matches what I'd
  already found by inspection) — same fix applies to both.

Traced through `EconoliteSNMPClient.get()`
(`ntcip_monitor/core/snmp_client.py:33`): it already internally re-chunks any
multi-OID call into `CHUNK_SIZE=1` single-OID requests and preserves
ordering, so batching the *call* doesn't change the wire behavior or risk
mis-pairing values to OIDs — it only removes redundant lock acquisitions (16→1
per output poll) and future-proofs for if `CHUNK_SIZE` is ever raised above 1
(see CLAUDE.md — don't actually change `CHUNK_SIZE` itself). Low risk, but
note: `self.stats['reads']` currently increments once per OID; after batching
it increments once per `get()` call, which changes what that stat means if
anything depends on it (e.g. the web UI `/api/stats` endpoint).

**Priority upgrade (2026-07-19, measured):** this is now the
highest-leverage accuracy item, not just hygiene. The channel-correlation
work (see DESIGN_HISTORY) measured the real cost of `CHUNK_SIZE=1`: one
detector sweep = 8 sequential round trips = **1.0–1.5 s effective sampling
cycle** (median 1.53 s), so NTCIP sees only ~7–42 % of true detector edges —
the direct cause of the phase-2/6/7 false-trigger storms. Batching the call
sites (the original scope) fixes none of that on its own; the win requires a
**hardware test**: try the 8 single-byte detector-group OIDs in one PDU
against the Cobalt (the "Too Big" failures were on *dense tables*, which this
is not). If one PDU works, sweep time drops ~8× to ~0.15–0.2 s and the
0.2 s `poll_interval` becomes real. If it fails, fall back to polling only
the groups the config actually uses (6 of 8 at intersection 201) and
consider per-group threads. Until one of these lands, treat discrepancy
triggers on high-duty channels as unreliable.

**Two-machine workflow (2026-07-19):** Claude Code cannot run on the
controller-reachable machine, so the hardware evidence is gathered by a
self-contained probe, `video_engine/tools/__probe_snmp_batch.py` (already
written and self-tested; read-only GETs, safe on a live controller). The 4a
loop is:

**Software half DONE (2026-07-19, Fable — see DESIGN_HISTORY):** the call
sites are batched (`detector_monitor._poll` / `output_monitor._poll` each
issue one `get(*oids)`), `EconoliteSNMPClient` takes a `chunk_size` param
(default 1 = today's verified-safe wire behavior), `system_runner` derives
`detector_range` from the config's detectors (201: groups 1–6 instead of all
8 — a guaranteed ~25 % sweep cut) and reads **`snmp_chunk_size`** from the
intersection config; the standalone app reads `controller.chunk_size`.
`stats['reads']` now counts get() calls (= poll cycles), not OIDs — noted in
the code for `/api/stats` readers. Tests:
`ntcip_monitor/tests/test_snmp_batching.py` (stubbed pysnmp; chunk math,
ordering, batched polls, range→groups).

What remains needs the controller, but **no Claude session**:

1. **[Owner, controller machine]** `git pull`, then
   `python3 video_engine/tools/__probe_snmp_batch.py --config
   _intersections.json --intersection 201` (~2–4 min, read-only).
2. **[Owner]** if the verdict line says a chunk size is clean, set
   `"snmp_chunk_size": <that value>` in the intersection config (also add it
   to `_intersections.json` in the repo); if not clean, leave it at 1 — the
   needed-groups cut still applies. Restart the monitor.
3. **[Owner, controller machine]** rerun `__capture_ntcip.py` ~10 min with a
   matching datZ pull; push capture + datZ + probe JSON.
4. **[Any Claude session, any model]** verify with `__correlate_channels.py`
   and the edge-capture-ratio check that sweep time and edge capture improved
   (baseline 2026-07-19: median sweep 1.53 s, 7–42 % of edges seen). Then
   move this item to DESIGN_HISTORY.

**Fallback design if the probe verdict is dirty (decided 2026-07-19, Fable —
implement only if needed, one Opus session):** concurrent per-group polling.
Key constraints already worked out: `EconoliteSNMPClient.get()` serializes on
an internal lock, so concurrency requires **N clients** (own `SnmpEngine` +
socket each), one worker thread per client, each polling a fixed subset of
the needed groups (2 workers × 3 groups halves the sweep; 6 × 1 ≈ one-RTT
sweeps). Workers only write their group's latest bitmask + read-timestamp
into a shared array (tiny lock or per-slot atomic tuple assignment);
**`DetectorMonitor._poll` stays the single edge-emitting thread**, consuming
snapshots — this preserves the one-emitter ordering guarantee the
discrepancy-engine callbacks rely on. Before implementing, extend
`__probe_snmp_batch.py` with a `--concurrency N` phase (N simultaneous
single-OID clients hammering distinct groups) and re-run it in the same
owner round trip — a weak Cobalt CPU may serialize or drop concurrent UDP
requests, and that must be measured, not assumed.

### 4b. Unused-import cleanup (one mechanical sweep, zero behavior risk)

All confirmed still unused (grep shows zero other references besides the
import line). Safe to remove in a single pass:

- `ntcip_monitor/core/snmp_client.py:1` — `Counter32`, `Unsigned32`, `Gauge32`
  (keep `Integer32`, used in `set()`).
- `ntcip_monitor/monitors/phase_monitor.py:7` — `PhaseStatus`.
- `examples.py:11` — `DetectorState`, `OutputState` (keep `SignalState`).
- `video_engine/routine_scheduler.py:106` — `timezone` (keep `date`, `datetime`).
- `ntcip_monitor/main.py:6` — `sys`.
- `ntcip_monitor/utils/config_loader.py:8` — `datetime`.
- `ntcip_monitor/utils/controller_control.py:12` — `Counter32`, `Integer32`.
- `ntcip_monitor/ui/web_ui.py:12` — `SignalState`, `DetectorState`, `OutputState`.

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 4b of ROADMAP.md: remove the confirmed
> unused imports listed there in one mechanical pass (keep the named
> exceptions). Do NOT touch the false positives called out in 4c. DESIGN_HISTORY
> one-liner + check off.

### 4c. False positives / non-issues to skip — don't action these

- `from __future__ import annotations` flagged as unused in
  `video_engine/routine_scheduler.py:97` and `video_engine/config_manager.py:143`.
  Known false-positive pattern for import linters: it's a PEP 563 compiler
  directive with no runtime reference by design, so naive "is this name
  referenced anywhere" checks always flag it. Used consistently across all 7
  files in `video_engine/` plus `ring_monitor.py` as a deliberate convention —
  removing it from just these two files would make them inconsistent for zero
  benefit.
- `ConfigLoader`/`ControllerControl` "unused" in `ntcip_monitor/utils/__init__.py:3`,
  and `NTCIPMonitorApp` "unused" in `ntcip_monitor/__init__.py:5`. Both false
  positives — this is the standard `__init__.py` re-export idiom
  (`from ntcip_monitor import NTCIPMonitorApp` is literally how the README
  tells users to import the package; `utils/__init__.py` lists both names in
  `__all__`). Jules's unused-import check doesn't appear to special-case
  `__init__.py` re-exports.
- "Uncached repeated configuration reads" in `video_engine/discrepancy_engine.py:444`.
  Checked both call sites (`__init__` and the explicit `reload()` method,
  `discrepancy_engine.py:402` and `:466`) — neither is in the hot evaluator
  loop, and `JsonFileConfigProvider` already parses the JSON file once at
  construction and caches it in memory; each `get_intersection_config()` call
  is just an in-memory deep-copy, not disk I/O. The hot-loop pattern the
  rationale describes doesn't actually exist here.

### 4d. Seed a test suite (no `tests/` directory exists yet)

Six "lacks test coverage" findings that are genuinely valid and all
pure/deterministic — no mocking needed — making a reasonable first
`tests/` directory:

- `ntcip_monitor/core/oid_definitions.py:109` — `get_phase_oids(group)`.
- `ntcip_monitor/core/oid_definitions.py:127` — `get_detector_oid(detector_num)`.
- `ntcip_monitor/core/oid_definitions.py:146` — `get_output_oid(output_num)`.
- `ntcip_monitor/core/data_models.py:179` — `parse_signal_state(red_bit, yellow_bit, green_bit)`.
- `video_engine/config_manager.py:262` — `ConfigProviderError` (tiny exception
  subclass; test just instantiates it and checks `args`/`__cause__`).
- `video_engine/discrepancy_engine.py:141` — `_resolve_pytz(tz_name, log)`.
  (Jules's finding cites this as `get_safe_timezone` at line 194 — that name/
  line is stale; it's the same function, just renamed/moved since. Still
  genuinely untested: feed it an invalid IANA name and assert it falls back
  to `pytz.utc` and logs a warning — no mocking needed,
  `unittest.TestCase.assertLogs` covers the log assertion.)

**Sequencing (2026-07-19):** Item 7 has landed and established the layout
precedent: per-package tests (`video_engine/tests/test_discrepancy_rules.py`),
**stdlib `unittest`** (pytest is not installed in the deployment env; the
tests run via `python3 video_engine/tests/test_discrepancy_rules.py` or
unittest discovery, and remain pytest-compatible if it's ever added). Mirror
that: put the ntcip_monitor cases in `ntcip_monitor/tests/`, use `unittest`,
and for the `_resolve_pytz` log assertion use
`unittest.TestCase.assertLogs` instead of pytest's `caplog`. With the layout
decision removed, 4d is pure mechanical breadth over deterministic functions —
hence the Sonnet target below, not Opus.

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 4d of ROADMAP.md: cover the six
> pure/deterministic functions listed there with stdlib `unittest` (pytest is
> not installed). Do NOT invent a new
> test layout — follow the `tests/` layout Item 7 already established (check
> `video_engine/tests/` and the DESIGN_HISTORY entry from 7) and place the
> `ntcip_monitor` tests in the matching per-package location. No mocking needed.
> DESIGN_HISTORY one-liner + check off. (Runs after Item 7; it is the
> scaffolding 4a/4e/4h build on.)

### 4e. Broader test backlog — needs mocking/fixtures, defer until after 4d

The remaining "lacks test coverage" findings all need a mocked `snmp_client`,
a Flask test client, an in-memory SQLite DB, or similar fixtures — a much
bigger lift than 4d's pure functions. Worth a real test-strategy session once
4d's `unittest` scaffolding exists, not ad hoc: `DetectorMonitor`, `OutputMonitor`,
`PhaseMonitor`, `ControllerControl`, `NTCIPMonitorApp`, `WebUI`,
`DiscrepancyEngine.__init__` error handling, `RoutineScheduler`,
`ConfigProvider`/`JsonFileConfigProvider`/`SqliteCentralConfigProvider`,
`VideoBufferServer`.

### 4f. Harden the web UI (security — one session)

Two real findings that belong together since they're both about
`ntcip_monitor/ui/web_ui.py` network exposure:

- **`web_ui.py:22`** — `WebUI.__init__` defaults `host='0.0.0.0'`, and nothing
  in `run.py` exposes a way to override it (`WebUI(app, port=args.web_port)` —
  no `host=` passed). Today there's no way to bind to localhost-only without
  editing source. Fix: default to `127.0.0.1`, add a `--web-host` CLI flag /
  config key for anyone who deliberately wants LAN access.
- **`web_ui.py:103`** — `/api/control/*` endpoints (sync time, place vehicle
  calls, toggle outputs — i.e. actions that touch real traffic signal
  hardware) have no authentication. `ARCHITECTURE.md` already documents this
  as a known gap ("Web UI has no authentication — add reverse proxy with auth
  if exposed"). Minimal viable fix matching the project's stated style
  (resist over-engineering): a shared-secret header check via config, not a
  full session/JWT system — full auth can come later if the deployment story
  changes.

Suggested prompt:
> [Opus] In the ntcip project, do Item 4f of ROADMAP.md: harden
> `ntcip_monitor/ui/web_ui.py` — default the bind host to `127.0.0.1` with a
> `--web-host`/config override, and gate the `/api/control/*` hardware-touching
> endpoints behind a config-driven shared-secret header check (not full
> auth). DESIGN_HISTORY entry noting the deliberately-minimal scope.

### 4g. Won't-fix — hardware constraint

`ntcip_monitor/core/snmp_client.py:58` — SNMPv1 with cleartext community
strings, flagged as insecure. Correct observation, but not a code bug:
Econolite Cobalt/EOS controllers require SNMPv1 (already documented in
CLAUDE.md/ARCHITECTURE.md as a hardware constraint, alongside port 501 and
`CHUNK_SIZE=1`). SNMPv3 isn't available on this hardware to upgrade to. The
real mitigation is deployment-level: keep controller traffic on an isolated/
segmented network, not a code change.

### 4h. Lower priority — readability, do after 4d exists

- **`_evaluate_pair`, `video_engine/discrepancy_engine.py:584`** (244 lines)
  and **`_fire_trigger`, `video_engine/discrepancy_engine.py:924`** (11 args)
  — both legitimate, but both sit inside the discrepancy engine's
  carefully-worked-out state machine (the cooldown/active-trigger-id
  interaction documented in the module's own docstring — see CLAUDE.md).
  Refactoring either without tests in place first risks silently breaking
  behavior that took real effort to get right. Do this *after* 4d (and
  ideally after extending 4d's tests to cover `_evaluate_pair`'s rules
  directly), not before. `_fire_trigger`'s fix is probably a small
  `TriggerSpec`-like dataclass to replace the 11 positional args.
- **`SystemRunner.__init__`, `video_engine/system_runner.py:162`** (7 args) —
  valid nitpick, low value: it's a top-level orchestrator constructor called
  from exactly one place (`main()`) with sensible defaults. Skip unless doing
  a broader `system_runner.py` pass anyway.

---

## 5 — Retire the superseded CFR buffers (Target: Sonnet, mechanical)

Item 1 (remux backend) is complete and real-stream verified (see
DESIGN_HISTORY.md 2026-07-15), which unblocks the follow-up cleanup the plan
deferred: `video_engine/_old_video_buffer.py` and
`video_engine/_edge_video_buffer.py` are superseded interim CFR attempts,
imported by nothing. Move them to a `legacy/` folder or delete them (they're
recoverable from git history either way), and update CLAUDE.md's "Known repo
clutter" note when done. Do **not** touch `video_buffer.py` — it remains the
supported `full` (central/server) backend.

Suggested prompt:
> [Sonnet] In the ntcip project, do Item 5 of ROADMAP.md: remove (or move to
> `video_engine/legacy/`) the superseded `_old_video_buffer.py` and
> `_edge_video_buffer.py`, confirm nothing imports them, update CLAUDE.md's
> clutter note, and add a DESIGN_HISTORY.md one-liner.

---

## 6 — Retire the `full` (CFR) video backend (Target: Opus)

`video_engine/video_buffer.py` is the legacy `full`/CFR `cv2.VideoWriter`
backend. As of 2026-07-15 it is **unused** (no intersection config sets
`video_backend`, so every deployment defaults to `remux`), **strictly worse**
than `remux` on the three edge constraints (clip-length accuracy, CPU, RAM), and
carries the documented **RAM-unboundedness bug** (`DiskWriter._write_loop`
buffers a whole clip in memory — tens of GB for a multi-minute 1080p clip).
Nothing consumes decoded pixels today, and the flagged *future* decoded path is
explicitly a new bounded branch, not this file — so `full` is not the seam for
it either.

Decide and implement one of:
- **(a, recommended)** delete `video_buffer.py` and collapse
  `system_runner._build_video_manager` to remux-only (drop the backend switch
  and the `full` config docs); or
- **(b)** if a central re-encode/decoded need turns out to be real, keep the
  *switch* but rebuild that need as a **RAM-bounded decoded** backend — do not
  ship the CFR one.

Coordinate with **Item 5**: that item removes `_old_`/`_edge_` and currently says
"do not touch `video_buffer.py` — it remains the supported `full` backend"; this
item reopens exactly that decision (it's the last CFR file). Do Item 5 first or
fold both into one sweep. On completion, update CLAUDE.md's hardware-constraints
/ "two backends" section and the `config_manager.py` config docs.

Suggested prompt:
> [Opus] In the ntcip project, do Item 6 of ROADMAP.md: retire the `full` (CFR)
> video backend. Confirm nothing selects it, then remove `video_buffer.py` and
> simplify `system_runner._build_video_manager` to remux-only (or, if a central
> decoded need is confirmed, replace it with a RAM-bounded decoded backend — not
> the CFR one). Update CLAUDE.md's backends section and `config_manager.py` docs,
> and land a DESIGN_HISTORY.md entry.

---

## Future (not yet scoped)

Items below need a planning pass before they're actionable (no Target/Scope/
prompt yet). Promote to a numbered item above — with the next unused stable
ID — when scoped.

- *(none yet — add as they come up)*
