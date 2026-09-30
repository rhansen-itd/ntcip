# NTCIP / SNMP rules

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 598-643). Read before changing the SNMP client, poll loops, or chunk sizes. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L598-643 -->

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
  per-channel *mapping* in `intersections/201.json` is verified correct against
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
