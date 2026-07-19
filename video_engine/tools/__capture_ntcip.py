#!/usr/bin/env python3
"""__capture_ntcip.py — raw NTCIP detector edge capture for channel-mapping audits.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed; not imported by production code).  Polls the controller's vehicle-
detector status groups over SNMP and records **every ON/OFF edge on every
channel** (1–64 by default) to a CSV, so the capture can be correlated
detector-by-detector against the pyatspm SQLite raw event log (codes 82/81)
to discover the *true* NTCIP-channel → physical-zone mapping — including
channels the intersection config doesn't currently claim.

It deliberately reuses the production stack — ``EconoliteSNMPClient``
(SNMPv1, port 501, CHUNK_SIZE=1) and the same ``DETECTOR_GROUPS`` OIDs +
LSB-first bit unpacking as ``DetectorMonitor``/``parse_detectors_from_bitmask``
— so what lands in the CSV is exactly what the discrepancy engine would see,
mapping bugs included.  (This is a standalone script, not a ``video_engine``
module, so importing ``ntcip_monitor`` here does not violate the package
boundary; the two packages themselves still never import each other.)

Output CSV columns (edge rows use ATSPM's event vocabulary so the pyatspm
side needs no translation):

    timestamp     Unix seconds (this machine's clock — same clock the
                  discrepancy engine stamps triggers with)
    iso_local     human-readable local time (see --tz)
    detector      channel number 1-64
    event_code    82 = ON edge, 81 = OFF edge, 0 = initial state row
    event         "on" / "off" / "init_on" / "init_off"

One ``init_*`` row per channel records the state seen on the first successful
poll, so a correlator knows the starting level.  A failed group read skips
that group for the cycle (states persist; no false edges are synthesized).

Usage::

    # Pull controller address from an intersection config:
    python3 video_engine/tools/__capture_ntcip.py \
        --config _intersections.json --intersection 201 --duration 600

    # Or fully explicit:
    python3 video_engine/tools/__capture_ntcip.py \
        --ip 10.37.23.200 --port 501 --community administrator \
        --poll 0.1 --duration 600 --out capture.csv

    # Smoke-test the tool itself with a built-in fake controller:
    python3 video_engine/tools/__capture_ntcip.py --simulate --duration 5

``--duration 0`` runs until Ctrl-C.  ``--echo`` prints each edge live.
A cadence summary (achieved poll rate, stalls, per-detector edge counts) is
printed on exit — check it before trusting a capture: if the achieved cycle
time is much worse than ``--poll``, short pulses may be aliased.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path

import pytz

# Bootstrap: repo root onto sys.path (tools/ -> video_engine/ -> repo root)
# so the ntcip_monitor package resolves from any working directory.
_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

# Load oid_definitions directly by file path: the ntcip_monitor package
# __init__ eagerly imports pysnmp, which --simulate mode must not require.
import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "_ntcip_oid_definitions",
    _REPO_ROOT / "ntcip_monitor" / "core" / "oid_definitions.py",
)
_oid_defs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_oid_defs)
DETECTOR_GROUPS = _oid_defs.DETECTOR_GROUPS

EVENT_ON = 82   # ATSPM high-res enumeration: detector ON
EVENT_OFF = 81  # ATSPM high-res enumeration: detector OFF


class _SimulatedClient:
    """Deterministic fake controller for offline smoke-testing.

    Each detector n pulses with its own period/duty derived from n, so the
    output has recognizable per-channel signatures without hardware.
    """

    def __init__(self):
        self.stats = {"reads": 0, "errors": 0}

    def get(self, oid):
        self.stats["reads"] += 1
        group_idx = DETECTOR_GROUPS.index(oid)
        now = time.time()
        bitmask = 0
        for bit in range(8):
            det = group_idx * 8 + 1 + bit
            period = 3.0 + (det % 7)          # 3–9 s cycle
            on_frac = 0.15 + 0.05 * (det % 4)  # 0.15–0.30 duty
            if (now / period) % 1.0 < on_frac:
                bitmask |= 1 << bit
        return bitmask


def _parse_detector_spec(spec: str) -> set:
    """Parse "1-64", "1-8,17,24", etc. into a set of detector numbers."""
    wanted = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            wanted.update(range(int(lo), int(hi) + 1))
        elif part:
            wanted.add(int(part))
    bad = [d for d in wanted if not 1 <= d <= 64]
    if bad:
        raise ValueError(f"detector numbers out of range 1-64: {bad}")
    return wanted


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Capture raw NTCIP detector ON/OFF edges to CSV "
                    "(all 64 channels by default) for pyatspm correlation."
    )
    src = ap.add_argument_group("controller address")
    src.add_argument("--config", help="intersection JSON (e.g. _intersections.json) "
                                      "to read ip/port/community from")
    src.add_argument("--intersection", help="intersection ID key inside --config")
    src.add_argument("--ip", help="controller IP (overrides --config)")
    src.add_argument("--port", type=int, default=None,
                     help="SNMP port (default 501, or from --config)")
    src.add_argument("--community", default=None,
                     help="community string (default 'administrator', or from --config)")

    ap.add_argument("--detectors", default="1-64",
                    help="channels to capture, e.g. '1-64' or '1-8,17,24-33' "
                         "(default: all 64 — capture everything, that's the point)")
    ap.add_argument("--poll", type=float, default=0.1,
                    help="target poll cycle in seconds (default 0.1; the "
                         "summary reports what was actually achieved)")
    ap.add_argument("--duration", type=float, default=600.0,
                    help="capture length in seconds; 0 = until Ctrl-C")
    ap.add_argument("--out", default=None,
                    help="output CSV path (default ntcip_capture_<ts>.csv)")
    ap.add_argument("--tz", default="America/Boise",
                    help="IANA timezone for the human-readable column")
    ap.add_argument("--echo", action="store_true",
                    help="print each edge as it is recorded")
    ap.add_argument("--simulate", action="store_true",
                    help="use a built-in fake controller (offline smoke test)")
    args = ap.parse_args()

    # ── Resolve controller address ───────────────────────────────────────
    ip, port, community = args.ip, args.port, args.community
    if args.config:
        import json
        cfg_doc = json.loads(Path(args.config).read_text(encoding="utf-8"))
        key = args.intersection or next(iter(cfg_doc))
        try:
            cfg = cfg_doc[key]
        except KeyError:
            print(f"ERROR: intersection '{key}' not in {args.config} "
                  f"(has: {', '.join(cfg_doc)})")
            return 2
        ip = ip or cfg.get("controller_ip")
        port = port or cfg.get("snmp_port")
        community = community or cfg.get("snmp_community")
        print(f"Controller from {args.config}[{key}]: {ip}:{port or 501}")
    port = port or 501
    community = community or "administrator"

    if not args.simulate and not ip:
        print("ERROR: no controller address — pass --ip or --config/--intersection "
              "(or --simulate for an offline smoke test).")
        return 2

    # ── Build the SNMP client (production class) ─────────────────────────
    if args.simulate:
        client = _SimulatedClient()

        class SNMPError(Exception):  # never raised by the simulator
            pass
        print("SIMULATED controller — deterministic per-channel pulse pattern.")
    else:
        try:
            from ntcip_monitor.core.snmp_client import (
                EconoliteSNMPClient, SNMPError,
            )
        except ImportError as exc:
            print(f"ERROR: cannot import the SNMP client ({exc}). "
                  f"Install requirements (pysnmp) or use --simulate.")
            return 2
        client = EconoliteSNMPClient(ip, port=port, community=community)

    wanted = _parse_detector_spec(args.detectors)
    groups = sorted({(d - 1) // 8 for d in wanted})
    tz = pytz.timezone(args.tz)

    out_path = Path(args.out) if args.out else Path(
        f"ntcip_capture_{datetime.now(tz).strftime('%Y%m%d_%H%M%S')}.csv"
    )
    print(f"Capturing detectors {args.detectors} "
          f"({len(groups)} SNMP group reads/cycle) -> {out_path}")
    print(f"Target poll {args.poll}s, duration "
          f"{'until Ctrl-C' if args.duration <= 0 else f'{args.duration:.0f}s'}.")

    # ── Capture loop ─────────────────────────────────────────────────────
    last_state: dict = {}
    edge_counts: dict = {d: 0 for d in wanted}
    cycles = 0
    errors = 0
    max_gap = 0.0
    started = time.time()
    prev_cycle_ts = None
    stop_at = started + args.duration if args.duration > 0 else None

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "iso_local", "detector", "event_code", "event"])

        def write_row(ts: float, det: int, code: int, label: str) -> None:
            iso = datetime.fromtimestamp(ts, tz).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            writer.writerow([f"{ts:.3f}", iso, det, code, label])
            if args.echo and code != 0:
                print(f"  {iso}  det {det:>2}  {label.upper()}")

        next_tick = time.monotonic()
        last_status = time.monotonic()
        try:
            while stop_at is None or time.time() < stop_at:
                cycle_ts = time.time()
                if prev_cycle_ts is not None:
                    max_gap = max(max_gap, cycle_ts - prev_cycle_ts)
                prev_cycle_ts = cycle_ts

                for gidx in groups:
                    try:
                        bitmask = client.get(DETECTOR_GROUPS[gidx])
                    except SNMPError as exc:
                        errors += 1
                        if errors <= 5 or errors % 100 == 0:
                            print(f"WARN: group {gidx + 1} read failed "
                                  f"(#{errors}): {exc}")
                        continue
                    ts = time.time()
                    for bit in range(8):
                        det = gidx * 8 + 1 + bit
                        if det not in wanted:
                            continue
                        state = (bitmask >> bit) & 1
                        prev = last_state.get(det)
                        if prev is None:
                            write_row(ts, det, 0, "init_on" if state else "init_off")
                        elif state != prev:
                            if state:
                                write_row(ts, det, EVENT_ON, "on")
                            else:
                                write_row(ts, det, EVENT_OFF, "off")
                            edge_counts[det] += 1
                        last_state[det] = state

                cycles += 1
                f.flush()

                now_mono = time.monotonic()
                if now_mono - last_status >= 5.0:
                    total_edges = sum(edge_counts.values())
                    rate = cycles / (time.time() - started)
                    print(f"  … {cycles} cycles ({rate:.1f}/s), "
                          f"{total_edges} edges, {errors} SNMP errors")
                    last_status = now_mono

                next_tick += args.poll
                delay = next_tick - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                else:
                    next_tick = time.monotonic()  # fell behind; don't spiral
        except KeyboardInterrupt:
            print("\nStopped by user.")

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.time() - started
    total_edges = sum(edge_counts.values())
    print(f"\nCapture complete: {elapsed:.1f}s, {cycles} cycles "
          f"({cycles / elapsed:.1f}/s achieved vs {1.0 / args.poll:.1f}/s target), "
          f"{total_edges} edges, {errors} SNMP errors, "
          f"worst cycle gap {max_gap * 1000:.0f} ms.")
    if cycles and (cycles / elapsed) < 0.5 / args.poll:
        print("WARN: achieved poll rate is far below target — short pulses may "
              "be aliased in this capture. Consider fewer --detectors groups.")
    active = {d: n for d, n in sorted(edge_counts.items()) if n}
    silent = sorted(d for d, n in edge_counts.items() if n == 0)
    print("\nEdges per detector (silent channels omitted):")
    for det, n in active.items():
        print(f"  det {det:>2}: {n:>5} edges")
    if silent:
        print(f"Silent channels ({len(silent)}): "
              f"{', '.join(str(d) for d in silent)}")
    print(f"\nWrote {out_path}")
    print("Next step: correlate per-channel against the pyatspm raw event log "
          "(codes 82/81) over the same window to recover the true channel map.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
