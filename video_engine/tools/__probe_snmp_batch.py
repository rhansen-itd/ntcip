#!/usr/bin/env python3
"""__probe_snmp_batch.py — one-shot hardware probe for ROADMAP 4a.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed).  Designed for the **two-machine workflow**: Claude Code cannot run
on the controller-reachable machine, so this script gathers *everything* the
4a decision needs in a single unattended run and writes it to a JSON results
file that gets committed back to the repo for analysis.

Question it answers empirically: can the Econolite Cobalt serve multiple
small OIDs in one SNMP PDU, or does the historical "Too Big" failure (seen on
dense tables) also apply to e.g. the 8 single-byte detector-group OIDs?  And
what is the real sweep time at each batch size?

What it does — **read-only GETs, no SETs, safe to run against a live
controller**:

  1. For chunk sizes 1, 2, 4, 8: read the 8 vehicle-detector group OIDs in
     ceil(8/chunk) PDUs, repeated ``--reps`` times.  Records per-rep success,
     error class (``tooBig`` errorStatus vs timeout vs other), sweep wall
     time, and whether the response varbinds echo the requested OIDs in
     order with sane byte values (0–255).
  2. The same for the 16 special-function output OIDs (chunk 1 vs 16) since
     ``output_monitor.py`` has the same per-OID loop.
  3. A "production sweep" simulation: the 6 detector groups intersection 201
     actually uses, at the best working chunk size.
  4. Prints a human summary with a computed verdict (largest chunk size with
     100 % success + its median sweep time) and writes the full record to
     ``snmp_batch_probe_<timestamp>.json``.

Pacing: ~50 ms between reps so the probe never hammers the controller.
A full default run (4 chunk sizes × 25 reps + extras) takes ~2–4 minutes.

Usage (on the controller-reachable machine)::

    python3 video_engine/tools/__probe_snmp_batch.py \
        --config _intersections.json --intersection 201
    # then: git add snmp_batch_probe_*.json && git commit && git push

    # offline self-test of the report/JSON plumbing (no hardware, no pysnmp):
    python3 video_engine/tools/__probe_snmp_batch.py --selftest
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Bootstrap: repo root (tools/ -> video_engine/ -> repo root); load
# oid_definitions by file path so --selftest works without pysnmp installed
# (the ntcip_monitor package __init__ imports pysnmp eagerly).
_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT))

import importlib.util  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "_ntcip_oid_definitions",
    _REPO_ROOT / "ntcip_monitor" / "core" / "oid_definitions.py",
)
_oid_defs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_oid_defs)
DETECTOR_GROUPS: List[str] = _oid_defs.DETECTOR_GROUPS
OUTPUT_OIDS: List[str] = _oid_defs.OUTPUT_OIDS

# Intersection 201 uses detectors 2..46 -> groups 1..6 (indices 0..5).
PRODUCTION_GROUP_INDICES = [0, 1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# PDU runner
# ---------------------------------------------------------------------------
# Returns (ok, error_class, error_detail, values, oids_echoed_in_order).
# error_class ∈ {"", "tooBig", "errorStatus", "timeout", "exception"}.
PduRunner = Callable[[List[str]], Tuple[bool, str, str, List[int], bool]]


def _make_real_runner(ip: str, port: int, community: str,
                      timeout: float, retries: int) -> PduRunner:
    """Build a PDU runner that issues one real SNMPv1 GET with N varbinds.

    Deliberately bypasses ``EconoliteSNMPClient.get()`` — that method
    re-chunks every request to CHUNK_SIZE=1, which is exactly the behavior
    under test.
    """
    from pysnmp.hlapi import (  # deferred: only needed for real runs
        getCmd, CommunityData, UdpTransportTarget, ContextData,
        ObjectType, ObjectIdentity, SnmpEngine,
    )
    engine = SnmpEngine()

    def run(oids: List[str]) -> Tuple[bool, str, str, List[int], bool]:
        try:
            iterator = getCmd(
                engine,
                CommunityData(community, mpModel=0),  # SNMPv1, like production
                UdpTransportTarget((ip, port), timeout=timeout,
                                   retries=retries),
                ContextData(),
                *[ObjectType(ObjectIdentity(o)) for o in oids],
            )
            error_indication, error_status, error_index, varbinds = \
                next(iterator)
        except StopIteration:
            return False, "timeout", "no SNMP response (StopIteration)", [], False
        except Exception as exc:  # noqa: BLE001 — record, never crash the probe
            return False, "exception", f"{type(exc).__name__}: {exc}", [], False

        if error_indication:
            detail = str(error_indication)
            cls = "timeout" if "timeout" in detail.lower() else "exception"
            return False, cls, detail, [], False
        if error_status:
            detail = (f"{error_status.prettyPrint()} at index {error_index}")
            cls = ("tooBig" if "toobig" in error_status.prettyPrint().lower()
                   else "errorStatus")
            return False, cls, detail, [], False

        try:
            values = [int(vb[1]) for vb in varbinds]
            echoed = [str(vb[0]) for vb in varbinds]
        except Exception as exc:  # noqa: BLE001
            return False, "exception", f"unparseable varbinds: {exc}", [], False
        in_order = echoed == list(oids)
        return True, "", "", values, in_order

    return run


def _make_selftest_runner() -> PduRunner:
    """Fake runner: chunk sizes >4 varbinds fail with tooBig, others succeed."""
    def run(oids: List[str]) -> Tuple[bool, str, str, List[int], bool]:
        time.sleep(0.01 + 0.005 * len(oids))  # latency grows mildly with size
        if len(oids) > 4:
            return False, "tooBig", "tooBig at index 0 (selftest)", [], False
        return True, "", "", [7] * len(oids), True
    return run


# ---------------------------------------------------------------------------
# Probe phases
# ---------------------------------------------------------------------------

def _sweep(runner: PduRunner, oids: List[str], chunk: int) -> Dict:
    """One full sweep of ``oids`` at the given chunk size; returns a record."""
    t0 = time.monotonic()
    pdus = []
    ok_all, values_all, order_all = True, [], True
    err_cls, err_detail = "", ""
    for i in range(0, len(oids), chunk):
        part = oids[i:i + chunk]
        ok, cls, detail, values, in_order = runner(part)
        pdus.append(ok)
        if not ok:
            ok_all = False
            err_cls, err_detail = cls, detail
            break
        values_all.extend(values)
        order_all = order_all and in_order
    elapsed = time.monotonic() - t0
    sane = all(0 <= v <= 255 for v in values_all) if values_all else False
    return {
        "ok": ok_all,
        "elapsed_ms": round(elapsed * 1000, 1),
        "error_class": err_cls,
        "error_detail": err_detail,
        "values": values_all,
        "oids_echoed_in_order": order_all,
        "values_in_byte_range": sane,
    }


def _run_phase(runner: PduRunner, name: str, oids: List[str],
               chunk: int, reps: int, pace_sec: float) -> Dict:
    print(f"  {name}: chunk={chunk}, {len(oids)} OIDs, {reps} reps … ",
          end="", flush=True)
    sweeps = []
    for _ in range(reps):
        sweeps.append(_sweep(runner, oids, chunk))
        time.sleep(pace_sec)
    ok_n = sum(1 for s in sweeps if s["ok"])
    times = [s["elapsed_ms"] for s in sweeps if s["ok"]]
    rec = {
        "phase": name,
        "chunk": chunk,
        "n_oids": len(oids),
        "reps": reps,
        "successes": ok_n,
        "success_rate": ok_n / reps if reps else 0.0,
        "median_ms": round(statistics.median(times), 1) if times else None,
        "p90_ms": (round(sorted(times)[int(0.9 * (len(times) - 1))], 1)
                   if times else None),
        "max_ms": max(times) if times else None,
        "order_ok": all(s["oids_echoed_in_order"] for s in sweeps if s["ok"]),
        "byte_range_ok": all(s["values_in_byte_range"]
                             for s in sweeps if s["ok"]),
        "first_error": next((f"{s['error_class']}: {s['error_detail']}"
                             for s in sweeps if not s["ok"]), ""),
        "sweeps": sweeps,
    }
    if ok_n == reps:
        print(f"OK  median {rec['median_ms']} ms")
    elif ok_n == 0:
        print(f"FAILED ({rec['first_error']})")
    else:
        print(f"FLAKY {ok_n}/{reps} ({rec['first_error']})")
    return rec


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Probe whether the controller accepts multi-OID PDUs and "
                    "measure sweep times (ROADMAP 4a hardware evidence)."
    )
    ap.add_argument("--config", help="intersection JSON for controller address")
    ap.add_argument("--intersection", help="intersection ID inside --config")
    ap.add_argument("--ip", help="controller IP (overrides --config)")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--community", default=None)
    ap.add_argument("--timeout", type=float, default=2.0,
                    help="SNMP timeout per request (default 2.0, production)")
    ap.add_argument("--retries", type=int, default=0,
                    help="SNMP retries (default 0 so timings are honest)")
    ap.add_argument("--reps", type=int, default=25,
                    help="sweeps per chunk size (default 25)")
    ap.add_argument("--pace", type=float, default=0.05,
                    help="sleep between sweeps, seconds (default 0.05)")
    ap.add_argument("--out", default=None, help="results JSON path")
    ap.add_argument("--selftest", action="store_true",
                    help="run against a fake controller (no pysnmp needed)")
    args = ap.parse_args()

    if args.selftest:
        runner = _make_selftest_runner()
        ip = "selftest"
        print("SELFTEST mode — fake controller: chunks >4 varbinds fail "
              "with tooBig.")
    else:
        ip, port, community = args.ip, args.port, args.community
        if args.config:
            doc = json.loads(Path(args.config).read_text(encoding="utf-8"))
            key = args.intersection or next(iter(doc))
            cfg = doc[key]
            ip = ip or cfg.get("controller_ip")
            port = port or cfg.get("snmp_port")
            community = community or cfg.get("snmp_community")
        port = port or 501
        community = community or "administrator"
        if not ip:
            print("ERROR: no controller address — pass --ip or "
                  "--config/--intersection (or --selftest).")
            return 2
        print(f"Probing {ip}:{port} (SNMPv1, timeout {args.timeout}s, "
              f"retries {args.retries}) — read-only GETs.")
        try:
            runner = _make_real_runner(ip, port, community,
                                       args.timeout, args.retries)
        except ImportError as exc:
            print(f"ERROR: pysnmp not available ({exc}). Run on the "
                  f"controller machine, or use --selftest.")
            return 2

    phases: List[Dict] = []
    print("\nDetector groups (8 OIDs):")
    for chunk in (1, 2, 4, 8):
        phases.append(_run_phase(runner, f"detector_groups_chunk{chunk}",
                                 DETECTOR_GROUPS, chunk, args.reps, args.pace))

    print("Outputs (16 OIDs):")
    for chunk in (1, 16):
        phases.append(_run_phase(runner, f"outputs_chunk{chunk}",
                                 OUTPUT_OIDS, chunk, args.reps, args.pace))

    # Production-shaped sweep: only the groups intersection 201 uses, at the
    # largest detector chunk size that was 100% clean.
    det_clean = [p for p in phases
                 if p["phase"].startswith("detector_groups")
                 and p["success_rate"] == 1.0 and p["order_ok"]
                 and p["byte_range_ok"]]
    best_chunk = max((p["chunk"] for p in det_clean), default=1)
    prod_oids = [DETECTOR_GROUPS[i] for i in PRODUCTION_GROUP_INDICES]
    print(f"Production-shaped sweep ({len(prod_oids)} groups, "
          f"chunk={best_chunk}):")
    phases.append(_run_phase(runner, f"production_6groups_chunk{best_chunk}",
                             prod_oids, best_chunk, args.reps, args.pace))

    # ── Verdict ──────────────────────────────────────────────────────────
    baseline = next((p for p in phases
                     if p["phase"] == "detector_groups_chunk1"), None)
    best = next((p for p in det_clean if p["chunk"] == best_chunk), None)
    print("\n" + "=" * 70)
    if baseline and baseline["median_ms"] is not None:
        print(f"Baseline (chunk=1, today's production wire behavior): "
              f"median sweep {baseline['median_ms']} ms")
    if best_chunk > 1 and best and best["median_ms"] is not None:
        speedup = (baseline["median_ms"] / best["median_ms"]
                   if baseline and baseline["median_ms"] else None)
        print(f"VERDICT: chunk={best_chunk} is clean over {args.reps} reps — "
              f"median sweep {best['median_ms']} ms"
              + (f" ({speedup:.1f}x faster)" if speedup else ""))
        print("=> 4a implementation can raise the per-call chunk for the "
              "detector-group sweep (do NOT change the global CHUNK_SIZE).")
    else:
        fails = [p for p in phases if p["phase"].startswith("detector_groups")
                 and p["success_rate"] < 1.0]
        detail = fails[0]["first_error"] if fails else "n/a"
        print(f"VERDICT: multi-OID PDUs are NOT clean on this controller "
              f"(first failure: {detail}).")
        print("=> fall back: poll only the config's needed groups, and/or "
              "per-group threading. Chunk stays 1.")
    print("=" * 70)

    out_path = Path(args.out) if args.out else Path(
        f"snmp_batch_probe_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    record = {
        "probe_version": 1,
        "target": ip,
        "timeout_sec": args.timeout,
        "retries": args.retries,
        "reps": args.reps,
        "started_local": datetime.now().isoformat(timespec="seconds"),
        "phases": phases,
    }
    out_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(f"\nWrote {out_path} — commit this file back to the repo for the "
          f"4a implementation session.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
