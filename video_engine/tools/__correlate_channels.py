#!/usr/bin/env python3
"""__correlate_channels.py — NTCIP-vs-controller detector channel correlation.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed).  Takes a raw NTCIP edge capture (from ``__capture_ntcip.py``) and a
controller high-res event export (``timestamp,event_code,parameter`` with
ATSPM codes 82=ON / 81=OFF, e.g. a pyatspm datZ extraction), reconstructs
each channel's ON/OFF waveform over the overlapping time window, and scores
every (NTCIP channel × controller channel) pair by the **Matthews
correlation coefficient** of the two binary waveforms.  For each NTCIP
channel the best-scoring controller channel is its empirically-true identity
— this is the tool that settles whether ``_intersections.json``'s channel
assignments are right.

Why MCC and not Jaccard/overlap: detector channels can sit ON for most of a
window (presence zones, stuck channels), and any two high-duty channels then
overlap heavily *by chance*.  MCC corrects for chance — a channel that is ON
90% of the time scores ≈0 against an unrelated 90%-duty channel but ≈1
against its true counterpart.  Each channel's ON-duty fraction is printed so
degenerate (always-ON / always-OFF) channels are visible.

Interpretation caveats the report encodes:

* **Co-located pairs really do co-actuate.**  A radar and a video channel
  watching the same stop bar produce similar waveforms *by design*, so the
  margin between "itself" and "its zone partner" can be small.  A REMAPPED
  verdict therefore requires the foreign channel to beat the channel's own
  number by a clear margin (``--margin``); anything closer is reported as
  ambiguous-with-partner rather than treated as mapping evidence.
* Two-pass clock alignment: a first pass at zero offset finds confident
  matches; the median ON-edge delta across them estimates edge-box-vs-
  controller clock skew (+ mean poll lag); the matrix is recomputed with the
  controller stream shifted by that estimate.
* Channels with few edges in the overlap are flagged low-confidence.

Usage::

    python3 video_engine/tools/__correlate_channels.py \
        ntcip_capture_20260719_173648.csv banks_events_20260719_1730.csv \
        [--config _intersections.json --intersection 201] \
        [--min-score 0.4] [--margin 0.15] [--min-edges 4] [--verbose]
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

Interval = Tuple[float, float]

EVENT_ON = "82"
EVENT_OFF = "81"


def _load_capture(
    path: str,
) -> Tuple[Dict[int, bool], Dict[int, List[Tuple[float, bool]]], float, float]:
    """Load an __capture_ntcip.py CSV.

    Returns (initial_state, edges_by_channel, start, end).  ``init_*`` rows
    are state declarations at capture start, not edges.
    """
    init: Dict[int, bool] = {}
    edges: Dict[int, List[Tuple[float, bool]]] = {}
    t0, t1 = None, 0.0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ts = float(row["timestamp"])
            det = int(row["detector"])
            if t0 is None:
                t0 = ts
            t1 = max(t1, ts)
            code = row["event_code"]
            if code == "0":
                init[det] = row["event"] == "init_on"
            elif code in (EVENT_ON, EVENT_OFF):
                edges.setdefault(det, []).append((ts, code == EVENT_ON))
    if t0 is None:
        raise SystemExit(f"{path}: no rows")
    return init, edges, t0, t1


def _load_controller(
    path: str,
) -> Tuple[Dict[int, List[Tuple[float, bool]]], float, float]:
    """Load a controller export (timestamp,event_code,parameter)."""
    edges: Dict[int, List[Tuple[float, bool]]] = {}
    t0, t1 = None, 0.0
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = row["event_code"]
            if code not in (EVENT_ON, EVENT_OFF):
                continue
            ts = float(row["timestamp"])
            det = int(row["parameter"])
            if t0 is None or ts < t0:
                t0 = ts
            t1 = max(t1, ts)
            edges.setdefault(det, []).append((ts, code == EVENT_ON))
    if t0 is None:
        raise SystemExit(f"{path}: no 82/81 rows")
    for lst in edges.values():
        lst.sort(key=lambda e: e[0])
    return edges, t0, t1


def _to_intervals(
    edge_list: List[Tuple[float, bool]],
    initial_on: Optional[bool],
    lo: float,
    hi: float,
) -> List[Interval]:
    """ON intervals clamped to [lo, hi] from an edge stream.

    ``initial_on`` is the state at ``lo``; ``None`` = unknown, inferred as
    "ON iff the first in-window edge is an OFF edge".
    """
    inw = [(t, s) for t, s in edge_list if lo <= t <= hi]
    if initial_on is None:
        initial_on = bool(inw) and inw[0][1] is False
    intervals: List[Interval] = []
    on_since: Optional[float] = lo if initial_on else None
    for ts, is_on in inw:
        if is_on:
            if on_since is None:
                on_since = ts
        else:
            if on_since is not None and ts > on_since:
                intervals.append((on_since, ts))
            on_since = None
    if on_since is not None and hi > on_since:
        intervals.append((on_since, hi))
    return intervals


def _intersection_time(a: List[Interval], b: List[Interval]) -> float:
    inter = 0.0
    i = j = 0
    while i < len(a) and j < len(b):
        lo = max(a[i][0], b[j][0])
        hi = min(a[i][1], b[j][1])
        if hi > lo:
            inter += hi - lo
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return inter


def _mcc(a: List[Interval], b: List[Interval], window: float) -> float:
    """Matthews correlation of two binary ON/OFF waveforms over the window."""
    t11 = _intersection_time(a, b)
    ta = sum(y - x for x, y in a)
    tb = sum(y - x for x, y in b)
    t10 = ta - t11
    t01 = tb - t11
    t00 = window - ta - tb + t11
    denom = math.sqrt((t11 + t10) * (t01 + t00) * (t11 + t01) * (t10 + t00))
    if denom <= 0:
        return 0.0
    return (t11 * t00 - t10 * t01) / denom


def _median(vals: List[float]) -> float:
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else 0.5 * (s[n // 2 - 1] + s[n // 2])


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Correlate NTCIP capture channels against controller "
                    "high-res channels to recover the true channel mapping."
    )
    ap.add_argument("capture_csv", help="output of __capture_ntcip.py")
    ap.add_argument("controller_csv",
                    help="controller events CSV (timestamp,event_code,parameter)")
    ap.add_argument("--config", help="intersection JSON to verdict against")
    ap.add_argument("--intersection", help="intersection ID inside --config")
    ap.add_argument("--min-score", type=float, default=0.4,
                    help="MCC below this = unresolved (default 0.4)")
    ap.add_argument("--margin", type=float, default=0.15,
                    help="a foreign channel must beat the channel's own "
                         "number by this much to call REMAPPED (default 0.15)")
    ap.add_argument("--min-edges", type=int, default=4,
                    help="fewer edges than this in overlap = low confidence")
    ap.add_argument("--verbose", action="store_true",
                    help="print the top-4 score list per channel")
    args = ap.parse_args()

    init, cap_edges, cap0, cap1 = _load_capture(args.capture_csv)
    ctl_edges, ctl0, ctl1 = _load_controller(args.controller_csv)
    lo, hi = max(cap0, ctl0), min(cap1, ctl1)
    window = hi - lo
    if window < 60:
        print(f"ERROR: only {window:.1f}s of overlap between the two files — "
              f"need at least a minute of common coverage.")
        return 1
    print(f"Overlap window: {window:.1f}s (unix {lo:.1f} – {hi:.1f})")

    def build(edges, initial):
        ivals, counts = {}, {}
        for ch, lst in edges.items():
            pre = [s for t, s in lst if t < lo]
            init_state = (pre[-1] if pre else initial.get(ch)) \
                if initial is not None else (pre[-1] if pre else None)
            ivals[ch] = _to_intervals(lst, init_state, lo, hi)
            counts[ch] = sum(1 for t, _ in lst if lo <= t <= hi)
        return ivals, counts

    nt_iv, nt_counts = build(cap_edges, init)
    ct_iv, ct_counts = build(ctl_edges, None)

    # Only channels with real in-window activity can be identified.
    nt_iv = {c: v for c, v in nt_iv.items() if nt_counts.get(c, 0) >= 2}
    ct_iv = {c: v for c, v in ct_iv.items() if ct_counts.get(c, 0) >= 2}
    duty_n = {c: sum(y - x for x, y in v) / window for c, v in nt_iv.items()}
    duty_c = {c: sum(y - x for x, y in v) / window for c, v in ct_iv.items()}
    print(f"NTCIP channels with edges in window: {sorted(nt_iv)}")
    print(f"Controller channels with edges in window: {sorted(ct_iv)}")

    def score_matrix():
        out = {}
        for nch, nints in nt_iv.items():
            scored = [(_mcc(nints, cints, window), cch)
                      for cch, cints in ct_iv.items()]
            scored.sort(key=lambda s: -s[0])
            out[nch] = scored
        return out

    # ── Pass 1: zero-offset scores → clock-skew estimate ─────────────────
    matrix = score_matrix()
    deltas: List[float] = []
    for nch, scored in matrix.items():
        if not scored or scored[0][0] < 0.3:
            continue
        c_on = [a for a, _ in ct_iv[scored[0][1]]]
        for a, _ in nt_iv[nch]:
            nearest = min(c_on, key=lambda x: abs(x - a), default=None)
            if nearest is not None and abs(nearest - a) <= 2.0:
                deltas.append(a - nearest)
    skew = _median(deltas) if deltas else 0.0
    print(f"Estimated NTCIP-minus-controller edge delay (clock skew + poll "
          f"lag): {skew * 1000:+.0f} ms over {len(deltas)} matched ON edges")

    # ── Pass 2: recompute with the controller stream shifted by the skew ─
    if abs(skew) > 0.05:
        ct_iv = {c: [(a + skew, b + skew) for a, b in v]
                 for c, v in ct_iv.items()}
        matrix = score_matrix()

    # ── Report ───────────────────────────────────────────────────────────
    print("\n" + "=" * 78)
    print(f"{'NTCIP':>5} {'edges':>5} {'duty':>5}  "
          f"{'self':>10}  {'best ctrl':>14}  {'runner-up':>13}  verdict")
    print("-" * 78)
    mapping: Dict[int, Optional[int]] = {}
    verdicts: Dict[int, str] = {}
    for nch in sorted(nt_iv):
        scored = matrix[nch]
        by_ch = {c: s for s, c in scored}
        self_s = by_ch.get(nch)
        best_s, best_c = scored[0] if scored else (0.0, None)
        run_s, run_c = scored[1] if len(scored) > 1 else (0.0, None)
        low_conf = nt_counts.get(nch, 0) < args.min_edges

        if best_c is None or best_s < args.min_score:
            verdict, mapping[nch] = "UNRESOLVED", None
        elif best_c == nch:
            verdict, mapping[nch] = "ok ✓", nch
        elif self_s is not None and best_s - self_s < args.margin:
            # Foreign winner but its own number is within the noise/partner
            # band — not remap evidence.
            verdict = f"ok (≈{best_c}, co-located?)"
            mapping[nch] = nch
        else:
            verdict, mapping[nch] = f"REMAPPED -> {best_c}", best_c
        if low_conf:
            verdict += " [few edges]"
        verdicts[nch] = verdict
        self_txt = f"{self_s:.2f}" if self_s is not None else "  — "
        print(f"{nch:>5} {nt_counts.get(nch, 0):>5} {duty_n[nch]:>5.0%}  "
              f"{self_txt:>10}  {str(best_c):>6} ({best_s:.2f})  "
              f"{str(run_c):>5} ({run_s:.2f})  {verdict}")
        if args.verbose:
            top = "  ".join(f"{c}:{s:.2f}" for s, c in scored[:4])
            print(f"{'':>12}top4: {top}")

    high_duty = [c for c, d in sorted(duty_n.items()) if d > 0.85]
    if high_duty:
        print(f"\nNTCIP channels ON >85% of the window (weak evidence, "
              f"scores unreliable): {high_duty}")
    unclaimed = sorted(set(ct_iv) - {v for v in mapping.values() if v})
    if unclaimed:
        print(f"Controller channels with activity matched by no NTCIP "
              f"channel: {unclaimed}")

    # ── Config verdicts ──────────────────────────────────────────────────
    if args.config:
        doc = json.loads(Path(args.config).read_text(encoding="utf-8"))
        key = args.intersection or next(iter(doc))
        dets = doc[key].get("detectors", {})
        print(f"\nConfig check ({args.config}[{key}]):")
        for did in sorted(dets, key=int):
            d = int(did)
            desc = dets[did].get("description", "")
            if d not in nt_iv:
                state = "no NTCIP edges in window — cannot verify"
            else:
                state = verdicts.get(d, "UNRESOLVED")
            print(f"  det {d:>2} ({desc}): {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
