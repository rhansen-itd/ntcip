#!/usr/bin/env python3
"""__make_gt_export.py — controller events → ATSPM ground-truth discrepancy export.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed).  Middle link of the accuracy chain, which otherwise had no script::

    .datZ  --__decode_datz.py-->  events CSV
           --__make_gt_export.py-->  ground-truth anomalies CSV
           --__accuracy_report.py (vs discrepancies_log.csv)-->  precision/recall

The anomalies are **pyatspm's**, not a reimplementation: this calls
``atspm.analysis.detectors.analyze_discrepancies()``, which applies the same
three rules the engine does (extended disagreement / isolated pulse / chatter
exception) to the controller's own 0.1 s high-res log.  That is what makes the
output usable as ground truth rather than a second opinion.

**Run it under pyatspm's interpreter, not this repo's** — ``analyze_``
``discrepancies`` needs pandas and numpy, which are deliberately not
dependencies here::

    /home/hansrkid/pyatspm/.venv/bin/python \
        video_engine/tools/__make_gt_export.py events.csv \
        --config video_engine/intersections/201.json --intersection 201 --out gt.csv

Two arguments must match the engine run being scored or the comparison is
meaningless:

* **the config** — pairs are read from its ``detectors`` map
  (``paired_detector_id``, scalar **or list**, symmetric and deduplicated
  exactly as ``discrepancy_engine._build_structures`` does it), so the GT
  covers exactly the pairs the engine was watching.  Check the engine log's
  pair set against it — ROADMAP 2 collapsed the repo's three drifted
  intersection JSONs into one file per site (2026-08-03), but a run scored
  against a config edited since is the same failure.  If this reader and the
  engine ever disagree about the
  schema, every trigger on a pair the export missed scores as a false
  positive — the "scoring against the wrong pair set" failure, arriving by a
  new route.
* **``--lag-threshold``** — must equal the detectors' ``lag_threshold_sec``
  (it is both the Rule 1 duration threshold and the Rule 2 half-window).  It
  is read from the config when every paired detector agrees; pass it
  explicitly otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_pairs(config_path: str, intersection: str):
    """Read deduplicated (phase, det_a, det_b) pairs from an intersection JSON.

    Args:
        config_path: Path to the intersection JSON, or to a directory of
            per-intersection ``*.json`` files (ROADMAP 2) — the same two
            shapes ``JsonFileConfigProvider`` accepts, so this can be handed
            whatever the engine ran with.
        intersection: Intersection ID key inside it.

    Returns:
        A ``(pairs, thresholds)`` tuple: the pair dicts
        ``analyze_discrepancies`` wants, and the set of distinct
        ``lag_threshold_sec`` values seen across them.

    Raises:
        SystemExit: If the intersection key is absent.
    """
    path = Path(config_path)
    if path.is_dir():
        doc = {}
        for one in sorted(path.glob("*.json")):
            doc.update(json.loads(one.read_text(encoding="utf-8")))
    else:
        doc = json.loads(path.read_text(encoding="utf-8"))
    try:
        detectors = doc[intersection]["detectors"]
    except KeyError:
        raise SystemExit(
            f"ERROR: intersection '{intersection}' not in {config_path} "
            f"(has: {', '.join(k for k in doc if isinstance(doc[k], dict))})"
        )

    seen, pairs, thresholds = set(), [], set()
    for num, det in detectors.items():
        partner = det.get("paired_detector_id")
        if not partner:
            continue
        # Scalar or list — the engine accepts both (a 3-way group may be
        # authored as a ring of scalars or as explicit lists), so this reader
        # must too or the export silently covers fewer pairs than the run.
        partners = partner if isinstance(partner, (list, tuple)) else [partner]
        for one in partners:
            if one is None:
                continue
            key = tuple(sorted((int(num), int(one))))
            if key[0] == key[1] or key in seen:
                continue
            seen.add(key)
            pairs.append({"phase": int(det["phase"]),
                          "det_a": key[0], "det_b": key[1]})
            if det.get("lag_threshold_sec") is not None:
                thresholds.add(float(det["lag_threshold_sec"]))
    pairs.sort(key=lambda p: (p["phase"], p["det_a"]))
    return pairs, thresholds


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Build the ATSPM ground-truth discrepancy export that "
                    "__accuracy_report.py scores the engine against."
    )
    ap.add_argument("events", help="controller events CSV from __decode_datz.py")
    ap.add_argument("--config", required=True,
                    help="intersection config the engine ran with: a directory "
                         "of per-intersection *.json files, or one JSON file")
    ap.add_argument("--intersection", required=True, help="intersection ID key")
    ap.add_argument("-o", "--out", required=True, help="output anomalies CSV")
    ap.add_argument("--lag-threshold", type=float, default=None,
                    help="Rule 1 threshold / Rule 2 half-window in seconds "
                         "(default: the config's, when the pairs agree)")
    ap.add_argument("--pyatspm", default="/home/hansrkid/pyatspm",
                    help="pyatspm checkout root (default /home/hansrkid/pyatspm)")
    args = ap.parse_args()

    try:
        import pandas as pd
    except ImportError:
        print("ERROR: pandas is required and is not a dependency of this repo. "
              "Run this under pyatspm's interpreter, e.g.\n"
              f"  {args.pyatspm}/.venv/bin/python {sys.argv[0]} ...")
        return 2

    sys.path.insert(0, str(Path(args.pyatspm) / "src"))
    try:
        from atspm.analysis.detectors import analyze_discrepancies
    except ImportError as exc:
        print(f"ERROR: cannot import atspm from {args.pyatspm} ({exc}).")
        return 2

    pairs, thresholds = _load_pairs(args.config, args.intersection)
    if not pairs:
        print(f"ERROR: no paired detectors in "
              f"{args.config}[{args.intersection}].")
        return 2

    threshold = args.lag_threshold
    if threshold is None:
        if len(thresholds) == 1:
            threshold = thresholds.pop()
        else:
            print(f"ERROR: the config's paired detectors disagree on "
                  f"lag_threshold_sec ({sorted(thresholds) or 'none set'}) — "
                  f"pass --lag-threshold explicitly.")
            return 2

    df = pd.read_csv(args.events)
    missing = {"timestamp", "event_code", "parameter"} - set(df.columns)
    if missing:
        print(f"ERROR: {args.events} is missing column(s) "
              f"{', '.join(sorted(missing))} — is it a __decode_datz.py CSV?")
        return 2
    df = df[df["event_code"].isin([81, 82])].copy()
    if df.empty:
        print(f"ERROR: no 82/81 detector rows in {args.events}.")
        return 2

    print(f"events: {len(df)} rows, "
          f"{df.timestamp.min():.1f} – {df.timestamp.max():.1f} "
          f"({(df.timestamp.max() - df.timestamp.min()) / 60:.0f} min)")
    print(f"pairs: {len(pairs)} from {args.config}[{args.intersection}] "
          f"@ lag_threshold_sec={threshold}")

    result = analyze_discrepancies(df, pairs, lag_threshold_sec=threshold)
    result.to_csv(args.out, index=False)
    print(f"\nWrote {args.out}: {len(result)} anomalies")
    if len(result):
        for name, count in result.groupby("anomaly_type").size().items():
            print(f"  {name}: {count}")
    print("Next step: __accuracy_report.py discrepancies_log.csv "
          f"{args.out} --poll <measured effective_cycle_sec>")
    return 0


if __name__ == "__main__":
    sys.exit(main())
