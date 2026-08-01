#!/usr/bin/env python3
"""__accuracy_report.py — engine-vs-ATSPM discrepancy accuracy report.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed).  Compares the discrepancy engine's trigger log against an ATSPM
ground-truth export from the sibling ``pyatspm`` project, and measures
**correspondence** — not raw counts, which diverge by design because of the
per-pair cooldown and the ~0.2 s NTCIP poll.

Two engine-log formats are accepted, auto-detected from the CSV header:

  * **``engine_decisions.csv``** (preferred, written by the engine itself as
    of ROADMAP 9C1) — one row per trigger the engine *emitted*, with exact
    Unix event windows.  Rows are complete: nothing downstream can suppress
    one, so recall measured against this file is an estimate, not a floor.
  * **``discrepancies_log.csv``** (legacy, written by the video-buffer
    backends) — one row per clip actually *recorded*.  A trigger dropped by
    the ``max_concurrent_writers`` cap or a low-disk abort leaves no row, so
    **recall read from this file is a floor** (measured 2026-07-31: the cap
    accounted for 43 % of the apparent misses).  Timing has to be
    reconstructed from a 1-second local timestamp plus a regex over the
    description, which is why ``--tz`` matters there and not in the new
    format.

Pass ``--recording-log`` alongside a decision log to quantify that gap
directly: decisions with no matching recording row are counted and broken
down by rule.

For every engine trigger it asks "is there a ground-truth event of the
corresponding type (rule1 ↔ extended_disagreement, rule2 ↔ isolated_pulse) on
the same detector pair within the start-time tolerance?" (→ precision), and
for every ground-truth event inside the engine's coverage window it asks "did
the engine fire, and if not, was the miss *expected*?" (→ recall).  Expected
misses are modelled explicitly:

  * **poll-aliased** — an isolated pulse shorter than ~2× the NTCIP poll
    interval can be sampled away entirely; the engine never saw both edges.
  * **cooldown/active suppression** — a ground-truth event that starts while
    the pair is inside a simulated post-trigger suppression window (cooldown
    after a rule 2 trigger; active recording + post-roll + cooldown after a
    rule 1 trigger).  NOTE: the engine's early cooldown reset (both detectors
    OFF) can shorten real cooldowns, so this category is an *upper bound* on
    expected suppression — inspect it before declaring victory.

Everything left over is a **true miss** (candidate engine defect), and every
engine trigger without a ground-truth counterpart is a **candidate false
positive**.

Engine-row timing is reconstructed without trusting the 1-second
``Local_Timestamp`` column where possible: rule 2 descriptions embed the exact
Unix observation window (pulse start/end are recovered from it); rule 1 rows
use ``Local_Timestamp − duration`` (start of the disagreement), so pass
``--tz`` matching the intersection's timezone.  A self-check compares the two
clocks on rule 2 rows and warns when the timezone looks wrong.

Usage::

    python3 video_engine/tools/__accuracy_report.py \
        engine_decisions.csv Discrepancies_20260719_140000_20260719_160000.csv \
        [--recording-log discrepancies_log.csv] \
        [--tz America/Boise] [--tolerance 3.0] [--poll 0.2] [--cooldown 60] \
        [--post-roll 20] [--gap 600] [--coverage-margin 30] [--verbose]
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pytz

# Rule-name correspondence between the two logs.
_ENGINE_TO_GT_TYPE = {
    "rule1_continuous_disagreement": "extended_disagreement",
    "rule2_orphan_pulse": "isolated_pulse",
}

_RE_RULE2 = re.compile(r"duration=([\d.]+)s window=\[([\d.]+), ([\d.]+)\]")
_RE_RULE1 = re.compile(r"for ([\d.]+)s \(threshold=")


@dataclass
class EngineTrigger:
    fire_ts: float          # wall-clock when the trigger fired (Unix)
    start_ts: float         # start of the underlying event
    end_ts: float           # end (rule2) or == fire_ts (rule1)
    rule: str
    pair: Tuple[str, str]
    description: str
    trigger_id: str = ""    # decision-log only; keys the recording cross-ref
    matched_gt: Optional["GTEvent"] = None
    nearest_diff: Optional[float] = None  # diagnostic for unmatched rows
    # A rule 2 row repeating an earlier row's exact pulse window: the stale-
    # refire bug (fixed 2026-07-19) re-firing on unchanged detector state.
    stale_refire: bool = False
    recorded: Optional[bool] = None  # set only when --recording-log is given


@dataclass
class GTEvent:
    start: float
    end: float
    duration: float
    gt_type: str
    pair: Tuple[str, str]
    on_det: str
    description: str
    matched_by: Optional[EngineTrigger] = None
    category: str = ""      # matched / aliased / cooldown / true_miss / outside


def _pair_key(a: str, b: str) -> Tuple[str, str]:
    return tuple(sorted((str(a).strip(), str(b).strip())))


def _parse_engine_csv(path: str, tz_name: str) -> Tuple[List[EngineTrigger], str]:
    """Load an engine log, auto-detecting which of the two formats it is.

    Returns:
        ``(triggers, fmt)`` where ``fmt`` is ``"decision"`` or ``"recording"``.
    """
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        rows = list(reader)

    if "event_timestamp" in fields:
        return _parse_decision_rows(rows), "decision"
    if "Rule_Type" in fields:
        return _parse_recording_rows(rows, tz_name), "recording"
    raise SystemExit(
        f"{path}: unrecognised engine log — expected either an "
        f"'event_timestamp' column (engine_decisions.csv) or a 'Rule_Type' "
        f"column (discrepancies_log.csv); got {fields}"
    )


def _parse_decision_rows(rows: List[dict]) -> List[EngineTrigger]:
    """Parse ``engine_decisions.csv`` — exact Unix columns, no reconstruction.

    ``stop`` rows are skipped: they close a Rule 1 recording rather than
    reporting a detection, and counting them would double every Rule 1 event.
    """
    triggers: List[EngineTrigger] = []
    seen_windows: set = set()
    missing_window = 0

    for row in rows:
        if row.get("action", "start").strip() != "start":
            continue
        rule = row["rule"].strip()
        if rule not in _ENGINE_TO_GT_TYPE:
            continue

        fire_ts = float(row["event_timestamp"])
        pair = _pair_key(row["det_a"], row["det_b"])

        start_txt = (row.get("event_start_ts") or "").strip()
        end_txt = (row.get("event_end_ts") or "").strip()
        if start_txt:
            start_ts = float(start_txt)
        else:
            # Only expected for a rule the engine did not window; fall back to
            # the same arithmetic the legacy format uses.
            missing_window += 1
            start_ts = fire_ts - float(row.get("disagreement_sec") or 0.0)
        # A rule 1 start has no end yet — the disagreement is still open, so
        # the trigger's own fire time is the best available bound.
        end_ts = float(end_txt) if end_txt else fire_ts

        window_key = (pair, start_ts, end_ts)
        triggers.append(EngineTrigger(
            fire_ts=fire_ts, start_ts=start_ts, end_ts=end_ts,
            rule=rule, pair=pair,
            description=row.get("description", ""),
            trigger_id=row.get("trigger_id", "").strip(),
            stale_refire=(rule == "rule2_orphan_pulse"
                          and window_key in seen_windows),
        ))
        if rule == "rule2_orphan_pulse":
            seen_windows.add(window_key)

    if missing_window:
        print(f"WARN: {missing_window} decision rows had no event_start_ts; "
              f"their start times were reconstructed from disagreement_sec.")

    triggers.sort(key=lambda t: t.start_ts)
    return triggers


def _parse_recording_rows(rows: List[dict], tz_name: str) -> List[EngineTrigger]:
    """Parse the legacy ``discrepancies_log.csv`` (recordings, not decisions).

    Timing is reconstructed: rule 2 windows come from the description's
    embedded Unix window, rule 1 starts from ``Local_Timestamp − duration``,
    so ``tz_name`` must match the intersection.  See the module docstring for
    why recall from this file is a floor.
    """
    tz = pytz.timezone(tz_name)
    triggers: List[EngineTrigger] = []
    clock_diffs: List[float] = []
    seen_windows: set = set()

    for row in rows:
        rule = row["Rule_Type"].strip()
        if rule not in _ENGINE_TO_GT_TYPE:
            continue
        desc = row["Description"]
        pair = _pair_key(row["Det_A"], row["Det_B"])

        # Local_Timestamp is "YYYY-mm-dd HH:MM:SS <TZABBREV>"; drop the
        # abbreviation (strptime %Z is unreliable) and localize via --tz.
        ts_txt = row["Local_Timestamp"].strip().rsplit(" ", 1)[0]
        naive = datetime.strptime(ts_txt, "%Y-%m-%d %H:%M:%S")
        fire_ts = tz.localize(naive).timestamp()

        if rule == "rule2_orphan_pulse":
            m = _RE_RULE2.search(desc)
            if not m:
                print(f"WARN: unparseable rule2 description: {desc!r}")
                continue
            dur = float(m.group(1))
            w_start, w_end = float(m.group(2)), float(m.group(3))
            # window = [pulse_on − thr, pulse_off + thr] and
            # dur = pulse_off − pulse_on  ⇒  pulse_on = (a + b − dur) / 2.
            pulse_on = (w_start + w_end - dur) / 2.0
            window_key = (pair, w_start, w_end)
            triggers.append(EngineTrigger(
                fire_ts=fire_ts, start_ts=pulse_on,
                end_ts=pulse_on + dur, rule=rule, pair=pair,
                description=desc,
                trigger_id=row.get("Trigger_ID", "").strip(),
                stale_refire=window_key in seen_windows,
            ))
            seen_windows.add(window_key)
            # The trigger fires right at window_end; use that to sanity-
            # check the Local_Timestamp → Unix conversion.
            clock_diffs.append(fire_ts - w_end)
        else:
            m = _RE_RULE1.search(desc)
            if not m:
                print(f"WARN: unparseable rule1 description: {desc!r}")
                continue
            dur = float(m.group(1))
            triggers.append(EngineTrigger(
                fire_ts=fire_ts, start_ts=fire_ts - dur,
                end_ts=fire_ts, rule=rule, pair=pair, description=desc,
                trigger_id=row.get("Trigger_ID", "").strip(),
            ))

    if clock_diffs:
        clock_diffs.sort()
        median = clock_diffs[len(clock_diffs) // 2]
        if abs(median) > 60.0:
            print(f"WARN: Local_Timestamp vs embedded Unix window disagree by "
                  f"~{median:.0f}s (median) — is --tz correct? Rule 1 start "
                  f"times will be off by the same amount.")

    triggers.sort(key=lambda t: t.start_ts)
    return triggers


def _annotate_recorded(triggers: List[EngineTrigger], recording_csv: str) -> int:
    """Mark which decisions produced an actual clip.

    Joins on ``Trigger_ID``, which both artifacts carry verbatim from the
    trigger payload.  Returns the number of decisions with no recording — the
    triggers the video buffer dropped (writer-cap saturation, a low-disk
    abort, or no matching camera), which is precisely the population that made
    recall unmeasurable from the recording log alone.
    """
    recorded_ids = set()
    with open(recording_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            tid = (row.get("Trigger_ID") or "").strip()
            if tid:
                recorded_ids.add(tid)

    unrecorded = 0
    for t in triggers:
        if not t.trigger_id:
            continue
        t.recorded = t.trigger_id in recorded_ids
        if not t.recorded:
            unrecorded += 1
    return unrecorded


def _parse_gt_csv(path: str) -> List[GTEvent]:
    events: List[GTEvent] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            events.append(GTEvent(
                start=float(row["start_timestamp"]),
                end=float(row["end_timestamp"]),
                duration=float(row["duration_sec"]),
                gt_type=row["anomaly_type"].strip(),
                pair=_pair_key(row["det_a_id"], row["det_b_id"]),
                on_det=str(row["on_det_id"]).strip(),
                description=row.get("description", ""),
            ))
    events.sort(key=lambda e: e.start)
    return events


def _coverage_blocks(
    triggers: List[EngineTrigger], gap_sec: float, margin_sec: float,
) -> List[Tuple[float, float]]:
    """Split the engine log into contiguous coverage blocks on large gaps."""
    blocks: List[Tuple[float, float]] = []
    if not triggers:
        return blocks
    times = sorted(t.fire_ts for t in triggers)
    block_start = times[0]
    prev = times[0]
    for ts in times[1:]:
        if ts - prev > gap_sec:
            blocks.append((block_start - margin_sec, prev + margin_sec))
            block_start = ts
        prev = ts
    blocks.append((block_start - margin_sec, prev + margin_sec))
    return blocks


def _in_blocks(ts: float, blocks: List[Tuple[float, float]]) -> bool:
    return any(lo <= ts <= hi for lo, hi in blocks)


def _match(
    triggers: List[EngineTrigger],
    gt_events: List[GTEvent],
    tolerance: float,
) -> None:
    """One-to-one nearest-start matching per (pair, type).

    All within-tolerance (trigger, event) candidate pairs are collected and
    assigned globally smallest-difference-first, so a later trigger is never
    starved of its own nearby event by an earlier trigger grabbing it (the
    early-cooldown-reset path legitimately produces same-pair triggers only
    seconds apart).
    """
    by_key: Dict[Tuple[Tuple[str, str], str], List[GTEvent]] = {}
    for e in gt_events:
        by_key.setdefault((e.pair, e.gt_type), []).append(e)

    within: List[Tuple[float, EngineTrigger, GTEvent]] = []
    for trig in triggers:
        candidates = by_key.get((trig.pair, _ENGINE_TO_GT_TYPE[trig.rule]), [])
        for e in candidates:
            diff = abs(e.start - trig.start_ts)
            if trig.nearest_diff is None or diff < trig.nearest_diff:
                trig.nearest_diff = diff
            if diff <= tolerance:
                within.append((diff, trig, e))

    within.sort(key=lambda item: item[0])
    for diff, trig, e in within:
        if trig.matched_gt is None and e.matched_by is None:
            trig.matched_gt = e
            e.matched_by = trig
            e.category = "matched"


def _suppression_windows(
    triggers: List[EngineTrigger],
    cooldown: float,
    post_roll: float,
) -> Dict[Tuple[str, str], List[Tuple[float, float]]]:
    """Simulated per-pair windows in which the engine cannot fire again.

    Rule 2: cooldown engages at fire time.  Rule 1: the pair is held by the
    active recording until the disagreement resolves (approximated by the
    matched ground-truth event's end, else the fire time), then post-roll,
    then cooldown.
    """
    windows: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for trig in triggers:
        if trig.rule == "rule2_orphan_pulse":
            lo, hi = trig.fire_ts, trig.fire_ts + cooldown
        else:
            resolved = trig.matched_gt.end if trig.matched_gt else trig.fire_ts
            lo, hi = trig.fire_ts, max(resolved, trig.fire_ts) + post_roll + cooldown
        windows.setdefault(trig.pair, []).append((lo, hi))
    return windows


def _fmt_ts(ts: float, tz: pytz.BaseTzInfo) -> str:
    return datetime.fromtimestamp(ts, tz).strftime("%H:%M:%S")


def _print_list(title: str, rows: List[str], verbose: bool, limit: int = 10) -> None:
    print(f"\n  {title} ({len(rows)}):")
    shown = rows if verbose else rows[:limit]
    for r in shown:
        print(f"    {r}")
    if not verbose and len(rows) > limit:
        print(f"    … {len(rows) - limit} more (use --verbose)")


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Precision/recall report: engine log vs ATSPM ground truth."
    )
    ap.add_argument("engine_csv",
                    help="engine_decisions.csv (preferred) or the legacy "
                         "discrepancies_log.csv; the format is auto-detected")
    ap.add_argument("gt_csv", help="ATSPM ground-truth export CSV")
    ap.add_argument("--recording-log", default=None,
                    help="discrepancies_log.csv to cross-reference against a "
                         "decision log, measuring how many decisions the video "
                         "buffer dropped")
    ap.add_argument("--tz", default="America/Boise",
                    help="IANA timezone of the engine log's Local_Timestamp "
                         "(legacy format only; decision logs carry Unix time)")
    ap.add_argument("--tolerance", type=float, default=3.0,
                    help="max start-time difference for a match, seconds "
                         "(≈ poll interval + SNMP lag + clock skew)")
    ap.add_argument("--poll", type=float, default=0.2,
                    help="NTCIP poll interval, seconds (aliasing model)")
    ap.add_argument("--cooldown", type=float, default=60.0,
                    help="engine per-pair cooldown, seconds")
    ap.add_argument("--post-roll", type=float, default=20.0,
                    help="engine post-roll after rule 1 resolution, seconds")
    ap.add_argument("--gap", type=float, default=600.0,
                    help="gap between engine rows that splits coverage blocks")
    ap.add_argument("--coverage-margin", type=float, default=30.0,
                    help="slack added around each coverage block, seconds")
    ap.add_argument("--verbose", action="store_true",
                    help="print full categorized lists, not just the head")
    args = ap.parse_args()

    tz = pytz.timezone(args.tz)
    all_triggers, fmt = _parse_engine_csv(args.engine_csv, args.tz)
    gt_events = _parse_gt_csv(args.gt_csv)
    if fmt == "decision":
        print("Engine log format: engine_decisions.csv (decisions; exact "
              "Unix event windows).")
    else:
        print("Engine log format: discrepancies_log.csv (RECORDINGS, not "
              "decisions — triggers the video buffer dropped are absent, so "
              "recall below is a FLOOR, not an estimate).")
    print(f"Parsed {len(all_triggers)} engine triggers, "
          f"{len(gt_events)} ground-truth events.")
    if not all_triggers or not gt_events:
        print("Nothing to compare.")
        return 1

    unrecorded = None
    if args.recording_log:
        if fmt != "decision":
            print("WARN: --recording-log is only meaningful with a decision "
                  "log; ignoring it.")
        else:
            unrecorded = _annotate_recorded(all_triggers, args.recording_log)

    # The GT export has its own time window; engine triggers outside it can
    # never match and must not count against precision.
    gt_lo = min(e.start for e in gt_events)
    gt_hi = max(e.end for e in gt_events)
    print(f"GT export window: {_fmt_ts(gt_lo, tz)} – {_fmt_ts(gt_hi, tz)}")
    triggers = [
        t for t in all_triggers
        if gt_lo - args.tolerance <= t.start_ts <= gt_hi + args.tolerance
    ]
    outside_gt = len(all_triggers) - len(triggers)
    if outside_gt:
        print(f"  ({outside_gt} engine triggers outside the GT export window "
              f"— excluded from precision)")

    blocks = _coverage_blocks(all_triggers, args.gap, args.coverage_margin)
    print("\nEngine coverage blocks (from the engine log itself):")
    for lo, hi in blocks:
        print(f"  {_fmt_ts(lo, tz)} – {_fmt_ts(hi, tz)} "
              f"({(hi - lo) / 60.0:.1f} min)")

    _match(triggers, gt_events, args.tolerance)
    windows = _suppression_windows(triggers, args.cooldown, args.post_roll)

    # ── Categorize ground-truth events ───────────────────────────────────
    aliasing_floor = 2.0 * args.poll
    for e in gt_events:
        if e.category == "matched":
            continue
        if not _in_blocks(e.start, blocks):
            e.category = "outside"
        elif e.start <= gt_lo + args.tolerance:
            # Event truncated by the export's start boundary: its true start
            # (and any engine trigger for it) precedes the export window, so
            # a start-time match is impossible by construction.
            e.category = "boundary"
        elif e.gt_type == "isolated_pulse" and e.duration < aliasing_floor:
            e.category = "aliased"
        elif any(lo <= e.start <= hi
                 for lo, hi in windows.get(e.pair, [])):
            e.category = "cooldown"
        else:
            e.category = "true_miss"

    in_cov = [e for e in gt_events if e.category != "outside"]
    matched = [e for e in in_cov if e.category == "matched"]
    aliased = [e for e in in_cov if e.category == "aliased"]
    boundary = [e for e in in_cov if e.category == "boundary"]
    cooldown = [e for e in in_cov if e.category == "cooldown"]
    true_miss = [e for e in in_cov if e.category == "true_miss"]
    outside = len(gt_events) - len(in_cov)

    tp = [t for t in triggers if t.matched_gt is not None]
    fp = [t for t in triggers if t.matched_gt is None]

    # ── Report ───────────────────────────────────────────────────────────
    def _rule_stats(rule: str, gt_type: str) -> str:
        r_tp = sum(1 for t in tp if t.rule == rule)
        r_all = sum(1 for t in triggers if t.rule == rule)
        g_m = sum(1 for e in matched if e.gt_type == gt_type)
        g_tm = sum(1 for e in true_miss if e.gt_type == gt_type)
        prec = r_tp / r_all if r_all else float("nan")
        rec = g_m / (g_m + g_tm) if (g_m + g_tm) else float("nan")
        return (f"precision {r_tp}/{r_all} = {prec:.1%}   "
                f"adjusted recall {g_m}/{g_m + g_tm} = {rec:.1%}")

    phantoms = [t for t in triggers if t.stale_refire]

    print("\n" + "=" * 66)
    print("PRECISION — engine triggers with a ground-truth counterpart")
    print(f"  overall: {len(tp)}/{len(triggers)} = "
          f"{len(tp) / len(triggers):.1%}" if triggers else "  (no triggers)")
    print(f"  rule1:   {_rule_stats('rule1_continuous_disagreement', 'extended_disagreement')}")
    print(f"  rule2:   {_rule_stats('rule2_orphan_pulse', 'isolated_pulse')}")
    if phantoms:
        projected_n = len(triggers) - len(phantoms)
        print(f"  {len(phantoms)} triggers are stale-refire phantoms "
              f"(identical pulse window re-fired; bug fixed 2026-07-19).")
        print(f"  projected post-fix precision: {len(tp)}/{projected_n} = "
              f"{len(tp) / projected_n:.1%}")

    print("\nRECALL — ground-truth events inside engine coverage "
          f"({len(in_cov)} of {len(gt_events)}; {outside} outside coverage)")
    print(f"  matched:                        {len(matched)}")
    print(f"  expected miss (poll-aliased):   {len(aliased)}  "
          f"(pulse < {aliasing_floor:.1f}s)")
    print(f"  expected miss (export-clipped): {len(boundary)}  "
          f"(truncated at the GT export's start boundary)")
    print(f"  expected miss (cooldown/active):{len(cooldown)}  "
          f"(upper bound — early reset may shorten real cooldowns)")
    print(f"  TRUE MISSES (candidate defects):{len(true_miss)}")
    strict = len(matched) / len(in_cov) if in_cov else float("nan")
    adj_den = len(matched) + len(true_miss)
    adjusted = len(matched) / adj_den if adj_den else float("nan")
    print(f"  strict recall   = {strict:.1%}   "
          f"(counts expected misses against the engine)")
    print(f"  adjusted recall = {adjusted:.1%}   "
          f"(expected misses excluded)")

    # ── Decision → recording delivery ────────────────────────────────────
    # The gap between what the engine decided and what the buffer recorded.
    # These decisions are correctly scored above (they are engine output) but
    # have no clip on disk, so an operator cannot review them.
    if unrecorded is not None:
        joinable = [t for t in triggers if t.recorded is not None]
        dropped = [t for t in joinable if not t.recorded]
        print("\nDELIVERY — decisions that became clips "
              f"({len(joinable)} decisions joinable by Trigger_ID)")
        share = len(dropped) / len(joinable) if joinable else float("nan")
        print(f"  no recording written:           {len(dropped)}  "
              f"= {share:.1%} of decisions")
        for rule in sorted({t.rule for t in dropped}):
            n = sum(1 for t in dropped if t.rule == rule)
            print(f"    {rule}: {n}")
        tp_dropped = sum(1 for t in dropped if t.matched_gt is not None)
        print(f"  …of which ground-truth-matched: {tp_dropped}  "
              f"(real events with no reviewable clip)")
        print("  These rows are the exact population missing from the "
              "recording log;\n  recall measured from that file alone "
              "understates the engine by this much.")

    # ── Per-pair breakdown ───────────────────────────────────────────────
    # A pair with many triggers and zero matches usually means the engine's
    # pairing config and the ATSPM export disagree about that pair, not a
    # timing problem — check detector/phase mapping before anything else.
    all_pairs = sorted({t.pair for t in triggers} | {e.pair for e in in_cov})
    print("\nPer-pair breakdown (engine triggers vs in-coverage GT events):")
    print(f"  {'pair':>7}  {'trig':>4} {'matched':>7} {'phantom':>7}  "
          f"{'GT':>4} {'true_miss':>9}")
    for pair in all_pairs:
        p_trig = [t for t in triggers if t.pair == pair]
        p_gt = [e for e in in_cov if e.pair == pair]
        print(f"  {pair[0]+':'+pair[1]:>7}  {len(p_trig):>4} "
              f"{sum(1 for t in p_trig if t.matched_gt):>7} "
              f"{sum(1 for t in p_trig if t.stale_refire):>7}  "
              f"{len(p_gt):>4} "
              f"{sum(1 for e in p_gt if e.category == 'true_miss'):>9}")

    # ── Categorized lists ────────────────────────────────────────────────
    fp_rows = [
        f"{_fmt_ts(t.start_ts, tz)}  {t.rule.split('_')[0]}  pair {t.pair[0]}:{t.pair[1]}  "
        + ("STALE-REFIRE PHANTOM (fixed)" if t.stale_refire
           else f"nearest GT Δ{t.nearest_diff:.1f}s" if t.nearest_diff is not None
           else "no same-pair GT of this type")
        for t in fp
    ]
    _print_list("Candidate FALSE POSITIVES (unmatched engine triggers)",
                fp_rows, args.verbose)

    tm_rows = [
        f"{_fmt_ts(e.start, tz)}  {e.gt_type}  pair {e.pair[0]}:{e.pair[1]}  "
        f"dur {e.duration:.1f}s  (det {e.on_det} ON)"
        for e in true_miss
    ]
    _print_list("TRUE MISSES (unmatched, un-suppressed, un-aliased GT events)",
                tm_rows, args.verbose)

    cd_rows = [
        f"{_fmt_ts(e.start, tz)}  {e.gt_type}  pair {e.pair[0]}:{e.pair[1]}  "
        f"dur {e.duration:.1f}s"
        for e in cooldown
    ]
    _print_list("Expected misses — cooldown/active suppression",
                cd_rows, args.verbose)

    return 0


if __name__ == "__main__":
    sys.exit(main())
