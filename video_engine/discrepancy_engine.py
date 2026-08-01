"""
discrepancy_engine.py — Co-located detector disagreement detection and trigger generation.

This module is the "Brain" of the integrated system.  It bridges the NTCIP
monitor (which reports raw detector state changes) and the video-buffer package
(which consumes trigger files from the Hot Folder).  It has **zero** imports
from either of those packages; all cross-boundary communication is handled via
atomic JSON files written to a spool directory.

Physical model
──────────────
Each detector *pair* represents **two different sensing technologies** watching
the **exact same physical zone** (e.g., a Radar unit and a Video detection
system both covering the same stop-bar).  The goal is not to measure upstream
vs. downstream travel time; it is to discover which system is misreporting.

Three discrepancy rules
───────────────────────

Rule 1 — Extended Holdover / Missed Call (Continuous Disagreement)
    If System A is ON and System B is OFF, a continuous disagreement timer
    starts at the moment of divergence.  If that exact A-ON / B-OFF state
    persists without interruption for > ``lag_threshold_sec``, a trigger fires.

    Unlike Rule 2, the duration of the disagreement is **not known at trigger
    time**.  The engine therefore:
      • Sends a ``"start"`` trigger with no cooldown engaged.
      • Actively tracks the pair in ``rt.active_trigger_id``.
      • Waits for both detectors to agree again (resolution).
      • Waits an additional ``post_roll_sec`` after resolution.
      • Sends a ``"stop"`` trigger, then engages cooldown.

    Catches:
      • One system picks up a vehicle the other completely misses.
      • One system drops its call while the other continues to hold.

Rule 2 — Orphan Pulse (Ghost Car)
    When System A turns OFF after a brief actuation whose total ON-duration was
    less than ``lag_threshold_sec``, we wait an additional ``lag_threshold_sec``
    window.  If System B was completely OFF during the entire window
    [A_on_time − threshold … A_off_time + threshold], a trigger fires.

    "Completely OFF" is decided against a bounded, time-windowed record of the
    partner's recent ON *intervals* (``_DetectorState.on_intervals``), not a
    single most-recent-edge scalar: the check fires only if **no** partner ON
    interval intersects the observation window **and** the partner is not
    currently ON.  (A scalar cannot represent an interval — it silently dropped
    legitimate orphans whenever the partner actuated *after* the window, and it
    missed mid-window overlaps that a newer partner ON had overwritten.)

    The verdict must also be **fresh**: if the evaluator only reaches the
    candidate more than ``_ORPHAN_DECISION_GRACE_SEC`` after the window closed
    (e.g. the pair sat in cooldown or inside an active Rule 1 recording), the
    candidate is discarded instead of fired — the RAM pre-roll for that moment
    is long gone, so a late trigger would record the wrong footage.

    Because the full orphan window has elapsed by the time the trigger fires,
    the **exact clip duration is known**: ``pre_roll_sec + disagreement_sec +
    post_roll_sec + threshold``.  This is passed as ``max_duration_sec`` in the
    start trigger and cooldown is engaged immediately — no stop trigger is sent.

    Catches:
      • A brief false-positive "ghost" pulse on one system that the other
        never saw at all.
      • Works symmetrically: A brief pulse on B that A never sees also fires.

    Rule 2 is additionally gated by the **sampling floor** (see below): a
    pulse shorter than ``min_pulse_floor_multiple × sampling_floor_sec`` is
    not registered as a candidate at all.

Rule 3 — Chatter Exception (must NOT trigger)
    System A is solidly ON for 30 s; System B is chattering (ON 2 s / OFF 0.5 s
    / ON 2 s …) during the same 30 s.  The continuous disagreement (Rule 1)
    never exceeds ``lag_threshold_sec`` at any single moment, and neither system
    produces an isolated orphan pulse that the other never overlaps.  No trigger
    fires.

Rule 1 active-resolution state machine
───────────────────────────────────────

    ┌──────────────────────────────────────────────────────────────────┐
    │                      _evaluate_pair tick                         │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Cooldown guard ─────────────────────────────────────────────────┐
    │  cooldown_active? → return                                       │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Active Rule 1 tracking (if active_trigger_id is set) ───────────┐
    │                                                                  │
    │  Disagreement resolved? (a_is_on == b_is_on)                    │
    │       YES → set resolution_start_time (if not already set)      │
    │       NO  → clear resolution_start_time (still diverging)       │
    │                                                                  │
    │  Post-roll elapsed? (now − resolution_start_time >= post_roll)  │
    │       YES → _fire_trigger(..., action="stop") → cooldown         │
    │       NO  → return (keep waiting)                                │
    │                                                                  │
    │  CRITICAL: always return here; never evaluate new rules while    │
    │  active_trigger_id is set.                                       │
    └──────────────────────────────────────────────────────────────────┘
                │  (only reached when active_trigger_id is None)
                ▼
    ┌─ Rule 1 new-detection ────────────────────────────────────────────┐
    │  Disagreement > threshold → _fire_trigger(action="start")        │
    │  NOTE: cooldown NOT engaged; active_trigger_id set instead.      │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Rule 2 orphan detection ─────────────────────────────────────────┐
    │  Confirmed orphan → _fire_trigger(action="start",                 │
    │                                   duration_override=exact_dur)    │
    │  Cooldown engaged immediately.                                    │
    └──────────────────────────────────────────────────────────────────┘

Sampling floor (ROADMAP 9 / SCOPE_sampling_floor.md, 2026-07-30)
────────────────────────────────────────────────────────────────
**The engine must not evaluate evidence finer than its own sampling
resolution.**  The upstream detector source samples on a cycle it measures
itself (an NTCIP sweep at ``chunk_size=1`` is RTT-bound — measured median
1.53 s on 2026-07-19, catching only 7–42 % of true detector edges).  Evidence
below that resolution is aliasing, not signal: a *seen* sub-floor pulse fires
while the partner's equally short response pulse is simply *unseen* — the
exact shape of the false-positive storms observed on high-duty channels.

The floor is injected, never imported: ``system_runner`` (the composition
root that already wires both packages) calls :meth:`DiscrepancyMonitor.
set_sampling_floor` at startup from config and thereafter on a slow cadence
from the detector monitor's measured cycle.  ``discrepancy_engine`` still has
zero imports from ``ntcip_monitor``.  A float assignment is atomic under the
GIL — same pattern as ``cooldown_active``.

Three consequences, in order of how much they change behavior:

1. **Rule 2 is gated.**  An orphan candidate is registered only if its pulse
   duration ≥ ``min_pulse_floor_multiple × floor`` (config, default 2.0×).
   Shorter pulses bump a per-pair ``below_floor_suppressed`` counter and are
   dropped at DEBUG.  Note the arithmetic: at the default 1.6 s floor the
   gate is 3.2 s, which *exceeds* a typical ``lag_threshold_sec`` of 2.0 s —
   so Rule 2 is effectively **off** until the sweep gets faster.  That is the
   intended reading of the measurement, not an accident.  After a green
   ``snmp_chunk_size: 8`` probe the floor drops to ~0.2 s and the gate to
   ~0.4 s, which suppresses almost nothing real.
2. **Rule 1 is not gated.**  Its threshold (seconds) is far above any floor.
   One residual imprecision is documented rather than coded: in the
   resolution state machine an *agreement* shorter than the floor is not
   reliable evidence that the disagreement resolved.  This is acceptable
   as-is because a re-divergence restarts the post-roll countdown, so a
   spurious agreement only delays the stop trigger — it never truncates the
   clip.
3. **High-duty pairs get an advisory, not suppression.**  A rolling ON-duty
   fraction per detector (over ``_DUTY_WINDOW_SEC``, computed on the
   evaluator thread from the same pruned ``on_intervals`` deque) drives a
   rate-limited structured WARNING when a pair's *minimum* duty exceeds
   ``high_duty_warn_fraction``: NTCIP data on such a channel is structurally
   unreliable regardless of the rules.  Setting ``suppress_high_duty_pairs``
   (default **false**) fully disables Rules 1+2 for those pairs — a
   deployment decision, off until the owner opts in.

Decision log vs. recording log
──────────────────────────────
The engine appends one row to its own **decision log** (``engine_decisions.
csv``, path injected by ``system_runner``) for every trigger it successfully
emits — *before* anything downstream decides whether that trigger becomes a
clip.  This is deliberately **not** the same artifact as
``discrepancies_log.csv``, which the video-buffer backend writes only after
``_writer_semaphore.acquire()`` succeeds: a trigger dropped by the
``max_concurrent_writers`` cap, by a low-disk abort, or by an unmatched camera
leaves no row there at all.  Measuring engine recall against the recording log
therefore charges the engine for the buffer's back-pressure (on the 2026-07-31
run the cap was saturated 11.6 % of wall clock yet accounted for 43 % of the
apparent misses).  Score accuracy against *this* log; diff the two to measure
what the buffer dropped.

Rows carry the underlying event's start/end as exact Unix floats
(``event_start_ts`` / ``event_end_ts``) rather than leaving a consumer to
recover them from a 1-second local timestamp and a regex over the description.
Either may be blank where the rule does not define it — a Rule 1 ``start``
knows when the disagreement began but not when it ends, and a ``stop`` row
describes neither.

Writing is best-effort and strictly downstream of trigger delivery: a failed
append logs an ERROR and is otherwise swallowed, because a full or read-only
disk must never stop a clip from being recorded.

Thread-safety contract
──────────────────────
``on_detector_on`` / ``on_detector_off`` are called from the NTCIP monitor's
event-dispatch thread and must return in microseconds.  They only acquire a
per-detector ``threading.Lock`` long enough to mutate four scalar fields plus,
on a falling edge, one O(1) ``deque.append`` recording the just-closed ON
interval.

The background evaluator thread acquires the same locks briefly to snapshot
each detector's state (pruning expired ON intervals while it holds the lock —
never from the callback path), then releases them before any comparison or I/O.

All ``_PairRuntimeState`` fields are written exclusively by the evaluator thread
except ``cooldown_active``, which the NTCIP callback thread may clear via the
early-reset path.  A boolean assignment is atomic under CPython's GIL and the
evaluator tolerates a one-tick stale read in the rare race window.  The
sampling floor is written by whatever thread calls
:meth:`DiscrepancyMonitor.set_sampling_floor` (in production, ``system_runner``'s
slow updater thread) and read by the evaluator — again a single atomic float
assignment, with a one-tick stale read tolerated by design.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
import pytz
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, List, Optional, Sequence, Tuple

from config_manager import ConfigProvider, ConfigProviderError

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# Maximum staleness of a Rule 2 verdict: an orphan candidate whose observation
# window closed more than this many seconds ago is discarded instead of fired
# (the pre-roll footage for it no longer exists).  Sized to tolerate evaluator
# scheduling jitter (ticks are 0.1 s) while staying far below the cooldown.
_ORPHAN_DECISION_GRACE_SEC = 2.0

# Hard cap on the partner ON-interval history deque.  The time-based pruning in
# the evaluator is the real bound (now ``max(~3 × threshold, _DUTY_WINDOW_SEC)``
# — the duty computation reads the same deque); this maxlen is a
# belt-and-suspenders RAM cap.  At the 0.2 s NTCIP poll floor a detector can
# produce at most ~2.5 intervals/s, so 512 covers the 120 s duty window with
# ~1.7× headroom (and ~6× at today's measured 1.5 s floor) for ~8 KB/detector.
_PARTNER_INTERVAL_MAXLEN = 512

# ---------------------------------------------------------------------------
# Sampling-floor defaults (ROADMAP 9 — see the module docstring)
# ---------------------------------------------------------------------------

# Assumed effective sampling cycle until system_runner injects a measured one.
# 1.6 s = today's measured NTCIP reality (median sweep 1.53 s at chunk_size 1).
_DEFAULT_SAMPLING_FLOOR_SEC = 1.6

# Rule 2 trusts an orphan pulse only if it lasted at least this many sampling
# cycles.  2.0 is the Nyquist-flavoured minimum: below two samples the "pulse"
# and the partner's absence of one are equally likely to be aliasing.
_DEFAULT_MIN_PULSE_FLOOR_MULTIPLE = 2.0

# A pair whose *minimum* detector ON-duty exceeds this fraction is flagged as
# operating outside the regime where NTCIP sampling can resolve its edges.
_DEFAULT_HIGH_DUTY_WARN_FRACTION = 0.8

# Rolling window over which ON-duty is measured.
_DUTY_WINDOW_SEC = 120.0

# Duty is an advisory statistic, not a per-tick decision input: recomputing it
# on every 0.1 s tick would walk the interval deques 10×/s per pair for no
# added fidelity on a 120 s window.  J1900-class CPUs care.
_DUTY_EVAL_INTERVAL_SEC = 5.0

# Minimum spacing between high-duty WARNINGs for the same pair.  The condition
# is a standing property of the channel, so this is a heartbeat, not an event.
_HIGH_DUTY_WARN_INTERVAL_SEC = 600.0

# ---------------------------------------------------------------------------
# Decision log (ROADMAP 9C1 — see the module docstring)
# ---------------------------------------------------------------------------

# Column order of ``engine_decisions.csv``.  Append-only: readers key on the
# header, and an existing file is never rewritten, so new columns go on the
# END of this list or a resumed log's rows stop lining up with its header.
# ``event_timestamp`` doubles as the format discriminator for consumers that
# also accept the video-buffer's ``discrepancies_log.csv``.
_DECISION_LOG_FIELDS = (
    "event_timestamp",      # exact Unix float — when the engine decided
    "local_timestamp",      # same instant, intersection-local, for humans
    "intersection_id",
    "trigger_id",
    "action",               # "start" | "stop"
    "rule",
    "pair_key",
    "det_a",
    "det_b",
    "det_a_type",
    "det_b_type",
    "event_start_ts",       # exact Unix start of the underlying event, or ""
    "event_end_ts",         # exact Unix end of the underlying event, or ""
    "disagreement_sec",
    "max_duration_sec",
    "cameras",              # ";"-joined camera IDs, as sent in the trigger
    "description",
)

# ---------------------------------------------------------------------------
# Timezone resolution helper
# ---------------------------------------------------------------------------

def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> pytz.BaseTzInfo:
    """Resolve an IANA timezone name to a :mod:`pytz` timezone object.

    ``pytz`` carries its own full copy of the IANA timezone database, so this
    function has **no dependency on system zone files or the** ``tzdata``
    **package**.  ``US/Mountain``, ``America/Boise``, and all other canonical
    and legacy aliases are resolved directly from the bundled database.

    Falls back to ``pytz.utc`` (equivalent to UTC) and emits a structured
    warning when the name is genuinely unknown — i.e. not present in pytz's
    bundled database at all.

    Args:
        tz_name: IANA timezone name string (e.g. ``"America/Boise"`` or the
            legacy alias ``"US/Mountain"``).
        fallback_log: Logger used to emit the structured warning on failure.

    Returns:
        A :mod:`pytz` timezone object.  Always returns a valid object; never
        raises.
    """
    try:
        return pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        fallback_log.warning(
            "Unknown IANA timezone name; falling back to UTC",
            extra={"timezone": tz_name},
        )
        return pytz.utc


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------

@dataclass
class _DetectorState:
    """Mutable state for a single physical detector / sensing system.

    All fields are protected by ``lock`` and must not be read or written
    outside of a ``with self.lock`` block.

    Attributes:
        detector_id: String identifier matching the config schema.
        is_on: Current ON/OFF state.
        last_on_time: ``time.time()`` of the most recent rising edge (ON).
            Zero if the detector has never been seen ON.
        last_off_time: ``time.time()`` of the most recent falling edge (OFF).
            Zero if the detector has never been seen OFF.
        last_pulse_on_time: ``time.time()`` of the ON-edge that immediately
            preceded the most recent OFF-edge.  Updated on every rising edge
            so the evaluator can reconstruct the pulse window
            ``[last_pulse_on_time, last_off_time]`` even after the detector
            has already transitioned back to OFF.
        on_intervals: Bounded history of recently *completed* ON intervals as
            ``(on_ts, off_ts)`` tuples, appended on each falling edge.  This is
            what Rule 2 consults to decide whether this detector (as the
            partner) overlapped an orphan candidate's observation window — a
            deque because a single most-recent-edge scalar cannot represent an
            interval.  The evaluator thread prunes entries older than the
            largest window Rule 2 ever inspects; ``maxlen`` is only a RAM
            backstop.  The *current* ON (if any) is not in the deque — it is
            represented by ``is_on`` / ``last_on_time``.
    """

    detector_id: str
    is_on: bool = False
    last_on_time: float = 0.0
    last_off_time: float = 0.0
    last_pulse_on_time: float = 0.0
    on_intervals: Deque[Tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=_PARTNER_INTERVAL_MAXLEN)
    )
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class _PairRuntimeState:
    """Per-pair mutable runtime bookkeeping owned exclusively by the evaluator thread.

    Because only the evaluator thread writes these fields, no additional lock
    is needed beyond the GIL.  The single exception is ``cooldown_active``,
    which the NTCIP callback thread may clear via the early-reset path; a
    boolean assignment is atomic under CPython's GIL and the evaluator
    tolerates a one-tick stale read in the rare race window.

    Attributes:
        pair_key: Canonical sorted pair identifier, e.g. ``"1:2"``.
        disagreement_start: ``time.time()`` when continuous one-ON / one-OFF
            divergence began, or ``None`` when both detectors agree.
        cooldown_active: ``True`` while the pair is inside a post-trigger
            cooldown window.
        triggered_at: ``time.time()`` when the most recent trigger fired.
        orphan_watch_a: Pending Rule-2 candidate for detector A as a
            ``(pulse_on_time, pulse_off_time)`` tuple, or ``None``.
        orphan_watch_b: Same for detector B.
        active_trigger_id: Hex trigger ID of an in-progress Rule 1 recording,
            or ``None`` when no Rule 1 recording is active.  While this field
            is set the evaluator skips all new-detection logic and runs only
            the resolution state machine.
        resolution_start_time: ``time.time()`` when both detectors first agreed
            again after a Rule 1 disagreement, marking the start of the
            post-roll countdown.  ``None`` if the disagreement is still ongoing
            or if no Rule 1 recording is active.
        below_floor_suppressed: Count of orphan candidates rejected because the
            pulse was shorter than the sampling floor allows us to trust.  A
            large value relative to fired triggers means the pair is being
            sampled too slowly to judge — it is diagnostic, never an input to
            any rule.
        last_duty_eval_ts: ``time.time()`` of the most recent ON-duty
            computation for this pair (throttled to ``_DUTY_EVAL_INTERVAL_SEC``).
        pair_min_duty: Most recently computed ``min(duty_a, duty_b)``.
        high_duty_active: Cached verdict of the last duty computation — whether
            ``pair_min_duty`` exceeded ``high_duty_warn_fraction``.
        last_high_duty_warn_ts: ``time.time()`` of the last high-duty WARNING
            emitted for this pair (rate limit).
    """

    pair_key: str
    disagreement_start: Optional[float] = None
    cooldown_active: bool = False
    triggered_at: float = 0.0
    orphan_watch_a: Optional[Tuple[float, float]] = None
    orphan_watch_b: Optional[Tuple[float, float]] = None
    # Rule 1 active-resolution tracking (new in Session 5)
    active_trigger_id: Optional[str] = None
    resolution_start_time: Optional[float] = None
    # Monotonic ON-edge timestamp of the most recent orphan pulse already
    # registered for each slot.  Prevents a single stale pulse from being
    # re-armed (and re-fired) after each cooldown expiry when the detector has
    # not actuated again.  See _maybe_register_orphan.
    last_handled_pulse_on_a: float = 0.0
    last_handled_pulse_on_b: float = 0.0
    # Sampling-floor bookkeeping (ROADMAP 9).
    below_floor_suppressed: int = 0
    last_duty_eval_ts: float = 0.0
    pair_min_duty: float = 0.0
    high_duty_active: bool = False
    last_high_duty_warn_ts: float = 0.0


# ---------------------------------------------------------------------------
# Pure disagreement-logic functions (no side-effects — fully unit-testable)
# ---------------------------------------------------------------------------

def _check_rule1_continuous(
    disagreement_start: Optional[float],
    now: float,
    threshold: float,
) -> Tuple[bool, float]:
    """Rule 1: detect a continuous one-ON / one-OFF disagreement exceeding the threshold.

    Called on every evaluator tick while one detector is ON and the other is OFF.

    Args:
        disagreement_start: Wall-clock time when the current divergence began,
            or ``None`` if the detectors are not currently diverged.
        now: Current ``time.time()``.
        threshold: Allowable disagreement window in seconds
            (``lag_threshold_sec`` from detector config).

    Returns:
        A ``(should_fire, duration_seconds)`` tuple.  ``should_fire`` is
        ``True`` when the continuous disagreement has exceeded ``threshold``.
    """
    if disagreement_start is None:
        return False, 0.0
    duration = now - disagreement_start
    return duration > threshold, round(duration, 3)


def _check_rule2_orphan(
    pulse_on: float,
    pulse_off: float,
    other_intervals: Sequence[Tuple[float, float]],
    other_is_on: bool,
    now: float,
    threshold: float,
    decision_grace_sec: float = _ORPHAN_DECISION_GRACE_SEC,
) -> Tuple[bool, str]:
    """Rule 2: detect an orphan pulse — a brief actuation the partner never saw.

    The observation window is ``[pulse_on − threshold … pulse_off + threshold]``.
    The check is deferred until ``now >= pulse_off + threshold`` so the full
    post-pulse grace period has elapsed before a verdict is rendered, and it is
    abandoned once ``now > pulse_off + threshold + decision_grace_sec`` — a
    verdict that late (pair was in cooldown or inside an active Rule 1
    recording) would trigger a clip whose pre-roll footage no longer exists.

    Rule 3 protection is embedded here: if **any** completed partner ON
    interval intersects the observation window, or the partner is currently
    ON, the check is suppressed.  Interval intersection (rather than a single
    most-recent-edge comparison) is what makes a partner actuation *after* the
    window correctly non-suppressing, and a partner ON that merely straddles
    the window boundary correctly suppressing.

    Args:
        pulse_on: ``time.time()`` of the orphan candidate's ON-edge.
        pulse_off: ``time.time()`` of the orphan candidate's OFF-edge.
        other_intervals: Completed ON intervals of the partner detector as
            ``(on_ts, off_ts)`` tuples, most-recent-last.  Need only cover the
            observation window — older entries are ignored by the overlap test.
        other_is_on: Current ON state of the partner detector.
        now: Current ``time.time()``.
        threshold: Allowable disagreement window in seconds.
        decision_grace_sec: Maximum verdict staleness past the window end.

    Returns:
        A ``(should_fire, description)`` tuple.  ``should_fire`` is ``True``
        when the orphan condition is confirmed.  ``description`` is a
        human-readable string for trigger metadata.
    """
    if now < pulse_off + threshold:
        return False, ""

    window_start = pulse_on - threshold
    window_end   = pulse_off + threshold

    if now > window_end + decision_grace_sec:
        return False, ""

    if other_is_on:
        return False, ""

    for on_ts, off_ts in other_intervals:
        if on_ts <= window_end and off_ts >= window_start:
            return False, ""

    pulse_duration = round(pulse_off - pulse_on, 3)
    desc = (
        f"orphan pulse duration={pulse_duration}s "
        f"window=[{window_start:.3f}, {window_end:.3f}]"
    )
    return True, desc


def _compute_on_duty_fraction(
    intervals: Sequence[Tuple[float, float]],
    is_on: bool,
    last_on_time: float,
    now: float,
    window_sec: float,
) -> float:
    """Compute the fraction of the trailing window a detector spent ON.

    Reads the same ``on_intervals`` history Rule 2 consults, plus the
    still-open ON interval implied by ``is_on`` / ``last_on_time``.  Intervals
    are clipped to ``[now − window_sec, now]``, so partial overlaps at either
    edge contribute only the part that falls inside the window.  Completed
    intervals never overlap each other (they are consecutive ON periods of one
    detector), so clipped durations can simply be summed.

    The result is only as complete as the retained history: entries pruned by
    the evaluator or evicted by the deque's ``maxlen`` are gone, which biases
    the fraction *downwards*.  That is the safe direction — it can make the
    high-duty advisory miss, never make it fire spuriously.

    Args:
        intervals: Completed ON intervals as ``(on_ts, off_ts)`` tuples.
        is_on: Whether the detector is currently ON.
        last_on_time: ``time.time()`` of the most recent rising edge; used
            only when ``is_on`` is ``True``.
        now: Current ``time.time()``.
        window_sec: Length of the trailing window in seconds.

    Returns:
        ON-duty fraction in ``[0.0, 1.0]``.  Returns ``0.0`` for a
        non-positive ``window_sec``.
    """
    if window_sec <= 0:
        return 0.0

    window_start = now - window_sec
    on_time = 0.0

    for on_ts, off_ts in intervals:
        start = max(on_ts, window_start)
        end = min(off_ts, now)
        if end > start:
            on_time += end - start

    if is_on and last_on_time > 0.0:
        start = max(last_on_time, window_start)
        if now > start:
            on_time += now - start

    return min(1.0, on_time / window_sec)


# ---------------------------------------------------------------------------
# DiscrepancyMonitor
# ---------------------------------------------------------------------------

class DiscrepancyMonitor:
    """Monitors co-located detector pairs and writes trigger files on disagreement.

    Each pair represents two sensing systems covering the same physical zone.
    The monitor applies three rules (see module docstring) and writes atomic
    JSON trigger files to the Hot Folder.

    Rule 1 recordings are actively managed with paired ``"start"`` /
    ``"stop"`` triggers.  Rule 2 recordings are self-contained ``"start"``
    triggers with an exact ``max_duration_sec``.

    This class is entirely decoupled from both ``ntcip_monitor`` and
    ``video_buffer``.

    Args:
        intersection_id: Canonical intersection identifier (e.g.
            ``"1234_main"``).  Must exist in the ``ConfigProvider``.
        config_provider: A :class:`~config_manager.ConfigProvider` instance
            used to fetch detector pairing rules, thresholds, and camera IDs.
        trigger_dir: Path to the Hot Folder directory where trigger JSON
            files will be written.  Created automatically if absent.
        cooldown_sec: Seconds to suppress re-triggering the same pair after a
            trigger fully completes.
        evaluator_interval_sec: Sleep duration between evaluator ticks.
            Default 0.1 s (100 ms).
        pre_roll_sec: Pre-roll seconds embedded in every trigger file and
            used in the Rule 2 exact-duration calculation.
        post_roll_sec: Seconds to wait after Rule 1 resolution before
            sending the ``"stop"`` trigger.  Also embedded in every trigger.
        max_duration_sec: Hard recording cap used as ``max_duration_sec``
            for Rule 1 triggers only (video-buffer safety net if the stop
            trigger is somehow missed).
        decision_log_path: Optional CSV path receiving one row per emitted
            trigger — the engine's own record of what it decided, kept
            separate from the video buffer's record of what it managed to
            record (see the module docstring).  ``None`` disables it.
            Parent directories are created on first write.

    Sampling-floor configuration (read from the intersection config; see the
    module docstring for why these exist):

    ``min_pulse_floor_multiple``
        Rule 2 pulse-length gate, in multiples of the sampling floor.
        Default ``2.0``.
    ``high_duty_warn_fraction``
        Pair min-ON-duty above which the advisory WARNING fires.  Default
        ``0.8``.
    ``suppress_high_duty_pairs``
        When ``True``, Rules 1+2 are disabled entirely for pairs over that
        duty threshold.  Default ``False`` — advisory only.

    The floor itself is **not** read from config here; it is injected by
    ``system_runner`` via :meth:`set_sampling_floor` (at startup from the
    config's ``sampling_floor_sec``, then from the detector monitor's measured
    cycle).  Until then the built-in default applies.

    Example::

        provider = JsonFileConfigProvider("/etc/traffic/intersections.json")
        monitor = DiscrepancyMonitor(
            intersection_id="1234_main",
            config_provider=provider,
            trigger_dir="./trigger_queue",
        )
        monitor.start()
    """

    def __init__(
        self,
        intersection_id: str,
        config_provider: ConfigProvider,
        trigger_dir: str | Path,
        cooldown_sec: float = 60.0,
        evaluator_interval_sec: float = 0.1,
        pre_roll_sec: float = 10.0,
        post_roll_sec: float = 20.0,
        max_duration_sec: float = 300.0,
        decision_log_path: Optional[str | Path] = None,
    ) -> None:
        self._intersection_id = intersection_id
        self._trigger_dir = Path(trigger_dir)
        self._cooldown_sec = cooldown_sec
        self._evaluator_interval_sec = evaluator_interval_sec
        self._pre_roll_sec = pre_roll_sec
        self._post_roll_sec = post_roll_sec
        self._max_duration_sec = max_duration_sec
        self._decision_log_path = (
            Path(decision_log_path) if decision_log_path else None
        )
        self._log = logging.getLogger(f"{__name__}.{intersection_id}")

        self._trigger_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._intersection_cfg = config_provider.get_intersection_config(
                intersection_id
            )
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Cannot load config for intersection '{intersection_id}': {exc}"
            ) from exc

        # ── Sampling floor (ROADMAP 9) ────────────────────────────────────
        # The floor itself is injected by the composition root; the tuning
        # knobs around it are engine-internal and come from the config block
        # the engine already owns.
        self._sampling_floor_sec = _DEFAULT_SAMPLING_FLOOR_SEC
        self._apply_floor_config()

        self._detector_states: Dict[str, _DetectorState] = {}
        self._pairs: Dict[str, Tuple[str, str]] = {}
        self._pair_runtime: Dict[str, _PairRuntimeState] = {}
        self._build_structures()

        self._running = False
        self._evaluator_thread = threading.Thread(
            target=self._evaluator_loop,
            name=f"evaluator-{intersection_id}",
            daemon=True,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background evaluator thread.

        Idempotent — subsequent calls while running are silently ignored.
        """
        if self._running:
            return
        self._running = True
        self._evaluator_thread.start()
        self._log.info(
            "DiscrepancyMonitor started",
            extra={
                "intersection_id": self._intersection_id,
                "pairs": list(self._pairs.keys()),
                "algorithm": "co-located-disagreement",
            },
        )

    def stop(self) -> None:
        """Signal the evaluator thread to exit and wait for it to join."""
        self._running = False
        self._evaluator_thread.join(timeout=self._evaluator_interval_sec * 5)
        self._log.info(
            "DiscrepancyMonitor stopped",
            extra={"intersection_id": self._intersection_id},
        )

    def reload(self, config_provider: ConfigProvider) -> None:
        """Hot-reload intersection configuration without stopping the monitor.

        Active cooldown state is preserved; transient fields (disagreement
        timers, orphan watches, Rule 1 active-tracking) are reset.

        Args:
            config_provider: Provider instance to fetch fresh config from.

        Raises:
            ValueError: If the intersection cannot be found or loaded.
        """
        try:
            new_cfg = config_provider.get_intersection_config(self._intersection_id)
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Reload failed for intersection '{self._intersection_id}': {exc}"
            ) from exc
        self._intersection_cfg = new_cfg
        self._apply_floor_config()
        self._build_structures(preserve_existing=True)
        self._log.info(
            "Configuration reloaded",
            extra={"intersection_id": self._intersection_id},
        )

    def _apply_floor_config(self) -> None:
        """Re-read the sampling-floor tuning knobs from the intersection config.

        Called from ``__init__`` and :meth:`reload`.  Does **not** touch
        ``_sampling_floor_sec`` itself — that value is owned by whoever calls
        :meth:`set_sampling_floor`, and a config reload must not clobber a
        measured floor with a stale assumption.
        """
        self._min_pulse_floor_multiple = float(
            self._intersection_cfg.get(
                "min_pulse_floor_multiple", _DEFAULT_MIN_PULSE_FLOOR_MULTIPLE
            )
        )
        self._high_duty_warn_fraction = float(
            self._intersection_cfg.get(
                "high_duty_warn_fraction", _DEFAULT_HIGH_DUTY_WARN_FRACTION
            )
        )
        self._suppress_high_duty_pairs = bool(
            self._intersection_cfg.get("suppress_high_duty_pairs", False)
        )

    def set_sampling_floor(self, sec: float) -> None:
        """Set the effective sampling resolution of the upstream detector source.

        This is the engine's only channel for that information — the package
        boundary forbids importing ``ntcip_monitor``, so the composition root
        (``system_runner``) measures it there and pushes it here: once at
        startup from the config's ``sampling_floor_sec``, then periodically
        from ``DetectorMonitor.effective_cycle_sec()``.

        Thread-safe by construction: a single float assignment is atomic under
        CPython's GIL, and the evaluator tolerates reading the previous value
        for one tick.

        Args:
            sec: Measured (or configured) seconds between consecutive samples
                of a detector's state.  Non-positive values are ignored — an
                unmeasured source must not silently disable the gate.
        """
        try:
            value = float(sec)
        except (TypeError, ValueError):
            return
        if value <= 0.0:
            return

        previous = self._sampling_floor_sec
        self._sampling_floor_sec = value

        # Only log meaningful movement; the updater calls this once a minute.
        if abs(value - previous) >= 0.05:
            self._log.info(
                "Sampling floor updated",
                extra={
                    "intersection_id": self._intersection_id,
                    "sampling_floor_sec": round(value, 4),
                    "previous_sampling_floor_sec": round(previous, 4),
                    "min_orphan_pulse_sec": round(
                        value * self._min_pulse_floor_multiple, 4
                    ),
                },
            )

    def get_sampling_floor(self) -> float:
        """Get the sampling floor currently in force.

        Returns:
            Seconds between consecutive samples of a detector's state, as last
            set by :meth:`set_sampling_floor` (or the built-in default).
        """
        return self._sampling_floor_sec

    # ------------------------------------------------------------------
    # Non-blocking callbacks (called from NTCIP event thread)
    # ------------------------------------------------------------------

    def on_detector_on(self, detector_id: str) -> None:
        """Record a rising edge for ``detector_id``.

        Must execute in microseconds — no I/O, no blocking calls.
        Silently ignores unknown detector IDs.

        Args:
            detector_id: String detector identifier (coerced to ``str``).
        """
        key = str(detector_id)
        state = self._detector_states.get(key)
        if state is None:
            return
        with state.lock:
            state.is_on = True
            now = time.time()
            state.last_on_time = now
            state.last_pulse_on_time = now

    def on_detector_off(self, detector_id: str) -> None:
        """Record a falling edge for ``detector_id``.

        Must execute in microseconds — no I/O, no blocking calls.
        Also initiates an early-cooldown-reset check.

        Args:
            detector_id: String detector identifier (coerced to ``str``).
        """
        key = str(detector_id)
        state = self._detector_states.get(key)
        if state is None:
            return
        with state.lock:
            state.is_on = False
            now = time.time()
            state.last_off_time = now
            # last_pulse_on_time intentionally NOT updated here — it retains
            # the ON-edge value so the evaluator can form [pulse_on, pulse_off].
            if state.last_pulse_on_time > 0.0:
                # Close out the ON interval for the partner-overlap history.
                # O(1) append under the already-held lock; pruning is the
                # evaluator thread's job.
                state.on_intervals.append((state.last_pulse_on_time, now))

        self._maybe_reset_cooldown_early(key)

    # ------------------------------------------------------------------
    # Background evaluator
    # ------------------------------------------------------------------

    def _evaluator_loop(self) -> None:
        """Daemon loop: evaluate all configured pairs on every tick."""
        while self._running:
            for pair_key, (det_a_id, det_b_id) in self._pairs.items():
                try:
                    self._evaluate_pair(pair_key, det_a_id, det_b_id)
                except Exception:  # noqa: BLE001
                    self._log.exception(
                        "Unhandled error in evaluator loop",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key": pair_key,
                        },
                    )
            time.sleep(self._evaluator_interval_sec)

    def _evaluate_pair(
        self,
        pair_key: str,
        det_a_id: str,
        det_b_id: str,
    ) -> None:
        """Apply Rules 1–3 to a single co-located detector pair each tick.

        Execution order:

        1. **Cooldown guard** — return immediately while in cooldown.
        2. **Rule 1 resolution state machine** — if ``rt.active_trigger_id``
           is set, manage the resolution countdown and send a ``"stop"``
           trigger when post-roll elapses.  Always ``return`` after this block.
        3. **State snapshot** — read both detectors under brief locks.
        3b. **High-duty advisory** — recompute the pair's rolling ON-duty at
           most every ``_DUTY_EVAL_INTERVAL_SEC``; warn (rate-limited) when it
           exceeds ``high_duty_warn_fraction``, and return early only if
           ``suppress_high_duty_pairs`` is enabled.
        4. **Rule 1 new-detection** — start/continue disagreement timer; fire
           ``"start"`` trigger (no cooldown) if threshold exceeded.
        5. **Rule 2 orphan detection** — register candidates (subject to the
           sampling-floor gate); fire ``"start"`` trigger with exact
           ``duration_override`` when confirmed.

        Args:
            pair_key: Canonical pair key used for runtime state lookup.
            det_a_id: First detector ID in the pair.
            det_b_id: Second detector ID in the pair.
        """
        rt  = self._pair_runtime[pair_key]
        now = time.time()

        # ── 1. Cooldown guard ─────────────────────────────────────────────
        if rt.cooldown_active:
            if now - rt.triggered_at < self._cooldown_sec:
                return
            rt.cooldown_active = False
            self._log.debug(
                "Pair cooldown expired",
                extra={
                    "intersection_id": self._intersection_id,
                    "pair_key": pair_key,
                },
            )

        # ── 2. Rule 1 active-resolution state machine ─────────────────────
        #
        # This block executes only when a Rule 1 "start" trigger has been
        # sent and we are waiting for the disagreement to resolve and the
        # post-roll to elapse before sending the "stop" trigger.
        if rt.active_trigger_id is not None:
            state_a = self._detector_states.get(det_a_id)
            state_b = self._detector_states.get(det_b_id)
            if state_a is None or state_b is None:
                # Detectors vanished during a reload; abandon the recording.
                rt.active_trigger_id     = None
                rt.resolution_start_time = None
                return

            with state_a.lock:
                a_is_on = state_a.is_on
            with state_b.lock:
                b_is_on = state_b.is_on

            # Resolution = both detectors agree (both ON or both OFF).
            both_agree = (a_is_on == b_is_on)

            if both_agree:
                # Start the post-roll countdown on the first tick of agreement.
                if rt.resolution_start_time is None:
                    rt.resolution_start_time = now
                    self._log.debug(
                        "Rule 1 disagreement resolved — post-roll countdown started",
                        extra={
                            "intersection_id":  self._intersection_id,
                            "pair_key":         pair_key,
                            "trigger_id":       rt.active_trigger_id,
                            "post_roll_sec":    self._post_roll_sec,
                        },
                    )

                # Send the "stop" trigger once post-roll has elapsed.
                post_roll_elapsed = now - rt.resolution_start_time
                if post_roll_elapsed >= self._post_roll_sec:
                    self._fire_trigger(
                        pair_key=pair_key,
                        det_a_id=det_a_id,
                        det_b_id=det_b_id,
                        rule="rule1_continuous_disagreement",
                        description=(
                            f"Rule 1 recording stopped after "
                            f"{post_roll_elapsed:.1f}s post-roll"
                        ),
                        disagreement_sec=0.0,   # not meaningful for stop
                        event_ts=now,
                        action="stop",
                        trigger_id_override=rt.active_trigger_id,
                    )
                    # State cleanup and cooldown engagement are handled inside
                    # _fire_trigger when action == "stop".
            else:
                # Still disagreeing — reset the post-roll countdown.  If the
                # detectors briefly agree then diverge again (tail-end chatter),
                # the countdown restarts from zero.
                if rt.resolution_start_time is not None:
                    self._log.debug(
                        "Rule 1 re-diverged during post-roll — timer reset",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key":        pair_key,
                            "trigger_id":      rt.active_trigger_id,
                        },
                    )
                rt.resolution_start_time = None

            # CRITICAL: always return here.  While a Rule 1 recording is
            # active we must not evaluate new Rule 1 or Rule 2 candidates —
            # doing so could fire a second "start" trigger for the same pair.
            return

        # ── 3. Snapshot both detector states ──────────────────────────────
        state_a = self._detector_states.get(det_a_id)
        state_b = self._detector_states.get(det_b_id)
        if state_a is None or state_b is None:
            return

        det_cfg   = self._intersection_cfg["detectors"].get(det_a_id, {})
        threshold = float(det_cfg.get("lag_threshold_sec", 2.0))

        # Rule 2 can never be intersected by an ON interval older than
        # 3×threshold + grace (window span plus verdict staleness), but the
        # ON-duty advisory reads the same deque over a much longer window, so
        # the retention horizon is the larger of the two.  Pruning happens
        # while the lock is held; the callback path never prunes.
        prune_before = now - max(
            3.0 * threshold + _ORPHAN_DECISION_GRACE_SEC + 1.0,
            _DUTY_WINDOW_SEC,
        )

        with state_a.lock:
            a_is_on         = state_a.is_on
            a_last_on       = state_a.last_on_time
            a_last_off      = state_a.last_off_time
            a_last_pulse_on = state_a.last_pulse_on_time
            while state_a.on_intervals and state_a.on_intervals[0][1] < prune_before:
                state_a.on_intervals.popleft()
            a_intervals     = tuple(state_a.on_intervals)

        with state_b.lock:
            b_is_on         = state_b.is_on
            b_last_on       = state_b.last_on_time
            b_last_off      = state_b.last_off_time
            b_last_pulse_on = state_b.last_pulse_on_time
            while state_b.on_intervals and state_b.on_intervals[0][1] < prune_before:
                state_b.on_intervals.popleft()
            b_intervals     = tuple(state_b.on_intervals)

        # ── 3b. High-duty advisory ────────────────────────────────────────
        if now - rt.last_duty_eval_ts >= _DUTY_EVAL_INTERVAL_SEC:
            rt.last_duty_eval_ts = now
            duty_a = _compute_on_duty_fraction(
                a_intervals, a_is_on, a_last_on, now, _DUTY_WINDOW_SEC
            )
            duty_b = _compute_on_duty_fraction(
                b_intervals, b_is_on, b_last_on, now, _DUTY_WINDOW_SEC
            )
            rt.pair_min_duty    = min(duty_a, duty_b)
            rt.high_duty_active = rt.pair_min_duty > self._high_duty_warn_fraction

            if (
                rt.high_duty_active
                and now - rt.last_high_duty_warn_ts >= _HIGH_DUTY_WARN_INTERVAL_SEC
            ):
                rt.last_high_duty_warn_ts = now
                self._log.warning(
                    "Pair operates above the NTCIP sampling-reliability regime",
                    extra={
                        "intersection_id":         self._intersection_id,
                        "pair_key":                pair_key,
                        "duty_a":                  round(duty_a, 3),
                        "duty_b":                  round(duty_b, 3),
                        "duty_window_sec":         _DUTY_WINDOW_SEC,
                        "high_duty_warn_fraction": self._high_duty_warn_fraction,
                        "sampling_floor_sec":      round(self._sampling_floor_sec, 4),
                        "suppressed":              self._suppress_high_duty_pairs,
                    },
                )

        if rt.high_duty_active and self._suppress_high_duty_pairs:
            # Opt-in deployment decision: this pair's edges are not resolvable
            # at the current sampling rate, so run no rules against it at all.
            # Clear the Rule 1 timer as we go: leaving it set would mean that
            # the first tick after the pair's duty falls back below the
            # threshold measures a disagreement from a timestamp before the
            # suppression began, and fires immediately.
            rt.disagreement_start = None
            return

        # ── 4. Rule 1 — Continuous Disagreement (new-detection path) ─────
        both_agree = (a_is_on == b_is_on)

        if both_agree:
            rt.disagreement_start = None
        else:
            if rt.disagreement_start is None:
                rt.disagreement_start = now

            fire, duration = _check_rule1_continuous(
                rt.disagreement_start, now, threshold
            )
            if fire:
                leading = det_a_id if a_is_on else det_b_id
                lagging = det_b_id if a_is_on else det_a_id
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule1_continuous_disagreement",
                    description=(
                        f"detector '{leading}' ON, detector '{lagging}' OFF "
                        f"for {duration}s (threshold={threshold}s)"
                    ),
                    disagreement_sec=duration,
                    event_ts=now,
                    action="start",
                    # No duration_override for Rule 1 — max_duration_sec is
                    # only a hard safety cap; the engine sends an explicit stop.
                    # The disagreement is still open, so it has no end yet.
                    event_window=(now - duration, None),
                )
                # Reset timer; active_trigger_id now set by _fire_trigger.
                rt.disagreement_start = None
                return  # skip Rule 2 on this tick

        # ── 5. Rule 2 — Orphan Pulse ──────────────────────────────────────
        # Sampling-floor gate: a pulse the source could not have resolved is
        # not evidence of anything (ROADMAP 9).  Read the floor once so both
        # slots are judged against the same value even if it changes mid-tick.
        min_pulse_sec = self._sampling_floor_sec * self._min_pulse_floor_multiple

        self._maybe_register_orphan(
            rt, "a", a_is_on, a_last_pulse_on, a_last_off, threshold,
            min_pulse_sec,
        )
        self._maybe_register_orphan(
            rt, "b", b_is_on, b_last_pulse_on, b_last_off, threshold,
            min_pulse_sec,
        )

        # Evaluate detector-A orphan candidate against detector-B history.
        if rt.orphan_watch_a is not None:
            pulse_on, pulse_off = rt.orphan_watch_a
            fire, desc = _check_rule2_orphan(
                pulse_on, pulse_off,
                b_intervals, b_is_on,
                now, threshold,
            )
            if fire:
                disagreement_sec = round(pulse_off - pulse_on, 3)
                # Exact clip duration: pre-roll already buffered in RAM +
                # the orphan pulse window + post-roll + the threshold we
                # waited to confirm the orphan.
                exact_duration = (
                    self._pre_roll_sec
                    + disagreement_sec
                    + self._post_roll_sec
                    + threshold
                )
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule2_orphan_pulse",
                    description=f"orphan on detector '{det_a_id}': {desc}",
                    disagreement_sec=disagreement_sec,
                    event_ts=now,
                    action="start",
                    duration_override=exact_duration,
                    event_window=(pulse_on, pulse_off),
                )
                rt.orphan_watch_a = None
                return
            if now >= pulse_off + threshold:
                rt.orphan_watch_a = None

        # Evaluate detector-B orphan candidate against detector-A history.
        if rt.orphan_watch_b is not None:
            pulse_on, pulse_off = rt.orphan_watch_b
            fire, desc = _check_rule2_orphan(
                pulse_on, pulse_off,
                a_intervals, a_is_on,
                now, threshold,
            )
            if fire:
                disagreement_sec = round(pulse_off - pulse_on, 3)
                exact_duration = (
                    self._pre_roll_sec
                    + disagreement_sec
                    + self._post_roll_sec
                    + threshold
                )
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule2_orphan_pulse",
                    description=f"orphan on detector '{det_b_id}': {desc}",
                    disagreement_sec=disagreement_sec,
                    event_ts=now,
                    action="start",
                    duration_override=exact_duration,
                    event_window=(pulse_on, pulse_off),
                )
                rt.orphan_watch_b = None
                return
            if now >= pulse_off + threshold:
                rt.orphan_watch_b = None

    # ------------------------------------------------------------------
    # Orphan candidate registration
    # ------------------------------------------------------------------

    @staticmethod
    def _maybe_register_orphan(
        rt: _PairRuntimeState,
        which: str,
        is_on: bool,
        last_pulse_on: float,
        last_off: float,
        threshold: float,
        min_pulse_sec: float = 0.0,
    ) -> None:
        """Register a new Rule-2 orphan candidate if one is not already tracked.

        A candidate is registered when ALL of the following hold:

        * The detector is currently OFF (the pulse has ended).
        * ``last_off`` is non-zero (an OFF edge has been observed).
        * ``last_pulse_on`` is non-zero (an ON edge has been observed).
        * The candidate differs from any already-registered candidate for
          this slot (prevents re-registering the same pulse each tick).
        * The pulse's ON-edge is newer than the last pulse already handled for
          this slot.  A pulse's ``last_pulse_on`` is monotonic (updated only on
          a fresh rising edge), so once a pulse has been armed it is never
          re-armed — even after its trigger fires and the post-trigger cooldown
          later expires while the detector's state is unchanged.  Without this
          guard a single stale ghost pulse re-fires once per cooldown period.
        * The pulse ON-duration is strictly less than ``threshold``.  Longer
          pulses are handled by Rule 1 and excluded here to avoid
          double-triggering.
        * The pulse ON-duration is at least ``min_pulse_sec`` — the sampling
          floor gate (ROADMAP 9).  A shorter pulse is below the resolution of
          the source that reported it, so its "the partner never saw it"
          counterpart is equally likely to be an unseen sample as a real
          absence.  Rejected pulses bump ``rt.below_floor_suppressed`` and are
          marked handled, so each distinct pulse is counted (and logged) once
          rather than on every 0.1 s tick it remains the detector's last pulse.

        Args:
            rt: The pair runtime state object to update.
            which: ``"a"`` or ``"b"`` — which orphan slot to populate.
            is_on: Current ON state of the detector.
            last_pulse_on: ``last_pulse_on_time`` from the detector snapshot.
            last_off: ``last_off_time`` from the detector snapshot.
            threshold: Allowable disagreement window in seconds.
            min_pulse_sec: Sampling-floor gate in seconds
                (``floor × min_pulse_floor_multiple``).  ``0.0`` disables the
                gate — used only by callers that have no floor to apply.
        """
        if is_on or last_pulse_on == 0.0 or last_off == 0.0:
            return

        pulse_duration = last_off - last_pulse_on
        if pulse_duration <= 0 or pulse_duration >= threshold:
            return

        attr: str = f"orphan_watch_{which}"
        existing: Optional[Tuple[float, float]] = getattr(rt, attr)
        if existing is not None and existing[0] == last_pulse_on:
            return  # Already watching this exact pulse.

        handled_attr = f"last_handled_pulse_on_{which}"
        if last_pulse_on <= getattr(rt, handled_attr):
            return  # This pulse was already armed once; don't re-arm it after
                    # a cooldown while the detector's state is unchanged.

        if min_pulse_sec > 0.0 and pulse_duration < min_pulse_sec:
            # Below the sampling floor — not evidence, aliasing.  Marked
            # handled so this pulse is accounted for exactly once.
            rt.below_floor_suppressed += 1
            setattr(rt, handled_attr, last_pulse_on)
            log.debug(
                "Orphan candidate below sampling floor — not registered",
                extra={
                    "pair_key": rt.pair_key,
                    "slot": which,
                    "pulse_duration_sec": round(pulse_duration, 3),
                    "min_pulse_sec": round(min_pulse_sec, 3),
                    "below_floor_suppressed": rt.below_floor_suppressed,
                },
            )
            return

        setattr(rt, attr, (last_pulse_on, last_off))
        setattr(rt, handled_attr, last_pulse_on)

    # ------------------------------------------------------------------
    # Early cooldown reset (called from NTCIP callback thread)
    # ------------------------------------------------------------------

    def _maybe_reset_cooldown_early(self, detector_id: str) -> None:
        """Lift the cooldown for any pair containing ``detector_id`` if both
        detectors are now OFF.

        Called from ``on_detector_off`` in the NTCIP event thread.  Must not
        perform any I/O.

        This method does **not** touch ``active_trigger_id`` or
        ``resolution_start_time``; those are owned exclusively by the
        evaluator thread's state machine.

        Args:
            detector_id: The detector that just transitioned to OFF.
        """
        for pair_key, (det_a_id, det_b_id) in self._pairs.items():
            if detector_id not in (det_a_id, det_b_id):
                continue

            partner_id = det_b_id if detector_id == det_a_id else det_a_id
            partner_state = self._detector_states.get(partner_id)
            if partner_state is None:
                continue

            with partner_state.lock:
                partner_is_on = partner_state.is_on

            if not partner_is_on:
                rt = self._pair_runtime.get(pair_key)
                if rt is not None and rt.cooldown_active:
                    rt.cooldown_active = False
                    self._log.debug(
                        "Cooldown reset early — both detectors OFF",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key": pair_key,
                        },
                    )

    # ------------------------------------------------------------------
    # Trigger file generation
    # ------------------------------------------------------------------

    def _fire_trigger(
        self,
        pair_key: str,
        det_a_id: str,
        det_b_id: str,
        rule: str,
        description: str,
        disagreement_sec: float,
        event_ts: float,
        action: str = "start",
        duration_override: Optional[float] = None,
        trigger_id_override: Optional[str] = None,
        event_window: Optional[Tuple[Optional[float], Optional[float]]] = None,
    ) -> None:
        """Write an atomic trigger JSON file and update pair runtime state.

        The file is written to a ``.tmp`` path first, then atomically renamed
        to ``.json`` so the video-buffer poller never reads a partial file.

        **State management after a successful write:**

        +------------------------------------------+----------------------------------+
        | Condition                                | Outcome                          |
        +==========================================+==================================+
        | ``action == "stop"``                     | Clear ``active_trigger_id`` and  |
        |                                          | ``resolution_start_time``;       |
        |                                          | engage cooldown.                 |
        +------------------------------------------+----------------------------------+
        | ``action == "start"`` and                | Set ``active_trigger_id``        |
        | ``rule == "rule1_continuous_…"``         | to new trigger ID; clear         |
        |                                          | ``resolution_start_time``.       |
        |                                          | Cooldown is NOT engaged.         |
        +------------------------------------------+----------------------------------+
        | ``action == "start"`` and any other rule | Engage cooldown immediately.     |
        | (e.g. ``"rule2_orphan_pulse"``)          | ``active_trigger_id`` unchanged. |
        +------------------------------------------+----------------------------------+

        Args:
            pair_key: Canonical pair key used for runtime state lookup.
            det_a_id: First detector ID.
            det_b_id: Second detector ID.
            rule: Rule identifier string (e.g. ``"rule1_continuous_disagreement"``).
            description: Human-readable description for trigger metadata.
            disagreement_sec: Measured disagreement duration in seconds.
            event_ts: Unix timestamp when the discrepancy was detected.
            action: ``"start"`` or ``"stop"``.  Defaults to ``"start"``.
            duration_override: When provided, overrides ``self._max_duration_sec``
                as the ``max_duration_sec`` field in the payload.  Pass the
                exact calculated clip duration for Rule 2 triggers.
            trigger_id_override: When provided (for ``action == "stop"``), use
                this hex string as ``trigger_id`` so the video-buffer layer can
                correlate the stop with its start.  A fresh UUID is generated
                when ``None`` (appropriate for ``"start"`` actions).
            event_window: ``(start_ts, end_ts)`` of the *underlying detector
                event* in Unix time, recorded verbatim in the decision log so a
                consumer never has to reconstruct it from a local timestamp and
                a description.  Either element may be ``None`` where the rule
                does not define it (Rule 1's ``start`` knows the beginning of
                the disagreement but not its end).  Not part of the trigger
                payload — the video buffer has no use for it, and the Hot
                Folder schema is deliberately hard to grow.
        """
        rt = self._pair_runtime[pair_key]

        # For "stop" actions we must reuse the same trigger_id as the
        # corresponding "start" so the video-buffer can match the pair.
        trigger_id = trigger_id_override if trigger_id_override else uuid.uuid4().hex

        # Resolve the intersection-local timezone for the filename timestamp and
        # the payload field.  _resolve_pytz uses pytz's bundled IANA database
        # so it works regardless of system zone files or tzdata.
        tz_name: str = self._intersection_cfg.get("timezone", "UTC")
        local_tz = _resolve_pytz(tz_name, self._log)
        # Normalise tz_name in case _resolve_pytz fell back to UTC.
        if local_tz is pytz.utc:
            tz_name = "UTC"

        iso_ts    = datetime.fromtimestamp(event_ts, tz=local_tz).strftime(
            "%Y%m%dT%H%M%S%f"
        )
        stem      = f"trigger_{iso_ts}_{trigger_id[:8]}"
        tmp_path  = self._trigger_dir / f"{stem}.tmp"
        json_path = self._trigger_dir / f"{stem}.json"

        cameras   = self._cameras_for_pair(det_a_id, det_b_id)
        det_a_cfg = self._intersection_cfg["detectors"].get(det_a_id, {})
        det_b_cfg = self._intersection_cfg["detectors"].get(det_b_id, {})

        payload = {
            "trigger_id":       trigger_id,
            "action":           action,
            "event_timestamp":  event_ts,
            "reason":           "detector_disagreement",
            "intersection_id":  self._intersection_id,
            "timezone":         tz_name,
            "cameras":          cameras,
            "pre_roll_sec":     self._pre_roll_sec,
            "post_roll_sec":    self._post_roll_sec,
            "max_duration_sec": (
                duration_override if duration_override is not None
                else self._max_duration_sec
            ),
            "metadata": {
                "det_a":            det_a_id,
                "det_b":            det_b_id,
                "det_a_type":       det_a_cfg.get("type", "unknown"),
                "det_b_type":       det_b_cfg.get("type", "unknown"),
                "det_a_phase":      det_a_cfg.get("phase"),
                "det_b_phase":      det_b_cfg.get("phase"),
                "rule":             rule,
                "description":      description,
                "disagreement_sec": disagreement_sec,
            },
        }

        try:
            tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.rename(tmp_path, json_path)
        except OSError as exc:
            self._log.error(
                "Failed to write trigger file",
                extra={
                    "intersection_id": self._intersection_id,
                    "pair_key":   pair_key,
                    "trigger_id": trigger_id,
                    "action":     action,
                    "error":      str(exc),
                },
            )
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            return

        self._log.info(
            "Trigger sent",
            extra={
                "trigger_id":       trigger_id,
                "action":           action,
                "intersection_id":  self._intersection_id,
                "pair_key":         pair_key,
                "rule":             rule,
                "disagreement_sec": disagreement_sec,
                "max_duration_sec": payload["max_duration_sec"],
                "cameras":          cameras,
                "path":             str(json_path),
            },
        )

        # Record the decision now that it has actually been handed off.  This
        # is deliberately after the rename and before any state management:
        # what the log claims the engine emitted is exactly what reached the
        # Hot Folder, whether or not the buffer later finds a writer slot.
        self._log_decision(payload, pair_key, event_window, local_tz)

        # ── Post-write state management ───────────────────────────────────

        if action == "stop":
            # Clear active Rule 1 tracking and engage cooldown.
            rt.active_trigger_id     = None
            rt.resolution_start_time = None
            rt.cooldown_active       = True
            rt.triggered_at          = time.time()
            rt.disagreement_start    = None

        elif rule == "rule1_continuous_disagreement":
            # START for Rule 1: arm the resolution state machine.
            # Cooldown is deliberately NOT engaged here — the evaluator will
            # engage it after the paired "stop" trigger is sent.
            rt.active_trigger_id     = trigger_id
            rt.resolution_start_time = None
            # disagreement_start was already reset by the caller.

        else:
            # START for Rule 2 (or any future instantaneous rule): the clip
            # duration is fully self-contained, so engage cooldown immediately.
            rt.cooldown_active    = True
            rt.triggered_at       = time.time()
            rt.disagreement_start = None

    def _log_decision(
        self,
        payload: dict,
        pair_key: str,
        event_window: Optional[Tuple[Optional[float], Optional[float]]],
        local_tz: "pytz.BaseTzInfo",
    ) -> None:
        """Append one row to the engine's decision log.

        Called from :meth:`_fire_trigger` for every trigger that reached the
        Hot Folder.  Unlike the video buffer's ``discrepancies_log.csv``, no
        downstream condition can suppress a row here — that difference is the
        whole point of the file (see the module docstring).

        Best-effort by contract: any write failure is logged and swallowed, so
        a full or read-only disk degrades measurement, never recording.  The
        engine is the single writer (all call sites are on the evaluator
        thread), so the append needs no lock.

        Args:
            payload: The trigger payload just written, used as the source of
                truth for the fields the two artifacts share.
            pair_key: Canonical pair key the decision was made for.
            event_window: ``(start_ts, end_ts)`` of the underlying detector
                event; either element may be ``None``, and ``None`` for the
                whole tuple means the rule/action defines neither.
            local_tz: Timezone already resolved by the caller, reused so the
                row's human-readable stamp cannot disagree with the trigger
                filename's.
        """
        if self._decision_log_path is None:
            return

        meta = payload.get("metadata", {})
        event_ts = payload["event_timestamp"]
        start_ts, end_ts = event_window if event_window else (None, None)

        try:
            local_stamp = datetime.fromtimestamp(
                event_ts, tz=local_tz
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, OSError, OverflowError):
            local_stamp = ""

        row = {
            "event_timestamp":  f"{event_ts:.3f}",
            "local_timestamp":  local_stamp,
            "intersection_id":  payload["intersection_id"],
            "trigger_id":       payload["trigger_id"],
            "action":           payload["action"],
            "rule":             meta.get("rule", ""),
            "pair_key":         pair_key,
            "det_a":            meta.get("det_a", ""),
            "det_b":            meta.get("det_b", ""),
            "det_a_type":       meta.get("det_a_type", ""),
            "det_b_type":       meta.get("det_b_type", ""),
            "event_start_ts":   "" if start_ts is None else f"{start_ts:.3f}",
            "event_end_ts":     "" if end_ts is None else f"{end_ts:.3f}",
            "disagreement_sec": meta.get("disagreement_sec", ""),
            "max_duration_sec": payload["max_duration_sec"],
            "cameras":          ";".join(payload.get("cameras", [])),
            "description":      meta.get("description", ""),
        }

        try:
            self._decision_log_path.parent.mkdir(parents=True, exist_ok=True)
            # Header only for a genuinely new (or truncated) file — the log
            # survives restarts and must not gain a header mid-stream.
            write_header = (
                not self._decision_log_path.exists()
                or self._decision_log_path.stat().st_size == 0
            )
            with self._decision_log_path.open(
                "a", newline="", encoding="utf-8"
            ) as fh:
                writer = csv.DictWriter(fh, fieldnames=list(_DECISION_LOG_FIELDS))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except OSError as exc:
            self._log.error(
                "Failed to append to the engine decision log",
                extra={
                    "intersection_id": self._intersection_id,
                    "trigger_id":      payload["trigger_id"],
                    "path":            str(self._decision_log_path),
                    "error":           str(exc),
                },
            )

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _build_structures(self, preserve_existing: bool = False) -> None:
        """Populate ``_detector_states``, ``_pairs``, and ``_pair_runtime``.

        Args:
            preserve_existing: If ``True``, retains existing
                :class:`_DetectorState` objects for IDs that survive the
                reload, and carries forward active cooldown state.  Transient
                fields (disagreement timers, orphan watches, and Rule 1
                active-tracking fields) are intentionally discarded to
                guarantee a clean slate after a config change.
        """
        detectors_cfg: dict = self._intersection_cfg.get("detectors", {})

        new_states: Dict[str, _DetectorState] = {}
        for det_id in detectors_cfg:
            key = str(det_id)
            if preserve_existing and key in self._detector_states:
                new_states[key] = self._detector_states[key]
            else:
                new_states[key] = _DetectorState(detector_id=key)
        self._detector_states = new_states

        new_pairs: Dict[str, Tuple[str, str]] = {}
        seen: set = set()

        for det_id, det_cfg in detectors_cfg.items():
            det_id_str  = str(det_id)
            partner_id  = det_cfg.get("paired_detector_id")
            if partner_id is None:
                continue
            partner_str = str(partner_id)

            if partner_str not in {str(k) for k in detectors_cfg}:
                self._log.warning(
                    "Detector references unknown paired_detector_id",
                    extra={
                        "intersection_id":    self._intersection_id,
                        "detector_id":        det_id_str,
                        "paired_detector_id": partner_str,
                    },
                )
                continue

            pair_key = ":".join(sorted([det_id_str, partner_str]))
            if pair_key in seen:
                continue
            seen.add(pair_key)
            new_pairs[pair_key] = (det_id_str, partner_str)

        self._pairs = new_pairs

        new_runtime: Dict[str, _PairRuntimeState] = {}
        for pair_key in new_pairs:
            if preserve_existing and pair_key in self._pair_runtime:
                old   = self._pair_runtime[pair_key]
                fresh = _PairRuntimeState(pair_key=pair_key)
                # Preserve only cooldown state; discard all transient fields
                # including any in-progress Rule 1 tracking.
                fresh.cooldown_active = old.cooldown_active
                fresh.triggered_at    = old.triggered_at
                new_runtime[pair_key] = fresh
            else:
                new_runtime[pair_key] = _PairRuntimeState(pair_key=pair_key)
        self._pair_runtime = new_runtime

        self._log.debug(
            "Structures built",
            extra={
                "intersection_id": self._intersection_id,
                "detectors":       list(self._detector_states.keys()),
                "pairs":           list(self._pairs.keys()),
            },
        )

    def _cameras_for_pair(self, det_a_id: str, det_b_id: str) -> List[str]:
        """Return the deduplicated ordered camera IDs for a detector pair.

        Args:
            det_a_id: First detector ID.
            det_b_id: Second detector ID.

        Returns:
            List of camera ID strings, or ``["all"]`` if none are configured.
        """
        detectors_cfg = self._intersection_cfg.get("detectors", {})
        cameras: List[str] = []
        for det_id in (det_a_id, det_b_id):
            cam = detectors_cfg.get(det_id, {}).get("camera_id")
            if cam and cam not in cameras:
                cameras.append(cam)
        return cameras if cameras else ["all"]