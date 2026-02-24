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

    Because the full orphan window has elapsed by the time the trigger fires,
    the **exact clip duration is known**: ``pre_roll_sec + disagreement_sec +
    post_roll_sec + threshold``.  This is passed as ``max_duration_sec`` in the
    start trigger and cooldown is engaged immediately — no stop trigger is sent.

    Catches:
      • A brief false-positive "ghost" pulse on one system that the other
        never saw at all.
      • Works symmetrically: A brief pulse on B that A never sees also fires.

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

Thread-safety contract
──────────────────────
``on_detector_on`` / ``on_detector_off`` are called from the NTCIP monitor's
event-dispatch thread and must return in microseconds.  They only acquire a
per-detector ``threading.Lock`` long enough to mutate four scalar fields.

The background evaluator thread acquires the same locks briefly to snapshot
each detector's state, then releases them before any comparison or I/O.

All ``_PairRuntimeState`` fields are written exclusively by the evaluator thread
except ``cooldown_active``, which the NTCIP callback thread may clear via the
early-reset path.  A boolean assignment is atomic under CPython's GIL and the
evaluator tolerates a one-tick stale read in the rare race window.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
import pytz
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config_manager import ConfigProvider, ConfigProviderError

# ---------------------------------------------------------------------------
# JSON-lines logger
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D102
        payload = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        _SKIP = {
            "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno",
            "funcName", "created", "msecs", "relativeCreated", "thread",
            "threadName", "processName", "process", "name", "message",
        }
        for k, v in record.__dict__.items():
            if k not in _SKIP:
                payload[k] = v
        return json.dumps(payload, default=str)


def _build_logger(name: str) -> logging.Logger:
    """Return a logger that emits JSON lines to stdout.

    Args:
        name: Logger hierarchy name.

    Returns:
        Configured :class:`logging.Logger`.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


log = _build_logger("discrepancy_engine")

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
    """

    detector_id: str
    is_on: bool = False
    last_on_time: float = 0.0
    last_off_time: float = 0.0
    last_pulse_on_time: float = 0.0
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
    other_last_on: float,
    other_last_off: float,
    other_is_on: bool,
    now: float,
    threshold: float,
) -> Tuple[bool, str]:
    """Rule 2: detect an orphan pulse — a brief actuation the partner never saw.

    The observation window is ``[pulse_on − threshold … pulse_off + threshold]``.
    The check is deferred until ``now >= pulse_off + threshold`` so the full
    post-pulse grace period has elapsed before a verdict is rendered.

    Rule 3 protection is embedded here: if the partner's ``last_on_time``
    falls anywhere inside the observation window, the check is suppressed.

    Args:
        pulse_on: ``time.time()`` of the orphan candidate's ON-edge.
        pulse_off: ``time.time()`` of the orphan candidate's OFF-edge.
        other_last_on: ``last_on_time`` of the partner detector.
        other_last_off: ``last_off_time`` of the partner detector.
        other_is_on: Current ON state of the partner detector.
        now: Current ``time.time()``.
        threshold: Allowable disagreement window in seconds.

    Returns:
        A ``(should_fire, description)`` tuple.  ``should_fire`` is ``True``
        when the orphan condition is confirmed.  ``description`` is a
        human-readable string for trigger metadata.
    """
    if now < pulse_off + threshold:
        return False, ""

    window_start = pulse_on - threshold
    window_end   = pulse_off + threshold

    if other_is_on:
        return False, ""

    if other_last_on > 0.0 and window_start <= other_last_on <= window_end:
        return False, ""

    if other_last_on == 0.0 or other_last_on < window_start:
        pulse_duration = round(pulse_off - pulse_on, 3)
        desc = (
            f"orphan pulse duration={pulse_duration}s "
            f"window=[{window_start:.3f}, {window_end:.3f}]"
        )
        return True, desc

    return False, ""


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
    ) -> None:
        self._intersection_id = intersection_id
        self._trigger_dir = Path(trigger_dir)
        self._cooldown_sec = cooldown_sec
        self._evaluator_interval_sec = evaluator_interval_sec
        self._pre_roll_sec = pre_roll_sec
        self._post_roll_sec = post_roll_sec
        self._max_duration_sec = max_duration_sec
        self._log = _build_logger(f"discrepancy_engine.{intersection_id}")

        self._trigger_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._intersection_cfg = config_provider.get_intersection_config(
                intersection_id
            )
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Cannot load config for intersection '{intersection_id}': {exc}"
            ) from exc

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
        self._build_structures(preserve_existing=True)
        self._log.info(
            "Configuration reloaded",
            extra={"intersection_id": self._intersection_id},
        )

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
            state.last_off_time = time.time()
            # last_pulse_on_time intentionally NOT updated here — it retains
            # the ON-edge value so the evaluator can form [pulse_on, pulse_off].

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
        4. **Rule 1 new-detection** — start/continue disagreement timer; fire
           ``"start"`` trigger (no cooldown) if threshold exceeded.
        5. **Rule 2 orphan detection** — register candidates; fire ``"start"``
           trigger with exact ``duration_override`` when confirmed.

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

        with state_a.lock:
            a_is_on         = state_a.is_on
            a_last_on       = state_a.last_on_time
            a_last_off      = state_a.last_off_time
            a_last_pulse_on = state_a.last_pulse_on_time

        with state_b.lock:
            b_is_on         = state_b.is_on
            b_last_on       = state_b.last_on_time
            b_last_off      = state_b.last_off_time
            b_last_pulse_on = state_b.last_pulse_on_time

        det_cfg   = self._intersection_cfg["detectors"].get(det_a_id, {})
        threshold = float(det_cfg.get("lag_threshold_sec", 2.0))

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
                )
                # Reset timer; active_trigger_id now set by _fire_trigger.
                rt.disagreement_start = None
                return  # skip Rule 2 on this tick

        # ── 5. Rule 2 — Orphan Pulse ──────────────────────────────────────
        self._maybe_register_orphan(
            rt, "a", a_is_on, a_last_pulse_on, a_last_off, threshold
        )
        self._maybe_register_orphan(
            rt, "b", b_is_on, b_last_pulse_on, b_last_off, threshold
        )

        # Evaluate detector-A orphan candidate against detector-B history.
        if rt.orphan_watch_a is not None:
            pulse_on, pulse_off = rt.orphan_watch_a
            fire, desc = _check_rule2_orphan(
                pulse_on, pulse_off,
                b_last_on, b_last_off, b_is_on,
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
                a_last_on, a_last_off, a_is_on,
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
    ) -> None:
        """Register a new Rule-2 orphan candidate if one is not already tracked.

        A candidate is registered when ALL of the following hold:

        * The detector is currently OFF (the pulse has ended).
        * ``last_off`` is non-zero (an OFF edge has been observed).
        * ``last_pulse_on`` is non-zero (an ON edge has been observed).
        * The candidate differs from any already-registered candidate for
          this slot (prevents re-registering the same pulse each tick).
        * The pulse ON-duration is strictly less than ``threshold``.  Longer
          pulses are handled by Rule 1 and excluded here to avoid
          double-triggering.

        Args:
            rt: The pair runtime state object to update.
            which: ``"a"`` or ``"b"`` — which orphan slot to populate.
            is_on: Current ON state of the detector.
            last_pulse_on: ``last_pulse_on_time`` from the detector snapshot.
            last_off: ``last_off_time`` from the detector snapshot.
            threshold: Allowable disagreement window in seconds.
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

        setattr(rt, attr, (last_pulse_on, last_off))

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
