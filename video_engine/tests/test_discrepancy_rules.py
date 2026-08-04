"""Unit tests for the discrepancy engine's pure rule functions.

Covers ``_check_rule1_continuous``, ``_check_rule2_orphan`` (interval-based
partner overlap), ``_compute_on_duty_fraction``, and
``DiscrepancyMonitor._maybe_register_orphan`` (including the 2026-07-19
stale-refire guard, the 2026-07-30 sampling-floor gate and the 2026-08-03
partner sub-floor-activity gate), plus a set of
integration tests that drive ``DiscrepancyMonitor._evaluate_pair`` directly
(no evaluator thread) against a stub ``ConfigProvider`` and a temp Hot Folder,
the 2026-08-01 decision log (``engine_decisions.csv``), the 2026-08-01
suppression log (``engine_suppressions.csv``), ``_resolve_pytz``
(ROADMAP 4d), and the derived detector groups plus cross-pair duplicate
rejection (ROADMAP 9C4).

Run from anywhere:

    python3 -m unittest video_engine.tests.test_discrepancy_rules   # if pkg'd
    python3 video_engine/tests/test_discrepancy_rules.py            # direct
"""

from __future__ import annotations

import csv
import json
import logging
import sys
import tempfile
import time
import unittest
from datetime import datetime
from pathlib import Path

import pytz

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_manager import ConfigProvider  # noqa: E402
from discrepancy_engine import (  # noqa: E402
    DiscrepancyMonitor,
    _DECISION_LOG_FIELDS,
    _DEFAULT_PARTNER_BLIP_MAX,
    _DEFAULT_PARTNER_BLIP_WINDOW_SEC,
    _DEFAULT_SAMPLING_FLOOR_SEC,
    _DUTY_WINDOW_SEC,
    _ORPHAN_DECISION_GRACE_SEC,
    _PairRuntimeState,
    _SUPPRESS_BELOW_FLOOR,
    _SUPPRESS_PARTNER_BLIP,
    _SUPPRESSION_LOG_FIELDS,
    _check_rule1_continuous,
    _check_rule2_orphan,
    _compute_on_duty_fraction,
    _resolve_pytz,
)

# The engine logs a WARNING for high-duty pairs.  Without a handler, logging's
# lastResort prints it to stderr and clutters the test output; assertLogs
# installs its own handler, so capturing tests are unaffected.
logging.getLogger("discrepancy_engine").addHandler(logging.NullHandler())


# ---------------------------------------------------------------------------
# Rule 1 — continuous disagreement
# ---------------------------------------------------------------------------

class TestRule1Continuous(unittest.TestCase):
    THRESHOLD = 5.0

    def test_no_disagreement_never_fires(self):
        fire, duration = _check_rule1_continuous(None, 100.0, self.THRESHOLD)
        self.assertFalse(fire)
        self.assertEqual(duration, 0.0)

    def test_at_threshold_boundary_does_not_fire(self):
        # The comparison is strictly greater-than: duration == threshold holds.
        fire, duration = _check_rule1_continuous(100.0, 105.0, self.THRESHOLD)
        self.assertFalse(fire)
        self.assertEqual(duration, 5.0)

    def test_just_over_threshold_fires(self):
        fire, duration = _check_rule1_continuous(100.0, 105.001, self.THRESHOLD)
        self.assertTrue(fire)
        self.assertEqual(duration, 5.001)


# ---------------------------------------------------------------------------
# Rule 2 — orphan pulse via partner interval overlap
# ---------------------------------------------------------------------------

class TestRule2Orphan(unittest.TestCase):
    """Pulse [100, 102] with threshold 5 → observation window [95, 107]."""

    THRESHOLD = 5.0
    PULSE_ON = 100.0
    PULSE_OFF = 102.0
    WINDOW_END = 107.0  # PULSE_OFF + THRESHOLD

    def check(self, intervals, other_is_on=False, now=None):
        return _check_rule2_orphan(
            self.PULSE_ON, self.PULSE_OFF,
            intervals, other_is_on,
            self.WINDOW_END if now is None else now,
            self.THRESHOLD,
        )

    def test_partner_silent_fires(self):
        fire, desc = self.check(())
        self.assertTrue(fire)
        self.assertIn("duration=2.0s", desc)

    def test_deferred_until_window_elapsed(self):
        fire, _ = self.check((), now=self.WINDOW_END - 0.1)
        self.assertFalse(fire)

    def test_partner_currently_on_suppresses(self):
        fire, _ = self.check((), other_is_on=True)
        self.assertFalse(fire)

    def test_partner_pulse_after_window_fires(self):
        # THE headline fix: the partner actuating after window_end must not
        # suppress the orphan.  The old scalar comparison (other_last_on >
        # window_end matched no branch) silently returned False here.
        fire, _ = self.check(((107.5, 108.4),), now=108.5)
        self.assertTrue(fire)

    def test_partner_pulse_inside_window_suppresses(self):
        fire, _ = self.check(((103.0, 104.0),))
        self.assertFalse(fire)

    def test_partner_straddling_window_start_suppresses(self):
        # Partner ON began before the window and ended inside it.  The old
        # scalar test (last_on < window_start → fire) called this an orphan —
        # a false positive the interval intersection now catches.
        fire, _ = self.check(((90.0, 96.0),))
        self.assertFalse(fire)

    def test_mid_window_overlap_survives_newer_partner_pulse(self):
        # A partner ON inside the window followed by a newer one after it.
        # With a single scalar the newer edge overwrote the mid-window one,
        # leaking a Rule 3 overlap; the interval history keeps both.
        fire, _ = self.check(((103.0, 103.5), (107.5, 108.0)), now=108.2)
        self.assertFalse(fire)

    def test_partner_pulse_entirely_before_window_fires(self):
        fire, _ = self.check(((80.0, 90.0),))
        self.assertTrue(fire)

    def test_chatter_straddling_window_suppresses(self):
        # Rule 3: partner chattering across the whole window must suppress.
        chatter = ((94.0, 96.0), (96.5, 98.5), (99.0, 101.0),
                   (101.5, 103.5), (104.0, 106.0), (106.5, 108.5))
        fire, _ = self.check(chatter, now=109.0)
        self.assertFalse(fire)

    def test_stale_verdict_is_discarded(self):
        # Verdict rendered long after the window closed (pair was in cooldown
        # or inside an active Rule 1 recording): must NOT fire — the pre-roll
        # footage for that moment is gone.
        late = self.WINDOW_END + _ORPHAN_DECISION_GRACE_SEC + 0.1
        fire, _ = self.check((), now=late)
        self.assertFalse(fire)

    def test_verdict_at_grace_boundary_still_fires(self):
        at_grace = self.WINDOW_END + _ORPHAN_DECISION_GRACE_SEC
        fire, _ = self.check((), now=at_grace)
        self.assertTrue(fire)


# ---------------------------------------------------------------------------
# Orphan candidate registration (incl. the stale-refire guard)
# ---------------------------------------------------------------------------

class TestMaybeRegisterOrphan(unittest.TestCase):
    THRESHOLD = 5.0

    def setUp(self):
        self.rt = _PairRuntimeState(pair_key="1:2")

    def register(self, is_on=False, last_pulse_on=100.0, last_off=102.0,
                 min_pulse_sec=0.0):
        return DiscrepancyMonitor._maybe_register_orphan(
            self.rt, "a", is_on, last_pulse_on, last_off, self.THRESHOLD,
            min_pulse_sec,
        )

    def test_registers_short_pulse(self):
        self.register()
        self.assertEqual(self.rt.orphan_watch_a, (100.0, 102.0))
        self.assertEqual(self.rt.last_handled_pulse_on_a, 100.0)

    def test_skips_while_detector_on(self):
        self.register(is_on=True)
        self.assertIsNone(self.rt.orphan_watch_a)

    def test_skips_before_any_edges_observed(self):
        self.register(last_pulse_on=0.0, last_off=0.0)
        self.assertIsNone(self.rt.orphan_watch_a)

    def test_skips_pulse_at_or_over_threshold(self):
        # Pulses >= threshold belong to Rule 1, not Rule 2.
        self.register(last_pulse_on=100.0, last_off=105.0)
        self.assertIsNone(self.rt.orphan_watch_a)

    def test_does_not_rearm_already_watched_pulse(self):
        self.register()
        watched = self.rt.orphan_watch_a
        self.register()
        self.assertIs(self.rt.orphan_watch_a, watched)

    def test_stale_refire_guard_pins_2026_07_19_fix(self):
        # Register + "fire" (watch cleared), then cooldown expires while the
        # detector state is unchanged: the same pulse must never re-arm.
        # Without the last_handled_pulse_on guard this re-fired once per
        # cooldown period (15/76 phantom rows in the 2026-07-19 sample).
        self.register()
        self.rt.orphan_watch_a = None  # trigger fired
        self.register()
        self.assertIsNone(self.rt.orphan_watch_a)

    def test_newer_pulse_rearms(self):
        self.register()
        self.rt.orphan_watch_a = None
        self.register(last_pulse_on=110.0, last_off=111.0)
        self.assertEqual(self.rt.orphan_watch_a, (110.0, 111.0))
        self.assertEqual(self.rt.last_handled_pulse_on_a, 110.0)

    # ── Sampling-floor gate (ROADMAP 9 item B, rule 1 of the gating list) ──

    def test_below_floor_pulse_not_registered(self):
        # 2 s pulse, floor gate 3.2 s (= 1.6 s measured floor x 2): the source
        # cannot resolve this pulse, so the partner's silence is not evidence.
        self.register(min_pulse_sec=3.2)
        self.assertIsNone(self.rt.orphan_watch_a)
        self.assertEqual(self.rt.below_floor_suppressed, 1)

    def test_below_floor_pulse_counted_once_not_per_tick(self):
        # The evaluator re-examines the same last-pulse every 0.1 s tick;
        # the suppression counter must track pulses, not ticks.
        for _ in range(5):
            self.register(min_pulse_sec=3.2)
        self.assertEqual(self.rt.below_floor_suppressed, 1)

    def test_pulse_exactly_at_floor_gate_registers(self):
        # Gate is "shorter than", so a pulse of exactly the gate length is
        # trusted (2.0 s pulse, gate 2.0 s).
        self.register(min_pulse_sec=2.0)
        self.assertEqual(self.rt.orphan_watch_a, (100.0, 102.0))
        self.assertEqual(self.rt.below_floor_suppressed, 0)

    def test_above_floor_pulse_registers(self):
        # Post-chunk-8 regime: floor ~0.2 s, gate ~0.4 s — suppresses nothing
        # real at a 2 s pulse.
        self.register(min_pulse_sec=0.4)
        self.assertEqual(self.rt.orphan_watch_a, (100.0, 102.0))
        self.assertEqual(self.rt.below_floor_suppressed, 0)

    def test_zero_gate_disables_the_check(self):
        self.register(last_pulse_on=100.0, last_off=100.001, min_pulse_sec=0.0)
        self.assertEqual(self.rt.orphan_watch_a, (100.0, 100.001))
        self.assertEqual(self.rt.below_floor_suppressed, 0)

    # ── Suppression reporting seam (ROADMAP 9C3) ──────────────────────────
    # The helper stays static and pure; it reports the pulse it suppressed
    # via its return value and the caller owns the log write.

    def test_suppressed_pulse_is_returned(self):
        suppressed = self.register(min_pulse_sec=3.2)
        self.assertEqual(suppressed.reason, _SUPPRESS_BELOW_FLOOR)
        self.assertEqual(suppressed.pulse, (100.0, 102.0))

    def test_registered_pulse_returns_none(self):
        self.assertIsNone(self.register(min_pulse_sec=0.4))

    def test_early_returns_report_nothing(self):
        # Every non-suppression path must be indistinguishable from "no
        # candidate" to the caller, or the log gains phantom rows.
        self.assertIsNone(self.register(is_on=True))
        self.assertIsNone(self.register(last_pulse_on=0.0, last_off=0.0))
        self.assertIsNone(self.register(last_pulse_on=100.0, last_off=105.0))

    def test_suppression_reported_once_not_per_tick(self):
        # Same guard as the counter: the evaluator re-examines this pulse
        # every 0.1 s tick, and only the first call may produce a row.
        results = [self.register(min_pulse_sec=3.2) for _ in range(5)]
        self.assertEqual(
            [None if r is None else r.pulse for r in results],
            [(100.0, 102.0), None, None, None, None],
        )

    def test_already_watched_pulse_reports_nothing(self):
        self.register(min_pulse_sec=0.4)                    # arms the slot
        self.assertIsNone(self.register(min_pulse_sec=3.2))  # same pulse


# ---------------------------------------------------------------------------
# ON-duty fraction (feeds the high-duty advisory)
# ---------------------------------------------------------------------------

class TestOnDutyFraction(unittest.TestCase):
    NOW = 1000.0
    WINDOW = 100.0

    def duty(self, intervals, is_on=False, last_on_time=0.0, window=None):
        return _compute_on_duty_fraction(
            intervals, is_on, last_on_time, self.NOW,
            self.WINDOW if window is None else window,
        )

    def test_no_history_is_zero(self):
        self.assertEqual(self.duty(()), 0.0)

    def test_completed_intervals_sum(self):
        # 20 s + 30 s inside a 100 s window.
        intervals = ((910.0, 930.0), (950.0, 980.0))
        self.assertAlmostEqual(self.duty(intervals), 0.5)

    def test_interval_clipped_at_window_start(self):
        # Started 50 s before the window; only the 10 s inside it counts.
        self.assertAlmostEqual(self.duty(((850.0, 910.0),)), 0.1)

    def test_interval_entirely_before_window_ignored(self):
        self.assertEqual(self.duty(((800.0, 850.0),)), 0.0)

    def test_open_interval_counts_up_to_now(self):
        # Currently ON since 40 s ago, no completed intervals.
        self.assertAlmostEqual(self.duty((), is_on=True, last_on_time=960.0), 0.4)

    def test_open_interval_adds_to_completed(self):
        intervals = ((910.0, 930.0),)  # 20 s
        self.assertAlmostEqual(
            self.duty(intervals, is_on=True, last_on_time=960.0), 0.6
        )

    def test_high_duty_channel_reads_above_warn_fraction(self):
        # The phase-2/6/7 shape: ON almost continuously with brief gaps.
        intervals = tuple(
            (self.NOW - self.WINDOW + i * 10.0, self.NOW - self.WINDOW + i * 10.0 + 9.0)
            for i in range(10)
        )
        self.assertAlmostEqual(self.duty(intervals), 0.9)

    def test_saturates_at_one(self):
        self.assertEqual(self.duty(((500.0, 1000.0),)), 1.0)

    def test_non_positive_window_is_zero(self):
        self.assertEqual(self.duty(((910.0, 930.0),), window=0.0), 0.0)


# ---------------------------------------------------------------------------
# Integration: DiscrepancyMonitor._evaluate_pair end-to-end (no thread)
# ---------------------------------------------------------------------------

class _StubProvider(ConfigProvider):
    """Minimal in-memory ConfigProvider for tests."""

    def __init__(self, cfg: dict):
        self._cfg = cfg

    def get_intersection_config(self, intersection_id: str) -> dict:
        return self._cfg

    def list_intersection_ids(self) -> list:
        return ["test_int"]


def _build_monitor(
    trigger_dir, threshold=0.2, floor=None, decision_log=None,
    suppression_log=None, **extra_cfg
):
    """Build a two-detector DiscrepancyMonitor over a temp Hot Folder.

    Args:
        trigger_dir: Directory the monitor writes trigger files into.
        threshold: ``lag_threshold_sec`` for both detectors.
        floor: When given, injected via ``set_sampling_floor`` the way
            ``system_runner`` does in production.
        decision_log: When given, the engine's decision-log CSV path.
        suppression_log: When given, the engine's suppression-log CSV path.
        **extra_cfg: Extra top-level intersection config keys (e.g.
            ``suppress_high_duty_pairs``).

    Returns:
        A constructed (not started) :class:`DiscrepancyMonitor`.
    """
    cfg = {
        "timezone": "UTC",
        "detectors": {
            "1": {"paired_detector_id": "2", "camera_id": "cam1",
                  "lag_threshold_sec": threshold, "type": "radar"},
            "2": {"paired_detector_id": "1", "camera_id": "cam1",
                  "lag_threshold_sec": threshold, "type": "loop"},
        },
    }
    cfg.update(extra_cfg)
    monitor = DiscrepancyMonitor(
        intersection_id="test_int",
        config_provider=_StubProvider(cfg),
        trigger_dir=trigger_dir,
        cooldown_sec=60.0,
        decision_log_path=decision_log,
        suppression_log_path=suppression_log,
    )
    if floor is not None:
        monitor.set_sampling_floor(floor)
    return monitor


class TestMonitorIntegration(unittest.TestCase):
    """Drives real callbacks + _evaluate_pair with a 0.2 s threshold."""

    THRESHOLD = 0.2

    # Sub-second pulses are only meaningful evidence if the source samples
    # faster than they last, so the tests declare a matching floor.  Without
    # this the production default (1.6 s x 2 = a 3.2 s gate) correctly refuses
    # every pulse these tests fire.
    FLOOR = 0.01

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR
        )

    def tearDown(self):
        self._tmp.cleanup()

    def evaluate(self):
        self.monitor._evaluate_pair("1:2", "1", "2")

    def triggers(self):
        out = []
        for path in sorted(Path(self._tmp.name).glob("trigger_*.json")):
            out.append(json.loads(path.read_text(encoding="utf-8")))
        return out

    def pulse(self, det_id: str, duration: float):
        self.monitor.on_detector_on(det_id)
        time.sleep(duration)
        self.monitor.on_detector_off(det_id)

    def test_orphan_pulse_fires_rule2_trigger(self):
        self.pulse("1", 0.05)
        time.sleep(self.THRESHOLD + 0.05)  # let the observation window elapse
        self.evaluate()
        trigs = self.triggers()
        self.assertEqual(len(trigs), 1)
        self.assertEqual(trigs[0]["metadata"]["rule"], "rule2_orphan_pulse")
        self.assertEqual(trigs[0]["action"], "start")
        self.assertEqual(trigs[0]["cameras"], ["cam1"])

    def test_partner_pulse_after_window_still_fires(self):
        # Regression for the scalar false negative: partner actuates after
        # the window closed; the orphan must still fire.
        self.pulse("1", 0.05)
        time.sleep(self.THRESHOLD + 0.05)  # window for det 1 has now closed
        self.pulse("2", 0.03)              # partner actuation after window
        self.evaluate()
        trigs = self.triggers()
        self.assertEqual(len(trigs), 1)
        self.assertEqual(trigs[0]["metadata"]["rule"], "rule2_orphan_pulse")
        self.assertIn("detector '1'", trigs[0]["metadata"]["description"])

    def test_partner_pulse_inside_window_suppresses(self):
        self.pulse("1", 0.05)
        time.sleep(0.03)
        self.pulse("2", 0.05)              # overlapping-window actuation
        time.sleep(self.THRESHOLD + 0.05)
        self.evaluate()
        self.assertEqual(self.triggers(), [])

    def test_rule1_continuous_disagreement_fires_start(self):
        self.monitor.on_detector_on("1")
        self.evaluate()                     # starts the disagreement timer
        time.sleep(self.THRESHOLD + 0.05)
        self.evaluate()                     # exceeds threshold → start trigger
        trigs = self.triggers()
        self.assertEqual(len(trigs), 1)
        self.assertEqual(
            trigs[0]["metadata"]["rule"], "rule1_continuous_disagreement"
        )
        self.assertEqual(trigs[0]["action"], "start")
        # Rule 1 arms the resolution state machine instead of cooldown.
        rt = self.monitor._pair_runtime["1:2"]
        self.assertEqual(rt.active_trigger_id, trigs[0]["trigger_id"])
        self.assertFalse(rt.cooldown_active)

    def test_callbacks_record_partner_intervals(self):
        self.pulse("2", 0.02)
        self.pulse("2", 0.02)
        intervals = self.monitor._detector_states["2"].on_intervals
        self.assertEqual(len(intervals), 2)
        for on_ts, off_ts in intervals:
            self.assertLess(on_ts, off_ts)


# ---------------------------------------------------------------------------
# Sampling-floor injection end-to-end (ROADMAP 9 item B)
# ---------------------------------------------------------------------------

class TestSamplingFloorInjection(unittest.TestCase):
    """The floor arrives via ``set_sampling_floor``, never via an import."""

    THRESHOLD = 0.2

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.monitor = _build_monitor(self._tmp.name, threshold=self.THRESHOLD)

    def tearDown(self):
        self._tmp.cleanup()

    def triggers(self):
        return sorted(Path(self._tmp.name).glob("trigger_*.json"))

    def pulse(self, det_id: str, duration: float):
        self.monitor.on_detector_on(det_id)
        time.sleep(duration)
        self.monitor.on_detector_off(det_id)

    def test_defaults_to_measured_ntcip_reality(self):
        # Nothing injected yet: assume the 2026-07-19 measured sweep, not an
        # optimistic poll_interval.
        self.assertEqual(
            self.monitor.get_sampling_floor(), _DEFAULT_SAMPLING_FLOOR_SEC
        )

    def test_set_and_get_round_trip(self):
        self.monitor.set_sampling_floor(0.25)
        self.assertAlmostEqual(self.monitor.get_sampling_floor(), 0.25)

    def test_unmeasured_values_are_ignored(self):
        # effective_cycle_sec() returns 0.0 before the first sweep completes;
        # accepting it would silently disable the gate.
        self.monitor.set_sampling_floor(0.25)
        for bogus in (0.0, -1.0, None, "slow"):
            self.monitor.set_sampling_floor(bogus)
            self.assertAlmostEqual(self.monitor.get_sampling_floor(), 0.25)

    def test_floor_update_takes_effect_on_rule2(self):
        # A floor too coarse to resolve the pulse: no trigger, counter bumped.
        self.monitor.set_sampling_floor(1.6)   # gate = 3.2 s
        self.pulse("1", 0.05)
        time.sleep(self.THRESHOLD + 0.05)
        self.monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(self.triggers(), [])
        rt = self.monitor._pair_runtime["1:2"]
        self.assertEqual(rt.below_floor_suppressed, 1)

        # Same engine, faster sweep measured (the chunk-8 outcome): an equally
        # short pulse is now resolvable evidence and fires.
        self.monitor.set_sampling_floor(0.01)  # gate = 0.02 s
        self.pulse("1", 0.05)
        time.sleep(self.THRESHOLD + 0.05)
        self.monitor._evaluate_pair("1:2", "1", "2")
        trigs = self.triggers()
        self.assertEqual(len(trigs), 1)
        payload = json.loads(trigs[0].read_text(encoding="utf-8"))
        self.assertEqual(payload["metadata"]["rule"], "rule2_orphan_pulse")


# ---------------------------------------------------------------------------
# High-duty advisory + opt-in suppression
# ---------------------------------------------------------------------------

class TestHighDutyAdvisory(unittest.TestCase):
    """Pairs whose duty cycle outruns NTCIP sampling are flagged, not silenced.

    Duty history is injected directly into the detectors' ``on_intervals``
    deques — the same structure the callbacks populate — so a 120 s window can
    be exercised without waiting 120 s.
    """

    THRESHOLD = 0.2
    LOGGER = "discrepancy_engine.test_int"

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, **extra_cfg):
        return _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=0.01, **extra_cfg
        )

    def load_duty(self, monitor, fraction):
        """Give both detectors ``fraction`` ON-duty over the rolling window."""
        now = time.time()
        span = _DUTY_WINDOW_SEC * fraction
        for det_id in ("1", "2"):
            state = monitor._detector_states[det_id]
            state.on_intervals.clear()
            state.on_intervals.append((now - span, now - 0.001))

    def test_warns_when_pair_duty_exceeds_fraction(self):
        monitor = self.build()
        self.load_duty(monitor, 0.9)
        with self.assertLogs(self.LOGGER, level="WARNING") as captured:
            monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(len(captured.records), 1)
        record = captured.records[0]
        self.assertIn("sampling-reliability regime", record.getMessage())
        self.assertGreater(record.duty_a, 0.8)
        self.assertFalse(record.suppressed)

    def test_warning_is_rate_limited_per_pair(self):
        monitor = self.build()
        self.load_duty(monitor, 0.9)
        with self.assertLogs(self.LOGGER, level="WARNING") as captured:
            monitor._evaluate_pair("1:2", "1", "2")
            # Force a duty recompute; the warning itself stays rate-limited.
            monitor._pair_runtime["1:2"].last_duty_eval_ts = 0.0
            monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(len(captured.records), 1)

    def test_no_warning_below_fraction(self):
        monitor = self.build()
        self.load_duty(monitor, 0.5)
        monitor._evaluate_pair("1:2", "1", "2")
        rt = monitor._pair_runtime["1:2"]
        self.assertFalse(rt.high_duty_active)
        self.assertAlmostEqual(rt.pair_min_duty, 0.5, places=2)

    def test_advisory_does_not_suppress_by_default(self):
        monitor = self.build()
        self.load_duty(monitor, 0.9)
        monitor.on_detector_on("1")            # divergence: 1 ON, 2 OFF
        monitor._evaluate_pair("1:2", "1", "2")  # starts the disagreement timer
        time.sleep(self.THRESHOLD + 0.05)
        monitor._evaluate_pair("1:2", "1", "2")
        trigs = sorted(Path(self._tmp.name).glob("trigger_*.json"))
        self.assertEqual(len(trigs), 1)
        self.assertTrue(monitor._pair_runtime["1:2"].high_duty_active)

    def test_opt_in_suppression_disables_rules(self):
        monitor = self.build(suppress_high_duty_pairs=True)
        self.load_duty(monitor, 0.9)
        monitor.on_detector_on("1")
        monitor._evaluate_pair("1:2", "1", "2")
        time.sleep(self.THRESHOLD + 0.05)
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(sorted(Path(self._tmp.name).glob("trigger_*.json")), [])
        self.assertTrue(monitor._pair_runtime["1:2"].high_duty_active)
        # No stale Rule 1 timer left behind: the first tick after duty falls
        # back below the threshold must not fire on a pre-suppression start.
        self.assertIsNone(monitor._pair_runtime["1:2"].disagreement_start)

    def test_rules_resume_when_duty_falls_back(self):
        monitor = self.build(suppress_high_duty_pairs=True)
        self.load_duty(monitor, 0.9)
        monitor.on_detector_on("1")
        monitor._evaluate_pair("1:2", "1", "2")   # suppressed
        time.sleep(self.THRESHOLD + 0.05)

        # Duty drops (quiet period); force a recompute the way 5 s of ticks
        # would, then confirm the pair is live again but timing afresh.
        self.load_duty(monitor, 0.1)
        monitor._pair_runtime["1:2"].last_duty_eval_ts = 0.0
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertFalse(monitor._pair_runtime["1:2"].high_duty_active)
        self.assertEqual(sorted(Path(self._tmp.name).glob("trigger_*.json")), [])

        time.sleep(self.THRESHOLD + 0.05)
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(
            len(sorted(Path(self._tmp.name).glob("trigger_*.json"))), 1
        )


# ---------------------------------------------------------------------------
# Timezone resolution (ROADMAP 4d)
# ---------------------------------------------------------------------------

class TestResolvePytz(unittest.TestCase):
    """``_resolve_pytz`` — IANA name → pytz zone, UTC + a warning on failure.

    The contract is string input (config's ``timezone`` key); the point of the
    function is that it reads pytz's *bundled* database, so it works on hosts
    with no system zone files and never raises on an unknown name.
    """

    LOGGER = "discrepancy_engine.tz_test"

    def setUp(self):
        self.log = logging.getLogger(self.LOGGER)

    def test_canonical_name_resolves(self):
        self.assertIs(
            _resolve_pytz("America/Boise", self.log),
            pytz.timezone("America/Boise"),
        )

    def test_legacy_alias_resolves_without_falling_back(self):
        # intersections.json ships "US/Mountain" — a legacy alias.  pytz's
        # bundled database knows it; a tzdata-dependent implementation might
        # not, which is exactly the failure this function exists to avoid.
        resolved = _resolve_pytz("US/Mountain", self.log)
        self.assertIs(resolved, pytz.timezone("US/Mountain"))
        self.assertIsNot(resolved, pytz.utc)

    def test_utc_resolves_to_the_utc_singleton(self):
        # _fire_trigger normalises tz_name by identity against pytz.utc, so
        # this must be the same object, not merely an equal one.
        self.assertIs(_resolve_pytz("UTC", self.log), pytz.utc)

    def test_unknown_name_falls_back_to_utc(self):
        with self.assertLogs(self.LOGGER, level="WARNING"):
            self.assertIs(_resolve_pytz("Mars/Olympus_Mons", self.log), pytz.utc)

    def test_fallback_warning_names_the_offending_zone(self):
        with self.assertLogs(self.LOGGER, level="WARNING") as caught:
            _resolve_pytz("Not/AZone", self.log)
        self.assertEqual(len(caught.records), 1)
        self.assertEqual(caught.records[0].timezone, "Not/AZone")

    def test_empty_and_none_fall_back_rather_than_raise(self):
        for bad in ("", None):
            with self.subTest(tz_name=bad):
                with self.assertLogs(self.LOGGER, level="WARNING"):
                    self.assertIs(_resolve_pytz(bad, self.log), pytz.utc)

    def test_a_valid_name_logs_nothing(self):
        # assertNoLogs needs 3.10+; assertLogs on a sentinel record is the
        # portable equivalent — only that record should come back.
        with self.assertLogs(self.LOGGER, level="WARNING") as caught:
            _resolve_pytz("America/Boise", self.log)
            self.log.warning("sentinel")
        self.assertEqual([r.getMessage() for r in caught.records], ["sentinel"])

    def test_resolved_zone_actually_localizes(self):
        # Guards against returning something zone-shaped but unusable.
        tz = _resolve_pytz("US/Mountain", self.log)
        stamp = datetime.fromtimestamp(1754000000.0, tz=tz)
        self.assertIsNotNone(stamp.tzinfo)
        self.assertIn("M", stamp.strftime("%Z"))    # MST or MDT


# ---------------------------------------------------------------------------
# Decision log (ROADMAP 9C1)
# ---------------------------------------------------------------------------

class TestDecisionLog(unittest.TestCase):
    """The engine's own record of every trigger it emitted.

    Distinct from the video buffer's ``discrepancies_log.csv``, which only
    covers triggers that won a writer slot — see the engine module docstring.
    """

    THRESHOLD = 0.2
    FLOOR = 0.01

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "out" / "engine_decisions.csv"
        self.monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR,
            decision_log=self.log_path,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def rows(self):
        with self.log_path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def pulse(self, det_id: str, duration: float):
        self.monitor.on_detector_on(det_id)
        time.sleep(duration)
        self.monitor.on_detector_off(det_id)

    def fire_rule2(self):
        """Drive one confirmed orphan pulse on detector 1."""
        self.pulse("1", 0.05)
        time.sleep(self.THRESHOLD + 0.05)
        self.monitor._evaluate_pair("1:2", "1", "2")

    def fire_rule1_start(self):
        """Drive one Rule 1 start trigger."""
        self.monitor.on_detector_on("1")
        self.monitor._evaluate_pair("1:2", "1", "2")   # arm the timer
        time.sleep(self.THRESHOLD + 0.05)
        self.monitor._evaluate_pair("1:2", "1", "2")   # fires

    def test_no_path_writes_nothing(self):
        monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR
        )
        monitor.on_detector_on("1")
        monitor._evaluate_pair("1:2", "1", "2")
        time.sleep(self.THRESHOLD + 0.05)
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertFalse(self.log_path.exists())

    def test_parent_directory_is_created(self):
        self.fire_rule2()
        self.assertTrue(self.log_path.exists())

    def test_rule2_row_carries_the_exact_pulse_window(self):
        self.fire_rule2()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["rule"], "rule2_orphan_pulse")
        self.assertEqual(row["action"], "start")
        self.assertEqual(row["pair_key"], "1:2")
        self.assertEqual(row["cameras"], "cam1")
        # The window is the pulse itself, not the observation window, and its
        # span must equal the reported disagreement.
        start, end = float(row["event_start_ts"]), float(row["event_end_ts"])
        self.assertLess(start, end)
        self.assertAlmostEqual(
            end - start, float(row["disagreement_sec"]), places=2
        )
        # The decision is rendered after the pulse closed, never before it.
        self.assertGreaterEqual(float(row["event_timestamp"]), end)

    def test_rule1_start_row_has_an_open_window(self):
        self.fire_rule1_start()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["rule"], "rule1_continuous_disagreement")
        self.assertEqual(row["action"], "start")
        # The disagreement has a known beginning but is still open.
        self.assertAlmostEqual(
            float(row["event_timestamp"]) - float(row["event_start_ts"]),
            float(row["disagreement_sec"]),
            places=2,
        )
        self.assertEqual(row["event_end_ts"], "")

    def test_rule1_stop_is_logged_and_shares_the_trigger_id(self):
        self.fire_rule1_start()
        start_id = self.rows()[0]["trigger_id"]

        # Resolve the disagreement, then let the post-roll elapse.
        self.monitor.on_detector_off("1")
        self.monitor._post_roll_sec = 0.0
        self.monitor._evaluate_pair("1:2", "1", "2")   # starts post-roll
        self.monitor._evaluate_pair("1:2", "1", "2")   # sends the stop

        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["action"], "stop")
        self.assertEqual(rows[1]["trigger_id"], start_id)
        # A stop closes a recording; it reports no detection window.
        self.assertEqual(rows[1]["event_start_ts"], "")
        self.assertEqual(rows[1]["event_end_ts"], "")

    def test_trigger_id_matches_the_hot_folder_file(self):
        # The Trigger_ID join used to cross-reference the recording log only
        # works if both artifacts name the same trigger.
        self.fire_rule2()
        payloads = [
            json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(Path(self._tmp.name).glob("trigger_*.json"))
        ]
        self.assertEqual(len(payloads), 1)
        self.assertEqual(self.rows()[0]["trigger_id"], payloads[0]["trigger_id"])

    def test_header_written_once_across_appends(self):
        self.fire_rule2()
        self.monitor._pair_runtime["1:2"].cooldown_active = False
        self.fire_rule2()
        self.assertEqual(len(self.rows()), 2)
        lines = self.log_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 3)                     # header + 2 rows
        self.assertEqual(lines[0].split(",")[0], "event_timestamp")

    def test_existing_log_is_appended_not_reheadered(self):
        self.fire_rule2()
        first = self.log_path.read_text(encoding="utf-8")
        # A fresh monitor over the same path — the restart case.
        monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR,
            decision_log=self.log_path,
        )
        monitor.on_detector_on("1")
        time.sleep(0.05)
        monitor.on_detector_off("1")
        time.sleep(self.THRESHOLD + 0.05)
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertTrue(
            self.log_path.read_text(encoding="utf-8").startswith(first)
        )
        self.assertEqual(len(self.rows()), 2)

    def test_write_failure_does_not_block_the_trigger(self):
        # A full or read-only disk must degrade measurement, never recording.
        self.monitor._decision_log_path = (
            Path(self._tmp.name) / "not-a-dir.txt" / "decisions.csv"
        )
        Path(self._tmp.name, "not-a-dir.txt").write_text("blocker")
        with self.assertLogs("discrepancy_engine.test_int", level="ERROR"):
            self.fire_rule2()
        self.assertEqual(
            len(sorted(Path(self._tmp.name).glob("trigger_*.json"))), 1
        )


# ---------------------------------------------------------------------------
# Suppression log (ROADMAP 9C3)
# ---------------------------------------------------------------------------

class TestSuppressionLog(unittest.TestCase):
    """The engine's record of candidates it deliberately declined to act on.

    The counterpart to the decision log: that file says what was emitted,
    this one says what was withheld and why.  Today the only reason is the
    Rule 2 sampling-floor gate.
    """

    THRESHOLD = 0.2
    FLOOR = 0.06          # gate = FLOOR x _DEFAULT_MIN_PULSE_FLOOR_MULTIPLE
    GATE = 0.12
    PULSE = 0.05          # below the gate, and below the threshold

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "out" / "engine_suppressions.csv"
        self.decision_path = Path(self._tmp.name) / "out" / "engine_decisions.csv"
        self.monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR,
            decision_log=self.decision_path, suppression_log=self.log_path,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def rows(self):
        with self.log_path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def suppress(self, det_id="1", duration=None):
        """Drive one below-floor pulse and evaluate the pair once."""
        self.monitor.on_detector_on(det_id)
        time.sleep(self.PULSE if duration is None else duration)
        self.monitor.on_detector_off(det_id)
        self.monitor._evaluate_pair("1:2", "1", "2")

    def test_no_path_writes_nothing(self):
        monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR
        )
        monitor.on_detector_on("1")
        time.sleep(self.PULSE)
        monitor.on_detector_off("1")
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertFalse(self.log_path.exists())

    def test_parent_directory_is_created(self):
        self.suppress()
        self.assertTrue(self.log_path.exists())

    def test_row_records_the_reason(self):
        self.suppress()
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], _SUPPRESS_BELOW_FLOOR)
        self.assertEqual(rows[0]["rule"], "rule2_orphan_pulse")
        self.assertEqual(rows[0]["pair_key"], "1:2")

    def test_row_carries_the_exact_pulse_window(self):
        self.suppress()
        row = self.rows()[0]
        start, end = float(row["event_start_ts"]), float(row["event_end_ts"])
        self.assertLess(start, end)
        # The window's span is the duration that was measured against the gate.
        self.assertAlmostEqual(
            end - start, float(row["pulse_duration_sec"]), places=2
        )
        # And the decision was taken after the pulse closed, never before.
        self.assertGreaterEqual(float(row["event_timestamp"]), end)

    def test_suppressed_pulse_was_actually_below_the_gate(self):
        self.suppress()
        row = self.rows()[0]
        self.assertLess(
            float(row["pulse_duration_sec"]), float(row["min_pulse_sec"])
        )

    def test_gate_factors_are_recorded_separately(self):
        # The counterfactual sweep ("what would 1.5x have kept?") needs the
        # floor and the multiple, not just their product.
        self.suppress()
        row = self.rows()[0]
        self.assertAlmostEqual(float(row["sampling_floor_sec"]), self.FLOOR)
        self.assertAlmostEqual(
            float(row["sampling_floor_sec"])
            * float(row["min_pulse_floor_multiple"]),
            float(row["min_pulse_sec"]),
            places=4,
        )

    def test_slot_and_orphan_det_identify_the_detector(self):
        self.suppress(det_id="2")
        row = self.rows()[0]
        self.assertEqual(row["slot"], "b")
        self.assertEqual(row["orphan_det"], "2")
        self.assertEqual(row["det_a"], "1")
        self.assertEqual(row["det_b"], "2")
        self.assertEqual(row["det_a_type"], "radar")
        self.assertEqual(row["det_b_type"], "loop")

    def test_one_row_per_pulse_not_per_tick(self):
        # The evaluator re-reads the same last-pulse every 0.1 s tick.  A row
        # per tick would make the file useless for counting suppressions.
        self.suppress()
        for _ in range(4):
            self.monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(len(self.rows()), 1)

    def test_a_new_pulse_produces_a_new_row(self):
        self.suppress()
        self.suppress()
        self.assertEqual(len(self.rows()), 2)

    def test_registered_pulse_writes_no_row(self):
        # A pulse above the gate is evidence; it belongs in the decision log
        # if it fires, and never in this file.
        self.monitor.on_detector_on("1")
        time.sleep(self.GATE + 0.03)
        self.monitor.on_detector_off("1")
        self.monitor._evaluate_pair("1:2", "1", "2")
        self.assertFalse(self.log_path.exists())

    def test_suppression_produces_no_trigger_and_no_decision_row(self):
        # The whole point: withheld from the Hot Folder, but not from the
        # record.  A suppressed candidate must not reach either.
        self.suppress()
        self.assertEqual(
            sorted(Path(self._tmp.name).glob("trigger_*.json")), []
        )
        self.assertFalse(self.decision_path.exists())
        self.assertEqual(len(self.rows()), 1)

    def test_counter_and_log_agree(self):
        self.suppress()
        self.suppress()
        self.assertEqual(
            self.monitor._pair_runtime["1:2"].below_floor_suppressed,
            len(self.rows()),
        )

    def test_header_matches_the_declared_field_order(self):
        self.suppress()
        header = self.log_path.read_text(encoding="utf-8").splitlines()[0]
        self.assertEqual(header.split(","), list(_SUPPRESSION_LOG_FIELDS))

    def test_header_written_once_across_appends(self):
        self.suppress()
        self.suppress()
        lines = self.log_path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 3)                     # header + 2 rows

    def test_existing_log_is_appended_not_reheadered(self):
        self.suppress()
        first = self.log_path.read_text(encoding="utf-8")
        # A fresh monitor over the same path — the restart case.
        monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR,
            suppression_log=self.log_path,
        )
        monitor.on_detector_on("1")
        time.sleep(self.PULSE)
        monitor.on_detector_off("1")
        monitor._evaluate_pair("1:2", "1", "2")
        self.assertTrue(
            self.log_path.read_text(encoding="utf-8").startswith(first)
        )
        self.assertEqual(len(self.rows()), 2)

    def test_write_failure_does_not_block_evaluation(self):
        # Same contract as the decision log: measurement degrades, the engine
        # keeps running.
        self.monitor._suppression_log_path = (
            Path(self._tmp.name) / "not-a-dir.txt" / "suppressions.csv"
        )
        Path(self._tmp.name, "not-a-dir.txt").write_text("blocker")
        with self.assertLogs("discrepancy_engine.test_int", level="ERROR"):
            self.suppress()
        # The pulse was still gated, and the engine still evaluates.
        self.assertEqual(
            self.monitor._pair_runtime["1:2"].below_floor_suppressed, 1
        )
        self.monitor._evaluate_pair("1:2", "1", "2")


# ---------------------------------------------------------------------------
# Partner sub-floor-activity gate (ROADMAP 12A)
# ---------------------------------------------------------------------------

class TestPartnerBlipGate(unittest.TestCase):
    """The gate itself, driven directly through the registration helper.

    Rule 2's evidence is the partner's silence; this gate refuses that
    evidence when the partner has recently been producing pulses the engine
    cannot resolve.  Ordering against the floor gate is part of the contract.
    """

    THRESHOLD = 5.0
    NOW = 1000.0

    def setUp(self):
        self.rt = _PairRuntimeState(pair_key="1:2")

    def register(self, blips=(), max_blips=5, window=300.0,
                 last_pulse_on=990.0, last_off=992.0, min_pulse_sec=0.0):
        return DiscrepancyMonitor._maybe_register_orphan(
            self.rt, "a", False, last_pulse_on, last_off, self.THRESHOLD,
            min_pulse_sec, blips, self.NOW, window, max_blips,
        )

    @staticmethod
    def blips(count, first_off=900.0, spacing=10.0):
        """``count`` sub-floor partner pulses, oldest first."""
        return tuple(
            (first_off + i * spacing - 0.2, first_off + i * spacing)
            for i in range(count)
        )

    def test_partner_at_the_limit_declines_the_candidate(self):
        suppressed = self.register(blips=self.blips(5))
        self.assertIsNone(self.rt.orphan_watch_a)
        self.assertEqual(suppressed.reason, _SUPPRESS_PARTNER_BLIP)
        self.assertEqual(suppressed.pulse, (990.0, 992.0))
        self.assertEqual(suppressed.partner_blip_count, 5)
        self.assertEqual(self.rt.partner_blip_suppressed, 1)
        self.assertEqual(self.rt.below_floor_suppressed, 0)

    def test_partner_below_the_limit_registers_normally(self):
        self.assertIsNone(self.register(blips=self.blips(4)))
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))
        self.assertEqual(self.rt.partner_blip_suppressed, 0)

    def test_blips_outside_the_horizon_do_not_count(self):
        # Five blips, but all of them closed before now - window.
        old = self.blips(5, first_off=self.NOW - 400.0, spacing=5.0)
        self.assertIsNone(self.register(blips=old))
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))

    def test_only_blips_inside_the_horizon_are_counted(self):
        mixed = self.blips(4, first_off=self.NOW - 400.0, spacing=5.0) + \
            self.blips(4, first_off=self.NOW - 100.0, spacing=5.0)
        self.assertIsNone(self.register(blips=mixed))   # 4 inside, not 5
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))

    def test_blip_exactly_at_the_horizon_edge_is_excluded(self):
        # off_ts must be strictly newer than now - window.
        edge = ((self.NOW - 300.2, self.NOW - 300.0),) + self.blips(4)
        self.assertIsNone(self.register(blips=edge))
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))

    def test_zero_max_disables_the_gate(self):
        self.assertIsNone(self.register(blips=self.blips(50), max_blips=0))
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))
        self.assertEqual(self.rt.partner_blip_suppressed, 0)

    def test_zero_window_disables_the_gate(self):
        self.assertIsNone(self.register(blips=self.blips(50), window=0.0))
        self.assertEqual(self.rt.orphan_watch_a, (990.0, 992.0))

    def test_floor_gate_runs_first(self):
        # Load-bearing ordering: a below-floor pulse on a blip-heavy pair is
        # a below-floor suppression, never a partner-activity one — otherwise
        # the two populations overlap and neither count means anything.
        suppressed = self.register(
            blips=self.blips(5), last_pulse_on=990.0, last_off=990.05,
            min_pulse_sec=0.5,
        )
        self.assertEqual(suppressed.reason, _SUPPRESS_BELOW_FLOOR)
        self.assertEqual(suppressed.partner_blip_count, 0)
        self.assertEqual(self.rt.below_floor_suppressed, 1)
        self.assertEqual(self.rt.partner_blip_suppressed, 0)

    def test_declined_candidate_is_counted_once_not_per_tick(self):
        results = [self.register(blips=self.blips(5)) for _ in range(5)]
        self.assertEqual(
            [None if r is None else r.reason for r in results],
            [_SUPPRESS_PARTNER_BLIP, None, None, None, None],
        )
        self.assertEqual(self.rt.partner_blip_suppressed, 1)

    def test_gate_does_not_reject_a_pulse_over_the_threshold(self):
        # Rule 1 territory: still not a Rule 2 candidate, and not a
        # suppression row either.
        self.assertIsNone(
            self.register(blips=self.blips(5), last_pulse_on=990.0,
                          last_off=996.0)
        )


class TestPartnerBlipGateIntegration(unittest.TestCase):
    """End to end: the floor gate fills the blip history the partner gate reads."""

    THRESHOLD = 0.4
    FLOOR = 0.06          # gate = FLOOR x _DEFAULT_MIN_PULSE_FLOOR_MULTIPLE
    GATE = 0.12
    BLIP = 0.05           # below the gate
    ORPHAN = 0.2          # above the gate, below the threshold

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "out" / "engine_suppressions.csv"
        self.monitor = _build_monitor(
            self._tmp.name, threshold=self.THRESHOLD, floor=self.FLOOR,
            suppression_log=self.log_path,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def rows(self):
        with self.log_path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def pulse(self, det_id, duration, pair="1:2", dets=("1", "2")):
        self.monitor.on_detector_on(det_id)
        time.sleep(duration)
        self.monitor.on_detector_off(det_id)
        self.monitor._evaluate_pair(pair, *dets)

    def blip_partner(self, count, det_id="2"):
        for _ in range(count):
            self.pulse(det_id, self.BLIP)

    def test_blip_history_comes_from_declined_floor_candidates(self):
        self.blip_partner(3)
        self.assertEqual(
            len(self.monitor._detector_states["2"].below_floor_pulses), 3
        )
        # A registered (above-floor) pulse is evidence, not a blip.
        self.pulse("2", self.ORPHAN)
        self.assertEqual(
            len(self.monitor._detector_states["2"].below_floor_pulses), 3
        )

    def test_blip_heavy_partner_declines_the_orphan(self):
        self.blip_partner(5)
        self.pulse("1", self.ORPHAN)
        rt = self.monitor._pair_runtime["1:2"]
        self.assertIsNone(rt.orphan_watch_a)
        self.assertEqual(rt.partner_blip_suppressed, 1)
        # Declined, so nothing reached the Hot Folder for it.
        self.assertEqual(sorted(Path(self._tmp.name).glob("trigger_*.json")), [])

    def test_quiet_partner_still_arms_the_orphan(self):
        self.blip_partner(4)
        self.pulse("1", self.ORPHAN)
        rt = self.monitor._pair_runtime["1:2"]
        self.assertIsNotNone(rt.orphan_watch_a)
        self.assertEqual(rt.partner_blip_suppressed, 0)

    def test_row_records_the_reason_count_and_horizon(self):
        self.blip_partner(5)
        self.pulse("1", self.ORPHAN)
        row = self.rows()[-1]
        self.assertEqual(row["reason"], _SUPPRESS_PARTNER_BLIP)
        self.assertEqual(row["orphan_det"], "1")
        self.assertEqual(row["slot"], "a")
        self.assertEqual(int(row["partner_blip_count"]), 5)
        self.assertAlmostEqual(
            float(row["partner_blip_window_sec"]),
            _DEFAULT_PARTNER_BLIP_WINDOW_SEC,
        )
        self.assertIn("'2' produced 5 below-floor pulses", row["description"])
        # The pulse columns still describe the declined candidate itself.
        self.assertAlmostEqual(
            float(row["pulse_duration_sec"]), self.ORPHAN, places=1
        )

    def test_below_floor_rows_leave_the_partner_columns_blank(self):
        self.blip_partner(5)
        floor_rows = [r for r in self.rows()
                      if r["reason"] == _SUPPRESS_BELOW_FLOOR]
        self.assertEqual(len(floor_rows), 5)
        for row in floor_rows:
            self.assertEqual(row["partner_blip_count"], "")
            self.assertEqual(row["partner_blip_window_sec"], "")

    def test_header_still_matches_the_declared_field_order(self):
        self.blip_partner(1)
        header = self.log_path.read_text(encoding="utf-8").splitlines()[0]
        self.assertEqual(header.split(","), list(_SUPPRESSION_LOG_FIELDS))

    def test_expired_blips_are_pruned_from_the_deque(self):
        self.blip_partner(2)
        blips = self.monitor._detector_states["2"].below_floor_pulses
        self.assertEqual(len(blips), 2)
        # Age both past the horizon; the next evaluation must drop them.
        self.monitor._partner_blip_window_sec = 0.001
        time.sleep(0.01)
        self.monitor._evaluate_pair("1:2", "1", "2")
        self.assertEqual(len(blips), 0)

    def test_triangle_records_one_entry_per_physical_pulse(self):
        # The same sub-floor pulse on a shared detector is declined once per
        # pair it participates in; the gate counts pulses, not evaluations.
        monitor = _build_grouped_monitor(
            self._tmp.name,
            {"1": _det("2", threshold=self.THRESHOLD),
             "2": _det("3", threshold=self.THRESHOLD),
             "3": _det("1", threshold=self.THRESHOLD)},
            floor=self.FLOOR,
        )
        monitor.on_detector_on("2")
        time.sleep(self.BLIP)
        monitor.on_detector_off("2")
        monitor._evaluate_pair("1:2", "1", "2")
        monitor._evaluate_pair("2:3", "2", "3")
        self.assertEqual(len(monitor._detector_states["2"].below_floor_pulses), 1)
        # Both pairs did count the suppression for themselves, though.
        self.assertEqual(monitor._pair_runtime["1:2"].below_floor_suppressed, 1)
        self.assertEqual(monitor._pair_runtime["2:3"].below_floor_suppressed, 1)


class TestPartnerBlipConfig(unittest.TestCase):
    """Config plumb-through for the gate's two keys."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, **cfg):
        return _build_monitor(self._tmp.name, **cfg)

    def test_defaults_when_absent(self):
        monitor = self.build()
        self.assertEqual(
            monitor._partner_blip_window_sec, _DEFAULT_PARTNER_BLIP_WINDOW_SEC
        )
        self.assertEqual(monitor._partner_blip_max, _DEFAULT_PARTNER_BLIP_MAX)

    def test_config_values_are_read(self):
        monitor = self.build(partner_blip_window_sec=120.0, partner_blip_max=3)
        self.assertEqual(monitor._partner_blip_window_sec, 120.0)
        self.assertEqual(monitor._partner_blip_max, 3)

    def test_explicit_zero_disables_each_side(self):
        self.assertEqual(self.build(partner_blip_max=0)._partner_blip_max, 0)
        self.assertEqual(
            self.build(partner_blip_window_sec=0)._partner_blip_window_sec, 0.0
        )

    def test_malformed_or_negative_values_fall_back_to_the_default(self):
        # A typo must not silently disable a gate that exists to prevent a
        # false-positive population — same posture as dedup_window_sec.
        for bad in ("banana", None, -5):
            with self.subTest(value=bad):
                monitor = self.build(
                    partner_blip_max=bad, partner_blip_window_sec=bad
                )
                self.assertEqual(
                    monitor._partner_blip_max, _DEFAULT_PARTNER_BLIP_MAX
                )
                self.assertEqual(
                    monitor._partner_blip_window_sec,
                    _DEFAULT_PARTNER_BLIP_WINDOW_SEC,
                )

    def test_reload_re_reads_both_keys(self):
        monitor = self.build(partner_blip_max=3)
        cfg = {
            "timezone": "UTC",
            "partner_blip_max": 9,
            "partner_blip_window_sec": 60.0,
            "detectors": monitor._intersection_cfg["detectors"],
        }
        monitor.reload(_StubProvider(cfg))
        self.assertEqual(monitor._partner_blip_max, 9)
        self.assertEqual(monitor._partner_blip_window_sec, 60.0)


# ---------------------------------------------------------------------------
# Detector groups — connected components over the pair graph (ROADMAP 9C4)
# ---------------------------------------------------------------------------

def _det(partner, phase=2, camera="cam1", threshold=0.2, det_type="radar"):
    """One detector config entry; ``partner`` may be a scalar or a list."""
    return {
        "paired_detector_id": partner,
        "phase":              phase,
        "camera_id":          camera,
        "lag_threshold_sec":  threshold,
        "type":               det_type,
    }


def _build_grouped_monitor(trigger_dir, detectors, floor=0.01,
                           suppression_log=None, **extra_cfg):
    """Build a monitor over an arbitrary detector map (no pair assumptions)."""
    cfg = {"timezone": "UTC", "detectors": detectors}
    cfg.update(extra_cfg)
    monitor = DiscrepancyMonitor(
        intersection_id="test_int",
        config_provider=_StubProvider(cfg),
        trigger_dir=trigger_dir,
        cooldown_sec=60.0,
        suppression_log_path=suppression_log,
    )
    monitor.set_sampling_floor(floor)
    return monitor


# The five triangles in intersections/201.json are authored as rings of scalars;
# the same group may also be authored as explicit lists.  Both must unify.
_RING_TRIANGLE = {
    "2":  _det("17"),
    "17": _det("46"),
    "46": _det("2"),
}
_LIST_TRIANGLE = {
    "2":  _det(["17", "46"]),
    "17": _det(["2", "46"]),
    "46": _det(["2", "17"]),
}


class TestDetectorGroups(unittest.TestCase):
    """Group derivation: both config forms, and the over-grouping guard."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, detectors, **extra_cfg):
        return _build_grouped_monitor(self._tmp.name, detectors, **extra_cfg)

    def test_ring_of_scalars_derives_one_triangle(self):
        m = self.build(_RING_TRIANGLE)
        self.assertEqual(sorted(m._pairs), ["17:2", "17:46", "2:46"])
        self.assertEqual(m._groups, {"2:17:46": ["2", "17", "46"]})

    def test_list_form_derives_the_identical_pairs_and_group(self):
        ring = self.build(_RING_TRIANGLE)
        lists = self.build(_LIST_TRIANGLE)
        # The coincidence that makes one mechanism serve both forms — and it
        # is specific to n=3 (see test_four_ring_does_not_become_all_pairs).
        self.assertEqual(sorted(lists._pairs), sorted(ring._pairs))
        self.assertEqual(lists._groups, ring._groups)

    def test_every_pair_maps_to_its_group(self):
        m = self.build(_RING_TRIANGLE)
        self.assertEqual(
            set(m._pair_group.values()), {"2:17:46"}
        )
        self.assertEqual(set(m._pair_group), set(m._pairs))

    def test_group_id_orders_numeric_ids_numerically(self):
        # "17:2:46" would be the lexicographic answer and is unreadable.
        m = self.build(_RING_TRIANGLE)
        self.assertIn("2:17:46", m._groups)

    def test_independent_pairs_stay_separate_groups(self):
        m = self.build({
            "1": _det("2", phase=2), "2": _det("1", phase=2),
            "3": _det("4", phase=4), "4": _det("3", phase=4),
        })
        self.assertEqual(sorted(m._groups), ["1:2", "3:4"])

    def test_four_ring_gives_four_pairs_not_six(self):
        # A group is a dedup scope, never an instruction to evaluate every
        # internal pair — a 4-ring must not silently grow two comparisons.
        m = self.build({
            "1": _det("2"), "2": _det("3"), "3": _det("4"), "4": _det("1"),
        })
        self.assertEqual(len(m._pairs), 4)
        self.assertEqual(m._groups, {"1:2:3:4": ["1", "2", "3", "4"]})

    def test_four_detector_list_form_gives_six_pairs(self):
        # The other half of the same point: the list form is how you say
        # "compare all of these", and it does.
        m = self.build({
            "1": _det(["2", "3", "4"]), "2": _det(["1", "3", "4"]),
            "3": _det(["1", "2", "4"]), "4": _det(["1", "2", "3"]),
        })
        self.assertEqual(len(m._pairs), 6)
        self.assertEqual(sorted(m._groups), ["1:2:3:4"])

    def test_unknown_id_inside_a_list_drops_only_that_link(self):
        with self.assertLogs("discrepancy_engine.test_int", level="WARNING"):
            m = self.build({
                "1": _det(["2", "99"]), "2": _det("1"),
            })
        self.assertEqual(sorted(m._pairs), ["1:2"])

    def test_self_link_is_ignored(self):
        with self.assertLogs("discrepancy_engine.test_int", level="WARNING"):
            m = self.build({"1": _det(["1", "2"]), "2": _det("1")})
        self.assertEqual(sorted(m._pairs), ["1:2"])

    def test_phase_coherent_group_warns_nothing(self):
        logger = logging.getLogger("discrepancy_engine.test_int")
        with self.assertLogs(logger, level="DEBUG") as captured:
            self.build(_RING_TRIANGLE)
        self.assertFalse([
            r for r in captured.records if r.levelno >= logging.WARNING
        ])

    def test_transitive_over_grouping_warns(self):
        # One stray link merges two intended groups with no other symptom.
        with self.assertLogs(
            "discrepancy_engine.test_int", level="WARNING"
        ) as captured:
            m = self.build({
                "1": _det("2", phase=2), "2": _det("3", phase=2),
                "3": _det("4", phase=4), "4": _det("3", phase=4),
            })
        self.assertEqual(len(m._groups), 1)
        self.assertTrue(any(
            "more than one phase" in r.getMessage()
            for r in captured.records
        ))

    def test_reload_dissolving_a_group_drops_its_anchor(self):
        m = self.build(_RING_TRIANGLE)
        m._group_last_fire[("2:17:46", "cam1")] = (100.0, "abcdef")
        m.reload(_StubProvider({
            "timezone": "UTC",
            "detectors": {"1": _det("2"), "2": _det("1")},
        }))
        self.assertEqual(m._group_last_fire, {})

    def test_reload_keeps_a_surviving_groups_anchor(self):
        m = self.build(_RING_TRIANGLE)
        m._group_last_fire[("2:17:46", "cam1")] = (100.0, "abcdef")
        m.reload(_StubProvider({"timezone": "UTC",
                                "detectors": dict(_LIST_TRIANGLE)}))
        self.assertIn(("2:17:46", "cam1"), m._group_last_fire)


# ---------------------------------------------------------------------------
# Cross-pair duplicate rejection (ROADMAP 9C4)
# ---------------------------------------------------------------------------

class TestCrossPairDuplicates(unittest.TestCase):
    """One physical event fires on two pairs of a triangle; only one records."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "engine_decisions.csv"
        self.monitor = self.build(_RING_TRIANGLE)

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, detectors, **extra_cfg):
        cfg = {"timezone": "UTC", "detectors": detectors}
        cfg.update(extra_cfg)
        monitor = DiscrepancyMonitor(
            intersection_id="test_int",
            config_provider=_StubProvider(cfg),
            trigger_dir=self._tmp.name,
            cooldown_sec=60.0,
            post_roll_sec=0.0,
            decision_log_path=self.log_path,
        )
        monitor.set_sampling_floor(0.01)
        return monitor

    def rows(self):
        with self.log_path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def triggers(self):
        return sorted(Path(self._tmp.name).glob("trigger_*.json"))

    def fire(self, monitor, pair_key, det_a, det_b, ts,
             rule="rule2_orphan_pulse", action="start", **kwargs):
        """Fire one trigger at an exact timestamp, bypassing the rules."""
        monitor._fire_trigger(
            pair_key=pair_key, det_a_id=det_a, det_b_id=det_b,
            rule=rule, description="test", disagreement_sec=1.0,
            event_ts=ts, action=action, **kwargs
        )

    # ── The core behavior ────────────────────────────────────────────────

    def test_sibling_pair_within_the_window_writes_no_trigger_file(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1000.1)
        self.assertEqual(len(self.triggers()), 1)

    def test_the_suppressed_decision_is_still_logged_and_marked(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1000.1)
        rows = self.rows()
        # Both decisions are recorded — ground truth contains the event on
        # both pairs, so dropping the row would score a miss.
        self.assertEqual(len(rows), 2)
        first, second = rows
        self.assertEqual(first["suppressed_as_duplicate"], "0")
        self.assertEqual(first["duplicate_of_trigger_id"], "")
        self.assertEqual(second["suppressed_as_duplicate"], "1")
        self.assertEqual(
            second["duplicate_of_trigger_id"], first["trigger_id"]
        )

    def test_every_row_carries_its_derived_group(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1000.1)
        self.assertEqual({r["dedup_group"] for r in self.rows()}, {"2:17:46"})

    def test_beyond_the_window_both_fire(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1004.0)
        self.assertEqual(len(self.triggers()), 2)
        self.assertEqual(
            [r["suppressed_as_duplicate"] for r in self.rows()], ["0", "0"]
        )

    def test_boundary_exactly_at_the_window_is_a_duplicate(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1003.0)
        self.assertEqual(len(self.triggers()), 1)

    def test_separate_groups_never_deduplicate_each_other(self):
        monitor = self.build({
            "1": _det("2", phase=2), "2": _det("1", phase=2),
            "3": _det("4", phase=4), "4": _det("3", phase=4),
        })
        self.fire(monitor, "1:2", "1", "2", 1000.0)
        self.fire(monitor, "3:4", "3", "4", 1000.1)
        self.assertEqual(len(self.triggers()), 2)

    def test_different_cameras_in_one_group_are_not_duplicates(self):
        monitor = self.build({
            "2":  _det("17", camera="camA"),
            "17": _det("46", camera="camA"),
            "46": _det("2",  camera="camB"),
        })
        # 17:2 → camA only; 17:46 → camA+camB: different footage, both fire.
        self.fire(monitor, "17:2", "2", "17", 1000.0)
        self.fire(monitor, "17:46", "17", "46", 1000.1)
        self.assertEqual(len(self.triggers()), 2)

    def test_suppression_does_not_anchor_the_window(self):
        # Otherwise a storm rolls the window forward and suppresses forever.
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1002.0)   # suppressed
        self.fire(self.monitor, "2:46", "2", "46", 1004.0)     # 4.0s from 1000
        self.assertEqual(len(self.triggers()), 2)
        self.assertEqual(
            [r["suppressed_as_duplicate"] for r in self.rows()],
            ["0", "1", "0"],
        )

    def test_zero_window_disables_the_mechanism(self):
        monitor = self.build(_RING_TRIANGLE, dedup_window_sec=0)
        self.fire(monitor, "17:2", "2", "17", 1000.0)
        self.fire(monitor, "17:46", "17", "46", 1000.0)
        self.assertEqual(len(self.triggers()), 2)

    def test_configured_window_is_honoured(self):
        monitor = self.build(_RING_TRIANGLE, dedup_window_sec=8.0)
        self.fire(monitor, "17:2", "2", "17", 1000.0)
        self.fire(monitor, "17:46", "17", "46", 1006.0)
        self.assertEqual(len(self.triggers()), 1)

    def test_malformed_window_falls_back_to_the_default(self):
        # A typo must not silently restore the duplicate storm.
        monitor = self.build(_RING_TRIANGLE, dedup_window_sec="soon")
        self.assertEqual(monitor._dedup_window_sec, 3.0)

    # ── Rule 1: the start/stop pairing (the fiddly part) ─────────────────

    def fire_rule1(self, monitor, pair_key, det_a, det_b, ts):
        self.fire(monitor, pair_key, det_a, det_b, ts,
                  rule="rule1_continuous_disagreement",
                  event_window=(ts - 1.0, None))

    def test_suppressed_rule1_start_does_not_arm_the_state_machine(self):
        # Arming it would later send the buffer a "stop" for a recording it
        # never started.
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1000.1)
        rt = self.monitor._pair_runtime["17:46"]
        self.assertIsNone(rt.active_trigger_id)

    def test_suppressed_start_engages_cooldown_instead(self):
        # The group is already recording this moment; the pair must not
        # re-fire on the same physical event one threshold later.
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1000.1)
        rt = self.monitor._pair_runtime["17:46"]
        self.assertTrue(rt.cooldown_active)
        self.assertIsNone(rt.disagreement_start)

    def test_rule1_is_never_folded_into_a_rule2_recording(self):
        # A rule 2 clip's length is fixed at fire time and never gets a stop,
        # so it cannot be held open until the rule 1 disagreement resolves.
        # Two clips beat one truncated one.
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)   # rule 2 owner
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1000.1)
        self.assertEqual(len(self.triggers()), 2)
        self.assertEqual(
            [r["suppressed_as_duplicate"] for r in self.rows()], ["0", "0"]
        )
        # And it owns its own recording, so it can close it.
        self.assertIsNotNone(
            self.monitor._pair_runtime["17:46"].active_trigger_id
        )

    def test_rule2_is_still_folded_into_a_rule1_recording(self):
        # The reverse direction is safe: an orphan pulse is complete before it
        # is even evaluated, so it needs no holding open.
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1000.1)
        self.assertEqual(len(self.triggers()), 1)

    def test_an_emitted_rule1_start_still_arms_the_state_machine(self):
        self.fire(self.monitor, "17:46", "17", "46", 1000.0,
                  rule="rule1_continuous_disagreement",
                  event_window=(999.0, None))
        rt = self.monitor._pair_runtime["17:46"]
        self.assertIsNotNone(rt.active_trigger_id)
        self.assertFalse(rt.cooldown_active)

    def test_a_stop_is_never_suppressed(self):
        # Its start is already recording; suppressing the stop would strand
        # the clip until max_duration_sec.
        self.fire(self.monitor, "17:46", "17", "46", 1000.0,
                  rule="rule1_continuous_disagreement",
                  event_window=(999.0, None))
        active_id = self.monitor._pair_runtime["17:46"].active_trigger_id
        self.fire(self.monitor, "17:2", "2", "17", 1000.2)   # sibling start
        self.fire(self.monitor, "17:46", "17", "46", 1000.3,
                  rule="rule1_continuous_disagreement", action="stop",
                  trigger_id_override=active_id)
        actions = [r["action"] for r in self.rows()]
        self.assertEqual(actions, ["start", "start", "stop"])
        self.assertEqual(
            [r["suppressed_as_duplicate"] for r in self.rows()],
            ["0", "1", "0"],
        )
        self.assertEqual(len(self.triggers()), 2)           # start + stop

    def test_a_stop_does_not_anchor_the_window(self):
        # Anchoring on it would suppress the *next* genuine group event.
        self.fire(self.monitor, "17:46", "17", "46", 1000.0,
                  rule="rule1_continuous_disagreement",
                  event_window=(999.0, None))
        active_id = self.monitor._pair_runtime["17:46"].active_trigger_id
        self.fire(self.monitor, "17:46", "17", "46", 1002.0,
                  rule="rule1_continuous_disagreement", action="stop",
                  trigger_id_override=active_id)
        self.fire(self.monitor, "17:2", "2", "17", 1002.1)
        self.assertEqual(
            [r["suppressed_as_duplicate"] for r in self.rows()],
            ["0", "0", "0"],
        )

    def test_suppressed_row_carries_the_same_columns_as_an_emitted_one(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire(self.monitor, "17:46", "17", "46", 1000.1,
                  event_window=(999.5, 999.9))
        emitted, suppressed = self.rows()
        self.assertEqual(set(emitted), set(suppressed))
        self.assertEqual(suppressed["pair_key"], "17:46")
        self.assertEqual(suppressed["event_start_ts"], "999.500")
        self.assertEqual(suppressed["event_end_ts"], "999.900")
        self.assertNotEqual(suppressed["trigger_id"], emitted["trigger_id"])

    def test_header_matches_the_declared_field_order(self):
        self.fire(self.monitor, "17:2", "2", "17", 1000.0)
        header = self.log_path.read_text(encoding="utf-8").splitlines()[0]
        self.assertEqual(header.split(","), list(_DECISION_LOG_FIELDS))

    # ── End-to-end through the rules, not _fire_trigger ──────────────────

    def test_triangle_event_end_to_end_records_once(self):
        # Detector 17 disagrees with both 2 and 46: two pairs, one physical
        # event, one clip.
        monitor = self.build(_RING_TRIANGLE)
        monitor.on_detector_on("17")
        for pair_key, (a, b) in monitor._pairs.items():
            monitor._evaluate_pair(pair_key, a, b)       # arm the timers
        time.sleep(0.25)                                  # threshold is 0.2
        for pair_key, (a, b) in monitor._pairs.items():
            monitor._evaluate_pair(pair_key, a, b)
        # Pairs 17:2 and 17:46 both crossed the threshold; 2:46 agrees.
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            sorted(r["suppressed_as_duplicate"] for r in rows), ["0", "1"]
        )
        self.assertEqual(len(self.triggers()), 1)


# ---------------------------------------------------------------------------
# Per-rule dedup windows + the Rule 2 coverage guard (ROADMAP 14)
# ---------------------------------------------------------------------------

class TestPerRuleDedupWindows(unittest.TestCase):
    """A Rule 1 fold is held open; a Rule 2 fold must already be on film.

    The windows differ because the guarantee differs, so these cases exercise
    the two paths independently — and the guard that bounds the Rule 2 one.
    """

    PRE_ROLL = 5.0
    POST_ROLL = 5.0
    THRESHOLD = 5.0

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "engine_decisions.csv"
        self.monitor = self.build()

    def tearDown(self):
        self._tmp.cleanup()

    def build(self, **extra_cfg):
        cfg = {
            "timezone": "UTC",
            "detectors": {
                k: _det(v, threshold=self.THRESHOLD)
                for k, v in (("2", "17"), ("17", "46"), ("46", "2"))
            },
        }
        cfg.update(extra_cfg)
        monitor = DiscrepancyMonitor(
            intersection_id="test_int",
            config_provider=_StubProvider(cfg),
            trigger_dir=self._tmp.name,
            cooldown_sec=60.0,
            pre_roll_sec=self.PRE_ROLL,
            post_roll_sec=self.POST_ROLL,
            decision_log_path=self.log_path,
        )
        monitor.set_sampling_floor(0.01)
        return monitor

    def triggers(self):
        return sorted(Path(self._tmp.name).glob("trigger_*.json"))

    def fire_rule1(self, monitor, pair_key, det_a, det_b, ts):
        monitor._fire_trigger(
            pair_key=pair_key, det_a_id=det_a, det_b_id=det_b,
            rule="rule1_continuous_disagreement", description="test",
            disagreement_sec=self.THRESHOLD, event_ts=ts, action="start",
            event_window=(ts - self.THRESHOLD, None),
        )

    def fire_rule2(self, monitor, pair_key, det_a, det_b, pulse_on, pulse_off):
        """Fire a Rule 2 orphan exactly as the evaluator would.

        The trigger lands one threshold after the pulse closes, and its clip
        length is the same arithmetic ``_evaluate_pair`` uses — which is what
        makes the owner's span, and therefore the guard, meaningful.
        """
        monitor._fire_trigger(
            pair_key=pair_key, det_a_id=det_a, det_b_id=det_b,
            rule="rule2_orphan_pulse", description="test",
            disagreement_sec=round(pulse_off - pulse_on, 3),
            event_ts=pulse_off + self.THRESHOLD, action="start",
            duration_override=(
                self.PRE_ROLL + (pulse_off - pulse_on)
                + self.POST_ROLL + self.THRESHOLD
            ),
            event_window=(pulse_on, pulse_off),
        )

    # ── The Rule 1 path: wide, because the AND-stop holds the clip open ──

    def test_rule1_candidate_eight_seconds_later_is_folded_and_held(self):
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1008.0)
        self.assertEqual(len(self.triggers()), 1)
        owner_rt = self.monitor._pair_runtime["17:2"]
        self.assertEqual(owner_rt.held_pair_keys, ["17:46"])

    def test_a_folded_rule1_start_still_never_arms_its_own_recording(self):
        # The 9C4 invariant, re-checked at the wider window.
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1008.0)
        rt = self.monitor._pair_runtime["17:46"]
        self.assertIsNone(rt.active_trigger_id)
        self.assertTrue(rt.cooldown_active)

    def test_rule1_into_a_rule2_owner_is_still_refused_at_the_wider_window(self):
        # dedup_window_rule1_sec must not override the rule-1-into-rule-2
        # refusal: a fixed-length clip cannot be held open.
        self.fire_rule2(self.monitor, "17:2", "2", "17", 993.0, 995.0)
        self.fire_rule1(self.monitor, "17:46", "17", "46", 1008.0)
        self.assertEqual(len(self.triggers()), 2)

    def test_the_rule1_window_does_not_widen_the_rule2_path(self):
        # A Rule 2 candidate 8 s after a Rule 1 owner is past its own 3.0 s
        # window, whatever the Rule 1 window says.
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule2(self.monitor, "17:46", "17", "46", 1001.0, 1003.0)
        self.assertEqual(len(self.triggers()), 2)

    # ── The Rule 2 path: narrow, and only onto footage that exists ───────

    def test_rule2_fold_inside_a_rule2_owners_fixed_span_is_suppressed(self):
        # Owner clip spans [988.0, 1005.0] in event coordinates.
        self.fire_rule2(self.monitor, "17:2", "2", "17", 993.0, 995.0)
        self.fire_rule2(self.monitor, "17:46", "17", "46", 995.0, 997.0)
        self.assertEqual(len(self.triggers()), 1)

    def test_a_pulse_ending_past_the_owners_span_is_not_folded(self):
        # Needed window runs to 1006.0; the owner's clip stops at 1005.0.
        self.fire_rule2(self.monitor, "17:2", "2", "17", 993.0, 995.0)
        self.fire_rule2(self.monitor, "17:46", "17", "46", 998.0, 1001.0)
        self.assertEqual(len(self.triggers()), 2)

    def test_a_pulse_starting_before_the_owners_span_is_not_folded(self):
        # The owner's clip is anchored on its own event; a candidate whose
        # pulse began earlier needs footage that clip never bought.
        self.fire_rule2(self.monitor, "17:2", "2", "17", 993.0, 995.0)
        self.fire_rule2(self.monitor, "17:46", "17", "46", 990.0, 996.0)
        self.assertEqual(len(self.triggers()), 2)

    def test_rule2_folds_into_a_rule1_owner_that_is_still_recording(self):
        self.fire_rule1(self.monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule2(self.monitor, "17:46", "17", "46", 995.5, 997.5)
        self.assertEqual(len(self.triggers()), 1)

    def test_rule2_is_not_folded_into_a_rule1_owner_that_has_stopped(self):
        # Only reachable with the window raised past the owner's own length,
        # which is exactly why the guard is not optional.
        monitor = self.build(dedup_window_sec=20.0)
        self.fire_rule1(monitor, "17:2", "2", "17", 1000.0)
        owner_id = monitor._pair_runtime["17:2"].active_trigger_id
        monitor._fire_trigger(
            pair_key="17:2", det_a_id="2", det_b_id="17",
            rule="rule1_continuous_disagreement", description="test",
            disagreement_sec=3.0, event_ts=1003.0, action="stop",
            trigger_id_override=owner_id,
        )
        self.fire_rule2(monitor, "17:46", "17", "46", 1000.0, 1002.0)
        # start + stop + the candidate's own start
        self.assertEqual(len(self.triggers()), 3)

    # ── Config ──────────────────────────────────────────────────────────

    def test_defaults_are_three_and_ten(self):
        self.assertEqual(self.monitor._dedup_window_sec, 3.0)
        self.assertEqual(self.monitor._dedup_window_rule1_sec, 10.0)

    def test_zero_rule2_window_leaves_the_rule1_path_alone(self):
        monitor = self.build(dedup_window_sec=0)
        self.fire_rule2(monitor, "17:2", "2", "17", 993.0, 995.0)
        self.fire_rule2(monitor, "17:46", "17", "46", 995.0, 997.0)
        self.assertEqual(len(self.triggers()), 2)
        self.fire_rule1(monitor, "2:46", "2", "46", 1100.0)
        self.fire_rule1(monitor, "17:2", "2", "17", 1108.0)
        self.assertEqual(len(self.triggers()), 3)

    def test_zero_rule1_window_leaves_the_rule2_path_alone(self):
        monitor = self.build(dedup_window_rule1_sec=0)
        self.fire_rule1(monitor, "17:2", "2", "17", 1000.0)
        self.fire_rule1(monitor, "17:46", "17", "46", 1008.0)
        self.assertEqual(len(self.triggers()), 2)
        self.fire_rule2(monitor, "2:46", "2", "46", 1093.0, 1095.0)
        self.fire_rule2(monitor, "17:2", "2", "17", 1095.0, 1097.0)
        self.assertEqual(len(self.triggers()), 3)

    def test_malformed_rule1_window_falls_back_to_the_default(self):
        monitor = self.build(dedup_window_rule1_sec="ten")
        self.assertEqual(monitor._dedup_window_rule1_sec, 10.0)

    def test_reload_picks_up_both_windows(self):
        self.monitor.reload(_StubProvider({
            "timezone": "UTC",
            "detectors": {
                k: _det(v, threshold=self.THRESHOLD)
                for k, v in (("2", "17"), ("17", "46"), ("46", "2"))
            },
            "dedup_window_sec": 4.0,
            "dedup_window_rule1_sec": 12.0,
        }))
        self.assertEqual(self.monitor._dedup_window_sec, 4.0)
        self.assertEqual(self.monitor._dedup_window_rule1_sec, 12.0)


# ---------------------------------------------------------------------------
# The stop is an AND across every disagreement the clip stands for (9C4)
# ---------------------------------------------------------------------------

class TestHeldPairResolution(unittest.TestCase):
    """Detector 17 disagrees with both 2 and 46; one clip covers both.

    The pair that fires first owns the recording, but it does not get to
    decide alone when the footage ends — the stop waits for every pair whose
    own trigger was folded into it.
    """

    THRESHOLD = 0.2

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.log_path = Path(self._tmp.name) / "engine_decisions.csv"
        self.monitor = DiscrepancyMonitor(
            intersection_id="test_int",
            config_provider=_StubProvider({
                "timezone": "UTC",
                "detectors": {
                    k: _det(v, threshold=self.THRESHOLD)
                    for k, v in (("2", "17"), ("17", "46"), ("46", "2"))
                },
            }),
            trigger_dir=self._tmp.name,
            cooldown_sec=60.0,
            post_roll_sec=0.0,
            decision_log_path=self.log_path,
        )
        self.monitor.set_sampling_floor(0.01)
        # Pair order is insertion order: 17:2 fires first and owns the clip.
        self.owner, self.held = "17:2", "17:46"

    def tearDown(self):
        self._tmp.cleanup()

    def evaluate_all(self):
        for pair_key, (a, b) in self.monitor._pairs.items():
            self.monitor._evaluate_pair(pair_key, a, b)

    def triggers(self):
        return [
            json.loads(p.read_text(encoding="utf-8"))
            for p in sorted(Path(self._tmp.name).glob("trigger_*.json"))
        ]

    def start_recording(self):
        """Drive 17 ON against both partners until the owner's start fires."""
        self.monitor.on_detector_on("17")
        self.evaluate_all()                       # arm both timers
        time.sleep(self.THRESHOLD + 0.05)
        self.evaluate_all()                       # owner fires, sibling folds
        return self.monitor._pair_runtime[self.owner]

    def test_setup_produces_one_owner_and_one_held_pair(self):
        rt = self.start_recording()
        self.assertEqual(len(self.triggers()), 1)
        self.assertEqual(rt.held_pair_keys, [self.held])
        self.assertEqual(
            self.monitor._pair_runtime[self.held].held_by_pair_key, self.owner
        )

    def test_stop_waits_while_a_held_pair_still_disagrees(self):
        rt = self.start_recording()
        # The owner's own detectors agree again (2 comes ON), but 46 does not.
        self.monitor.on_detector_on("2")
        for _ in range(3):
            self.evaluate_all()
        self.assertIsNone(rt.resolution_start_time)
        self.assertIsNotNone(rt.active_trigger_id)
        self.assertEqual([t["action"] for t in self.triggers()], ["start"])

    def test_stop_fires_once_every_participant_agrees(self):
        rt = self.start_recording()
        self.monitor.on_detector_on("2")
        self.evaluate_all()
        self.monitor.on_detector_on("46")         # the last disagreement ends
        self.evaluate_all()                       # starts the post-roll (0 s)
        self.evaluate_all()                       # sends the stop
        actions = [t["action"] for t in self.triggers()]
        self.assertEqual(actions, ["start", "stop"])
        self.assertIsNone(rt.active_trigger_id)
        # One clip, opened and closed by the same trigger ID.
        self.assertEqual(
            self.triggers()[0]["trigger_id"], self.triggers()[1]["trigger_id"]
        )

    def test_a_held_pair_rediverging_resets_the_post_roll(self):
        self.monitor._post_roll_sec = 5.0
        rt = self.start_recording()
        self.monitor.on_detector_on("2")
        self.monitor.on_detector_on("46")
        self.evaluate_all()                       # countdown starts
        self.assertIsNotNone(rt.resolution_start_time)
        self.monitor.on_detector_off("46")        # held pair diverges again
        self.evaluate_all()
        self.assertIsNone(rt.resolution_start_time)

    def test_a_held_pair_runs_no_rules_while_held(self):
        self.start_recording()
        held_rt = self.monitor._pair_runtime[self.held]
        # Prove guard 0 does the work, not the cooldown: the callback path can
        # clear a cooldown early, and the pair must still stay quiet.
        held_rt.cooldown_active = False
        for _ in range(3):
            time.sleep(self.THRESHOLD + 0.05)
            self.evaluate_all()
        self.assertEqual(len(self.triggers()), 1)
        self.assertIsNone(held_rt.disagreement_start)

    def test_release_puts_held_pairs_into_a_fresh_cooldown(self):
        rt = self.start_recording()
        self.monitor.on_detector_on("2")
        self.monitor.on_detector_on("46")
        self.evaluate_all()
        self.evaluate_all()                       # stop goes out
        held_rt = self.monitor._pair_runtime[self.held]
        self.assertEqual(rt.held_pair_keys, [])
        self.assertIsNone(held_rt.held_by_pair_key)
        # Released into cooldown, not straight back into service — otherwise
        # it re-fires on the tail of the footage just recorded.
        self.assertTrue(held_rt.cooldown_active)
        self.assertGreater(held_rt.triggered_at, 0.0)

    def test_an_abandoned_recording_releases_its_held_pairs(self):
        self.start_recording()
        # Detectors vanish under an active recording (the reload case).
        del self.monitor._detector_states["2"]
        self.evaluate_all()
        held_rt = self.monitor._pair_runtime[self.held]
        self.assertIsNone(held_rt.held_by_pair_key)
        self.assertEqual(
            self.monitor._pair_runtime[self.owner].held_pair_keys, []
        )

    def test_a_held_pair_whose_detectors_vanish_stops_holding(self):
        rt = self.start_recording()
        # 46 disappears; the held pair can never resolve, and leaving it in
        # the list would hold the clip open to the max_duration_sec cap.
        del self.monitor._detector_states["46"]
        self.monitor.on_detector_on("2")
        self.evaluate_all()
        self.evaluate_all()
        self.assertEqual(rt.held_pair_keys, [])
        self.assertEqual([t["action"] for t in self.triggers()],
                         ["start", "stop"])

    def test_the_clip_covers_the_later_disagreement(self):
        # The property the whole mechanism exists for, stated in time: the
        # stop timestamp is after the moment the *held* pair resolved, not the
        # owner's.
        rt = self.start_recording()
        self.monitor.on_detector_on("2")
        self.evaluate_all()
        time.sleep(0.15)
        self.monitor.on_detector_on("46")
        held_resolved_at = time.time()
        self.evaluate_all()
        self.evaluate_all()
        stop = [t for t in self.triggers() if t["action"] == "stop"][0]
        self.assertGreaterEqual(stop["event_timestamp"], held_resolved_at)


if __name__ == "__main__":
    unittest.main()
