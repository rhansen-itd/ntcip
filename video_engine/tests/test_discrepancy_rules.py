"""Unit tests for the discrepancy engine's pure rule functions.

Covers ``_check_rule1_continuous``, ``_check_rule2_orphan`` (interval-based
partner overlap), ``_compute_on_duty_fraction``, and
``DiscrepancyMonitor._maybe_register_orphan`` (including the 2026-07-19
stale-refire guard and the 2026-07-30 sampling-floor gate), plus a set of
integration tests that drive ``DiscrepancyMonitor._evaluate_pair`` directly
(no evaluator thread) against a stub ``ConfigProvider`` and a temp Hot Folder,
the 2026-08-01 decision log (``engine_decisions.csv``), and ``_resolve_pytz``
(ROADMAP 4d).

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
    _DEFAULT_SAMPLING_FLOOR_SEC,
    _DUTY_WINDOW_SEC,
    _ORPHAN_DECISION_GRACE_SEC,
    _PairRuntimeState,
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
        DiscrepancyMonitor._maybe_register_orphan(
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
    trigger_dir, threshold=0.2, floor=None, decision_log=None, **extra_cfg
):
    """Build a two-detector DiscrepancyMonitor over a temp Hot Folder.

    Args:
        trigger_dir: Directory the monitor writes trigger files into.
        threshold: ``lag_threshold_sec`` for both detectors.
        floor: When given, injected via ``set_sampling_floor`` the way
            ``system_runner`` does in production.
        decision_log: When given, the engine's decision-log CSV path.
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


if __name__ == "__main__":
    unittest.main()
