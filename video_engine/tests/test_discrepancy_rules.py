"""Unit tests for the discrepancy engine's pure rule functions.

Covers ``_check_rule1_continuous``, ``_check_rule2_orphan`` (interval-based
partner overlap), and ``DiscrepancyMonitor._maybe_register_orphan`` (including
the 2026-07-19 stale-refire guard), plus a small set of integration tests that
drive ``DiscrepancyMonitor._evaluate_pair`` directly (no evaluator thread)
against a stub ``ConfigProvider`` and a temp Hot Folder.

Run from anywhere:

    python3 -m unittest video_engine.tests.test_discrepancy_rules   # if pkg'd
    python3 video_engine/tests/test_discrepancy_rules.py            # direct
"""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_manager import ConfigProvider  # noqa: E402
from discrepancy_engine import (  # noqa: E402
    DiscrepancyMonitor,
    _ORPHAN_DECISION_GRACE_SEC,
    _PairRuntimeState,
    _check_rule1_continuous,
    _check_rule2_orphan,
)


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

    def register(self, is_on=False, last_pulse_on=100.0, last_off=102.0):
        DiscrepancyMonitor._maybe_register_orphan(
            self.rt, "a", is_on, last_pulse_on, last_off, self.THRESHOLD
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


class TestMonitorIntegration(unittest.TestCase):
    """Drives real callbacks + _evaluate_pair with a 0.2 s threshold."""

    THRESHOLD = 0.2

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        cfg = {
            "timezone": "UTC",
            "detectors": {
                "1": {"paired_detector_id": "2", "camera_id": "cam1",
                      "lag_threshold_sec": self.THRESHOLD, "type": "radar"},
                "2": {"paired_detector_id": "1", "camera_id": "cam1",
                      "lag_threshold_sec": self.THRESHOLD, "type": "loop"},
            },
        }
        self.monitor = DiscrepancyMonitor(
            intersection_id="test_int",
            config_provider=_StubProvider(cfg),
            trigger_dir=self._tmp.name,
            cooldown_sec=60.0,
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


if __name__ == "__main__":
    unittest.main()
