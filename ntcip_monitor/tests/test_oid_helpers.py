"""Unit tests for the pure OID/state helpers (ROADMAP 4d).

Covers ``oid_definitions.get_phase_oids`` / ``get_detector_oid`` /
``get_output_oid`` and ``data_models.parse_signal_state`` — all deterministic
and dependency-free, so no mocking is involved.

``ntcip_monitor/core`` is put on ``sys.path`` and the two modules imported
directly rather than through ``ntcip_monitor.core``: the package ``__init__``
re-exports ``snmp_client``, which needs pysnmp.  Importing the leaves keeps
this suite runnable on a bare interpreter (same reasoning as the overlay
tests).

Run from anywhere:

    python3 ntcip_monitor/tests/test_oid_helpers.py
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core"))

import oid_definitions as oids  # noqa: E402
from data_models import SignalState, parse_signal_state  # noqa: E402


# ---------------------------------------------------------------------------
# get_phase_oids
# ---------------------------------------------------------------------------

class TestGetPhaseOids(unittest.TestCase):
    """Group number → (reds, yellows, greens) triple."""

    def test_group_1_is_phases_1_8(self):
        self.assertEqual(
            oids.get_phase_oids(1),
            (oids.PHASE_1_8_REDS, oids.PHASE_1_8_YELLOWS, oids.PHASE_1_8_GREENS),
        )

    def test_group_2_is_phases_9_16(self):
        self.assertEqual(
            oids.get_phase_oids(2),
            (oids.PHASE_9_16_REDS, oids.PHASE_9_16_YELLOWS, oids.PHASE_9_16_GREENS),
        )

    def test_group_3_is_overlaps(self):
        self.assertEqual(
            oids.get_phase_oids(3),
            (oids.OVERLAP_REDS, oids.OVERLAP_YELLOWS, oids.OVERLAP_GREENS),
        )

    def test_defaults_to_group_1(self):
        self.assertEqual(oids.get_phase_oids(), oids.get_phase_oids(1))

    def test_unknown_group_falls_back_to_group_1(self):
        # Documented ``.get(group, oids[1])`` behavior: no raise, no None.
        for bad in (0, 4, -1, None, "1", 1.5):
            with self.subTest(group=bad):
                self.assertEqual(oids.get_phase_oids(bad), oids.get_phase_oids(1))

    def test_always_returns_three_distinct_oids(self):
        for group in (1, 2, 3):
            with self.subTest(group=group):
                triple = oids.get_phase_oids(group)
                self.assertIsInstance(triple, tuple)
                self.assertEqual(len(triple), 3)
                self.assertEqual(len(set(triple)), 3)

    def test_groups_do_not_overlap_each_other(self):
        every = [o for g in (1, 2, 3) for o in oids.get_phase_oids(g)]
        self.assertEqual(len(set(every)), 9)

    def test_colour_column_ordering(self):
        # NTCIP column 2 = reds, 3 = yellows, 4 = greens.  The tuple order is
        # what callers unpack positionally, so pin it against the columns.
        for group in (1, 2, 3):
            with self.subTest(group=group):
                red, yellow, green = oids.get_phase_oids(group)
                self.assertEqual(red.split(".")[-2], "2")
                self.assertEqual(yellow.split(".")[-2], "3")
                self.assertEqual(green.split(".")[-2], "4")

    def test_all_oids_live_under_the_ntcip_base(self):
        for group in (1, 2, 3):
            for oid in oids.get_phase_oids(group):
                with self.subTest(oid=oid):
                    self.assertTrue(oid.startswith(oids.NTCIP_BASE + "."))


# ---------------------------------------------------------------------------
# get_detector_oid
# ---------------------------------------------------------------------------

class TestGetDetectorOid(unittest.TestCase):
    """Detector number (1-64) → (group OID, bit position 0-7)."""

    def test_first_detector_is_bit_zero_of_the_first_group(self):
        self.assertEqual(oids.get_detector_oid(1), (oids.DETECTOR_1_8, 0))

    def test_last_detector_of_a_group_is_bit_seven(self):
        self.assertEqual(oids.get_detector_oid(8), (oids.DETECTOR_1_8, 7))

    def test_group_rolls_over_at_nine(self):
        self.assertEqual(oids.get_detector_oid(9), (oids.DETECTOR_9_16, 0))

    def test_last_detector_overall(self):
        self.assertEqual(oids.get_detector_oid(64), (oids.DETECTOR_57_64, 7))

    def test_intersection_201_channels_land_where_expected(self):
        # Spot-check the real channels from intersections.json — the mapping
        # verified against controller high-res data (see CLAUDE.md).
        self.assertEqual(oids.get_detector_oid(26), (oids.DETECTOR_25_32, 1))
        self.assertEqual(oids.get_detector_oid(33), (oids.DETECTOR_33_40, 0))
        self.assertEqual(oids.get_detector_oid(52), (oids.DETECTOR_49_56, 3))

    def test_every_detector_maps_uniquely(self):
        seen = {oids.get_detector_oid(n) for n in range(1, 65)}
        self.assertEqual(len(seen), 64)

    def test_group_and_bit_arithmetic_holds_across_the_range(self):
        for n in range(1, 65):
            with self.subTest(detector=n):
                oid, bit = oids.get_detector_oid(n)
                self.assertEqual(oid, oids.DETECTOR_GROUPS[(n - 1) // 8])
                self.assertEqual(bit, (n - 1) % 8)
                self.assertIn(bit, range(8))

    def test_below_range_raises(self):
        for bad in (0, -1):
            with self.subTest(detector=bad):
                with self.assertRaises(ValueError):
                    oids.get_detector_oid(bad)

    def test_above_range_raises(self):
        for bad in (65, 100):
            with self.subTest(detector=bad):
                with self.assertRaises(ValueError):
                    oids.get_detector_oid(bad)


# ---------------------------------------------------------------------------
# get_output_oid
# ---------------------------------------------------------------------------

class TestGetOutputOid(unittest.TestCase):
    """Output number (1-16) → one OID string.

    These pin the OID the code *currently* emits, not the one the controller
    accepts: ROADMAP 10 records that ``OUTPUT_BASE`` returns ``noSuchName`` on
    the Cobalt at 10.37.23.200.  When that item corrects the column, these
    expectations move with it — they are not evidence the OID is right.
    """

    def test_first_output(self):
        self.assertEqual(oids.get_output_oid(1), f"{oids.OUTPUT_BASE}.1")

    def test_last_output(self):
        self.assertEqual(oids.get_output_oid(16), f"{oids.OUTPUT_BASE}.16")

    def test_indexing_is_one_based_across_the_range(self):
        for n in range(1, 17):
            with self.subTest(output=n):
                self.assertEqual(oids.get_output_oid(n), f"{oids.OUTPUT_BASE}.{n}")

    def test_every_output_maps_uniquely(self):
        self.assertEqual(len({oids.get_output_oid(n) for n in range(1, 17)}), 16)

    def test_matches_the_precomputed_list(self):
        self.assertEqual(
            [oids.get_output_oid(n) for n in range(1, 17)], oids.OUTPUT_OIDS
        )

    def test_below_range_raises(self):
        for bad in (0, -1):
            with self.subTest(output=bad):
                with self.assertRaises(ValueError):
                    oids.get_output_oid(bad)

    def test_above_range_raises(self):
        for bad in (17, 64):
            with self.subTest(output=bad):
                with self.assertRaises(ValueError):
                    oids.get_output_oid(bad)


# ---------------------------------------------------------------------------
# parse_signal_state
# ---------------------------------------------------------------------------

class TestParseSignalState(unittest.TestCase):
    """Red/yellow/green bits → :class:`SignalState`.

    The function is a priority ladder, not a validator: it never raises, and
    an impossible bit combination resolves to the highest-priority colour set
    rather than being rejected.
    """

    def test_green_only(self):
        self.assertIs(parse_signal_state(0, 0, 1), SignalState.GREEN)

    def test_yellow_only(self):
        self.assertIs(parse_signal_state(0, 1, 0), SignalState.YELLOW)

    def test_red_only(self):
        self.assertIs(parse_signal_state(1, 0, 0), SignalState.RED)

    def test_no_bits_set_is_dark(self):
        self.assertIs(parse_signal_state(0, 0, 0), SignalState.DARK)

    def test_green_outranks_yellow_and_red(self):
        self.assertIs(parse_signal_state(1, 1, 1), SignalState.GREEN)
        self.assertIs(parse_signal_state(0, 1, 1), SignalState.GREEN)
        self.assertIs(parse_signal_state(1, 0, 1), SignalState.GREEN)

    def test_yellow_outranks_red(self):
        self.assertIs(parse_signal_state(1, 1, 0), SignalState.YELLOW)

    def test_bits_are_read_for_truthiness_not_equality(self):
        # Callers pass masked bits, which can be any non-zero int (e.g. a
        # bitmask AND that yields 4), and the web UI passes booleans.
        self.assertIs(parse_signal_state(0, 0, 4), SignalState.GREEN)
        self.assertIs(parse_signal_state(0, 2, 0), SignalState.YELLOW)
        self.assertIs(parse_signal_state(True, False, False), SignalState.RED)
        self.assertIs(parse_signal_state(False, False, False), SignalState.DARK)

    def test_covers_every_bit_combination(self):
        expected = {
            (0, 0, 0): SignalState.DARK,
            (1, 0, 0): SignalState.RED,
            (0, 1, 0): SignalState.YELLOW,
            (1, 1, 0): SignalState.YELLOW,
            (0, 0, 1): SignalState.GREEN,
            (1, 0, 1): SignalState.GREEN,
            (0, 1, 1): SignalState.GREEN,
            (1, 1, 1): SignalState.GREEN,
        }
        for (r, y, g), want in expected.items():
            with self.subTest(red=r, yellow=y, green=g):
                self.assertIs(parse_signal_state(r, y, g), want)


if __name__ == "__main__":
    unittest.main()
