"""Unit tests for the live video overlay's shape loader and status resolution
(ROADMAP Item 11a).

Both modules under test are deliberately dependency-free — no Flask, no
OpenCV, no ``atspm``, no monitor imports — so this suite runs on a bare stdlib
Python with nothing installed. Test CSVs are generated into a temporary
directory rather than committed as fixtures, so the suite is hermetic; the one
test that reads the owner's real calibration file skips itself when that file
is absent.

Run from anywhere:

    python3 ntcip_monitor/tests/test_overlay_shapes.py
"""

from __future__ import annotations

import logging
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ntcip_monitor.ui.overlay.shapes import (  # noqa: E402
    DEFAULT_COLOR,
    MAX_MONITORED_OVERLAP,
    OVERLAP_LETTER_MAP,
    ShapeConfig,
    resolve_stopbar_target,
)
from ntcip_monitor.ui.overlay.status import (  # noqa: E402
    STATUS_LOOP_OFF,
    STATUS_LOOP_ON,
    STATUS_NA,
    resolve_all,
    resolve_shape_status,
)

SHAPES_LOGGER = "ntcip_monitor.ui.overlay.shapes"

REAL_CSV = Path("/home/hansrkid/vid_cfg720.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _CsvTestCase(unittest.TestCase):
    """Base class giving each test a scratch directory to author CSVs in."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_dir = Path(self._tmp.name)

    def write_csv(self, text, name="shapes.csv"):
        """Write *text* to a file in the scratch dir and return its path.

        Args:
            text: Full CSV contents. Leading newline and indentation are
                stripped so tests can use triple-quoted literals.
            name: File name within the scratch directory.

        Returns:
            The path written.
        """
        path = self.tmp_dir / name
        cleaned = "\n".join(line.strip() for line in text.strip().splitlines())
        path.write_text(cleaned + "\n", newline="")
        return path


# The same three shapes, authored in each of the two supported layouts.
TWO_SECTION_CSV = """
video_width,video_height
720,720
type,points,color,input,phase,name
loop,"100,100;200,100;200,200;100,200","0,255,0",38,,
stopbar,"470,537;529,511","0,0,0",,4,
stopbar,"188,466;192,522","0,0,0",,OLB,
"""

LEGACY_CSV = """
type,points,color,input,phase,direction,video_width,video_height
loop,"100,100;200,100;200,200;100,200","0,255,0",38,,,720,720
stopbar,"470,537;529,511","0,0,0",,4,NB,720,720
stopbar,"188,466;192,522","0,0,0",,OLB,EB,720,720
"""


# ---------------------------------------------------------------------------
# CSV layout handling
# ---------------------------------------------------------------------------

class TestFormatEquivalence(_CsvTestCase):
    """Both CSV layouts must produce byte-identical shape lists."""

    def test_identical_shapes_and_metadata(self):
        two = ShapeConfig.load(self.write_csv(TWO_SECTION_CSV, "two.csv"))
        legacy = ShapeConfig.load(self.write_csv(LEGACY_CSV, "legacy.csv"))

        self.assertEqual((two.video_width, two.video_height), (720, 720))
        self.assertEqual((legacy.video_width, legacy.video_height), (720, 720))
        self.assertEqual(two.shapes, legacy.shapes)
        self.assertEqual(len(two.shapes), 3)

    def test_parsed_fields(self):
        config = ShapeConfig.load(self.write_csv(LEGACY_CSV))
        loop, stopbar, overlap_bar = config.shapes

        self.assertEqual(loop["type"], "loop")
        self.assertEqual(loop["points"],
                         [(100, 100), (200, 100), (200, 200), (100, 200)])
        self.assertEqual(loop["color"], (0, 255, 0))
        self.assertEqual(loop["input"], 38)
        self.assertIsNone(loop["phase"])

        self.assertEqual(stopbar["points"], [(470, 537), (529, 511)])
        self.assertIsNone(stopbar["input"])
        self.assertEqual(stopbar["phase"], "4")
        self.assertEqual(overlap_bar["phase"], "OLB")


class TestLegacySpecifics(_CsvTestCase):
    """Legacy-layout quirks: no name column, a direction column, per-row size."""

    def test_direction_ignored_and_name_is_none(self):
        config = ShapeConfig.load(self.write_csv(LEGACY_CSV))
        for shape in config.shapes:
            self.assertIsNone(shape["name"])
            self.assertNotIn("direction", shape)

    def test_resolution_read_from_first_data_row(self):
        config = ShapeConfig.load(self.write_csv("""
            type,points,color,input,phase,direction,video_width,video_height
            loop,"1,1;2,2","0,255,0",5,,,1920,1080
        """))
        self.assertEqual((config.video_width, config.video_height), (1920, 1080))

    def test_two_section_name_column_preserved(self):
        config = ShapeConfig.load(self.write_csv("""
            video_width,video_height
            720,720
            type,points,color,input,phase,name
            loop,"1,1;2,2","0,255,0",5,,South Loop 5
        """))
        self.assertEqual(config.shapes[0]["name"], "South Loop 5")

    def test_empty_file_is_empty_config(self):
        config = ShapeConfig.load(self.write_csv("type,points,color,input,phase,direction,video_width,video_height"))
        self.assertEqual(config.shapes, [])
        self.assertIsNone(config.video_width)

    def test_missing_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            ShapeConfig.load(self.tmp_dir / "nope.csv")


# ---------------------------------------------------------------------------
# Malformed rows: skipped, never fatal
# ---------------------------------------------------------------------------

class TestMalformedRows(_CsvTestCase):
    """A broken row must not take down the whole overlay."""

    BAD_CSV = """
    type,points,color,input,phase,direction,video_width,video_height
    loop,"100,100;200,100","0,255,0",38,,,720,720
    loop,"429,57x;466,568","0,255,0",39,,,720,720
    loop,"1,1;2,2","0,255",40,,,720,720
    loop,"1,1;2,2","0,255,0",4x,,,720,720
    loop,"300,300;400,400","0,255,0",41,,,720,720
    """

    def test_bad_rows_skipped_good_rows_survive(self):
        path = self.write_csv(self.BAD_CSV)
        with self.assertLogs(SHAPES_LOGGER, level="WARNING"):
            config = ShapeConfig.load(path)

        self.assertEqual([s["input"] for s in config.shapes], [38, 41])
        self.assertEqual((config.video_width, config.video_height), (720, 720))

    def test_one_warning_naming_the_right_lines(self):
        path = self.write_csv(self.BAD_CSV)
        with self.assertLogs(SHAPES_LOGGER, level="WARNING") as captured:
            ShapeConfig.load(path)

        self.assertEqual(len(captured.records), 1)
        record = captured.records[0]
        self.assertEqual(record.event, "overlay_shapes_rows_skipped")
        self.assertEqual(record.skipped_count, 3)
        # Header is line 1, so the three bad rows are lines 3, 4, and 5.
        self.assertEqual([r["line"] for r in record.skipped_rows], [3, 4, 5])

    def test_two_section_skip_reports_absolute_line_numbers(self):
        path = self.write_csv("""
            video_width,video_height
            720,720
            type,points,color,input,phase,name
            loop,"1,1;2,2","0,255,0",1,,
            loop,"bad","0,255,0",2,,
        """)
        with self.assertLogs(SHAPES_LOGGER, level="WARNING") as captured:
            config = ShapeConfig.load(path)

        self.assertEqual(len(config.shapes), 1)
        self.assertEqual(captured.records[0].skipped_rows[0]["line"], 5)

    def test_empty_color_defaults(self):
        config = ShapeConfig.load(self.write_csv("""
            type,points,color,input,phase,direction,video_width,video_height
            loop,"1,1;2,2",,7,,,720,720
        """))
        self.assertEqual(config.shapes[0]["color"], DEFAULT_COLOR)

    def test_blank_lines_are_not_reported_as_errors(self):
        path = self.write_csv("""
            type,points,color,input,phase,direction,video_width,video_height
            loop,"1,1;2,2","0,255,0",7,,,720,720

            loop,"3,3;4,4","0,255,0",8,,,720,720
        """)
        logging.getLogger(SHAPES_LOGGER).addHandler(logging.NullHandler())
        config = ShapeConfig.load(path)
        self.assertEqual(len(config.shapes), 2)

    def test_missing_type_is_skipped(self):
        path = self.write_csv("""
            type,points,color,input,phase,direction,video_width,video_height
            ,"1,1;2,2","0,255,0",7,,,720,720
        """)
        with self.assertLogs(SHAPES_LOGGER, level="WARNING"):
            config = ShapeConfig.load(path)
        self.assertEqual(config.shapes, [])


# ---------------------------------------------------------------------------
# Overlap / phase resolution
# ---------------------------------------------------------------------------

class TestResolveStopbarTarget(unittest.TestCase):
    """The vendored letter/number convention (A=1 ... P=16)."""

    def test_overlap_letters(self):
        self.assertEqual(resolve_stopbar_target("OLA"), ("overlap", 1))
        self.assertEqual(resolve_stopbar_target("OLB"), ("overlap", 2))
        self.assertEqual(resolve_stopbar_target("OLP"), ("overlap", 16))
        self.assertEqual(len(OVERLAP_LETTER_MAP), 16)

    def test_case_and_whitespace_tolerance(self):
        self.assertEqual(resolve_stopbar_target("  olc "), ("overlap", 3))
        self.assertEqual(resolve_stopbar_target(" 7 "), ("phase", 7))

    def test_phase_numbers_int_and_str(self):
        self.assertEqual(resolve_stopbar_target(7), ("phase", 7))
        self.assertEqual(resolve_stopbar_target("7"), ("phase", 7))
        self.assertEqual(resolve_stopbar_target(1), ("phase", 1))
        self.assertEqual(resolve_stopbar_target(16), ("phase", 16))

    def test_out_of_range_phase_raises(self):
        for bad in (0, 17, -1):
            with self.assertRaises(ValueError):
                resolve_stopbar_target(bad)

    def test_unknown_code_raises(self):
        for bad in ("OLQ", "OLZ", "green", ""):
            with self.assertRaises(ValueError):
                resolve_stopbar_target(bad)


class TestUnmonitoredOverlapWarning(_CsvTestCase):
    """Overlaps above what PhaseMonitor polls get named once at load."""

    def test_warns_once_naming_offending_shapes(self):
        path = self.write_csv("""
            video_width,video_height
            720,720
            type,points,color,input,phase,name
            stopbar,"1,1;2,2","0,0,0",,OLB,In range
            stopbar,"3,3;4,4","0,0,0",,OLI,Too high
            stopbar,"5,5;6,6","0,0,0",,OLP,Way too high
        """)
        with self.assertLogs(SHAPES_LOGGER, level="WARNING") as captured:
            config = ShapeConfig.load(path)

        self.assertEqual(len(config.shapes), 3)
        self.assertEqual(len(captured.records), 1)
        record = captured.records[0]
        self.assertEqual(record.event, "overlay_shapes_unmonitored_overlaps")
        self.assertEqual(record.max_monitored_overlap, MAX_MONITORED_OVERLAP)
        self.assertEqual([s["phase"] for s in record.shapes], ["OLI", "OLP"])
        self.assertEqual([s["name"] for s in record.shapes], ["Too high", "Way too high"])
        self.assertEqual([s["index"] for s in record.shapes], [1, 2])

    def test_no_warning_for_in_range_overlaps(self):
        path = self.write_csv("""
            type,points,color,input,phase,direction,video_width,video_height
            stopbar,"1,1;2,2","0,0,0",,OLB,,720,720
            stopbar,"3,3;4,4","0,0,0",,OLF,,720,720
            stopbar,"5,5;6,6","0,0,0",,8,,720,720
            loop,"7,7;8,8","0,255,0",17,,,720,720
        """)
        # White-box on purpose: assertNoLogs needs Python 3.10+, and this suite
        # must run on whatever stdlib the edge box ships.
        records = []

        class _Capture(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = _Capture()
        log = logging.getLogger(SHAPES_LOGGER)
        log.addHandler(handler)
        try:
            ShapeConfig.load(path)
        finally:
            log.removeHandler(handler)

        self.assertEqual(records, [])


# ---------------------------------------------------------------------------
# Status resolution
# ---------------------------------------------------------------------------

LOOP = {"type": "loop", "points": [], "color": (0, 255, 0),
        "input": 38, "phase": None, "name": None}
BAR_PHASE = {"type": "stopbar", "points": [], "color": (0, 0, 0),
             "input": None, "phase": "4", "name": None}
BAR_OVERLAP = {"type": "stopbar", "points": [], "color": (0, 0, 0),
               "input": None, "phase": "OLB", "name": None}


class TestLoopStatus(unittest.TestCase):
    """Loops are on only when the detector reports ACTIVE."""

    def test_active_is_on(self):
        self.assertEqual(
            resolve_shape_status(LOOP, detectors={38: "ACTIVE"}), STATUS_LOOP_ON)

    def test_inactive_is_off(self):
        self.assertEqual(
            resolve_shape_status(LOOP, detectors={38: "INACTIVE"}), STATUS_LOOP_OFF)

    def test_missing_detector_is_off(self):
        self.assertEqual(resolve_shape_status(LOOP, detectors={39: "ACTIVE"}),
                         STATUS_LOOP_OFF)
        self.assertEqual(resolve_shape_status(LOOP, detectors={}), STATUS_LOOP_OFF)
        self.assertEqual(resolve_shape_status(LOOP), STATUS_LOOP_OFF)

    def test_string_keys_from_jsonify(self):
        self.assertEqual(
            resolve_shape_status(LOOP, detectors={"38": "ACTIVE"}), STATUS_LOOP_ON)

    def test_loop_without_input_is_off(self):
        shape = dict(LOOP, input=None)
        self.assertEqual(resolve_shape_status(shape, detectors={38: "ACTIVE"}),
                         STATUS_LOOP_OFF)


class TestStopbarStatus(unittest.TestCase):
    """Stopbars map SignalState names onto G / Y / R, everything else to na."""

    def test_phase_states(self):
        for state, expected in (("GREEN", "G"), ("YELLOW", "Y"), ("RED", "R")):
            self.assertEqual(
                resolve_shape_status(BAR_PHASE, phases={4: state}), expected)

    def test_dark_phase_is_na(self):
        self.assertEqual(resolve_shape_status(BAR_PHASE, phases={4: "DARK"}), STATUS_NA)

    def test_missing_phase_is_na(self):
        self.assertEqual(resolve_shape_status(BAR_PHASE, phases={2: "GREEN"}), STATUS_NA)
        self.assertEqual(resolve_shape_status(BAR_PHASE), STATUS_NA)

    def test_overlap_states_read_the_overlap_dict(self):
        self.assertEqual(
            resolve_shape_status(BAR_OVERLAP, overlaps={2: "GREEN"}), "G")
        # Overlap 2 must not be satisfied by phase 2.
        self.assertEqual(
            resolve_shape_status(BAR_OVERLAP, phases={2: "GREEN"}), STATUS_NA)

    def test_string_keys_from_jsonify(self):
        self.assertEqual(resolve_shape_status(BAR_PHASE, phases={"4": "RED"}), "R")
        self.assertEqual(resolve_shape_status(BAR_OVERLAP, overlaps={"2": "RED"}), "R")

    def test_unmonitored_overlap_is_na_not_an_error(self):
        shape = dict(BAR_OVERLAP, phase="OLI")
        self.assertEqual(resolve_shape_status(shape, overlaps={9: "GREEN"}), "G")
        self.assertEqual(resolve_shape_status(shape, overlaps={}), STATUS_NA)

    def test_unparseable_phase_field_is_na(self):
        for bad in ("OLQ", "green", "99"):
            shape = dict(BAR_PHASE, phase=bad)
            self.assertEqual(resolve_shape_status(shape, phases={4: "GREEN"}), STATUS_NA)

    def test_stopbar_without_phase_is_na(self):
        shape = dict(BAR_PHASE, phase=None)
        self.assertEqual(resolve_shape_status(shape, phases={4: "GREEN"}), STATUS_NA)

    def test_unknown_shape_type_is_na(self):
        shape = dict(BAR_PHASE, type="polygon")
        self.assertEqual(resolve_shape_status(shape, phases={4: "GREEN"}), STATUS_NA)


class TestResolveAll(_CsvTestCase):
    """The state list is always positionally parallel to config.shapes."""

    def _config(self):
        return ShapeConfig.load(self.write_csv(LEGACY_CSV))

    def test_parallel_to_shapes(self):
        config = self._config()
        payload = {
            "phases": {4: "GREEN"},
            "overlaps": {2: "YELLOW"},
            "detectors": {38: "ACTIVE"},
        }
        self.assertEqual(resolve_all(config, payload), [STATUS_LOOP_ON, "G", "Y"])

    def test_empty_payload_still_parallel(self):
        config = self._config()
        for payload in (None, {}, {"phases": {}, "overlaps": {}, "detectors": {}}):
            statuses = resolve_all(config, payload)
            self.assertEqual(len(statuses), len(config.shapes))
            self.assertEqual(statuses, [STATUS_LOOP_OFF, STATUS_NA, STATUS_NA])

    def test_jsonified_payload(self):
        config = self._config()
        payload = {
            "phases": {"4": "RED"},
            "overlaps": {"2": "RED"},
            "detectors": {"38": "INACTIVE"},
        }
        self.assertEqual(resolve_all(config, payload), [STATUS_LOOP_OFF, "R", "R"])

    def test_empty_config(self):
        self.assertEqual(resolve_all(ShapeConfig()), [])


# ---------------------------------------------------------------------------
# The real calibration file
# ---------------------------------------------------------------------------

@unittest.skipUnless(REAL_CSV.exists(), f"{REAL_CSV} not present on this machine")
class TestRealCalibrationFile(unittest.TestCase):
    """Pins the owner's real (legacy-format) 720x720 calibration."""

    def test_metadata_and_shape_count(self):
        config = ShapeConfig.load(REAL_CSV)
        self.assertEqual((config.video_width, config.video_height), (720, 720))
        self.assertEqual(len(config.shapes), 37)
        self.assertEqual(sum(1 for s in config.shapes if s["type"] == "loop"), 28)
        self.assertEqual(sum(1 for s in config.shapes if s["type"] == "stopbar"), 9)

    def test_colors_are_kept_in_authored_bgr_order(self):
        # pyatspm writes OpenCV BGR: "255,0,0" is blue, not red. The loader
        # must not reverse it — that is the renderer's job (ROADMAP 11b).
        config = ShapeConfig.load(REAL_CSV)
        loops = [s for s in config.shapes if s["type"] == "loop"]
        self.assertEqual(loops[0]["color"], (255, 0, 0))
        self.assertIn((0, 0, 255), [s["color"] for s in loops])

    def test_detector_inputs_and_overlaps_resolve(self):
        config = ShapeConfig.load(REAL_CSV)
        inputs = {s["input"] for s in config.shapes if s["type"] == "loop"}
        self.assertTrue({17, 24, 26, 33, 38, 46}.issubset(inputs))

        overlaps = set()
        for shape in config.shapes:
            if shape["type"] == "stopbar" and shape["phase"]:
                kind, num = resolve_stopbar_target(shape["phase"])
                if kind == "overlap":
                    overlaps.add(num)
        self.assertEqual(overlaps, {2, 3, 4, 6})  # OLB, OLC, OLD, OLF
        self.assertTrue(all(n <= MAX_MONITORED_OVERLAP for n in overlaps))


if __name__ == "__main__":
    unittest.main()
