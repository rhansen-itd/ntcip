"""Unit tests for ``video_cleanup`` — duplicate-clip deletion and log rewrite.

Covers the four layers separately:

* the filename parser (a file this module did not write is never a candidate),
* the containment predicate and its tolerance,
* :func:`plan_removals` — including the invariant the whole design rests on,
  that a keeper is never itself deleted,
* :class:`ClipCleaner`'s scan/sweep, with the duration probe stubbed so the
  suite needs neither PyAV nor real video.

Clips are ordinary files with a clip-shaped name whose mtime is set with
``os.utime`` — that plus a stub probe is the whole fixture, because the span
model is exactly "mtime is the end, the container says how long it ran".

Run from anywhere:

    python3 video_engine/tests/test_video_cleanup.py
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts and the other suites).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import video_cleanup  # noqa: E402
from video_cleanup import (  # noqa: E402
    CLEANUP_LOG_NAME,
    ClipCleaner,
    ClipSpan,
    contains,
    parse_clip_name,
    plan_removals,
    rewrite_reference_column,
)

logging.getLogger("video_cleanup").addHandler(logging.NullHandler())

# An arbitrary but fixed "now"; every span in these tests is relative to it.
T0 = 1_785_600_000.0


def span(
    name: str,
    start: float,
    end: float,
    camera_id: str = "fisheye",
    size: int = 1000,
) -> ClipSpan:
    """Build a :class:`ClipSpan` directly, bypassing the filesystem.

    Args:
        name: Clip filename.
        start: Unix start time.
        end: Unix end time.
        camera_id: Owning camera.
        size: File size in bytes.

    Returns:
        The constructed span.
    """
    return ClipSpan(
        path=Path("/clips") / name,
        camera_id=camera_id,
        trigger_prefix=name[:8],
        dispatch_ts=start,
        start_ts=start,
        end_ts=end,
        duration_sec=end - start,
        size_bytes=size,
    )


# ---------------------------------------------------------------------------
# parse_clip_name
# ---------------------------------------------------------------------------

class ParseClipNameTests(unittest.TestCase):
    """The filename is the only thing that marks a file as ours."""

    def test_parses_the_format_the_buffer_writes(self):
        parsed = parse_clip_name("90217f10_fisheye_1785612451.ts")
        self.assertEqual(parsed, ("90217f10", "fisheye", 1785612451.0))

    def test_camera_id_may_contain_underscores(self):
        parsed = parse_clip_name("90217f10_north_east_cam_1785612451.ts")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed[1], "north_east_cam")
        self.assertEqual(parsed[2], 1785612451.0)

    def test_uppercase_hex_prefix_is_accepted(self):
        self.assertIsNotNone(parse_clip_name("90217F10_fisheye_1785612451.ts"))

    def test_other_extension_is_not_a_clip(self):
        self.assertIsNone(parse_clip_name("90217f10_fisheye_1785612451.mp4"))

    def test_honors_a_configured_extension(self):
        self.assertIsNotNone(
            parse_clip_name("90217f10_fisheye_1785612451.mp4", ".mp4")
        )

    def test_logs_and_other_files_are_not_clips(self):
        for name in (
            "discrepancies_log.csv",
            "engine_decisions.csv",
            "video_cleanup_log.csv",
            "notes.ts",
            "manual_export.ts",
            "_fisheye_1785612451.ts",
            "90217f10_fisheye_.ts",
            "90217f10__1785612451.ts",
            "90217g10_fisheye_1785612451.ts",   # 'g' is not hex
            "90217f1_fisheye_1785612451.ts",    # 7-char prefix
            "90217f10_fisheye_notanepoch.ts",
        ):
            with self.subTest(name=name):
                self.assertIsNone(parse_clip_name(name))


# ---------------------------------------------------------------------------
# contains
# ---------------------------------------------------------------------------

class ContainsTests(unittest.TestCase):
    """The containment predicate, and what the tolerance is allowed to do."""

    def test_strict_containment(self):
        outer = span("a.ts", T0, T0 + 40)
        inner = span("b.ts", T0 + 5, T0 + 20)
        self.assertTrue(contains(outer, inner, 0.5))
        self.assertFalse(contains(inner, outer, 0.5))

    def test_a_clip_does_not_contain_itself(self):
        clip = span("a.ts", T0, T0 + 40)
        self.assertFalse(contains(clip, clip, 0.5))

    def test_different_cameras_are_never_compared(self):
        outer = span("a.ts", T0, T0 + 40, camera_id="fisheye")
        inner = span("b.ts", T0 + 5, T0 + 20, camera_id="approach")
        self.assertFalse(contains(outer, inner, 0.5))

    def test_tolerance_absorbs_poll_latency_at_both_ends(self):
        outer = span("a.ts", T0 + 0.3, T0 + 29.8)
        inner = span("b.ts", T0, T0 + 30)
        self.assertFalse(contains(outer, inner, 0.0))
        self.assertTrue(contains(outer, inner, 0.5))

    def test_tolerance_does_not_swallow_a_real_overhang(self):
        outer = span("a.ts", T0, T0 + 30)
        inner = span("b.ts", T0 + 5, T0 + 45)
        self.assertFalse(contains(outer, inner, 0.5))

    def test_partial_overlap_is_not_containment(self):
        first = span("a.ts", T0, T0 + 30)
        second = span("b.ts", T0 + 20, T0 + 50)
        self.assertFalse(contains(first, second, 0.5))
        self.assertFalse(contains(second, first, 0.5))


# ---------------------------------------------------------------------------
# plan_removals
# ---------------------------------------------------------------------------

class PlanRemovalsTests(unittest.TestCase):
    """The planner, and the invariant the log rewrite depends on."""

    def assertKeepersSurvive(self, removals):
        """Assert no clip is both a keeper and a victim (the core invariant)."""
        victims = {r.victim.path for r in removals}
        keepers = {r.keeper.path for r in removals}
        self.assertEqual(victims & keepers, set())

    def test_no_clips(self):
        self.assertEqual(plan_removals([]), [])

    def test_disjoint_clips_are_all_kept(self):
        clips = [
            span("a.ts", T0, T0 + 10),
            span("b.ts", T0 + 20, T0 + 30),
        ]
        self.assertEqual(plan_removals(clips), [])

    def test_a_short_clip_inside_a_long_one_is_removed(self):
        outer = span("a.ts", T0, T0 + 40)
        inner = span("b.ts", T0 + 5, T0 + 20)
        removals = plan_removals([inner, outer])
        self.assertEqual(len(removals), 1)
        self.assertEqual(removals[0].victim.path, inner.path)
        self.assertEqual(removals[0].keeper.path, outer.path)

    def test_near_identical_clips_keep_exactly_one(self):
        first = span("a.ts", T0, T0 + 30.0)
        second = span("b.ts", T0 + 0.2, T0 + 30.1)
        removals = plan_removals([second, first], tolerance_sec=0.5)
        self.assertEqual(len(removals), 1)
        # The earlier-starting clip wins; the ordering makes it deterministic
        # regardless of the input order.
        self.assertEqual(removals[0].keeper.path, first.path)
        self.assertEqual(removals[0].victim.path, second.path)
        self.assertKeepersSurvive(removals)

    def test_exactly_equal_spans_break_the_tie_on_name(self):
        a = span("aaaaaaaa_fisheye_1.ts", T0, T0 + 30)
        b = span("bbbbbbbb_fisheye_1.ts", T0, T0 + 30)
        removals = plan_removals([b, a])
        self.assertEqual(len(removals), 1)
        self.assertEqual(removals[0].keeper.path, a.path)

    def test_a_chain_points_every_victim_at_a_surviving_clip(self):
        outer = span("a.ts", T0, T0 + 60)
        middle = span("b.ts", T0 + 5, T0 + 40)
        inner = span("c.ts", T0 + 10, T0 + 20)
        removals = plan_removals([inner, middle, outer])
        self.assertEqual(len(removals), 2)
        self.assertKeepersSurvive(removals)
        # Both are repointed at the outermost clip, not at each other — a log
        # row must never name a file this same sweep is about to delete.
        self.assertEqual({r.keeper.path for r in removals}, {outer.path})

    def test_cameras_are_planned_independently(self):
        outer = span("a.ts", T0, T0 + 40, camera_id="fisheye")
        other = span("b.ts", T0 + 5, T0 + 20, camera_id="approach")
        self.assertEqual(plan_removals([outer, other]), [])

    def test_a_clip_starting_earlier_is_kept_even_if_nearly_covered(self):
        # Deliberately conservative: 'early' is not contained in 'long' (it
        # starts before it), so both survive.  Keeping an extra file is a cost;
        # losing unique footage is a defect.
        early = span("a.ts", T0, T0 + 20)
        long_clip = span("b.ts", T0 + 2, T0 + 90)
        self.assertEqual(plan_removals([early, long_clip], tolerance_sec=0.5), [])


# ---------------------------------------------------------------------------
# rewrite_reference_column
# ---------------------------------------------------------------------------

class RewriteReferenceColumnTests(unittest.TestCase):
    """The log rewrite: atomic, header-preserving, idempotent."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.csv_path = self.dir / "discrepancies_log.csv"
        self.addCleanup(self._tmp.cleanup)

    def write_log(self, rows, fieldnames=None):
        """Write a discrepancies-log-shaped CSV."""
        fieldnames = fieldnames or [
            "Local_Timestamp", "Trigger_ID", "Video_Filename", "Rule_Type",
            "Det_A", "Det_B", "Description",
        ]
        with self.csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def read_log(self):
        with self.csv_path.open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def row(self, trigger_id, video):
        return {
            "Local_Timestamp": "2026-08-01 13:27:31 MDT",
            "Trigger_ID": trigger_id,
            "Video_Filename": video,
            "Rule_Type": "rule1_continuous_disagreement",
            "Det_A": "31",
            "Det_B": "8",
            "Description": "detector '8' ON, detector '31' OFF for 5.039s",
        }

    def test_missing_file_is_not_an_error(self):
        self.assertEqual(
            rewrite_reference_column(self.dir / "nope.csv", "Video_Filename", {"a": "b"}),
            {},
        )

    def test_empty_mapping_does_nothing(self):
        self.write_log([self.row("t1", "a.ts")])
        self.assertEqual(rewrite_reference_column(self.csv_path, "Video_Filename", {}), {})
        self.assertEqual(self.read_log()[0]["Video_Filename"], "a.ts")

    def test_absent_column_is_left_alone(self):
        self.write_log([], fieldnames=["Local_Timestamp", "Trigger_ID"])
        self.assertEqual(
            rewrite_reference_column(self.csv_path, "Video_Filename", {"a.ts": "b.ts"}),
            {},
        )

    def test_matching_rows_are_repointed_and_counted(self):
        self.write_log([
            self.row("t1", "a.ts"),
            self.row("t2", "b.ts"),
            self.row("t3", "a.ts"),
        ])
        counts = rewrite_reference_column(
            self.csv_path, "Video_Filename", {"a.ts": "keep.ts"}
        )
        self.assertEqual(counts, {"a.ts": 2})
        videos = [r["Video_Filename"] for r in self.read_log()]
        self.assertEqual(videos, ["keep.ts", "b.ts", "keep.ts"])

    def test_header_column_order_and_other_fields_survive(self):
        original = self.row("t1", "a.ts")
        self.write_log([original])
        rewrite_reference_column(self.csv_path, "Video_Filename", {"a.ts": "keep.ts"})
        with self.csv_path.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            self.assertEqual(reader.fieldnames, list(original.keys()))
            row = next(reader)
        self.assertEqual(row["Description"], original["Description"])
        self.assertEqual(row["Trigger_ID"], "t1")

    def test_no_temp_file_is_left_behind(self):
        self.write_log([self.row("t1", "a.ts")])
        rewrite_reference_column(self.csv_path, "Video_Filename", {"a.ts": "keep.ts"})
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()),
                         ["discrepancies_log.csv"])

    def test_rerunning_the_same_rewrite_is_a_no_op(self):
        self.write_log([self.row("t1", "a.ts")])
        mapping = {"a.ts": "keep.ts"}
        rewrite_reference_column(self.csv_path, "Video_Filename", mapping)
        self.assertEqual(
            rewrite_reference_column(self.csv_path, "Video_Filename", mapping), {}
        )


# ---------------------------------------------------------------------------
# ClipCleaner
# ---------------------------------------------------------------------------

class ClipCleanerTestCase(unittest.TestCase):
    """Shared fixture: a temp output_dir plus a stubbed duration probe."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)
        self.durations = {}

    def make_clip(
        self,
        prefix: str,
        camera_id: str,
        start: float,
        end: float,
        size: int = 1024,
        dispatch: float = None,
    ) -> Path:
        """Create a clip-shaped file whose mtime and probed duration give a span.

        Args:
            prefix: 8-hex-char trigger prefix.
            camera_id: Camera field of the filename.
            start: Intended clip start (Unix).
            end: Intended clip end (Unix) — written as the file's mtime.
            size: Bytes of filler.
            dispatch: Epoch encoded in the filename; defaults to ``start``,
                which always satisfies the cross-check.

        Returns:
            The created path.
        """
        stamp = int(start if dispatch is None else dispatch)
        path = self.dir / f"{prefix}_{camera_id}_{stamp}.ts"
        path.write_bytes(b"\0" * size)
        os.utime(path, (end, end))
        self.durations[path.name] = end - start
        return path

    def probe(self, path: Path):
        """Stubbed duration probe — no PyAV, no real video."""
        return self.durations.get(path.name)

    def cleaner(self, **kwargs) -> ClipCleaner:
        """Build a cleaner over the temp dir with the stub probe installed."""
        kwargs.setdefault("tolerance_sec", 0.5)
        kwargs.setdefault("min_age_sec", 0.0)
        return ClipCleaner(
            output_dir=self.dir,
            duration_probe=self.probe,
            **kwargs,
        )

    def write_discrepancy_log(self, rows):
        """Write a discrepancies_log.csv naming the given (trigger, video) pairs."""
        path = self.dir / "discrepancies_log.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=["Local_Timestamp", "Trigger_ID", "Video_Filename",
                            "Rule_Type", "Det_A", "Det_B", "Description"],
            )
            writer.writeheader()
            for trigger_id, video in rows:
                writer.writerow({
                    "Local_Timestamp": "2026-08-01 13:27:31 MDT",
                    "Trigger_ID": trigger_id,
                    "Video_Filename": video,
                    "Rule_Type": "rule1_continuous_disagreement",
                    "Det_A": "31",
                    "Det_B": "8",
                    "Description": "disagreement",
                })
        return path

    def read_csv(self, name):
        with (self.dir / name).open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))


class ClipCleanerScanTests(ClipCleanerTestCase):
    """What ``scan`` will and will not put on the candidate list."""

    def test_a_clip_is_timed_from_its_mtime_and_duration(self):
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30)
        clips, skipped = self.cleaner().scan(now=T0 + 1000)
        self.assertEqual(skipped, 0)
        self.assertEqual(len(clips), 1)
        self.assertAlmostEqual(clips[0].start_ts, T0, places=3)
        self.assertAlmostEqual(clips[0].end_ts, T0 + 30, places=3)
        self.assertEqual(clips[0].camera_id, "fisheye")

    def test_non_clip_files_are_ignored_entirely(self):
        (self.dir / "discrepancies_log.csv").write_text("x")
        (self.dir / "operator_export.ts").write_bytes(b"\0" * 10)
        clips, skipped = self.cleaner().scan(now=T0 + 1000)
        self.assertEqual((clips, skipped), ([], 0))

    def test_a_recently_modified_clip_is_held_back(self):
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30)
        clips, skipped = self.cleaner(min_age_sec=60.0).scan(now=T0 + 40)
        self.assertEqual(clips, [])
        self.assertEqual(skipped, 1)

    def test_a_protected_clip_is_held_back(self):
        path = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30)
        cleaner = ClipCleaner(
            output_dir=self.dir,
            min_age_sec=0.0,
            duration_probe=self.probe,
            protected_paths=lambda: {path},
        )
        clips, skipped = cleaner.scan(now=T0 + 1000)
        self.assertEqual(clips, [])
        self.assertEqual(skipped, 1)

    def test_an_empty_file_is_skipped(self):
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30, size=0)
        clips, skipped = self.cleaner().scan(now=T0 + 1000)
        self.assertEqual((clips, skipped), ([], 1))

    def test_an_unprobeable_clip_is_skipped(self):
        path = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30)
        del self.durations[path.name]
        clips, skipped = self.cleaner().scan(now=T0 + 1000)
        self.assertEqual((clips, skipped), ([], 1))

    def test_a_clip_whose_mtime_contradicts_its_name_is_skipped(self):
        # mtime says the clip ended at T0+30, the filename says it was
        # dispatched an hour later — the file was probably copied without -p.
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 30, dispatch=T0 + 3600)
        with self.assertLogs("video_cleanup", level="WARNING") as cm:
            clips, skipped = self.cleaner().scan(now=T0 + 7200)
        self.assertIn("dispatch cross-check", "".join(cm.output))
        self.assertEqual((clips, skipped), ([], 1))


class ClipCleanerSweepTests(ClipCleanerTestCase):
    """The sweep: dry run, delete, rewrite, audit, and the failure path."""

    def test_dry_run_changes_nothing(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        inner = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20)
        self.write_discrepancy_log([("t1", outer.name), ("t2", inner.name)])

        result = self.cleaner().sweep(apply=False, now=T0 + 1000)

        self.assertFalse(result.applied)
        self.assertEqual(len(result.removals), 1)
        self.assertTrue(inner.exists())
        self.assertEqual(result.rows_updated, 0)
        self.assertFalse((self.dir / CLEANUP_LOG_NAME).exists())
        self.assertEqual(
            [r["Video_Filename"] for r in self.read_csv("discrepancies_log.csv")],
            [outer.name, inner.name],
        )

    def test_apply_deletes_the_victim_and_repoints_its_log_rows(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40, size=4000)
        inner = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20, size=1500)
        self.write_discrepancy_log([("t1", outer.name), ("t2", inner.name)])

        result = self.cleaner().sweep(apply=True, now=T0 + 1000)

        self.assertTrue(outer.exists())
        self.assertFalse(inner.exists())
        self.assertEqual(result.bytes_reclaimed, 1500)
        self.assertEqual(result.rows_updated, 1)
        self.assertEqual(result.errors, 0)
        self.assertEqual(
            [r["Video_Filename"] for r in self.read_csv("discrepancies_log.csv")],
            [outer.name, outer.name],
        )

    def test_the_audit_log_records_both_spans(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        inner = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20, size=1500)
        self.write_discrepancy_log([("t2", inner.name)])

        self.cleaner().sweep(apply=True, now=T0 + 1000)

        rows = self.read_csv(CLEANUP_LOG_NAME)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["deleted_file"], inner.name)
        self.assertEqual(row["kept_file"], outer.name)
        self.assertEqual(row["camera_id"], "fisheye")
        self.assertAlmostEqual(float(row["deleted_start_ts"]), T0 + 5, places=1)
        self.assertAlmostEqual(float(row["kept_end_ts"]), T0 + 40, places=1)
        self.assertEqual(row["bytes_reclaimed"], "1500")
        self.assertEqual(row["log_rows_updated"], "1")

    def test_the_audit_log_is_appended_not_reheadered(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20)
        cleaner = self.cleaner()
        cleaner.sweep(apply=True, now=T0 + 1000)
        self.make_clip("cccccccc", "fisheye", T0 + 6, T0 + 21)
        cleaner.sweep(apply=True, now=T0 + 2000)

        text = (self.dir / CLEANUP_LOG_NAME).read_text()
        self.assertEqual(text.count("deleted_file"), 1)
        rows = self.read_csv(CLEANUP_LOG_NAME)
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["kept_file"] for r in rows}, {outer.name})

    def test_clips_from_different_cameras_are_both_kept(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        other = self.make_clip("bbbbbbbb", "approach", T0 + 5, T0 + 20)
        result = self.cleaner().sweep(apply=True, now=T0 + 1000)
        self.assertEqual(result.removals, [])
        self.assertTrue(outer.exists() and other.exists())

    def test_a_chain_leaves_only_the_outermost_clip(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 60)
        middle = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 40)
        inner = self.make_clip("cccccccc", "fisheye", T0 + 10, T0 + 20)
        self.write_discrepancy_log(
            [("t1", outer.name), ("t2", middle.name), ("t3", inner.name)]
        )

        self.cleaner().sweep(apply=True, now=T0 + 1000)

        self.assertTrue(outer.exists())
        self.assertFalse(middle.exists())
        self.assertFalse(inner.exists())
        # Every surviving row names a file that still exists.
        for row in self.read_csv("discrepancies_log.csv"):
            self.assertTrue((self.dir / row["Video_Filename"]).exists())

    def test_nothing_is_deleted_when_the_log_rewrite_fails(self):
        outer = self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        inner = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20)
        self.write_discrepancy_log([("t2", inner.name)])

        def boom(*args, **kwargs):
            raise OSError("read-only filesystem")

        original = video_cleanup.rewrite_reference_column
        video_cleanup.rewrite_reference_column = boom
        self.addCleanup(setattr, video_cleanup, "rewrite_reference_column", original)

        with self.assertLogs("video_cleanup", level="ERROR"):
            result = self.cleaner().sweep(apply=True, now=T0 + 1000)

        self.assertEqual(result.errors, 1)
        self.assertEqual(result.bytes_reclaimed, 0)
        self.assertTrue(inner.exists())
        self.assertTrue(outer.exists())

    def test_a_missing_discrepancy_log_is_not_an_error(self):
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        inner = self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20)
        result = self.cleaner().sweep(apply=True, now=T0 + 1000)
        self.assertEqual(result.errors, 0)
        self.assertEqual(result.rows_updated, 0)
        self.assertFalse(inner.exists())

    def test_the_sweep_holds_the_shared_csv_lock(self):
        # The buffer appends to discrepancies_log.csv under the same lock; if
        # the sweep did not take it, a rewrite could drop a concurrent row.
        self.make_clip("aaaaaaaa", "fisheye", T0, T0 + 40)
        self.make_clip("bbbbbbbb", "fisheye", T0 + 5, T0 + 20)
        lock = threading.Lock()
        held = []

        class WatchedLock:
            def __enter__(inner_self):
                held.append(lock.acquire())
                return inner_self

            def __exit__(inner_self, *exc):
                lock.release()
                return False

        cleaner = ClipCleaner(
            output_dir=self.dir,
            min_age_sec=0.0,
            duration_probe=self.probe,
            log_lock=WatchedLock(),
        )
        cleaner.sweep(apply=True, now=T0 + 1000)
        self.assertEqual(held, [True])
        self.assertFalse(lock.locked())

    def test_start_and_stop_are_idempotent(self):
        cleaner = self.cleaner(interval_sec=3600.0)
        cleaner.start()
        cleaner.start()
        cleaner.stop()
        cleaner.stop()


if __name__ == "__main__":
    unittest.main()
