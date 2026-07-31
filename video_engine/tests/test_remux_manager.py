"""Unit tests for ``remux_video_buffer.VideoBufferManager``'s bookkeeping.

Covers the ROADMAP Item 8 fixes: the single ``_state_lock`` around
``_active_writers`` / ``_stop_timers`` / ``_draining``, the timer-generation
guard that stops a superseded auto-stop timer from ending a clip it no longer
owns, and the single-camera WARNING.

``ClipRemuxer`` and ``PacketStreamBuffer`` are stubbed — no PyAV containers, no
streams, no disk writes — so these tests exercise start/auto-stop/stop/reap
sequencing from several threads at once, not the remux path itself (that is
covered by ``tools/__replay_verify.py`` against a real capture).

Run from anywhere:

    python3 -m unittest video_engine.tests.test_remux_manager   # if pkg'd
    python3 video_engine/tests/test_remux_manager.py            # direct
"""

from __future__ import annotations

import logging
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts and test_discrepancy_rules.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import remux_video_buffer  # noqa: E402
from remux_video_buffer import VideoBufferConfig, VideoBufferManager  # noqa: E402

# The manager logs a WARNING for multi-camera triggers; without a handler
# logging's lastResort clutters test output.  assertLogs installs its own.
logging.getLogger("remux_video_buffer.manager").addHandler(logging.NullHandler())


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------

class StubRemuxer:
    """Stand-in for ``ClipRemuxer``: a thread that blocks until ``finish()``.

    Mirrors the real writer's lifecycle contract — the thread is alive from
    ``start()`` until the stop sentinel is consumed, and the semaphore is
    released exactly once when it ends — without touching PyAV or disk.
    """

    instances: list = []

    def __init__(
        self,
        trigger_id,
        camera_id,
        output_path,
        event_ts,
        pre_roll_sec,
        semaphore,
        template_provider,
    ):
        self.trigger_id = trigger_id
        self.camera_id = camera_id
        self.output_path = output_path
        self._semaphore = semaphore
        self._finish_evt = threading.Event()
        self.finished = False
        self.joined = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        StubRemuxer.instances.append(self)

    def _run(self):
        self._finish_evt.wait()
        self._semaphore.release()

    def start(self):
        self._thread.start()

    def finish(self):
        self.finished = True
        self._finish_evt.set()

    def join(self, timeout=10.0):
        self.joined += 1
        self._thread.join(timeout=timeout)

    def is_alive(self):
        return self._thread.is_alive()

    def on_packet(self, rec):  # pragma: no cover - never fed in these tests
        pass

    def prime_preroll(self, snapshot):  # pragma: no cover
        pass


class StubStreamBuffer:
    """Stand-in for ``PacketStreamBuffer``: records subscribe/unsubscribe only."""

    def __init__(self, camera_id, url):
        self.camera_id = camera_id
        self.url = url
        self.template = object()
        self.subscribers = []
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def subscribe(self, remuxer):
        self.subscribers.append(remuxer)

    def unsubscribe(self, remuxer):
        if remuxer in self.subscribers:
            self.subscribers.remove(remuxer)


class ManagerTestCase(unittest.TestCase):
    """Builds a manager with stubbed writers/streams over a temp directory."""

    CAMERAS = {"fisheye": "rtsp://stub/1"}

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        StubRemuxer.instances = []
        self._real_remuxer = remux_video_buffer.ClipRemuxer
        remux_video_buffer.ClipRemuxer = StubRemuxer
        self.addCleanup(self._restore)

        root = Path(self._tmp.name)
        self.config = VideoBufferConfig(
            streams=dict(self.CAMERAS),
            trigger_dir=str(root / "triggers"),
            output_dir=str(root / "clips"),
            poll_interval_sec=0.05,
            max_concurrent_writers=2,
            min_free_disk_mb=0.0,
        )
        self.manager = VideoBufferManager(
            self.config, stream_buffer_factory=StubStreamBuffer
        )
        # start() blocks in the poll loop, so wire the stream buffers by hand.
        for cam_id, url in self.config.streams.items():
            buf = StubStreamBuffer(cam_id, url)
            buf.start()
            self.manager._stream_buffers[cam_id] = buf

    def _restore(self):
        remux_video_buffer.ClipRemuxer = self._real_remuxer
        self._tmp.cleanup()

    def start_trigger(self, trigger_id, max_duration_sec=300.0, cameras=None):
        """Dispatch a ``start`` trigger through the manager."""
        self.manager._handle_start(
            {
                "trigger_id": trigger_id,
                "action": "start",
                "event_timestamp": time.time(),
                "cameras": cameras if cameras is not None else ["all"],
                "max_duration_sec": max_duration_sec,
            }
        )


# ---------------------------------------------------------------------------
# Start / stop / reap bookkeeping
# ---------------------------------------------------------------------------

class TestStartStopBookkeeping(ManagerTestCase):

    def test_start_registers_writer_and_timer(self):
        self.start_trigger("t1")
        self.assertIn("t1", self.manager._active_writers)
        self.assertIn("t1", self.manager._stop_timers)
        self.assertEqual(len(StubRemuxer.instances), 1)
        self.assertEqual(
            self.manager._stream_buffers["fisheye"].subscribers,
            StubRemuxer.instances,
        )

    def test_duplicate_start_is_ignored(self):
        self.start_trigger("t1")
        self.start_trigger("t1")
        self.assertEqual(len(StubRemuxer.instances), 1)

    def test_stop_moves_writer_to_draining_and_cancels_timer(self):
        self.start_trigger("t1")
        timer = self.manager._stop_timers["t1"][1]
        remuxer = StubRemuxer.instances[0]

        self.manager._stop_trigger("t1")

        self.assertNotIn("t1", self.manager._active_writers)
        self.assertNotIn("t1", self.manager._stop_timers)
        self.assertIn(remuxer, self.manager._draining)
        self.assertTrue(remuxer.finished)
        self.assertEqual(self.manager._stream_buffers["fisheye"].subscribers, [])
        self.assertTrue(timer.finished.is_set())  # cancelled

    def test_stop_unknown_trigger_is_a_noop(self):
        self.manager._stop_trigger("nope")
        self.assertEqual(self.manager._draining, [])

    def test_reap_joins_only_dead_writers(self):
        self.start_trigger("t1")
        remuxer = StubRemuxer.instances[0]
        self.manager._stop_trigger("t1")

        # Still finalizing (thread alive) — not reaped.
        remuxer._finish_evt.clear()
        self.manager._reap_finished()

        remuxer.finish()
        remuxer.join()
        self.manager._reap_finished()
        self.assertEqual(self.manager._draining, [])
        self.assertGreaterEqual(remuxer.joined, 1)

    def test_semaphore_is_released_exactly_once_per_clip(self):
        for i in range(4):
            tid = f"t{i}"
            self.start_trigger(tid)
            self.manager._stop_trigger(tid)
            StubRemuxer.instances[-1].join()
            self.manager._reap_finished()
        # Cap is 2; every clip must have handed its slot back.
        self.assertTrue(self.manager._writer_semaphore.acquire(blocking=False))
        self.assertTrue(self.manager._writer_semaphore.acquire(blocking=False))
        self.assertFalse(self.manager._writer_semaphore.acquire(blocking=False))

    def test_concurrent_writer_cap_drops_the_third_trigger(self):
        self.start_trigger("t1")
        self.start_trigger("t2")
        with self.assertLogs("remux_video_buffer.manager", level="WARNING") as cm:
            self.start_trigger("t3")
        self.assertNotIn("t3", self.manager._active_writers)
        self.assertIn("Concurrent writer cap reached", "".join(cm.output))


# ---------------------------------------------------------------------------
# Auto-stop timers and the generation guard
# ---------------------------------------------------------------------------

class TestAutoStopTimers(ManagerTestCase):

    def test_short_max_duration_fires_and_stops_the_clip(self):
        # The timer is registered before it is armed, so even a near-zero
        # max_duration cannot fire against missing bookkeeping.
        self.start_trigger("t1", max_duration_sec=0.01)
        remuxer = StubRemuxer.instances[0]
        deadline = time.monotonic() + 3.0
        while remuxer.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertTrue(remuxer.finished)
        self.assertNotIn("t1", self.manager._active_writers)
        self.assertNotIn("t1", self.manager._stop_timers)

    def test_extend_reschedules_and_supersedes_the_old_timer(self):
        self.start_trigger("t1", max_duration_sec=300.0)
        gen1, old_timer = self.manager._stop_timers["t1"]

        self.manager._handle_extend(
            {"trigger_id": "t1", "action": "extend", "max_duration_sec": 300.0}
        )
        gen2, new_timer = self.manager._stop_timers["t1"]

        self.assertNotEqual(gen1, gen2)
        self.assertIsNot(old_timer, new_timer)
        self.assertTrue(old_timer.finished.is_set())  # cancelled
        self.assertIn("t1", self.manager._active_writers)

    def test_extend_on_unknown_trigger_is_a_noop(self):
        self.manager._handle_extend(
            {"trigger_id": "nope", "action": "extend", "max_duration_sec": 10.0}
        )
        self.assertEqual(self.manager._stop_timers, {})

    def test_superseded_timer_callback_does_not_stop_the_clip(self):
        # A timer that fires just as extend() replaces it — its cancel() lost
        # the race — must not end a clip it no longer owns.
        self.start_trigger("t1", max_duration_sec=300.0)
        stale_gen = self.manager._stop_timers["t1"][0]
        self.manager._handle_extend(
            {"trigger_id": "t1", "action": "extend", "max_duration_sec": 300.0}
        )

        self.manager._auto_stop("t1", stale_gen)

        self.assertIn("t1", self.manager._active_writers)
        self.assertFalse(StubRemuxer.instances[0].finished)

    def test_current_timer_callback_stops_the_clip(self):
        self.start_trigger("t1", max_duration_sec=300.0)
        gen = self.manager._stop_timers["t1"][0]
        self.manager._auto_stop("t1", gen)
        self.assertNotIn("t1", self.manager._active_writers)
        self.assertTrue(StubRemuxer.instances[0].finished)

    def test_auto_stop_after_manual_stop_is_a_noop(self):
        self.start_trigger("t1", max_duration_sec=300.0)
        gen = self.manager._stop_timers["t1"][0]
        self.manager._stop_trigger("t1")
        remuxer = StubRemuxer.instances[0]

        self.manager._auto_stop("t1", gen)  # stale: bookkeeping already gone

        # Exactly one writer in _draining, not two entries for the same clip.
        self.assertEqual(self.manager._draining, [remuxer])


# ---------------------------------------------------------------------------
# Concurrency — the actual Item 8 hazard
# ---------------------------------------------------------------------------

class TestConcurrentBookkeeping(ManagerTestCase):
    """Drive start/stop/reap from several threads and assert nothing is lost.

    Before the lock, ``_draining`` was iterated-and-removed by ``_reap_finished``
    while Timer threads appended to it (``RuntimeError`` / dropped writers), and
    the pop-timer/pop-writer/append-draining sequence could interleave.
    """

    def test_stop_and_reap_race_leaves_no_orphaned_writers(self):
        stop_flag = threading.Event()
        errors = []

        def reaper():
            try:
                while not stop_flag.is_set():
                    self.manager._reap_finished()
            except Exception as exc:  # noqa: BLE001 - surfaced as a failure
                errors.append(exc)

        reap_thread = threading.Thread(target=reaper, daemon=True)
        reap_thread.start()

        def stopper(tid):
            try:
                self.manager._stop_trigger(tid)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        for round_idx in range(30):
            tids = [f"r{round_idx}_a", f"r{round_idx}_b"]
            for tid in tids:
                self.start_trigger(tid)
            # Two threads stopping two clips while the reaper walks _draining.
            threads = [
                threading.Thread(target=stopper, args=(tid,)) for tid in tids
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            for rem in StubRemuxer.instances[-2:]:
                rem.join()

        stop_flag.set()
        reap_thread.join(timeout=5.0)
        self.manager._reap_finished()

        self.assertEqual(errors, [])
        self.assertEqual(self.manager._active_writers, {})
        self.assertEqual(self.manager._stop_timers, {})
        self.assertEqual(self.manager._draining, [])
        self.assertEqual(len(StubRemuxer.instances), 60)
        self.assertTrue(all(r.finished for r in StubRemuxer.instances))

    def test_two_threads_stopping_the_same_trigger_finalize_once(self):
        for i in range(20):
            tid = f"t{i}"
            self.start_trigger(tid)
            barrier = threading.Barrier(2)

            def racer():
                barrier.wait()
                self.manager._stop_trigger(tid)

            threads = [threading.Thread(target=racer) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            remuxer = StubRemuxer.instances[-1]
            # The writer is tracked exactly once, never twice.
            self.assertEqual(self.manager._draining.count(remuxer), 1)
            remuxer.join()
            self.manager._reap_finished()

    def test_stop_joins_writers_appended_during_shutdown(self):
        self.start_trigger("t1", max_duration_sec=300.0)
        self.start_trigger("t2", max_duration_sec=300.0)
        remuxers = list(StubRemuxer.instances)

        self.manager.stop()

        self.assertFalse(self.manager._running)
        self.assertEqual(self.manager._active_writers, {})
        self.assertEqual(self.manager._stop_timers, {})
        self.assertEqual(self.manager._draining, [])
        for rem in remuxers:
            self.assertTrue(rem.finished)
            self.assertGreaterEqual(rem.joined, 1)
            self.assertFalse(rem.is_alive())
        for buf in self.manager._stream_buffers.values():
            self.assertTrue(buf.stopped)

    def test_reap_parked_in_join_is_not_clobbered_by_stop(self):
        """Regression for the pre-Item-8 ``_draining`` hazard.

        The old ``_reap_finished`` walked a *snapshot* of ``_draining``, joined,
        and only then removed — while ``stop()`` joined its own snapshot and
        ``clear()``-ed the list.  Parking the reaper inside ``join()`` and
        running ``stop()`` meanwhile made the reaper's later ``remove()`` raise
        ``ValueError`` and double-joined the writer.  With selection+removal
        done together under the lock, ``stop()`` simply finds nothing to drain.
        """

        class GatedRemuxer(StubRemuxer):
            entered = threading.Event()
            gate = threading.Event()

            def join(self, timeout=10.0):
                GatedRemuxer.entered.set()
                GatedRemuxer.gate.wait(timeout=5.0)
                super().join(timeout=timeout)

        remux_video_buffer.ClipRemuxer = GatedRemuxer
        self.start_trigger("t1")
        self.manager._stop_trigger("t1")
        remuxer = StubRemuxer.instances[-1]
        deadline = time.monotonic() + 3.0
        while remuxer.is_alive() and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertFalse(remuxer.is_alive())

        errors = []

        def reaper():
            try:
                self.manager._reap_finished()
            except Exception as exc:  # noqa: BLE001 - surfaced as a failure
                errors.append(exc)

        reap_thread = threading.Thread(target=reaper, daemon=True)
        reap_thread.start()
        self.assertTrue(GatedRemuxer.entered.wait(timeout=3.0))

        # Release the gate slightly late so a stop() that also blocks on the
        # same join (the old behaviour) can't deadlock the test.
        releaser = threading.Timer(0.3, GatedRemuxer.gate.set)
        releaser.daemon = True
        releaser.start()

        self.manager.stop()
        GatedRemuxer.gate.set()
        reap_thread.join(timeout=5.0)

        self.assertEqual(errors, [])
        self.assertEqual(self.manager._draining, [])
        self.assertEqual(remuxer.joined, 1)

    def test_stop_is_safe_with_a_timer_firing_concurrently(self):
        # A ~immediate auto-stop timer races shutdown; both paths mutate the
        # same three fields.  Nothing may be left unjoined.
        self.start_trigger("t1", max_duration_sec=0.02)
        self.start_trigger("t2", max_duration_sec=0.02)
        remuxers = list(StubRemuxer.instances)
        self.manager.stop()
        for rem in remuxers:
            rem.join()
        self.manager._reap_finished()
        self.assertEqual(self.manager._draining, [])
        self.assertTrue(all(r.finished for r in remuxers))


# ---------------------------------------------------------------------------
# Single-camera assumption
# ---------------------------------------------------------------------------

class TestSingleCameraAssumption(ManagerTestCase):
    CAMERAS = {"fisheye": "rtsp://stub/1", "approach": "rtsp://stub/2"}

    def test_multi_camera_trigger_warns_and_records_the_first(self):
        with self.assertLogs("remux_video_buffer.manager", level="WARNING") as cm:
            self.start_trigger("t1", cameras=["fisheye", "approach"])
        self.assertIn("recording only the first", "".join(cm.output))
        self.assertEqual(len(StubRemuxer.instances), 1)
        self.assertEqual(StubRemuxer.instances[0].camera_id, "fisheye")

    def test_all_at_a_multi_camera_intersection_warns(self):
        with self.assertLogs("remux_video_buffer.manager", level="WARNING") as cm:
            self.start_trigger("t1", cameras=["all"])
        record = cm.records[0]
        self.assertEqual(len(record.cameras_recorded), 1)
        self.assertEqual(len(record.cameras_requested), 2)

    def test_single_camera_trigger_does_not_warn(self):
        with self.assertLogs("remux_video_buffer.manager", level="INFO") as cm:
            self.start_trigger("t1", cameras=["approach"])
        self.assertNotIn("recording only the first", "".join(cm.output))
        self.assertEqual(StubRemuxer.instances[0].camera_id, "approach")

    def test_unknown_camera_names_are_dropped(self):
        with self.assertLogs("remux_video_buffer.manager", level="WARNING") as cm:
            self.start_trigger("t1", cameras=["ghost"])
        self.assertIn("No matching cameras", "".join(cm.output))
        self.assertEqual(StubRemuxer.instances, [])


if __name__ == "__main__":
    unittest.main()
