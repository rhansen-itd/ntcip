"""Unit tests for the dashboard/overlay SSE plumbing (ROADMAP Item 15b).

``ntcip_monitor/ui/events.py`` is deliberately stdlib-only — no Flask, no
monitor imports — so this suite runs on a bare interpreter, same rule as
``test_overlay_shapes.py``. The monitors are stubbed with a minimal emitter
that mirrors ``core/event_monitor.py``'s contract (callbacks invoked outside
the emitter's lock), which is enough to pin every behavior that lives here:
coalescing, overflow recovery, close/wake, and the attach-once fan-out.

The Flask routes in ``web_ui.py`` that consume this are still uncovered —
they need a Flask test client, which is ROADMAP 4e's work.

Run from anywhere:

    python3 ntcip_monitor/tests/test_ui_events.py
"""

from __future__ import annotations

import enum
import json
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ntcip_monitor.ui.events import (  # noqa: E402
    DEFAULT_QUEUE_MAXSIZE,
    KEEPALIVE_FRAME,
    MONITOR_BINDINGS,
    StateBroadcaster,
    StateSubscriber,
    format_sse,
)


class FakeState(enum.Enum):
    """Stand-in for ``DetectorState`` / ``SignalState``."""

    ACTIVE = 2
    INACTIVE = 1
    GREEN = 3


class FakeMonitor:
    """Minimal ``EventEmitter`` stand-in: register callbacks, emit to them."""

    def __init__(self):
        self.callbacks = {}

    def on(self, event_name, callback):
        self.callbacks.setdefault(event_name, []).append(callback)

    def off(self, event_name, callback):
        self.callbacks.get(event_name, []).remove(callback)

    def emit(self, event_name, *args):
        for callback in list(self.callbacks.get(event_name, [])):
            callback(*args)


class FakeApp:
    """Stand-in for ``NTCIPMonitorApp``'s monitor attributes."""

    def __init__(self, phase=True, detector=True, output=True):
        self.phase_monitor = FakeMonitor() if phase else None
        self.detector_monitor = FakeMonitor() if detector else None
        self.output_monitor = FakeMonitor() if output else None


# ============================================================================
# format_sse
# ============================================================================

class TestFormatSSE(unittest.TestCase):

    def test_frame_ends_with_a_blank_line(self):
        frame = format_sse({'a': 1})
        self.assertTrue(frame.endswith('\n\n'))

    def test_data_line_round_trips_as_json(self):
        payload = {'type': 'delta', 'detectors': {'26': 'ACTIVE'}}
        frame = format_sse(payload)
        body = frame[len('data: '):].strip()
        self.assertEqual(json.loads(body), payload)

    def test_default_frame_has_no_event_line(self):
        self.assertFalse(format_sse({'a': 1}).startswith('event:'))

    def test_named_event_is_prefixed(self):
        frame = format_sse({'a': 1}, event='status')
        self.assertTrue(frame.startswith('event: status\ndata: '))

    def test_payload_is_compact(self):
        # No stray spaces: these frames go out on every detector edge.
        self.assertNotIn(', ', format_sse({'a': 1, 'b': 2}))

    def test_keepalive_is_a_comment_frame(self):
        self.assertTrue(KEEPALIVE_FRAME.startswith(':'))
        self.assertTrue(KEEPALIVE_FRAME.endswith('\n\n'))


# ============================================================================
# StateSubscriber
# ============================================================================

class TestStateSubscriber(unittest.TestCase):

    def test_wait_returns_none_on_timeout(self):
        sub = StateSubscriber()
        self.assertIsNone(sub.wait_delta(0.01))

    def test_single_change_becomes_a_one_entry_delta(self):
        sub = StateSubscriber()
        sub.push('detectors', 26, 'ACTIVE')
        self.assertEqual(sub.wait_delta(0.5), {'detectors': {'26': 'ACTIVE'}})

    def test_numbers_are_stringified_like_the_json_payload(self):
        sub = StateSubscriber()
        sub.push('phases', 6, 'GREEN')
        delta = sub.wait_delta(0.5)
        self.assertIn('6', delta['phases'])
        self.assertNotIn(6, delta['phases'])

    def test_changes_queued_together_are_coalesced_into_one_delta(self):
        sub = StateSubscriber()
        sub.push('detectors', 26, 'ACTIVE')
        sub.push('detectors', 33, 'ACTIVE')
        sub.push('phases', 2, 'GREEN')
        self.assertEqual(sub.wait_delta(0.5), {
            'detectors': {'26': 'ACTIVE', '33': 'ACTIVE'},
            'phases': {'2': 'GREEN'},
        })

    def test_last_state_for_a_number_wins_within_one_delta(self):
        sub = StateSubscriber()
        sub.push('detectors', 26, 'ACTIVE')
        sub.push('detectors', 26, 'INACTIVE')
        self.assertEqual(sub.wait_delta(0.5), {'detectors': {'26': 'INACTIVE'}})

    def test_queue_drains_fully_so_a_second_wait_times_out(self):
        sub = StateSubscriber()
        sub.push('detectors', 26, 'ACTIVE')
        sub.push('detectors', 27, 'ACTIVE')
        sub.wait_delta(0.5)
        self.assertIsNone(sub.wait_delta(0.01))

    def test_wait_blocks_until_a_change_arrives(self):
        sub = StateSubscriber()

        def produce():
            time.sleep(0.05)
            sub.push('detectors', 1, 'ACTIVE')

        threading.Thread(target=produce, daemon=True).start()
        started = time.monotonic()
        delta = sub.wait_delta(2.0)
        self.assertEqual(delta, {'detectors': {'1': 'ACTIVE'}})
        self.assertLess(time.monotonic() - started, 1.0)

    def test_no_overflow_reported_under_the_bound(self):
        sub = StateSubscriber(maxsize=4)
        for i in range(4):
            sub.push('detectors', i, 'ACTIVE')
        self.assertFalse(sub.take_overflow())

    def test_overflow_is_reported_once_and_then_cleared(self):
        sub = StateSubscriber(maxsize=2)
        for i in range(10):
            sub.push('detectors', i, 'ACTIVE')
        self.assertTrue(sub.take_overflow())
        self.assertFalse(sub.take_overflow())

    def test_overflow_discards_the_stale_backlog(self):
        # The queued items are the *oldest* changes; the caller's response is
        # a full snapshot, so replaying them afterwards would be wrong.
        sub = StateSubscriber(maxsize=2)
        for i in range(10):
            sub.push('detectors', i, 'ACTIVE')
        self.assertTrue(sub.take_overflow())
        self.assertIsNone(sub.wait_delta(0.01))

    def test_push_never_blocks_when_full(self):
        sub = StateSubscriber(maxsize=1)
        started = time.monotonic()
        for i in range(500):
            sub.push('detectors', i, 'ACTIVE')
        self.assertLess(time.monotonic() - started, 1.0)

    def test_close_wakes_a_blocked_wait(self):
        sub = StateSubscriber()

        def closer():
            time.sleep(0.05)
            sub.close()

        threading.Thread(target=closer, daemon=True).start()
        started = time.monotonic()
        self.assertIsNone(sub.wait_delta(5.0))
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertTrue(sub.is_closed())

    def test_close_still_delivers_changes_queued_ahead_of_it(self):
        sub = StateSubscriber()
        sub.push('detectors', 26, 'ACTIVE')
        sub.close()
        self.assertEqual(sub.wait_delta(0.5), {'detectors': {'26': 'ACTIVE'}})

    def test_wait_after_close_returns_none(self):
        sub = StateSubscriber()
        sub.close()
        self.assertIsNone(sub.wait_delta(0.5))

    def test_push_after_close_is_dropped(self):
        sub = StateSubscriber()
        sub.close()
        sub.push('detectors', 26, 'ACTIVE')
        self.assertIsNone(sub.wait_delta(0.01))

    def test_close_is_idempotent(self):
        sub = StateSubscriber()
        sub.close()
        sub.close()
        self.assertTrue(sub.is_closed())

    def test_close_on_a_full_queue_does_not_raise(self):
        sub = StateSubscriber(maxsize=1)
        sub.push('detectors', 1, 'ACTIVE')
        sub.push('detectors', 2, 'ACTIVE')
        sub.close()
        self.assertTrue(sub.is_closed())

    def test_default_bound_is_generous(self):
        self.assertGreaterEqual(DEFAULT_QUEUE_MAXSIZE, 64)


# ============================================================================
# StateBroadcaster
# ============================================================================

class TestStateBroadcaster(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.broadcaster = StateBroadcaster(self.app)

    def test_no_callbacks_are_attached_before_the_first_subscriber(self):
        self.assertEqual(self.app.detector_monitor.callbacks, {})

    def test_first_subscribe_attaches_every_binding(self):
        self.broadcaster.subscribe()
        self.assertTrue(MONITOR_BINDINGS)
        for attr, event, _category in MONITOR_BINDINGS:
            monitor = getattr(self.app, attr)
            self.assertEqual(len(monitor.callbacks.get(event, [])), 1,
                             f'{attr}/{event}')

    def test_second_subscribe_does_not_attach_again(self):
        self.broadcaster.subscribe()
        self.broadcaster.subscribe()
        self.assertEqual(
            len(self.app.detector_monitor.callbacks['detector_change']), 1)

    def test_detector_change_reaches_the_subscriber(self):
        sub = self.broadcaster.subscribe()
        self.app.detector_monitor.emit(
            'detector_change', 26, FakeState.INACTIVE, FakeState.ACTIVE)
        self.assertEqual(sub.wait_delta(0.5), {'detectors': {'26': 'ACTIVE'}})

    def test_every_binding_lands_in_its_own_category(self):
        sub = self.broadcaster.subscribe()
        self.app.phase_monitor.emit('phase_change', 2, None, FakeState.GREEN)
        self.app.phase_monitor.emit('overlap_change', 3, None, FakeState.GREEN)
        self.app.phase_monitor.emit('pedestrian_change', 4, None, FakeState.GREEN)
        self.app.detector_monitor.emit('detector_change', 5, None, FakeState.ACTIVE)
        self.app.output_monitor.emit('output_change', 6, None, FakeState.ACTIVE)
        self.assertEqual(sub.wait_delta(0.5), {
            'phases': {'2': 'GREEN'},
            'overlaps': {'3': 'GREEN'},
            'pedestrians': {'4': 'GREEN'},
            'detectors': {'5': 'ACTIVE'},
            'outputs': {'6': 'ACTIVE'},
        })

    def test_categories_match_the_status_payload_keys(self):
        # A delta is applied by the same client code as a snapshot, so the
        # category names must be exactly _build_status()'s keys.
        categories = {category for _attr, _event, category in MONITOR_BINDINGS}
        self.assertEqual(
            categories,
            {'phases', 'overlaps', 'pedestrians', 'detectors', 'outputs'})

    def test_enum_state_is_reduced_to_its_member_name(self):
        sub = self.broadcaster.subscribe()
        self.app.detector_monitor.emit('detector_change', 1, None, FakeState.ACTIVE)
        self.assertEqual(sub.wait_delta(0.5)['detectors']['1'], 'ACTIVE')

    def test_non_enum_state_falls_back_to_str(self):
        sub = self.broadcaster.subscribe()
        self.app.detector_monitor.emit('detector_change', 1, None, 'RAW')
        self.assertEqual(sub.wait_delta(0.5)['detectors']['1'], 'RAW')

    def test_none_state_falls_back_to_str(self):
        sub = self.broadcaster.subscribe()
        self.app.detector_monitor.emit('detector_change', 1, FakeState.ACTIVE, None)
        self.assertEqual(sub.wait_delta(0.5)['detectors']['1'], 'None')

    def test_every_subscriber_receives_the_same_change(self):
        subs = [self.broadcaster.subscribe() for _ in range(3)]
        self.app.detector_monitor.emit('detector_change', 9, None, FakeState.ACTIVE)
        for sub in subs:
            self.assertEqual(sub.wait_delta(0.5), {'detectors': {'9': 'ACTIVE'}})

    def test_unsubscribed_client_stops_receiving(self):
        sub = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(sub)
        self.app.detector_monitor.emit('detector_change', 9, None, FakeState.ACTIVE)
        self.assertIsNone(sub.wait_delta(0.01))

    def test_unsubscribe_closes_the_subscriber(self):
        sub = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(sub)
        self.assertTrue(sub.is_closed())

    def test_unsubscribe_leaves_the_other_clients_connected(self):
        first = self.broadcaster.subscribe()
        second = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(first)
        self.app.detector_monitor.emit('detector_change', 9, None, FakeState.ACTIVE)
        self.assertEqual(second.wait_delta(0.5), {'detectors': {'9': 'ACTIVE'}})

    def test_callbacks_stay_attached_after_the_last_client_leaves(self):
        # Attach-once is deliberate: a callback with no subscribers costs a
        # lock and an empty list, while attach/detach races do not.
        sub = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(sub)
        self.assertEqual(
            len(self.app.detector_monitor.callbacks['detector_change']), 1)

    def test_a_later_client_still_receives_after_a_full_teardown(self):
        first = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(first)
        second = self.broadcaster.subscribe()
        self.app.detector_monitor.emit('detector_change', 9, None, FakeState.ACTIVE)
        self.assertEqual(second.wait_delta(0.5), {'detectors': {'9': 'ACTIVE'}})

    def test_emitting_with_no_subscribers_does_not_raise(self):
        sub = self.broadcaster.subscribe()
        self.broadcaster.unsubscribe(sub)
        self.app.detector_monitor.emit('detector_change', 9, None, FakeState.ACTIVE)

    def test_unsubscribe_of_an_unknown_subscriber_is_a_no_op(self):
        stranger = StateSubscriber()
        self.broadcaster.unsubscribe(stranger)
        self.assertEqual(self.broadcaster.subscriber_count(), 0)

    def test_subscriber_count_tracks_connections(self):
        self.assertEqual(self.broadcaster.subscriber_count(), 0)
        first = self.broadcaster.subscribe()
        second = self.broadcaster.subscribe()
        self.assertEqual(self.broadcaster.subscriber_count(), 2)
        self.broadcaster.unsubscribe(first)
        self.assertEqual(self.broadcaster.subscriber_count(), 1)
        self.broadcaster.unsubscribe(second)
        self.assertEqual(self.broadcaster.subscriber_count(), 0)

    def test_close_releases_every_client(self):
        subs = [self.broadcaster.subscribe() for _ in range(3)]
        self.broadcaster.close()
        self.assertEqual(self.broadcaster.subscriber_count(), 0)
        for sub in subs:
            self.assertTrue(sub.is_closed())

    def test_missing_monitors_are_skipped(self):
        app = FakeApp(phase=False, output=False)
        broadcaster = StateBroadcaster(app)
        sub = broadcaster.subscribe()
        app.detector_monitor.emit('detector_change', 1, None, FakeState.ACTIVE)
        self.assertEqual(sub.wait_delta(0.5), {'detectors': {'1': 'ACTIVE'}})

    def test_an_app_with_no_monitors_at_all_still_subscribes(self):
        broadcaster = StateBroadcaster(object())
        sub = broadcaster.subscribe()
        self.assertIsNone(sub.wait_delta(0.01))
        broadcaster.unsubscribe(sub)

    def test_a_slow_client_does_not_stall_the_dispatch(self):
        # The producer here stands for a monitor's polling thread: it must
        # return regardless of what any browser is doing.
        slow = self.broadcaster.subscribe(maxsize=2)
        healthy = self.broadcaster.subscribe()
        started = time.monotonic()
        for i in range(200):
            self.app.detector_monitor.emit(
                'detector_change', i, None, FakeState.ACTIVE)
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertTrue(slow.take_overflow())
        self.assertFalse(healthy.take_overflow())

    def test_concurrent_subscribe_attaches_exactly_once(self):
        results = []
        barrier = threading.Barrier(8)

        def join():
            barrier.wait()
            results.append(self.broadcaster.subscribe())

        threads = [threading.Thread(target=join) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 8)
        self.assertEqual(
            len(self.app.detector_monitor.callbacks['detector_change']), 1)
        self.app.detector_monitor.emit('detector_change', 1, None, FakeState.ACTIVE)
        for sub in results:
            self.assertEqual(sub.wait_delta(0.5), {'detectors': {'1': 'ACTIVE'}})


if __name__ == '__main__':
    unittest.main()
