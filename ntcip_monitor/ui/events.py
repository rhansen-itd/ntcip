"""Server-Sent Events plumbing for the dashboard and overlay (ROADMAP 15).

Both pages used to poll (``/api/status``, ``/api/overlay/state``) every
250 ms, so a detector edge reached the browser up to a full poll period after
the SNMP sweep saw it — on top of the ~0.33 s sampling cycle, roughly half a
second of perceived lag.  This module turns that around: the monitors already
emit a change event the instant a sweep detects one, so a browser holding an
``EventSource`` gets the delta pushed at that moment instead.

Design rules, all load-bearing:

* **No Flask and no monitor imports.**  Like ``ui/overlay/*``, this module is
  stdlib-only so its tests run on a bare interpreter.  The event names below
  are string literals rather than imports from ``core.event_monitor``,
  because ``ntcip_monitor.core.__init__`` re-exports ``snmp_client`` and would
  drag pysnmp in — the same reason ``overlay/status.py`` compares state
  *names* instead of importing the enums.  Keep them in sync with
  ``core/event_monitor.py``, which is canonical.
* **A monitor callback must return in microseconds** (see CLAUDE.md's NTCIP
  rules).  ``StateBroadcaster._dispatch`` therefore does nothing but read the
  enum's name and call :meth:`StateSubscriber.push`, which is one
  non-blocking ``Queue.put_nowait``.  Every derived payload — the full status
  snapshot, the overlay's shape resolution — is built later, on the HTTP
  worker thread that serves the stream.
* **A slow client must never stall a sweep.**  Each subscriber owns a bounded
  queue; when it fills, the push is dropped and an overflow flag is raised.
  The stream then discards the backlog and re-sends a full snapshot, so the
  client resynchronises rather than replaying a stale delta trail.
* **Subscriptions attach once and are never detached.**  Ref-counting the
  monitor callbacks (as ``overlay/source.py`` does for its decoder) would buy
  nothing here: an attached callback with no subscribers costs one lock and
  an empty list, whereas an attach/detach race could silently leave the
  stream deaf.  The decoder ref-counts because an idle RTSP session is a real
  resource; a no-op callback is not.
"""

from __future__ import annotations

import json
import queue
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple

#: Changes buffered per connected client before pushes start being dropped.
#: At ~0.33 s per sweep this is minutes of ordinary traffic; reaching it means
#: the client is not reading, in which case a snapshot is the right recovery.
DEFAULT_QUEUE_MAXSIZE = 256

#: Seconds a stream waits for a change before emitting a keepalive comment.
#: The comment is what makes a dead connection observable: the write fails,
#: the generator is closed, and its ``finally`` unsubscribes.
KEEPALIVE_SEC = 15.0

#: SSE comment frame.  Comments are ignored by ``EventSource`` but still have
#: to cross the socket, which is the point.
KEEPALIVE_FRAME = ": keepalive\n\n"

#: ``(NTCIPMonitorApp attribute, event name, status category)``.
#:
#: The event names mirror ``core/event_monitor.py``'s ``EVENT_*`` constants
#: and the categories mirror ``WebUI._build_status()``'s keys — a delta is
#: shaped exactly like the subset of a status payload it replaces, so the
#: dashboard applies both through the same code path.
MONITOR_BINDINGS: Tuple[Tuple[str, str, str], ...] = (
    ('phase_monitor', 'phase_change', 'phases'),
    ('phase_monitor', 'overlap_change', 'overlaps'),
    ('phase_monitor', 'pedestrian_change', 'pedestrians'),
    ('detector_monitor', 'detector_change', 'detectors'),
    ('output_monitor', 'output_change', 'outputs'),
)

#: Sentinel enqueued by :meth:`StateSubscriber.close` to wake a blocked wait.
_CLOSED = object()


def format_sse(payload: Any, event: Optional[str] = None) -> str:
    """Render one payload as an SSE frame.

    Args:
        payload: JSON-serialisable object to send as the frame's data.
        event: Optional SSE event name.  Omitted for the default ``message``
            event, which is what ``EventSource.onmessage`` receives.

    Returns:
        str: The complete frame, terminated by the blank line SSE requires.
    """
    body = json.dumps(payload, separators=(',', ':'))
    prefix = f"event: {event}\n" if event else ""
    return f"{prefix}data: {body}\n\n"


class StateSubscriber:
    """One connected client's queue of state changes.

    Producers (monitor polling threads) call :meth:`push`; the consumer (the
    HTTP worker serving the stream) calls :meth:`wait_delta` and
    :meth:`take_overflow`.  Nothing here blocks a producer.
    """

    def __init__(self, maxsize: int = DEFAULT_QUEUE_MAXSIZE):
        """Initialize the subscriber.

        Args:
            maxsize: Bound on the change queue; see
                :data:`DEFAULT_QUEUE_MAXSIZE`.
        """
        self._queue: "queue.Queue" = queue.Queue(maxsize)
        self._overflowed = False
        self._closed = False

    def push(self, category: str, number: Any, state_name: str) -> None:
        """Record one state change.  Called on a monitor's polling thread.

        Args:
            category: Status category (``'detectors'``, ``'phases'``, ...).
            number: Detector/phase/overlap/output number.
            state_name: The new state's enum member name.
        """
        if self._closed:
            return
        try:
            self._queue.put_nowait((category, number, state_name))
        except queue.Full:
            # Dropping is deliberate: the sweep must not wait on a browser.
            # take_overflow() turns this into a resynchronising snapshot.
            self._overflowed = True

    def wait_delta(self, timeout: float) -> Optional[Dict[str, Dict[str, str]]]:
        """Block for the next change, then coalesce everything queued behind it.

        One sweep can flip several detectors, each arriving as its own event.
        Draining them into a single delta means one frame and one browser
        repaint per sweep rather than one per channel.

        Args:
            timeout: Seconds to wait for the first change.

        Returns:
            dict | None: ``{category: {number: state_name}}``, or ``None`` if
            the wait timed out or the subscriber was closed with nothing
            pending.  Numbers are stringified, matching the JSON keys the
            polled payload produces.
        """
        try:
            # Once closed there is nothing more to wait *for*, but whatever
            # was queued before the close is still real state and is handed
            # over rather than dropped.
            item = self._queue.get(block=not self._closed, timeout=timeout)
        except queue.Empty:
            return None

        delta: Dict[str, Dict[str, str]] = {}
        while item is not None:
            if item is _CLOSED:
                self._closed = True
                break
            category, number, state_name = item
            delta.setdefault(category, {})[str(number)] = state_name
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                item = None

        return delta or None

    def take_overflow(self) -> bool:
        """Report and clear the dropped-changes flag.

        Also discards whatever is still queued: after a drop the backlog is
        the *oldest* changes, and the caller's response is a full snapshot
        that supersedes all of them.

        Returns:
            bool: True if changes were dropped since the last call.
        """
        if not self._overflowed:
            return False
        self._overflowed = False
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        return True

    def close(self) -> None:
        """Close the subscriber and wake any blocked :meth:`wait_delta`."""
        if self._closed:
            return
        try:
            self._queue.put_nowait(_CLOSED)
        except queue.Full:
            pass
        self._closed = True

    def is_closed(self) -> bool:
        """Return True once :meth:`close` has been called."""
        return self._closed


class StateBroadcaster:
    """Fan monitor change events out to every connected SSE client.

    Subscribes to the monitors named in :data:`MONITOR_BINDINGS` on the first
    :meth:`subscribe` call and stays subscribed for the process lifetime — see
    the module docstring for why this deliberately does not ref-count.
    """

    def __init__(self, app_instance: Any):
        """Initialize the broadcaster.

        Args:
            app_instance: The ``NTCIPMonitorApp`` whose monitors to follow.
                Monitors that are absent or ``None`` (a disabled monitor) are
                skipped; the categories they own simply never produce deltas.
        """
        self._app = app_instance
        self._lock = threading.Lock()
        self._subscribers: set = set()
        self._attached = False
        self._bindings: List[Tuple[Any, str, Callable]] = []

    def subscribe(self, maxsize: int = DEFAULT_QUEUE_MAXSIZE) -> StateSubscriber:
        """Register a new client.

        Args:
            maxsize: Queue bound for this client.

        Returns:
            StateSubscriber: The client's queue handle.  Pass it back to
            :meth:`unsubscribe` when the connection ends.
        """
        subscriber = StateSubscriber(maxsize)
        with self._lock:
            self._subscribers.add(subscriber)
        self._attach()
        return subscriber

    def unsubscribe(self, subscriber: StateSubscriber) -> None:
        """Drop a client and wake it if it is blocked.

        Args:
            subscriber: The handle returned by :meth:`subscribe`.
        """
        with self._lock:
            self._subscribers.discard(subscriber)
        subscriber.close()

    def subscriber_count(self) -> int:
        """Return the number of currently connected clients."""
        with self._lock:
            return len(self._subscribers)

    def close(self) -> None:
        """Close every subscriber so their streams can end (used by shutdown)."""
        with self._lock:
            subscribers = list(self._subscribers)
            self._subscribers.clear()
        for subscriber in subscribers:
            subscriber.close()

    def _attach(self) -> None:
        """Subscribe to the monitors, at most once per broadcaster."""
        with self._lock:
            if self._attached:
                return
            self._attached = True

        # Act outside the lock: EventEmitter.on() takes the monitor's own lock
        # (the discipline used throughout this repo — decide under the lock,
        # act after releasing it).
        for attr, event_name, category in MONITOR_BINDINGS:
            monitor = getattr(self._app, attr, None)
            if monitor is None:
                continue
            callback = self._make_callback(category)
            monitor.on(event_name, callback)
            self._bindings.append((monitor, event_name, callback))

    def _make_callback(self, category: str) -> Callable:
        """Build the monitor callback that feeds *category* into the fan-out.

        Args:
            category: Status category this monitor event belongs to.

        Returns:
            Callable: A ``(number, old_state, new_state)`` callback.
        """
        def _on_change(number, old_state, new_state):
            self._dispatch(category, number, new_state)
        return _on_change

    def _dispatch(self, category: str, number: Any, state: Any) -> None:
        """Push one change to every subscriber.

        Runs on a monitor's polling thread, so it stays trivial: read the
        enum's name and enqueue.

        Args:
            category: Status category.
            number: Detector/phase/overlap/output number.
            state: The new state — an enum, or anything with a usable ``str``.
        """
        name = getattr(state, 'name', None) or str(state)
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.push(category, number, name)
