"""
Event-Driven Monitor Base Class

Provides callback/signal system for external modules to subscribe to events.
"""

import logging
import threading
import time
from typing import Callable, Dict, List, Any
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)

# Smoothing factor for the effective-cycle EMA.  0.1 ≈ a ~10-cycle memory:
# fast enough to follow a real change in controller responsiveness, slow
# enough that a single retried PDU doesn't move the reported floor.
_CYCLE_EMA_ALPHA = 0.1

# Minimum spacing between "sweep slower than configured" INFO logs, per
# monitor.  The condition is persistent by nature, so this is a heartbeat,
# not an event.
_SLOW_SWEEP_LOG_INTERVAL_SEC = 300.0


class EventEmitter:
    """
    Simple event emitter for pub/sub pattern.
    
    Allows modules to subscribe to specific events and get notified when they occur.
    """
    
    def __init__(self):
        self._callbacks: Dict[str, List[Callable]] = {}
        self._lock = threading.Lock()
    
    def on(self, event_name: str, callback: Callable):
        """
        Subscribe to an event.
        
        Args:
            event_name: Name of event to subscribe to
            callback: Function to call when event occurs
        
        Example:
            monitor.on('phase_change', lambda phase, old_state, new_state: ...)
        """
        with self._lock:
            if event_name not in self._callbacks:
                self._callbacks[event_name] = []
            self._callbacks[event_name].append(callback)
    
    def off(self, event_name: str, callback: Callable):
        """
        Unsubscribe from an event.
        
        Args:
            event_name: Name of event
            callback: Callback function to remove
        """
        with self._lock:
            if event_name in self._callbacks:
                try:
                    self._callbacks[event_name].remove(callback)
                except ValueError:
                    pass
    
    def emit(self, event_name: str, *args, **kwargs):
        """
        Emit an event to all subscribers.
        
        Args:
            event_name: Name of event
            *args, **kwargs: Arguments to pass to callbacks
        """
        with self._lock:
            callbacks = self._callbacks.get(event_name, []).copy()
        
        # Call callbacks outside the lock to avoid deadlocks
        for callback in callbacks:
            try:
                callback(*args, **kwargs)
            except Exception as e:
                print(f"Error in event callback for '{event_name}': {e}")
    
    def clear_all(self):
        """Remove all event subscriptions."""
        with self._lock:
            self._callbacks.clear()


class BaseMonitor(ABC, EventEmitter):
    """
    Abstract base class for all monitors.
    
    Provides:
    - Event emission for state changes
    - Threading support
    - Start/stop lifecycle
    - Periodic polling
    - Self-measurement of the *effective* sampling cycle (see
      ``effective_cycle_sec``)

    Sampling-rate note (ROADMAP 9 / SCOPE_sampling_floor.md)
    ────────────────────────────────────────────────────────
    ``poll_interval`` is only the sleep *between* sweeps.  On an Econolite
    Cobalt at ``chunk_size=1`` a detector sweep costs one SNMP round trip per
    group, so the real sampling cycle is RTT-bound (measured median 1.53 s at
    8 groups on 2026-07-19) and no configured ``poll_interval`` can lower it.
    Downstream consumers must not evaluate evidence finer than this measured
    cycle, so the loop records ``_poll()`` + sleep as an EMA and exposes it.
    """

    def __init__(self, snmp_client, poll_interval=0.25, name="Monitor"):
        """
        Initialize monitor.

        Args:
            snmp_client: EconoliteSNMPClient instance
            poll_interval: Time between polls in seconds
            name: Monitor name for logging
        """
        EventEmitter.__init__(self)

        self.snmp_client = snmp_client
        self.poll_interval = poll_interval
        self.name = name

        self._running = False
        self._thread = None
        self._last_state = None

        # Effective-cycle self-measurement.  Written only by the polling
        # thread; read (float assignment is atomic under the GIL) by any
        # thread via effective_cycle_sec()/get_stats().
        self._effective_cycle_sec = 0.0
        self._cycle_count = 0
        self._last_slow_sweep_log = 0.0

    def start(self):
        """Start the monitor in a background thread."""
        if self._running:
            print(f"{self.name} already running")
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name=self.name)
        self._thread.start()
        print(f"{self.name} started (poll interval: {self.poll_interval}s)")
    
    def stop(self):
        """Stop the monitor."""
        if not self._running:
            return
        
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print(f"{self.name} stopped")
    
    def is_running(self):
        """Check if monitor is running."""
        return self._running
    
    def _run_loop(self):
        """Main monitoring loop (runs in background thread)."""
        while self._running:
            cycle_start = time.perf_counter()
            try:
                self._poll()
            except Exception as e:
                print(f"Error in {self.name}: {e}")
                # Emit error event
                self.emit('error', e)

            time.sleep(self.poll_interval)
            self._record_cycle(time.perf_counter() - cycle_start)

    def _record_cycle(self, elapsed):
        """Fold one completed cycle's wall-clock duration into the EMA.

        A "cycle" is one ``_poll()`` plus the following sleep — i.e. the true
        spacing between consecutive samples of the controller, which is what
        bounds the resolution of anything derived from the emitted events.

        Args:
            elapsed: Wall-clock seconds the cycle took (poll + sleep).
        """
        if self._cycle_count == 0:
            self._effective_cycle_sec = elapsed
        else:
            self._effective_cycle_sec = (
                _CYCLE_EMA_ALPHA * elapsed
                + (1.0 - _CYCLE_EMA_ALPHA) * self._effective_cycle_sec
            )
        self._cycle_count += 1
        self._maybe_log_slow_sweep()

    def _maybe_log_slow_sweep(self):
        """Log (rate-limited) when the sweep, not the config, sets the rate.

        An EMA above ``2 x poll_interval`` means more than half of each cycle
        is spent inside ``_poll()`` — the operator's signal that SNMP round
        trips, not ``poll_interval``, determine the sampling rate.
        """
        if self._effective_cycle_sec <= 2.0 * self.poll_interval:
            return

        now = time.monotonic()
        if now - self._last_slow_sweep_log < _SLOW_SWEEP_LOG_INTERVAL_SEC:
            return
        self._last_slow_sweep_log = now

        log.info(
            "Effective sampling cycle exceeds 2x poll_interval — "
            "sweep time, not config, sets the sampling rate",
            extra={
                "monitor": self.name,
                "poll_interval_sec": self.poll_interval,
                "effective_cycle_sec": round(self._effective_cycle_sec, 4),
                "cycles": self._cycle_count,
            },
        )

    def effective_cycle_sec(self):
        """Get the measured effective sampling cycle in seconds.

        Returns:
            EMA of ``_poll()`` + sleep wall-clock duration, or ``0.0`` before
            the first cycle completes.  Callers must treat ``0.0`` as "not
            measured yet" and fall back to a configured default.
        """
        return self._effective_cycle_sec

    @abstractmethod
    def _poll(self):
        """
        Poll the controller and emit events on changes.

        Must be implemented by subclasses.
        """
        pass

    def get_last_state(self):
        """Get the last known state."""
        return self._last_state

    def get_stats(self):
        """Get monitor runtime statistics.

        Returns:
            Dict with the monitor name, running flag, configured
            ``poll_interval_sec``, the measured ``effective_cycle_sec``, and
            the number of completed cycles.  ``effective_cycle_sec`` is the
            number to trust when reasoning about sampling resolution;
            ``poll_interval_sec`` is only a lower bound on it.
        """
        return {
            'name': self.name,
            'running': self._running,
            'poll_interval_sec': self.poll_interval,
            'effective_cycle_sec': round(self._effective_cycle_sec, 4),
            'cycles': self._cycle_count,
        }


# ============================================================================
# COMMON EVENT NAMES (for consistency across monitors)
# ============================================================================

# Phase events
EVENT_PHASE_CHANGE = 'phase_change'               # (phase_num, old_state, new_state)
EVENT_PHASE_GREEN_START = 'phase_green_start'     # (phase_num)
EVENT_PHASE_RED_START = 'phase_red_start'         # (phase_num)
EVENT_PHASE_YELLOW_START = 'phase_yellow_start'   # (phase_num)

# Pedestrian events
EVENT_PEDESTRIAN_CHANGE = 'pedestrian_change'                    # (ped_num, old_state, new_state)
EVENT_PEDESTRIAN_WALK_START = 'pedestrian_walk_start'            # (ped_num)
EVENT_PEDESTRIAN_CLEARANCE_START = 'pedestrian_clearance_start'  # (ped_num)
EVENT_PEDESTRIAN_DONT_WALK_START = 'pedestrian_dont_walk_start'  # (ped_num)

# Overlap events
EVENT_OVERLAP_CHANGE = 'overlap_change'           # (overlap_num, old_state, new_state)

# Detector events
EVENT_DETECTOR_CHANGE = 'detector_change'         # (detector_num, old_state, new_state)
EVENT_DETECTOR_ON = 'detector_on'                 # (detector_num)
EVENT_DETECTOR_OFF = 'detector_off'               # (detector_num)

# Output events
EVENT_OUTPUT_CHANGE = 'output_change'             # (output_num, old_state, new_state)
EVENT_OUTPUT_ON = 'output_on'                     # (output_num)
EVENT_OUTPUT_OFF = 'output_off'                   # (output_num)

# Ring events
EVENT_RING_STATE_CHANGE = 'ring_state_change'
# Emitted when bits 0-2 (coded timing state) change for a ring.
# Signature: (ring_num: int, old_state: RingState, new_state: RingState)

EVENT_RING_TERMINATION = 'ring_termination'
# Emitted on the 0->1 edge of any termination bit (GapOut, MaxOut, ForceOff).
# Signature: (ring_num: int, reason: TerminationReason)

EVENT_PHASE_TERMINATED = 'phase_terminated'
# Emitted by PhaseMonitor (proxied from RingMonitor) when the ring that owns
# a phase registers a termination while that phase is in Yellow.
# Signature: (phase_num: int, ring_num: int, reason: TerminationReason)

# System events
EVENT_ERROR = 'error'                             # (exception)
EVENT_CONNECTION_LOST = 'connection_lost'         # ()
EVENT_CONNECTION_RESTORED = 'connection_restored' # ()
