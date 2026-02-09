"""
Event-Driven Monitor Base Class

Provides callback/signal system for external modules to subscribe to events.
"""

import threading
import time
from typing import Callable, Dict, List, Any
from abc import ABC, abstractmethod


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
            try:
                self._poll()
            except Exception as e:
                print(f"Error in {self.name}: {e}")
                # Emit error event
                self.emit('error', e)
            
            time.sleep(self.poll_interval)
    
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


# ============================================================================
# COMMON EVENT NAMES (for consistency across monitors)
# ============================================================================

# Phase events
EVENT_PHASE_CHANGE = 'phase_change'           # (phase_num, old_state, new_state)
EVENT_PHASE_GREEN_START = 'phase_green_start' # (phase_num)
EVENT_PHASE_RED_START = 'phase_red_start'     # (phase_num)
EVENT_PHASE_YELLOW_START = 'phase_yellow_start' # (phase_num)

# Overlap events
EVENT_OVERLAP_CHANGE = 'overlap_change'       # (overlap_num, old_state, new_state)

# Detector events
EVENT_DETECTOR_CHANGE = 'detector_change'     # (detector_num, old_state, new_state)
EVENT_DETECTOR_ON = 'detector_on'             # (detector_num)
EVENT_DETECTOR_OFF = 'detector_off'           # (detector_num)

# Output events
EVENT_OUTPUT_CHANGE = 'output_change'         # (output_num, old_state, new_state)
EVENT_OUTPUT_ON = 'output_on'                 # (output_num)
EVENT_OUTPUT_OFF = 'output_off'               # (output_num)

# System events
EVENT_ERROR = 'error'                         # (exception)
EVENT_CONNECTION_LOST = 'connection_lost'     # ()
EVENT_CONNECTION_RESTORED = 'connection_restored' # ()
