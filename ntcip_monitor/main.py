"""
Main Application - Orchestrates all monitors and services
"""

import logging
import time
import sys
import threading
from typing import Optional

from .core.snmp_client import EconoliteSNMPClient, SNMPError
from .core.data_models import TerminationReason
from .core.event_monitor import EVENT_PHASE_TERMINATED, EVENT_PHASE_YELLOW_START
from .core.oid_definitions import get_phase_ring_oid
from .monitors.phase_monitor import PhaseMonitor
from .monitors.detector_monitor import DetectorMonitor
from .monitors.output_monitor import OutputMonitor
from .monitors.ring_monitor import RingMonitor
from .utils.config_loader import ConfigLoader
from .utils.controller_control import ControllerControl

logger = logging.getLogger(__name__)


class NTCIPMonitorApp:
    """
    Main application orchestrator.
    
    Responsibilities:
    - Load and monitor configuration file
    - Initialize SNMP client
    - Start/stop monitors based on config
    - Provide access to monitors for external modules
    """
    
    def __init__(self, config_path='config.json'):
        """
        Initialize application.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_loader = ConfigLoader(config_path)
        self.snmp_client = None
        self.controller = None
        
        # Monitors
        self.phase_monitor: Optional[PhaseMonitor] = None
        self.detector_monitor: Optional[DetectorMonitor] = None
        self.output_monitor: Optional[OutputMonitor] = None
        self.ring_monitor: Optional[RingMonitor] = None

        # Maps phase_num (int) -> ring_num (int), populated at start-up
        self.phase_to_ring_map: dict[int, int] = {}
        
        # Control
        self._running = False
        self._config_check_thread = None
        
        print("NTCIP Monitor Application initialized")
    
    def start(self):
        """Start the application."""
        if self._running:
            print("Application already running")
            return
        
        print("\n" + "="*60)
        print("STARTING NTCIP MONITOR APPLICATION")
        print("="*60)
        
        # Initialize SNMP client
        self._init_snmp_client()
        
#        # Test connection
#        if not self._test_connection():
#            print("ERROR: Cannot connect to controller. Check configuration.")
#            sys.exit(1)
        
        # Initialize controller control
        self.controller = ControllerControl(self.snmp_client)
        
        # Start monitors based on config
        self._start_monitors()
        
        # Start config monitoring thread
        self._running = True
        self._config_check_thread = threading.Thread(
            target=self._config_check_loop,
            daemon=True,
            name="ConfigMonitor"
        )
        self._config_check_thread.start()
        
        print("\n" + "="*60)
        print("APPLICATION STARTED")
        print("="*60)
    
    def stop(self):
        """Stop the application."""
        if not self._running:
            return
        
        print("\nStopping application...")
        
        self._running = False
        
        # Stop all monitors
        if self.phase_monitor:
            self.phase_monitor.stop()
        if self.detector_monitor:
            self.detector_monitor.stop()
        if self.output_monitor:
            self.output_monitor.stop()
        if self.ring_monitor:
            self.ring_monitor.stop()
        
        print("Application stopped")
    
    def _init_snmp_client(self):
        """Initialize SNMP client from configuration."""
        ip = self.config_loader.get('controller.ip', '10.37.2.68')
        port = self.config_loader.get('controller.port', 501)
        community = self.config_loader.get('controller.community', 'administrator')
        timeout = self.config_loader.get('controller.timeout', 2)
        retries = self.config_loader.get('controller.retries', 2)
        # OIDs per PDU; keep the default 1 unless a hardware probe
        # (__probe_snmp_batch.py, ROADMAP 4a) proved this controller accepts
        # multi-OID PDUs.
        chunk_size = self.config_loader.get('controller.chunk_size', 1)

        self.snmp_client = EconoliteSNMPClient(
            ip=ip,
            port=port,
            community=community,
            timeout=timeout,
            retries=retries,
            chunk_size=chunk_size
        )

        print(f"SNMP Client: {ip}:{port} (community: {community}, "
              f"chunk_size: {self.snmp_client.chunk_size})")
    
    def _test_connection(self):
        """Test connection to controller."""
        print("\nTesting connection...")
        
        try:
            if self.snmp_client.test_connection():
                print("✓ Connection successful")
                return True
            else:
                print("✗ Connection failed")
                return False
        except SNMPError as e:
            print(f"✗ Connection error: {e}")
            return False
    
    def _start_monitors(self):
        """Start monitors based on configuration."""
        print("\nStarting monitors...")
        
        # Phase Monitor
        if self.config_loader.get('monitors.phases.enabled', True):
            self.phase_monitor = PhaseMonitor(
                self.snmp_client,
                poll_interval=self.config_loader.get('monitors.phases.poll_interval', 0.25),
                monitor_phases_1_8=self.config_loader.get('monitors.phases.monitor_1_8', True),
                monitor_phases_9_16=self.config_loader.get('monitors.phases.monitor_9_16', False),
                monitor_overlaps=self.config_loader.get('monitors.phases.monitor_overlaps', False),
                monitor_pedestrians=self.config_loader.get('monitors.phases.monitor_pedestrians', False)
            )
            self.phase_monitor.start()
        
        # Detector Monitor
        if self.config_loader.get('monitors.detectors.enabled', False):
            det_range = self.config_loader.get('monitors.detectors.detector_range', [1, 65])
            self.detector_monitor = DetectorMonitor(
                self.snmp_client,
                poll_interval=self.config_loader.get('monitors.detectors.poll_interval', 0.1),
                detector_range=tuple(det_range)
            )
            self.detector_monitor.start()
        
        # Output Monitor
        if self.config_loader.get('monitors.outputs.enabled', False):
            out_range = self.config_loader.get('monitors.outputs.output_range', [1, 17])
            self.output_monitor = OutputMonitor(
                self.snmp_client,
                poll_interval=self.config_loader.get('monitors.outputs.poll_interval', 0.25),
                output_range=tuple(out_range)
            )
            self.output_monitor.start()

        # Ring Monitor (must be started last so phase_monitor is already set)
        self._start_ring_monitor()

    def _start_ring_monitor(self) -> None:
        """Initialise and start the RingMonitor if enabled in config.

        Also calls :meth:`_init_phase_ring_mapping` and wires the
        ``phase_yellow_start`` -> ``phase_terminated`` bridge when both
        the phase monitor and ring monitor are active.
        """
        if not self.config_loader.get('monitors.rings.enabled', False):
            return

        rings_to_monitor = self.config_loader.get(
            'monitors.rings.rings_to_monitor', [1, 2]
        )
        poll_interval = self.config_loader.get(
            'monitors.rings.poll_interval', 0.25
        )

        self.ring_monitor = RingMonitor(
            self.snmp_client,
            poll_interval=poll_interval,
            rings_to_monitor=rings_to_monitor,
        )
        self.ring_monitor.start()

        # Build phase->ring map once the SNMP client is live
        self._init_phase_ring_mapping()

        # Wire the phase-termination bridge only when both monitors are active
        if self.phase_monitor is not None:
            self.phase_monitor.on(
                EVENT_PHASE_YELLOW_START,
                self._handle_phase_yellow_start,
            )
            logger.info(
                '{"event":"bridge_registered",'
                '"listener":"_handle_phase_yellow_start"}'
            )

    def _init_phase_ring_mapping(self) -> None:
        """Query the controller for each phase's ring assignment.

        Iterates phases 1-16 and performs an SNMP GET for ``phaseRing``
        (``PHASE_RING_BASE.<phase_num>``).  On success, stores the result in
        ``self.phase_to_ring_map``.  On any SNMP failure the method falls back
        to the standard NEMA dual-ring layout:

        * Phases  1-4  -> Ring 1
        * Phases  5-8  -> Ring 2
        * Phases  9-12 -> Ring 3
        * Phases 13-16 -> Ring 4

        The fallback is safe for the vast majority of Econolite deployments
        that use the default NEMA configuration.
        """
        fallback_map: dict[int, int] = {
            phase_num: ((phase_num - 1) // 4) + 1
            for phase_num in range(1, 17)
        }

        try:
            live_map: dict[int, int] = {}
            for phase_num in range(1, 17):
                oid = get_phase_ring_oid(phase_num)
                ring_val: int = self.snmp_client.get(oid)
                if 1 <= ring_val <= 4:
                    live_map[phase_num] = ring_val
                else:
                    logger.warning(
                        '{"event":"phase_ring_unexpected_value",'
                        '"phase":%d,"value":%d,"using_fallback":true}',
                        phase_num, ring_val,
                    )
                    live_map[phase_num] = fallback_map[phase_num]

            self.phase_to_ring_map = live_map
            logger.info(
                '{"event":"phase_ring_map_loaded","source":"controller","map":%s}',
                str(live_map),
            )

        except SNMPError as exc:
            self.phase_to_ring_map = fallback_map
            logger.warning(
                '{"event":"phase_ring_map_fallback","reason":"snmp_error",'
                '"error":"%s","map":%s}',
                exc,
                str(fallback_map),
            )

    def _handle_phase_yellow_start(self, phase_num: int) -> None:
        """Bridge a yellow-start event to a phase-terminated event.

        Registered on the PhaseMonitor for ``EVENT_PHASE_YELLOW_START``.
        When fired, immediately consults the RingMonitor for the current
        termination reason on the ring that owns ``phase_num``.

        If a non-NONE termination reason is present, re-emits
        ``EVENT_PHASE_TERMINATED`` **on the PhaseMonitor** so that all
        external subscribers only need to listen to a single event source.

        This method must execute in microseconds – no I/O, no sleeps.

        Args:
            phase_num: The phase number that just transitioned to yellow.
        """
        if self.ring_monitor is None:
            return

        ring_num: Optional[int] = self.phase_to_ring_map.get(phase_num)
        if ring_num is None:
            logger.debug(
                '{"event":"phase_terminated_no_ring","phase":%d}', phase_num
            )
            return

        reason: Optional[TerminationReason] = (
            self.ring_monitor.get_current_termination_reason(ring_num)
        )

        if reason is None or reason == TerminationReason.NONE:
            return

        logger.info(
            '{"event":"phase_terminated","phase":%d,"ring":%d,"reason":"%s"}',
            phase_num, ring_num, reason.name,
        )

        # Emit on phase_monitor so external code subscribes to one place
        self.phase_monitor.emit(
            EVENT_PHASE_TERMINATED, phase_num, ring_num, reason
        )

    def _config_check_loop(self):
        """Background thread to check for config file changes."""
        while self._running:
            try:
                if self.config_loader.check_for_updates():
                    print("Configuration updated! Restarting monitors...")
                    self._restart_monitors()
            except Exception as e:
                print(f"Error checking config: {e}")
            
            time.sleep(5)
    
    def _restart_monitors(self):
        """Restart monitors with new configuration."""
        # Stop existing monitors
        if self.phase_monitor:
            self.phase_monitor.stop()
            self.phase_monitor = None
        if self.detector_monitor:
            self.detector_monitor.stop()
            self.detector_monitor = None
        if self.output_monitor:
            self.output_monitor.stop()
            self.output_monitor = None
        if self.ring_monitor:
            self.ring_monitor.stop()
            self.ring_monitor = None
        self.phase_to_ring_map = {}
        
        # Reinitialize SNMP client (in case connection params changed)
        self._init_snmp_client()
        
        # Restart monitors
        self._start_monitors()
    
    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def get_phase_monitor(self) -> Optional[PhaseMonitor]:
        """Return the PhaseMonitor instance."""
        return self.phase_monitor
    
    def get_detector_monitor(self) -> Optional[DetectorMonitor]:
        """Return the DetectorMonitor instance."""
        return self.detector_monitor
    
    def get_output_monitor(self) -> Optional[OutputMonitor]:
        """Return the OutputMonitor instance."""
        return self.output_monitor

    def get_ring_monitor(self) -> Optional[RingMonitor]:
        """Return the RingMonitor instance.

        Returns:
            The running monitor, or ``None`` if rings are disabled in config.
        """
        return self.ring_monitor
    
    def get_controller(self) -> ControllerControl:
        """Return the ControllerControl instance."""
        return self.controller


# Example: How to use the event system for external modules (e.g., video buffering)
def example_video_buffer_integration(app):
    """
    Example of how to integrate video buffer system with phase changes.
    
    This demonstrates how an external module can subscribe to phase events
    to trigger video saves, including the new phase_terminated event.
    """
    phase_monitor = app.get_phase_monitor()
    
    if phase_monitor is None:
        print("Phase monitor not enabled")
        return
    
    # Subscribe to phase change events
    def on_phase_2_green_to_red(phase_num, old_state, new_state):
        from .core.data_models import SignalState
        
        if phase_num == 2 and old_state == SignalState.GREEN and new_state == SignalState.RED:
            print("Phase 2: GREEN -> RED detected! Trigger video save.")
            # video_buffer.save_last_30_seconds(filename=f"phase2_red_{timestamp}.mp4")
    
    def on_phase_6_yellow_start(phase_num):
        if phase_num == 6:
            print("Phase 6 went YELLOW! Pre-trigger video buffer.")
            # video_buffer.mark_event_start()

    def on_phase_terminated(phase_num, ring_num, reason):
        from .core.data_models import TerminationReason
        print(
            f"Phase {phase_num} (Ring {ring_num}) terminated: {reason.name}"
        )
        # video_buffer.annotate_clip(reason=reason.name)
    
    # Register callbacks
    phase_monitor.on('phase_change', on_phase_2_green_to_red)
    phase_monitor.on('phase_yellow_start', on_phase_6_yellow_start)
    phase_monitor.on('phase_terminated', on_phase_terminated)
    
    print("Video buffer integration callbacks registered")


if __name__ == '__main__':
    # Run standalone
    app = NTCIPMonitorApp()
    
    try:
        app.start()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        app.stop()
