"""
Main Application - Orchestrates all monitors and services
"""

import time
import sys
import threading
from .core.snmp_client import EconoliteSNMPClient, SNMPError
from .monitors.phase_monitor import PhaseMonitor
from .monitors.detector_monitor import DetectorMonitor
from .monitors.output_monitor import OutputMonitor
from .utils.config_loader import ConfigLoader
from .utils.controller_control import ControllerControl


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
        self.phase_monitor = None
        self.detector_monitor = None
        self.output_monitor = None
        
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
        
        print("Application stopped")
    
    def _init_snmp_client(self):
        """Initialize SNMP client from configuration."""
        ip = self.config_loader.get('controller.ip', '10.37.2.68')
        port = self.config_loader.get('controller.port', 501)
        community = self.config_loader.get('controller.community', 'administrator')
        timeout = self.config_loader.get('controller.timeout', 2)
        retries = self.config_loader.get('controller.retries', 2)
        
        self.snmp_client = EconoliteSNMPClient(
            ip=ip,
            port=port,
            community=community,
            timeout=timeout,
            retries=retries
        )
        
        print(f"SNMP Client: {ip}:{port} (community: {community})")
    
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
    
    def _config_check_loop(self):
        """Background thread to check for config file changes."""
        while self._running:
            try:
                if self.config_loader.check_for_updates():
                    print("Configuration updated! Restarting monitors...")
                    self._restart_monitors()
            except Exception as e:
                print(f"Error checking config: {e}")
            
            # Check every 5 seconds
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
        
        # Reinitialize SNMP client (in case connection params changed)
        self._init_snmp_client()
        
        # Restart monitors
        self._start_monitors()
    
    def get_phase_monitor(self):
        """Get phase monitor instance."""
        return self.phase_monitor
    
    def get_detector_monitor(self):
        """Get detector monitor instance."""
        return self.detector_monitor
    
    def get_output_monitor(self):
        """Get output monitor instance."""
        return self.output_monitor
    
    def get_controller(self):
        """Get controller control instance."""
        return self.controller


# Example: How to use the event system for external modules (e.g., video buffering)
def example_video_buffer_integration(app):
    """
    Example of how to integrate video buffer system with phase changes.
    
    This demonstrates how an external module can subscribe to phase events
    to trigger video saves.
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
            # Your video buffer code here:
            # video_buffer.save_last_30_seconds(filename=f"phase2_red_{timestamp}.mp4")
    
    def on_phase_6_yellow_start(phase_num):
        if phase_num == 6:
            print("Phase 6 went YELLOW! Pre-trigger video buffer.")
            # video_buffer.mark_event_start()
    
    # Register callbacks
    phase_monitor.on('phase_change', on_phase_2_green_to_red)
    phase_monitor.on('phase_yellow_start', on_phase_6_yellow_start)
    
    print("Video buffer integration callbacks registered")


if __name__ == '__main__':
    # Run standalone
    app = NTCIPMonitorApp()
    
    try:
        app.start()
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        app.stop()
