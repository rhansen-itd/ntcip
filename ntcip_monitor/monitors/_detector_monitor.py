"""
Detector Monitor - Monitors vehicle detectors 1-64
"""

from ..core.event_monitor import BaseMonitor, EVENT_DETECTOR_CHANGE, EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF
from ..core.oid_definitions import DETECTOR_GROUPS
from ..core.data_models import parse_detectors_from_bitmask, DetectorState
from ..core.snmp_client import SNMPError


class DetectorMonitor(BaseMonitor):
    """
    Monitor vehicle detectors 1-64.
    
    Emits events:
    - 'detector_change': (detector_num, old_state, new_state)
    - 'detector_on': (detector_num)
    - 'detector_off': (detector_num)
    
    Example usage:
        monitor = DetectorMonitor(snmp_client)
        monitor.on('detector_on', lambda det: print(f"Detector {det} activated"))
        monitor.on('detector_off', lambda det: print(f"Detector {det} deactivated"))
        monitor.start()
    """
    
    def __init__(self, snmp_client, poll_interval=0.1, detector_range=None):
        """
        Initialize detector monitor.
        
        Args:
            snmp_client: EconoliteSNMPClient instance
            poll_interval: Poll interval in seconds (default 0.1 = 10Hz)
            detector_range: Tuple of (start, end) detector numbers to monitor,
                          or None to monitor all 64 (default None)
        """
        super().__init__(snmp_client, poll_interval, name="DetectorMonitor")
        
        if detector_range is None:
            self.detector_range = (1, 65)  # All 64 detectors
        else:
            self.detector_range = detector_range
        
        # Calculate which groups we need to poll
        self._groups_to_poll = self._calculate_groups_to_poll()
        
        # Store last state
        self._last_detectors = {}  # {detector_num: DetectorState}
    
    def _calculate_groups_to_poll(self):
        """Calculate which detector groups (1-8) need to be polled."""
        start, end = self.detector_range
        groups = set()
        
        for det in range(start, end):
            group = (det - 1) // 8  # 0-7
            groups.add(group)
        
        return sorted(groups)
    
    def _poll(self):
        """Poll detectors and emit events on changes."""
        try:
            # Read all required detector groups
            oids_to_read = [DETECTOR_GROUPS[i] for i in self._groups_to_poll]
            bitmasks = self.snmp_client.get(*oids_to_read)
            
            # Handle single value vs list
            if not isinstance(bitmasks, list):
                bitmasks = [bitmasks]
            
            # Parse each group
            all_detectors = []
            for group_idx, bitmask in zip(self._groups_to_poll, bitmasks):
                start_detector = group_idx * 8 + 1
                detectors = parse_detectors_from_bitmask(bitmask, start_detector)
                all_detectors.extend(detectors)
            
            # Filter to our range and check for changes
            start, end = self.detector_range
            for detector in all_detectors:
                det_num = detector.detector_num
                
                if start <= det_num < end:
                    new_state = detector.state
                    old_state = self._last_detectors.get(det_num)
                    
                    if old_state != new_state:
                        self._emit_detector_change_events(det_num, old_state, new_state)
                        self._last_detectors[det_num] = new_state
            
            self._last_state = {'detectors': all_detectors}
            
        except SNMPError:
            pass
    
    def _emit_detector_change_events(self, detector_num, old_state, new_state):
        """Emit detector change events."""
        # General change event
        self.emit(EVENT_DETECTOR_CHANGE, detector_num, old_state, new_state)
        
        # Specific state events
        if new_state == DetectorState.ACTIVE:
            self.emit(EVENT_DETECTOR_ON, detector_num)
        elif new_state == DetectorState.INACTIVE:
            self.emit(EVENT_DETECTOR_OFF, detector_num)
    
    def get_current_detector_state(self, detector_num):
        """
        Get current state of a specific detector.
        
        Args:
            detector_num: Detector number (1-64)
        
        Returns:
            DetectorState or None if not monitored/unknown
        """
        return self._last_detectors.get(detector_num)
    
    def get_all_detectors(self):
        """Get dictionary of all current detector states."""
        return self._last_detectors.copy()
    
    def get_active_detectors(self):
        """Get list of currently active detector numbers."""
        return [num for num, state in self._last_detectors.items() 
                if state == DetectorState.ACTIVE]
