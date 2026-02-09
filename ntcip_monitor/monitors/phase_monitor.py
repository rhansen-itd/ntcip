"""
Phase Monitor - Monitors phases 1-16 and overlaps 1-8
"""

from ..core.event_monitor import BaseMonitor, EVENT_PHASE_CHANGE, EVENT_PHASE_GREEN_START, EVENT_PHASE_RED_START, EVENT_PHASE_YELLOW_START, EVENT_OVERLAP_CHANGE
from ..core.oid_definitions import get_phase_oids, OVERLAP_REDS, OVERLAP_YELLOWS, OVERLAP_GREENS, PED_1_8_DW, PED_1_8_FDW, PED_1_8_W
from ..core.data_models import parse_phases_from_bitmask, SignalState, PhaseStatus, OverlapStatus
from ..core.snmp_client import SNMPError


class PhaseMonitor(BaseMonitor):
    """
    Monitor phases 1-16 and overlaps 1-8.
    
    Emits events:
    - 'phase_change': (phase_num, old_state, new_state)
    - 'phase_green_start': (phase_num)
    - 'phase_red_start': (phase_num)
    - 'phase_yellow_start': (phase_num)
    - 'overlap_change': (overlap_num, old_state, new_state)
    - 'pedestrian_change': (pedestrian_num, old_state, new_state)

    Example usage:
        monitor = PhaseMonitor(snmp_client)
        monitor.on('phase_change', lambda phase, old, new: print(f"Phase {phase}: {old} -> {new}"))
        monitor.on('phase_green_start', lambda phase: print(f"Phase {phase} went GREEN"))
        monitor.start()
    """
    
    def __init__(self, snmp_client, poll_interval=0.25, 
                 monitor_phases_1_8=True, 
                 monitor_phases_9_16=False, 
                 monitor_overlaps=False,
                 monitor_pedestrians=False):
        """
        Initialize phase monitor.
        
        Args:
            snmp_client: EconoliteSNMPClient instance
            poll_interval: Poll interval in seconds
            monitor_phases_1_8: Monitor phases 1-8 (default True)
            monitor_phases_9_16: Monitor phases 9-16 (default False)
            monitor_overlaps: Monitor overlaps 1-8 (default False)
            monitor_pedestrians: Monitor pedestrian phases (default False)
        """
        super().__init__(snmp_client, poll_interval, name="PhaseMonitor")
        
        self.monitor_phases_1_8 = monitor_phases_1_8
        self.monitor_phases_9_16 = monitor_phases_9_16
        self.monitor_overlaps = monitor_overlaps
        self.monitor_pedestrians = monitor_pedestrians
        
        # Store last state for each phase/overlap
        self._last_phases = {}      # {phase_num: SignalState}
        self._last_overlaps = {}    # {overlap_num: SignalState}
        self._last_pedestrians = {} # {ped_num: SignalState}
    
    def _poll(self):
        """Poll phases and emit events on changes."""
        try:
            # Poll phases 1-8
            if self.monitor_phases_1_8:
                self._poll_phase_group(1, range(1, 9))
            
            # Poll phases 9-16
            if self.monitor_phases_9_16:
                self._poll_phase_group(2, range(9, 17))
            
            # Poll overlaps 1-8
            if self.monitor_overlaps:
                self._poll_overlaps()

            # Poll pedestrian phases if enabled
            if self.monitor_pedestrians:
                self._poll_pedestrians()
                
        except SNMPError as e:
            # Connection issue - will be retried next poll
            pass
    
    def _poll_phase_group(self, group_num, phase_range):
        """Poll a group of phases and emit change events."""
        # Get OIDs for this group
        reds_oid, yellows_oid, greens_oid = get_phase_oids(group_num)
        
        # Read all three colors in one SNMP query
        reds, yellows, greens = self.snmp_client.get(reds_oid, yellows_oid, greens_oid)
        
        # Parse phases
        start_phase = min(phase_range)
        phases = parse_phases_from_bitmask(reds, yellows, greens, start_phase)
        
        # Check for changes and emit events
        for phase in phases:
            phase_num = phase.phase_num
            new_state = phase.state
            old_state = self._last_phases.get(phase_num)
            
            if old_state != new_state:
                # State changed - emit events
                self._emit_phase_change_events(phase_num, old_state, new_state)
                self._last_phases[phase_num] = new_state
        
        # Update last state
        self._last_state = {'phases': phases}
    
    def _poll_overlaps(self):
        """Poll overlaps and emit change events."""
        # Read overlap status
        reds, yellows, greens = self.snmp_client.get(OVERLAP_REDS, OVERLAP_YELLOWS, OVERLAP_GREENS)
        
        # Parse overlaps (same as phases, but numbered 1-8)
        overlaps = parse_phases_from_bitmask(reds, yellows, greens, start_phase=1)
        
        # Check for changes
        for overlap_data in overlaps:
            overlap_num = overlap_data.phase_num  # Reusing phase parser, so phase_num is actually overlap_num
            new_state = overlap_data.state
            old_state = self._last_overlaps.get(overlap_num)
            
            if old_state != new_state:
                # Convert to OverlapStatus
                overlap = OverlapStatus(overlap_num, new_state, overlap_data.timestamp)
                
                # Emit overlap change event
                self.emit(EVENT_OVERLAP_CHANGE, overlap_num, old_state, new_state)
                self._last_overlaps[overlap_num] = new_state
    
    def _poll_pedestrians(self):
        """Poll pedestrian signals and emit change events."""
        try:
            from ..core.oid_definitions import PED_1_8_DW, PED_1_8_FDW, PED_1_8_W
            
            # Read all 3 in ONE call to avoid threading issues
            results = self.snmp_client.get(PED_1_8_DW, PED_1_8_FDW, PED_1_8_W)
            
            # Handle both single value and list
            if isinstance(results, list):
                dws, fdws, walks = results[0], results[1], results[2]
            else:
                # Only got one value - means OIDs are wrong
                print(f"Warning: Expected 3 pedestrian values, got 1")
                return
            
            # Parse pedestrians
            pedestrians = parse_phases_from_bitmask(dws, fdws, walks, start_phase=1)
            
            # Check for changes
            for ped_data in pedestrians:
                ped_num = ped_data.phase_num
                new_state = ped_data.state
                old_state = self._last_pedestrians.get(ped_num)
                
                if old_state != new_state:
                    self.emit('pedestrian_change', ped_num, old_state, new_state)
                    
                    if new_state == SignalState.GREEN:
                        self.emit('pedestrian_walk_start', ped_num)
                    elif new_state == SignalState.YELLOW:
                        self.emit('pedestrian_clearance_start', ped_num)
                    elif new_state == SignalState.RED:
                        self.emit('pedestrian_dont_walk_start', ped_num)
                    
                    self._last_pedestrians[ped_num] = new_state
        
        except Exception as e:
            # Don't break phase monitoring
            print(f"Pedestrian monitoring error (suppressed): {e}")

    def _emit_phase_change_events(self, phase_num, old_state, new_state):
        """Emit phase change events."""
        # General change event
        self.emit(EVENT_PHASE_CHANGE, phase_num, old_state, new_state)
        
        # Specific transition events
        if new_state == SignalState.GREEN:
            self.emit(EVENT_PHASE_GREEN_START, phase_num)
        elif new_state == SignalState.RED:
            self.emit(EVENT_PHASE_RED_START, phase_num)
        elif new_state == SignalState.YELLOW:
            self.emit(EVENT_PHASE_YELLOW_START, phase_num)
    
    def get_current_phase_state(self, phase_num):
        """
        Get current state of a specific phase.
        
        Args:
            phase_num: Phase number (1-16)
        
        Returns:
            SignalState or None if not monitored/unknown
        """
        return self._last_phases.get(phase_num)
    
    def get_current_overlap_state(self, overlap_num):
        """
        Get current state of a specific overlap.
        
        Args:
            overlap_num: Overlap number (1-8)
        
        Returns:
            SignalState or None if not monitored/unknown
        """
        return self._last_overlaps.get(overlap_num)
    
    def get_current_pedestrian_state(self, ped_num):
        """
        Get current state of a specific pedestrian signal.
        
        Args:
            ped_num: Pedestrian number (1-8)
        
        Returns:
            SignalState or None if not monitored/unknown
        """
        return self._last_pedestrians.get(ped_num)
    
    def get_all_phases(self):
        """Get dictionary of all current phase states."""
        return self._last_phases.copy()
    
    def get_all_overlaps(self):
        """Get dictionary of all current overlap states."""
        return self._last_overlaps.copy()

    def get_all_pedestrians(self):
        """Get dictionary of all current pedestrian states."""
        return self._last_pedestrians.copy()