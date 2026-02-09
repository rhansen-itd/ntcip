"""
Output Monitor - Monitors digital outputs 1-16
"""

from ..core.event_monitor import BaseMonitor, EVENT_OUTPUT_CHANGE, EVENT_OUTPUT_ON, EVENT_OUTPUT_OFF
from ..core.oid_definitions import OUTPUT_OIDS
from ..core.data_models import OutputState, OutputStatus
from ..core.snmp_client import SNMPError


class OutputMonitor(BaseMonitor):
    """
    Monitor digital outputs 1-16.
    
    Emits events:
    - 'output_change': (output_num, old_state, new_state)
    - 'output_on': (output_num)
    - 'output_off': (output_num)
    
    Example usage:
        monitor = OutputMonitor(snmp_client)
        monitor.on('output_change', lambda out, old, new: print(f"Output {out}: {old} -> {new}"))
        monitor.start()
    """
    
    def __init__(self, snmp_client, poll_interval=0.25, output_range=None):
        """
        Initialize output monitor.
        
        Args:
            snmp_client: EconoliteSNMPClient instance
            poll_interval: Poll interval in seconds
            output_range: Tuple of (start, end) output numbers to monitor,
                         or None to monitor all 16 (default None)
        """
        super().__init__(snmp_client, poll_interval, name="OutputMonitor")
        
        if output_range is None:
            self.output_range = (1, 17)  # All 16 outputs
        else:
            self.output_range = output_range
        
        # Store last state
        self._last_outputs = {}  # {output_num: OutputState}
    
    def _poll(self):
        """Poll outputs and emit events on changes."""
        try:
            # Determine which outputs to read
            start, end = self.output_range
            outputs_to_read = range(start, end)
            oids = [OUTPUT_OIDS[i - 1] for i in outputs_to_read]
            
            # Read all outputs
            values = self.snmp_client.get(*oids)
            
            # Handle single value vs list
            if not isinstance(values, list):
                values = [values]
            
            # Check each output for changes
            for output_num, value in zip(outputs_to_read, values):
                new_state = OutputState.ON if value == 1 else OutputState.OFF
                old_state = self._last_outputs.get(output_num)
                
                if old_state != new_state:
                    self._emit_output_change_events(output_num, old_state, new_state)
                    self._last_outputs[output_num] = new_state
            
            self._last_state = {'outputs': self._last_outputs.copy()}
            
        except SNMPError:
            pass
    
    def _emit_output_change_events(self, output_num, old_state, new_state):
        """Emit output change events."""
        # General change event
        self.emit(EVENT_OUTPUT_CHANGE, output_num, old_state, new_state)
        
        # Specific state events
        if new_state == OutputState.ON:
            self.emit(EVENT_OUTPUT_ON, output_num)
        elif new_state == OutputState.OFF:
            self.emit(EVENT_OUTPUT_OFF, output_num)
    
    def get_current_output_state(self, output_num):
        """
        Get current state of a specific output.
        
        Args:
            output_num: Output number (1-16)
        
        Returns:
            OutputState or None if not monitored/unknown
        """
        return self._last_outputs.get(output_num)
    
    def get_all_outputs(self):
        """Get dictionary of all current output states."""
        return self._last_outputs.copy()
    
    def get_active_outputs(self):
        """Get list of currently active output numbers."""
        return [num for num, state in self._last_outputs.items() 
                if state == OutputState.ON]
