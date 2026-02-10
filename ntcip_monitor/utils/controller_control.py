"""
Controller Control Functions

Functions to:
- Set system time
- Place vehicle calls
- Control digital outputs
"""

import time
from datetime import datetime
from pysnmp.hlapi import Counter32, Integer32
from ..core.oid_definitions import GLOBAL_TIME, PHASE_1_8_VEH_CALL, PHASE_9_16_VEH_CALL, get_output_oid
from ..core.snmp_client import SNMPError


class ControllerControl:
    """
    Control functions for traffic controller.
    
    Provides methods to:
    - Set controller time
    - Place vehicle calls
    - Set digital outputs
    """
    
    def __init__(self, snmp_client):
        """
        Initialize controller control.
        
        Args:
            snmp_client: EconoliteSNMPClient instance
        """
        self.snmp_client = snmp_client
    
    def set_system_time(self, dt: datetime = None) -> bool:
        """
        Set controller system time.
        
        Args:
            dt: datetime object to set (default: current system time)
        
        Returns:
            bool: True on success
        
        Raises:
            SNMPError: On SNMP communication error
        """
        if dt is None:
            dt = datetime.now()
        
        # Convert to seconds since epoch (UTC)
        timestamp = int(dt.timestamp())
        
        try:
            self.snmp_client.set(GLOBAL_TIME, timestamp)
            print(f"Controller time set to: {dt.strftime('%Y-%m-%d %H:%M:%S')}")
            return True
        except SNMPError as e:
            print(f"Failed to set controller time: {e}")
            raise
    
    def sync_time_to_system(self) -> bool:
        
        """
        Sync controller time to current system time.
        
        Returns:
            bool: True on success
        """
        return self.set_system_time(datetime.now())
    
    def place_vehicle_call(self, phase_num: int, duration: float = 0) -> bool:
        """
        Place a vehicle call on a specific phase.
        
        Args:
            phase_num: Phase number (1-16)
            duration: Duration to hold call in seconds (0 = momentary pulse)
        
        Returns:
            bool: True on success
        
        Raises:
            ValueError: If phase number is invalid
            SNMPError: On SNMP communication error
        """
        if not 1 <= phase_num <= 16:
            raise ValueError("Phase number must be 1-16")
        
        # Determine which group and bit
        if phase_num <= 8:
            oid = PHASE_1_8_VEH_CALL
            bit = phase_num - 1  # Phase 1 = bit 0
        else:
            oid = PHASE_9_16_VEH_CALL
            bit = phase_num - 9  # Phase 9 = bit 0
        
        # Create bitmask
        bitmask = 1 << bit
        
        try:
            # Set the bit
            self.snmp_client.set(oid, bitmask)
            print(f"Vehicle call placed on phase {phase_num}")
            
            # If duration specified, hold then clear
            if duration > 0:
                time.sleep(duration)
                self.snmp_client.set(oid, 0)
                print(f"Vehicle call cleared on phase {phase_num}")
            
            return True
            
        except SNMPError as e:
            print(f"Failed to place vehicle call on phase {phase_num}: {e}")
            raise
    
    def set_output(self, output_num: int, state: bool) -> bool:
        """
        Set a digital output ON or OFF.
        
        Args:
            output_num: Output number (1-16)
            state: True for ON, False for OFF
        
        Returns:
            bool: True on success
        
        Raises:
            ValueError: If output number is invalid
            SNMPError: On SNMP communication error
        """
        if not 1 <= output_num <= 16:
            raise ValueError("Output number must be 1-16")
        
        oid = get_output_oid(output_num)
        value = 1 if state else 0
        
        try:
            self.snmp_client.set(oid, value)
            state_str = "ON" if state else "OFF"
            print(f"Output {output_num} set to {state_str}")
            return True
            
        except SNMPError as e:
            print(f"Failed to set output {output_num}: {e}")
            raise
    
    def pulse_output(self, output_num: int, duration: float = 1.0) -> bool:
        """
        Pulse an output ON for a duration then turn OFF.
        
        Args:
            output_num: Output number (1-16)
            duration: Duration in seconds to keep output ON
        
        Returns:
            bool: True on success
        """
        try:
            self.set_output(output_num, True)
            time.sleep(duration)
            self.set_output(output_num, False)
            return True
        except SNMPError:
            return False
