"""
Data Models for Traffic Controller State
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime


class SignalState(Enum):
    """Traffic signal state."""
    DARK = 0
    RED = 1
    YELLOW = 2
    GREEN = 3


class DetectorState(Enum):
    """Detector state."""
    INACTIVE = 0
    ACTIVE = 1


class OutputState(Enum):
    """Output state."""
    OFF = 0
    ON = 1


@dataclass
class PhaseStatus:
    """Status of a single phase."""
    phase_num: int
    state: SignalState
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __str__(self):
        return f"Phase {self.phase_num}: {self.state.name}"


@dataclass
class OverlapStatus:
    """Status of a single overlap."""
    overlap_num: int
    state: SignalState
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __str__(self):
        return f"Overlap {self.overlap_num}: {self.state.name}"


@dataclass
class DetectorStatus:
    """Status of a single detector."""
    detector_num: int
    state: DetectorState
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __str__(self):
        return f"Detector {self.detector_num}: {self.state.name}"


@dataclass
class OutputStatus:
    """Status of a single output."""
    output_num: int
    state: OutputState
    timestamp: datetime = field(default_factory=datetime.now)
    
    def __str__(self):
        return f"Output {self.output_num}: {self.state.name}"


@dataclass
class ControllerSnapshot:
    """Complete snapshot of controller state at a point in time."""
    timestamp: datetime
    phases: List[PhaseStatus] = field(default_factory=list)
    overlaps: List[OverlapStatus] = field(default_factory=list)
    detectors: List[DetectorStatus] = field(default_factory=list)
    outputs: List[OutputStatus] = field(default_factory=list)
    
    def get_phase(self, phase_num: int) -> Optional[PhaseStatus]:
        """Get status of specific phase."""
        for phase in self.phases:
            if phase.phase_num == phase_num:
                return phase
        return None
    
    def get_overlap(self, overlap_num: int) -> Optional[OverlapStatus]:
        """Get status of specific overlap."""
        for overlap in self.overlaps:
            if overlap.overlap_num == overlap_num:
                return overlap
        return None
    
    def get_detector(self, detector_num: int) -> Optional[DetectorStatus]:
        """Get status of specific detector."""
        for detector in self.detectors:
            if detector.detector_num == detector_num:
                return detector
        return None
    
    def get_output(self, output_num: int) -> Optional[OutputStatus]:
        """Get status of specific output."""
        for output in self.outputs:
            if output.output_num == output_num:
                return output
        return None


def parse_signal_state(red_bit: int, yellow_bit: int, green_bit: int) -> SignalState:
    """
    Parse signal state from red/yellow/green bits.
    
    Args:
        red_bit: 1 if red is on, 0 if off
        yellow_bit: 1 if yellow is on, 0 if off
        green_bit: 1 if green is on, 0 if off
    
    Returns:
        SignalState enum value
    """
    if green_bit:
        return SignalState.GREEN
    elif yellow_bit:
        return SignalState.YELLOW
    elif red_bit:
        return SignalState.RED
    else:
        return SignalState.DARK


def parse_phases_from_bitmask(red_mask: int, yellow_mask: int, green_mask: int, 
                               start_phase: int = 1) -> List[PhaseStatus]:
    """
    Parse phase statuses from 8-bit bitmasks.
    
    Args:
        red_mask: 8-bit red bitmask
        yellow_mask: 8-bit yellow bitmask
        green_mask: 8-bit green bitmask
        start_phase: Starting phase number (1 for phases 1-8, 9 for phases 9-16)
    
    Returns:
        List of PhaseStatus objects
    """
    phases = []
    for i in range(8):
        phase_num = start_phase + i
        
        # Econolite bit order: Phase N = bit (N-1)
        # Phase 1 = bit 0, Phase 8 = bit 7
        red_bit = (red_mask >> i) & 1
        yellow_bit = (yellow_mask >> i) & 1
        green_bit = (green_mask >> i) & 1
        
        state = parse_signal_state(red_bit, yellow_bit, green_bit)
        phases.append(PhaseStatus(phase_num, state))
    
    return phases


def parse_detectors_from_bitmask(bitmask: int, start_detector: int = 1) -> List[DetectorStatus]:
    """
    Parse detector statuses from 8-bit bitmask.
    
    Args:
        bitmask: 8-bit detector bitmask (1 = active, 0 = inactive)
        start_detector: Starting detector number
    
    Returns:
        List of DetectorStatus objects
    """
    detectors = []
    for i in range(8):
        detector_num = start_detector + i
        active = (bitmask >> i) & 1
        state = DetectorState.ACTIVE if active else DetectorState.INACTIVE
        detectors.append(DetectorStatus(detector_num, state))
    
    return detectors
