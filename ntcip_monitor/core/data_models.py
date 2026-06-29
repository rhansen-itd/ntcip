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


# ============================================================================
# RING STATE  (bits 0-2 of ringStatus byte)
# ============================================================================

class RingState(Enum):
    """Coded phase-timing state extracted from bits 0-2 of ringStatus.

    Values match the NTCIP 1202 §5.8.6.1 specification exactly.
    """
    MIN_GREEN     = 0  # Phase is timing minimum green
    EXTENSION     = 1  # Phase is timing a vehicle extension
    MAXIMUM       = 2  # Phase is timing maximum green
    GREEN_REST    = 3  # Phase is resting in green (no demand)
    YELLOW_CHANGE = 4  # Phase is in yellow-change interval
    RED_CLEARANCE = 5  # Phase is in red-clearance interval
    RED_REST      = 6  # Phase is resting in red
    UNDEFINED     = 7  # Undefined / controller-specific


# ============================================================================
# TERMINATION REASON  (bits 3-5 of ringStatus byte)
# ============================================================================

class TerminationReason(Enum):
    """Reason a ring's active phase was (or is being) terminated.

    Priority when multiple bits are simultaneously set:
    ForceOff (bit 5) > MaxOut (bit 4) > GapOut (bit 3) > NONE.
    """
    NONE      = 0  # No termination in progress
    GAP_OUT   = 3  # Bit 3 – phase ended due to gap-out
    MAX_OUT   = 4  # Bit 4 – phase ended due to max-out
    FORCE_OFF = 5  # Bit 5 – phase ended due to force-off


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


# ============================================================================
# RING STATUS DATACLASS
# ============================================================================

@dataclass
class RingStatus:
    """Parsed status for a single controller ring.

    Attributes:
        ring_num: Ring number (1-4).
        state: Coded timing state from bits 0-2.
        termination_reason: Active termination source from bits 3-5, or
            ``TerminationReason.NONE`` when no termination bit is set.
        raw_value: The original 8-bit integer returned by SNMP, preserved
            for diagnostics and logging.
        timestamp: Wall-clock time this snapshot was taken (machine clock,
            never the controller clock).
    """
    ring_num: int
    state: RingState
    termination_reason: TerminationReason
    raw_value: int
    timestamp: datetime = field(default_factory=datetime.now)

    def __str__(self) -> str:
        return (
            f"Ring {self.ring_num}: {self.state.name} "
            f"[term={self.termination_reason.name}] (raw=0x{self.raw_value:02X})"
        )


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


def parse_ring_status(ring_num: int, raw_value: int) -> RingStatus:
    """Parse a raw SNMP ringStatus byte into a structured RingStatus.

    Bit layout (NTCIP 1202 §5.8.6.1):

    +---------+--------------------------------------------------+
    | Bit(s)  | Meaning                                          |
    +=========+==================================================+
    | 5       | Force Off  (1 = active phase forced off)         |
    | 4       | Max Out    (1 = active phase timed out on max)   |
    | 3       | Gap Out    (1 = active phase ended on gap)       |
    | 2-0     | Coded status (0=MinGreen ... 7=Undefined)        |
    +---------+--------------------------------------------------+

    Termination priority when multiple bits are set simultaneously:
    ForceOff > MaxOut > GapOut > NONE.

    Args:
        ring_num: Ring number (1-4) – stored on the returned object.
        raw_value: 8-bit integer returned by SNMP GET for ringStatus.

    Returns:
        A fully populated RingStatus instance whose timestamp reflects the
        calling machine's datetime.now(), never the controller clock.

    Raises:
        ValueError: If ``ring_num`` is outside [1, 4] or ``raw_value``
            is outside [0, 255].
    """
    if not 1 <= ring_num <= 4:
        raise ValueError(f"ring_num must be 1-4, got {ring_num!r}")
    if not 0 <= raw_value <= 255:
        raise ValueError(f"raw_value must be 0-255, got {raw_value!r}")

    # Coded state: bits 0-2
    coded = raw_value & 0b00000111
    state = RingState(coded)

    # Termination reason: bits 3-5, evaluated in priority order
    force_off = bool(raw_value & (1 << 5))  # bit 5
    max_out   = bool(raw_value & (1 << 4))  # bit 4
    gap_out   = bool(raw_value & (1 << 3))  # bit 3

    if force_off:
        termination_reason = TerminationReason.FORCE_OFF
    elif max_out:
        termination_reason = TerminationReason.MAX_OUT
    elif gap_out:
        termination_reason = TerminationReason.GAP_OUT
    else:
        termination_reason = TerminationReason.NONE

    return RingStatus(
        ring_num=ring_num,
        state=state,
        termination_reason=termination_reason,
        raw_value=raw_value,
        # timestamp is set by the dataclass default_factory (machine clock)
    )
