"""NTCIP Monitor - Core Package"""

from .snmp_client import EconoliteSNMPClient, SNMPError
from .data_models import (
    SignalState, DetectorState, OutputState,
    PhaseStatus, OverlapStatus, DetectorStatus, OutputStatus,
    RingState, TerminationReason, RingStatus,
    parse_ring_status,
)
from .event_monitor import (
    BaseMonitor, EventEmitter,
    EVENT_PHASE_CHANGE, EVENT_PHASE_GREEN_START, EVENT_PHASE_RED_START, EVENT_PHASE_YELLOW_START,
    EVENT_OVERLAP_CHANGE,
    EVENT_DETECTOR_CHANGE, EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF,
    EVENT_OUTPUT_CHANGE, EVENT_OUTPUT_ON, EVENT_OUTPUT_OFF,
    EVENT_RING_STATE_CHANGE, EVENT_RING_TERMINATION, EVENT_PHASE_TERMINATED,
    EVENT_ERROR, EVENT_CONNECTION_LOST, EVENT_CONNECTION_RESTORED,
)

__all__ = [
    # SNMP
    'EconoliteSNMPClient', 'SNMPError',
    # Signal / detector / output models
    'SignalState', 'DetectorState', 'OutputState',
    'PhaseStatus', 'OverlapStatus', 'DetectorStatus', 'OutputStatus',
    # Ring models
    'RingState', 'TerminationReason', 'RingStatus', 'parse_ring_status',
    # Base classes
    'BaseMonitor', 'EventEmitter',
    # Phase events
    'EVENT_PHASE_CHANGE', 'EVENT_PHASE_GREEN_START', 'EVENT_PHASE_RED_START',
    'EVENT_PHASE_YELLOW_START', 'EVENT_PHASE_TERMINATED',
    # Overlap events
    'EVENT_OVERLAP_CHANGE',
    # Detector events
    'EVENT_DETECTOR_CHANGE', 'EVENT_DETECTOR_ON', 'EVENT_DETECTOR_OFF',
    # Output events
    'EVENT_OUTPUT_CHANGE', 'EVENT_OUTPUT_ON', 'EVENT_OUTPUT_OFF',
    # Ring events
    'EVENT_RING_STATE_CHANGE', 'EVENT_RING_TERMINATION',
    # System events
    'EVENT_ERROR', 'EVENT_CONNECTION_LOST', 'EVENT_CONNECTION_RESTORED',
]
