"""
NTCIP 1201/1202 OID Definitions for Econolite Cobalt/EOS Controllers
"""

# Base OID for NTCIP
NTCIP_BASE = '1.3.6.1.4.1.1206'

# ============================================================================
# PHASE STATUS (NTCIP 1202 Section 3.5.4)
# ============================================================================
# Pattern: phaseStatusGroup[Color][GroupNumber]
# Groups: 1=Phases 1-8, 2=Phases 9-16, 3=Overlaps 1-8

PHASE_STATUS_BASE = f'{NTCIP_BASE}.4.2.1.1.4.1'

# Phases 1-8 (Group 1)
PHASE_1_8_REDS = f'{PHASE_STATUS_BASE}.2.1'
PHASE_1_8_YELLOWS = f'{PHASE_STATUS_BASE}.3.1'
PHASE_1_8_GREENS = f'{PHASE_STATUS_BASE}.4.1'

# Phases 9-16 (Group 2)
PHASE_9_16_REDS = f'{PHASE_STATUS_BASE}.2.2'
PHASE_9_16_YELLOWS = f'{PHASE_STATUS_BASE}.3.2'
PHASE_9_16_GREENS = f'{PHASE_STATUS_BASE}.4.2'

# Pedestrian Phases
PED_1_8_DW = f'{PHASE_STATUS_BASE}.5.1'  # Don't Walks 1-8
PED_1_8_FDW = f'{PHASE_STATUS_BASE}.6.1'  # Flashing Don't Walks 1-8
PED_1_8_W = f'{PHASE_STATUS_BASE}.7.1'  # Walks 1-8

# ============================================================================
# OVERLAPS (User Verified OID: 1.3.6.1.4.1.1206.4.2.1.9.4.1)
# ============================================================================
# Based on MIB Entry: overlapStatusGroupGreens = ...1.9.4.1.4
# Base Table: 1.3.6.1.4.1.1206.4.2.1.9.4.1 (overlapStatusGroupEntry)
# Column 2 = Reds
# Column 3 = Yellows
# Column 4 = Greens
# Index .1 = Group 1 (Overlaps 1-8)

OVERLAP_STATUS_BASE = f'{NTCIP_BASE}.4.2.1.9.4.1'

# Overlaps 1-8 (Group 1)
OVERLAP_REDS    = f'{OVERLAP_STATUS_BASE}.2.1'  # Column 2, Group 1
OVERLAP_YELLOWS = f'{OVERLAP_STATUS_BASE}.3.1'  # Column 3, Group 1
OVERLAP_GREENS  = f'{OVERLAP_STATUS_BASE}.4.1'  # Column 4, Group 1

# ============================================================================
# VEHICLE DETECTORS (NTCIP 1202 Section 3.6.4)
# ============================================================================
# vehicleDetectorStatusGroupActive - 8 groups of 8 detectors each (64 total)
DETECTOR_BASE = f'{NTCIP_BASE}.4.2.1.2.4.1.2'

DETECTOR_1_8 = f'{DETECTOR_BASE}.1'      # Detectors 1-8
DETECTOR_9_16 = f'{DETECTOR_BASE}.2'     # Detectors 9-16
DETECTOR_17_24 = f'{DETECTOR_BASE}.3'    # Detectors 17-24
DETECTOR_25_32 = f'{DETECTOR_BASE}.4'    # Detectors 25-32
DETECTOR_33_40 = f'{DETECTOR_BASE}.5'    # Detectors 33-40
DETECTOR_41_48 = f'{DETECTOR_BASE}.6'    # Detectors 41-48
DETECTOR_49_56 = f'{DETECTOR_BASE}.7'    # Detectors 49-56
DETECTOR_57_64 = f'{DETECTOR_BASE}.8'    # Detectors 57-64

DETECTOR_GROUPS = [
    DETECTOR_1_8, DETECTOR_9_16, DETECTOR_17_24, DETECTOR_25_32,
    DETECTOR_33_40, DETECTOR_41_48, DETECTOR_49_56, DETECTOR_57_64
]

# ============================================================================
# DIGITAL OUTPUTS / SPECIAL FUNCTIONS (NTCIP 1202 Section 3.8.14)
# ============================================================================
# specialFunctionOutputState - Individual outputs 1-16
OUTPUT_BASE = f'{NTCIP_BASE}.4.2.1.3.14.1.2'

# Outputs are accessed individually: OUTPUT_BASE.1 through OUTPUT_BASE.16
OUTPUT_OIDS = [f'{OUTPUT_BASE}.{i}' for i in range(1, 17)]

# ============================================================================
# PHASE CONTROL (NTCIP 1202 Section 3.5.5)
# ============================================================================
# phaseControlGroupVehCall - Place vehicle calls
PHASE_CONTROL_BASE = f'{NTCIP_BASE}.4.2.1.1.5.1'

PHASE_1_8_VEH_CALL = f'{PHASE_CONTROL_BASE}.6.1'    # Phases 1-8
PHASE_9_16_VEH_CALL = f'{PHASE_CONTROL_BASE}.6.2'   # Phases 9-16

# ============================================================================
# RING STATUS (NTCIP 1202 Section 3.5.4.4)
# ============================================================================
# ringStatus - Returns 8-bit integer containing termination and state bits
# Base Table: 1.3.6.1.4.1.1206.4.2.1.7.6.1 (ringStatusEntry)
# Column 1 = ringStatus
RING_STATUS_BASE = f'{NTCIP_BASE}.4.2.1.7.6.1.1'

RING_1_STATUS = f'{RING_STATUS_BASE}.1'
RING_2_STATUS = f'{RING_STATUS_BASE}.2'
RING_3_STATUS = f'{RING_STATUS_BASE}.3'
RING_4_STATUS = f'{RING_STATUS_BASE}.4'

# ============================================================================
# TIME BASE (NTCIP 1201 Section 2.6.3)
# ============================================================================
# globalTime - Controller clock (Integer32: seconds since 1970-01-01 00:00:00 UTC)
GLOBAL_TIME = f'{NTCIP_BASE}.4.2.6.3.1.0'

# ============================================================================
# HELPER MAPPINGS
# ============================================================================

def get_phase_oids(group=1):
    """
    Get phase status OIDs for a specific group.
    
    Args:
        group: 1 for phases 1-8, 2 for phases 9-16, 3 for overlaps 1-8
    
    Returns:
        tuple: (reds_oid, yellows_oid, greens_oid)
    """
    oids = {
        1: (PHASE_1_8_REDS, PHASE_1_8_YELLOWS, PHASE_1_8_GREENS),
        2: (PHASE_9_16_REDS, PHASE_9_16_YELLOWS, PHASE_9_16_GREENS),
        3: (OVERLAP_REDS, OVERLAP_YELLOWS, OVERLAP_GREENS)
    }
    return oids.get(group, oids[1])


def get_detector_oid(detector_num):
    """
    Get OID for a specific detector (1-64).
    
    Args:
        detector_num: Detector number (1-64)
    
    Returns:
        tuple: (oid, bit_position) where bit_position is 0-7 within the byte
    """
    if not 1 <= detector_num <= 64:
        raise ValueError("Detector number must be 1-64")
    
    group_index = (detector_num - 1) // 8  # 0-7
    bit_position = (detector_num - 1) % 8   # 0-7
    
    return DETECTOR_GROUPS[group_index], bit_position


def get_output_oid(output_num):
    """
    Get OID for a specific output (1-16).
    
    Args:
        output_num: Output number (1-16)
    
    Returns:
        str: OID for the output
    """
    if not 1 <= output_num <= 16:
        raise ValueError("Output number must be 1-16")
    
    return OUTPUT_OIDS[output_num - 1]


def get_ring_status_oid(ring_num):
    """
    Get OID for a specific ring status (1-4).
    
    Args:
        ring_num: Ring number (1-4)
        
    Returns:
        str: OID for the ring status
    """
    if not 1 <= ring_num <= 4:
        raise ValueError("Ring number must be 1-4")
        
    return RING_STATUS_OIDS[ring_num - 1]
