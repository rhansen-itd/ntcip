"""NTCIP Monitor - Live Video Overlay support (ROADMAP Item 11).

Pure, stdlib-only building blocks for the ``/overlay`` page:

- :mod:`~ntcip_monitor.ui.overlay.shapes` reads a pyatspm-calibrated shape CSV
  (loops and stopbars over a fixed video resolution).
- :mod:`~ntcip_monitor.ui.overlay.status` maps an ``/api/status`` payload onto
  those shapes, yielding one status string per shape.

Neither module imports Flask, OpenCV, ``atspm``, or any ``ntcip_monitor``
monitor — they are deliberately dependency-free so they stay fully unit
testable (see ``ntcip_monitor/tests/test_overlay_shapes.py``).
"""

from .shapes import (
    MAX_MONITORED_OVERLAP,
    MAX_PHASE_NUMBER,
    MIN_PHASE_NUMBER,
    OVERLAP_LETTER_MAP,
    ShapeConfig,
    resolve_stopbar_target,
)
from .status import (
    STATUS_NA,
    STATUS_LOOP_OFF,
    STATUS_LOOP_ON,
    resolve_all,
    resolve_shape_status,
)

__all__ = [
    'MAX_MONITORED_OVERLAP',
    'MAX_PHASE_NUMBER',
    'MIN_PHASE_NUMBER',
    'OVERLAP_LETTER_MAP',
    'ShapeConfig',
    'resolve_stopbar_target',
    'STATUS_NA',
    'STATUS_LOOP_OFF',
    'STATUS_LOOP_ON',
    'resolve_all',
    'resolve_shape_status',
]
