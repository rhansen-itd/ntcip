"""Map an ``/api/status`` payload onto overlay shapes (ROADMAP Item 11a).

Pure functions: no I/O, no Flask, no monitor imports. The overlay's
``/api/overlay/state`` route is polled at the dashboard's 250 ms interval, so
every mapping decision lives here in Python — where the unit tests reach it —
rather than in browser JavaScript.

The payload consumed is exactly what ``WebUI``'s ``/api/status`` builds:
``phases`` / ``overlaps`` / ``detectors`` dicts of number -> state *name*
(``SignalState`` / ``DetectorState`` member names from
``ntcip_monitor/core/data_models.py``). Names are compared as strings rather
than importing the enums, which keeps this module free of monitor imports and
testable with plain dicts. Keys may be ``int`` (the dicts as the monitors hand
them over, in-process) or ``str`` (the same payload round-tripped through
``jsonify``); both are accepted.

Resolution table:

===========================  ==================  =========================
Shape                        Source              Result
===========================  ==================  =========================
``loop``                     ``detectors[input]``  ``ACTIVE`` -> ``"on"``,
                                                   else ``"off"``
``stopbar``, integer phase   ``phases[n]``         ``G`` / ``Y`` / ``R``
``stopbar``, ``OLx``         ``overlaps[n]``       ``G`` / ``Y`` / ``R``
===========================  ==================  =========================

``"na"`` is the catch-all: a ``DARK`` phase, a phase or overlap the monitor
isn't polling, an overlap above ``MAX_MONITORED_OVERLAP``, an unparseable
``phase`` field, or a stopbar with no ``phase`` at all. Nothing here raises —
a single bad shape must never break the poll.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from .shapes import ShapeConfig, resolve_stopbar_target

# Status vocabulary shared with the renderer.
STATUS_NA = "na"
STATUS_LOOP_ON = "on"
STATUS_LOOP_OFF = "off"

#: ``SignalState`` member name -> single-letter status. ``DARK`` is absent on
#: purpose: it resolves to ``STATUS_NA``, same as "not being polled".
SIGNAL_STATE_STATUS: Dict[str, str] = {
    "GREEN": "G",
    "YELLOW": "Y",
    "RED": "R",
}

#: ``DetectorState`` member name that counts as an occupied loop.
DETECTOR_ACTIVE_NAME = "ACTIVE"


def _lookup(mapping: Optional[Mapping[Any, Any]], number: int) -> Optional[Any]:
    """Fetch ``number`` from a payload dict keyed by either int or str.

    Args:
        mapping: A ``phases`` / ``overlaps`` / ``detectors`` dict, or ``None``.
        number: The phase, overlap, or detector number to look up.

    Returns:
        The stored value, or ``None`` if absent or *mapping* is falsy.
    """
    if not mapping:
        return None
    if number in mapping:
        return mapping[number]
    return mapping.get(str(number))


def _signal_status(mapping: Optional[Mapping[Any, Any]], number: int) -> str:
    """Resolve a phase/overlap number to a ``G`` / ``Y`` / ``R`` / ``na``.

    Args:
        mapping: The ``phases`` or ``overlaps`` dict from the payload.
        number: The phase or overlap number.

    Returns:
        ``"G"``, ``"Y"``, ``"R"``, or ``STATUS_NA``.
    """
    state = _lookup(mapping, number)
    if state is None:
        return STATUS_NA
    return SIGNAL_STATE_STATUS.get(str(state).strip().upper(), STATUS_NA)


def resolve_shape_status(
    shape: Mapping[str, Any],
    phases: Optional[Mapping[Any, Any]] = None,
    overlaps: Optional[Mapping[Any, Any]] = None,
    detectors: Optional[Mapping[Any, Any]] = None,
) -> str:
    """Resolve one shape to its current status string.

    Args:
        shape: A shape dict as produced by
            :meth:`~ntcip_monitor.ui.overlay.shapes.ShapeConfig.load`.
        phases: ``phases`` dict from the ``/api/status`` payload.
        overlaps: ``overlaps`` dict from the ``/api/status`` payload.
        detectors: ``detectors`` dict from the ``/api/status`` payload.

    Returns:
        ``"on"``/``"off"`` for a loop; ``"G"``/``"Y"``/``"R"`` for a stopbar;
        ``STATUS_NA`` when the state is dark, unknown, or unresolvable. Never
        raises.
    """
    shape_type = str(shape.get("type") or "").strip().lower()

    if shape_type == "loop":
        state = _lookup(detectors, shape.get("input")) if shape.get("input") is not None else None
        if state is not None and str(state).strip().upper() == DETECTOR_ACTIVE_NAME:
            return STATUS_LOOP_ON
        return STATUS_LOOP_OFF

    if shape_type == "stopbar":
        phase_field = shape.get("phase")
        if not phase_field:
            return STATUS_NA
        try:
            kind, number = resolve_stopbar_target(phase_field)
        except ValueError:
            return STATUS_NA
        if kind == "overlap":
            return _signal_status(overlaps, number)
        return _signal_status(phases, number)

    return STATUS_NA


def resolve_all(
    config: ShapeConfig,
    status_payload: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    """Resolve every shape in *config* against a status payload.

    Args:
        config: The loaded shape config.
        status_payload: An ``/api/status`` payload (or ``None`` / ``{}`` before
            the monitors have produced one).

    Returns:
        A list of status strings, positionally parallel to ``config.shapes``.
    """
    payload = status_payload or {}
    phases = payload.get("phases")
    overlaps = payload.get("overlaps")
    detectors = payload.get("detectors")

    return [
        resolve_shape_status(shape, phases=phases, overlaps=overlaps, detectors=detectors)
        for shape in config.shapes
    ]
