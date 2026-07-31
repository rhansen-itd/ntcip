"""Loop/stopbar shape configuration for the live video overlay.

**Vendored** from ``pyatspm/src/atspm/data/video.py`` (L50-234) rather than
imported: ``ntcip`` and ``pyatspm`` must stay independently distributable, and
a shared third package is not worth it for ~80 lines of CSV reading (ROADMAP
Item 11, decision A). pyatspm's calibrator (``atspm video-calibrate-shapes``)
remains the authoring tool; this module only reads what it writes.

Deliberate deviations from the pyatspm original — do not "sync" them away:

1. **Two CSV layouts are accepted.** pyatspm's current writer emits a
   two-section file (a ``video_width,video_height`` metadata row, then the
   shape table). Real calibration files in the field — including the owner's
   ``vid_cfg720.csv`` — are in the older *legacy* layout: one header, per-row
   ``video_width``/``video_height``, an extra ``direction`` column, and no
   ``name``. pyatspm's loader crashes on the legacy layout (``int("stopbar")``),
   so :meth:`ShapeConfig.load` sniffs row 1 and reads both.
2. **No ``validate_resolution()``.** It exists to refuse rescaling a recorded
   video; the overlay draws on a ``<canvas>`` sized to ``video_width`` /
   ``video_height`` and scaled by the browser, so the concern doesn't apply.
   The metadata itself is still kept — the page needs it.
3. **No ``save()`` / ``relevant_*()``.** This side never authors CSVs, and
   nothing consumes the ``relevant_*`` helpers yet.
4. **Malformed rows are skipped, not fatal.** :meth:`ShapeConfig.load` raises
   only ``FileNotFoundError``. A row with unparseable points/color/input is
   dropped and reported in a single WARNING naming the offending line numbers.
   An overlay that comes up missing one loop beats a page that won't load.

Colors are stored exactly as authored, which is **BGR** (pyatspm writes OpenCV
order): ``"255,0,0"`` is *blue*. Anything rendering these in a browser must
reverse the triple. :func:`shapes_payload` does that once, on the way out to
``/api/overlay/shapes`` — the in-memory shapes keep the authored BGR so this
module still round-trips what the calibrator wrote. The reversal is here rather
than in the page's JavaScript because the mistake looks entirely plausible on
screen; in Python a unit test pins it.

File layouts
------------

Two-section (pyatspm current)::

    video_width,video_height
    720,720
    type,points,color,input,phase,name
    loop,"100,100;200,100;200,200","0,255,0",3,,South Loop 3
    stopbar,"50,300;250,300","0,0,255",,2,SB Stop Bar

Legacy (field calibrations)::

    type,points,color,input,phase,direction,video_width,video_height
    loop,"100,100;200,100;200,200","0,255,0",3,,,720,720
    stopbar,"50,300;250,300","0,0,255",,2,,720,720

Overlap numbering
-----------------
Overlap letters ``"OLA"``-``"OLP"`` map to ``1``-``16`` (``A=1, B=2, ...``),
matching the Indiana/Purdue Hi-Res Logger Enumerations spec's "Overlap # (as
number A=1 B=2, etc)" convention.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Overlap letter <-> number convention (fixed, global -- see module docstring)
# ---------------------------------------------------------------------------

OVERLAP_LETTER_MAP: Dict[str, int] = {f"OL{chr(ord('A') + i)}": i + 1 for i in range(16)}

# Valid signal phase numbers, matching the overlap range above: the
# Indiana/Purdue Hi-Res Logger Enumerations spec numbers both 1-16.
MIN_PHASE_NUMBER: int = 1
MAX_PHASE_NUMBER: int = 16

# The highest overlap this deployment's monitor actually polls. NOT a format
# limit -- the CSV permits OLA-OLP (1-16), but PhaseMonitor's `monitor_overlaps`
# option covers overlaps 1-8 only. A shape referencing OLI+ renders permanently
# grey with no explanation, so `load()` names those shapes in a WARNING; the
# status resolver then reports them as "na" rather than raising.
MAX_MONITORED_OVERLAP: int = 8

DEFAULT_COLOR: Tuple[int, int, int] = (0, 255, 0)

_META_FIELDS = ["video_width", "video_height"]
_CSV_FIELDS = ["type", "points", "color", "input", "phase", "name"]
_LEGACY_FIELDS = ["type", "points", "color", "input", "phase", "direction",
                  "video_width", "video_height"]


def resolve_stopbar_target(phase_field: Union[int, str]) -> Tuple[str, int]:
    """Resolve a stopbar shape's ``phase`` field to a lookup target.

    Args:
        phase_field: Either an integer/numeric-string phase number
            (``1``-``16``), or an overlap letter code (``"OLA"``-``"OLP"``).

    Returns:
        A ``(kind, number)`` tuple where ``kind`` is ``"phase"`` or
        ``"overlap"``.

    Raises:
        ValueError: If ``phase_field`` is neither a valid phase number nor
            a recognised overlap letter, or if it is an integer outside the
            ``MIN_PHASE_NUMBER``-``MAX_PHASE_NUMBER`` range.
    """
    s = str(phase_field).strip().upper()
    if s in OVERLAP_LETTER_MAP:
        return ("overlap", OVERLAP_LETTER_MAP[s])
    try:
        number = int(s)
    except ValueError as exc:
        raise ValueError(
            f"Unrecognised stopbar phase field {phase_field!r}: not an "
            f"integer phase number or an OLA-OLP overlap code."
        ) from exc
    if not MIN_PHASE_NUMBER <= number <= MAX_PHASE_NUMBER:
        raise ValueError(
            f"Stopbar phase number {number} out of range: phases are "
            f"{MIN_PHASE_NUMBER}-{MAX_PHASE_NUMBER}."
        )
    return ("phase", number)


def _parse_points(points_field: str) -> List[Tuple[int, int]]:
    """Parse a ``"x,y;x,y;..."`` points field.

    Args:
        points_field: The raw ``points`` column value.

    Returns:
        A list of ``(x, y)`` integer tuples.

    Raises:
        ValueError: If the field is empty or any coordinate pair is malformed.
    """
    raw = (points_field or "").strip()
    if not raw:
        raise ValueError("empty points field")

    points: List[Tuple[int, int]] = []
    for pt_str in raw.split(";"):
        parts = pt_str.split(",")
        if len(parts) != 2:
            raise ValueError(f"malformed point {pt_str!r}: expected 'x,y'")
        x, y = (int(p) for p in parts)
        points.append((x, y))
    return points


def _parse_color(color_field: str) -> Tuple[int, int, int]:
    """Parse a ``"b,g,r"`` color field (BGR — OpenCV order, see module doc).

    Args:
        color_field: The raw ``color`` column value. Empty means "unset".

    Returns:
        A 3-tuple of ints, or :data:`DEFAULT_COLOR` if the field is empty.

    Raises:
        ValueError: If the field is present but not three integers.
    """
    raw = (color_field or "").strip()
    if not raw:
        return DEFAULT_COLOR

    parts = raw.split(",")
    if len(parts) != 3:
        raise ValueError(f"malformed color {raw!r}: expected 'b,g,r'")
    b, g, r = (int(p) for p in parts)
    return (b, g, r)


def _row_to_shape(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert one CSV row into a shape dict.

    Both CSV layouts funnel through here so points/colors are parsed in exactly
    one place. ``direction`` and the per-row ``video_width``/``video_height``
    of the legacy layout are ignored — resolution is config-level metadata.

    Args:
        row: A ``csv.DictReader`` row.

    Returns:
        A shape dict with ``type``, ``points``, ``color``, ``input``,
        ``phase``, and ``name`` keys.

    Raises:
        ValueError: If ``points``, ``color``, or ``input`` won't parse.
    """
    shape_type = (row.get("type") or "").strip()
    if not shape_type:
        raise ValueError("missing shape type")

    input_raw = (row.get("input") or "").strip()
    try:
        input_num = int(input_raw) if input_raw else None
    except ValueError as exc:
        raise ValueError(f"malformed input {input_raw!r}: expected an integer") from exc

    phase_raw = (row.get("phase") or "").strip()
    name_raw = (row.get("name") or "").strip()

    return {
        "type": shape_type,
        "points": _parse_points(row.get("points", "")),
        "color": _parse_color(row.get("color", "")),
        "input": input_num,
        "phase": phase_raw or None,
        "name": name_raw or None,
    }


def _to_int(value: Optional[str]) -> Optional[int]:
    """Best-effort int conversion for resolution metadata.

    Args:
        value: A raw CSV cell, possibly empty or absent.

    Returns:
        The int value, or ``None`` if it is empty or unparseable.
    """
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def bgr_to_rgb(color: Tuple[int, int, int]) -> List[int]:
    """Reverse an authored BGR triple into browser RGB order.

    CSV colors come out of pyatspm's OpenCV-based calibrator, so ``"255,0,0"``
    means *blue*. Rendering it as CSS ``rgb(255,0,0)`` would silently draw red
    — plausible enough on screen that it needs a test rather than an eyeball.

    Args:
        color: A ``(b, g, r)`` triple as stored on a shape.

    Returns:
        list: ``[r, g, b]``, JSON-serialisable.
    """
    b, g, r = color
    return [r, g, b]


def shapes_payload(config: "ShapeConfig") -> Dict[str, Any]:
    """Serialise a shape config for the ``/api/overlay/shapes`` route.

    This is the static half of the overlay payload, fetched once per page load;
    the per-poll half is :func:`~ntcip_monitor.ui.overlay.status.resolve_all`,
    whose output is positionally parallel to ``shapes`` here.

    Args:
        config: The loaded shape config.

    Returns:
        dict: ``{"video_width", "video_height", "shapes": [...]}``. Each shape
        carries ``type``, ``points`` (``[[x, y], ...]``), ``color`` as
        **RGB** (see :func:`bgr_to_rgb`), ``input``, ``phase``, and ``name``.
    """
    return {
        "video_width": config.video_width,
        "video_height": config.video_height,
        "shapes": [
            {
                "type": shape["type"],
                "points": [[x, y] for x, y in shape["points"]],
                "color": bgr_to_rgb(shape["color"]),
                "input": shape["input"],
                "phase": shape["phase"],
                "name": shape["name"],
            }
            for shape in config.shapes
        ],
    }


class ShapeConfig:
    """Loop/stopbar shape definitions for a single camera.

    Attributes:
        shapes: List of shape dicts (``type``, ``points``, ``color``,
            ``input``, ``phase``, ``name``). ``color`` is BGR as authored.
        video_width: Calibrated frame width in pixels, or ``None``.
        video_height: Calibrated frame height in pixels, or ``None``.
    """

    def __init__(
        self,
        shapes: Optional[List[Dict[str, Any]]] = None,
        video_width: Optional[int] = None,
        video_height: Optional[int] = None,
    ) -> None:
        """Initialize a shape config.

        Args:
            shapes: Shape dicts, or ``None`` for an empty config.
            video_width: Calibrated frame width in pixels.
            video_height: Calibrated frame height in pixels.
        """
        self.shapes: List[Dict[str, Any]] = shapes if shapes is not None else []
        self.video_width = video_width
        self.video_height = video_height

    @classmethod
    def load(cls, path: Union[str, Path]) -> "ShapeConfig":
        """Load a shape CSV in either the two-section or legacy layout.

        Row 1 is sniffed: a ``video_width,video_height`` header means the
        two-section layout; a header starting with ``type`` means the legacy
        layout, whose resolution is read from the first data row.

        Malformed shape rows are skipped rather than fatal — a single WARNING
        names the offending 1-based line numbers. A second WARNING names any
        stopbar shape referencing an overlap above
        :data:`MAX_MONITORED_OVERLAP`, which would otherwise render grey
        forever with no explanation.

        Args:
            path: Path to the shape CSV.

        Returns:
            A populated ``ShapeConfig``.

        Raises:
            FileNotFoundError: If *path* does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Shape config not found: {path}")

        with open(path, "r", newline="") as f:
            rows = list(csv.reader(f))

        header = rows[0] if rows else []
        first_cell = header[0].strip().lower() if header else ""

        if first_cell == "video_width":
            config = cls._from_two_section(path, rows)
        else:
            config = cls._from_legacy(path, rows)

        config._warn_unmonitored_overlaps(path)
        return config

    @classmethod
    def _from_two_section(cls, path: Path, rows: List[List[str]]) -> "ShapeConfig":
        """Parse the two-section layout (metadata row, then the shape table).

        Args:
            path: Source path, used only for log context.
            rows: All CSV rows, header included.

        Returns:
            A populated ``ShapeConfig``.
        """
        meta_row = rows[1] if len(rows) > 1 else []
        video_width = _to_int(meta_row[0]) if len(meta_row) > 0 else None
        video_height = _to_int(meta_row[1]) if len(meta_row) > 1 else None

        shape_header = rows[2] if len(rows) > 2 else list(_CSV_FIELDS)
        shape_header = [h.strip() for h in shape_header]

        # Line 4 is the first shape row (1-based, header + meta + shape header).
        shapes = cls._parse_shape_rows(path, shape_header, rows[3:], first_line=4)
        return cls(shapes=shapes, video_width=video_width, video_height=video_height)

    @classmethod
    def _from_legacy(cls, path: Path, rows: List[List[str]]) -> "ShapeConfig":
        """Parse the legacy layout (single header, per-row width/height).

        Args:
            path: Source path, used only for log context.
            rows: All CSV rows, header included.

        Returns:
            A populated ``ShapeConfig``.
        """
        header = [h.strip() for h in rows[0]] if rows else list(_LEGACY_FIELDS)
        data_rows = rows[1:]

        video_width = video_height = None
        if data_rows:
            first = dict(zip(header, data_rows[0]))
            video_width = _to_int(first.get("video_width"))
            video_height = _to_int(first.get("video_height"))

        # Line 2 is the first shape row (1-based, header only).
        shapes = cls._parse_shape_rows(path, header, data_rows, first_line=2)
        return cls(shapes=shapes, video_width=video_width, video_height=video_height)

    @staticmethod
    def _parse_shape_rows(
        path: Path,
        header: List[str],
        data_rows: List[List[str]],
        first_line: int,
    ) -> List[Dict[str, Any]]:
        """Convert data rows to shapes, skipping and reporting malformed ones.

        Args:
            path: Source path, used only for log context.
            header: Column names to zip each row against.
            data_rows: The raw shape rows.
            first_line: 1-based file line number of ``data_rows[0]``.

        Returns:
            The successfully parsed shapes, in file order.
        """
        shapes: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []

        for offset, row in enumerate(data_rows):
            line_no = first_line + offset
            if not any((cell or "").strip() for cell in row):
                continue  # blank line — not an error worth reporting
            try:
                shapes.append(_row_to_shape(dict(zip(header, row))))
            except ValueError as exc:
                skipped.append({"line": line_no, "reason": str(exc)})

        if skipped:
            logger.warning(
                "Skipped %d malformed shape row(s) in %s",
                len(skipped), path,
                extra={
                    "event": "overlay_shapes_rows_skipped",
                    "path": str(path),
                    "skipped_count": len(skipped),
                    "skipped_rows": skipped,
                },
            )

        return shapes

    def _warn_unmonitored_overlaps(self, path: Path) -> None:
        """Emit one WARNING naming shapes bound to un-polled overlaps.

        Args:
            path: Source path, used only for log context.
        """
        offenders: List[Dict[str, Any]] = []
        for index, shape in enumerate(self.shapes):
            if shape["type"] != "stopbar" or not shape["phase"]:
                continue
            try:
                kind, number = resolve_stopbar_target(shape["phase"])
            except ValueError:
                continue  # reported as "na" at request time; not this warning
            if kind == "overlap" and number > MAX_MONITORED_OVERLAP:
                offenders.append({
                    "index": index,
                    "phase": shape["phase"],
                    "name": shape["name"],
                })

        if offenders:
            logger.warning(
                "%d shape(s) in %s reference overlaps above %d, which the "
                "monitor does not poll; they will render as 'na'",
                len(offenders), path, MAX_MONITORED_OVERLAP,
                extra={
                    "event": "overlay_shapes_unmonitored_overlaps",
                    "path": str(path),
                    "max_monitored_overlap": MAX_MONITORED_OVERLAP,
                    "shapes": offenders,
                },
            )
