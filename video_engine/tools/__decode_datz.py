#!/usr/bin/env python3
"""__decode_datz.py — Econolite .datZ → controller event CSV.

Standalone dev/verification tool (``video_engine/tools/``, ``print()``
allowed).  Produces the ``timestamp,event_code,parameter`` CSV that
``__correlate_channels.py`` and ``__accuracy_report.py`` consume as controller
ground truth, from the raw ``.datZ`` files pulled off the controller.

**The decoding itself is pyatspm's**, not a reimplementation: this loads
``atspm/analysis/decoders.py`` by file path and calls its pure helpers
(``_extract_header_fields`` + ``_parse_binary_payload``) directly.  The public
``parse_datz_bytes`` is bypassed only because it returns a ``pandas``
DataFrame and pandas is not installed here — the byte-level logic, header
handling and timestamp base are pyatspm's own.

Two things this tool exists to get right, both of which an ad-hoc extraction
got wrong once (see DESIGN_HISTORY 2026-07-31):

* **The header offset is applied.**  Binary time offsets are measured from the
  ``Controller Data Log Beginning`` instant, which sits 0.0–1.0 s past the
  clock boundary in the filename — so the sub-minute delta from the header is
  added to the base.  Skipping it shifts the whole export by up to a second,
  which silently inflates any NTCIP-vs-controller lag measurement.
* **The filename boundary is local time.**  ``..._2026_07_31_1800.datZ`` means
  18:00 in the site's timezone (``--tz``, default America/Boise), not UTC.

Usage::

    python3 video_engine/tools/__decode_datz.py ECON_*.datZ -o banks_events.csv
    python3 video_engine/tools/__decode_datz.py capture.zip -o events.csv \
        --detectors-only --start 2026-07-31T18:00 --end 2026-07-31T18:30

``--detectors-only`` keeps just the ATSPM 82/81 detector ON/OFF codes, which
is all the correlation and accuracy tools read.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import re
import sys
import types
import zlib
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

EVENT_ON = "82"
EVENT_OFF = "81"

#: Sibling pyatspm checkout; resolved from this file, not the cwd.
_DEFAULT_PYATSPM = Path(__file__).resolve().parents[3] / "pyatspm"

_NAME_RE = re.compile(r"_(\d{4})_(\d{2})_(\d{2})_(\d{2})(\d{2})\.datZ$", re.I)


def _load_decoders(pyatspm_root: Path):
    """Import pyatspm's ``decoders`` module with a stub for pandas.

    Args:
        pyatspm_root: Path to the pyatspm checkout.

    Returns:
        The loaded module.

    Raises:
        SystemExit: If ``decoders.py`` is not found under ``pyatspm_root``.
    """
    path = pyatspm_root / "src" / "atspm" / "analysis" / "decoders.py"
    if not path.is_file():
        raise SystemExit(
            f"ERROR: pyatspm decoder not found at {path}. "
            f"Pass --pyatspm <checkout root>."
        )
    if "pandas" not in sys.modules:
        # decoders.py imports pandas only to build the returned DataFrame;
        # the pure helpers this tool calls never touch it.
        stub = types.ModuleType("pandas")
        stub.DataFrame = object
        sys.modules["pandas"] = stub
    spec = importlib.util.spec_from_file_location("_atspm_decoders", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _boundary_timestamp(name: str, tz: ZoneInfo) -> float:
    """Unix epoch of the clock boundary encoded in a .datZ filename.

    Args:
        name: Filename, e.g. ``ECON_10.37.23.200_2026_07_31_1800.datZ``.
        tz: Site timezone the boundary is expressed in.

    Returns:
        Epoch seconds of that local wall-clock instant.

    Raises:
        SystemExit: If the filename carries no parseable boundary.
    """
    m = _NAME_RE.search(name)
    if not m:
        raise SystemExit(
            f"ERROR: cannot read a clock boundary from '{name}' "
            f"(expected ..._YYYY_MM_DD_HHMM.datZ)."
        )
    year, month, day, hour, minute = (int(g) for g in m.groups())
    return datetime(year, month, day, hour, minute, tzinfo=tz).timestamp()


def _decode_one(
    raw: bytes, name: str, tz: ZoneInfo, dec
) -> List[Tuple[float, int, int]]:
    """Decode one .datZ payload into (timestamp, event_code, parameter) rows.

    Mirrors ``parse_datz_bytes`` step for step, minus the DataFrame wrapper.

    Args:
        raw: Compressed file bytes.
        name: Filename (for the clock boundary and error messages).
        tz: Site timezone.
        dec: The loaded pyatspm decoders module.

    Returns:
        Event tuples in file order.

    Raises:
        SystemExit: If the payload is not a decodable datZ file.
    """
    try:
        content = zlib.decompress(raw)
    except zlib.error as exc:
        raise SystemExit(f"ERROR: {name}: decompression failed: {exc}")

    marker = b"Phases in use:"
    pos = content.find(marker)
    if pos == -1:
        raise SystemExit(f"ERROR: {name}: 'Phases in use:' marker not found.")
    newline = content.find(b"\n", pos)
    if newline == -1:
        raise SystemExit(f"ERROR: {name}: no newline after the marker.")

    base = _boundary_timestamp(name, tz)
    header = dec._extract_header_fields(content)
    if header is not None:
        base += header["second_offset"]
    else:
        print(f"WARN: {name}: no header line — using the filename boundary "
              f"unshifted (events may be up to 1 s early).")
    return dec._parse_binary_payload(content[newline + 1:], base)


def _iter_inputs(paths: Iterable[str]) -> Iterable[Tuple[str, bytes]]:
    """Yield (name, raw bytes) for each .datZ, expanding .zip archives."""
    for p in paths:
        path = Path(p)
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as zf:
                for info in sorted(zf.infolist(), key=lambda i: i.filename):
                    if info.filename.lower().endswith(".datz"):
                        yield Path(info.filename).name, zf.read(info)
        else:
            yield path.name, path.read_bytes()


def _parse_when(text: Optional[str], tz: ZoneInfo) -> Optional[float]:
    """Parse an ISO-ish local datetime into epoch seconds, or None."""
    if not text:
        return None
    return datetime.fromisoformat(text).replace(tzinfo=tz).timestamp()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Decode Econolite .datZ controller logs to an event CSV "
                    "(timestamp,event_code,parameter) using pyatspm's decoder."
    )
    ap.add_argument("files", nargs="+",
                    help=".datZ files, and/or .zip archives containing them")
    ap.add_argument("-o", "--out", required=True, help="output CSV path")
    ap.add_argument("--tz", default="America/Boise",
                    help="timezone the filename clock boundaries are in "
                         "(default America/Boise)")
    ap.add_argument("--pyatspm", default=str(_DEFAULT_PYATSPM),
                    help=f"pyatspm checkout root (default {_DEFAULT_PYATSPM})")
    ap.add_argument("--detectors-only", action="store_true",
                    help="keep only ATSPM detector codes 82 (ON) / 81 (OFF)")
    ap.add_argument("--start", help="drop events before this local time "
                                    "(e.g. 2026-07-31T18:00)")
    ap.add_argument("--end", help="drop events after this local time")
    args = ap.parse_args()

    tz = ZoneInfo(args.tz)
    dec = _load_decoders(Path(args.pyatspm))
    lo = _parse_when(args.start, tz)
    hi = _parse_when(args.end, tz)

    records: List[Tuple[float, int, int]] = []
    files = 0
    for name, raw in _iter_inputs(args.files):
        rows = _decode_one(raw, name, tz, dec)
        files += 1
        print(f"  {name}: {len(rows)} events")
        records.extend(rows)

    records.sort(key=lambda r: r[0])
    kept = []
    for ts, code, param in records:
        if lo is not None and ts < lo:
            continue
        if hi is not None and ts > hi:
            continue
        if args.detectors_only and str(code) not in (EVENT_ON, EVENT_OFF):
            continue
        kept.append((ts, code, param))

    out = Path(args.out)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "event_code", "parameter"])
        for ts, code, param in kept:
            writer.writerow([f"{ts:.1f}", code, param])

    if not kept:
        print("WARN: no events survived the filters — check --start/--end/--tz.")
        print(f"\nWrote {out} (0 events from {files} file(s)).")
        return 1

    span_lo = datetime.fromtimestamp(kept[0][0], tz)
    span_hi = datetime.fromtimestamp(kept[-1][0], tz)
    print(f"\nWrote {out}: {len(kept)} events from {files} file(s), "
          f"{span_lo:%Y-%m-%d %H:%M:%S} – {span_hi:%H:%M:%S} {args.tz}.")
    print("Next step: correlate against an __capture_ntcip.py capture over the "
          "same window with __correlate_channels.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
