#!/usr/bin/env python3
"""grab_calibration_still.py — save one camera frame as a JPEG.

Step one of the overlay calibration workflow: pull a still off the camera, hand
it to pyatspm's ``atspm video-calibrate-shapes`` to draw loops and stopbars on,
and drop the resulting CSV at ``overlay.shapes_csv``. The still doubles as the
``overlay.image_path`` background for ``background: "file"``.

The frame is fetched through the overlay's own :class:`RtspMjpegSource`, not a
private copy of the PyAV plumbing. That is deliberate: a successful grab is
also proof that the live overlay path can reach this camera, decode it, and
encode a JPEG from it — one fewer thing to debug later. Deploy-time tool at the
repo root; nothing imports it, so ``print()`` is fine.

The calibration CSV records the resolution it was drawn at, and the overlay
canvas is sized from it, so **calibrate against a still from the same camera
profile you will watch live**. The resolution is printed here for that reason.

Usage:
    # straight from a URL
    python tools/grab_calibration_still.py rtsp://user:pw@host/stream

    # or resolve the URL out of the video engine's intersection config
    python tools/grab_calibration_still.py -i 201 --camera fisheye

    # let auto-exposure settle, write where config.json expects it
    python tools/grab_calibration_still.py -i 201 --settle 3 \
        -o overlay/201_fisheye.jpg
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

# Deploy-time tool: bootstrap the repo root so ntcip_monitor is importable
# regardless of the working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ntcip_monitor.ui.overlay.source import RtspMjpegSource, av  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sync_ui_config import (  # noqa: E402
    load_json,
    load_intersections,
    mask_url,
    select_camera,
    select_intersection,
)


def resolve_url(args: argparse.Namespace) -> tuple:
    """Work out which camera URL to grab from.

    Args:
        args: Parsed command line.

    Returns:
        tuple: ``(url, label)`` where the label names the source for the output
        filename default.

    Raises:
        SystemExit: If neither a URL nor a resolvable intersection was given.
    """
    if args.url:
        return (args.url, "camera")

    if not args.intersection:
        sys.exit("error: give a camera URL, or -i/--intersection to look one "
                 "up in " + str(args.intersections))

    data = load_intersections(args.intersections)
    intersection_id, section = select_intersection(data, args.intersection)
    camera_id, url = select_camera(section, args.camera)
    if not url:
        sys.exit(f"error: intersection {intersection_id} has no camera URL")
    return (url, f"{intersection_id}_{camera_id}")


def main() -> int:
    """Grab the still.

    Returns:
        int: Process exit status.
    """
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("url", nargs="?",
                    help="camera URL (rtsp://... or http://...); omit to use "
                         "-i/--intersection")
    ap.add_argument("-i", "--intersection", metavar="ID",
                    help="look the URL up in the video engine's config")
    ap.add_argument("--intersections", type=Path,
                    default=Path("video_engine/intersections"),
                    help="intersection config to look in "
                         "(default: video_engine/intersections)")
    ap.add_argument("--camera", metavar="ID",
                    help="camera to grab from (default: the only one defined)")
    ap.add_argument("-o", "--output", type=Path,
                    help="output JPEG (default: overlay/<intersection>_<cam>.jpg)")
    ap.add_argument("--settle", type=float, default=1.0,
                    help="seconds to keep decoding before saving a frame, so "
                         "auto-exposure settles (default 1)")
    ap.add_argument("--quality", type=int, default=3,
                    help="JPEG quality, 1 best to 31 worst (default 3 — a "
                         "calibration still should be sharp)")
    ap.add_argument("--rtsp-transport", default="tcp", choices=["tcp", "udp"],
                    help="RTSP transport (default: tcp)")
    ap.add_argument("--timeout", type=float, default=20.0,
                    help="seconds to wait for the first frame (default 20)")
    ap.add_argument("--force", action="store_true",
                    help="overwrite the output file if it exists")
    args = ap.parse_args()

    if av is None:
        sys.exit("error: PyAV is not installed (pip install -r requirements.txt)")

    url, label = resolve_url(args)
    output: Optional[Path] = args.output or Path("overlay") / f"{label}.jpg"
    if output.exists() and not args.force:
        sys.exit(f"error: {output} exists; pass --force to overwrite")

    print(f"Camera: {mask_url(url)}")
    source = RtspMjpegSource(
        url,
        quality=args.quality,
        rtsp_transport=args.rtsp_transport,
        first_frame_timeout_sec=args.timeout,
        idle_grace_sec=0.0,
    )
    try:
        image = source.get_image()
        if image is None:
            sys.exit(f"error: no frame within {args.timeout:.0f}s — check the "
                     "URL, credentials, and that the camera is reachable")
        if args.settle > 0:
            time.sleep(args.settle)
            image = source.get_image() or image
        data, _content_type = image
        size = source.stats().get("resolution")
    finally:
        source.close()

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)

    print(f"Wrote {output} ({len(data):,} bytes"
          f"{f', {size[0]}x{size[1]}' if size else ''})")
    print("\nNext: calibrate shapes against this still with pyatspm, e.g.")
    print(f"  atspm video-calibrate-shapes --targetid <id> --camera {label} "
          f"--video {output}")
    print("  (--video takes any file whose first frame OpenCV can read; if a "
          "JPEG won't open,\n   record a few seconds with "
          "video_engine/tools/__capture_rtsp.py and calibrate against that)")
    print("Then copy the CSV it writes to config.json's overlay.shapes_csv.")
    print("Keep the camera profile unchanged — the overlay canvas is sized "
          "from the resolution recorded in that CSV.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
