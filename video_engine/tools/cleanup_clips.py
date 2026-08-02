#!/usr/bin/env python3
"""cleanup_clips.py — delete clips wholly contained inside another clip.

Manual front end for ``video_engine/video_cleanup.py``, which the video buffer
also runs on a timer.  Use it to inspect what the automatic sweep would do, or
to clean up a directory of clips from a finished run.

**Dry run by default** — nothing is deleted and no log is rewritten until
``--apply`` is passed (same convention as ``tools/sync_ui_config.py``).

    # what would go?
    python3 video_engine/tools/cleanup_clips.py --output-dir completed_videos

    # do it
    python3 video_engine/tools/cleanup_clips.py --output-dir completed_videos --apply

A clip is deleted only when another clip from the **same camera** covers its
whole wall-clock span (within ``--tolerance``); every reference to it in
``discrepancies_log.csv`` is repointed at the surviving clip first, and each
deletion is recorded in ``video_cleanup_log.csv``.  See ``video_cleanup.py``'s
module docstring for how a clip's span is recovered and why the containment
test is deliberately conservative.

Note ``--min-age`` defaults to 0 here, not to the buffer's 60 s: when you run
this by hand nothing is recording.  Keep the default only if that is true.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the other tools that import the package's modules).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from video_cleanup import ClipCleaner, format_result  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser.

    Returns:
        The configured :class:`argparse.ArgumentParser`.
    """
    parser = argparse.ArgumentParser(
        description="Delete clips wholly contained in another clip (dry run by "
                    "default).",
    )
    parser.add_argument(
        "--output-dir",
        default="./completed_videos",
        help="Directory holding the clips and discrepancies_log.csv.",
    )
    parser.add_argument(
        "--ext",
        default=".ts",
        help="Clip container extension (default: .ts).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.5,
        help="Seconds of containment slack, so two clips of the same moment "
             "that differ by poll latency still compare as duplicates "
             "(default: 0.5).",
    )
    parser.add_argument(
        "--min-age",
        type=float,
        default=0.0,
        help="Ignore clips modified more recently than this many seconds. "
             "Raise it if the video buffer is running (default: 0).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete and rewrite. Without it, nothing is changed.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log skipped clips and probe failures.",
    )
    return parser


def main() -> int:
    """Run one sweep and print the plan or the outcome.

    Returns:
        Process exit code — non-zero if the sweep reported errors.
    """
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(message)s",
    )

    output_dir = Path(args.output_dir)
    if not output_dir.is_dir():
        print(f"No such directory: {output_dir}", file=sys.stderr)
        return 2

    cleaner = ClipCleaner(
        output_dir=output_dir,
        container_ext=args.ext,
        tolerance_sec=args.tolerance,
        min_age_sec=args.min_age,
    )
    result = cleaner.sweep(apply=args.apply)
    for line in format_result(result):
        print(line)
    return 1 if result.errors else 0


if __name__ == "__main__":
    sys.exit(main())
