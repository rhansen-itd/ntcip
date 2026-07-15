#!/usr/bin/env python3
"""record_clip.py — one-shot manual recorder for the remux video buffer.

A clean CLI entry point for capturing a single clip through the production
``remux`` backend (``remux_video_buffer.VideoBufferManager``) — the same code
path the live discrepancy engine drives, but fired manually for smoke-testing a
camera or the buffer itself. Standalone debug/manual tool (lives in
``video_engine/tools/``, ``print()`` allowed); not imported by production code.

Three modes:

  * **Now** (default): warm the pre-roll buffer, then record ``--seconds`` and
    exit. Use this when you just want "a clip of length N, starting about now."
  * **Scheduled** (``--at HH:MM[:SS]``): wait until that wall-clock time, then
    record ``--seconds`` — e.g. ``--at 14:00 --seconds 180`` records the
    14:00:00–14:03:00 window.
  * **Serve** (``--serve``): open the stream and keep the pre-roll buffer
    running until Ctrl-C, dropping *no* triggers itself — you drop them as
    needed (e.g. with ``drop_trigger.py``) to record one or many clips. This is
    the clean replacement for the old ``__record.py`` harness. ``--at`` and
    ``--seconds`` are ignored in this mode; ``--pre-roll`` still sizes the ring.

Why scheduling is tool-side, not in the trigger: the trigger's
``event_timestamp`` is a *retrospective* anchor into the small RAM pre-roll ring
(~``pre_roll + keyframe_margin`` seconds), not a scheduler — a future timestamp
would just make the manager start from the latest keyframe immediately. So this
tool holds off dropping the trigger until the target time arrives, opening the
stream a few seconds beforehand to prime the pre-roll.

Backend note: this intentionally uses ``remux`` only. The ``full`` (CFR) backend
is RAM-unbounded and central/server-only; if you ever need it, select it via an
intersection config's ``video_backend`` key through ``system_runner.py`` — not
here.

Usage:
    # record ~30s starting now
    python tools/record_clip.py "rtsp://user:pass@host:554/stream" --seconds 30

    # record the 14:00:00–14:03:00 window (local time)
    python tools/record_clip.py "rtsp://…" --at 14:00 --seconds 180 --pre-roll 0

    # writes ./completed_videos/<id>_<cam>_<ts>.ts
"""

import argparse
import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# This tool lives in video_engine/tools/; put video_engine/ on sys.path so the
# backend import below resolves regardless of the working directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from remux_video_buffer import VideoBufferConfig, VideoBufferManager


def _parse_start_time(text: str) -> float:
    """Parse a ``--at`` value into a POSIX timestamp in the machine's local time.

    Accepts a full datetime (``YYYY-MM-DD HH:MM[:SS]``, ``T`` separator ok) or a
    time-only value (``HH:MM`` / ``HH:MM:SS``) which is taken as **today**, local
    time (matching the "monitoring machine's own clock" convention).

    Args:
        text: The raw ``--at`` argument.

    Returns:
        The target start time as a Unix timestamp.

    Raises:
        ValueError: If the value matches none of the accepted formats.
    """
    text = text.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(text, fmt).timestamp()  # naive → local
        except ValueError:
            pass
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t = datetime.strptime(text, fmt).time()
            return datetime.combine(datetime.now().date(), t).timestamp()
        except ValueError:
            pass
    raise ValueError(
        f"unrecognized --at time {text!r}; use HH:MM[:SS] or 'YYYY-MM-DD HH:MM:SS'"
    )


def _drop_start_trigger(
    trigger_dir: str, camera: str, pre_roll_sec: float, seconds: float
) -> str:
    """Atomically write a ``start`` trigger into the Hot Folder.

    Args:
        trigger_dir: Spool directory the manager polls.
        camera: Camera id to record (must match a configured stream).
        pre_roll_sec: Pre-roll to pull from the RAM buffer, in seconds.
        seconds: Clip length; the remux backend auto-stops after this
            (``max_duration_sec``).

    Returns:
        The generated ``trigger_id`` (hex), whose first 8 chars prefix the
        output filename.
    """
    trigger_id = uuid.uuid4().hex
    payload = {
        "trigger_id": trigger_id,
        "action": "start",
        "event_timestamp": datetime.now(timezone.utc).timestamp(),
        "reason": "manual",
        "intersection_id": "manual",
        "cameras": [camera],
        "pre_roll_sec": pre_roll_sec,
        "post_roll_sec": 0,
        "max_duration_sec": seconds,   # remux auto-stops the clip after this
        "metadata": {"tool": "record_clip"},
    }
    Path(trigger_dir).mkdir(parents=True, exist_ok=True)
    tmp = Path(trigger_dir) / f"trigger_manual_{trigger_id[:8]}.tmp"
    final = tmp.with_suffix(".json")
    tmp.write_text(json.dumps(payload, indent=2))
    os.rename(tmp, final)   # atomic: a reader never sees a partial file
    return trigger_id


def _sleep_until(target_ts: float, label: str) -> None:
    """Sleep until ``target_ts`` (no-op if already past), printing a heads-up."""
    remaining = target_ts - time.time()
    if remaining <= 0:
        return
    when = datetime.fromtimestamp(target_ts).strftime("%H:%M:%S")
    print(f"[record] {label} at {when} (in {remaining:.0f}s)...")
    time.sleep(remaining)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("url", help="RTSP/HTTP stream URL to record")
    ap.add_argument("--serve", action="store_true",
                    help="keep the stream buffering until Ctrl-C and drop no "
                         "triggers (record via drop_trigger.py); replaces __record.py")
    ap.add_argument("-t", "--seconds", type=float, default=30.0,
                    help="clip length to record, in seconds (default 30)")
    ap.add_argument("--at", "--start-time", dest="at", metavar="TIME",
                    help="wall-clock start time: HH:MM[:SS] today, or "
                         "'YYYY-MM-DD HH:MM:SS' (local time). Default: start now.")
    ap.add_argument("--pre-roll", type=float, default=5.0,
                    help="pre-roll seconds pulled from the RAM buffer (default 5). "
                         "With --at, use 0 to start exactly at the given time.")
    ap.add_argument("--camera", default="cam1",
                    help="camera id label for the stream (default cam1)")
    ap.add_argument("--output-dir", default="./completed_videos",
                    help="directory for the finished clip (default ./completed_videos)")
    ap.add_argument("--trigger-dir", default="./trigger_queue",
                    help="Hot Folder spool dir (default ./trigger_queue)")
    args = ap.parse_args()

    if args.seconds <= 0:
        ap.error("--seconds must be positive")
    if args.pre_roll < 0:
        ap.error("--pre-roll cannot be negative")

    cfg = VideoBufferConfig(
        streams={args.camera: args.url},
        trigger_dir=args.trigger_dir,
        output_dir=args.output_dir,
        pre_roll_sec=args.pre_roll,
        backend="remux",
    )

    # -- serve mode: run the buffer, drop no triggers, until Ctrl-C ------------
    if args.serve:
        manager = VideoBufferManager(cfg)
        runner = threading.Thread(
            target=manager.start, name="video-manager", daemon=True
        )
        runner.start()
        print(f"[serve] buffering '{args.camera}' <- {args.url}")
        print(f"[serve] pre-roll ring ~{args.pre_roll + cfg.keyframe_margin_sec:.0f}s; "
              f"drop triggers into {args.trigger_dir}/ (e.g. tools/drop_trigger.py),")
        print(f"[serve] clips land in {args.output_dir}/.  Ctrl-C to stop.")
        try:
            while runner.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n[serve] shutting down (finalizing any active clips)...")
        manager.stop()
        runner.join(timeout=10.0)
        return 0

    # Lead time needed to open the stream and prime the pre-roll ring before the
    # clip's start instant. One keyframe interval isn't known up front, so budget
    # a comfortable margin over the configured keyframe window.
    lead = args.pre_roll + cfg.keyframe_margin_sec + 3.0

    now = time.time()
    if args.at:
        try:
            start_ts = _parse_start_time(args.at)
        except ValueError as exc:
            ap.error(str(exc))
        if start_ts < now - 2.0:
            ap.error(
                f"--at {args.at!r} resolves to "
                f"{datetime.fromtimestamp(start_ts):%Y-%m-%d %H:%M:%S}, which is in "
                f"the past — the pre-roll buffer only reaches back a few seconds, "
                f"so a past window can't be recorded. Give a future time."
            )
        end_str = datetime.fromtimestamp(start_ts + args.seconds).strftime("%H:%M:%S")
        start_str = datetime.fromtimestamp(start_ts).strftime("%H:%M:%S")
        print(f"[record] scheduled: {start_str}–{end_str} "
              f"({args.seconds:.0f}s, pre-roll {args.pre_roll:.0f}s)")
    else:
        start_ts = now + lead   # start "now", after a warmup to prime the pre-roll

    manager = VideoBufferManager(cfg)

    # Idle-wait (nothing open) until it's time to open the stream, so a far-future
    # schedule doesn't hold an RTSP connection open for hours.
    _sleep_until(start_ts - lead, "opening stream")

    # manager.start() runs the Hot Folder poll loop and blocks, so drive it on a
    # background thread and control it from here.
    runner = threading.Thread(target=manager.start, name="video-manager", daemon=True)
    runner.start()

    # Remaining warmup: let the pre-roll ring fill up to the start instant.
    _sleep_until(start_ts, "recording starts")

    trigger_id = _drop_start_trigger(
        args.trigger_dir, args.camera, args.pre_roll, args.seconds
    )
    print(f"[record] recording ~{args.seconds:.0f}s (trigger {trigger_id[:8]})...")

    # Wait out the clip: poll latency to pick up the trigger + the auto-stop
    # duration + a finalize margin for the container to close.
    time.sleep(args.seconds + cfg.poll_interval_sec + 3.0)

    print("[record] finalizing...")
    manager.stop()
    runner.join(timeout=10.0)

    clips = sorted(
        Path(args.output_dir).glob(f"{trigger_id[:8]}_*{cfg.container_ext}")
    )
    if clips:
        clip = clips[-1]
        print(f"[record] done: {clip}  ({clip.stat().st_size / 1e6:.1f} MB)")
        return 0

    print(
        "[record] no clip produced — check the URL / camera reachability "
        "(see warnings above).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
