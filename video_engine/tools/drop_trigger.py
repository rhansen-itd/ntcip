#!/usr/bin/env python3
"""drop_trigger.py — write a Hot Folder trigger to drive the video buffer.

Clean CLI replacement for the old hardcoded ``__trigger.py``. Writes one trigger
file (atomically: ``*.tmp`` then ``os.rename``) into the spool directory a
running ``VideoBufferManager`` polls — e.g. one started by
``record_clip.py --serve`` or by ``system_runner.py``. Standalone manual tool
(lives in ``video_engine/tools/``, ``print()`` allowed); not imported by
production code.

Actions:
  start   Begin a clip (default). Auto-generates a ``trigger_id`` (printed);
          pass it back to ``stop``/``extend`` to target this clip.
  stop    Finalize the in-progress clip named by ``--trigger-id``.
  extend  Push that clip's max-duration deadline out by ``--seconds``.

The payload matches the trigger schema documented in ``config_manager.py`` /
CLAUDE.md ("Hot Folder pattern").

Usage:
    # start a 60s clip on cam1
    python tools/drop_trigger.py start --seconds 60 --camera cam1
    # ... prints trigger_id abc12345; later, against that id:
    python tools/drop_trigger.py stop   --trigger-id abc12345
    python tools/drop_trigger.py extend --trigger-id abc12345 --seconds 120
    # every configured camera:
    python tools/drop_trigger.py start --all --seconds 30
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("action", nargs="?", default="start",
                    choices=["start", "stop", "extend"],
                    help="trigger action (default: start)")
    ap.add_argument("--trigger-id",
                    help="target clip's id (required for stop/extend)")
    ap.add_argument("-t", "--seconds", type=float, default=300.0,
                    help="max clip duration for start/extend, in seconds (default 300)")
    ap.add_argument("--camera", action="append", metavar="ID",
                    help="camera id to record; repeatable (default: cam1)")
    ap.add_argument("--all", action="store_true",
                    help="target every configured camera (cameras=['all'])")
    ap.add_argument("--pre-roll", type=float, default=5.0,
                    help="pre-roll seconds for start (default 5)")
    ap.add_argument("--post-roll", type=float, default=0.0,
                    help="post-roll seconds for start (default 0)")
    ap.add_argument("--reason", default="manual",
                    help="reason string recorded in the trigger (default: manual)")
    ap.add_argument("--intersection-id", default="manual",
                    help="intersection id recorded in the trigger (default: manual)")
    ap.add_argument("--trigger-dir", default="./trigger_queue",
                    help="Hot Folder spool dir (default ./trigger_queue)")
    args = ap.parse_args()

    if args.action in ("stop", "extend") and not args.trigger_id:
        ap.error(f"--trigger-id is required for '{args.action}'")

    # stop/extend target an existing clip by id; start mints a fresh one.
    trigger_id = args.trigger_id or uuid.uuid4().hex
    cameras = ["all"] if args.all else (args.camera or ["cam1"])

    payload = {
        "trigger_id": trigger_id,
        "action": args.action,
        "event_timestamp": datetime.now(timezone.utc).timestamp(),
        "reason": args.reason,
        "intersection_id": args.intersection_id,
        "cameras": cameras,
        "pre_roll_sec": args.pre_roll,
        "post_roll_sec": args.post_roll,
        "max_duration_sec": args.seconds,
        "metadata": {"tool": "drop_trigger"},
    }

    trigger_dir = Path(args.trigger_dir)
    trigger_dir.mkdir(parents=True, exist_ok=True)
    # Filename is independent of trigger_id (so stop/extend never collide with the
    # start file) and time-sortable (the reader polls oldest-first).
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    tmp = trigger_dir / f"trigger_manual_{stamp}_{uuid.uuid4().hex[:8]}.tmp"
    final = tmp.with_suffix(".json")
    tmp.write_text(json.dumps(payload, indent=2))
    os.rename(tmp, final)   # atomic: a reader never sees a partial file

    print(f"[trigger] {args.action} -> {final}")
    print(f"[trigger] trigger_id: {trigger_id}")
    if args.action == "start":
        print(f"[trigger] stop/extend this clip with:  --trigger-id {trigger_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
