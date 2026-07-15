#!/usr/bin/env python3
"""Capture a faithful RTSP sample + a jitter/GOP profile, for later
verification of the video-buffer rewrite (ROADMAP Item 1).

This is a standalone debug/fixture tool (``__``-prefixed, print() allowed —
same class as __trigger.py / __record.py), not a production module. It does
NOT decode or re-encode: it stream-copies the RTSP feed to a container with
the camera's original packet timestamps intact, then profiles the video
packets so we can see the real jitter we're designing against — all without
a stream on the dev box.

Two outputs:
  1. <out>            — faithful stream-copy (default .ts, kill-safe).
  2. <out>.packets.jsonl + a printed summary — per-video-packet pts/dts/
     keyframe/size, plus PTS-delta (jitter) and keyframe-interval (GOP) stats.

Why .ts by default: MPEG-TS survives a Ctrl-C / dropped connection without
a corrupt-container (no trailing moov atom like .mp4). Pass --out foo.mkv if
you prefer Matroska (also VFR-safe); avoid .mp4 for interrupted captures.

Requires the ``ffmpeg`` and ``ffprobe`` binaries on PATH (no Python deps).
The real buffer will use PyAV; this tool stays dependency-light so it runs on
whatever box can actually reach a camera.

Usage:
    python __capture_rtsp.py rtsp://user:pass@host:554/stream \\
        --seconds 180 --out sample.ts --transport tcp

Then hand me sample.ts + sample.ts.packets.jsonl and I can cut both short
(~15-30s) and long (~3-5min) test clips from the one recording and replay
them through the new buffer.
"""

import argparse
import json
import shutil
import signal
import statistics
import subprocess
import sys


def _require(binary: str) -> None:
    if shutil.which(binary) is None:
        sys.exit(
            f"error: '{binary}' not found on PATH.\n"
            f"  Install ffmpeg (which provides both ffmpeg and ffprobe):\n"
            f"    Debian/Ubuntu:  sudo apt install ffmpeg\n"
            f"    macOS (brew):   brew install ffmpeg\n"
            f"    Windows:        winget install Gyan.FFmpeg"
        )


def capture(url: str, out: str, seconds: int, transport: str) -> None:
    """Stream-copy `url` to `out` for `seconds`, preserving original packets."""
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-rtsp_transport", transport,   # tcp is far more reliable for capture
        "-rw_timeout", "10000000",      # 10s I/O timeout so a bad URL fails fast
        "-i", url,
        "-t", str(seconds),
        "-c", "copy",                   # NO re-encode: keep the camera's timing
        "-map", "0",                    # all streams (video + any audio)
        "-y",
        out,
    ]
    print(f"[capture] {' '.join(cmd)}")
    print(f"[capture] recording ~{seconds}s to {out} (Ctrl-C to stop early)...")
    # Let ffmpeg own SIGINT so Ctrl-C finalizes the file cleanly.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    proc = subprocess.run(cmd)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    if proc.returncode not in (0, 255):  # 255 = interrupted, file still usable
        print(f"[capture] ffmpeg exited {proc.returncode}; profiling what we got.")


def profile(out: str) -> None:
    """ffprobe the video packets, write a JSONL log, print jitter + GOP stats."""
    probe = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "packet=pts_time,dts_time,flags,size",
            "-of", "json",
            out,
        ],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        print(f"[profile] ffprobe failed: {probe.stderr.strip()}")
        return

    packets = json.loads(probe.stdout or "{}").get("packets", [])
    if not packets:
        print("[profile] no video packets found — was anything captured?")
        return

    log_path = out + ".packets.jsonl"
    pts, keyframe_idx, total_bytes = [], [], 0
    with open(log_path, "w") as fh:
        for i, p in enumerate(packets):
            t = p.get("pts_time")
            if t in (None, "N/A"):
                t = p.get("dts_time")
            t = None if t in (None, "N/A") else float(t)
            is_key = "K" in (p.get("flags") or "")
            size = int(p.get("size") or 0)
            total_bytes += size
            if t is not None:
                pts.append(t)
            if is_key:
                keyframe_idx.append(i)
            fh.write(json.dumps({
                "i": i, "pts_time": t, "keyframe": is_key, "size": size,
            }) + "\n")

    n = len(pts)
    print(f"\n[profile] wrote per-packet log: {log_path}")
    print(f"[profile] video packets: {len(packets)}  "
          f"(with usable timestamps: {n})")

    if n >= 2:
        span = pts[-1] - pts[0]
        deltas = [b - a for a, b in zip(pts, pts[1:]) if b > a]
        nominal_fps = (n - 1) / span if span > 0 else float("nan")
        print(f"[profile] stream PTS span (true clip length): {span:.3f} s")
        print(f"[profile] mean frame rate (n-1)/span: {nominal_fps:.4f} fps")
        if deltas:
            ms = [d * 1000 for d in deltas]
            jitter = statistics.pstdev(ms) if len(ms) > 1 else 0.0
            print("[profile] inter-frame PTS delta (ms): "
                  f"min={min(ms):.1f} median={statistics.median(ms):.1f} "
                  f"mean={statistics.mean(ms):.1f} max={max(ms):.1f} "
                  f"stdev={jitter:.1f}")
            print("[profile]   ^ stdev/max well above median => real jitter; "
                  "a fixed-fps writer drifts by exactly this much over a clip.")

    if len(keyframe_idx) >= 2:
        gaps = [b - a for a, b in zip(keyframe_idx, keyframe_idx[1:])]
        gop_frames = statistics.mean(gaps)
        print(f"[profile] keyframes: {len(keyframe_idx)}  "
              f"mean GOP: {gop_frames:.1f} frames")
        if n >= 2 and span > 0:
            print(f"[profile]   ~{gop_frames / nominal_fps:.2f} s between keyframes "
                  "=> worst-case pre-roll snap-back to start a copyable clip.")
    else:
        print("[profile] <2 keyframes seen — capture longer to gauge GOP size.")

    print(f"[profile] total video payload: {total_bytes / 1e6:.1f} MB")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("url", help="RTSP URL, e.g. rtsp://user:pass@host:554/stream")
    ap.add_argument("--out", default="sample.ts",
                    help="output file (default sample.ts; .ts/.mkv are kill-safe)")
    ap.add_argument("--seconds", type=int, default=180,
                    help="capture duration (default 180 — enough for both a "
                         "short and a long test clip)")
    ap.add_argument("--transport", default="tcp", choices=["tcp", "udp"],
                    help="RTSP transport (default tcp)")
    ap.add_argument("--profile-only", metavar="FILE",
                    help="skip capture; just profile an existing recording")
    args = ap.parse_args()

    if args.profile_only:
        _require("ffprobe")
        profile(args.profile_only)
        return

    _require("ffmpeg")
    _require("ffprobe")
    capture(args.url, args.out, args.seconds, args.transport)
    profile(args.out)


if __name__ == "__main__":
    main()
