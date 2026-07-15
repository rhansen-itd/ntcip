#!/usr/bin/env python3
"""
__probe_adversarial.py — Adversarial edge-case probes for the remux buffer.

Debug tool (``__``-prefixed, ``print()`` allowed — not a production module).
Complements ``__replay_verify.py``: where that script checks the happy path
(length fidelity, keyframe start, RAM-boundedness), this one attacks the
plan-§4 edge cases (``VIDEO_BUFFER_REMUX_PLAN.md``) against the real capture
in ``video_engine/tests/fixtures/sample.ts``:

    A. Windowed clip at 1x on the real long-GOP stream (keyframe-aligned bound).
    B. Mid-clip **backward** PTS/DTS jump (self-concatenated TS) — must be
       clamped to one frame, all packets kept, output DTS monotonic.
    C. Mid-clip **forward** PTS gap (spliced 60s hole) — passed through by
       design (no frames arrived = real elapsed time); asserts DTS monotonic
       and reports the observed behavior.
    D. B-frames on real content (re-encode with ``-bf 2``) — fidelity, DTS
       monotonicity, ``pts >= dts``, full decode.
    E. Concurrent triggers under the semaphore — two clips correct, a third
       dropped at the cap.
    F. Source drop + reconnect mid-recording — clip spans the reconnect,
       PTS restart clamped, clean finalize, full decode.

First run green 2026-07-15 (Fable verification pass); results logged in
DESIGN_HISTORY.md.

Usage::

    python __probe_adversarial.py            # all probes (~2 min)
    python __probe_adversarial.py B C F      # a subset

Set ``PROBE_WORK=/some/dir`` to keep/reuse the derived fixtures (the
concatenated / re-encoded streams) between runs; default is a temp dir
removed on exit. Exits non-zero if any assertion fails.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

# This tool lives in video_engine/tools/; make both the tools dir (for the
# __replay_verify harness) and video_engine/ (for remux_video_buffer)
# importable regardless of the working directory.
_TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_TOOLS_DIR))
sys.path.insert(0, str(_TOOLS_DIR.parent))

import av

import __replay_verify as rv
from remux_video_buffer import VideoBufferConfig, VideoBufferManager

SAMPLE = str(_TOOLS_DIR.parent / "tests" / "fixtures" / "sample.ts")

FAIL: list = []


def check(cond: bool, label: str, detail: str = "") -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAIL.append(f"{label}: {detail}")


def dts_scan(path: str):
    """Return ``(monotonic_dts, pts_ge_dts, n_packets, max_fwd_gap_sec)``."""
    c = av.open(path)
    s = c.streams.video[0]
    tb = float(s.time_base)
    prev = None
    mono = True
    pge = True
    n = 0
    max_gap = 0.0
    for p in c.demux(s):
        if p.dts is None:
            continue
        n += 1
        if p.pts is not None and p.pts < p.dts:
            pge = False
        if prev is not None:
            if p.dts <= prev:
                mono = False
            else:
                max_gap = max(max_gap, (p.dts - prev) * tb)
        prev = p.dts
    c.close()
    return mono, pge, n, max_gap


def decode_all(path: str, limit=None):
    """Decode frames; return ``(n_decoded, error_str_or_None)``."""
    try:
        c = av.open(path)
        s = c.streams.video[0]
        n = 0
        for _ in c.decode(s):
            n += 1
            if limit and n >= limit:
                break
        c.close()
        return n, None
    except Exception as exc:  # noqa: BLE001 — probe tool, report anything
        return 0, str(exc)


# ---------------------------------------------------------------------------
def probe_windowed_real(work: Path) -> None:
    """A. Windowed clip @1x on the real capture — 6.2s GOP stresses keyframe seek."""
    print("\n== A. windowed[real @1x, GOP 6.2s] ==")
    pre_roll = 4.0
    gop_sec = 6.2
    h = rv.Harness(SAMPLE, work, speed=1.0, pre_roll_sec=pre_roll,
                   keyframe_margin_sec=8.0)
    h.start()
    time.sleep(pre_roll + 9.0)  # fill pre-roll + >1 GOP of headroom
    tid = uuid.uuid4().hex
    t_start = time.time()
    rv.write_trigger(h.trigger_dir, "start", tid, t_start, pre_roll_sec=pre_roll)
    time.sleep(6.0)
    t_stop = time.time()
    rv.write_trigger(h.trigger_dir, "stop", tid, t_stop)
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()
    if not clip:
        check(False, "windowed clip produced", "no output file")
        return
    span, _, _ = rv.pts_span(clip)
    dec, key = rv.first_frame_decodes(clip)
    expected = pre_roll + (t_stop - t_start)
    lo, hi = expected - 0.4, expected + gop_sec + 0.2 + 0.5
    print(f"  expected~{expected:.2f}s  span={span:.3f}s  window=[{lo:.2f},{hi:.2f}]")
    check(lo <= span <= hi, "windowed length tracks request (keyframe-aligned)",
          f"{span:.3f} in [{lo:.2f},{hi:.2f}]")
    check(dec and key, "first frame decodes and is keyframe")


def probe_backward_jump(work: Path) -> None:
    """B. Mid-clip backward PTS/DTS jump: self-concatenated TS."""
    print("\n== B. backward discontinuity[sample.ts + sample.ts] ==")
    double = work / "double.ts"
    if not double.exists():
        with open(double, "wb") as out:
            for _ in range(2):
                out.write(Path(SAMPLE).read_bytes())
    h = rv.Harness(str(double), work, speed=40.0, pre_roll_sec=10000.0,
                   keyframe_margin_sec=10000.0)
    h.start()
    if not h.wait_exhausted(timeout=120):
        check(False, "double source drained", "timed out")
        h.stop()
        return
    tid = uuid.uuid4().hex
    rv.write_trigger(h.trigger_dir, "start", tid, time.time(), pre_roll_sec=10000.0)
    time.sleep(0.8)
    rv.write_trigger(h.trigger_dir, "stop", tid, time.time())
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()
    if not clip:
        check(False, "backward-jump clip produced", "no output file")
        return
    span, n, _ = rv.pts_span(clip)
    mono, _, _, max_gap = dts_scan(clip)
    dec, key = rv.first_frame_decodes(clip)
    print(f"  out span={span:.3f}s ({n} pk)  dts_monotonic={mono}  max_fwd_gap={max_gap:.3f}s")
    check(abs(span - 360.11) <= 1.0, "clamped length ~= 2x source (gap clamped to 1 frame)",
          f"span={span:.3f}s vs ~360.11s")
    check(mono, "output DTS strictly monotonic across the jump")
    check(n == 3602, "all packets kept across the jump", f"n={n} (want 3602)")
    check(dec and key, "first frame decodes (keyframe)")


def probe_forward_gap(work: Path) -> None:
    """C. Mid-clip forward PTS gap (60s hole): preserved by design."""
    print("\n== C. forward gap[0-60s + 120-180s, copyts] ==")
    seg1, seg2, gap = work / "seg1.ts", work / "seg2.ts", work / "gap.ts"
    if not gap.exists():
        for (ss, to, out) in (("0", "60", seg1), ("120", "180", seg2)):
            subprocess.run(
                ["ffmpeg", "-y", "-copyts", "-ss", ss, "-to", to, "-i", SAMPLE,
                 "-c", "copy", "-muxdelay", "0", "-muxpreload", "0", str(out)],
                check=True, capture_output=True)
        with open(gap, "wb") as f:
            f.write(seg1.read_bytes())
            f.write(seg2.read_bytes())
    smono, _, sn, sgap = dts_scan(str(gap))
    print(f"  fixture: n={sn} dts_mono={smono} max_fwd_gap={sgap:.1f}s")
    h = rv.Harness(str(gap), work, speed=40.0, pre_roll_sec=10000.0,
                   keyframe_margin_sec=10000.0)
    h.start()
    if not h.wait_exhausted(timeout=60):
        check(False, "gap source drained", "timed out")
        h.stop()
        return
    tid = uuid.uuid4().hex
    rv.write_trigger(h.trigger_dir, "start", tid, time.time(), pre_roll_sec=10000.0)
    time.sleep(0.8)
    rv.write_trigger(h.trigger_dir, "stop", tid, time.time())
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()
    if not clip:
        check(False, "forward-gap clip produced", "no output file")
        return
    span, n, _ = rv.pts_span(clip)
    mono, _, _, max_gap = dts_scan(clip)
    print(f"  out span={span:.3f}s ({n} pk)  dts_monotonic={mono}  max_fwd_gap={max_gap:.3f}s")
    check(mono, "output DTS monotonic")
    check(max_gap > 30.0, "forward gap preserved (design: real elapsed time)",
          f"out max gap {max_gap:.1f}s")


def probe_bframe_real(work: Path) -> None:
    """D. B-frames on real content: re-encode of the real capture with -bf 2."""
    print("\n== D. bframe[real content, -bf 2] ==")
    bsrc = work / "bframe_real.ts"
    if not bsrc.exists():
        subprocess.run(
            ["ffmpeg", "-y", "-i", SAMPLE, "-c:v", "libx264", "-bf", "2",
             "-g", "62", "-preset", "veryfast", "-pix_fmt", "yuv420p", str(bsrc)],
            check=True, capture_output=True)
    c = av.open(str(bsrc))
    s = c.streams.video[0]
    nb = sum(1 for p in c.demux(s)
             if p.dts is not None and p.pts is not None and p.pts != p.dts)
    c.close()
    print(f"  fixture: {nb} packets with pts!=dts (B-frames present: {nb > 0})")
    src_span, src_n, fd = rv.pts_span(str(bsrc))
    h = rv.Harness(str(bsrc), work, speed=40.0, pre_roll_sec=10000.0,
                   keyframe_margin_sec=10000.0)
    h.start()
    if not h.wait_exhausted(timeout=60):
        check(False, "bframe source drained", "timed out")
        h.stop()
        return
    tid = uuid.uuid4().hex
    rv.write_trigger(h.trigger_dir, "start", tid, time.time(), pre_roll_sec=10000.0)
    time.sleep(0.8)
    rv.write_trigger(h.trigger_dir, "stop", tid, time.time())
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()
    if not clip:
        check(False, "bframe clip produced", "no output file")
        return
    span, n, _ = rv.pts_span(clip)
    mono, pge, _, _ = dts_scan(clip)
    dec, key = rv.first_frame_decodes(clip)
    ndec, derr = decode_all(clip)
    tol = max(2.5 * fd, 0.05)
    print(f"  src span={src_span:.4f}s ({src_n} pk)  out span={span:.4f}s ({n} pk)")
    check(abs(span - src_span) <= tol, "bframe length fidelity",
          f"|{span:.4f}-{src_span:.4f}| <= {tol:.4f}")
    check(mono, "output DTS strictly monotonic (B-frames)")
    check(pge, "pts >= dts invariant held")
    check(dec and key, "first frame decodes (keyframe)")
    check(derr is None and ndec >= src_n - 5, "full clip decodes",
          f"decoded {ndec} frames, err={derr}")


def probe_concurrent(work: Path) -> None:
    """E. Two concurrent triggers on one camera; third dropped at the cap."""
    print("\n== E. concurrent triggers[cap=2] ==")
    pre_roll = 2.0
    h = rv.Harness(SAMPLE, work, speed=1.0, pre_roll_sec=pre_roll,
                   keyframe_margin_sec=8.0)
    h.start()
    time.sleep(pre_roll + 8.0)
    t1, t2, t3 = uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex
    rv.write_trigger(h.trigger_dir, "start", t1, time.time(), pre_roll_sec=pre_roll)
    time.sleep(1.0)
    rv.write_trigger(h.trigger_dir, "start", t2, time.time(), pre_roll_sec=pre_roll)
    time.sleep(1.0)
    rv.write_trigger(h.trigger_dir, "start", t3, time.time(), pre_roll_sec=pre_roll)
    time.sleep(3.0)
    rv.write_trigger(h.trigger_dir, "stop", t1, time.time())
    rv.write_trigger(h.trigger_dir, "stop", t2, time.time())
    rv.write_trigger(h.trigger_dir, "stop", t3, time.time())
    h.wait_writers_done()
    clips = sorted(Path(h.output_dir).glob("*.ts"))
    h.stop()
    print(f"  clips: {[c.name for c in clips]}")
    check(len(clips) == 2, "exactly 2 clips (third trigger dropped at cap)",
          f"got {len(clips)}")
    for c in clips:
        span, n, _ = rv.pts_span(str(c))
        dec, key = rv.first_frame_decodes(str(c))
        mono, _, _, _ = dts_scan(str(c))
        print(f"  {c.name}: span={span:.2f}s pk={n}")
        check(dec and key and mono and span > 3.0,
              f"{c.name} valid (decodes, keyframe start, monotonic)")


class LoopingReplayBuffer(rv.PacedReplayStreamBuffer):
    """Replays the file N times with ``reconnect_on_eof=True`` to simulate an
    RTSP drop + reconnect (each reopen restarts PTS -> backward jump mid-clip)."""

    def __init__(self, *args, loops: int = 2, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.reconnect_on_eof = True  # base forces False; undo for this probe
        self._loops_left = loops

    def _open_container(self):
        if self._loops_left <= 0:
            self.exhausted.set()
            self._running = False
            raise OSError("no more loops")
        self._loops_left -= 1
        return super()._open_container()


def probe_reconnect(work: Path) -> None:
    """F. Source drop + reconnect mid-recording: clip must stay continuous."""
    print("\n== F. drop/reconnect mid-recording ==")
    bufs = []

    def factory(cam: str, url: str) -> LoopingReplayBuffer:
        b = LoopingReplayBuffer(camera_id=cam, url=url, pre_roll_sec=4.0,
                                keyframe_margin_sec=8.0, speed=30.0, loops=2)
        bufs.append(b)
        return b

    tdir = work / f"trig_{uuid.uuid4().hex[:6]}"
    odir = work / f"out_{uuid.uuid4().hex[:6]}"
    cfg = VideoBufferConfig(streams={"cam1": SAMPLE}, trigger_dir=str(tdir),
                            output_dir=str(odir), pre_roll_sec=4.0,
                            keyframe_margin_sec=8.0, poll_interval_sec=0.2,
                            min_free_disk_mb=1.0)
    mgr = VideoBufferManager(cfg, stream_buffer_factory=factory)
    th = threading.Thread(target=mgr.start, daemon=True)
    th.start()
    time.sleep(2.0)  # ~60s of stream at 30x
    tid = uuid.uuid4().hex
    rv.write_trigger(tdir, "start", tid, time.time(), pre_roll_sec=2.0,
                     max_duration_sec=9999.0)
    # Pass 1 ends at ~6s wall (180s/30), reconnect ~1s, pass 2 runs ~6s more;
    # stop mid-pass-2 so the reconnect landed mid-recording.
    time.sleep(9.0)
    rv.write_trigger(tdir, "stop", tid, time.time())
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and (mgr._active_writers or mgr._draining):  # noqa: SLF001
        time.sleep(0.05)
    clips = sorted(odir.glob("*.ts"))
    mgr.stop()
    if not clips:
        check(False, "reconnect clip produced", "no output file")
        return
    clip = str(clips[-1])
    span, n, _ = rv.pts_span(clip)
    mono, _, _, max_gap = dts_scan(clip)
    dec, key = rv.first_frame_decodes(clip)
    ndec, derr = decode_all(clip)
    print(f"  span={span:.2f}s pk={n} dts_mono={mono} max_fwd_gap={max_gap:.2f}s "
          f"decoded={ndec} err={derr}")
    check(span > 150.0, "clip spans across the reconnect", f"span={span:.1f}s (>150)")
    check(mono, "output DTS monotonic across reconnect")
    check(dec and key, "first frame decodes (keyframe)")
    check(derr is None, "full clip decodes cleanly", f"err={derr}")


PROBES = {
    "A": probe_windowed_real,
    "B": probe_backward_jump,
    "C": probe_forward_gap,
    "D": probe_bframe_real,
    "E": probe_concurrent,
    "F": probe_reconnect,
}


def main() -> int:
    if shutil.which("ffmpeg") is None:
        print("ffmpeg not found on PATH — required to build probe fixtures.")
        return 2
    if not Path(SAMPLE).exists():
        print(f"real capture missing: {SAMPLE}")
        return 2

    keep = os.environ.get("PROBE_WORK")
    work = Path(keep) if keep else Path(tempfile.mkdtemp(prefix="remux_probe_"))
    work.mkdir(parents=True, exist_ok=True)
    print(f"work dir: {work}{' (kept)' if keep else ''}")

    which = [w.upper() for w in sys.argv[1:]] or list(PROBES)
    try:
        for key in which:
            PROBES[key](work)
    finally:
        if not keep:
            shutil.rmtree(work, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAIL:
        print(f"RESULT: {len(FAIL)} FAILURE(S)")
        for f in FAIL:
            print(f"  - {f}")
        return 1
    print("RESULT: ALL PROBES PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
