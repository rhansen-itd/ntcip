#!/usr/bin/env python3
"""
__replay_verify.py — Blind self-test for the remux video buffer (ROADMAP Item 1).

Debug tool (``__``-prefixed, ``print()`` allowed — not a production module). It
exercises ``remux_video_buffer`` end-to-end **without a camera** by replaying
ffmpeg-synthesized streams through the real ``VideoBufferManager`` (real Hot
Folder triggers, real semaphore/disk logic), standing in for live RTSP via a
real-time-paced ``PacketStreamBuffer`` subclass injected through the manager's
``stream_buffer_factory`` seam.

What it asserts (plan section 7.1 / acceptance criteria 2-3):

* **Length fidelity** — a fully-remuxed clip's PTS span equals the source PTS
  span within ~1-2 frames, on CFR, jittered, and B-frame streams. This is the
  core defect Item 1 fixes: no assumed-FPS drift.
* **Keyframe-seek start** — the first frame of every clip decodes and is a
  keyframe (no leading gray / mid-GOP start).
* **Windowed length** — a start/stop-windowed clip's length tracks the requested
  ``pre_roll + (stop - start)`` window (keyframe-aligned, so it may run up to one
  GOP longer, never unboundedly off).
* **RAM-boundedness** — process RSS stays flat across a 4-minute clip (host-
  independent, so it proves the fix even off-edge).

Usage::

    python __replay_verify.py                # synthesize streams + full matrix
    python __replay_verify.py path/to/sample.ts   # run against a real capture (Fable)

Exits non-zero if any assertion fails.
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
from typing import List, Optional, Tuple

# This tool lives in video_engine/tools/; put video_engine/ on sys.path so the
# remux_video_buffer import below resolves regardless of the working directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import av

from remux_video_buffer import (
    PacketStreamBuffer,
    VideoBufferConfig,
    VideoBufferManager,
)


# ---------------------------------------------------------------------------
# Paced replay source — stands in for live RTSP
# ---------------------------------------------------------------------------
class PacedReplayStreamBuffer(PacketStreamBuffer):
    """Replay a file through the buffer at (a multiple of) real time.

    Overrides the ``_open_container`` / ``_demux`` seam so packets arrive spaced
    by their DTS deltas divided by ``speed`` — recv_wall stamping in the base
    capture loop then reflects realistic arrival times. ``reconnect_on_eof`` is
    forced off so the finite file ends the capture loop and sets ``exhausted``.
    """

    def __init__(self, *args, speed: float = 1.0, **kwargs) -> None:
        kwargs["reconnect_on_eof"] = False
        super().__init__(*args, **kwargs)
        self.speed = speed

    def _open_container(self):
        container = av.open(self.url)
        return container, container.streams.video[0]

    def _demux(self, container, stream):
        tb = float(stream.time_base)
        prev_dts = None
        for pkt in container.demux(stream):
            if pkt.dts is None:
                continue
            if prev_dts is not None and self.speed > 0:
                dt = (pkt.dts - prev_dts) * tb / self.speed
                if dt > 0:
                    time.sleep(min(dt, 1.0))
            prev_dts = pkt.dts
            yield pkt


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------
def pts_span(path: str) -> Tuple[float, int, float]:
    """Return ``(pts_span_sec, packet_count, mean_frame_dur_sec)`` for a file."""
    container = av.open(path)
    stream = container.streams.video[0]
    ptss: List[float] = []
    durs: List[float] = []
    for pkt in container.demux(stream):
        if pkt.pts is None:
            continue
        ptss.append(float(pkt.pts * pkt.time_base))
        if pkt.duration:
            durs.append(float(pkt.duration * pkt.time_base))
    container.close()
    if not ptss:
        return 0.0, 0, 0.0
    frame_dur = (sum(durs) / len(durs)) if durs else 0.0
    span = max(ptss) - min(ptss) + frame_dur
    return span, len(ptss), frame_dur


def first_frame_decodes(path: str) -> Tuple[bool, bool]:
    """Return ``(decoded_ok, is_keyframe)`` for the first frame of ``path``."""
    try:
        container = av.open(path)
        stream = container.streams.video[0]
        frame = next(container.decode(stream))
        container.close()
        return True, bool(frame.key_frame)
    except (av.AVError, StopIteration, OSError):
        return False, False


def rss_mb() -> float:
    """Current resident set size in MB (Linux ``/proc/self/statm``)."""
    with open("/proc/self/statm", encoding="ascii") as fh:
        pages = int(fh.read().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)


def write_trigger(
    trigger_dir: Path,
    action: str,
    trigger_id: str,
    event_ts: float,
    pre_roll_sec: float = 4.0,
    max_duration_sec: float = 300.0,
    cameras: Optional[List[str]] = None,
) -> None:
    """Atomically drop a trigger file into the Hot Folder (tmp -> rename)."""
    import json

    payload = {
        "trigger_id": trigger_id,
        "action": action,
        "event_timestamp": event_ts,
        "reason": "detector_disagreement",
        "intersection_id": "selftest",
        "timezone": "UTC",
        "cameras": cameras or ["all"],
        "pre_roll_sec": pre_roll_sec,
        "post_roll_sec": 20,
        "max_duration_sec": max_duration_sec,
        "metadata": {"rule": "selftest", "det_a": "A", "det_b": "B"},
    }
    stem = f"trigger_{action}_{trigger_id[:8]}"
    tmp = trigger_dir / f"{stem}.tmp"
    dst = trigger_dir / f"{stem}.json"
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.rename(tmp, dst)


# ---------------------------------------------------------------------------
# Synthetic stream generation
# ---------------------------------------------------------------------------
def synth(work: Path, name: str, args: List[str]) -> str:
    """Generate a synthetic stream with ffmpeg if absent; return its path."""
    path = work / name
    if path.exists():
        return str(path)
    cmd = ["ffmpeg", "-y", "-f", "lavfi"] + args + [str(path)]
    subprocess.run(cmd, check=True, capture_output=True)
    return str(path)


def make_streams(work: Path) -> dict:
    """Synthesize the CFR / jitter / B-frame / long test streams."""
    return {
        "cfr": synth(
            work, "cfr.ts",
            ["-i", "testsrc=size=320x240:rate=20:duration=12",
             "-c:v", "libx264", "-g", "10", "-bf", "0", "-pix_fmt", "yuv420p"],
        ),
        "jitter": synth(
            work, "jitter.ts",
            ["-i", "testsrc=size=320x240:rate=20:duration=12",
             "-vf", "setpts=PTS*(1+0.1*sin(N/10))",
             "-c:v", "libx264", "-g", "10", "-bf", "0", "-pix_fmt", "yuv420p"],
        ),
        "bframe": synth(
            work, "bframe.ts",
            ["-i", "testsrc=size=320x240:rate=20:duration=12",
             "-c:v", "libx264", "-g", "10", "-bf", "2", "-pix_fmt", "yuv420p"],
        ),
        "long": synth(
            work, "long.ts",
            ["-i", "testsrc=size=320x240:rate=20:duration=240",
             "-c:v", "libx264", "-g", "40", "-bf", "0", "-pix_fmt", "yuv420p"],
        ),
    }


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------
class Harness:
    """Builds a manager over a paced replay of one file and drives triggers."""

    def __init__(
        self,
        source: str,
        work: Path,
        speed: float,
        pre_roll_sec: float,
        keyframe_margin_sec: float = 4.0,
    ) -> None:
        self.source = source
        self.trigger_dir = work / f"trig_{uuid.uuid4().hex[:6]}"
        self.output_dir = work / f"out_{uuid.uuid4().hex[:6]}"
        self.buffers: List[PacedReplayStreamBuffer] = []
        self.cfg = VideoBufferConfig(
            streams={"cam1": source},
            trigger_dir=str(self.trigger_dir),
            output_dir=str(self.output_dir),
            pre_roll_sec=pre_roll_sec,
            keyframe_margin_sec=keyframe_margin_sec,
            poll_interval_sec=0.2,
            min_free_disk_mb=1.0,
        )

        def factory(cam: str, url: str) -> PacedReplayStreamBuffer:
            buf = PacedReplayStreamBuffer(
                camera_id=cam,
                url=url,
                pre_roll_sec=pre_roll_sec,
                keyframe_margin_sec=keyframe_margin_sec,
                speed=speed,
            )
            self.buffers.append(buf)
            return buf

        self.mgr = VideoBufferManager(self.cfg, stream_buffer_factory=factory)
        self._thread = threading.Thread(target=self.mgr.start, daemon=True)

    def start(self) -> None:
        self._thread.start()
        # Wait for the stream buffer to be created and the poll loop to be live.
        for _ in range(100):
            if self.buffers:
                return
            time.sleep(0.05)

    def wait_exhausted(self, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.buffers and self.buffers[0].exhausted.is_set():
                return True
            time.sleep(0.05)
        return False

    def wait_writers_done(self, timeout: float = 30.0) -> None:
        # Wait until no writer is active AND none is still finalizing (draining),
        # so the output container is fully closed before we read it.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.mgr._active_writers and not self.mgr._draining:  # noqa: SLF001
                return
            time.sleep(0.05)

    def output_clip(self) -> Optional[str]:
        clips = sorted(Path(self.output_dir).glob("*.ts"))
        return str(clips[-1]) if clips else None

    def stop(self) -> None:
        self.mgr.stop()


# ---------------------------------------------------------------------------
# Individual tests
# ---------------------------------------------------------------------------
FAILURES: List[str] = []


def check(cond: bool, label: str, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {label}{('  — ' + detail) if detail else ''}")
    if not cond:
        FAILURES.append(f"{label}: {detail}")


def test_fidelity(name: str, source: str, work: Path) -> None:
    """Full-consume remux: output PTS span must equal source PTS span (~1-2 frames)."""
    print(f"\n== fidelity[{name}] ==")
    src_span, src_n, frame_dur = pts_span(source)
    # High speed is fine: full consume + huge pre-roll captures the whole source,
    # and PTS fidelity is pacing-independent (timestamps are copied, not derived).
    h = Harness(source, work, speed=30.0, pre_roll_sec=1000.0,
                keyframe_margin_sec=1000.0)
    h.start()
    if not h.wait_exhausted():
        check(False, f"{name} source drained", "timed out")
        h.stop()
        return
    tid = uuid.uuid4().hex
    write_trigger(h.trigger_dir, "start", tid, time.time(), pre_roll_sec=1000.0)
    time.sleep(0.6)  # let the poll loop pick up + prime the whole ring
    write_trigger(h.trigger_dir, "stop", tid, time.time())
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()

    if not clip:
        check(False, f"{name} clip produced", "no output file")
        return
    out_span, out_n, _ = pts_span(clip)
    decoded, is_key = first_frame_decodes(clip)
    tol = max(2.5 * frame_dur, 0.05)
    print(f"  source span={src_span:.4f}s ({src_n} pk)  "
          f"out span={out_span:.4f}s ({out_n} pk)  tol={tol:.4f}s")
    check(abs(out_span - src_span) <= tol, f"{name} length fidelity",
          f"|{out_span:.4f}-{src_span:.4f}|={abs(out_span-src_span):.4f} <= {tol:.4f}")
    check(decoded, f"{name} first frame decodes")
    check(is_key, f"{name} first frame is keyframe")


def test_windowed(source: str, work: Path) -> None:
    """Real-time windowed clip: length tracks pre_roll + (stop - start)."""
    print("\n== windowed[cfr @1x] ==")
    pre_roll = 4.0
    gop_sec = 0.5  # g=10 @ 20fps
    h = Harness(source, work, speed=1.0, pre_roll_sec=pre_roll,
                keyframe_margin_sec=2.0)
    h.start()
    time.sleep(pre_roll + 3.0)  # fill pre-roll window + live headroom
    tid = uuid.uuid4().hex
    t_start = time.time()
    write_trigger(h.trigger_dir, "start", tid, t_start, pre_roll_sec=pre_roll)
    record_sec = 5.0
    time.sleep(record_sec)
    t_stop = time.time()
    write_trigger(h.trigger_dir, "stop", tid, t_stop)
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()

    if not clip:
        check(False, "windowed clip produced", "no output file")
        return
    out_span, _, _ = pts_span(clip)
    decoded, is_key = first_frame_decodes(clip)
    expected = pre_roll + (t_stop - t_start)
    lo = expected - 0.4
    hi = expected + gop_sec + h.cfg.poll_interval_sec + 0.5
    print(f"  expected~{expected:.2f}s  out span={out_span:.4f}s  "
          f"window=[{lo:.2f},{hi:.2f}]")
    check(lo <= out_span <= hi, "windowed length tracks request",
          f"{out_span:.3f} in [{lo:.2f},{hi:.2f}]")
    check(decoded and is_key, "windowed first frame decodes (keyframe)")


def test_ram_flat(source: str, work: Path) -> None:
    """4-minute clip: RSS must stay flat (no per-clip frame accumulation)."""
    print("\n== ram_flat[240s clip] ==")
    src_span, _, _ = pts_span(source)
    h = Harness(source, work, speed=20.0, pre_roll_sec=2.0, keyframe_margin_sec=4.0)
    h.start()
    time.sleep(0.3)
    tid = uuid.uuid4().hex
    write_trigger(h.trigger_dir, "start", tid, time.time(),
                  pre_roll_sec=2.0, max_duration_sec=99999.0)
    time.sleep(0.6)
    baseline = rss_mb()
    peak = baseline
    while not (h.buffers and h.buffers[0].exhausted.is_set()):
        peak = max(peak, rss_mb())
        time.sleep(0.1)
    peak = max(peak, rss_mb())
    write_trigger(h.trigger_dir, "stop", tid, time.time())
    h.wait_writers_done()
    clip = h.output_clip()
    h.stop()

    if not clip:
        check(False, "ram clip produced", "no output file")
        return
    out_span, out_n, _ = pts_span(clip)
    growth = peak - baseline
    decoded, is_key = first_frame_decodes(clip)
    print(f"  source={src_span:.1f}s  clip span={out_span:.1f}s ({out_n} pk)  "
          f"RSS baseline={baseline:.1f}MB peak={peak:.1f}MB growth={growth:.1f}MB")
    check(out_span >= 180.0, "multi-minute clip written",
          f"span={out_span:.1f}s (>=180)")
    check(growth < 80.0, "RSS flat across clip (RAM-bounded)",
          f"growth={growth:.1f}MB (<80)")
    check(decoded and is_key, "ram clip first frame decodes (keyframe)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    if shutil.which("ffmpeg") is None:
        print("ffmpeg not found on PATH — required to synthesize test streams.")
        return 2

    work = Path(tempfile.mkdtemp(prefix="remux_verify_"))
    print(f"work dir: {work}")

    try:
        if len(sys.argv) > 1:
            # Real-capture mode (Fable): run fidelity + RAM against the given file.
            sample = sys.argv[1]
            print(f"real-capture mode: {sample}")
            test_fidelity("sample", sample, work)
            test_ram_flat(sample, work)
        else:
            streams = make_streams(work)
            test_fidelity("cfr", streams["cfr"], work)
            test_fidelity("jitter", streams["jitter"], work)
            test_fidelity("bframe", streams["bframe"], work)
            test_windowed(streams["cfr"], work)
            test_ram_flat(streams["long"], work)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"RESULT: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("RESULT: ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
