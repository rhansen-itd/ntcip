"""
video_buffer.py — Core module for the video_buffer package.

Responsibilities:
  - Continuously read RTSP/HTTP video streams into a RAM-backed pre-roll deque
    (Zero-Drift: no time.sleep() in the capture loop).
  - Poll a "Hot Folder" for JSON trigger files conforming to the project schema.
  - Route pre-roll + live frames to a bounded disk-writer queue on START triggers,
    and seal the output file on STOP/extend triggers.
  - Enforce edge-device constraints: concurrent-encoder semaphore, pre-write disk
    check, atomic trigger-file deletion.

Usage:
    manager = VideoBufferManager(config)
    manager.start()          # blocks; call from main thread or a dedicated thread
    manager.stop()           # graceful shutdown from another thread / signal handler
"""

from __future__ import annotations

import collections
import csv
import json
import logging
import os
import queue
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2  # type: ignore
import pytz  # type: ignore

# ---------------------------------------------------------------------------
# Structured JSON logger
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Args:
        fmt_keys: Extra static key/value pairs to merge into every record.
    """

    def __init__(self, fmt_keys: Optional[Dict[str, str]] = None) -> None:
        super().__init__()
        self._fmt_keys = fmt_keys or {}

    def format(self, record: logging.LogRecord) -> str:  # noqa: D102
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(self._fmt_keys)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        # Merge any extra fields attached via `extra=` kwarg
        for k, v in record.__dict__.items():
            if k not in {
                "msg", "args", "levelname", "levelno", "pathname", "filename",
                "module", "exc_info", "exc_text", "stack_info", "lineno",
                "funcName", "created", "msecs", "relativeCreated", "thread",
                "threadName", "processName", "process", "name", "message",
            }:
                payload[k] = v
        return json.dumps(payload, default=str)


def _build_logger(name: str) -> logging.Logger:
    """Return a logger that emits JSON lines to stdout.

    Args:
        name: Logger name (typically ``__name__`` or a sub-path).

    Returns:
        Configured :class:`logging.Logger`.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


log = _build_logger("video_buffer")


def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> "pytz.BaseTzInfo":
    """Resolve an IANA timezone name to a pytz timezone object.

    Args:
        tz_name: IANA timezone string (e.g. ``"America/New_York"``).  If the
            name is unrecognised, UTC is used and a warning is logged.
        fallback_log: Logger to emit the warning on.

    Returns:
        A :class:`pytz.BaseTzInfo` instance.
    """
    try:
        return pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        fallback_log.warning(
            "Unknown IANA timezone name; falling back to UTC",
            extra={"timezone": tz_name},
        )
        return pytz.utc


# ---------------------------------------------------------------------------
# Configuration dataclass
# ---------------------------------------------------------------------------

@dataclass
class VideoBufferConfig:
    """All tunables for a single VideoBufferManager deployment.

    Args:
        streams: Mapping of camera_id → stream URL (RTSP or HTTP).
        trigger_dir: Directory to poll for JSON trigger files.
        output_dir: Directory where completed MP4 clips are written.
        pre_roll_sec: Seconds of frames kept in the RAM deque per stream.
        poll_interval_sec: Seconds between hot-folder scans (2–5 recommended).
        max_concurrent_writers: Hard cap on simultaneous encoder threads (edge: 2).
        min_free_disk_mb: Abort recording if free space falls below this value.
        fourcc: OpenCV FourCC codec string for output files.
        fps_fallback: FPS to use when the stream does not report it.
    """

    streams: Dict[str, str]
    trigger_dir: str = "./trigger_queue"
    output_dir: str = "./completed_videos"
    pre_roll_sec: float = 10.0
    poll_interval_sec: float = 2.0
    max_concurrent_writers: int = 2
    min_free_disk_mb: float = 500.0
    fourcc: str = "mp4v"
    fps_fallback: float = 15.0

# ---------------------------------------------------------------------------
# Internal sentinel objects for the writer queue
# ---------------------------------------------------------------------------

class _StopWriter:
    """Sentinel placed in the writer queue to signal end-of-clip."""
    __slots__ = ()


_STOP_WRITER = _StopWriter()


# ---------------------------------------------------------------------------
# StreamBuffer — per-camera capture + pre-roll deque
# ---------------------------------------------------------------------------

class StreamBuffer:
    """Continuously reads a single video stream and maintains a RAM pre-roll.

    The capture loop never calls ``time.sleep()``. OpenCV's ``grab()``/
    ``retrieve()`` cadence is kept tight so that frame timestamps reflect
    wall-clock reality as closely as possible.

    Args:
        camera_id: Logical identifier used for logging and output filenames.
        url: RTSP or HTTP stream URL passed directly to ``cv2.VideoCapture``.
        pre_roll_sec: How many seconds of frames to retain in ``_deque``.
        fps_fallback: FPS assumed when the stream does not report a valid value.
    """

    def __init__(
        self,
        camera_id: str,
        url: str,
        pre_roll_sec: float = 10.0,
        fps_fallback: float = 15.0,
    ) -> None:
        self.camera_id = camera_id
        self.url = url
        self.pre_roll_sec = pre_roll_sec
        self.fps_fallback = fps_fallback

        self._cap: Optional[cv2.VideoCapture] = None
        self._fps: float = fps_fallback
        self._stable_fps: Optional[float] = None   # set once on first open; reused on reconnect
        self._frame_shape: Optional[tuple] = None

        # maxlen enforced once _fps is known; will be reset in _open()
        self._deque: collections.deque = collections.deque()
        self._deque_lock = threading.Lock()

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._log = _build_logger(f"video_buffer.stream.{camera_id}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Open the stream and launch the background capture thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._capture_loop,
            name=f"capture-{self.camera_id}",
            daemon=True,
        )
        self._thread.start()
        self._log.info("StreamBuffer started", extra={"camera_id": self.camera_id, "url": self.url})

    def stop(self) -> None:
        """Signal the capture thread to exit and release the capture device."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        if self._cap:
            self._cap.release()
        self._log.info("StreamBuffer stopped", extra={"camera_id": self.camera_id})

    def drain_preroll(self) -> List[Any]:
        """Return a snapshot of the pre-roll deque as an ordered list.

        Frames are ordered oldest-first. The deque is **not** cleared so that
        the live capture loop can continue populating it normally.

        Returns:
            List of ``(frame_ndarray, capture_timestamp_float)`` tuples.
        """
        with self._deque_lock:
            return list(self._deque)

    def frames_since(self, after_ts: float) -> List[Any]:
        """Return all frames with a timestamp strictly greater than ``after_ts``.

        The deque is **never cleared** so the pre-roll history remains intact
        for subsequent triggers and for ``drain_preroll`` calls.  The caller
        is responsible for advancing its own watermark to the timestamp of the
        last returned frame.

        Args:
            after_ts: Monotonic timestamp of the last frame already consumed
                by the caller.  Pass ``0.0`` to receive every frame currently
                in the deque.

        Returns:
            Ordered list of ``(frame_ndarray, capture_timestamp_float)`` tuples
            with ``ts > after_ts``, oldest first.
        """
        with self._deque_lock:
            return [(f, ts) for f, ts in self._deque if ts > after_ts]

    @property
    def fps(self) -> float:
        """Detected (or fallback) frames-per-second for this stream."""
        return self._fps

    @property
    def frame_shape(self) -> Optional[tuple]:
        """``(height, width, channels)`` of the most recently decoded frame."""
        return self._frame_shape

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _open(self) -> bool:
        """Attempt to open the stream and configure the deque max-length.

        FPS resolution priority:
          1. ``_stable_fps`` — a measurement already taken on a previous open
             (preserved across reconnections so in-flight writers stay consistent).
          2. Live measurement via :meth:`_measure_fps` (first open only).
          3. ``CAP_PROP_FPS`` — camera-reported value (fallback if measurement
             returns ``None``).
          4. ``fps_fallback`` — hardcoded default of last resort.

        Returns:
            ``True`` if the stream opened successfully, ``False`` otherwise.
        """
        self._cap = cv2.VideoCapture(self.url)
        if not self._cap.isOpened():
            self._log.error(
                "Failed to open stream",
                extra={"camera_id": self.camera_id, "url": self.url},
            )
            return False

        # Only measure on the very first open.  On reconnection we reuse the
        # previously measured value so any in-flight DiskWriter is not stamped
        # with a different FPS mid-recording.
        if self._stable_fps is None:
            reported_fps = self._cap.get(cv2.CAP_PROP_FPS)
            measured_fps = self._measure_fps(sample_sec=5.0)

            if measured_fps and measured_fps > 0:
                self._stable_fps = measured_fps
                self._log.info(
                    "FPS measured from stream",
                    extra={
                        "camera_id":    self.camera_id,
                        "measured_fps": round(measured_fps, 2),
                        "reported_fps": reported_fps,
                    },
                )
            elif reported_fps and reported_fps > 0:
                self._stable_fps = reported_fps
                self._log.warning(
                    "FPS measurement failed — using reported FPS",
                    extra={"camera_id": self.camera_id, "reported_fps": reported_fps},
                )
            else:
                self._stable_fps = self.fps_fallback
                self._log.warning(
                    "FPS measurement and reported FPS both unusable — using fallback",
                    extra={"camera_id": self.camera_id, "fps_fallback": self.fps_fallback},
                )

        self._fps = self._stable_fps
        maxlen = max(1, int(self._fps * self.pre_roll_sec))

        with self._deque_lock:
            # Preserve existing frames when reconnecting; trim to new maxlen.
            old_frames = list(self._deque)
            self._deque = collections.deque(old_frames[-maxlen:], maxlen=maxlen)

        self._log.info(
            "Stream opened",
            extra={
                "camera_id": self.camera_id,
                "fps": self._fps,
                "deque_maxlen": maxlen,
            },
        )
        return True

    def _measure_fps(self, sample_sec: float = 5.0) -> Optional[float]:
        """Measure the actual delivered FPS by timing fully-decoded frame reads.

        Uses ``read()`` rather than ``grab()`` to force network round-trips and
        full decode work, ensuring the count reflects true camera delivery rate
        rather than local loop speed.

        Args:
            sample_sec: Seconds to count frames over.  5 s gives a stable
                reading without too much startup delay.

        Returns:
            Measured FPS as a float, or ``None`` if fewer than 2 frames were
            received (stream stalled or immediately failed).
        """
        count = 0
        start = time.monotonic()
        deadline = start + sample_sec

        while time.monotonic() < deadline:
            ret, _ = self._cap.read()
            if not ret:
                break
            count += 1

        elapsed = time.monotonic() - start
        if count < 2 or elapsed == 0:
            return None
        return count / elapsed

    def _capture_loop(self) -> None:
        """Background thread: grab frames continuously, no sleeping.

        On stream failure the loop attempts reconnection with a brief
        ``threading.Event`` wait (non-blocking from the GIL perspective).
        """
        reconnect_event = threading.Event()

        while self._running:
            if not self._open():
                reconnect_event.wait(timeout=3.0)
                reconnect_event.clear()
                continue

            while self._running:
                grabbed = self._cap.grab()
                if not grabbed:
                    self._log.warning(
                        "Stream grab failed — reconnecting",
                        extra={"camera_id": self.camera_id},
                    )
                    self._cap.release()
                    break

                ret, frame = self._cap.retrieve()
                if not ret or frame is None:
                    continue

                ts = time.monotonic()
                self._frame_shape = frame.shape

                with self._deque_lock:
                    self._deque.append((frame, ts))

        if self._cap:
            self._cap.release()


# ---------------------------------------------------------------------------
# DiskWriter — single clip encoder running in its own thread
# ---------------------------------------------------------------------------

class DiskWriter:
    """Consumes frames from a queue, writes them to disk incrementally, and
    stamps the container with a statistically accurate FPS.

    Design constraints
    ──────────────────
    Two conflicting requirements must be satisfied simultaneously:

    1. **RAM safety (J1900 edge device):** Frames must be written to disk as
       they arrive.  Buffering an entire clip in RAM is not viable — a 5-min
       1080p recording at 20 fps requires ~35 GB of raw ndarray memory.

    2. **Accurate FPS (jitter-robust):** The container FPS must reflect true
       camera delivery rate, not metadata claims.  A 2-frame sample is
       unreliable because RTSP streams deliver frames in micro-bursts caused
       by keyframe decoding, network packet grouping, and OS scheduling jitter.
       Only a multi-second window averages jitter out reliably.

    Solution: warm-up window + incremental write
    ─────────────────────────────────────────────
    The writer collects the first ``_FPS_WARMUP_SEC`` seconds of frames in a
    small staging list (typically 15–30 frames at traffic-cam frame rates).
    Once the warmup window elapses, FPS is computed as::

        fps = warmup_frame_count / (last_warmup_ts - first_warmup_ts)

    The ``VideoWriter`` is then opened with this derived FPS and all warmup
    frames are flushed to disk immediately.  Every subsequent frame is written
    incrementally — never held in RAM — so peak memory overhead is the warmup
    window only (~30 frames × 6 MB = ~180 MB at 1080p, ~80 MB at 720p).

    Fallback chain
    ──────────────
    * Warmup window too short (< 2 frames or < 0.5 s elapsed) → ``fallback_fps``
    * ``VideoWriter`` fails to open → log error and release semaphore cleanly

    Args:
        output_path: Full path of the MP4 file to create.
        fallback_fps: FPS to use if the warmup window produces an unusable
            sample (degenerate stream or stop arrived before warmup completed).
        fourcc_str: OpenCV FourCC string (e.g. ``"mp4v"``).
        semaphore: Shared :class:`threading.Semaphore` controlling concurrent writers.
        trigger_id: ID string used for structured log correlation.
        fps_warmup_sec: Seconds of frames to collect before computing FPS.
            Default 3 s; increase to 5 s on highly jittery streams.
    """

    _MIN_WARMUP_DURATION = 0.5   # reject sample if elapsed time is suspiciously short
    _MIN_WARMUP_FRAMES   = 2     # reject sample if fewer than this many frames arrived

    def __init__(
        self,
        output_path: Path,
        fallback_fps: float,
        fourcc_str: str,
        semaphore: threading.Semaphore,
        trigger_id: str,
        fps_warmup_sec: float = 3.0,
    ) -> None:
        self._output_path    = output_path
        self._fallback_fps   = fallback_fps
        self._fourcc_str     = fourcc_str
        self._semaphore      = semaphore
        self._trigger_id     = trigger_id
        self._fps_warmup_sec = fps_warmup_sec
        self._frame_queue: queue.Queue = queue.Queue(maxsize=0)
        self._writer: Optional[cv2.VideoWriter] = None
        self._thread = threading.Thread(
            target=self._write_loop,
            name=f"writer-{trigger_id[:8]}",
            daemon=True,
        )
        self._log = _build_logger("video_buffer.disk_writer")

    def start(self) -> None:
        """Launch the writer thread."""
        self._thread.start()

    def push(self, item: Any) -> None:
        """Enqueue a frame tuple or the ``_STOP_WRITER`` sentinel.

        Args:
            item: Either a ``(frame_ndarray, timestamp_float)`` tuple or
                  :data:`_STOP_WRITER`.
        """
        self._frame_queue.put(item)

    def _write_loop(self) -> None:
        """Drain the queue, derive FPS from a warmup window, write incrementally."""
        # Phase 1 — warmup: collect frames until fps_warmup_sec elapses
        warmup_frames: List[tuple] = []
        derived_fps: Optional[float] = None

        try:
            while derived_fps is None:
                item = self._frame_queue.get()

                if isinstance(item, _StopWriter):
                    # Stop arrived before warmup completed — flush whatever we have.
                    derived_fps = self._compute_fps(warmup_frames) or self._fallback_fps
                    self._log.warning(
                        "Stop arrived during FPS warmup — using partial sample",
                        extra={
                            "trigger_id":    self._trigger_id,
                            "warmup_frames": len(warmup_frames),
                            "derived_fps":   round(derived_fps, 3),
                        },
                    )
                    self._flush_warmup(warmup_frames, derived_fps)
                    return   # nothing more to write; finally block handles cleanup

                warmup_frames.append(item)

                # Check whether the warmup window has elapsed
                if (
                    len(warmup_frames) >= self._MIN_WARMUP_FRAMES
                    and (warmup_frames[-1][1] - warmup_frames[0][1]) >= self._fps_warmup_sec
                ):
                    derived_fps = self._compute_fps(warmup_frames) or self._fallback_fps

            # Open writer and flush warmup frames to disk
            if not self._flush_warmup(warmup_frames, derived_fps):
                return  # VideoWriter failed to open

            self._log.info(
                "DiskWriter FPS derived from warmup window",
                extra={
                    "trigger_id":    self._trigger_id,
                    "derived_fps":   round(derived_fps, 3),
                    "fallback_fps":  self._fallback_fps,
                    "warmup_frames": len(warmup_frames),
                    "warmup_sec":    round(
                        warmup_frames[-1][1] - warmup_frames[0][1], 2
                    ) if len(warmup_frames) >= 2 else 0,
                },
            )
            warmup_frames.clear()  # release RAM now that frames are on disk

            # Phase 2 — incremental write: drain queue frame-by-frame
            while True:
                item = self._frame_queue.get()
                if isinstance(item, _StopWriter):
                    break
                frame, _ts = item
                self._writer.write(frame)

        finally:
            if self._writer:
                self._writer.release()
            self._semaphore.release()
            self._log.info(
                "Recording finished",
                extra={
                    "trigger_id":  self._trigger_id,
                    "output_path": str(self._output_path),
                },
            )

    def _compute_fps(self, frames: List[tuple]) -> Optional[float]:
        """Derive FPS from the timestamps of the first and last warmup frames.

        Args:
            frames: List of ``(frame_ndarray, timestamp_float)`` tuples.

        Returns:
            Computed FPS, or ``None`` if the sample is too short to be reliable.
        """
        if len(frames) < self._MIN_WARMUP_FRAMES:
            return None
        elapsed = frames[-1][1] - frames[0][1]
        if elapsed < self._MIN_WARMUP_DURATION:
            return None
        return len(frames) / elapsed

    def _flush_warmup(self, frames: List[tuple], fps: float) -> bool:
        """Open the VideoWriter and write all warmup frames to disk.

        Args:
            frames: Warmup frame list to flush.
            fps: Derived FPS to stamp into the container header.

        Returns:
            ``True`` if the writer opened and all frames were written,
            ``False`` if the writer failed to open.
        """
        if not frames:
            return True  # nothing to flush; not an error

        h, w = frames[0][0].shape[:2]
        fourcc = cv2.VideoWriter_fourcc(*self._fourcc_str)
        self._writer = cv2.VideoWriter(
            str(self._output_path), fourcc, fps, (w, h)
        )
        if not self._writer.isOpened():
            self._log.error(
                "VideoWriter failed to open",
                extra={
                    "trigger_id":  self._trigger_id,
                    "output_path": str(self._output_path),
                },
            )
            self._writer = None
            return False

        for frame, _ts in frames:
            self._writer.write(frame)
        return True


# ---------------------------------------------------------------------------
# VideoBufferManager — top-level orchestrator
# ---------------------------------------------------------------------------

class VideoBufferManager:
    """Orchestrates stream buffers, hot-folder polling, and disk writers.

    This is the single entry point for the ``video_buffer`` package. It:

    1. Starts one :class:`StreamBuffer` per configured camera.
    2. Polls ``config.trigger_dir`` for JSON trigger files.
    3. Dispatches :class:`DiskWriter` instances on ``start`` triggers,
       bounded by a semaphore to protect edge CPU/RAM.
    4. Routes frames to the appropriate writer until a ``stop`` trigger
       (or ``max_duration_sec``) is received.

    Args:
        config: Fully-populated :class:`VideoBufferConfig` instance.
    """

    def __init__(self, config: VideoBufferConfig) -> None:
        self._config = config
        self._stream_buffers: Dict[str, StreamBuffer] = {}
        self._active_writers: Dict[str, DiskWriter] = {}   # trigger_id → writer
        self._active_cameras: Dict[str, str] = {}          # camera_id → trigger_id
        # Per-writer watermark: trigger_id → monotonic ts of last frame pushed.
        # Lets frames_since() return only genuinely new frames each tick without
        # ever clearing the StreamBuffer deque (preserving pre-roll history).
        self._writer_watermarks: Dict[str, float] = {}     # trigger_id → last_ts
        self._writer_semaphore = threading.Semaphore(config.max_concurrent_writers)
        self._running = False
        self._log = _build_logger("video_buffer.manager")

        Path(config.trigger_dir).mkdir(parents=True, exist_ok=True)
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start all stream buffers and enter the hot-folder polling loop.

        This method blocks the calling thread. Call :meth:`stop` from a
        signal handler or a separate thread to exit cleanly.
        """
        self._running = True

        for cam_id, url in self._config.streams.items():
            buf = StreamBuffer(
                camera_id=cam_id,
                url=url,
                pre_roll_sec=self._config.pre_roll_sec,
                fps_fallback=self._config.fps_fallback,
            )
            buf.start()
            self._stream_buffers[cam_id] = buf

        self._log.info(
            "VideoBufferManager started",
            extra={"num_streams": len(self._stream_buffers)},
        )

        self._poll_loop()

    def stop(self) -> None:
        """Signal the polling loop to exit and stop all stream buffers."""
        self._running = False
        for buf in self._stream_buffers.values():
            buf.stop()
        self._log.info("VideoBufferManager stopped")

    # ------------------------------------------------------------------
    # Hot-folder polling
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        """Main loop: scan trigger dir, dispatch triggers, feed active writers.

        The loop yields for 10 ms each iteration via ``time.sleep(0.01)``.
        This does **not** violate the Zero-Drift rule — that rule targets the
        capture thread where sleeping would drop source frames. Here we are
        only consuming already-buffered frames; 10 ms is imperceptible at
        30 fps (one frame every ~33 ms) but reduces CPU utilisation from a
        spin-lock (~100 %) to near-idle (~1 %), which is critical on J1900
        hardware. The hot-folder scan is additionally rate-limited to at
        most once per ``poll_interval_sec`` to minimise disk I/O.
        """
        last_scan: float = 0.0

        while self._running:
            now = time.monotonic()

            # --- Feed live frames to any active writers ---
            self._feed_active_writers()

            # --- Rate-limited hot-folder scan ---
            if (now - last_scan) >= self._config.poll_interval_sec:
                self._scan_trigger_dir()
                last_scan = time.monotonic()

            # --- Cooperative yield (see docstring) ---
            time.sleep(0.01)

    def _scan_trigger_dir(self) -> None:
        """Collect, sort, and process all pending trigger files.

        Files are processed oldest-first (by ``st_ctime``). Each file is
        read, validated, dispatched, and then deleted only after successful
        processing.
        """
        trigger_dir = Path(self._config.trigger_dir)
        candidates = sorted(
            trigger_dir.glob("trigger_*.json"),
            key=lambda p: p.stat().st_ctime,
        )

        for path in candidates:
            try:
                trigger = self._read_trigger(path)
            except (json.JSONDecodeError, OSError, KeyError, ValueError) as exc:
                self._log.error(
                    "Invalid trigger file — skipping",
                    extra={"path": str(path), "error": str(exc)},
                )
                self._safe_delete(path)
                continue

            action = trigger.get("action", "")
            if action == "start":
                self._handle_start(trigger)
            elif action in ("stop", "extend"):
                self._handle_stop(trigger, extend=(action == "extend"))
            else:
                self._log.warning(
                    "Unknown trigger action",
                    extra={"action": action, "trigger_id": trigger.get("trigger_id")},
                )

            self._safe_delete(path)

    # ------------------------------------------------------------------
    # Trigger handlers
    # ------------------------------------------------------------------

    def _handle_start(self, trigger: dict) -> None:
        """Validate, acquire semaphore, dump pre-roll, and start a DiskWriter.

        Args:
            trigger: Parsed trigger dictionary conforming to the project schema.
        """
        trigger_id: str = trigger["trigger_id"]
        cameras: List[str] = trigger.get("cameras", ["all"])
        pre_roll_sec: float = float(trigger.get("pre_roll_sec", self._config.pre_roll_sec))
        max_duration_sec: float = float(trigger.get("max_duration_sec", 300))

        if trigger_id in self._active_writers:
            self._log.warning(
                "Duplicate start trigger — ignoring",
                extra={"trigger_id": trigger_id},
            )
            return

        target_cams = (
            list(self._stream_buffers.keys())
            if cameras == ["all"]
            else [c for c in cameras if c in self._stream_buffers]
        )

        if not target_cams:
            self._log.error(
                "No valid cameras for trigger",
                extra={"trigger_id": trigger_id, "requested": cameras},
            )
            return

        # Semaphore: non-blocking acquire; warn if cap reached
        if not self._writer_semaphore.acquire(blocking=False):
            self._log.warning(
                "Concurrent writer cap reached — trigger dropped",
                extra={
                    "trigger_id": trigger_id,
                    "max_concurrent": self._config.max_concurrent_writers,
                },
            )
            return

        # Disk space check
        if not self._check_disk_space():
            self._writer_semaphore.release()
            return

        # Pick the first target camera for this clip (multi-cam extension left
        # as a per-intersection policy in the analysis layer).
        cam_id = target_cams[0]
        buf = self._stream_buffers[cam_id]

        output_path = (
            Path(self._config.output_dir)
            / f"{trigger_id}_{cam_id}_{int(time.time())}.mp4"
        )

        writer = DiskWriter(
            output_path=output_path,
            fallback_fps=buf.fps,
            fourcc_str=self._config.fourcc,
            semaphore=self._writer_semaphore,
            trigger_id=trigger_id,
        )
        writer.start()

        # Dump pre-roll frames accumulated up to pre_roll_sec
        pre_roll_frames = buf.drain_preroll()
        fps = buf.fps or self._config.fps_fallback
        max_preroll_frames = int(fps * pre_roll_sec)
        for frame_item in pre_roll_frames[-max_preroll_frames:]:
            writer.push(frame_item)

        self._active_writers[trigger_id] = writer
        self._active_cameras[cam_id] = trigger_id
        # Watermark starts at the timestamp of the last pre-roll frame so
        # _feed_active_writers does not re-push frames already sent above.
        preroll_last_ts = pre_roll_frames[-1][1] if pre_roll_frames else 0.0
        self._writer_watermarks[trigger_id] = preroll_last_ts

        # Schedule automatic stop after max_duration_sec
        stopper = threading.Timer(
            interval=max_duration_sec,
            function=self._auto_stop,
            args=(trigger_id, cam_id),
        )
        stopper.daemon = True
        stopper.start()

        self._log.info(
            "Recording started",
            extra={
                "trigger_id": trigger_id,
                "camera_id": cam_id,
                "output_path": str(output_path),
                "pre_roll_frames_dumped": len(pre_roll_frames[-max_preroll_frames:]),
                "max_duration_sec": max_duration_sec,
            },
        )

        self._log_discrepancy_to_csv(trigger, output_path)

    def _handle_stop(self, trigger: dict, extend: bool = False) -> None:
        """Send the stop sentinel to the matching DiskWriter.

        Args:
            trigger: Parsed trigger dictionary.
            extend: If ``True``, the "extend" action is treated as a plain stop
                    for now; future logic may append additional post-roll.
        """
        trigger_id: str = trigger["trigger_id"]

        if trigger_id not in self._active_writers:
            self._log.warning(
                "Stop trigger for unknown writer — ignoring",
                extra={"trigger_id": trigger_id},
            )
            return

        self._active_writers[trigger_id].push(_STOP_WRITER)

        # Remove camera→trigger mapping
        self._active_cameras = {
            k: v for k, v in self._active_cameras.items() if v != trigger_id
        }
        del self._active_writers[trigger_id]
        self._writer_watermarks.pop(trigger_id, None)

        self._log.info(
            "Recording stopped",
            extra={"trigger_id": trigger_id, "action": "extend" if extend else "stop"},
        )

    def _auto_stop(self, trigger_id: str, cam_id: str) -> None:
        """Called by the safety timer when ``max_duration_sec`` elapses.

        Args:
            trigger_id: ID of the recording to terminate.
            cam_id: Camera ID associated with the recording.
        """
        if trigger_id in self._active_writers:
            self._log.warning(
                "max_duration_sec reached — force-stopping recording",
                extra={"trigger_id": trigger_id, "camera_id": cam_id},
            )
            self._active_writers[trigger_id].push(_STOP_WRITER)
            self._active_cameras.pop(cam_id, None)
            self._active_writers.pop(trigger_id, None)
            self._writer_watermarks.pop(trigger_id, None)

    # ------------------------------------------------------------------
    # Live-frame routing
    # ------------------------------------------------------------------

    def _feed_active_writers(self) -> None:
        """Push new frames from each active stream to its DiskWriter.

        Uses ``StreamBuffer.frames_since(watermark)`` to fetch only frames
        that have not yet been pushed to this writer.  The StreamBuffer deque
        is **never cleared**, so the pre-roll history remains intact for
        concurrent or back-to-back triggers on the same camera.

        The per-writer watermark is advanced to the timestamp of the last
        frame pushed each tick, guaranteeing no frame is sent twice and no
        frame is skipped.
        """
        for cam_id, trigger_id in list(self._active_cameras.items()):
            buf = self._stream_buffers.get(cam_id)
            writer = self._active_writers.get(trigger_id)
            if not buf or not writer:
                continue

            watermark = self._writer_watermarks.get(trigger_id, 0.0)
            new_frames = buf.frames_since(watermark)
            for frame_item in new_frames:
                writer.push(frame_item)

            if new_frames:
                self._writer_watermarks[trigger_id] = new_frames[-1][1]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _log_discrepancy_to_csv(self, trigger: dict, video_path: Path) -> None:
        """Append a row to the discrepancy CSV log for detector-disagreement triggers.

        Silently skips triggers whose ``reason`` is not
        ``"detector_disagreement"`` so routine and manual recordings do not
        pollute the log.  The CSV is created with a header row on first write.

        The ``timezone`` field in the trigger payload is used to convert the
        Unix ``event_timestamp`` to a human-readable local time string.  If
        the field is absent or unrecognised, UTC is used.

        Args:
            trigger: Parsed trigger dictionary conforming to the project schema.
            video_path: Filesystem path of the MP4 file that was created, used
                as the ``Video_Filename`` column value.
        """
        if trigger.get("reason") != "detector_disagreement":
            return

        csv_path = Path(self._config.output_dir) / "discrepancies_log.csv"
        write_header = not csv_path.exists()

        meta     = trigger.get("metadata", {})
        event_ts = trigger.get("event_timestamp", 0.0)
        tz_name: str = trigger.get("timezone", "UTC") or "UTC"
        display_tz   = _resolve_pytz(tz_name, self._log)

        try:
            local_dt = (
                datetime.fromtimestamp(event_ts, tz=timezone.utc)
                .astimezone(display_tz)
            )
            ts_str = local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        except (ValueError, TypeError, OSError):
            ts_str = "UNKNOWN_TIME"

        row = {
            "Local_Timestamp": ts_str,
            "Trigger_ID":      trigger.get("trigger_id", ""),
            "Video_Filename":  video_path.name,
            "Rule_Type":       meta.get("rule", ""),
            "Det_A":           meta.get("det_a", ""),
            "Det_B":           meta.get("det_b", ""),
            "Description":     meta.get("description", ""),
        }

        try:
            with csv_path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except OSError as exc:
            self._log.error(
                "Failed to write to discrepancy CSV log",
                extra={"error": str(exc)},
            )

    @staticmethod
    def _read_trigger(path: Path) -> dict:
        """Read and parse a JSON trigger file.

        Args:
            path: Filesystem path to the ``.json`` file.

        Returns:
            Parsed trigger dictionary.

        Raises:
            json.JSONDecodeError: If the file is not valid JSON.
            KeyError: If mandatory fields are absent.
            ValueError: If field values are semantically invalid.
        """
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        # Minimal schema validation
        required = {"trigger_id", "action", "event_timestamp"}
        missing = required - data.keys()
        if missing:
            raise KeyError(f"Trigger missing required fields: {missing}")

        return data

    def _check_disk_space(self) -> bool:
        """Return ``True`` if free disk space exceeds ``min_free_disk_mb``.

        Logs a structured error and returns ``False`` if space is insufficient.
        """
        free_mb = shutil.disk_usage(self._config.output_dir).free / (1024 * 1024)
        if free_mb < self._config.min_free_disk_mb:
            self._log.error(
                "Insufficient disk space — recording aborted",
                extra={
                    "free_mb": round(free_mb, 1),
                    "required_mb": self._config.min_free_disk_mb,
                    "output_dir": self._config.output_dir,
                },
            )
            return False
        return True

    @staticmethod
    def _safe_delete(path: Path) -> None:
        """Delete a trigger file, suppressing errors if already gone.

        Args:
            path: Path to remove.
        """
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("Could not delete trigger file", extra={"path": str(path), "error": str(exc)})


# ---------------------------------------------------------------------------
# Example __main__ entry-point (for manual smoke testing only)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    config = VideoBufferConfig(
        streams={
            "cam1": "rtsp://192.168.1.10/stream1",
            "cam2": "rtsp://192.168.1.11/stream1",
        },
        trigger_dir="./trigger_queue",
        output_dir="./completed_videos",
        pre_roll_sec=10.0,
        poll_interval_sec=2.0,
        max_concurrent_writers=2,
        min_free_disk_mb=500.0,
    )

    manager = VideoBufferManager(config)

    import signal

    def _shutdown(signum, frame):  # noqa: ANN001
        log.info("Shutdown signal received")
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    manager.start()  # blocks
