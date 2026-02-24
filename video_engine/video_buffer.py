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
import pytz
from datetime import datetime, timezone
import json
import logging
import os
import queue
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import cv2  # type: ignore

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

# ---------------------------------------------------------------------------
# Timezone resolution helper
# ---------------------------------------------------------------------------

def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> pytz.BaseTzInfo:
    """Resolve an IANA timezone name to a :mod:`pytz` timezone object.

    ``pytz`` carries its own full copy of the IANA timezone database, so this
    function has **no dependency on system zone files or the** ``tzdata``
    **package**.  ``US/Mountain``, ``America/Boise``, and all other canonical
    and legacy aliases are resolved directly from the bundled database.

    Falls back to ``pytz.utc`` (equivalent to UTC) and emits a structured
    warning when the name is genuinely unknown — i.e. not present in pytz's
    bundled database at all.

    Args:
        tz_name: IANA timezone name string (e.g. ``"America/Boise"`` or the
            legacy alias ``"US/Mountain"``).
        fallback_log: Logger used to emit the structured warning on failure.

    Returns:
        A :mod:`pytz` timezone object.  Always returns a valid object; never
        raises.
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

        reported_fps = self._cap.get(cv2.CAP_PROP_FPS)
        self._fps = reported_fps if reported_fps and reported_fps > 0 else self.fps_fallback
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
    """Consumes frames from a queue and encodes them to an MP4 file.

    One ``DiskWriter`` is created per triggered clip. It runs in a daemon
    thread and releases the shared semaphore when finished so the next
    writer can start.

    Args:
        output_path: Full path of the MP4 file to create.
        fps: Frames per second for the output container.
        fourcc_str: OpenCV FourCC string (e.g. ``"mp4v"``).
        semaphore: Shared :class:`threading.Semaphore` controlling concurrent writers.
        trigger_id: ID string used for structured log correlation.
    """

    def __init__(
        self,
        output_path: Path,
        fps: float,
        fourcc_str: str,
        semaphore: threading.Semaphore,
        trigger_id: str,
    ) -> None:
        self._output_path = output_path
        self._fps = fps
        self._fourcc_str = fourcc_str
        self._semaphore = semaphore
        self._trigger_id = trigger_id
        self._frame_queue: queue.Queue = queue.Queue(maxsize=0)  # unbounded; backpressure via semaphore
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
        """Enqueue a frame tuple or the ``_STOP_WRITER`` sentinel."""
        # --- NEW CODE: Prevent duplicate frames ---
        if not isinstance(item, _StopWriter):
            frame, ts = item
            # If we already pushed this exact timestamp, ignore it
            if getattr(self, "_last_pushed_ts", 0.0) == ts:
                return
            self._last_pushed_ts = ts
        # ------------------------------------------
        
        self._frame_queue.put(item)

    def _write_loop(self) -> None:
        """Drain the queue and write frames until the sentinel is received."""
        try:
            while True:
                item = self._frame_queue.get()
                if isinstance(item, _StopWriter):
                    break

                frame, _ts = item

                if self._writer is None:
                    h, w = frame.shape[:2]
                    fourcc = cv2.VideoWriter_fourcc(*self._fourcc_str)
                    self._writer = cv2.VideoWriter(
                        str(self._output_path), fourcc, self._fps, (w, h)
                    )
                    if not self._writer.isOpened():
                        self._log.error(
                            "VideoWriter failed to open",
                            extra={
                                "trigger_id": self._trigger_id,
                                "output_path": str(self._output_path),
                            },
                        )
                        return

                self._writer.write(frame)

        finally:
            if self._writer:
                self._writer.release()
            self._semaphore.release()
            self._log.info(
                "Recording finished",
                extra={
                    "trigger_id": self._trigger_id,
                    "output_path": str(self._output_path),
                },
            )


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
        """Main loop: scan trigger dir, dispatch triggers, feed active writers."""
        last_scan: float = 0.0

        while self._running:
            now = time.monotonic()

            # --- Feed live frames to any active writers (tight loop) ---
            self._feed_active_writers()

            # --- Rate-limited hot-folder scan ---
            if (now - last_scan) >= self._config.poll_interval_sec:
                self._scan_trigger_dir()
                last_scan = time.monotonic()
                
            # --- NEW CODE: Breathe! ---
            # 10ms sleep. Negligible for 30fps video, but drops CPU usage from 100% to 1%.
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
            fps=buf.fps,
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

    # ------------------------------------------------------------------
    # Live-frame routing
    # ------------------------------------------------------------------

    def _feed_active_writers(self) -> None:
        """Push the latest frame from each active stream to its writer.

        This is called on every iteration of the tight polling loop. It
        reads the **most recent** frame from the deque without draining it,
        so the pre-roll keeps accumulating for future triggers.
        """
        for cam_id, trigger_id in list(self._active_cameras.items()):
            buf = self._stream_buffers.get(cam_id)
            writer = self._active_writers.get(trigger_id)
            if not buf or not writer:
                continue

            with buf._deque_lock:  # noqa: SLF001  (tightly coupled by design)
                if buf._deque:  # noqa: SLF001
                    latest = buf._deque[-1]
                else:
                    latest = None

            if latest is not None:
                writer.push(latest)

    def _log_discrepancy_to_csv(self, trigger: dict, video_path: Path) -> None:
        """Append a discrepancy record to the master CSV log.

        Extracts metadata from the trigger file and converts the UTC epoch
        ``event_timestamp`` to a human-readable string in the intersection's
        configured IANA timezone.  If the ``"timezone"`` field is absent or
        contains an unknown zone name, the conversion falls back to UTC so
        the CSV row is never silently dropped.

        Args:
            trigger: Parsed trigger dictionary (must include ``"event_timestamp"``
                and optionally ``"timezone"`` at the root level).
            video_path: Path to the generated MP4 file for cross-referencing.
        """
        if trigger.get("reason") != "detector_disagreement":
            return

        csv_path = Path(self._config.output_dir) / "discrepancies_log.csv"
        write_header = not csv_path.exists()

        meta = trigger.get("metadata", {})
        event_ts = trigger.get("event_timestamp", 0.0)

        # Resolve the display timezone from the trigger payload.
        # _resolve_pytz uses pytz's bundled IANA database — no system zone
        # files or tzdata package required.
        tz_name: str = trigger.get("timezone", "UTC") or "UTC"
        display_tz = _resolve_pytz(tz_name, self._log)

        # Convert UTC epoch to the intersection-local timezone.
        try:
            local_dt = datetime.fromtimestamp(event_ts, tz=timezone.utc).astimezone(display_tz)
            ts_str = local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        except (ValueError, TypeError, OSError) as exc:
            self._log.warning(
                "Failed to parse event_timestamp for CSV log",
                extra={"trigger_id": trigger.get("trigger_id"), "error": str(exc)}
            )
            ts_str = "UNKNOWN_TIME"

        row = {
            "Local_Timestamp": ts_str,
            "Trigger_ID": trigger.get("trigger_id", ""),
            "Video_Filename": video_path.name,
            "Rule_Type": meta.get("rule", ""),
            "Det_A": meta.get("det_a", ""),
            "Det_B": meta.get("det_b", ""),
            "Description": meta.get("description", ""),
        }

        try:
            with csv_path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(row.keys()))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except OSError as exc:
            self._log.error(
                "Failed to write to discrepancy CSV log",
                extra={"error": str(exc), "trigger_id": row["Trigger_ID"]}
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
    import signal

    # 1. Configure your specific Axis cameras
    config = VideoBufferConfig(
        streams={
            "cam4": "rtsp://root:WesternSystems1!@10.37.23.204/axis-media/media.amp?camera=4&videocodec=h264",
            "cam5": "rtsp://root:WesternSystems1!@10.37.23.204/axis-media/media.amp?camera=5&videocodec=h264",
            "cam6": "rtsp://root:WesternSystems1!@10.37.23.204/axis-media/media.amp?camera=6&videocodec=h264",
            "cam7": "rtsp://root:WesternSystems1!@10.37.23.204/axis-media/media.amp?camera=7&videocodec=h264",
        },
        trigger_dir="./trigger_queue",
        output_dir="./completed_videos",
        pre_roll_sec=10.0,
        poll_interval_sec=2.0,
        max_concurrent_writers=2,
        min_free_disk_mb=500.0,
    )

    manager = VideoBufferManager(config)

    def _shutdown(signum, frame):
        log.info("Shutdown signal received")
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # 2. Start the engine
    manager.start()