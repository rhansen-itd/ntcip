"""
video_buffer.py — Core module for the video_buffer package.
Fixed: Implemented true sequential frame consumption and precise dynamic FPS 
       calculation to match real-world stream duration perfectly.
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

log = logging.getLogger(__name__)

def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> pytz.BaseTzInfo:
    try:
        return pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        fallback_log.warning(
            "Unknown IANA timezone name; falling back to UTC",
            extra={"timezone": tz_name},
        )
        return pytz.utc


@dataclass
class VideoBufferConfig:
    streams: Dict[str, str]
    trigger_dir: str = "./trigger_queue"
    output_dir: str = "./completed_videos"
    pre_roll_sec: float = 10.0
    poll_interval_sec: float = 2.0
    max_concurrent_writers: int = 2
    min_free_disk_mb: float = 500.0
    fourcc: str = "mp4v"
    fps_fallback: float = 15.0


class _StopWriter:
    __slots__ = ()

_STOP_WRITER = _StopWriter()


# ---------------------------------------------------------------------------
# StreamBuffer — Continuous reading into a thread-safe list/deque
# ---------------------------------------------------------------------------
class StreamBuffer:
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

        self._deque: collections.deque = collections.deque()
        self._deque_lock = threading.Lock()

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._log = logging.getLogger(f"{__name__}.stream.{camera_id}")

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(
            target=self._capture_loop,
            name=f"capture-{self.camera_id}",
            daemon=True,
        )
        self._thread.start()
        self._log.info("StreamBuffer started", extra={"camera_id": self.camera_id, "url": self.url})

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        if self._cap:
            self._cap.release()
        self._log.info("StreamBuffer stopped", extra={"camera_id": self.camera_id})

    def drain_preroll(self) -> List[Any]:
        """Returns a copy of the pre-roll deque without clearing it."""
        with self._deque_lock:
            return list(self._deque)

    def flush_new_frames(self) -> List[Any]:
        """Drains and returns all frames currently in the deque, clearing it.
        Used by the live routing engine to get a true chronological sequence.
        """
        with self._deque_lock:
            frames = list(self._deque)
            self._deque.clear()
            return frames

    @property
    def fps(self) -> float:
        return self._fps

    @property
    def frame_shape(self) -> Optional[tuple]:
        return self._frame_shape

    def _open(self) -> bool:
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
# DiskWriter — Two-pass/Post-processed FPS standardizer
# ---------------------------------------------------------------------------
class DiskWriter:
    """Consumes frames from an internal list and defers creation of the VideoWriter
    until the recording is complete. This allows us to calculate an exact mathematical
    FPS based on the true number of frames and true elapsed duration.
    """

    def __init__(
        self,
        output_path: Path,
        fallback_fps: float,
        fourcc_str: str,
        semaphore: threading.Semaphore,
        trigger_id: str,
    ) -> None:
        self._output_path = output_path
        self._fallback_fps = fallback_fps
        self._fourcc_str = fourcc_str
        self._semaphore = semaphore
        self._trigger_id = trigger_id
        
        self._frame_queue: queue.Queue = queue.Queue()
        self._collected_frames: List[tuple] = []
        
        self._thread = threading.Thread(
            target=self._write_loop,
            name=f"writer-{trigger_id[:8]}",
            daemon=True,
        )
        self._log = logging.getLogger(f"{__name__}.disk_writer")

    def start(self) -> None:
        self._thread.start()

    def push(self, item: Any) -> None:
        self._frame_queue.put(item)

    def _write_loop(self) -> None:
        try:
            # Phase 1: Collect all frames sequentially until _STOP_WRITER arrives
            while True:
                item = self._frame_queue.get()
                if isinstance(item, _StopWriter):
                    break
                self._collected_frames.append(item)

            if not self._collected_frames:
                self._log.warning("No frames collected for this trigger. Aborting write.", extra={"trigger_id": self._trigger_id})
                return

            # Phase 2: Compute exact required FPS to match real-world duration
            first_frame_ts = self._collected_frames[0][1]
            last_frame_ts = self._collected_frames[-1][1]
            total_duration = last_frame_ts - first_frame_ts
            total_frames = len(self._collected_frames)

            # Prevent division by zero; enforce fallback if time span is invalid
            if total_duration > 0.1:
                final_fps = total_frames / total_duration
            else:
                final_fps = self._fallback_fps

            self._log.info(
                "Calculating final stream metrics",
                extra={
                    "trigger_id": self._trigger_id,
                    "total_frames": total_frames,
                    "measured_duration_sec": round(total_duration, 2),
                    "computed_fps": round(final_fps, 2)
                }
            )

            # Phase 3: Initialize VideoWriter and commit frames to disk
            writer: Optional[cv2.VideoWriter] = None
            for frame, _ts in self._collected_frames:
                if writer is None:
                    h, w = frame.shape[:2]
                    fourcc = cv2.VideoWriter_fourcc(*self._fourcc_str)
                    writer = cv2.VideoWriter(
                        str(self._output_path), fourcc, final_fps, (w, h)
                    )
                    if not writer.isOpened():
                        self._log.error(
                            "VideoWriter failed to open",
                            extra={"trigger_id": self._trigger_id, "output_path": str(self._output_path)},
                        )
                        return

                writer.write(frame)

            if writer:
                writer.release()

        finally:
            self._semaphore.release()
            self._log.info(
                "Recording finished and sealed",
                extra={
                    "trigger_id": self._trigger_id,
                    "output_path": str(self._output_path),
                },
            )


# ---------------------------------------------------------------------------
# VideoBufferManager — Stream-draining orchestration engine
# ---------------------------------------------------------------------------
class VideoBufferManager:
    def __init__(self, config: VideoBufferConfig) -> None:
        self._config = config
        self._stream_buffers: Dict[str, StreamBuffer] = {}
        self._active_writers: Dict[str, DiskWriter] = {}   
        self._active_cameras: Dict[str, str] = {}          
        self._writer_semaphore = threading.Semaphore(config.max_concurrent_writers)
        self._running = False
        self._log = logging.getLogger(f"{__name__}.manager")

        Path(config.trigger_dir).mkdir(parents=True, exist_ok=True)
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

    def start(self) -> None:
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
        self._running = False
        for buf in self._stream_buffers.values():
            buf.stop()
        self._log.info("VideoBufferManager stopped")

    def _poll_loop(self) -> None:
        last_scan: float = 0.0

        while self._running:
            now = time.monotonic()

            # True stream frame routing happens here
            self._feed_active_writers()

            if (now - last_scan) >= self._config.poll_interval_sec:
                self._scan_trigger_dir()
                last_scan = time.monotonic()
                
            time.sleep(0.01)

    def _scan_trigger_dir(self) -> None:
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

    def _handle_start(self, trigger: dict) -> None:
        trigger_id: str = trigger["trigger_id"]
        cameras: List[str] = trigger.get("cameras", ["all"])
        pre_roll_sec: float = float(trigger.get("pre_roll_sec", self._config.pre_roll_sec))
        max_duration_sec: float = float(trigger.get("max_duration_sec", 300))

        if trigger_id in self._active_writers:
            return

        target_cams = (
            list(self._stream_buffers.keys())
            if cameras == ["all"]
            else [c for c in cameras if c in self._stream_buffers]
        )

        if not target_cams:
            return

        # Single-camera assumption (shared with the remux backend): only
        # target_cams[0] is recorded. See config_manager.py's trigger schema.
        if len(target_cams) > 1:
            self._log.warning(
                "Trigger resolved to multiple cameras — recording only the first "
                "(single-camera assumption; per-camera writers not implemented)",
                extra={
                    "trigger_id": trigger_id,
                    "cameras_requested": target_cams,
                    "cameras_recorded": target_cams[:1],
                },
            )

        if not self._writer_semaphore.acquire(blocking=False):
            self._log.warning("Concurrent writer cap reached — trigger dropped", extra={"trigger_id": trigger_id})
            return

        if not self._check_disk_space():
            self._writer_semaphore.release()
            return

        cam_id = target_cams[0]
        buf = self._stream_buffers[cam_id]

        output_path = (
            Path(self._config.output_dir)
            / f"{trigger_id}_{cam_id}_{int(time.time())}.mp4"
        )

        # Pass buffer FPS as a fallback standardizer
        writer = DiskWriter(
            output_path=output_path,
            fallback_fps=buf.fps,
            fourcc_str=self._config.fourcc,
            semaphore=self._writer_semaphore,
            trigger_id=trigger_id,
        )
        writer.start()

        # Safely extract pre-roll segment chronologically
        pre_roll_frames = buf.drain_preroll()
        fps = buf.fps or self._config.fps_fallback
        max_preroll_frames = int(fps * pre_roll_sec)
        for frame_item in pre_roll_frames[-max_preroll_frames:]:
            writer.push(frame_item)

        self._active_writers[trigger_id] = writer
        self._active_cameras[cam_id] = trigger_id

        stopper = threading.Timer(
            interval=max_duration_sec,
            function=self._auto_stop,
            args=(trigger_id, cam_id),
        )
        stopper.daemon = True
        stopper.start()

        self._log_discrepancy_to_csv(trigger, output_path)

    def _handle_stop(self, trigger: dict, extend: bool = False) -> None:
        trigger_id: str = trigger["trigger_id"]

        if trigger_id not in self._active_writers:
            return

        self._active_writers[trigger_id].push(_STOP_WRITER)

        self._active_cameras = {
            k: v for k, v in self._active_cameras.items() if v != trigger_id
        }
        del self._active_writers[trigger_id]

    def _auto_stop(self, trigger_id: str, cam_id: str) -> None:
        if trigger_id in self._active_writers:
            self._active_writers[trigger_id].push(_STOP_WRITER)
            self._active_cameras.pop(cam_id, None)
            self._active_writers.pop(trigger_id, None)

    def _feed_active_writers(self) -> None:
        """Consumes all unwritten frames chronologically from the buffer."""
        for cam_id, trigger_id in list(self._active_cameras.items()):
            buf = self._stream_buffers.get(cam_id)
            writer = self._active_writers.get(trigger_id)
            if not buf or not writer:
                continue

            # Flush everything that accumulated in the buffer since our last loop tick
            new_frames = buf.flush_new_frames()
            for frame_item in new_frames:
                writer.push(frame_item)

    def _log_discrepancy_to_csv(self, trigger: dict, video_path: Path) -> None:
        if trigger.get("reason") != "detector_disagreement":
            return

        csv_path = Path(self._config.output_dir) / "discrepancies_log.csv"
        write_header = not csv_path.exists()

        meta = trigger.get("metadata", {})
        event_ts = trigger.get("event_timestamp", 0.0)

        tz_name: str = trigger.get("timezone", "UTC") or "UTC"
        display_tz = _resolve_pytz(tz_name, self._log)

        try:
            local_dt = datetime.fromtimestamp(event_ts, tz=timezone.utc).astimezone(display_tz)
            ts_str = local_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        except (ValueError, TypeError, OSError):
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
            self._log.error("Failed to write to discrepancy CSV log", extra={"error": str(exc)})

    @staticmethod
    def _read_trigger(path: Path) -> dict:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        required = {"trigger_id", "action", "event_timestamp"}
        missing = required - data.keys()
        if missing:
            raise KeyError(f"Trigger missing required fields: {missing}")
        return data

    def _check_disk_space(self) -> bool:
        free_mb = shutil.disk_usage(self._config.output_dir).free / (1024 * 1024)
        if free_mb < self._config.min_free_disk_mb:
            return False
        return True

    @staticmethod
    def _safe_delete(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("Could not delete trigger file", extra={"path": str(path), "error": str(exc)})