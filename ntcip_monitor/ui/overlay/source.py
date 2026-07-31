"""Background image sources for the live video overlay (ROADMAP Item 11b).

The overlay page draws shapes on a ``<canvas>`` stacked over a background
image. Where that image comes from is pluggable:

* ``file`` — :class:`FileImageSource`, a still on disk (a calibration grab).
  Implemented here.
* ``live`` — an MJPEG feed decoded from the camera's RTSP stream. That is
  ROADMAP Item 11c; :func:`create_background_source` raises
  :class:`NotImplementedError` for it today.

Both feed the same canvas, so the page's coordinate math is identical either
way and a deployment can fall back to a still when the camera is unreachable.

This module is stdlib-only on the ``file`` path — no Flask, no OpenCV, no
``video_engine`` import. The overlay lives in ``ntcip_monitor`` and must not
reach into the video engine (ROADMAP Item 11, decision D).
"""

from __future__ import annotations

import logging
import mimetypes
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

#: Served when the file's suffix isn't a recognised image type.
DEFAULT_CONTENT_TYPE = "image/jpeg"


class BackgroundSource(ABC):
    """A source of background images for the overlay canvas.

    Implementations must be safe to call from Flask worker threads: the
    ``file`` source is polled by every page load, and the ``live`` source of
    Item 11c is shared by every viewer.

    Attributes:
        kind: Short identifier matching the config's ``background`` value
            (``"file"`` / ``"live"``). The page template branches on it.
    """

    kind: str = "unknown"

    @abstractmethod
    def get_image(self) -> Optional[Tuple[bytes, str]]:
        """Return the current background image.

        Returns:
            tuple | None: ``(image_bytes, content_type)``, or ``None`` when no
            image is available yet (missing file, stream not connected).
        """

    def supports_stream(self) -> bool:
        """Whether this source can serve ``/api/overlay/stream``.

        Returns:
            bool: False here; the live source of Item 11c overrides it.
        """
        return False

    def mjpeg_frames(self):
        """Yield JPEG frames for one streaming client.

        The HTTP multipart framing lives in ``web_ui.py`` so this module needs
        no Flask; a source only has to produce bytes and clean up after itself
        when the generator is closed.

        Yields:
            bytes: One complete JPEG image per frame.

        Raises:
            NotImplementedError: Unless :meth:`supports_stream` is True.
        """
        raise NotImplementedError(
            f'{type(self).__name__} does not support streaming'
        )

    def close(self) -> None:
        """Release any resources held by the source. Idempotent."""


class FileImageSource(BackgroundSource):
    """A still image read from disk, re-read when the file changes.

    The mtime/size check means an operator can drop a fresh calibration still
    over the configured path and see it in the browser without restarting the
    monitor — the same "swap the file, don't restart the process" property the
    shape CSV has.

    A read failure is never fatal: the last good bytes keep being served (and
    ``None`` is returned only if nothing was ever read), because a half-written
    replacement image should not blank the operator's page.
    """

    kind = "file"

    def __init__(self, path):
        """Initialize the source.

        Args:
            path: Path to the image file. Relative paths resolve against the
                process's working directory, like ``config.json`` itself.
        """
        self.path = Path(path)
        self._content_type = (
            mimetypes.guess_type(self.path.name)[0] or DEFAULT_CONTENT_TYPE
        )
        self._lock = threading.Lock()
        self._data: Optional[bytes] = None
        self._stamp: Optional[Tuple[float, int]] = None
        self._missing_logged = False

    def get_image(self) -> Optional[Tuple[bytes, str]]:
        """Return the file's bytes, re-reading it if it changed on disk.

        Returns:
            tuple | None: ``(image_bytes, content_type)``, or ``None`` if the
            file has never been readable.
        """
        try:
            stat = self.path.stat()
            stamp = (stat.st_mtime, stat.st_size)
        except OSError as exc:
            self._log_missing(exc)
            with self._lock:
                return (self._data, self._content_type) if self._data else None

        with self._lock:
            if self._data is not None and stamp == self._stamp:
                return (self._data, self._content_type)

        # Read outside the lock — this is disk I/O, and a concurrent reader
        # should keep getting the previous image rather than block on it.
        try:
            data = self.path.read_bytes()
        except OSError as exc:
            self._log_missing(exc)
            with self._lock:
                return (self._data, self._content_type) if self._data else None

        if not data:
            # A zero-byte file is almost always a copy in progress.
            with self._lock:
                return (self._data, self._content_type) if self._data else None

        with self._lock:
            reloaded = self._data is not None
            self._data = data
            self._stamp = stamp
            self._missing_logged = False
            content_type = self._content_type

        if reloaded:
            logger.info(
                "Overlay background reloaded from %s (%d bytes)",
                self.path, len(data),
                extra={
                    "event": "overlay_background_reloaded",
                    "path": str(self.path),
                    "bytes": len(data),
                },
            )
        return (data, content_type)

    def _log_missing(self, exc: OSError) -> None:
        """Log an unreadable background file once per outage.

        Args:
            exc: The error raised by the failed stat/read.
        """
        with self._lock:
            if self._missing_logged:
                return
            self._missing_logged = True
        logger.warning(
            "Overlay background image unreadable: %s (%s)", self.path, exc,
            extra={
                "event": "overlay_background_unreadable",
                "path": str(self.path),
                "error": str(exc),
            },
        )


def create_background_source(config: Optional[Mapping[str, Any]]) -> BackgroundSource:
    """Build the background source described by an ``overlay`` config section.

    Args:
        config: The ``overlay`` dict from ``config.json``. Recognised keys:
            ``background`` (``"file"`` or ``"live"``, default ``"file"``),
            ``image_path`` (required for ``file``), and — for ``live``, in
            Item 11c — ``camera_url`` / ``stream_fps``.

    Returns:
        BackgroundSource: A ready-to-use source.

    Raises:
        ValueError: If ``background`` is unrecognised, or ``image_path`` is
            missing for the ``file`` source.
        NotImplementedError: If ``background`` is ``"live"`` — that source
            arrives with ROADMAP Item 11c.
    """
    settings = config or {}
    kind = str(settings.get("background") or "file").strip().lower()

    if kind == "file":
        image_path = settings.get("image_path")
        if not image_path:
            raise ValueError(
                'overlay.background is "file" but overlay.image_path is not set'
            )
        return FileImageSource(image_path)

    if kind == "live":
        raise NotImplementedError(
            'overlay.background "live" is not implemented yet (ROADMAP Item '
            '11c); use "file" with an image_path until then'
        )

    raise ValueError(
        f'Unknown overlay.background {kind!r}: expected "file" or "live"'
    )
