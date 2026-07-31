"""Background image sources for the live video overlay (ROADMAP Items 11b/11c).

The overlay page draws shapes on a ``<canvas>`` stacked over a background
image. Where that image comes from is pluggable:

* ``file`` — :class:`FileImageSource`, a still on disk (a calibration grab).
* ``live`` — :class:`RtspMjpegSource`, JPEG frames decoded from the camera's
  RTSP stream with PyAV and re-encoded as MJPEG (Item 11c).

Both feed the same canvas, so the page's coordinate math is identical either
way and a deployment can fall back to a still when the camera is unreachable.

This module is stdlib-only apart from PyAV, which is imported defensively and
only ever touched on the ``live`` path — no Flask, no OpenCV, no
``video_engine`` import. The overlay lives in ``ntcip_monitor`` and must not
reach into the video engine (ROADMAP Item 11, decision D); it also must stay
importable on a bare interpreter so the unit tests run with nothing installed.
The HTTP multipart framing lives in ``web_ui.py``, not here.
"""

from __future__ import annotations

import logging
import mimetypes
import threading
import time
from abc import ABC, abstractmethod
from fractions import Fraction
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Tuple

try:  # PyAV is required only by the ``live`` source; see the module docstring.
    import av
except ImportError:  # pragma: no cover - exercised only on a bare interpreter
    av = None

logger = logging.getLogger(__name__)

#: Served when the file's suffix isn't a recognised image type.
DEFAULT_CONTENT_TYPE = "image/jpeg"

#: Errors a decode/connect attempt may raise; PyAV's are added when available.
_STREAM_ERRORS: Tuple[type, ...] = (OSError, RuntimeError, ValueError)
if av is not None:  # pragma: no branch - trivial
    _STREAM_ERRORS = _STREAM_ERRORS + (av.FFmpegError,)


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


class _DecoderSession:
    """Liveness token for one decoder thread.

    The thread loops while ``running`` is True. Giving each thread its own
    token (rather than sharing one ``self._running`` flag) means a thread that
    is shutting down can never stop its successor: the shutdown path clears
    ``self._session`` only when it still points at the retiring thread's token.
    Same idea as the generation counter on the remux manager's stop timers.
    """

    __slots__ = ("running",)

    def __init__(self) -> None:
        self.running = True


class RtspMjpegSource(BackgroundSource):
    """Live camera frames, decoded once and shared by every viewer.

    One decoder thread per source — not per client. Viewers (an
    ``/api/overlay/stream`` generator, or a single ``/api/overlay/background``
    request) attach as ref-counted subscribers; the thread is opened lazily on
    the first subscriber and torn down ``idle_grace_sec`` after the last one
    leaves. So an idle page costs no RTSP session at all, and N open tabs cost
    one stream off the intersection rather than N.

    Frames are decoded at the source rate but encoded and published at
    ``stream_fps`` — encoding is the expensive half, and the page only needs a
    background for shapes whose own resolution is the ~1-1.5 s SNMP sweep.

    Locking follows the discipline CLAUDE.md documents for
    ``remux_video_buffer.VideoBufferManager``: **under the lock, decide and
    collect; release; then act.** The condition is held only to publish a
    finished JPEG (a reference assignment plus ``notify_all``) or to read
    bookkeeping — never across a connect, a decode, an encode, or a socket
    write.

    Attributes:
        url: The RTSP (or file, for the self-test) URL being decoded.
        stream_fps: Publish rate ceiling, in frames per second.
    """

    kind = "live"

    #: JPEG quantiser bounds are 1 (best) - 31 (worst) in FFmpeg's mjpeg encoder.
    MIN_QUALITY = 1
    MAX_QUALITY = 31

    def __init__(
        self,
        url: str,
        stream_fps: float = 5.0,
        quality: int = 12,
        rtsp_transport: str = "tcp",
        open_timeout_sec: float = 10.0,
        read_timeout_sec: float = 5.0,
        idle_grace_sec: float = 10.0,
        first_frame_timeout_sec: float = 8.0,
        keepalive_sec: float = 2.0,
        base_backoff_sec: float = 1.0,
        max_backoff_sec: float = 30.0,
    ) -> None:
        """Initialize the source. No connection is made until a subscriber arrives.

        Args:
            url: Camera URL passed to ``av.open`` (``rtsp://...``; a file path
                works too, which is how the self-test runs without a camera).
            stream_fps: Maximum frames published per second. Frames decoded in
                between are dropped without being encoded.
            quality: MJPEG quantiser, 1 (best) to 31 (worst). Clamped.
            rtsp_transport: ``av.open`` transport hint; ``tcp`` avoids the UDP
                packet loss the remux buffer also steers around.
            open_timeout_sec: Connect timeout.
            read_timeout_sec: Per-read timeout. Also bounds how long a dead
                stream can delay idle teardown.
            idle_grace_sec: How long the decoder keeps running after the last
                subscriber leaves. Covers a page reload without dropping and
                re-establishing the RTSP session.
            first_frame_timeout_sec: How long :meth:`get_image` waits for a
                frame when the stream is cold.
            keepalive_sec: While no new frame arrives, a streaming client is
                re-sent the last good one this often, so the ``<img>`` survives
                a reconnect instead of showing a broken image.
            base_backoff_sec: First reconnect delay; doubles per failure.
            max_backoff_sec: Ceiling for the reconnect backoff.
        """
        self.url = url
        self.stream_fps = max(float(stream_fps or 0.0), 0.1)
        self.quality = max(self.MIN_QUALITY, min(self.MAX_QUALITY, int(quality)))
        self.rtsp_transport = rtsp_transport
        self.open_timeout_sec = float(open_timeout_sec)
        self.read_timeout_sec = float(read_timeout_sec)
        self.idle_grace_sec = float(idle_grace_sec)
        self.first_frame_timeout_sec = float(first_frame_timeout_sec)
        self.keepalive_sec = float(keepalive_sec)
        self.max_backoff_sec = float(max_backoff_sec)
        self.base_backoff_sec = min(float(base_backoff_sec), self.max_backoff_sec)

        self._cond = threading.Condition()
        # Everything below is guarded by _cond.
        self._subscribers = 0
        self._last_active = time.monotonic()
        self._latest: Optional[bytes] = None
        self._latest_seq = 0
        self._session: Optional[_DecoderSession] = None
        self._thread: Optional[threading.Thread] = None
        self._closed = False

        # Touched only by the decoder thread.
        self._encoder = None
        self._encoder_size: Optional[Tuple[int, int]] = None
        self._pts = 0

    # -- public surface ----------------------------------------------------

    def supports_stream(self) -> bool:
        """Whether this source can serve ``/api/overlay/stream``.

        Returns:
            bool: Always True.
        """
        return True

    def get_image(self) -> Optional[Tuple[bytes, str]]:
        """Return the most recent decoded frame as a JPEG.

        Attaches as a subscriber for the duration, so hitting
        ``/api/overlay/background`` on a live source starts the stream the same
        way opening the MJPEG endpoint does — the still endpoint works for both
        source kinds. A page that polls it keeps the decoder warm through the
        idle grace period.

        Returns:
            tuple | None: ``(jpeg_bytes, "image/jpeg")``, or ``None`` if no
            frame arrived before ``first_frame_timeout_sec``.
        """
        self._attach()
        try:
            jpeg, _seq = self._wait_for_frame(0, self.first_frame_timeout_sec)
        finally:
            self._detach()
        if jpeg is None:
            return None
        return (jpeg, DEFAULT_CONTENT_TYPE)

    def mjpeg_frames(self) -> Iterator[bytes]:
        """Yield JPEG frames for one streaming client until it disconnects.

        The generator holds a subscription for its lifetime; ``web_ui.py`` wraps
        each frame in multipart framing. When the stream stalls (a reconnect in
        progress), the last good frame is re-sent every ``keepalive_sec`` rather
        than letting the client's ``<img>`` break.

        Yields:
            bytes: One complete JPEG image per frame.
        """
        self._attach()
        last_seq = 0
        try:
            while True:
                jpeg, seq = self._wait_for_frame(last_seq, self.keepalive_sec)
                with self._cond:
                    if self._closed:
                        return
                if jpeg is None:
                    # Nothing decoded yet — the stream is still coming up.
                    continue
                last_seq = seq
                yield jpeg
        finally:
            self._detach()

    def close(self) -> None:
        """Stop the decoder, drop the RTSP session, and end every stream. Idempotent."""
        with self._cond:
            already = self._closed
            self._closed = True
            session, self._session = self._session, None
            thread, self._thread = self._thread, None
            if session is not None:
                session.running = False
            self._cond.notify_all()

        if thread is not None and thread is not threading.current_thread():
            # Outside the lock: the decoder takes it to publish.
            thread.join(timeout=self.read_timeout_sec + 2.0)
        if not already:
            logger.info(
                "Live overlay source closed",
                extra={"event": "overlay_live_closed", "url": self.url},
            )

    def stats(self) -> dict:
        """Return a snapshot of the source's bookkeeping (for logs and tests).

        Returns:
            dict: ``subscribers``, ``running`` (a decoder thread is alive),
            ``frames`` (published so far), ``has_frame``, and ``resolution``
            (``(width, height)`` of the last encoded frame, or None). The
            resolution is read without the lock — it is a tuple reference the
            decoder thread rebinds, so a stale read is the worst case.
        """
        with self._cond:
            return {
                "subscribers": self._subscribers,
                "running": self._session is not None,
                "frames": self._latest_seq,
                "has_frame": self._latest is not None,
                "resolution": self._encoder_size,
            }

    # -- subscriber bookkeeping -------------------------------------------

    def _attach(self) -> None:
        """Register a subscriber, starting the decoder thread if it is idle."""
        with self._cond:
            self._subscribers += 1
            self._last_active = time.monotonic()
            if self._closed or self._session is not None:
                return
            session = _DecoderSession()
            thread = threading.Thread(
                target=self._decoder_loop,
                args=(session,),
                name=f"overlay-mjpeg-{id(self) & 0xFFFF:04x}",
                daemon=True,
            )
            self._session = session
            self._thread = thread

        # Start outside the lock — the thread's first act is to take it.
        thread.start()
        logger.info(
            "Live overlay decoder started",
            extra={"event": "overlay_live_started", "url": self.url},
        )

    def _detach(self) -> None:
        """Drop a subscriber, arming the idle-teardown clock if it was the last."""
        with self._cond:
            self._subscribers = max(0, self._subscribers - 1)
            self._last_active = time.monotonic()
            self._cond.notify_all()

    def _publish(self, jpeg: bytes) -> None:
        """Hand a finished frame to every waiting subscriber.

        Args:
            jpeg: Complete JPEG image bytes.
        """
        with self._cond:
            self._latest = jpeg
            self._latest_seq += 1
            self._cond.notify_all()

    def _wait_for_frame(
        self, after_seq: int, timeout: float
    ) -> Tuple[Optional[bytes], int]:
        """Wait for a frame newer than ``after_seq``.

        Args:
            after_seq: Sequence number the caller has already seen; ``0`` means
                "any frame, including one decoded before I attached".
            timeout: Maximum seconds to wait.

        Returns:
            tuple: ``(jpeg_or_None, sequence_number)``. On timeout this is the
            *current* frame — the same one the caller already saw, which is what
            makes the keepalive re-send work.
        """
        deadline = time.monotonic() + timeout
        with self._cond:
            while not self._closed and self._latest_seq <= after_seq:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(remaining)
            return self._latest, self._latest_seq

    def _retire_if_idle(self, session: _DecoderSession) -> bool:
        """Decide — under the lock — whether this decoder should stop.

        Committing the decision while holding the lock is what makes the
        start/stop race safe: a subscriber that arrives before the commit is
        counted and keeps the thread alive, and one that arrives after it finds
        ``_session is None`` and starts a fresh thread.

        Args:
            session: The calling thread's liveness token.

        Returns:
            bool: True if the caller should shut down and close its container.
        """
        with self._cond:
            if self._closed or not session.running:
                return True
            if self._subscribers > 0:
                self._last_active = time.monotonic()
                return False
            if (time.monotonic() - self._last_active) < self.idle_grace_sec:
                return False
            session.running = False
            if self._session is session:
                self._session = None
                self._thread = None
            return True

    def _wait_before_retry(self, session: _DecoderSession, seconds: float) -> None:
        """Sleep between reconnect attempts, waking early on close.

        Args:
            session: The calling thread's liveness token.
            seconds: Maximum seconds to wait.
        """
        deadline = time.monotonic() + seconds
        with self._cond:
            while session.running and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(min(remaining, 0.25))

    # -- decoding ----------------------------------------------------------

    def _open_container(self) -> Tuple[Any, Any]:
        """Open the camera stream.

        Overridable seam: the unit tests substitute a fake container so the
        subscriber bookkeeping is testable without PyAV or a camera.

        Returns:
            tuple: ``(container, video_stream)``.

        Raises:
            RuntimeError: If PyAV is not installed.
        """
        if av is None:
            raise RuntimeError(
                "PyAV is not installed; the live overlay background needs it "
                "(pip install -r requirements.txt)"
            )
        container = av.open(
            self.url,
            options={"rtsp_transport": self.rtsp_transport},
            timeout=(self.open_timeout_sec, self.read_timeout_sec),
        )
        return container, container.streams.video[0]

    def _decode(self, container: Any, stream: Any) -> Iterator[Any]:
        """Yield decoded video frames.

        Overridable seam, like :meth:`_open_container`.

        Args:
            container: The open input container.
            stream: The video stream to decode.

        Yields:
            Decoded frames.
        """
        yield from container.decode(stream)

    def _encode_jpeg(self, frame: Any) -> Optional[bytes]:
        """Encode one decoded frame as a JPEG.

        The encoder is created once per connection and reused; it is rebuilt if
        the stream's resolution changes across a reconnect. Called only from the
        decoder thread, so the encoder needs no lock.

        Args:
            frame: A decoded ``av.VideoFrame``.

        Returns:
            bytes | None: JPEG bytes, or None if the encoder produced nothing
            for this frame.
        """
        size = (frame.width, frame.height)
        if self._encoder is None or self._encoder_size != size:
            encoder = av.CodecContext.create("mjpeg", "w")
            encoder.width, encoder.height = size
            encoder.pix_fmt = "yuvj420p"
            encoder.time_base = Fraction(1, 1000)
            # FFmpeg's mjpeg encoder takes its quality from the quantiser
            # bounds; -q:v / qscale options are ignored here (verified).
            encoder.qmin = encoder.qmax = self.quality
            self._encoder = encoder
            self._encoder_size = size

        # The encoder rejects a frame whose timestamp it can't place, so stamp
        # a synthetic monotonic pts rather than passing the source's through.
        jpeg_frame = frame.reformat(format="yuvj420p")
        self._pts += 1
        jpeg_frame.pts = self._pts
        jpeg_frame.time_base = Fraction(1, 1000)
        for packet in self._encoder.encode(jpeg_frame):
            data = bytes(packet)
            if data:
                return data
        return None

    def _decoder_loop(self, session: _DecoderSession) -> None:
        """Decode, throttle, encode, and publish until idle or closed.

        Args:
            session: This thread's liveness token.
        """
        backoff = self.base_backoff_sec
        min_interval = 1.0 / self.stream_fps

        while session.running and not self._closed:
            if self._retire_if_idle(session):
                break
            try:
                container, stream = self._open_container()
            except _STREAM_ERRORS as exc:
                logger.warning(
                    "Live overlay stream unavailable — retrying in %.0fs: %s",
                    backoff, exc,
                    extra={
                        "event": "overlay_live_connect_failed",
                        "url": self.url,
                        "error": str(exc),
                        "retry_in_sec": backoff,
                    },
                )
                self._wait_before_retry(session, backoff)
                backoff = min(backoff * 2, self.max_backoff_sec)
                continue

            self._encoder = None
            self._encoder_size = None
            logger.info(
                "Live overlay stream connected",
                extra={"event": "overlay_live_connected", "url": self.url},
            )
            next_publish = 0.0
            try:
                for frame in self._decode(container, stream):
                    if not session.running or self._closed:
                        break
                    if self._retire_if_idle(session):
                        break

                    now = time.monotonic()
                    if now < next_publish:
                        continue  # Decoded, deliberately not encoded.
                    next_publish = now + min_interval

                    jpeg = self._encode_jpeg(frame)
                    if jpeg is None:
                        continue
                    self._publish(jpeg)
                    backoff = self.base_backoff_sec
            except _STREAM_ERRORS as exc:
                logger.warning(
                    "Live overlay stream dropped — reconnecting: %s", exc,
                    extra={
                        "event": "overlay_live_dropped",
                        "url": self.url,
                        "error": str(exc),
                    },
                )
            finally:
                try:
                    container.close()
                except Exception:  # noqa: BLE001 - a failed close is not fatal
                    pass

            if not session.running or self._closed:
                break
            # Clean EOF (a finite source, or a camera that ended the session)
            # is a reconnect too — the last good frame keeps serving meanwhile.
            self._wait_before_retry(session, backoff)
            backoff = min(backoff * 2, self.max_backoff_sec)

        with self._cond:
            if self._session is session:
                self._session = None
                self._thread = None
            self._cond.notify_all()
        logger.info(
            "Live overlay decoder stopped",
            extra={"event": "overlay_live_stopped", "url": self.url},
        )


def create_background_source(config: Optional[Mapping[str, Any]]) -> BackgroundSource:
    """Build the background source described by an ``overlay`` config section.

    Args:
        config: The ``overlay`` dict from ``config.json``. Recognised keys:
            ``background`` (``"file"`` or ``"live"``, default ``"file"``),
            ``image_path`` (required for ``file``), and for ``live``:
            ``camera_url`` (required), ``stream_fps`` (default 5),
            ``stream_quality`` (1 best - 31 worst, default 12), and
            ``rtsp_transport`` (default ``tcp``).

    Returns:
        BackgroundSource: A ready-to-use source. Nothing connects until the
        first viewer asks for an image.

    Raises:
        ValueError: If ``background`` is unrecognised, ``image_path`` is
            missing for the ``file`` source, or ``camera_url`` is missing for
            the ``live`` one.
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
        camera_url = settings.get("camera_url")
        if not camera_url:
            raise ValueError(
                'overlay.background is "live" but overlay.camera_url is not set'
            )
        return RtspMjpegSource(
            camera_url,
            stream_fps=settings.get("stream_fps") or 5.0,
            quality=settings.get("stream_quality") or 12,
            rtsp_transport=settings.get("rtsp_transport") or "tcp",
        )

    raise ValueError(
        f'Unknown overlay.background {kind!r}: expected "file" or "live"'
    )
