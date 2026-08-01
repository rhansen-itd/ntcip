"""
remux_video_buffer.py — Edge-viable video buffer via PyAV remux / stream-copy.

This is the **only** video backend (ROADMAP Item 1; ROADMAP Item 6 retired the
alternative on 2026-08-01). It replaced the constant-frame-rate
``cv2.VideoWriter`` path in ``video_buffer.py`` — since deleted, recoverable
from git history — with a *remux* path built on PyAV:

    demux the RTSP/HTTP stream into **encoded packets**, keep a RAM-bounded
    rolling pre-roll of packets, and on a trigger copy packets straight to disk
    **using the camera's own presentation timestamps** — no decode, no encode.

Why remux (see ``VIDEO_BUFFER_REMUX_PLAN.md`` for the full rationale):

* **Accurate length by construction.** Clip duration equals the span of the
  source packets' timestamps — the true wall-clock elapsed time — regardless of
  RTSP jitter. This is the exact defect ``video_buffer.py`` has, where a single
  assumed FPS drifts under jitter.
* **Edge-CPU-friendly.** Stream-copy does no decode and no encode; near-zero CPU
  on a J1900-class box.
* **RAM-bounded.** The pre-roll holds *encoded* packets sized to a **time
  window** (bitrate x window), ~100x smaller than decoded frames and
  **independent of clip length** — resolving the CLAUDE.md "dump pre-roll, then
  route live frames straight to disk" violation.

Roles map onto ``video_buffer.py``'s three classes:

    StreamBuffer (decoded-frame deque)  ->  PacketStreamBuffer (encoded-packet ring)
    DiskWriter   (buffer-all, write-on-stop) -> ClipRemuxer (incremental mux, rebase)
    VideoBufferManager (Hot Folder / semaphore / disk-check)  ->  unchanged surface

Timestamp handling (the correctness core; see plan section 4):

* **Two clocks, two jobs.** Wall-clock ``recv_wall`` (stamped by the monitoring
  machine per CLAUDE.md, never camera time) decides only the coarse clip
  **boundaries** — which packet starts/ends the window. Source **PTS/DTS**
  decides all **intra-clip** timing, giving jitter-exact spacing and true length.
* **Per-clip rebase with a single offset.** Every muxed packet has the first
  written packet's *DTS* subtracted from **both** its PTS and DTS. Using one
  offset (not separate pts0/dts0) is mandatory: subtracting different offsets
  breaks the ``pts >= dts`` invariant on B-frame streams and the muxer rejects
  it. The span (max_pts - min_pts) is offset-invariant, so length is preserved.
* **Keyframe-seek pre-roll.** A clip must begin on a keyframe or it won't decode;
  the pre-roll ring always retains a keyframe before the pre-roll horizon and
  ``ClipRemuxer`` seeks back to the last keyframe at/or before the window start.
* **DTS monotonicity / discontinuities.** Muxers require strictly increasing DTS.
  A mid-clip backward jump or wraparound (camera reboot / RTCP resync) is
  handled by *re-anchoring the offset* so output DTS stays continuous (the "clamp
  the gap" choice from plan section 4 — keep all frames, one-frame gap, rather
  than splitting the clip). **Forward** gaps are deliberately passed through
  unclamped: a forward PTS jump means no frames arrived for that interval (stall
  / frame loss), so preserving it keeps the clip's span equal to true elapsed
  time — the design's core promise. Verified against a real capture with a
  spliced 64s forward gap (2026-07-15 Fable pass; see DESIGN_HISTORY.md). If
  real hardware ever exhibits an absurd forward resync jump (hours), revisit
  with a bounded forward clamp.

Future decoded path (plan section 6 — leave room, do NOT build here):
``PacketStreamBuffer`` deals in **packets**; a future ``FrameStreamBuffer`` would
deal in **decoded frames** behind the same recorder-facing contract the manager
uses (``subscribe`` / ``on_packet`` / ``prime_preroll`` / ``finish`` / ``join`` /
``camera_id``). ``ClipRemuxer``'s *lifecycle* (keyframe seek, trigger-driven
start/stop/extend, semaphore, disk check, finalize) is kept separable from its
*write step* (``_mux``), so a future ``ClipEncoder`` (decode -> process -> encode)
can reuse the lifecycle and swap only the write step. Selecting decoded would be
a *second* ``backend`` value later — out of scope now, and per ROADMAP Item 6 it
must be RAM-bounded, never a revival of the CFR path.

Contracts inherited from ``video_buffer.py`` (do not break): the Hot Folder
trigger schema and ``start``/``stop``/``extend`` actions, oldest-first polling
with no sleep in the capture loop, ``Semaphore(max_concurrent_writers)``, the
disk-free check, and structured JSON-lines logging.
"""

from __future__ import annotations

import collections
import csv
import json
import logging
import queue
import shutil
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, Iterator, List, Optional, Tuple

import pytz
import av  # PyAV (FFmpeg bindings)

log = logging.getLogger(__name__)


def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> pytz.BaseTzInfo:
    """Resolve an IANA timezone name, falling back to UTC on failure.

    Args:
        tz_name: IANA timezone name (e.g. ``"America/Boise"``).
        fallback_log: Logger used to warn when the name is unknown.

    Returns:
        The resolved :class:`pytz` timezone, or ``pytz.utc`` on failure.
    """
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
    """Configuration for the remux video buffer.

    Originally mirrored ``video_buffer.VideoBufferConfig`` so ``system_runner.py``
    could select either backend by config; that backend is gone (ROADMAP Item 6)
    and this is now the whole surface, remux-specific knobs included.

    Args:
        streams: Mapping of ``camera_id`` -> stream URL (RTSP/HTTP) or, for the
            replay self-test, a local file path.
        trigger_dir: Hot Folder directory polled for trigger files.
        output_dir: Directory where finished clips are written.
        pre_roll_sec: Default seconds of pre-trigger footage to include.
        poll_interval_sec: Trigger-directory scan interval.
        max_concurrent_writers: Hard cap on simultaneous ``ClipRemuxer`` threads.
        min_free_disk_mb: Abort a clip start below this free-space threshold.
        backend: ``"remux"`` (this module) — recorded for provenance only; there
            is no longer a second value to select between.
        keyframe_margin_sec: Extra seconds retained behind the pre-roll horizon so
            a keyframe is always available before the window start. Default ~2x a
            typical GOP.
        container_ext: Output container extension. Default ``.ts`` (MPEG-TS):
            survives an abrupt process kill with no trailer to finalize (unlike
            ``.mp4``'s trailing ``moov`` atom) and matches ``__capture_rtsp.py``.
        rtsp_transport: Passed to ``av.open`` as ``rtsp_transport`` (``tcp`` avoids
            UDP packet loss on congested edge links).
        open_timeout_sec: Socket open/read timeout for ``av.open``.
    """

    streams: Dict[str, str]
    trigger_dir: str = "./trigger_queue"
    output_dir: str = "./completed_videos"
    pre_roll_sec: float = 10.0
    poll_interval_sec: float = 2.0
    max_concurrent_writers: int = 2
    min_free_disk_mb: float = 500.0
    backend: str = "remux"
    keyframe_margin_sec: float = 4.0
    container_ext: str = ".ts"
    rtsp_transport: str = "tcp"
    open_timeout_sec: float = 10.0


@dataclass
class StreamTemplate:
    """Codec parameters captured once at stream-open, detached from the container.

    Building the output stream from these (rather than from an input
    ``Stream``/``add_stream_from_template``) is deliberate: pre-roll packets and
    packets buffered across an RTSP reconnect **outlive their source container**,
    so anything holding a live reference into that container (a ``Stream``, or an
    ``av.Packet`` whose ``.stream`` points back into it) is a use-after-free once
    the container closes. Everything the writer needs is copied out here.

    Args:
        codec_name: FFmpeg codec name (e.g. ``"h264"``, ``"hevc"``).
        width: Coded width in pixels (0 if not yet known).
        height: Coded height in pixels (0 if not yet known).
        pix_fmt: Pixel format name, or ``None``.
        extradata: Codec extradata (SPS/PPS etc.); empty if none.
        time_base: Source stream time base.
    """

    codec_name: str
    width: int
    height: int
    pix_fmt: Optional[str]
    extradata: bytes
    time_base: Any


@dataclass
class PacketRecord:
    """A demuxed encoded packet **detached from its source container**.

    The encoded payload is copied to ``data`` and all timing is captured as plain
    values, so a record can safely sit in the pre-roll ring and be consumed by
    multiple concurrent remuxers long after (and across reconnects of) the
    container it came from — see :class:`StreamTemplate` for why detaching matters.

    Args:
        data: The encoded packet payload (``bytes(packet)``).
        pts: Presentation timestamp in ``time_base`` units (may equal ``dts``).
        dts: Decode timestamp in ``time_base`` units.
        duration: Packet duration in ``time_base`` units (0 if unknown).
        time_base: The packet's time base.
        is_keyframe: Whether this packet begins a decodable GOP.
        recv_wall: Wall-clock arrival time (``time.time()``), the boundary clock.
    """

    data: bytes
    pts: int
    dts: int
    duration: int
    time_base: Any
    is_keyframe: bool
    recv_wall: float


class _StopRemuxer:
    """Sentinel queued to a ``ClipRemuxer`` to finalize its output cleanly."""

    __slots__ = ()


_STOP_REMUXER = _StopRemuxer()


# ---------------------------------------------------------------------------
# PacketStreamBuffer — continuous demux into a time-windowed packet ring
# ---------------------------------------------------------------------------
class PacketStreamBuffer:
    """Demux one stream into a RAM-bounded, time-windowed ring of encoded packets.

    Zero-drift capture: the read loop has **no ``time.sleep()``** — ``demux()``
    blocks on I/O naturally. The ring is bounded by a **time window**
    (``pre_roll_sec + keyframe_margin_sec``), not clip length, and always retains
    a keyframe before the pre-roll horizon.

    Recorders attach via :meth:`subscribe`, which atomically hands them the
    current pre-roll snapshot and registers them to receive subsequent live
    packets — so there is no gap or duplicate at the pre-roll/live seam.

    The ``_open_container`` / ``_demux`` seam is overridable: the replay self-test
    subclasses it to feed a real-time-paced file instead of a live socket.

    Args:
        camera_id: Camera identifier (used as the output filename component).
        url: Stream URL, or a file path for the replay self-test.
        pre_roll_sec: Pre-roll window length.
        keyframe_margin_sec: Extra retention behind the pre-roll horizon.
        rtsp_transport: ``av.open`` transport hint (``tcp``/``udp``).
        open_timeout_sec: ``av.open`` open/read timeout.
        reconnect_on_eof: Reconnect after the source ends (live RTSP). Set
            ``False`` for a finite replay so EOF ends the capture loop.
    """

    def __init__(
        self,
        camera_id: str,
        url: str,
        pre_roll_sec: float = 10.0,
        keyframe_margin_sec: float = 4.0,
        rtsp_transport: str = "tcp",
        open_timeout_sec: float = 10.0,
        reconnect_on_eof: bool = True,
    ) -> None:
        self.camera_id = camera_id
        self.url = url
        self.pre_roll_sec = pre_roll_sec
        self.keyframe_margin_sec = keyframe_margin_sec
        self.rtsp_transport = rtsp_transport
        self.open_timeout_sec = open_timeout_sec
        self.reconnect_on_eof = reconnect_on_eof

        self._ring: Deque[PacketRecord] = collections.deque()
        self._subscribers: List["ClipRemuxer"] = []
        self._lock = threading.Lock()
        self.template: Optional[StreamTemplate] = None

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.exhausted = threading.Event()
        self._log = logging.getLogger(f"{__name__}.stream.{camera_id}")

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Launch the daemon capture thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._capture_loop,
            name=f"demux-{self.camera_id}",
            daemon=True,
        )
        self._thread.start()
        self._log.info(
            "PacketStreamBuffer started",
            extra={"camera_id": self.camera_id, "url": self.url},
        )

    def stop(self) -> None:
        """Stop the capture thread and wait briefly for it to exit."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        self._log.info("PacketStreamBuffer stopped", extra={"camera_id": self.camera_id})

    # -- recorder attachment ----------------------------------------------

    def subscribe(self, remuxer: "ClipRemuxer") -> None:
        """Atomically prime a remuxer with the pre-roll and register it live.

        Done under the capture lock so no live packet interleaves between the
        pre-roll snapshot and live registration: the remuxer's queue receives the
        selected pre-roll packets first, then every subsequent live packet in
        order.

        Args:
            remuxer: The :class:`ClipRemuxer` to attach.
        """
        with self._lock:
            remuxer.prime_preroll(list(self._ring))
            self._subscribers.append(remuxer)

    def unsubscribe(self, remuxer: "ClipRemuxer") -> None:
        """Detach a remuxer so it receives no further live packets.

        Args:
            remuxer: The :class:`ClipRemuxer` to detach.
        """
        with self._lock:
            try:
                self._subscribers.remove(remuxer)
            except ValueError:
                pass

    # -- capture -----------------------------------------------------------

    def _open_container(self) -> Tuple[Any, Any]:
        """Open the source container and return ``(container, video_stream)``.

        Overridable seam: the replay self-test subclasses this to open a file.

        Returns:
            A ``(container, stream)`` tuple; the stream is the first video stream.
        """
        container = av.open(
            self.url,
            options={"rtsp_transport": self.rtsp_transport},
            timeout=self.open_timeout_sec,
        )
        return container, container.streams.video[0]

    @staticmethod
    def _capture_template(stream: Any) -> StreamTemplate:
        """Extract a container-independent :class:`StreamTemplate` from a stream.

        Args:
            stream: The demuxed input video stream.

        Returns:
            A :class:`StreamTemplate` with the codec parameters copied out.
        """
        cc = stream.codec_context
        extradata = bytes(cc.extradata) if cc.extradata else b""
        return StreamTemplate(
            codec_name=cc.name,
            width=int(cc.width or 0),
            height=int(cc.height or 0),
            pix_fmt=cc.pix_fmt,
            extradata=extradata,
            time_base=stream.time_base,
        )

    def _demux(self, container: Any, stream: Any) -> Iterator["av.Packet"]:
        """Yield demuxed packets from ``container``.

        Overridable seam: the replay self-test paces packets in real time.

        Args:
            container: The open source container.
            stream: The video stream to demux.

        Yields:
            :class:`av.Packet` objects in DTS order (encoded, not decoded).
        """
        yield from container.demux(stream)

    def _capture_loop(self) -> None:
        """Demux packets into the ring and fan out live packets to subscribers.

        No ``time.sleep()`` here — this is the zero-drift capture path. Reconnects
        on error/EOF for live streams; ends and sets :attr:`exhausted` on EOF for
        a finite replay.
        """
        while self._running:
            try:
                container, stream = self._open_container()
            except (av.FFmpegError, OSError) as exc:
                self._log.error(
                    "Failed to open stream — retrying",
                    extra={"camera_id": self.camera_id, "error": str(exc)},
                )
                self._interruptible_wait(3.0)
                continue

            self.template = self._capture_template(stream)
            self._log.info(
                "Stream opened",
                extra={
                    "camera_id": self.camera_id,
                    "codec": self.template.codec_name,
                    "resolution": f"{self.template.width}x{self.template.height}",
                },
            )
            try:
                for packet in self._demux(container, stream):
                    if not self._running:
                        break
                    # Flush/empty packets (e.g. end-of-stream markers) carry no
                    # timestamp and are not muxable — skip them.
                    if packet.dts is None:
                        continue

                    # Detach the packet from the source container immediately
                    # (copy payload + timing); the record must outlive `container`.
                    rec = PacketRecord(
                        data=bytes(packet),
                        pts=packet.pts if packet.pts is not None else packet.dts,
                        dts=packet.dts,
                        duration=packet.duration or 0,
                        time_base=packet.time_base,
                        is_keyframe=bool(packet.is_keyframe),
                        recv_wall=time.time(),
                    )
                    with self._lock:
                        self._ring.append(rec)
                        self._evict(rec.recv_wall)
                        for sub in self._subscribers:
                            sub.on_packet(rec)
            except (av.FFmpegError, OSError) as exc:
                self._log.warning(
                    "Demux error — reconnecting",
                    extra={"camera_id": self.camera_id, "error": str(exc)},
                )
            finally:
                try:
                    container.close()
                except (av.FFmpegError, OSError):
                    pass

            if not self._running:
                break
            if not self.reconnect_on_eof:
                self.exhausted.set()
                self._log.info("Source exhausted", extra={"camera_id": self.camera_id})
                break
            self._interruptible_wait(1.0)

    def _interruptible_wait(self, seconds: float) -> None:
        """Sleep up to ``seconds`` in short slices, bailing out on stop.

        Used only between reconnect attempts — never inside the packet loop.

        Args:
            seconds: Maximum time to wait.
        """
        deadline = time.monotonic() + seconds
        while self._running and time.monotonic() < deadline:
            time.sleep(0.05)

    def _evict(self, now: float) -> None:
        """Drop packets older than the pre-roll horizon, keeping a leading keyframe.

        Retains everything from the newest keyframe whose ``recv_wall`` is at or
        before ``now - (pre_roll_sec + keyframe_margin_sec)`` onward, guaranteeing
        a decodable start exists for any pre-roll window. Must be called under the
        lock.

        Args:
            now: Current wall-clock time (the just-appended packet's ``recv_wall``).
        """
        cutoff = now - (self.pre_roll_sec + self.keyframe_margin_sec)
        floor_idx = 0
        for i, rec in enumerate(self._ring):
            if rec.recv_wall > cutoff:
                break
            if rec.is_keyframe:
                floor_idx = i
        for _ in range(floor_idx):
            self._ring.popleft()


# ---------------------------------------------------------------------------
# ClipRemuxer — per-trigger incremental stream-copy writer
# ---------------------------------------------------------------------------
class ClipRemuxer:
    """Stream-copy one clip to disk incrementally, rebasing source timestamps.

    Created on a ``start`` trigger (under the semaphore, after the disk check).
    Its writer thread consumes packet records from a queue — first the selected
    pre-roll (from a keyframe), then live packets — copies each packet, rebases
    its timestamps against a single per-clip DTS offset, and muxes it straight to
    disk. RAM does not grow with clip length.

    Lifecycle (keyframe seek, start/stop/extend via the manager, semaphore, disk
    check, finalize) is intentionally separable from the write step (:meth:`_mux`)
    to leave room for a future decoded ``ClipEncoder`` (see module docstring).

    Args:
        trigger_id: Trigger identifier (used for correlation and the filename).
        camera_id: Source camera identifier.
        output_path: Destination clip path (extension chosen by the manager).
        event_ts: Discrepancy-detection wall-clock time; the pre-roll window is
            ``[event_ts - pre_roll_sec, ...]``.
        pre_roll_sec: Seconds of pre-trigger footage to include.
        semaphore: Concurrency semaphore released exactly once when the writer
            thread ends.
        template_provider: Callable returning the source :class:`StreamTemplate`
            (read lazily at output-open time, by which point the stream buffer has
            connected and captured it).
    """

    def __init__(
        self,
        trigger_id: str,
        camera_id: str,
        output_path: Path,
        event_ts: float,
        pre_roll_sec: float,
        semaphore: threading.Semaphore,
        template_provider: Callable[[], Optional[StreamTemplate]],
    ) -> None:
        self.trigger_id = trigger_id
        self.camera_id = camera_id
        self._output_path = output_path
        self._event_ts = event_ts
        self._pre_roll_sec = pre_roll_sec
        self._semaphore = semaphore
        self._template_provider = template_provider

        self._queue: "queue.Queue[Any]" = queue.Queue()
        self._thread = threading.Thread(
            target=self._write_loop,
            name=f"remux-{trigger_id[:8]}",
            daemon=True,
        )

        self._out: Optional[Any] = None
        self._out_stream: Optional[Any] = None
        self._offset: Optional[int] = None
        self._last_out_dts: Optional[int] = None
        self._last_duration: Optional[int] = None
        self._n_written = 0
        self._n_discontinuities = 0
        self._preroll_truncated = False
        self._dropped_leading = 0
        self._log = logging.getLogger(f"{__name__}.remux")

    # -- attachment API (called by PacketStreamBuffer / manager) ----------

    def start(self) -> None:
        """Launch the daemon writer thread (blocks on its queue until primed)."""
        self._thread.start()

    def on_packet(self, rec: PacketRecord) -> None:
        """Enqueue a live packet record. Called under the buffer lock — must be
        fast and non-blocking (a queue put).

        Args:
            rec: The just-arrived :class:`PacketRecord`.
        """
        self._queue.put(rec)

    def prime_preroll(self, snapshot: List[PacketRecord]) -> None:
        """Select the keyframe-seek pre-roll and enqueue it ahead of live packets.

        Chooses the **last keyframe with ``recv_wall <= event_ts - pre_roll_sec``**
        as the clip start; if the ring doesn't reach that far back, falls back to
        the earliest keyframe and flags ``preroll_truncated``. Called under the
        buffer lock so the enqueued pre-roll strictly precedes any live packet.

        Args:
            snapshot: A copy of the pre-roll ring at subscribe time.
        """
        clip_start_wall = self._event_ts - self._pre_roll_sec

        start_idx: Optional[int] = None
        for i, rec in enumerate(snapshot):
            if rec.is_keyframe and rec.recv_wall <= clip_start_wall:
                start_idx = i
        if start_idx is None:
            # Ring doesn't reach the requested window; use the earliest keyframe.
            for i, rec in enumerate(snapshot):
                if rec.is_keyframe:
                    start_idx = i
                    self._preroll_truncated = True
                    break

        if start_idx is None:
            # No keyframe buffered at all — the writer will open on the first live
            # keyframe instead (leading non-key packets are dropped in _mux).
            self._preroll_truncated = True
            return

        for rec in snapshot[start_idx:]:
            self._queue.put(rec)

    def finish(self) -> None:
        """Signal the writer thread to flush and finalize the container."""
        self._queue.put(_STOP_REMUXER)

    def join(self, timeout: Optional[float] = 10.0) -> None:
        """Wait for the writer thread to finish finalizing.

        Args:
            timeout: Maximum seconds to wait.
        """
        self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        """Return whether the writer thread is still running."""
        return self._thread.is_alive()

    # -- writer thread -----------------------------------------------------

    def _write_loop(self) -> None:
        """Consume records until the stop sentinel, muxing each; always finalize."""
        try:
            while True:
                item = self._queue.get()
                if isinstance(item, _StopRemuxer):
                    break
                self._mux(item)
        except Exception as exc:  # noqa: BLE001 — never leak from the writer thread
            self._log.error(
                "Remux writer error",
                extra={"trigger_id": self.trigger_id, "error": str(exc)},
            )
        finally:
            self._finalize()
            self._semaphore.release()

    def _mux(self, rec: PacketRecord) -> None:
        """Copy, rebase, and mux one packet to disk incrementally.

        Opens the output container lazily on the first keyframe (a clip must start
        decodable). Applies the single-offset per-clip rebase and clamps DTS
        discontinuities so output DTS stays strictly increasing.

        Args:
            rec: The packet record to write.
        """
        if self._out is None:
            if not rec.is_keyframe:
                # Never open a clip mid-GOP — wait for the first keyframe.
                self._dropped_leading += 1
                return
            if not self._open_output():
                return
            self._offset = rec.dts

        src_dts = rec.dts
        src_pts = rec.pts

        assert self._offset is not None
        out_dts = src_dts - self._offset
        if self._last_out_dts is not None and out_dts <= self._last_out_dts:
            # Backward jump / wraparound / stall: re-anchor the offset so output
            # DTS advances by one frame past the last (plan section 4: clamp the
            # gap, keep every frame). PTS shifts by the same offset, preserving
            # the pts-dts relationship for B-frames.
            step = rec.duration or self._last_duration or 1
            self._offset = src_dts - (self._last_out_dts + step)
            out_dts = src_dts - self._offset
            self._n_discontinuities += 1
            self._log.warning(
                "DTS discontinuity clamped",
                extra={"trigger_id": self.trigger_id, "src_dts": src_dts},
            )

        out_pts = src_pts - self._offset
        if out_pts < out_dts:
            out_pts = out_dts  # muxer invariant: pts >= dts

        new_pkt = av.Packet(rec.data)
        new_pkt.pts = out_pts
        new_pkt.dts = out_dts
        new_pkt.time_base = rec.time_base
        new_pkt.duration = rec.duration
        if rec.is_keyframe:
            new_pkt.is_keyframe = True
        new_pkt.stream = self._out_stream

        self._out.mux(new_pkt)
        self._last_out_dts = out_dts
        self._last_duration = rec.duration
        self._n_written += 1

    def _open_output(self) -> bool:
        """Open the output container and build a stream-copy output stream.

        The output stream is built from the captured :class:`StreamTemplate`
        (codec name + parameters), not from a live input stream, so no encoder is
        created and nothing references the possibly-closed source container.

        Returns:
            ``True`` if the output opened; ``False`` (clip aborted) if no template
            is available yet.
        """
        template = self._template_provider()
        if template is None:
            self._log.error(
                "No stream template available — clip aborted",
                extra={"trigger_id": self.trigger_id},
            )
            return False

        self._out = av.open(str(self._output_path), "w")
        out_stream = self._out.add_stream(template.codec_name)
        occ = out_stream.codec_context
        if template.width and template.height:
            occ.width = template.width
            occ.height = template.height
        if template.pix_fmt:
            occ.pix_fmt = template.pix_fmt
        if template.extradata:
            occ.extradata = template.extradata
        out_stream.time_base = template.time_base
        self._out_stream = out_stream
        self._log.info(
            "Clip opened",
            extra={
                "trigger_id": self.trigger_id,
                "camera_id": self.camera_id,
                "output_path": str(self._output_path),
                "preroll_truncated": self._preroll_truncated,
            },
        )
        return True

    def _finalize(self) -> None:
        """Flush and close the container cleanly; log clip metrics."""
        if self._out is not None:
            try:
                self._out.close()
            except (av.FFmpegError, OSError) as exc:
                self._log.error(
                    "Error finalizing clip",
                    extra={"trigger_id": self.trigger_id, "error": str(exc)},
                )
        self._log.info(
            "Clip finalized",
            extra={
                "trigger_id": self.trigger_id,
                "output_path": str(self._output_path),
                "packets_written": self._n_written,
                "discontinuities": self._n_discontinuities,
                "preroll_truncated": self._preroll_truncated,
                "dropped_leading": self._dropped_leading,
            },
        )


# ---------------------------------------------------------------------------
# VideoBufferManager — Hot Folder poll / semaphore / disk check (reused surface)
# ---------------------------------------------------------------------------
class VideoBufferManager:
    """Poll the Hot Folder and drive per-trigger ``ClipRemuxer`` recorders.

    Public surface matches ``video_buffer.VideoBufferManager`` (``start`` /
    ``stop``) so ``system_runner.py`` selects this backend by config. Packet
    routing is by subscription — a started remuxer subscribes to its camera's
    :class:`PacketStreamBuffer` and receives pre-roll + live packets directly — so
    there is no per-tick frame-flush loop; the poll loop only scans triggers and
    reaps finished writers.

    **Thread-safety (ROADMAP Item 8, 2026-07-31).** The writer bookkeeping —
    ``_active_writers``, ``_stop_timers``, ``_draining`` — is touched from three
    contexts: the poll loop, ``threading.Timer`` callbacks (``_auto_stop``), and
    the main thread (:meth:`stop`). All three fields are guarded by a single
    ``_state_lock``. The critical sections are **pure bookkeeping** (dict/list
    pop/append); ``writer.finish()``, ``join()``, ``timer.cancel()``, semaphore
    acquires and any I/O happen **outside** the lock — ``_auto_stop`` runs on a
    Timer thread and re-enters ``_stop_trigger``, so holding the lock across a
    join would deadlock the reap path. The pattern throughout is: under the
    lock, pop/collect what to act on; release; then act.

    Timers carry a **generation** so a timer that fires while ``extend``/``stop``
    is superseding it (i.e. whose ``cancel()`` lost the race) cannot stop a clip
    it no longer owns.

    **Single-camera assumption.** A ``start`` trigger naming several cameras (or
    ``["all"]`` at a multi-camera intersection) records only the *first* resolved
    camera and logs a WARNING; per-camera writers are deliberately not
    implemented (no second camera is deployed to test against). Documented in
    ``config_manager.py``'s trigger-schema section; the same assumption exists in
    ``video_buffer.py``.

    Args:
        config: The :class:`VideoBufferConfig`.
        stream_buffer_factory: Optional ``(camera_id, url) -> PacketStreamBuffer``
            factory. Overridable seam for the replay self-test; defaults to a live
            :class:`PacketStreamBuffer`.
    """

    def __init__(
        self,
        config: VideoBufferConfig,
        stream_buffer_factory: Optional[
            Callable[[str, str], PacketStreamBuffer]
        ] = None,
    ) -> None:
        self._config = config
        self._stream_buffers: Dict[str, PacketStreamBuffer] = {}
        # --- guarded by _state_lock (see the class docstring) -----------------
        self._active_writers: Dict[str, ClipRemuxer] = {}
        # Writers that have been told to finish but whose thread may still be
        # finalizing the container. Tracked until the thread dies so shutdown
        # can join them (clean finalize) and callers can await completion.
        self._draining: List[ClipRemuxer] = []
        # trigger_id -> (generation, timer); the generation lets a timer that
        # fired just as it was being superseded detect that and do nothing.
        self._stop_timers: Dict[str, Tuple[int, threading.Timer]] = {}
        self._timer_generation = 0
        self._state_lock = threading.Lock()
        # ---------------------------------------------------------------------
        self._writer_semaphore = threading.Semaphore(config.max_concurrent_writers)
        self._running = False
        self._sb_factory = stream_buffer_factory or self._default_stream_buffer
        self._log = logging.getLogger(f"{__name__}.manager")

        Path(config.trigger_dir).mkdir(parents=True, exist_ok=True)
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

    def _default_stream_buffer(self, camera_id: str, url: str) -> PacketStreamBuffer:
        """Build a live :class:`PacketStreamBuffer` from config (default factory)."""
        return PacketStreamBuffer(
            camera_id=camera_id,
            url=url,
            pre_roll_sec=self._config.pre_roll_sec,
            keyframe_margin_sec=self._config.keyframe_margin_sec,
            rtsp_transport=self._config.rtsp_transport,
            open_timeout_sec=self._config.open_timeout_sec,
        )

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Start all stream buffers, then run the Hot Folder poll loop (blocks)."""
        self._running = True
        for cam_id, url in self._config.streams.items():
            buf = self._sb_factory(cam_id, url)
            buf.start()
            self._stream_buffers[cam_id] = buf

        self._log.info(
            "VideoBufferManager started",
            extra={"num_streams": len(self._stream_buffers), "backend": "remux"},
        )
        self._poll_loop()

    def stop(self) -> None:
        """Finalize active clips, join writers, then stop the stream buffers.

        Snapshots the bookkeeping under the lock and does every blocking step
        (cancel, finish, join) outside it. ``_draining`` is drained by repeated
        locked pops rather than a snapshot-then-clear so a writer appended by a
        Timer thread mid-shutdown is still joined instead of being dropped.
        """
        self._running = False

        with self._state_lock:
            timers = [timer for _, timer in self._stop_timers.values()]
            self._stop_timers.clear()
            trigger_ids = list(self._active_writers.keys())
        for timer in timers:
            timer.cancel()

        for trigger_id in trigger_ids:
            self._stop_trigger(trigger_id)

        while True:
            with self._state_lock:
                if not self._draining:
                    break
                remuxer = self._draining.pop(0)
            remuxer.join()

        for buf in self._stream_buffers.values():
            buf.stop()
        self._log.info("VideoBufferManager stopped")

    def _poll_loop(self) -> None:
        """Scan the trigger directory on the configured interval and reap writers."""
        last_scan = 0.0
        while self._running:
            now = time.monotonic()
            self._reap_finished()
            if (now - last_scan) >= self._config.poll_interval_sec:
                self._scan_trigger_dir()
                last_scan = time.monotonic()
            time.sleep(0.05)

    def _reap_finished(self) -> None:
        """Join draining writers whose finalize thread has exited.

        Selection and removal happen together under the lock (a Timer thread may
        be appending concurrently); the joins happen after releasing it.
        """
        with self._state_lock:
            finished = [r for r in self._draining if not r.is_alive()]
            for remuxer in finished:
                self._draining.remove(remuxer)
        for remuxer in finished:
            remuxer.join()

    # -- Hot Folder --------------------------------------------------------

    def _scan_trigger_dir(self) -> None:
        """Read and dispatch trigger files oldest-first, deleting each after."""
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
            elif action == "stop":
                self._handle_stop(trigger)
            elif action == "extend":
                self._handle_extend(trigger)
            else:
                self._log.warning(
                    "Unknown trigger action",
                    extra={"action": action, "trigger_id": trigger.get("trigger_id")},
                )
            self._safe_delete(path)

    def _handle_start(self, trigger: dict) -> None:
        """Begin a clip for a ``start`` trigger, under the semaphore + disk check.

        Only the first resolved camera is recorded (single-camera assumption —
        see the class docstring); a trigger resolving to more than one logs a
        WARNING naming what was requested versus what is being recorded.
        """
        trigger_id: str = trigger["trigger_id"]
        cameras: List[str] = trigger.get("cameras", ["all"])
        pre_roll_sec: float = float(
            trigger.get("pre_roll_sec", self._config.pre_roll_sec)
        )
        max_duration_sec: float = float(trigger.get("max_duration_sec", 300))
        event_ts: float = float(trigger.get("event_timestamp", time.time()))

        with self._state_lock:
            if trigger_id in self._active_writers:
                return

        target_cams = (
            list(self._stream_buffers.keys())
            if cameras == ["all"]
            else [c for c in cameras if c in self._stream_buffers]
        )
        if not target_cams:
            self._log.warning(
                "No matching cameras for trigger — dropped",
                extra={"trigger_id": trigger_id, "cameras": cameras},
            )
            return

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
            self._log.warning(
                "Concurrent writer cap reached — trigger dropped",
                extra={"trigger_id": trigger_id},
            )
            return

        if not self._check_disk_space():
            self._writer_semaphore.release()
            self._log.error(
                "Insufficient free disk — clip aborted",
                extra={"trigger_id": trigger_id},
            )
            return

        cam_id = target_cams[0]
        buf = self._stream_buffers[cam_id]
        output_path = (
            Path(self._config.output_dir)
            / f"{trigger_id[:8]}_{cam_id}_{int(time.time())}{self._config.container_ext}"
        )

        remuxer = ClipRemuxer(
            trigger_id=trigger_id,
            camera_id=cam_id,
            output_path=output_path,
            event_ts=event_ts,
            pre_roll_sec=pre_roll_sec,
            semaphore=self._writer_semaphore,
            template_provider=lambda b=buf: b.template,
        )
        remuxer.start()
        buf.subscribe(remuxer)  # primes pre-roll, then routes live packets

        # Register the writer and its auto-stop timer together, *before* the
        # timer is armed: a very short max_duration must not let _auto_stop run
        # against bookkeeping that doesn't exist yet (it would find nothing to
        # stop and the clip would run forever).
        with self._state_lock:
            self._timer_generation += 1
            generation = self._timer_generation
            timer = threading.Timer(
                interval=max_duration_sec,
                function=self._auto_stop,
                args=(trigger_id, generation),
            )
            timer.daemon = True
            self._active_writers[trigger_id] = remuxer
            self._stop_timers[trigger_id] = (generation, timer)
        timer.start()

        self._log.info(
            "Clip start dispatched",
            extra={
                "trigger_id": trigger_id,
                "cameras_requested": target_cams,
                "cameras_recorded": [cam_id],
                "output_path": str(output_path),
            },
        )
        self._log_discrepancy_to_csv(trigger, output_path)

    def _handle_stop(self, trigger: dict) -> None:
        """Finalize the clip named by a ``stop`` trigger."""
        self._stop_trigger(trigger["trigger_id"])

    def _handle_extend(self, trigger: dict) -> None:
        """Push the max-duration deadline out for an in-progress clip.

        Unlike the retired ``full`` backend (where ``extend`` behaved as
        ``stop``), the remux backend honors ``extend`` per the plan: it reschedules
        the auto-stop timer for another ``max_duration_sec`` from now.
        """
        trigger_id: str = trigger["trigger_id"]
        max_duration_sec: float = float(trigger.get("max_duration_sec", 300))

        with self._state_lock:
            if trigger_id not in self._active_writers:
                return
            old = self._stop_timers.pop(trigger_id, None)
            self._timer_generation += 1
            generation = self._timer_generation
            timer = threading.Timer(
                interval=max_duration_sec,
                function=self._auto_stop,
                args=(trigger_id, generation),
            )
            timer.daemon = True
            self._stop_timers[trigger_id] = (generation, timer)

        if old is not None:
            old[1].cancel()
        timer.start()
        self._log.info("Clip extended", extra={"trigger_id": trigger_id})

    def _auto_stop(self, trigger_id: str, generation: int) -> None:
        """Stop a clip that hit its ``max_duration_sec`` cap.

        Runs on a Timer thread. A timer whose generation is no longer the
        registered one has been superseded by an ``extend`` (or the clip has
        already stopped) and its ``cancel()`` simply lost the race — it must not
        stop a clip it no longer owns.

        Args:
            trigger_id: The clip's trigger identifier.
            generation: The timer generation this callback was armed with.
        """
        with self._state_lock:
            current = self._stop_timers.get(trigger_id)
            if current is None or current[0] != generation:
                return
        self._log.info("Max-duration cap reached", extra={"trigger_id": trigger_id})
        self._stop_trigger(trigger_id)

    def _stop_trigger(self, trigger_id: str) -> None:
        """Unsubscribe, finalize, and drop bookkeeping for one clip.

        Re-entrant across threads: the whole writer/timer/draining hand-off is
        done in one locked bookkeeping step, so two callers racing on the same
        ``trigger_id`` cannot both finalize it. The unsubscribe/``finish()``/
        ``cancel()`` steps deliberately run after the lock is released.

        Args:
            trigger_id: The clip's trigger identifier.
        """
        with self._state_lock:
            remuxer = self._active_writers.pop(trigger_id, None)
            timer = self._stop_timers.pop(trigger_id, None)
            if remuxer is not None:
                # Track it while it finalizes; _reap_finished / stop() join it
                # once its thread exits, guaranteeing a clean container close.
                # Safe to append before finish(): the writer thread blocks on
                # its queue, so it stays alive until the sentinel is consumed.
                self._draining.append(remuxer)

        if timer is not None:
            timer[1].cancel()
        if remuxer is None:
            return
        buf = self._stream_buffers.get(remuxer.camera_id)
        if buf is not None:
            buf.unsubscribe(remuxer)
        remuxer.finish()

    # -- CSV discrepancy log (inherited from the retired full backend) ------

    def _log_discrepancy_to_csv(self, trigger: dict, video_path: Path) -> None:
        """Append a row to ``discrepancies_log.csv`` for detector-disagreement triggers."""
        if trigger.get("reason") != "detector_disagreement":
            return

        csv_path = Path(self._config.output_dir) / "discrepancies_log.csv"
        write_header = not csv_path.exists()

        meta = trigger.get("metadata", {})
        event_ts = trigger.get("event_timestamp", 0.0)

        tz_name: str = trigger.get("timezone", "UTC") or "UTC"
        display_tz = _resolve_pytz(tz_name, self._log)
        try:
            local_dt = datetime.fromtimestamp(event_ts, tz=timezone.utc).astimezone(
                display_tz
            )
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
            self._log.error(
                "Failed to write discrepancy CSV log", extra={"error": str(exc)}
            )

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _read_trigger(path: Path) -> dict:
        """Load and minimally validate a trigger file.

        Args:
            path: Path to the trigger JSON.

        Returns:
            The parsed trigger dict.

        Raises:
            KeyError: If a required field is missing.
        """
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        required = {"trigger_id", "action", "event_timestamp"}
        missing = required - data.keys()
        if missing:
            raise KeyError(f"Trigger missing required fields: {missing}")
        return data

    def _check_disk_space(self) -> bool:
        """Return whether free space in the output dir is above the threshold."""
        free_mb = shutil.disk_usage(self._config.output_dir).free / (1024 * 1024)
        return free_mb >= self._config.min_free_disk_mb

    @staticmethod
    def _safe_delete(path: Path) -> None:
        """Delete a consumed trigger file, logging but not raising on failure."""
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning(
                "Could not delete trigger file",
                extra={"path": str(path), "error": str(exc)},
            )
