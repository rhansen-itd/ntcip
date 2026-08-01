"""
system_runner.py — Top-level orchestrator for the traffic monitoring system.

This script is the single entry-point that instantiates every subsystem,
wires the event callbacks, and manages graceful shutdown.  It deliberately
contains *no* business logic of its own; it is pure "glue".

Run
───
    python system_runner.py --intersection 1234_main \
                            --config /etc/traffic/intersections.json

Or with all defaults:
    python system_runner.py

Dependency graph (all arrows are one-way; no module imports another laterally)
───────────────────────────────────────────────────────────────────────────────

    intersections.json
           │
           ▼
    JsonFileConfigProvider
           │
    ┌──────┼───────────────┬──────────────────────┐
    │      │               │                      │
    ▼      ▼               ▼                      ▼
 VideoBufferConfig  DiscrepancyMonitor    RoutineScheduler
 VideoBufferManager      ▲
                         │  .on_detector_on / .on_detector_off
                   TrafficMonitor  ← NTCIP / SNMP
                   (ntcip_monitor)
                         │
                    Hot Folder  ◀──────────────────┘
                         │
                   VideoBufferManager polls it

Shutdown order (important — must drain writers before exit)
───────────────────────────────────────────────────────────
1. Stop NTCIP monitor  → no more callbacks fire
2. Stop DiscrepancyMonitor  → evaluator thread exits
3. Stop RoutineScheduler  → scheduler thread exits
4. Stop VideoBufferManager  → stream threads + all DiskWriters drain and exit
5. sys.exit(0)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# ── Adjust these imports to match your actual package / module layout ──────
# ---------------------------------------------------------------------------

# Config layer (Session 2)
from config_manager import JsonFileConfigProvider, ConfigProviderError

# Video buffer (Session 1). remux_video_buffer is the only backend; its import
# stays deferred to _build_video_manager so importing this module doesn't pull
# in PyAV (the CFR "full" backend was retired 2026-08-01, ROADMAP Item 6).

# Discrepancy engine (Session 3)
from discrepancy_engine import DiscrepancyMonitor

# Routine scheduler (Session 4)
from routine_scheduler import RoutineScheduler


try:
    from ntcip_monitor.core.snmp_client import EconoliteSNMPClient
    from ntcip_monitor.monitors.detector_monitor import DetectorMonitor
    from ntcip_monitor.core.event_monitor import EVENT_DETECTOR_ON, EVENT_DETECTOR_OFF
    _NTCIP_AVAILABLE = True

except ImportError as e:  # pragma: no cover
    # Allows the script to be imported and tested without the NTCIP package
    # installed.  A clear warning is logged at runtime.
    TrafficMonitor = None  # type: ignore[assignment,misc]
    _NTCIP_AVAILABLE = False
    print(e)

# ---------------------------------------------------------------------------
# JSON-lines logger (consistent with all other modules in this project)
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D102
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        _SKIP = {
            "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno",
            "funcName", "created", "msecs", "relativeCreated", "thread",
            "threadName", "processName", "process", "name", "message",
        }
        for k, v in record.__dict__.items():
            if k not in _SKIP:
                payload[k] = v
        return json.dumps(payload, default=str)


def _configure_root_logger() -> logging.Logger:
    """Configure the root logger to emit JSON lines and return it.

    Returns:
        The root :class:`logging.Logger` with a JSON stream handler attached.
    """
    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        root.addHandler(handler)
    root.setLevel(logging.DEBUG)
    return root


log = _configure_root_logger().getChild("system_runner")

# ---------------------------------------------------------------------------
# Sampling-floor injection (ROADMAP 9 — see SCOPE_sampling_floor.md)
# ---------------------------------------------------------------------------

# Cadence at which the measured NTCIP sweep time is pushed into the
# discrepancy engine.  The floor is a slow-moving property of the controller
# link, so a minute is plenty; the engine reads whatever value is current.
_SAMPLING_FLOOR_UPDATE_SEC = 60.0

# Assumed floor before any measurement exists, overridable per-intersection
# via the config's "sampling_floor_sec".  1.6 s = the 2026-07-19 measured
# median sweep at chunk_size 1.
_DEFAULT_SAMPLING_FLOOR_SEC = 1.6

# ---------------------------------------------------------------------------
# SystemRunner
# ---------------------------------------------------------------------------

class SystemRunner:
    """Owns the lifecycle of every subsystem and wires them together.

    Construction is intentionally separated from startup so that the object
    can be inspected or tested before any threads are launched.

    Args:
        intersection_id: Canonical intersection identifier (e.g.
            ``"1234_main"``).  Must exist in the config file.
        config_path: Path to the ``intersections.json`` file.
        trigger_dir: Hot Folder directory shared by the engines and the
            video buffer.  Created automatically if absent.
        output_dir: Directory where completed MP4 clips are stored, alongside
            the video buffer's ``discrepancies_log.csv`` (one row per
            *recording*) and the engine's ``engine_decisions.csv`` (one row per
            *decision*).  The two are separate artifacts on purpose — see the
            discrepancy engine's module docstring.
        min_free_disk_mb: Abort a recording if free disk space falls below
            this threshold (passed through to :class:`VideoBufferManager`).
        max_concurrent_writers: Hard cap on simultaneous encoder threads.
            Keep at 2 for J1900 edge devices.

    Raises:
        SystemExit: If the config file is missing or the intersection ID is
            not found.  We exit early here so the process doesn't start with
            a broken configuration.
    """

    def __init__(
        self,
        intersection_id: str,
        config_path: str | Path = "./intersections.json",
        trigger_dir: str | Path = "./trigger_queue",
        output_dir: str | Path = "./completed_videos",
        min_free_disk_mb: float = 500.0,
        max_concurrent_writers: int = 2,
    ) -> None:
        self._intersection_id = intersection_id
        self._trigger_dir = Path(trigger_dir)
        self._output_dir = Path(output_dir)
        self._shutdown_event = threading.Event()

        # ── 1. Configuration ─────────────────────────────────────────────
        log.info(
            "Loading configuration",
            extra={"config_path": str(config_path), "intersection_id": intersection_id},
        )
        try:
            self._config_provider = JsonFileConfigProvider(config_path)
        except ConfigProviderError as exc:
            log.error("Failed to load configuration", extra={"error": str(exc)})
            sys.exit(1)

        try:
            self._intersection_cfg = self._config_provider.get_intersection_config(
                intersection_id
            )
        except KeyError:
            log.error(
                "Intersection not found in config",
                extra={
                    "intersection_id": intersection_id,
                    "available": self._config_provider.list_intersection_ids(),
                },
            )
            sys.exit(1)

        # ── 2. Video buffer ───────────────────────────────────────────────
        # Build the stream URL map directly from the config's cameras block.
        # The VideoBufferManager is agnostic to intersection topology.
        cameras_cfg: dict = self._intersection_cfg.get("cameras", {})
        if not cameras_cfg:
            log.warning(
                "No cameras found in intersection config — video buffer will "
                "start but record nothing",
                extra={"intersection_id": intersection_id},
            )

        stream_map: Dict[str, str] = {
            cam_id: cam["url"] for cam_id, cam in cameras_cfg.items()
        }

        # Pull optional per-camera pre-roll; use the first camera's value as
        # the global default, or fall back to 10 s.  A more sophisticated
        # implementation could run one VideoBufferManager per camera with its
        # own pre_roll_sec, but for most intersections a shared value is fine.
        pre_roll_sec: float = 10.0
        if cameras_cfg:
            first_cam = next(iter(cameras_cfg.values()))
            pre_roll_sec = float(first_cam.get("pre_roll_sec", pre_roll_sec))

        self._video_manager = self._build_video_manager(
            stream_map=stream_map,
            pre_roll_sec=pre_roll_sec,
            max_concurrent_writers=max_concurrent_writers,
            min_free_disk_mb=min_free_disk_mb,
        )

        # ── 3. Discrepancy engine ─────────────────────────────────────────
        # DiscrepancyMonitor reads detector pairing and lag thresholds from
        # the ConfigProvider.  It outputs trigger files; it never touches
        # the video buffer directly.
        self._discrepancy_monitor = DiscrepancyMonitor(
            intersection_id=intersection_id,
            config_provider=self._config_provider,
            trigger_dir=self._trigger_dir,
            # Tune these per-deployment; they can also be read from config
            # if you add them to the intersection schema.
            cooldown_sec=60.0,
            evaluator_interval_sec=0.1,
            pre_roll_sec=pre_roll_sec,
            post_roll_sec=float(
                next(iter(cameras_cfg.values()), {}).get("post_roll_sec", 20)
            ),
            max_duration_sec=300.0,
            # The engine's own record of what it decided, alongside — never
            # instead of — the buffer's record of what it recorded.  The two
            # differ by exactly the triggers back-pressure dropped, which is
            # the only way to tell an engine miss from a busy writer pool.
            decision_log_path=self._output_dir / "engine_decisions.csv",
        )

        # The engine must not evaluate detector evidence finer than the rate
        # at which that evidence is sampled.  It cannot import ntcip_monitor
        # to find that rate out, so this composition root injects it: the
        # configured assumption now, the monitor's own measurement later (see
        # _sampling_floor_updater).
        self._configured_sampling_floor_sec = float(
            self._intersection_cfg.get(
                "sampling_floor_sec", _DEFAULT_SAMPLING_FLOOR_SEC
            )
        )
        self._discrepancy_monitor.set_sampling_floor(
            self._configured_sampling_floor_sec
        )
        log.info(
            "Initial sampling floor set from config",
            extra={
                "intersection_id": intersection_id,
                "sampling_floor_sec": self._configured_sampling_floor_sec,
            },
        )

        # ── 4. Routine scheduler ──────────────────────────────────────────
        # RoutineScheduler reads routine_recordings from the same config.
        # If the key is absent the scheduler starts but never fires — no error.
        self._routine_scheduler = RoutineScheduler(
            intersection_id=intersection_id,
            config_provider=self._config_provider,
            trigger_dir=self._trigger_dir,
            check_interval_sec=10.0,
        )

        # ── 5. NTCIP monitor (constructed but not started yet) ────────────
        # We build it here so any constructor-time errors surface before we
        # launch background threads.
        self._ntcip_monitor: Optional[Any] = None
        if _NTCIP_AVAILABLE:
            self._ntcip_monitor = self._build_ntcip_monitor()
        else:
            log.warning(
                "ntcip_monitor package not found — SNMP monitoring disabled. "
                "Only routine scheduled recordings will operate.",
                extra={"intersection_id": intersection_id},
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start all subsystems and block until a shutdown signal is received.

        Call :meth:`shutdown` from a signal handler or another thread to
        unblock this method.
        """
        log.info(
            "Starting all subsystems",
            extra={"intersection_id": self._intersection_id},
        )

        # Start the video buffer first so the stream buffers are pre-filling
        # their deques before any triggers can arrive.
        _start_in_thread(
            target=self._video_manager.start,
            name="video-buffer-manager",
        )

        # Give streams a moment to open before the NTCIP monitor starts
        # generating events (optional courtesy delay; not strictly required).
        time.sleep(2.0)

        # Start the discrepancy evaluator daemon thread.
        self._discrepancy_monitor.start()

        # Start the routine scheduler daemon thread.
        self._routine_scheduler.start()

        # Wire detector callbacks and start NTCIP polling.
        if self._ntcip_monitor is not None:
            self._wire_ntcip_events()
            # DetectorMonitor.start() launches the polling thread
            self._ntcip_monitor.start()
            log.info(
                "NTCIP monitor started",
                extra={"intersection_id": self._intersection_id},
            )
            # Feed the monitor's measured sweep time to the discrepancy engine
            # on a slow cadence, replacing the configured assumption.
            _start_in_thread(
                target=self._sampling_floor_updater,
                name="sampling-floor-updater",
            )

        log.info(
            "System fully operational — waiting for shutdown signal",
            extra={"intersection_id": self._intersection_id},
        )

        # Block the main thread here.  signal handlers call shutdown() which
        # sets this event, unblocking us.
        self._shutdown_event.wait()

        log.info(
            "Shutdown event received — beginning ordered teardown",
            extra={"intersection_id": self._intersection_id},
        )
        self._teardown()

    def shutdown(self) -> None:
        """Signal the main :meth:`run` loop to begin an ordered teardown.

        Safe to call from a signal handler, another thread, or a test.
        Idempotent — repeated calls are harmless.
        """
        self._shutdown_event.set()

    # ------------------------------------------------------------------
    # Sampling-floor injection
    # ------------------------------------------------------------------

    def _sampling_floor_updater(self) -> None:
        """Push the NTCIP monitor's measured sampling cycle into the engine.

        This is the whole reason the floor is *injected* rather than read:
        ``discrepancy_engine`` must not import ``ntcip_monitor``, so the only
        place both are visible is here.  Runs until shutdown, waking every
        :data:`_SAMPLING_FLOOR_UPDATE_SEC` — the floor is a property of the
        controller link and moves slowly, so polling it faster buys nothing.

        Before the detector monitor has completed its first cycle,
        ``effective_cycle_sec()`` returns ``0.0``; the configured value stays
        in force until a real measurement exists.
        """
        while not self._shutdown_event.wait(_SAMPLING_FLOOR_UPDATE_SEC):
            try:
                measured = float(self._ntcip_monitor.effective_cycle_sec())
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "Failed to read effective sampling cycle",
                    extra={
                        "intersection_id": self._intersection_id,
                        "error": str(exc),
                    },
                )
                continue

            if measured <= 0.0:
                continue  # No completed cycle yet — keep the configured floor.

            self._discrepancy_monitor.set_sampling_floor(measured)

        log.debug(
            "Sampling-floor updater exiting",
            extra={"intersection_id": self._intersection_id},
        )

    # ------------------------------------------------------------------
    # Ordered teardown
    # ------------------------------------------------------------------

    def _teardown(self) -> None:
        """Stop every subsystem in the correct dependency order.

        Order is critical:
        1. **NTCIP monitor first** — stops new detector callbacks from firing
           so no new trigger files are generated mid-shutdown.
        2. **DiscrepancyMonitor** — evaluator thread exits cleanly.
        3. **RoutineScheduler** — scheduler thread exits cleanly.
        4. **VideoBufferManager last** — allows in-flight DiskWriter threads
           to drain their frame queues and finalise MP4 files before the
           process exits.  ``VideoBufferManager.stop()`` joins those threads.
        """
        # Step 1 — Stop NTCIP monitor
        if self._ntcip_monitor is not None:
            log.info("Stopping NTCIP monitor")
            try:
                # DetectorMonitor.stop() (inherited from BaseMonitor)
                self._ntcip_monitor.stop()
            except Exception as exc:  # noqa: BLE001
                log.error("Error stopping NTCIP monitor", extra={"error": str(exc)})

        # Step 2 — Stop discrepancy evaluator
        log.info("Stopping DiscrepancyMonitor")
        try:
            self._discrepancy_monitor.stop()
        except Exception as exc:  # noqa: BLE001
            log.error("Error stopping DiscrepancyMonitor", extra={"error": str(exc)})

        # Step 3 — Stop routine scheduler
        log.info("Stopping RoutineScheduler")
        try:
            self._routine_scheduler.stop()
        except Exception as exc:  # noqa: BLE001
            log.error("Error stopping RoutineScheduler", extra={"error": str(exc)})

        # Step 4 — Stop video buffer (blocks until DiskWriters flush)
        log.info("Stopping VideoBufferManager — waiting for writers to flush")
        try:
            self._video_manager.stop()
        except Exception as exc:  # noqa: BLE001
            log.error("Error stopping VideoBufferManager", extra={"error": str(exc)})

        log.info(
            "Teardown complete — exiting",
            extra={"intersection_id": self._intersection_id},
        )

    # ------------------------------------------------------------------
    # Video buffer backend selection
    # ------------------------------------------------------------------

    def _build_video_manager(
        self,
        stream_map: Dict[str, str],
        pre_roll_sec: float,
        max_concurrent_writers: int,
        min_free_disk_mb: float,
    ) -> Any:
        """Build the video-buffer backend.

        ``remux_video_buffer`` (PyAV stream-copy) is the only backend: accurate
        clip length by construction, near-zero CPU, RAM bounded by a time
        window rather than by clip length. The legacy ``"full"`` CFR
        ``cv2.VideoWriter`` backend was retired 2026-08-01 (ROADMAP Item 6) —
        it was unselected by every deployment, strictly worse on all three edge
        constraints, and buffered a whole clip in RAM. A config still carrying
        ``"video_backend"`` is honored as remux and warned about rather than
        rejected, so a stale deployment config keeps recording.

        Args:
            stream_map: Mapping of ``camera_id`` -> stream URL.
            pre_roll_sec: Shared pre-roll window length.
            max_concurrent_writers: Concurrent-recording cap.
            min_free_disk_mb: Disk-free abort threshold.

        Returns:
            A constructed (not yet started) video buffer manager.
        """
        backend = str(self._intersection_cfg.get("video_backend", "remux")).lower()
        if backend != "remux":
            log.warning(
                "Ignoring 'video_backend' — 'remux' is the only backend "
                "(the CFR 'full' backend was retired; see ROADMAP Item 6)",
                extra={
                    "intersection_id": self._intersection_id,
                    "video_backend": backend,
                },
            )
        from remux_video_buffer import VideoBufferConfig, VideoBufferManager

        log.info(
            "Using 'remux' (stream-copy) video backend",
            extra={"intersection_id": self._intersection_id},
        )
        video_cfg = VideoBufferConfig(
            streams=stream_map,
            trigger_dir=str(self._trigger_dir),
            output_dir=str(self._output_dir),
            pre_roll_sec=pre_roll_sec,
            poll_interval_sec=2.0,
            max_concurrent_writers=max_concurrent_writers,
            min_free_disk_mb=min_free_disk_mb,
            backend="remux",
        )
        return VideoBufferManager(video_cfg)

    # ------------------------------------------------------------------
    # NTCIP wiring
    # ------------------------------------------------------------------

    def _build_ntcip_monitor(self) -> Any:
        """Constructs a DetectorMonitor (backed by EconoliteSNMPClient) from intersection config.

        Returns:
            A configured (but not yet started) :class:`DetectorMonitor` instance.

        The ``poll_interval_sec`` value is read from the intersection config.
        If it is below 0.5 s, ``config_manager`` already emitted a warning
        during validation; we pass it through unchanged so the NTCIP monitor
        can apply its own guard.
        """
        ctrl_ip: str = self._intersection_cfg["controller_ip"]
        snmp_port: int = int(self._intersection_cfg.get("snmp_port", 501))
        community: str = self._intersection_cfg.get("snmp_community", "public")
        poll_interval: float = float(
            self._intersection_cfg.get("poll_interval_sec", 1.0)
        )
        # ROADMAP 4a: each detector group is one SNMP round trip at
        # chunk_size=1, so the sweep is RTT-bound (~1.0-1.5 s measured
        # 2026-07-19). Two config-driven mitigations:
        #  - poll only the groups the configured detectors occupy (the
        #    derived range below) instead of all 8;
        #  - "snmp_chunk_size" (default 1) raises OIDs-per-PDU — set it >1
        #    ONLY after a green __probe_snmp_batch.py run on this controller.
        chunk_size: int = int(self._intersection_cfg.get("snmp_chunk_size", 1))
        det_ids = [
            int(d) for d in self._intersection_cfg.get("detectors", {})
            if str(d).isdigit()
        ]
        detector_range = (min(det_ids), max(det_ids) + 1) if det_ids else (1, 65)

        log.info(
            "Building NTCIP monitor",
            extra={
                "controller_ip": ctrl_ip,
                "snmp_port": snmp_port,
                "poll_interval_sec": poll_interval,
                "snmp_chunk_size": chunk_size,
                "detector_range": list(detector_range),
            },
        )

        snmp_client = EconoliteSNMPClient(
            ip=ctrl_ip,
            port=snmp_port,
            community=community,
            timeout=2,
            retries=2,
            chunk_size=chunk_size,
        )

        return DetectorMonitor(
            snmp_client,
            poll_interval=poll_interval,
            detector_range=detector_range,
        )

    def _wire_ntcip_events(self) -> None:
        """Subscribe DiscrepancyMonitor callbacks to NTCIP detector events.

        The callbacks are non-blocking (microsecond execution) as required by
        the project constitution.  The NTCIP monitor's event thread is never
        held waiting for I/O.
        """
        self._ntcip_monitor.on(EVENT_DETECTOR_ON, self._discrepancy_monitor.on_detector_on)
        self._ntcip_monitor.on(EVENT_DETECTOR_OFF, self._discrepancy_monitor.on_detector_off)

        log.info(
            "NTCIP event callbacks wired",
            extra={
                "intersection_id": self._intersection_id,
                "subscribed_events": ["detector_on", "detector_off"],
            },
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _start_in_thread(target: Any, name: str) -> threading.Thread:
    """Launch ``target`` in a named daemon thread and return the thread.

    Used for subsystems whose ``start()`` method blocks (e.g.
    ``VideoBufferManager.start()`` runs the poll loop forever).

    Args:
        target: Callable to run in the background thread.
        name: Thread name (appears in debuggers and ``ps`` output).

    Returns:
        The started :class:`threading.Thread`.
    """
    thread = threading.Thread(target=target, name=name, daemon=True)
    thread.start()
    log.debug("Background thread launched", extra={"thread_name": name})
    return thread


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------

def _install_signal_handlers(runner: SystemRunner) -> None:
    """Register SIGINT and SIGTERM handlers that trigger graceful shutdown.

    Both signals call :meth:`SystemRunner.shutdown`, which sets the internal
    event that unblocks :meth:`SystemRunner.run` and starts the ordered
    teardown.  The handler is registered on the main thread (Python's signal
    handling requirement).

    Args:
        runner: The :class:`SystemRunner` instance to shut down on signal.
    """
    def _handler(signum: int, _frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        log.info(
            "Signal received — initiating graceful shutdown",
            extra={"signal": sig_name},
        )
        runner.shutdown()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    log.debug("Signal handlers installed", extra={"signals": ["SIGINT", "SIGTERM"]})


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed :class:`argparse.Namespace` with the following attributes:

        - ``intersection``: Intersection ID string.
        - ``config``: Path to the ``intersections.json`` file.
        - ``trigger_dir``: Hot Folder directory path.
        - ``output_dir``: Completed-video output directory.
        - ``min_free_mb``: Minimum free disk space in MB before aborting a
          recording.
        - ``max_writers``: Maximum concurrent video writers.
    """
    parser = argparse.ArgumentParser(
        description="Traffic monitoring system runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--intersection",
        default="1234_main",
        help="Canonical intersection_id to monitor.",
    )
    parser.add_argument(
        "--config",
        default="./intersections.json",
        help="Path to the intersections JSON configuration file.",
    )
    parser.add_argument(
        "--trigger-dir",
        default="./trigger_queue",
        dest="trigger_dir",
        help="Hot Folder directory for trigger files.",
    )
    parser.add_argument(
        "--output-dir",
        default="./completed_videos",
        dest="output_dir",
        help="Directory for completed MP4 recordings.",
    )
    parser.add_argument(
        "--min-free-mb",
        type=float,
        default=500.0,
        dest="min_free_mb",
        help="Minimum free disk space (MB) before recording is aborted.",
    )
    parser.add_argument(
        "--max-writers",
        type=int,
        default=2,
        dest="max_writers",
        help="Maximum simultaneous video encoders (keep ≤ 2 on J1900).",
    )
    return parser.parse_args()


def main() -> None:
    """Parse arguments, build the runner, install signal handlers, and run.

    This is the sole entry-point for production use.  It exits with code 0
    on clean shutdown and code 1 on startup failure.
    """
    args = _parse_args()

    log.info(
        "system_runner starting",
        extra={
            "intersection": args.intersection,
            "config": args.config,
            "trigger_dir": args.trigger_dir,
            "output_dir": args.output_dir,
        },
    )

    runner = SystemRunner(
        intersection_id=args.intersection,
        config_path=args.config,
        trigger_dir=args.trigger_dir,
        output_dir=args.output_dir,
        min_free_disk_mb=args.min_free_mb,
        max_concurrent_writers=args.max_writers,
    )

    _install_signal_handlers(runner)

    # run() blocks until shutdown() is called by a signal handler.
    runner.run()
    sys.exit(0)


if __name__ == "__main__":
    main()
