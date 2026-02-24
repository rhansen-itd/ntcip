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

# Video buffer (Session 1)
from video_buffer import VideoBufferConfig, VideoBufferManager

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
        output_dir: Directory where completed MP4 clips are stored.
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

        video_cfg = VideoBufferConfig(
            streams=stream_map,
            trigger_dir=str(self._trigger_dir),
            output_dir=str(self._output_dir),
            pre_roll_sec=pre_roll_sec,
            poll_interval_sec=2.0,        # conservative; adjust if needed
            max_concurrent_writers=max_concurrent_writers,
            min_free_disk_mb=min_free_disk_mb,
        )
        self._video_manager = VideoBufferManager(video_cfg)

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

        log.info(
            "Building NTCIP monitor",
            extra={
                "controller_ip": ctrl_ip,
                "snmp_port": snmp_port,
                "poll_interval_sec": poll_interval,
            },
        )

        snmp_client = EconoliteSNMPClient(
            ip=ctrl_ip,
            port=snmp_port,
            community=community,
            timeout=2,
            retries=2,
        )

        return DetectorMonitor(
            snmp_client,
            poll_interval=poll_interval,
            detector_range=(1, 65),
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
