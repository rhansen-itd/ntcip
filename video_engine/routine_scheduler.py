"""
routine_scheduler.py — Scheduled baseline-recording trigger generator.

This module provides time-based, routine video recording on top of the
event-driven discrepancy system.  It reads a ``routine_recordings`` list
from the intersection configuration and drops ``"start"`` trigger files
into the Hot Folder at each designated wall-clock time.

Because the ``video_buffer`` package handles automatic stop via
``max_duration_sec``, this scheduler emits **only** ``"start"`` actions.
No ``"stop"`` file is ever written by this module.

Architecture overview
─────────────────────

┌─────────────────────────┐
│   RoutineScheduler      │
│                         │
│  • daemon thread        │
│    sleeps ~10 s         │
│    wakes → checks HH:MM │
│    vs. scheduled times  │
│                         │
│  • _last_fired dict     │
│    (label → date)       │
│    prevents double-fire │
│    within same minute   │
└──────────┬──────────────┘
           │ atomic JSON write
           ▼
┌─────────────────────────┐
│   Hot Folder            │
│   trigger_queue/        │
│   trigger_<ts>_<uid>    │
│   .json                 │
└─────────────────────────┘
           │ polled by
           ▼
┌─────────────────────────┐
│   video_buffer          │
│   (VideoBufferManager)  │
└─────────────────────────┘

Expected config schema extension
─────────────────────────────────
The ``routine_recordings`` key is **optional**.  If absent or empty the
scheduler starts cleanly and simply never fires.

.. code-block:: json

    {
      "1234_main": {
        "intersection_id": "1234_main",
        "controller_ip": "192.168.10.5",
        "snmp_port": 501,
        "cameras": { ... },
        "detectors": { ... },
        "routine_recordings": [
          {
            "label":        "morning_rush_baseline",
            "time":         "08:00",
            "duration_sec": 600,
            "cameras":      ["all"],
            "reason":       "morning_rush_baseline",
            "pre_roll_sec": 0
          },
          {
            "label":        "evening_peak",
            "time":         "17:00",
            "duration_sec": 900,
            "cameras":      ["cam1"],
            "reason":       "evening_peak_baseline",
            "pre_roll_sec": 5
          }
        ]
      }
    }

Routine entry field reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``label``         str  — Unique key within this intersection used for
                         deduplication.  Defaults to ``"time"`` value if
                         omitted, but a unique label is strongly recommended
                         when multiple routines share the same ``time``.
``time``          str  — 24-hour ``"HH:MM"`` wall-clock time in the local
                         timezone of the host machine.
``duration_sec``  int  — Clip length forwarded as ``max_duration_sec`` in
                         the trigger; the video-buffer layer auto-stops.
``cameras``       list — Camera IDs or ``["all"]``.
``reason``        str  — Free-form label embedded in the trigger ``reason``
                         field (use values meaningful to your reporting layer).
``pre_roll_sec``  int  — Pre-roll seconds (default 0; RAM buffer is rarely
                         useful for a scheduled recording whose start time
                         is known in advance).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
import pytz
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from config_manager import ConfigProvider, ConfigProviderError

# ---------------------------------------------------------------------------
# JSON-lines logger (consistent with video_buffer and discrepancy_engine)
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects.

    Merges any ``extra=`` keyword arguments passed to the logger call
    directly into the JSON payload, enabling structured field queries.
    """

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


def _build_logger(name: str) -> logging.Logger:
    """Return a logger configured for JSON-line output to stdout.

    Args:
        name: Logger hierarchy name (e.g. ``"routine_scheduler.1234_main"``).

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


log = _build_logger("routine_scheduler")

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
# Validation helpers
# ---------------------------------------------------------------------------

_REQUIRED_ROUTINE_FIELDS = {"time"}


def _parse_hhmm(value: str, label: str) -> tuple[int, int]:
    """Parse a ``"HH:MM"`` string into ``(hour, minute)`` integers.

    Args:
        value: The time string to parse.
        label: Human-readable routine label used in error messages.

    Returns:
        ``(hour, minute)`` tuple, both zero-indexed.

    Raises:
        ValueError: If ``value`` does not conform to ``"HH:MM"`` or the
            hour/minute values are out of range.
    """
    try:
        parts = value.strip().split(":")
        if len(parts) != 2:
            raise ValueError("Expected exactly one ':'")
        hour, minute = int(parts[0]), int(parts[1])
    except (ValueError, AttributeError) as exc:
        raise ValueError(
            f"Routine '{label}': invalid time format '{value}'. "
            "Expected 'HH:MM' (24-hour)."
        ) from exc

    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(
            f"Routine '{label}': time '{value}' has out-of-range "
            f"hour={hour} or minute={minute}."
        )
    return hour, minute


def _validate_routine_entry(entry: dict, index: int) -> str:
    """Validate a single routine entry and return its resolved label.

    Args:
        entry: A dictionary parsed from the ``routine_recordings`` list.
        index: Zero-based position in the list, used as fallback label.

    Returns:
        The resolved label string for this routine.

    Raises:
        ValueError: If required fields are missing or values are invalid.
    """
    missing = _REQUIRED_ROUTINE_FIELDS - entry.keys()
    if missing:
        raise ValueError(
            f"Routine at index {index} is missing required fields: {missing}"
        )

    label = str(entry.get("label") or entry["time"])
    _parse_hhmm(entry["time"], label)   # raises on invalid format

    duration = entry.get("duration_sec", 0)
    if not isinstance(duration, (int, float)) or duration <= 0:
        raise ValueError(
            f"Routine '{label}': 'duration_sec' must be a positive number, "
            f"got {duration!r}."
        )

    cameras = entry.get("cameras", ["all"])
    if not isinstance(cameras, list) or not cameras:
        raise ValueError(
            f"Routine '{label}': 'cameras' must be a non-empty list."
        )

    return label


# ---------------------------------------------------------------------------
# Parsed routine dataclass
# ---------------------------------------------------------------------------

class _RoutineEntry:
    """Immutable, validated snapshot of one ``routine_recordings`` entry.

    Args:
        raw: Raw dictionary from the intersection config.
        index: Position in the list (used for fallback label generation).

    Raises:
        ValueError: Propagated from :func:`_validate_routine_entry`.
    """

    __slots__ = (
        "label", "hour", "minute", "duration_sec",
        "cameras", "reason", "pre_roll_sec",
    )

    def __init__(self, raw: dict, index: int) -> None:
        self.label: str = _validate_routine_entry(raw, index)
        self.hour, self.minute = _parse_hhmm(raw["time"], self.label)
        self.duration_sec: float = float(raw.get("duration_sec", 600))
        self.cameras: List[str] = list(raw.get("cameras", ["all"]))
        self.reason: str = str(raw.get("reason", "routine_baseline"))
        self.pre_roll_sec: float = float(raw.get("pre_roll_sec", 0))

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"_RoutineEntry(label={self.label!r}, "
            f"time={self.hour:02d}:{self.minute:02d}, "
            f"duration_sec={self.duration_sec})"
        )


# ---------------------------------------------------------------------------
# RoutineScheduler
# ---------------------------------------------------------------------------

class RoutineScheduler:
    """Fires scheduled ``"start"`` trigger files into the Hot Folder.

    The scheduler runs a single daemon thread that wakes approximately every
    ``check_interval_sec`` seconds, reads the current local wall-clock time,
    and compares ``HH:MM`` against every routine in the loaded config.  When a
    match is found and the routine has not yet fired today, an atomic trigger
    file is written.

    The ``video_buffer`` layer handles the recording stop automatically via
    ``max_duration_sec``; this class **never** writes ``"stop"`` triggers.

    Args:
        intersection_id: Canonical intersection identifier.
        config_provider: :class:`~config_manager.ConfigProvider` used to
            load the ``routine_recordings`` list.
        trigger_dir: Hot Folder path where trigger JSON files are written.
        check_interval_sec: Seconds the background thread sleeps between
            schedule checks.  10 seconds is a safe default; the value must
            be less than 60 seconds to guarantee no scheduled minute is
            skipped entirely.
        post_roll_sec: Post-roll seconds embedded in every trigger (passed
            through to the video-buffer layer for context).

    Raises:
        ValueError: If ``check_interval_sec >= 60`` (would risk skipping a
            scheduled minute) or if the intersection config cannot be loaded.

    Example::

        provider = JsonFileConfigProvider("/etc/traffic/intersections.json")
        scheduler = RoutineScheduler(
            intersection_id="1234_main",
            config_provider=provider,
            trigger_dir="./trigger_queue",
        )
        scheduler.start()
        # Runs in the background; call scheduler.stop() for graceful shutdown.
    """

    def __init__(
        self,
        intersection_id: str,
        config_provider: ConfigProvider,
        trigger_dir: str | Path,
        check_interval_sec: float = 10.0,
        post_roll_sec: float = 0.0,
    ) -> None:
        if check_interval_sec >= 60:
            raise ValueError(
                f"check_interval_sec must be < 60 to guarantee no scheduled "
                f"minute is missed; got {check_interval_sec}."
            )

        self._intersection_id = intersection_id
        self._trigger_dir = Path(trigger_dir)
        self._check_interval_sec = check_interval_sec
        self._post_roll_sec = post_roll_sec
        self._log = _build_logger(f"routine_scheduler.{intersection_id}")

        self._trigger_dir.mkdir(parents=True, exist_ok=True)

        # Mutable state — protected by _config_lock
        self._routines: List[_RoutineEntry] = []
        self._config_lock = threading.RLock()

        # Resolved from intersection config during _load_routines.
        # Initialised to pytz.utc so the attribute always exists even if the
        # first load raises before the assignment.
        self._timezone: pytz.BaseTzInfo = pytz.utc

        # _last_fired: label → datetime.date of last successful trigger
        # No lock needed beyond _config_lock since only the scheduler thread
        # writes it (callbacks are not involved).
        self._last_fired: Dict[str, date] = {}

        self._load_routines(config_provider)

        self._running = False
        self._thread = threading.Thread(
            target=self._scheduler_loop,
            name=f"routine-scheduler-{intersection_id}",
            daemon=True,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background scheduler thread.

        Idempotent: subsequent calls while already running are silently
        ignored.
        """
        if self._running:
            return
        self._running = True
        self._thread.start()
        self._log.info(
            "RoutineScheduler started",
            extra={
                "intersection_id": self._intersection_id,
                "routines": self._routine_summary(),
                "check_interval_sec": self._check_interval_sec,
            },
        )

    def stop(self) -> None:
        """Signal the scheduler thread to exit and wait for it to join.

        Blocks for at most ``check_interval_sec * 2`` seconds to allow the
        sleeping thread to wake and notice the stop flag.
        """
        self._running = False
        self._thread.join(timeout=self._check_interval_sec * 2)
        self._log.info(
            "RoutineScheduler stopped",
            extra={"intersection_id": self._intersection_id},
        )

    def reload(self, config_provider: ConfigProvider) -> None:
        """Hot-reload the routine schedule without stopping the thread.

        The running thread continues using the previous schedule until the
        swap under ``_config_lock`` completes.  ``_last_fired`` entries for
        labels that survive the reload are preserved; entries for removed
        routines are pruned to avoid stale memory growth.

        Args:
            config_provider: Provider to fetch updated config from.

        Raises:
            ValueError: If the intersection config cannot be loaded or a
                routine entry fails validation.
        """
        self._load_routines(config_provider, preserve_fired=True)
        self._log.info(
            "Schedule reloaded",
            extra={
                "intersection_id": self._intersection_id,
                "routines": self._routine_summary(),
            },
        )

    # ------------------------------------------------------------------
    # Scheduler loop
    # ------------------------------------------------------------------

    def _scheduler_loop(self) -> None:
        """Background daemon: wake every N seconds and check the schedule.

        Uses ``time.sleep`` in a tight outer loop.  Wall-clock drift over the
        sleep period is irrelevant because we compare full ``HH:MM`` strings
        rather than relying on elapsed-time counting.

        ``datetime.now(self._timezone)`` is used so that the ``HH:MM``
        comparison reflects the intersection's IANA-configured local time,
        not the host machine's system timezone.
        """
        while self._running:
            now = datetime.now(self._timezone)
            try:
                self._check_schedule(now)
            except Exception:  # noqa: BLE001
                self._log.exception(
                    "Unhandled error in scheduler loop",
                    extra={"intersection_id": self._intersection_id},
                )
            time.sleep(self._check_interval_sec)

    def _check_schedule(self, now: datetime) -> None:
        """Compare current time against every routine and fire if due.

        A routine is due when:
        - ``now.hour == routine.hour`` and ``now.minute == routine.minute``,
          AND
        - The routine has not already fired today (``_last_fired[label] !=
          today``).

        Taking a snapshot of ``_routines`` under the lock and then releasing
        before doing any I/O keeps the critical section as short as possible.

        Args:
            now: Current local :class:`datetime.datetime` (injected so that
                 unit tests can supply a synthetic clock).
        """
        today = now.date()

        with self._config_lock:
            routines_snapshot = list(self._routines)

        for routine in routines_snapshot:
            if now.hour != routine.hour or now.minute != routine.minute:
                continue

            last = self._last_fired.get(routine.label)
            if last == today:
                # Already fired this minute/day — skip
                continue

            self._fire_trigger(routine, now)
            # Record after a successful write; _fire_trigger raises on I/O
            # failure so we only reach here on success.
            self._last_fired[routine.label] = today

    # ------------------------------------------------------------------
    # Trigger file generation
    # ------------------------------------------------------------------

    def _fire_trigger(self, routine: _RoutineEntry, event_dt: datetime) -> None:
        """Write an atomic ``"start"`` trigger file for a routine recording.

        The file is written to a ``.tmp`` path and atomically renamed to
        ``.json`` so the video-buffer poller never reads a partial file.

        Args:
            routine: The :class:`_RoutineEntry` that is due.
            event_dt: The :class:`datetime.datetime` at which the match was
                detected (used for the trigger filename and
                ``event_timestamp``).

        Raises:
            OSError: If the ``.tmp`` write or ``os.rename`` fails.  The
                caller is responsible for logging and deciding whether to
                retry.
        """
        trigger_id = uuid.uuid4().hex
        iso_ts = event_dt.strftime("%Y%m%dT%H%M%S%f")
        stem = f"trigger_{iso_ts}_{trigger_id[:8]}"
        tmp_path = self._trigger_dir / f"{stem}.tmp"
        json_path = self._trigger_dir / f"{stem}.json"

        payload = {
            "trigger_id": trigger_id,
            "action": "start",
            "event_timestamp": event_dt.timestamp(),
            "reason": routine.reason,
            "intersection_id": self._intersection_id,
            "cameras": routine.cameras,
            "pre_roll_sec": routine.pre_roll_sec,
            "post_roll_sec": self._post_roll_sec,
            "max_duration_sec": routine.duration_sec,
            "metadata": {
                "scheduled_label": routine.label,
                "scheduled_time": f"{routine.hour:02d}:{routine.minute:02d}",
                "trigger_source": "routine_scheduler",
            },
        }

        try:
            tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.rename(tmp_path, json_path)
        except OSError as exc:
            self._log.error(
                "Failed to write routine trigger file",
                extra={
                    "intersection_id": self._intersection_id,
                    "routine_label": routine.label,
                    "trigger_id": trigger_id,
                    "error": str(exc),
                },
            )
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

        self._log.info(
            "Routine trigger sent",
            extra={
                "trigger_id": trigger_id,
                "intersection_id": self._intersection_id,
                "routine_label": routine.label,
                "cameras": routine.cameras,
                "max_duration_sec": routine.duration_sec,
                "path": str(json_path),
            },
        )

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _load_routines(
        self,
        config_provider: ConfigProvider,
        preserve_fired: bool = False,
    ) -> None:
        """Fetch, parse, and validate the ``routine_recordings`` list.

        Args:
            config_provider: Provider to fetch the intersection config from.
            preserve_fired: If ``True``, existing ``_last_fired`` entries
                whose labels survive the reload are kept; stale labels are
                pruned.  If ``False`` (initial load), ``_last_fired`` is
                left empty.

        Raises:
            ValueError: If the intersection cannot be found, the config
                cannot be loaded, or any routine entry fails validation.
        """
        try:
            cfg = config_provider.get_intersection_config(self._intersection_id)
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Cannot load config for intersection "
                f"'{self._intersection_id}': {exc}"
            ) from exc

        # Resolve the intersection-local timezone.  _resolve_pytz uses pytz's
        # bundled IANA database so it works regardless of system zone files.
        # Falls back to pytz.utc (with a warning) only for genuinely unknown
        # zone names, so the scheduler can still run rather than refusing to start.
        tz_name: str = cfg.get("timezone", "UTC")
        self._timezone = _resolve_pytz(tz_name, self._log)

        raw_routines: list = cfg.get("routine_recordings", [])
        if not isinstance(raw_routines, list):
            raise ValueError(
                f"Intersection '{self._intersection_id}': "
                "'routine_recordings' must be a list."
            )

        new_routines: List[_RoutineEntry] = []
        seen_labels: set[str] = set()

        for idx, entry in enumerate(raw_routines):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"Intersection '{self._intersection_id}': "
                    f"routine at index {idx} must be a JSON object, "
                    f"got {type(entry).__name__}."
                )
            routine = _RoutineEntry(entry, idx)
            if routine.label in seen_labels:
                raise ValueError(
                    f"Intersection '{self._intersection_id}': "
                    f"duplicate routine label '{routine.label}'. "
                    "Each routine must have a unique label."
                )
            seen_labels.add(routine.label)
            new_routines.append(routine)

        with self._config_lock:
            self._routines = new_routines

            if preserve_fired:
                # Prune _last_fired entries whose labels no longer exist
                self._last_fired = {
                    label: fired_date
                    for label, fired_date in self._last_fired.items()
                    if label in seen_labels
                }

        self._log.debug(
            "Routines loaded",
            extra={
                "intersection_id": self._intersection_id,
                "count": len(new_routines),
                "labels": [r.label for r in new_routines],
            },
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _routine_summary(self) -> list[dict]:
        """Return a serialisable summary of the loaded routines for logging.

        Returns:
            List of dicts with ``label``, ``time``, and ``duration_sec``.
        """
        with self._config_lock:
            return [
                {
                    "label": r.label,
                    "time": f"{r.hour:02d}:{r.minute:02d}",
                    "duration_sec": r.duration_sec,
                }
                for r in self._routines
            ]

    @property
    def loaded_routines(self) -> List[_RoutineEntry]:
        """A snapshot copy of the currently loaded routine entries.

        Returns:
            List of :class:`_RoutineEntry` objects (shallow copy; entries
            are immutable so mutation of the list does not affect the
            scheduler).
        """
        with self._config_lock:
            return list(self._routines)
