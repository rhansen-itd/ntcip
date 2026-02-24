"""
config_manager.py — Hardware-agnostic configuration abstraction layer.

This module defines the ``ConfigProvider`` interface and its concrete
implementations so that all upstream business logic (NTCIP monitoring,
discrepancy detection, video triggering) can load intersection configuration
through a single, stable API regardless of the deployment target.

────────────────────────────────────────────────────────────────────────────
Expected JSON schema (``JsonFileConfigProvider``)
────────────────────────────────────────────────────────────────────────────

A single JSON file may contain **one or more** intersection blocks, keyed by
``intersection_id``.  Example:

.. code-block:: json

    {
      "1234_main": {
        "intersection_id": "1234_main",
        "description": "Main St & 1st Ave",
        "controller_ip": "192.168.10.5",
        "snmp_port": 501,
        "snmp_community": "public",
        "poll_interval_sec": 1.0,
        "timezone": "America/Boise",
        "cameras": {
          "cam1": {
            "url": "rtsp://192.168.10.21/stream1",
            "description": "Northbound approach",
            "pre_roll_sec": 10,
            "post_roll_sec": 20
          },
          "cam2": {
            "url": "rtsp://192.168.10.22/stream1",
            "description": "Eastbound approach",
            "pre_roll_sec": 10,
            "post_roll_sec": 20
          }
        },
        "detectors": {
          "1": {
            "type": "radar",
            "phase": 2,
            "description": "Northbound radar advance",
            "paired_detector_id": "2",
            "camera_id": "cam1",
            "lag_threshold_sec": 2.0
          },
          "2": {
            "type": "loop",
            "phase": 2,
            "description": "Northbound stop-bar loop",
            "paired_detector_id": "1",
            "camera_id": "cam1",
            "lag_threshold_sec": 2.0
          },
          "3": {
            "type": "radar",
            "phase": 4,
            "description": "Eastbound radar advance",
            "paired_detector_id": "4",
            "camera_id": "cam2",
            "lag_threshold_sec": 2.0
          },
          "4": {
            "type": "loop",
            "phase": 4,
            "description": "Eastbound stop-bar loop",
            "paired_detector_id": "3",
            "camera_id": "cam2",
            "lag_threshold_sec": 2.0
          }
        }
      },
      "5678_oak": {
        "intersection_id": "5678_oak",
        "description": "Oak Ave & Commerce Blvd",
        "controller_ip": "192.168.10.8",
        "snmp_port": 501,
        "snmp_community": "public",
        "poll_interval_sec": 0.5,
        "timezone": "America/Boise",
        "cameras": {
          "cam3": {
            "url": "rtsp://192.168.10.31/stream1",
            "description": "Southbound approach",
            "pre_roll_sec": 15,
            "post_roll_sec": 25
          }
        },
        "detectors": {
          "5": {
            "type": "loop",
            "phase": 6,
            "description": "Southbound stop-bar loop",
            "paired_detector_id": null,
            "camera_id": "cam3",
            "lag_threshold_sec": 1.5
          }
        }
      }
    }

────────────────────────────────────────────────────────────────────────────
Field reference
────────────────────────────────────────────────────────────────────────────

Top-level intersection fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``intersection_id``     str   — Canonical ID matching the key and trigger schema.
``description``         str   — Human-readable label (optional but recommended).
``controller_ip``       str   — IPv4 address of the NTCIP controller.
``snmp_port``           int   — UDP port (almost always 161).
``snmp_community``      str   — SNMP v1/v2c community string.
``poll_interval_sec``   float — SNMP polling cadence; warn if < 0.5.
``timezone``            str   — IANA timezone name (e.g. ``"America/Boise"``).
                                Used by the scheduler and trigger layer so that
                                scheduled times and ISO-8601 filenames reflect
                                the intersection's local time rather than the
                                host machine's system timezone.

cameras mapping (camera_id → CameraConfig)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``url``                 str   — RTSP or HTTP stream URL.
``description``         str   — Human-readable label (optional).
``pre_roll_sec``        int   — Seconds of RAM buffer to prepend to clips.
``post_roll_sec``       int   — Seconds of live recording after trigger stop.

detectors mapping (detector_id → DetectorConfig)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``type``                str   — ``"radar"``, ``"loop"``, or ``"video"``.
``phase``               int   — NTCIP signal phase this detector belongs to.
``description``         str   — Human-readable label (optional).
``paired_detector_id``  str|null — ID of the complementary detector for lag
                                   comparison, or ``null`` if unpaired.
``camera_id``           str   — ID of the camera that covers this detector;
                                 used by the trigger layer to select a stream.
``lag_threshold_sec``   float — Seconds of actuation lag that constitutes a
                                 discrepancy worth triggering a clip.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class ConfigProvider(ABC):
    """Abstract interface for intersection configuration retrieval.

    All business logic must depend on this interface, never on a concrete
    implementation, so that the same analysis code runs unchanged on an
    edge J1900 node (JSON file) and a central server (SQLite / Postgres).

    Implementations are expected to be lightweight objects that can be
    constructed once and reused across the lifetime of the process.
    """

    @abstractmethod
    def get_intersection_config(self, intersection_id: str) -> dict:
        """Return the full configuration block for a single intersection.

        Args:
            intersection_id: The canonical intersection identifier string
                (e.g. ``"1234_main"``).  This value must match the
                ``intersection_id`` used in trigger files.

        Returns:
            A dictionary conforming to the schema documented in this
            module's docstring.  The returned dict is a **deep copy**;
            callers may mutate it freely without affecting the provider's
            internal state.

        Raises:
            KeyError: If ``intersection_id`` is not found in the backing
                store.
            ConfigProviderError: If the backing store cannot be read or
                the stored data fails validation.
        """

    @abstractmethod
    def list_intersection_ids(self) -> list[str]:
        """Return all intersection IDs known to this provider.

        Returns:
            A sorted list of intersection ID strings.  May be empty if the
            backing store contains no intersections.

        Raises:
            ConfigProviderError: If the backing store cannot be read.
        """

    def get_camera_config(self, intersection_id: str, camera_id: str) -> dict:
        """Convenience accessor for a single camera within an intersection.

        Args:
            intersection_id: Canonical intersection identifier.
            camera_id: Camera identifier (e.g. ``"cam1"``).

        Returns:
            The camera configuration dict for ``camera_id``.

        Raises:
            KeyError: If either ``intersection_id`` or ``camera_id`` is not
                found.
            ConfigProviderError: Propagated from
                :meth:`get_intersection_config`.
        """
        cfg = self.get_intersection_config(intersection_id)
        cameras: dict = cfg.get("cameras", {})
        if camera_id not in cameras:
            raise KeyError(
                f"Camera '{camera_id}' not found in intersection '{intersection_id}'."
            )
        return cameras[camera_id]

    def get_detector_config(self, intersection_id: str, detector_id: str) -> dict:
        """Convenience accessor for a single detector within an intersection.

        Args:
            intersection_id: Canonical intersection identifier.
            detector_id: Detector identifier, coerced to ``str`` so callers
                may pass integer NTCIP OID indices directly.

        Returns:
            The detector configuration dict for ``detector_id``.

        Raises:
            KeyError: If either ``intersection_id`` or ``detector_id`` is
                not found.
            ConfigProviderError: Propagated from
                :meth:`get_intersection_config`.
        """
        cfg = self.get_intersection_config(intersection_id)
        detectors: dict = cfg.get("detectors", {})
        key = str(detector_id)
        if key not in detectors:
            raise KeyError(
                f"Detector '{key}' not found in intersection '{intersection_id}'."
            )
        return detectors[key]


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------

class ConfigProviderError(RuntimeError):
    """Raised when a :class:`ConfigProvider` cannot fulfil a request.

    Wraps lower-level I/O or parse errors so callers need only catch one
    exception type from this module.

    Args:
        message: Human-readable description of the failure.
        cause: The original exception, if any.
    """

    def __init__(self, message: str, cause: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.__cause__ = cause


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

_REQUIRED_TOP_LEVEL = {"intersection_id", "controller_ip", "snmp_port", "cameras", "detectors", "timezone"}
_REQUIRED_CAMERA = {"url"}
_REQUIRED_DETECTOR = {"type", "phase", "camera_id"}
_VALID_DETECTOR_TYPES = {"radar", "loop", "video"}
_MIN_POLL_INTERVAL = 0.5


def _validate_intersection(cfg: dict, source_label: str) -> None:
    """Validate that a configuration dict conforms to the expected schema.

    Validation is intentionally non-destructive: missing *optional* fields
    are tolerated but logged; missing *required* fields raise immediately.

    Args:
        cfg: The intersection configuration dictionary to validate.
        source_label: A human-readable identifier used in error messages
            (e.g. a filename or database row ID).

    Raises:
        ConfigProviderError: If required fields are absent or values are
            semantically invalid.
    """
    iid = cfg.get("intersection_id", "<unknown>")

    missing_top = _REQUIRED_TOP_LEVEL - cfg.keys()
    if missing_top:
        raise ConfigProviderError(
            f"[{source_label}] Intersection '{iid}' is missing required fields: {missing_top}"
        )

    poll = cfg.get("poll_interval_sec")
    if poll is not None and poll < _MIN_POLL_INTERVAL:
        log.warning(
            "poll_interval_sec below minimum",
            extra={
                "intersection_id": iid,
                "poll_interval_sec": poll,
                "minimum": _MIN_POLL_INTERVAL,
            },
        )

    for cam_id, cam in cfg.get("cameras", {}).items():
        missing_cam = _REQUIRED_CAMERA - cam.keys()
        if missing_cam:
            raise ConfigProviderError(
                f"[{source_label}] Camera '{cam_id}' in intersection '{iid}' "
                f"is missing required fields: {missing_cam}"
            )

    for det_id, det in cfg.get("detectors", {}).items():
        missing_det = _REQUIRED_DETECTOR - det.keys()
        if missing_det:
            raise ConfigProviderError(
                f"[{source_label}] Detector '{det_id}' in intersection '{iid}' "
                f"is missing required fields: {missing_det}"
            )
        det_type = det.get("type", "")
        if det_type not in _VALID_DETECTOR_TYPES:
            raise ConfigProviderError(
                f"[{source_label}] Detector '{det_id}' in intersection '{iid}' "
                f"has invalid type '{det_type}'. Must be one of {_VALID_DETECTOR_TYPES}."
            )
        cam_ref = det.get("camera_id")
        if cam_ref and cam_ref not in cfg.get("cameras", {}):
            raise ConfigProviderError(
                f"[{source_label}] Detector '{det_id}' in intersection '{iid}' "
                f"references unknown camera_id '{cam_ref}'."
            )


# ---------------------------------------------------------------------------
# JsonFileConfigProvider — edge / single-node deployment
# ---------------------------------------------------------------------------

class JsonFileConfigProvider(ConfigProvider):
    """Load intersection configurations from a local JSON file.

    Suitable for edge deployments (e.g. Intel J1900 per-intersection nodes)
    where a single JSON file is written once during commissioning and read
    repeatedly at runtime.  The file is parsed eagerly on construction and
    cached in memory for the lifetime of the object; call :meth:`reload` to
    pick up changes without restarting the process.

    Args:
        config_path: Path to the JSON configuration file.  May be a
            ``str`` or :class:`pathlib.Path`.
        validate: If ``True`` (default), each intersection block is
            validated against the expected schema on load.

    Raises:
        ConfigProviderError: If the file does not exist, cannot be parsed,
            or fails schema validation (when ``validate=True``).

    Example::

        provider = JsonFileConfigProvider("/etc/traffic/intersections.json")
        cfg = provider.get_intersection_config("1234_main")
        rtsp_url = cfg["cameras"]["cam1"]["url"]
    """

    def __init__(self, config_path: str | Path, validate: bool = True) -> None:
        self._path = Path(config_path)
        self._validate = validate
        self._data: Dict[str, dict] = {}
        self._load()

    # ------------------------------------------------------------------
    # ConfigProvider interface
    # ------------------------------------------------------------------

    def get_intersection_config(self, intersection_id: str) -> dict:
        """Return a deep copy of the config block for ``intersection_id``.

        Args:
            intersection_id: Canonical intersection identifier.

        Returns:
            Deep-copied configuration dictionary.

        Raises:
            KeyError: If ``intersection_id`` is not present in the file.
            ConfigProviderError: Propagated from the internal load if the
                file has become unreadable since construction.
        """
        if intersection_id not in self._data:
            raise KeyError(
                f"Intersection '{intersection_id}' not found in '{self._path}'."
            )
        return _deep_copy_dict(self._data[intersection_id])

    def list_intersection_ids(self) -> list[str]:
        """Return all intersection IDs present in the JSON file.

        Returns:
            Sorted list of intersection ID strings.
        """
        return sorted(self._data.keys())

    # ------------------------------------------------------------------
    # Extras
    # ------------------------------------------------------------------

    def reload(self) -> None:
        """Re-read the JSON file from disk and replace the in-memory cache.

        Useful when the configuration file is updated in place without
        restarting the process (e.g. during commissioning or remote
        reconfiguration).

        Raises:
            ConfigProviderError: If the file cannot be read or parsed.
        """
        self._load()
        log.info("Config reloaded", extra={"path": str(self._path)})

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Read, parse, and optionally validate the JSON file.

        Raises:
            ConfigProviderError: On any I/O or parse failure.
        """
        if not self._path.exists():
            raise ConfigProviderError(
                f"Configuration file not found: '{self._path}'"
            )

        try:
            raw = self._path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConfigProviderError(
                f"Cannot read configuration file '{self._path}': {exc}", cause=exc
            ) from exc

        try:
            parsed: Dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigProviderError(
                f"Invalid JSON in configuration file '{self._path}': {exc}", cause=exc
            ) from exc

        if not isinstance(parsed, dict):
            raise ConfigProviderError(
                f"Configuration file '{self._path}' must contain a JSON object at "
                "the top level, keyed by intersection_id."
            )

        if self._validate:
            for iid, block in parsed.items():
                _validate_intersection(block, source_label=str(self._path))

        self._data = parsed
        log.info(
            "Configuration loaded",
            extra={"path": str(self._path), "intersections": list(parsed.keys())},
        )


# ---------------------------------------------------------------------------
# SqliteCentralConfigProvider — central server deployment
# ---------------------------------------------------------------------------

class SqliteCentralConfigProvider(ConfigProvider):
    """Load intersection configurations from a SQLite database.

    Intended for central server deployments managing multiple intersections.
    Each intersection's configuration is stored as a JSON blob in a single
    ``intersections`` table, making it straightforward to migrate the same
    schema to PostgreSQL (via ``psycopg2``) by swapping only this class.

    Expected table DDL::

        CREATE TABLE IF NOT EXISTS intersections (
            intersection_id TEXT PRIMARY KEY,
            config_json     TEXT NOT NULL,
            updated_at      REAL NOT NULL   -- Unix timestamp
        );

    Args:
        db_path: Path to the SQLite database file.  Pass ``":memory:"`` for
            ephemeral in-process use (useful in tests).
        validate: If ``True`` (default), fetched records are validated
            against the expected schema before being returned.

    Raises:
        ConfigProviderError: If the database cannot be opened.

    Example::

        provider = SqliteCentralConfigProvider("/var/db/traffic.db")
        cfg = provider.get_intersection_config("1234_main")
    """

    _DDL = """
        CREATE TABLE IF NOT EXISTS intersections (
            intersection_id TEXT PRIMARY KEY,
            config_json     TEXT NOT NULL,
            updated_at      REAL NOT NULL
        );
    """

    def __init__(self, db_path: str | Path, validate: bool = True) -> None:
        self._db_path = str(db_path)
        self._validate = validate
        try:
            # check_same_thread=False: provider is read-only and called from
            # a single analysis thread; adjust if write access is added.
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute(self._DDL)
            self._conn.commit()
        except sqlite3.Error as exc:
            raise ConfigProviderError(
                f"Cannot open SQLite database '{self._db_path}': {exc}", cause=exc
            ) from exc

        log.info("SQLite config provider ready", extra={"db_path": self._db_path})

    # ------------------------------------------------------------------
    # ConfigProvider interface
    # ------------------------------------------------------------------

    def get_intersection_config(self, intersection_id: str) -> dict:
        """Fetch and return the configuration for a single intersection.

        Args:
            intersection_id: Canonical intersection identifier.

        Returns:
            Deep-copied configuration dictionary.

        Raises:
            KeyError: If ``intersection_id`` is not found in the database.
            ConfigProviderError: On database or parse errors.
        """
        try:
            cursor = self._conn.execute(
                "SELECT config_json FROM intersections WHERE intersection_id = ?",
                (intersection_id,),
            )
            row = cursor.fetchone()
        except sqlite3.Error as exc:
            raise ConfigProviderError(
                f"Database error querying intersection '{intersection_id}': {exc}",
                cause=exc,
            ) from exc

        if row is None:
            raise KeyError(
                f"Intersection '{intersection_id}' not found in database '{self._db_path}'."
            )

        try:
            cfg: dict = json.loads(row["config_json"])
        except json.JSONDecodeError as exc:
            raise ConfigProviderError(
                f"Corrupt JSON for intersection '{intersection_id}' in "
                f"database '{self._db_path}': {exc}",
                cause=exc,
            ) from exc

        if self._validate:
            _validate_intersection(cfg, source_label=f"db:{self._db_path}")

        return cfg

    def list_intersection_ids(self) -> list[str]:
        """Return all intersection IDs stored in the database.

        Returns:
            Sorted list of intersection ID strings.

        Raises:
            ConfigProviderError: On database errors.
        """
        try:
            cursor = self._conn.execute(
                "SELECT intersection_id FROM intersections ORDER BY intersection_id"
            )
            return [row["intersection_id"] for row in cursor.fetchall()]
        except sqlite3.Error as exc:
            raise ConfigProviderError(
                f"Database error listing intersections: {exc}", cause=exc
            ) from exc

    def upsert_intersection(self, cfg: dict) -> None:
        """Insert or replace an intersection configuration in the database.

        This is a convenience method for provisioning and testing.  Production
        deployments may prefer a dedicated migration/admin tool.

        Args:
            cfg: Configuration dictionary conforming to the module schema.
                 ``intersection_id`` must be present.

        Raises:
            ConfigProviderError: If ``intersection_id`` is missing, validation
                fails, or the database write fails.
        """
        if "intersection_id" not in cfg:
            raise ConfigProviderError("cfg must contain 'intersection_id'.")

        if self._validate:
            _validate_intersection(cfg, source_label="upsert")

        import time as _time  # local import to avoid shadowing at module scope

        try:
            self._conn.execute(
                """
                INSERT INTO intersections (intersection_id, config_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(intersection_id) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at  = excluded.updated_at
                """,
                (cfg["intersection_id"], json.dumps(cfg), _time.time()),
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            raise ConfigProviderError(
                f"Failed to upsert intersection '{cfg['intersection_id']}': {exc}",
                cause=exc,
            ) from exc

        log.info(
            "Intersection upserted",
            extra={"intersection_id": cfg["intersection_id"], "db_path": self._db_path},
        )

    def close(self) -> None:
        """Close the underlying SQLite connection.

        Should be called when the provider is no longer needed, typically
        during process shutdown.
        """
        self._conn.close()
        log.info("SQLite config provider closed", extra={"db_path": self._db_path})


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _deep_copy_dict(obj: Any) -> Any:
    """Return a deep copy of a JSON-serialisable object via round-trip.

    Using ``json`` rather than ``copy.deepcopy`` is intentional: it is
    faster for plain data structures and guarantees that only JSON-safe
    types survive, which aligns with the schema contract.

    Args:
        obj: A JSON-serialisable Python object.

    Returns:
        A fully independent deep copy.
    """
    return json.loads(json.dumps(obj))
