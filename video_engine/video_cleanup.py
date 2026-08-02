"""video_cleanup.py — delete clips wholly contained inside another clip.

Follow-on to the cross-pair duplicate rejection of ROADMAP 9C4.  That item stops
the *engine* from firing two triggers for one physical event **within a detector
group**.  It cannot stop two clips of the same moment reaching disk when the
overlap is not a group duplicate at all: a short Rule 2 orphan clip that lands
entirely inside a long Rule 1 clip, two unrelated pairs disagreeing about the
same approach seconds apart, or a manually dropped trigger over live footage.
Those clips are not wrong — each was a real decision — but the second one holds
no footage the first does not already contain, and on a J1900 edge box disk is
the scarce resource.

This module is the disk-side cleanup: it deletes a clip **only** when another
clip from the **same camera** covers its whole time span, and rewrites every log
reference to name the surviving clip instead.  It is deliberately conservative
— it would rather keep a duplicate forever than lose a second of footage.

────────────────────────────────────────────────────────────────────────────
How a clip's wall-clock span is recovered (load-bearing)
────────────────────────────────────────────────────────────────────────────

Nothing in the recording path persists a clip's start and end wall-clock times,
so they are recovered from the finished file itself:

*   ``end_ts`` = the file's **mtime**.  ``ClipRemuxer._finalize`` closes the
    container as its last act, so mtime is the instant the last packet hit disk.
*   ``duration`` = the container's own duration (PyAV).  Per the remux design,
    clip length equals the source PTS span equals true elapsed time — there is
    no FPS to guess — so this is exact, not an estimate.
*   ``start_ts`` = ``end_ts - duration``.

That is cross-checked against the **dispatch epoch encoded in the filename**
(``{trigger8}_{camera}_{int(time.time())}{ext}``, written by
``remux_video_buffer._handle_start``): the dispatch instant must fall inside the
recovered span, give or take :data:`_DISPATCH_SLACK_SEC`.  A clip that fails the
check — most likely because its mtime was rewritten by a copy that did not
preserve it — is **skipped, never deleted**.  A file whose name does not parse
as a clip is ignored outright, so nothing else living in ``output_dir`` (the
three CSV logs, an operator's hand-named export) is ever a deletion candidate.

────────────────────────────────────────────────────────────────────────────
The containment rule
────────────────────────────────────────────────────────────────────────────

``outer`` contains ``inner`` when they share a camera and

    outer.start_ts <= inner.start_ts + tolerance
    outer.end_ts   >= inner.end_ts   - tolerance

The tolerance (default 0.5 s) exists for one reason: two clips of the *same*
moment differ by a few hundred milliseconds of poll latency, so a strict test
would keep both.  It is not a licence to delete a clip that genuinely starts
earlier or ends later than its container.

:func:`plan_removals` is a **single pass over clips ordered by (start asc, end
desc, name)**, keeping a running list of survivors and removing a clip only if
an already-kept clip contains it.  Three properties fall out and all three are
load-bearing:

*   **A keeper is never itself deleted**, so no log rewrite can ever point at a
    file that a later step removes, and no chain resolution is needed.
*   **Mutual containment resolves deterministically** — near-identical clips
    keep the earlier-starting, longer one, and exactly one of them dies.
*   **It is conservative by construction.**  A clip starting slightly *before* a
    much longer one is kept even though the longer clip nearly covers it,
    because it is not contained in the sense asked for.  Keeping an extra file
    is a cost; losing unique footage is a defect.

────────────────────────────────────────────────────────────────────────────
Order of operations, and the logs
────────────────────────────────────────────────────────────────────────────

**Logs are rewritten first, the file is deleted second.**  If the delete then
fails, the logs already name a clip that exists and still contains the event —
harmless, and the next sweep retries the delete with the rewrite already
idempotent.  The reverse order would leave a log row naming a file that is gone.

Which logs get rewritten is the single table :data:`REFERENCE_COLUMNS` —
``(filename, column)`` pairs relative to ``output_dir``.  Today that is
``discrepancies_log.csv`` / ``Video_Filename``, the only artifact in the tree
that names a clip file; ``engine_decisions.csv`` and ``engine_suppressions.csv``
are written before any clip exists and carry no filename.  Add a pair here when
a new consumer starts naming clips — that is the whole extension point.

Every deletion is recorded in ``video_cleanup_log.csv`` (in ``output_dir``,
alongside the other three).  Deleting footage is the one irreversible thing this
system does; the audit row carries both spans, so a reviewer can re-check the
containment decision after the evidence is gone.  ``_CLEANUP_LOG_FIELDS`` is
**append-only** for the same reason as the engine's two logs: a resumed file
keeps its original header.

Nothing here imports ``ntcip_monitor``, and nothing here imports
``remux_video_buffer`` — the manager imports *this*, one direction only.  PyAV is
imported lazily inside :func:`probe_duration_sec` so the module (and its test
suite) loads on a bare interpreter.
"""

from __future__ import annotations

import csv
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

log = logging.getLogger(__name__)

# Name of the audit log, written into output_dir next to the other three.
CLEANUP_LOG_NAME = "video_cleanup_log.csv"

# Append-only (see the module docstring).  Times are exact Unix floats on the
# monitoring machine's clock, like every other timestamp in this system;
# ``timestamp_utc`` is the same instant rendered for humans, in UTC rather than
# intersection-local because this module is handed no timezone.
_CLEANUP_LOG_FIELDS: Tuple[str, ...] = (
    "timestamp",
    "timestamp_utc",
    "camera_id",
    "deleted_file",
    "kept_file",
    "deleted_start_ts",
    "deleted_end_ts",
    "kept_start_ts",
    "kept_end_ts",
    "deleted_duration_sec",
    "bytes_reclaimed",
    "log_rows_updated",
    "tolerance_sec",
)

# (filename relative to output_dir, column naming a clip file).  The extension
# point for new consumers — see the module docstring.
REFERENCE_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("discrepancies_log.csv", "Video_Filename"),
)

# How far outside the recovered span the filename's dispatch epoch may fall
# before the clip is treated as untimeable and skipped.  Generous on purpose:
# the check exists to catch a rewritten mtime, not to police poll latency (and
# the filename's epoch is truncated to whole seconds, so it is up to 1 s early).
_DISPATCH_SLACK_SEC = 5.0


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClipSpan:
    """One finished clip, placed on the wall clock.

    Args:
        path: Absolute path to the clip file.
        camera_id: Camera the clip came from (parsed from the filename).
        trigger_prefix: First 8 hex chars of the trigger ID (filename field).
        dispatch_ts: Unix time encoded in the filename — when the buffer
            dispatched the recording, used only as a sanity check on the span.
        start_ts: Unix time of the clip's first frame (``end_ts - duration``).
        end_ts: Unix time of the clip's last frame (the file's mtime).
        duration_sec: Container duration; the true elapsed span by construction.
        size_bytes: File size, for the reclaimed-bytes tally.
    """

    path: Path
    camera_id: str
    trigger_prefix: str
    dispatch_ts: float
    start_ts: float
    end_ts: float
    duration_sec: float
    size_bytes: int


@dataclass(frozen=True)
class Removal:
    """A planned deletion: ``victim`` is wholly contained in ``keeper``."""

    victim: ClipSpan
    keeper: ClipSpan


@dataclass
class CleanupResult:
    """Outcome of one sweep.

    Args:
        applied: ``False`` for a dry run — nothing was deleted or rewritten.
        scanned: Clips that were successfully timed and considered.
        skipped: Files that looked like clips but could not be timed (unreadable
            duration, failed dispatch cross-check) or were held back as too
            young / in-flight.
        removals: The planned (dry run) or executed deletions.
        bytes_reclaimed: Total size of the files actually deleted.
        rows_updated: Log rows repointed at a surviving clip.
        errors: Deletions or rewrites that raised and were swallowed.
    """

    applied: bool = False
    scanned: int = 0
    skipped: int = 0
    removals: List[Removal] = field(default_factory=list)
    bytes_reclaimed: int = 0
    rows_updated: int = 0
    errors: int = 0


# ---------------------------------------------------------------------------
# Pure helpers (no I/O — the unit-testable core)
# ---------------------------------------------------------------------------

def parse_clip_name(
    name: str, container_ext: str = ".ts"
) -> Optional[Tuple[str, str, float]]:
    """Split a clip filename into its three encoded fields.

    The format is ``{trigger_id[:8]}_{camera_id}_{int(time.time())}{ext}``, as
    written by ``remux_video_buffer._handle_start``.  Splitting on the *first*
    and *last* underscore rather than pattern-matching the middle keeps camera
    IDs containing underscores intact.

    Args:
        name: The bare filename (no directory).
        container_ext: Expected extension, including the dot.

    Returns:
        ``(trigger_prefix, camera_id, dispatch_ts)``, or ``None`` if the name is
        not a clip this module wrote — in which case it is never a candidate for
        deletion.
    """
    if not name.endswith(container_ext):
        return None
    stem = name[: -len(container_ext)]
    if "_" not in stem:
        return None
    prefix, rest = stem.split("_", 1)
    if len(prefix) != 8 or not all(c in "0123456789abcdefABCDEF" for c in prefix):
        return None
    if "_" not in rest:
        return None
    camera_id, epoch = rest.rsplit("_", 1)
    if not camera_id or not epoch.isdigit():
        return None
    return prefix, camera_id, float(epoch)


def contains(outer: ClipSpan, inner: ClipSpan, tolerance_sec: float) -> bool:
    """Return whether ``outer`` wholly covers ``inner``'s span on one camera.

    Args:
        outer: The candidate containing clip.
        inner: The candidate contained clip.
        tolerance_sec: Slack applied to both bounds, so two clips of the same
            moment that differ by poll latency still compare as duplicates.

    Returns:
        ``True`` if ``inner`` holds no footage ``outer`` does not already hold.
    """
    if outer.path == inner.path:
        return False
    if outer.camera_id != inner.camera_id:
        return False
    return (
        outer.start_ts <= inner.start_ts + tolerance_sec
        and outer.end_ts >= inner.end_ts - tolerance_sec
    )


def plan_removals(
    clips: Iterable[ClipSpan], tolerance_sec: float = 0.5
) -> List[Removal]:
    """Decide which clips are redundant, and which clip replaces each.

    A single pass over ``(start asc, end desc, name)`` order against a running
    list of survivors.  See the module docstring for why this ordering makes
    keepers self-evidently safe and the result deterministic.

    Args:
        clips: Timed clips, any order, any mix of cameras.
        tolerance_sec: Passed to :func:`contains`.

    Returns:
        One :class:`Removal` per redundant clip; every ``keeper`` is a clip that
        this plan keeps.
    """
    removals: List[Removal] = []
    by_camera: Dict[str, List[ClipSpan]] = {}
    for clip in clips:
        by_camera.setdefault(clip.camera_id, []).append(clip)

    for camera_id in sorted(by_camera):
        ordered = sorted(
            by_camera[camera_id],
            key=lambda c: (c.start_ts, -c.end_ts, c.path.name),
        )
        kept: List[ClipSpan] = []
        for clip in ordered:
            keeper = next((k for k in kept if contains(k, clip, tolerance_sec)), None)
            if keeper is None:
                kept.append(clip)
            else:
                removals.append(Removal(victim=clip, keeper=keeper))
    return removals


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def probe_duration_sec(path: Path) -> Optional[float]:
    """Read a container's duration in seconds.

    PyAV is imported here rather than at module scope so this module — and its
    test suite — load on an interpreter without ``av``.

    Args:
        path: The clip file.

    Returns:
        Duration in seconds, or ``None`` if the file cannot be opened or carries
        no usable duration (in which case the caller must not delete it).
    """
    try:
        import av  # noqa: PLC0415 — deliberately lazy; see the docstring
    except ImportError:
        log.error("PyAV unavailable — cannot time clips", extra={"path": str(path)})
        return None

    try:
        with av.open(str(path)) as container:
            if container.duration:
                # container.duration is in AV_TIME_BASE units (microseconds).
                return float(container.duration) / 1_000_000.0
            for stream in container.streams:
                if stream.duration and stream.time_base:
                    return float(stream.duration * stream.time_base)
    except Exception as exc:  # noqa: BLE001 — any probe failure means "skip it"
        log.warning(
            "Could not probe clip duration",
            extra={"path": str(path), "error": str(exc)},
        )
        return None
    return None


def rewrite_reference_column(
    csv_path: Path, column: str, mapping: Dict[str, str]
) -> Dict[str, int]:
    """Repoint a filename column at surviving clips, atomically.

    Reads the whole file, substitutes, writes a sibling ``.tmp`` and
    ``os.replace()``s it — the same write-then-rename discipline the Hot Folder
    uses, so a reader never sees a half-written log.  The original header and
    column order are preserved exactly; rows without the column are passed
    through untouched.

    Args:
        csv_path: The log to rewrite.  A missing file is not an error.
        column: The column holding a clip filename.
        mapping: ``{deleted_filename: surviving_filename}``.

    Returns:
        ``{deleted_filename: rows_updated}`` — counts per victim, empty if
        nothing matched.

    Raises:
        OSError: If the file exists but cannot be read or replaced.
    """
    counts: Dict[str, int] = {}
    if not mapping or not csv_path.exists():
        return counts

    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames
        if not fieldnames or column not in fieldnames:
            return counts
        rows = list(reader)

    changed = False
    for row in rows:
        current = row.get(column)
        replacement = mapping.get(current or "")
        if replacement is None:
            continue
        row[column] = replacement
        counts[current] = counts.get(current, 0) + 1
        changed = True

    if not changed:
        return counts

    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, csv_path)
    return counts


# ---------------------------------------------------------------------------
# ClipCleaner
# ---------------------------------------------------------------------------

class ClipCleaner:
    """Periodically delete clips wholly contained in another clip.

    Owns an optional daemon thread (:meth:`start` / :meth:`stop`); a single
    sweep is :meth:`sweep`, which the CLI tool calls directly.

    Two independent safety rails keep an in-flight recording off the candidate
    list: ``protected_paths`` (the manager's live view of active and draining
    writers, authoritative within the process) and ``min_age_sec`` (mtime-based,
    which also covers clips left behind by a crashed run).

    Args:
        output_dir: Directory holding the clips and the logs.
        container_ext: Clip extension, matching ``VideoBufferConfig``.
        tolerance_sec: Containment slack — see :func:`contains`.
        min_age_sec: Ignore clips whose mtime is more recent than this.
        interval_sec: Sweep cadence for the background thread.
        protected_paths: Callable returning paths that must not be touched
            (defaults to none).  Called once per sweep.
        duration_probe: Overridable seam for timing a clip; defaults to
            :func:`probe_duration_sec`.  Tests inject a stub so the suite needs
            neither PyAV nor real video.
        log_lock: Lock shared with the video buffer's ``discrepancies_log.csv``
            appends, so a rewrite cannot interleave with a new row.
        logger: Logger override; defaults to this module's.
    """

    def __init__(
        self,
        output_dir: str | Path,
        container_ext: str = ".ts",
        tolerance_sec: float = 0.5,
        min_age_sec: float = 60.0,
        interval_sec: float = 300.0,
        protected_paths: Optional[Callable[[], Set[Path]]] = None,
        duration_probe: Optional[Callable[[Path], Optional[float]]] = None,
        log_lock: Optional[threading.Lock] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._container_ext = container_ext
        self._tolerance_sec = float(tolerance_sec)
        self._min_age_sec = float(min_age_sec)
        self._interval_sec = float(interval_sec)
        self._protected_paths = protected_paths or (lambda: set())
        self._duration_probe = duration_probe or probe_duration_sec
        self._log_lock = log_lock or threading.Lock()
        self._log = logger or log

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Launch the background sweep thread (idempotent)."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="clip-cleanup", daemon=True
        )
        self._thread.start()
        self._log.info(
            "Clip cleanup started",
            extra={
                "output_dir": str(self._output_dir),
                "interval_sec": self._interval_sec,
                "tolerance_sec": self._tolerance_sec,
                "min_age_sec": self._min_age_sec,
            },
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the sweep thread to exit and join it.

        Args:
            timeout: Maximum seconds to wait for the thread.
        """
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        """Sweep on the configured interval until stopped.

        The first sweep waits one full interval: at startup the manager has just
        opened its streams and a clip finished seconds before the restart is
        still settling.
        """
        while not self._stop_event.wait(self._interval_sec):
            try:
                self.sweep(apply=True)
            except Exception as exc:  # noqa: BLE001 — a sweep must never kill the thread
                self._log.error("Clip cleanup sweep failed", extra={"error": str(exc)})

    # -- the sweep ---------------------------------------------------------

    def scan(self, now: Optional[float] = None) -> Tuple[List[ClipSpan], int]:
        """Time every eligible clip in ``output_dir``.

        Args:
            now: Reference time for the age check; defaults to ``time.time()``.

        Returns:
            ``(timed_clips, skipped_count)``.  A file that cannot be timed is
            counted as skipped and, by construction, can never be deleted.
        """
        now = time.time() if now is None else now
        protected = {Path(p) for p in self._protected_paths()}
        clips: List[ClipSpan] = []
        skipped = 0

        for path in sorted(self._output_dir.glob(f"*{self._container_ext}")):
            parsed = parse_clip_name(path.name, self._container_ext)
            if parsed is None:
                continue  # not ours — silently none of our business
            if path in protected:
                skipped += 1
                continue
            try:
                stat = path.stat()
            except OSError:
                skipped += 1
                continue
            if stat.st_size == 0 or (now - stat.st_mtime) < self._min_age_sec:
                skipped += 1
                continue

            duration = self._duration_probe(path)
            if not duration or duration <= 0:
                skipped += 1
                continue

            prefix, camera_id, dispatch_ts = parsed
            end_ts = stat.st_mtime
            start_ts = end_ts - duration
            if not (
                start_ts - _DISPATCH_SLACK_SEC
                <= dispatch_ts
                <= end_ts + _DISPATCH_SLACK_SEC
            ):
                # mtime and the filename disagree about when this clip happened
                # — most likely a copy that did not preserve mtime.  We cannot
                # place it on the clock, so we must not judge it redundant.
                self._log.warning(
                    "Clip span failed the dispatch cross-check — skipped",
                    extra={
                        "path": str(path),
                        "dispatch_ts": dispatch_ts,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                    },
                )
                skipped += 1
                continue

            clips.append(
                ClipSpan(
                    path=path,
                    camera_id=camera_id,
                    trigger_prefix=prefix,
                    dispatch_ts=dispatch_ts,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    duration_sec=duration,
                    size_bytes=stat.st_size,
                )
            )
        return clips, skipped

    def sweep(self, apply: bool = True, now: Optional[float] = None) -> CleanupResult:
        """Scan, plan, and (unless dry-run) rewrite the logs and delete.

        Args:
            apply: ``False`` plans without touching anything.
            now: Reference time, forwarded to :meth:`scan` and used to stamp the
                audit rows.

        Returns:
            The :class:`CleanupResult` for this sweep.
        """
        now = time.time() if now is None else now
        clips, skipped = self.scan(now=now)
        removals = plan_removals(clips, self._tolerance_sec)
        result = CleanupResult(
            applied=apply,
            scanned=len(clips),
            skipped=skipped,
            removals=removals,
        )
        if not removals:
            return result
        if not apply:
            return result

        mapping = {r.victim.path.name: r.keeper.path.name for r in removals}

        # Logs first, file second — see the module docstring.  The lock is the
        # buffer's: a concurrent _log_discrepancy_to_csv append must not
        # interleave with the read-modify-replace below.
        per_victim: Dict[str, int] = {}
        with self._log_lock:
            for log_name, column in REFERENCE_COLUMNS:
                try:
                    counts = rewrite_reference_column(
                        self._output_dir / log_name, column, mapping
                    )
                except OSError as exc:
                    result.errors += 1
                    self._log.error(
                        "Failed to rewrite clip references",
                        extra={"log": log_name, "error": str(exc)},
                    )
                    # A log we could not repoint must keep its file: drop every
                    # removal rather than orphan a row.
                    return result
                for name, count in counts.items():
                    per_victim[name] = per_victim.get(name, 0) + count
            result.rows_updated = sum(per_victim.values())

            for removal in removals:
                victim = removal.victim
                try:
                    victim.path.unlink()
                except OSError as exc:
                    result.errors += 1
                    self._log.error(
                        "Failed to delete duplicate clip",
                        extra={"path": str(victim.path), "error": str(exc)},
                    )
                    continue
                result.bytes_reclaimed += victim.size_bytes
                self._log.info(
                    "Deleted clip wholly contained in another",
                    extra={
                        "deleted": victim.path.name,
                        "kept": removal.keeper.path.name,
                        "camera_id": victim.camera_id,
                        "bytes_reclaimed": victim.size_bytes,
                        "rows_updated": per_victim.get(victim.path.name, 0),
                    },
                )
                self._append_audit_row(
                    removal, now, per_victim.get(victim.path.name, 0)
                )
        return result

    # -- audit log ---------------------------------------------------------

    def _append_audit_row(
        self, removal: Removal, ts: float, rows_updated: int
    ) -> None:
        """Append one row to ``video_cleanup_log.csv``.

        Best-effort, exactly like the engine's two logs: a failed append logs an
        ERROR and is swallowed, because a full disk must not turn a successful
        cleanup into a crash.  Called with ``_log_lock`` held.

        Args:
            removal: The deletion being recorded.
            ts: Unix time of the deletion.
            rows_updated: Log rows repointed for this victim.
        """
        victim, keeper = removal.victim, removal.keeper
        row = {
            "timestamp": f"{ts:.6f}",
            "timestamp_utc": datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            ),
            "camera_id": victim.camera_id,
            "deleted_file": victim.path.name,
            "kept_file": keeper.path.name,
            "deleted_start_ts": f"{victim.start_ts:.3f}",
            "deleted_end_ts": f"{victim.end_ts:.3f}",
            "kept_start_ts": f"{keeper.start_ts:.3f}",
            "kept_end_ts": f"{keeper.end_ts:.3f}",
            "deleted_duration_sec": f"{victim.duration_sec:.3f}",
            "bytes_reclaimed": victim.size_bytes,
            "log_rows_updated": rows_updated,
            "tolerance_sec": self._tolerance_sec,
        }
        csv_path = self._output_dir / CLEANUP_LOG_NAME
        try:
            write_header = not csv_path.exists()
            with csv_path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(_CLEANUP_LOG_FIELDS))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except OSError as exc:
            self._log.error(
                "Failed to append to the clip cleanup log",
                extra={"deleted": victim.path.name, "error": str(exc)},
            )


def format_result(result: CleanupResult) -> Sequence[str]:
    """Render a :class:`CleanupResult` as human-readable lines (for the CLI).

    Args:
        result: The sweep outcome.

    Returns:
        Lines suitable for printing, one per removal plus a summary.
    """
    lines: List[str] = []
    verb = "Deleted" if result.applied else "Would delete"
    for removal in result.removals:
        victim, keeper = removal.victim, removal.keeper
        lines.append(
            f"{verb} {victim.path.name} "
            f"[{victim.start_ts:.1f}..{victim.end_ts:.1f}] "
            f"({victim.duration_sec:.1f}s, {victim.size_bytes / 1e6:.1f} MB) "
            f"-> contained in {keeper.path.name} "
            f"[{keeper.start_ts:.1f}..{keeper.end_ts:.1f}]"
        )
    lines.append(
        f"scanned={result.scanned} skipped={result.skipped} "
        f"redundant={len(result.removals)} "
        f"reclaimed={result.bytes_reclaimed / 1e6:.1f} MB "
        f"rows_updated={result.rows_updated} errors={result.errors}"
        + ("" if result.applied else "  (dry run — nothing changed)")
    )
    return lines
