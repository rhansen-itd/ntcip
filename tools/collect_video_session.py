#!/usr/bin/env python3
"""collect_video_session.py — record .ts clips plus the controller's .datZ for them.

Collects test material for pyatspm's video overlay (``atspm video-overlay`` /
``video-locate-phase-change``): a few camera clips recorded through the
production remux buffer, the controller hi-res ``.datZ`` files that cover them,
and a manifest giving each clip's first-frame time guess in the site's local
time. **No NTCIP monitor and no discrepancy engine run** — this tool drops its
own Hot Folder triggers, on a schedule. Deploy-time tool at the repo root;
nothing imports it, so ``print()`` is fine.

Schedule (default ``--align quarter``): each clip is centred on a quarter-hour
boundary, one clip per boundary, so every clip straddles two ``.datZ`` files.
That is deliberate: a clip that crosses a file boundary exercises the
per-file header timebase in ingestion, which a clip inside one file cannot.
Three 10-minute clips take ~40 minutes from the first boundary, then the tool
waits for the controller to close the last ``.datZ`` (up to 15 minutes more)
and pulls the files. ``--align now`` instead records back-to-back clips
starting right away.

Timing caveats, also written into the manifest:

* Triggers carry pre-roll 0, so a clip opens on the last keyframe at or
  before the trigger: its first frame is up to one GOP **earlier** than the
  ``start_guess`` recorded. Correct it with ``video-locate-phase-change``.
* ``start_guess`` is this machine's clock. ``.datZ`` timestamps are the
  controller's. The controller clock is read over SNMP (``globalTime``, 1 s
  resolution, GET only) before and after so a gross skew is visible.
* Local times are in the intersection's ``timezone``, not this machine's
  (the monitoring machine has run PDT against an MDT site before).

The ``.datZ`` pull mirrors pyatspm's ``atspm retrieve`` (SSH to the controller,
``ls`` the log folder) but streams each file with ``cat`` over the SSH channel,
so it needs only ``paramiko``, not the ``scp`` package. The password comes from
``$CONTROLLER_SSH_PASSWORD`` or a prompt at startup — the prompt is up front so
the unattended part never blocks on input. The SSH login is tested before any
recording starts.

Usage:
    # defaults: 3 x 10-minute clips, centred on the next quarter-hours
    python tools/collect_video_session.py -i 201

    # one 20-minute clip starting now, no .datZ pull
    python tools/collect_video_session.py -i 201 --align now --clips 1 \
        --seconds 1200 --no-datz

Output (``--out``, default ``collections/<id>_<YYYYmmdd_HHMM>/``)::

    clips/<trigger8>_<camera>_<epoch>.ts
    datz/ECON_<ip>_<YYYY_MM_DD_HHMM>.datZ
    manifest.json
"""

import argparse
import getpass
import json
import math
import os
import shlex
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

_ROOT = Path(__file__).resolve().parents[1]
# Deploy-time tool: bootstrap the repo root (ntcip_monitor), this directory
# (sync_ui_config) and video_engine/tools (record_clip, which in turn puts
# video_engine/ on sys.path for remux_video_buffer).
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(_ROOT / "video_engine" / "tools"))

from record_clip import _drop_start_trigger  # noqa: E402
from remux_video_buffer import VideoBufferConfig, VideoBufferManager  # noqa: E402
from sync_ui_config import (  # noqa: E402
    load_intersections,
    mask_url,
    select_camera,
    select_intersection,
)

#: Controller hi-res log bin length; files are named for the bin's start.
_DATZ_BIN = timedelta(minutes=15)
#: Wait this long past a bin's end before expecting its file on the controller.
_DATZ_CLOSE_MARGIN_SEC = 120.0
#: Seconds to open the stream before the first clip, beyond the pre-roll ring.
_STREAM_WARMUP_SEC = 5.0


# -- controller clock ----------------------------------------------------------

def read_controller_clock(ip: str, port: int, community: str) -> Dict[str, Any]:
    """Read the controller's ``globalTime`` once and compare it to this machine.

    Read-only (SNMP GET). Failures are reported in the result, never raised —
    a missing clock reading must not stop a collection.

    Args:
        ip: Controller IP.
        port: SNMP port (Cobalt/EOS: 501).
        community: SNMP community.

    Returns:
        dict: ``machine_epoch``, ``controller_global_time`` and
        ``offset_sec`` (controller minus machine; ±1 s from the OID's
        resolution), plus ``offset_residual_sec`` with whole hours removed in
        case the controller keeps local time in an OID that should be UTC.
        On failure, ``error`` instead of the readings.
    """
    try:
        from ntcip_monitor.core.oid_definitions import GLOBAL_TIME
        from ntcip_monitor.core.snmp_client import EconoliteSNMPClient

        client = EconoliteSNMPClient(ip, port=port, community=community)
        before = time.time()
        value = int(client.get(GLOBAL_TIME))
        machine = (before + time.time()) / 2.0
    except Exception as exc:  # noqa: BLE001 — best effort by design
        return {"error": f"{type(exc).__name__}: {exc}"}

    offset = value - machine
    residual = offset - 3600.0 * round(offset / 3600.0)
    return {
        "machine_epoch": round(machine, 3),
        "controller_global_time": value,
        "offset_sec": round(offset, 1),
        "offset_residual_sec": round(residual, 1),
    }


def _print_clock(label: str, reading: Dict[str, Any]) -> None:
    if "error" in reading:
        print(f"[clock] {label}: unavailable ({reading['error']})")
        return
    print(f"[clock] {label}: controller - machine = {reading['offset_sec']:+.1f} s"
          f" (whole hours removed: {reading['offset_residual_sec']:+.1f} s; "
          f"OID resolution 1 s)")


# -- controller .datZ pull -----------------------------------------------------

class DatzPuller:
    """Fetch named ``.datZ`` files off a controller over SSH.

    Args:
        host: Controller IP.
        port: SSH port.
        user: SSH user.
        password: SSH password.
        remote_folder: Directory holding the ``.datZ`` files.
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 remote_folder: str) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.remote_folder = remote_folder.rstrip("/")

    def _connect(self):
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.host, port=self.port, username=self.user,
                    password=self.password, timeout=15)
        return ssh

    @staticmethod
    def _run(ssh, command: str) -> bytes:
        _stdin, stdout, stderr = ssh.exec_command(command)
        data = stdout.read()
        if stdout.channel.recv_exit_status() != 0:
            err = stderr.read().decode(errors="replace").strip()
            raise RuntimeError(f"remote {command!r} failed: {err or 'unknown error'}")
        return data

    def list_files(self) -> List[str]:
        """List the remote folder's files (same command ``atspm retrieve`` uses).

        Returns:
            list: Bare filenames.
        """
        ssh = self._connect()
        try:
            folder = shlex.quote(self.remote_folder)
            out = self._run(ssh, f"ls -p {folder} | grep -v /")
        finally:
            ssh.close()
        return [line.strip() for line in out.decode(errors="replace").splitlines()
                if line.strip()]

    def fetch(self, names: List[str], dest: Path) -> List[Path]:
        """Copy each named file into ``dest``, checking its size against the remote.

        Args:
            names: Bare filenames in the remote folder.
            dest: Local directory (created if missing).

        Returns:
            list: Local paths written.

        Raises:
            RuntimeError: If a remote command fails or a size doesn't match.
        """
        dest.mkdir(parents=True, exist_ok=True)
        written = []
        ssh = self._connect()
        try:
            for name in names:
                remote = shlex.quote(f"{self.remote_folder}/{name}")
                expected = int(self._run(ssh, f"wc -c < {remote}").split()[0])
                data = self._run(ssh, f"cat {remote}")
                if len(data) != expected:
                    raise RuntimeError(
                        f"{name}: got {len(data)} bytes, remote has {expected}")
                path = dest / name
                path.write_bytes(data)
                written.append(path)
                print(f"[datz] {name}  ({len(data):,} bytes)")
        finally:
            ssh.close()
        return written


def datz_bin_starts(first: datetime, last: datetime) -> List[datetime]:
    """Bin starts covering ``[first, last]`` plus one bin of lead-in.

    The extra bin before the first clip gives the overlay the signal state at
    frame 0 (``video-overlay --lookback`` reaches back 10 minutes by default).

    Args:
        first: Earliest clip start (tz-aware, site local).
        last: Latest clip end (tz-aware, site local).

    Returns:
        list: Tz-aware bin start times, ascending.
    """
    start = floor_bin(first) - _DATZ_BIN
    end = floor_bin(last)
    out = []
    t = start
    while t <= end:
        out.append(t)
        t += _DATZ_BIN
    return out


def floor_bin(dt: datetime) -> datetime:
    """Floor a tz-aware datetime to its 15-minute ``.datZ`` bin."""
    return dt.replace(minute=dt.minute - dt.minute % 15, second=0, microsecond=0)


def match_datz_names(remote: List[str], bins: List[datetime]) -> Dict[str, Optional[str]]:
    """Map each bin's ``YYYY_MM_DD_HHMM`` stamp to its remote filename, if present.

    Args:
        remote: Remote filenames.
        bins: Bin start times (site local).

    Returns:
        dict: Stamp -> filename, or None when the controller has no such file.
    """
    out: Dict[str, Optional[str]] = {}
    for b in bins:
        stamp = b.strftime("%Y_%m_%d_%H%M")
        hits = [n for n in remote if n.lower().endswith(f"_{stamp}.datz")]
        out[stamp] = hits[0] if hits else None
    return out


# -- schedule ------------------------------------------------------------------

def plan_clips(now: float, clips: int, seconds: float, align: str,
               lead: float, tz) -> List[float]:
    """Work out each clip's start (epoch seconds).

    Args:
        now: Current epoch.
        clips: Number of clips.
        seconds: Clip length.
        align: ``quarter`` (centre on successive quarter-hours) or ``now``.
        lead: Minimum seconds before the first start (stream warmup).
        tz: Site timezone (quarter-hours are taken in it; UTC offsets are
            whole or half hours, so this only matters for clarity).

    Returns:
        list: Clip start epochs, ascending.
    """
    if align == "now":
        first = now + lead
        return [first + i * (seconds + 5.0) for i in range(clips)]

    half = seconds / 2.0
    earliest_centre = datetime.fromtimestamp(now + lead + half, tz)
    centre = floor_bin(earliest_centre)
    if centre < earliest_centre:
        centre += _DATZ_BIN
    return [(centre + i * _DATZ_BIN).timestamp() - half for i in range(clips)]


def _sleep_until(target_ts: float, label: str, tz) -> None:
    """Sleep until ``target_ts``, announcing it in the site's timezone.

    ``record_clip._sleep_until`` prints this machine's local time, which
    disagrees with the plan when the machine and the site are in different
    zones.
    """
    remaining = target_ts - time.time()
    if remaining <= 0:
        return
    when = datetime.fromtimestamp(target_ts, tz).strftime("%H:%M:%S %Z")
    print(f"[wait] {label} at {when} (in {remaining:.0f}s)...")
    time.sleep(remaining)


def _local_iso(epoch: float, tz) -> str:
    return datetime.fromtimestamp(epoch, tz).isoformat(timespec="milliseconds")


def _probe_duration(path: Path) -> Optional[float]:
    try:
        import av

        with av.open(str(path)) as c:
            return round(c.duration / 1e6, 3) if c.duration else None
    except Exception:  # noqa: BLE001 — informational only
        return None


# -- main ----------------------------------------------------------------------

def main() -> int:
    """Record the clips, pull the .datZ, write the manifest.

    Returns:
        int: Process exit status.
    """
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-i", "--intersection", required=True, metavar="ID",
                    help="intersection in the video engine's config (e.g. 201)")
    ap.add_argument("--intersections", type=Path,
                    default=_ROOT / "video_engine" / "intersections",
                    help="intersection config file or directory "
                         "(default: video_engine/intersections)")
    ap.add_argument("--camera", metavar="ID",
                    help="camera to record (default: the only one defined)")
    ap.add_argument("--clips", type=int, default=3,
                    help="number of clips (default 3)")
    ap.add_argument("-t", "--seconds", type=float, default=600.0,
                    help="length of each clip in seconds (default 600)")
    ap.add_argument("--align", choices=["quarter", "now"], default="quarter",
                    help="quarter: centre each clip on a quarter-hour so it "
                         "straddles two .datZ files (default); now: back-to-back "
                         "clips starting right away")
    ap.add_argument("--out", type=Path,
                    help="output directory (default collections/<id>_<YYYYmmdd_HHMM>)")
    ap.add_argument("--no-datz", action="store_true",
                    help="skip the controller .datZ pull")
    ap.add_argument("--ssh-user", default="econolite",
                    help="controller SSH user (default econolite)")
    ap.add_argument("--ssh-port", type=int, default=22,
                    help="controller SSH port (default 22)")
    ap.add_argument("--remote-folder", default="/opt/econolite/set1",
                    help="controller .datZ folder (default /opt/econolite/set1)")
    args = ap.parse_args()

    if args.clips < 1:
        ap.error("--clips must be at least 1")
    if args.seconds <= 0:
        ap.error("--seconds must be positive")
    if args.align == "quarter" and args.seconds >= _DATZ_BIN.total_seconds():
        ap.error("--align quarter needs --seconds under 900 (clips would overlap); "
                 "use --align now for longer clips")

    data = load_intersections(args.intersections)
    iid, section = select_intersection(data, args.intersection)
    camera, url = select_camera(section, args.camera)
    if not url:
        sys.exit(f"error: intersection {iid} has no camera URL")
    tz_name = section.get("timezone") or "UTC"
    tz = pytz.timezone(tz_name)
    ctrl_ip = section.get("controller_ip")

    print(f"[setup] intersection {iid}, camera {camera}: {mask_url(url)}")
    print(f"[setup] site timezone {tz_name}; controller {ctrl_ip or '(none)'}")

    # -- credentials + SSH check up front, before anything unattended starts --
    puller: Optional[DatzPuller] = None
    if not args.no_datz:
        if not ctrl_ip:
            sys.exit("error: no controller_ip in config; pass --no-datz")
        try:
            import paramiko  # noqa: F401
        except ImportError:
            sys.exit("error: paramiko is not installed (pip install paramiko), "
                     "or pass --no-datz and pull the files later")
        password = os.environ.get("CONTROLLER_SSH_PASSWORD") or getpass.getpass(
            f"SSH password for {args.ssh_user}@{ctrl_ip}: ")
        puller = DatzPuller(ctrl_ip, args.ssh_port, args.ssh_user, password,
                            args.remote_folder)
        try:
            n = len(puller.list_files())
        except Exception as exc:  # noqa: BLE001
            sys.exit(f"error: SSH check failed ({type(exc).__name__}: {exc}); "
                     "fix it or pass --no-datz")
        print(f"[setup] SSH ok: {n} files in {args.remote_folder}")

    stamp = datetime.now(tz).strftime("%Y%m%d_%H%M")
    out_dir = args.out or (_ROOT / "collections" / f"{iid}_{stamp}")
    clip_dir = out_dir / "clips"
    trigger_dir = out_dir / "trigger_queue"
    clip_dir.mkdir(parents=True, exist_ok=True)

    cfg = VideoBufferConfig(
        streams={camera: url},
        trigger_dir=str(trigger_dir),
        output_dir=str(clip_dir),
        pre_roll_sec=0.0,
        backend="remux",
    )
    lead = cfg.keyframe_margin_sec + _STREAM_WARMUP_SEC
    starts = plan_clips(time.time(), args.clips, args.seconds, args.align,
                        lead + 2.0, tz)

    print(f"[plan] {args.clips} x {args.seconds:.0f}s clip(s), local {tz_name}:")
    for k, s in enumerate(starts, 1):
        print(f"[plan]   {k}: {_local_iso(s, tz)[:19]} -> "
              f"{_local_iso(s + args.seconds, tz)[11:19]}")

    manifest: Dict[str, Any] = {
        "intersection_id": iid,
        "camera": camera,
        "timezone": tz_name,
        "controller_ip": ctrl_ip,
        "align": args.align,
        "clip_seconds": args.seconds,
        "notes": [
            "start_guess is the trigger time on the recording machine's clock, "
            "in the site timezone. The clip opens on the last keyframe at or "
            "before it, so the true first frame is up to one GOP earlier.",
            "Refine with: atspm video-locate-phase-change ... --start <start_guess>",
        ],
        "controller_clock": {},
        "clips": [],
        "datz": {},
    }
    if ctrl_ip:
        reading = read_controller_clock(ctrl_ip, int(section.get("snmp_port", 501)),
                                        section.get("snmp_community", "administrator"))
        manifest["controller_clock"]["before"] = reading
        _print_clock("before", reading)

    manifest_path = out_dir / "manifest.json"

    def write_manifest() -> None:
        manifest_path.write_text(json.dumps(manifest, indent=2))

    write_manifest()

    # -- record --------------------------------------------------------------
    manager = VideoBufferManager(cfg)
    runner: Optional[threading.Thread] = None
    interrupted = False
    try:
        _sleep_until(starts[0] - lead, "opening stream", tz)
        runner = threading.Thread(target=manager.start, name="video-manager",
                                  daemon=True)
        runner.start()
        for k, s in enumerate(starts, 1):
            _sleep_until(s, f"clip {k}/{len(starts)} starts", tz)
            trigger_epoch = time.time()
            trigger_id = _drop_start_trigger(str(trigger_dir), camera, 0.0,
                                             args.seconds)
            manifest["clips"].append({
                "trigger_id": trigger_id,
                "trigger_epoch": round(trigger_epoch, 3),
                "start_guess": _local_iso(trigger_epoch, tz),
                "seconds_requested": args.seconds,
            })
            write_manifest()
            print(f"[record] clip {k} recording {args.seconds:.0f}s "
                  f"(trigger {trigger_id[:8]}, start_guess "
                  f"{manifest['clips'][-1]['start_guess']})")
        # Last clip: pickup latency + its duration + finalize margin.
        _sleep_until(time.time() + args.seconds + cfg.poll_interval_sec + 3.0,
                     "last clip finishes", tz)
    except KeyboardInterrupt:
        interrupted = True
        print("\n[record] interrupted — finalizing clips recorded so far")
    finally:
        manager.stop()
        if runner is not None:
            runner.join(timeout=15.0)

    for clip in manifest["clips"]:
        hits = sorted(clip_dir.glob(f"{clip['trigger_id'][:8]}_*{cfg.container_ext}"))
        if hits:
            clip["file"] = str(hits[-1].relative_to(out_dir))
            clip["bytes"] = hits[-1].stat().st_size
            clip["duration_sec"] = _probe_duration(hits[-1])
            print(f"[record] {clip['file']}  ({clip['bytes'] / 1e6:.1f} MB, "
                  f"{clip['duration_sec']} s)")
        else:
            clip["file"] = None
            print(f"[record] trigger {clip['trigger_id'][:8]}: NO CLIP — check "
                  "camera reachability in the log above", file=sys.stderr)

    if ctrl_ip:
        reading = read_controller_clock(ctrl_ip, int(section.get("snmp_port", 501)),
                                        section.get("snmp_community", "administrator"))
        manifest["controller_clock"]["after"] = reading
        _print_clock("after", reading)
    write_manifest()

    recorded = [c for c in manifest["clips"] if c.get("file")]

    # -- pull .datZ ------------------------------------------------------------
    if puller is not None and recorded and not interrupted:
        first = datetime.fromtimestamp(min(c["trigger_epoch"] for c in recorded), tz)
        last = datetime.fromtimestamp(
            max(c["trigger_epoch"] for c in recorded) + args.seconds, tz)
        bins = datz_bin_starts(first, last)
        ready = (bins[-1] + _DATZ_BIN).timestamp() + _DATZ_CLOSE_MARGIN_SEC
        _sleep_until(ready, "controller closes the last .datZ", tz)
        try:
            matched = match_datz_names(puller.list_files(), bins)
            names = [n for n in matched.values() if n]
            puller.fetch(names, out_dir / "datz")
            manifest["datz"] = matched
            missing = [s for s, n in matched.items() if n is None]
            if missing:
                print(f"[datz] not on controller: {', '.join(missing)}",
                      file=sys.stderr)
        except Exception as exc:  # noqa: BLE001 — clips are already safe on disk
            manifest["datz"] = {"error": f"{type(exc).__name__}: {exc}"}
            print(f"[datz] pull failed: {exc}. The controller keeps ~30 h; "
                  "re-run the pull with atspm retrieve or by hand.", file=sys.stderr)
        write_manifest()

    print(f"\n[done] {len(recorded)}/{len(manifest['clips'])} clip(s) -> {out_dir}")
    print(f"[done] manifest: {manifest_path}")
    if recorded:
        c = recorded[0]
        print("\nNext, in pyatspm (copy datz/*.datZ into the intersection's "
              "raw_data/ first):")
        print(f"  atspm process --targetid {iid}")
        print(f"  atspm video-locate-phase-change --targetid {iid} --camera {camera} "
              f"--video <abs path to {Path(c['file']).name}> --phase <N> "
              f"--start {c['start_guess'][:23]}")
    return 0 if recorded and not interrupted else 1


if __name__ == "__main__":
    sys.exit(main())
