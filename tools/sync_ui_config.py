#!/usr/bin/env python3
"""sync_ui_config.py — copy shared deployment values into the monitor's config.

The two packages keep their own config file on purpose: ``ntcip_monitor`` reads
``config.json`` and ``video_engine`` reads ``intersections/``, and neither
imports the other (CLAUDE.md, "Module boundaries"). A handful of values are
nonetheless the *same fact* about one intersection — the controller's SNMP
endpoint and the camera's URL — and hand-copying them is how they drift.

This script is that de-duplication mechanism: it reads the video engine's
intersection config as the authoring source and writes those values into the
monitor's ``config.json``. It runs at **deploy time**, never at runtime, which
is why it lives at the repo root rather than inside either package (the same
role ``video_engine/system_runner.py`` plays at runtime). Standalone tool —
``print()`` is fine here; nothing imports it.

Synced (intersection config -> config.json):

    controller_ip       -> controller.ip
    snmp_port           -> controller.port
    snmp_community      -> controller.community
    snmp_chunk_size     -> controller.chunk_size   (only when present)
    cameras.<id>.url    -> overlay.camera_url

Deliberately **not** synced: ``poll_interval_sec`` (the monitor tunes four
monitors separately under ``monitors.*``, so one engine-side number does not
map onto them), ``timezone`` (used by the engine's CSV log, which the monitor
has no equivalent of), and anything under ``web_ui`` (host, port, and the
control token are properties of the *host you run the UI on*, not of the
intersection).

**Dry run by default** — nothing is written without ``--apply``. Credentials
embedded in a camera URL are masked in the printed output.

Usage:
    python tools/sync_ui_config.py -i 201                # show 201's drift
    python tools/sync_ui_config.py -i 201 --apply
    python tools/sync_ui_config.py -i 701 --apply           # the other site
    python tools/sync_ui_config.py -i 201 --camera fisheye --show-secrets
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

#: intersection-config key -> dotted path in config.json.
FIELD_MAP = (
    ("controller_ip", "controller.ip"),
    ("snmp_port", "controller.port"),
    ("snmp_community", "controller.community"),
    ("snmp_chunk_size", "controller.chunk_size"),
)

#: Written from the selected camera rather than a top-level key.
CAMERA_URL_TARGET = "overlay.camera_url"


def mask_url(url: str) -> str:
    """Replace the password in a URL's userinfo with ``***``.

    Args:
        url: Any URL, with or without credentials.

    Returns:
        str: The URL with its password hidden, unchanged if it carries none.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if not parts.hostname or "@" not in parts.netloc:
        return url

    userinfo, _, hostport = parts.netloc.rpartition("@")
    user = userinfo.split(":", 1)[0]
    return urlunsplit(parts._replace(netloc=f"{user}:***@{hostport}"))


def load_json(path: Path) -> Dict[str, Any]:
    """Read a JSON object from disk.

    Args:
        path: File to read.

    Returns:
        dict: The parsed object.

    Raises:
        SystemExit: If the file is missing or not valid JSON — this is a CLI,
            and a stack trace helps nobody at deploy time.
    """
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        sys.exit(f"error: {path} not found (run from the repo root?)")
    except json.JSONDecodeError as exc:
        sys.exit(f"error: {path} is not valid JSON: {exc}")


def load_intersections(path: Path) -> Dict[str, Any]:
    """Read the video engine's intersection config, file or directory.

    Mirrors ``JsonFileConfigProvider``'s two accepted shapes (ROADMAP 2) so a
    deploy-time tool sees exactly what the runtime will: a directory of
    per-intersection ``*.json`` files, or a single multi-intersection file.
    Deliberately re-implemented rather than importing the provider — these
    scripts belong to neither package, and a half-authored config that fails
    the provider's validation is precisely when you still want the tool that
    helps you finish it to run.

    Args:
        path: Config directory or file.

    Returns:
        dict: All intersection blocks, keyed by intersection ID.

    Raises:
        SystemExit: If the path is missing, a directory holds no ``*.json``
            files, a file is not valid JSON, or an intersection is defined
            twice — a silent last-file-wins would be the worst outcome here.
    """
    if not path.is_dir():
        return load_json(path)

    files = sorted(path.glob("*.json"))
    if not files:
        sys.exit(f"error: {path} contains no *.json intersection configs")

    merged: Dict[str, Any] = {}
    sources: Dict[str, Path] = {}
    for one in files:
        for iid, block in load_json(one).items():
            if iid in sources:
                sys.exit(f"error: intersection {iid} is defined in both "
                         f"{sources[iid]} and {one}")
            merged[iid] = block
            sources[iid] = one
    return merged


def get_dotted(config: Dict[str, Any], path: str) -> Tuple[bool, Any]:
    """Read a dotted key path out of a nested dict.

    Args:
        config: The config object.
        path: Dotted path, e.g. ``controller.ip``.

    Returns:
        tuple: ``(present, value)``; ``value`` is ``None`` when absent.
    """
    node: Any = config
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return (False, None)
        node = node[part]
    return (True, node)


def set_dotted(config: Dict[str, Any], path: str, value: Any) -> None:
    """Write a dotted key path into a nested dict, creating sections as needed.

    Args:
        config: The config object, mutated in place.
        path: Dotted path, e.g. ``overlay.camera_url``.
        value: Value to store.
    """
    parts = path.split(".")
    node = config
    for part in parts[:-1]:
        existing = node.get(part)
        if not isinstance(existing, dict):
            existing = {}
            node[part] = existing
        node = existing
    node[parts[-1]] = value


def select_intersection(
    data: Dict[str, Any], wanted: Optional[str]
) -> Tuple[str, Dict[str, Any]]:
    """Pick the intersection to sync from.

    Args:
        data: The parsed intersection config (keyed by intersection ID),
            merged across every file when a directory was given.
        wanted: The requested ID, or None to take the only one present.

    Returns:
        tuple: ``(intersection_id, section)``.

    Raises:
        SystemExit: If the ID is absent, or omitted with several to choose from.
    """
    ids = [key for key, value in data.items() if isinstance(value, dict)]
    if wanted is None:
        if len(ids) != 1:
            sys.exit("error: --intersection is required; available: "
                     + ", ".join(ids))
        return (ids[0], data[ids[0]])
    if wanted not in data:
        sys.exit(f"error: intersection {wanted!r} not found; "
                 "available: " + ", ".join(ids))
    return (wanted, data[wanted])


def select_camera(
    section: Dict[str, Any], wanted: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Pick the camera whose URL feeds the overlay.

    Args:
        section: One intersection's config.
        wanted: The requested camera ID, or None to take the only one present.

    Returns:
        tuple: ``(camera_id, url)``, both None when the intersection has no
        usable camera — a monitor without an overlay is a legitimate config, so
        this is a skip, not an error.

    Raises:
        SystemExit: If a named camera is absent, or the ID is omitted with
            several cameras defined.
    """
    cameras = section.get("cameras")
    if not isinstance(cameras, dict) or not cameras:
        return (None, None)

    if wanted is None:
        if len(cameras) != 1:
            sys.exit("error: --camera is required; this intersection has: "
                     + ", ".join(cameras))
        wanted = next(iter(cameras))
    elif wanted not in cameras:
        sys.exit(f"error: camera {wanted!r} not defined here; available: "
                 + ", ".join(cameras))

    url = (cameras[wanted] or {}).get("url")
    if not url:
        return (wanted, None)
    return (wanted, url)


def plan_changes(
    section: Dict[str, Any], config: Dict[str, Any], camera_url: Optional[str]
) -> List[Tuple[str, Any, Any]]:
    """Work out which config.json values differ from the intersection config.

    Args:
        section: One intersection's config (the authoring source).
        config: The monitor's parsed ``config.json``.
        camera_url: URL of the selected camera, or None to leave the overlay
            key alone.

    Returns:
        list: ``(dotted_path, current_value, new_value)`` for each key that
        would change. Absent keys are reported with a current value of None.
    """
    changes = []
    pairs = [(section.get(source), target)
             for source, target in FIELD_MAP
             if section.get(source) is not None]
    if camera_url:
        pairs.append((camera_url, CAMERA_URL_TARGET))

    for value, target in pairs:
        present, current = get_dotted(config, target)
        if not present or current != value:
            changes.append((target, current if present else None, value))
    return changes


def render(path: str, value: Any, show_secrets: bool) -> str:
    """Format a config value for the console, masking credentials.

    Args:
        path: The dotted key the value belongs to.
        value: The value itself.
        show_secrets: Print camera URLs verbatim instead of masked.

    Returns:
        str: A display string.
    """
    if value is None:
        return "(unset)"
    if value == "":
        return "(empty)"
    if isinstance(value, str) and path == CAMERA_URL_TARGET and not show_secrets:
        return mask_url(value)
    return str(value)


def write_config(path: Path, config: Dict[str, Any]) -> None:
    """Write config.json atomically.

    The file is re-serialised with two-space indentation, so hand-authored
    formatting and comments-by-key-order are normalised — the same trade the
    Hot Folder writer makes, and the reason ``--apply`` is opt-in.

    Args:
        path: Destination path.
        config: The object to write.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")
    os.replace(tmp, path)


def main() -> int:
    """Run the sync.

    Returns:
        int: Process exit status.
    """
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-i", "--intersection", metavar="ID",
                    help="intersection ID to sync from (default: the only one "
                         "in the file)")
    ap.add_argument("--intersections", type=Path,
                    default=Path("video_engine/intersections"),
                    help="video engine intersection config: a directory of "
                         "per-intersection *.json files, or one multi-"
                         "intersection file (default: video_engine/intersections)")
    ap.add_argument("--config", type=Path, default=Path("config.json"),
                    help="monitor config to update (default: config.json)")
    ap.add_argument("--camera", metavar="ID",
                    help="camera whose URL feeds overlay.camera_url "
                         "(default: the only one defined)")
    ap.add_argument("--apply", action="store_true",
                    help="write the changes; without this nothing is modified")
    ap.add_argument("--show-secrets", action="store_true",
                    help="print camera URLs with their credentials intact")
    args = ap.parse_args()

    data = load_intersections(args.intersections)
    config = load_json(args.config)

    intersection_id, section = select_intersection(data, args.intersection)
    camera_id, camera_url = select_camera(section, args.camera)

    print(f"Source: {args.intersections} [{intersection_id}]"
          f"{f' camera {camera_id}' if camera_id else ''}")
    print(f"Target: {args.config}")
    if camera_id and not camera_url:
        print(f"  note: camera {camera_id!r} has no url; "
              "overlay.camera_url left alone")
    elif not camera_id:
        print("  note: no cameras defined; overlay.camera_url left alone")

    changes = plan_changes(section, config, camera_url)
    if not changes:
        print("\nAlready in sync — nothing to do.")
        return 0

    width = max(len(path) for path, _, _ in changes)
    print()
    for path, current, value in changes:
        print(f"  {path:<{width}}  {render(path, current, args.show_secrets)}"
              f"  ->  {render(path, value, args.show_secrets)}")

    if not args.apply:
        print(f"\nDry run — {len(changes)} change(s) not written. "
              "Re-run with --apply.")
        return 0

    for path, _current, value in changes:
        set_dotted(config, path, value)
    write_config(args.config, config)
    print(f"\nWrote {len(changes)} change(s) to {args.config}.")
    if any(path == CAMERA_URL_TARGET for path, _, _ in changes):
        print('Set overlay.background to "live" to use the camera feed as the '
              "overlay background.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
