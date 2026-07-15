"""simulate_playback.py — Time-travel simulator for the discrepancy engine.

Reads raw detector events (Codes 81/82) from a pyatspm SQLite database and
plays them back into DiscrepancyMonitor using a simulated clock.
"""

import os
import sys

# This tool lives in video_engine/tools/; put video_engine/ on sys.path so the
# sibling-module imports below (config_manager, discrepancy_engine) resolve
# regardless of the working directory. (The pyatspm path below is intentionally
# left cwd-relative — same behavior as before the move.)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

current_dir = os.getcwd()
atspm_project_path = os.path.abspath(os.path.join(current_dir, '..', 'pyatspm', 'src'))

if atspm_project_path not in sys.path:
    sys.path.insert(0, atspm_project_path)
    print(f"🔗 Added to sys.path: {atspm_project_path}")

# Now this import (and the internal imports in system_runner) will work
from atspm.data.manager import DatabaseManager


import argparse
import datetime
import json
from pathlib import Path

import pytz

from config_manager import JsonFileConfigProvider
import discrepancy_engine
from discrepancy_engine import DiscrepancyMonitor




def run_simulation(
    db_path: str,
    config_path: str,
    intersection_id: str,
    start_time_str: str,
    end_time_str: str,
    timezone_str: str = "US/Mountain",
    trigger_dir: str = "./trigger_queue_simulated",
    tick_interval: float = 0.1,
    clear_dir: bool = False,
):
    print(f"--- Starting ATSPM Simulation for {intersection_id} ---")

    # 1. Parse local times → UTC epoch (ATSPM stores timestamps as UTC floats)
    tz = pytz.timezone(timezone_str)
    start_dt = tz.localize(datetime.datetime.fromisoformat(start_time_str))
    end_dt = tz.localize(datetime.datetime.fromisoformat(end_time_str))
    start_epoch = start_dt.timestamp()
    end_epoch = end_dt.timestamp()

    print(f"Time window: {start_dt} → {end_dt}  ({(end_epoch - start_epoch)/3600:.1f} hours)")

    # 2. Load events
    print("Fetching events from pyatspm DB...")
    with DatabaseManager(Path(db_path)) as db:
        events_df = db.query_events(
            start_time=start_epoch,
            end_time=end_epoch,
            event_codes=[81, 82],          # 82 = ON, 81 = OFF
        )

    if events_df.empty:
        print("No detector events in the window.")
        return

    events = events_df.to_dict("records")
    events = sorted(events, key=lambda e: e.get("timestamp", 0))  # defensive sort

    # 3. Build monitor (this also builds _pairs and detector states)
    provider = JsonFileConfigProvider(config_path)
    trig_path = Path(trigger_dir)
    if clear_dir:
        for f in trig_path.glob("trigger_*.json"):
            f.unlink(missing_ok=True)
    trig_path.mkdir(parents=True, exist_ok=True)

    monitor = DiscrepancyMonitor(
        intersection_id=intersection_id,
        config_provider=provider,
        trigger_dir=str(trig_path),
        cooldown_sec=60.0,
        evaluator_interval_sec=0.1,   # not used in manual mode but kept for consistency
    )

    # Filter to only detectors this intersection actually cares about
    configured_dets = set(monitor._detector_states.keys())
    events = [
        e for e in events
        if str(int(e.get("parameter", -1))) in configured_dets
    ]
    print(f"Loaded & filtered to {len(events)} relevant detector events.")

    # 4. Monkey-patch time for the entire discrepancy module
    original_time = discrepancy_engine.time.time
    simulated_clock = start_epoch
    discrepancy_engine.time.time = lambda: simulated_clock

    event_idx = 0
    total_events = len(events)
    last_progress = simulated_clock

    print("Starting playback...")
    while simulated_clock <= end_epoch:
        # Fire all events that have occurred by now
        while event_idx < total_events and events[event_idx]["timestamp"] <= simulated_clock:
            ev = events[event_idx]
            det_id = str(int(ev["parameter"]))
            if ev["event_code"] == 82:
                monitor.on_detector_on(det_id)
            elif ev["event_code"] == 81:
                monitor.on_detector_off(det_id)
            event_idx += 1

        # Manually drive evaluator (exactly what the background thread does)
        for pair_key, (det_a, det_b) in list(monitor._pairs.items()):
            monitor._evaluate_pair(pair_key, det_a, det_b)

        # Progress every 5 simulated minutes
        if simulated_clock - last_progress >= 300:
            sim_dt = datetime.datetime.fromtimestamp(simulated_clock, tz=tz)
            print(f"  Simulated {sim_dt.isoformat()} | Events: {event_idx}/{total_events}")
            last_progress = simulated_clock

        simulated_clock += tick_interval

    # Restore real time
    discrepancy_engine.time.time = original_time

    # 5. Summarize what was generated
    triggers = sorted(trig_path.glob("trigger_*.json"))
    print(f"\n--- Simulation Complete! Generated {len(triggers)} trigger files ---")

    summary = {"start": 0, "stop": 0, "rules": {}}
    for p in triggers[:10]:  # show first 10 as samples
        with p.open(encoding="utf-8") as f:
            data = json.load(f)
        action = data.get("action")
        rule = data.get("metadata", {}).get("rule", "unknown")
        summary[action] = summary.get(action, 0) + 1
        summary["rules"][rule] = summary["rules"].get(rule, 0) + 1
        print(f"  {p.name} → {action} | {rule}")

    if len(triggers) > 10:
        print(f"  ... and {len(triggers)-10} more")

    print("Summary:", summary)
    print(f"Triggers are in: {trig_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ATSPM discrepancy engine simulator")
    parser.add_argument("--db-path", required=True, help="Path to pyatspm SQLite DB")
    parser.add_argument("--config", default="intersections.json", help="intersections.json")
    parser.add_argument("--intersection", default="1234_main", help="intersection_id")
    parser.add_argument("--start", required=True, help="Local start time YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--end", required=True, help="Local end time YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--timezone", default="US/Mountain")
    parser.add_argument("--trigger-dir", default="./trigger_queue_simulated")
    parser.add_argument("--tick-interval", type=float, default=0.1, help="Simulation resolution (seconds)")
    parser.add_argument("--clear", action="store_true", help="Clear trigger dir before run")
    args = parser.parse_args()

    run_simulation(
        db_path=args.db_path,
        config_path=args.config,
        intersection_id=args.intersection,
        start_time_str=args.start,
        end_time_str=args.end,
        timezone_str=args.timezone,
        trigger_dir=args.trigger_dir,
        tick_interval=args.tick_interval,
        clear_dir=args.clear,
    )