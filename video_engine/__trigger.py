
# trigger.py
import json, os, uuid
from datetime import datetime, timezone
from pathlib import Path

Path("./trigger_queue").mkdir(exist_ok=True)

trigger_id = uuid.uuid4().hex
payload = {
    "trigger_id": trigger_id,
    "action": "start",
    "event_timestamp": datetime.now(timezone.utc).timestamp(),
    "reason": "manual",
    "intersection_id": "manual",
    "cameras": ["cam1"],
    "pre_roll_sec": 0,
    "post_roll_sec": 0,
    "max_duration_sec": 300,   # recording length — adjust as needed
    "metadata": {}
}

tmp = f"./trigger_queue/trigger_manual_{trigger_id[:8]}.tmp"
out = tmp.replace(".tmp", ".json")
with open(tmp, "w") as f:
    json.dump(payload, f, indent=2)
os.rename(tmp, out)

print(f"Trigger dropped: {out}")
print(f"trigger_id: {trigger_id}")