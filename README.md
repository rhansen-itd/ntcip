# NTCIP Traffic Controller Monitor

Event-driven monitoring system for NTCIP 1201/1202 traffic signal controllers.  
Optimized for **Econolite Cobalt/EOS** controllers.

## Features

✅ **Event-Driven Architecture** - Subscribe to phase changes, detector activations, etc.  
✅ **Modular Design** - Easy to extend and integrate with external systems  
✅ **Hot-Reload Configuration** - Update settings without restarting  
✅ **Web Dashboard** - Real-time visualization of controller state  
✅ **Full NTCIP Support** - Phases, Overlaps, Detectors, Outputs  
✅ **Control Functions** - Set time, place calls, control outputs  

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Edit config.json (set your controller IP)

# 3. Run
python3 run.py

# 4. Open http://localhost:5000 in browser
```

## Event System - Integration Example

```python
from ntcip_monitor import NTCIPMonitorApp
from ntcip_monitor.core import SignalState

app = NTCIPMonitorApp()
app.start()

# Subscribe to phase changes for video buffer system
def on_phase_2_red(phase_num, old_state, new_state):
    if phase_num == 2 and new_state == SignalState.RED:
        print("Phase 2 RED - save video!")
        # video_buffer.save_last_30_seconds()

app.get_phase_monitor().on('phase_change', on_phase_2_red)
```

## Available Events

- `phase_change`, `phase_green_start`, `phase_red_start`
- `detector_on`, `detector_off`
- `output_change`

See full documentation in README.md

## Configuration (config.json)

```json
{
  "controller": {
    "ip": "10.37.2.68",
    "port": 501,
    "community": "administrator"
  },
  "monitors": {
    "phases": {"enabled": true, "poll_interval": 0.25},
    "detectors": {"enabled": false},
    "outputs": {"enabled": false}
  }
}
```

## Command Line

```bash
python3 run.py                    # Run with web UI
python3 run.py --no-web           # No web UI
python3 run.py --config custom.json
```

## Econolite Specifics

- Port: **501** (not 161)
- SNMP: **v1** (not v2c)  
- Community: **Controller username**

For full documentation, see project files.
