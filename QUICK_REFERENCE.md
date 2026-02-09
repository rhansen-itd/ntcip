# NTCIP Monitor - Quick Reference

## Project Structure

```
ntcip_monitor/
├── core/
│   ├── snmp_client.py          # EconoliteSNMPClient - SNMP communication
│   ├── oid_definitions.py      # All NTCIP OIDs and helper functions
│   ├── data_models.py          # SignalState, PhaseStatus, etc.
│   └── event_monitor.py        # BaseMonitor, EventEmitter
│
├── monitors/
│   ├── phase_monitor.py        # PhaseMonitor - Phases 1-16 + Overlaps
│   ├── detector_monitor.py     # DetectorMonitor - Detectors 1-64
│   └── output_monitor.py       # OutputMonitor - Outputs 1-16
│
├── utils/
│   ├── config_loader.py        # ConfigLoader - Hot-reload support
│   └── controller_control.py   # ControllerControl - Set time, calls, outputs
│
├── ui/
│   ├── web_ui.py              # Flask web dashboard
│   └── templates/
│       └── dashboard.html      # Web interface
│
└── main.py                     # NTCIPMonitorApp - Main orchestrator

run.py          # Entry point script
config.json     # Configuration file
```

## Key Classes

### EconoliteSNMPClient
```python
client = EconoliteSNMPClient(
    ip='10.37.2.68',
    port=501,
    community='administrator'
)
value = client.get('1.3.6.1.2.1.1.1.0')
client.set('oid', 123)
```

### NTCIPMonitorApp
```python
app = NTCIPMonitorApp('config.json')
app.start()
phase_monitor = app.get_phase_monitor()
detector_monitor = app.get_detector_monitor()
controller = app.get_controller()
```

### PhaseMonitor
```python
monitor = PhaseMonitor(client, poll_interval=0.25)
monitor.on('phase_change', callback)
monitor.on('phase_green_start', callback)
monitor.start()
state = monitor.get_current_phase_state(2)
```

### ControllerControl
```python
controller = ControllerControl(client)
controller.sync_time_to_system()
controller.place_vehicle_call(phase_num=2)
controller.set_output(output_num=5, state=True)
```

## Event Signatures

```python
# Phase events
def on_phase_change(phase_num: int, old_state: SignalState, new_state: SignalState)
def on_phase_green_start(phase_num: int)
def on_phase_red_start(phase_num: int)

# Detector events
def on_detector_change(detector_num: int, old_state: DetectorState, new_state: DetectorState)
def on_detector_on(detector_num: int)

# Output events
def on_output_change(output_num: int, old_state: OutputState, new_state: OutputState)
```

## Configuration Keys

```json
{
  "controller": {
    "ip": "string",
    "port": 501,
    "community": "string",
    "timeout": 2,
    "retries": 2
  },
  "monitors": {
    "phases": {
      "enabled": bool,
      "poll_interval": float,
      "monitor_1_8": bool,
      "monitor_9_16": bool,
      "monitor_overlaps": bool
    },
    "detectors": {
      "enabled": bool,
      "poll_interval": float,
      "detector_range": [start, end]
    },
    "outputs": {
      "enabled": bool,
      "poll_interval": float,
      "output_range": [start, end]
    }
  }
}
```

## NTCIP OIDs

### Phase Status
- Phases 1-8: `.1206.4.2.1.1.4.1.{2|3|4}.1`
- Phases 9-16: `.1206.4.2.1.1.4.1.{2|3|4}.2`
- Overlaps 1-8: `.1206.4.2.1.1.4.1.{2|3|4}.3`

Where {2|3|4} = {Reds|Yellows|Greens}

### Detectors
- Groups 1-8: `.1206.4.2.1.2.4.1.2.{1-8}`
- Each group = 8 detectors (64 total)

### Outputs
- Individual: `.1206.4.2.1.3.14.1.2.{1-16}`

### Control
- Time: `.1206.4.2.6.3.0`
- Veh Call 1-8: `.1206.4.2.1.1.5.1.6.1`
- Veh Call 9-16: `.1206.4.2.1.1.5.1.6.2`

## Usage Examples

### Basic Monitoring
```python
from ntcip_monitor import NTCIPMonitorApp

app = NTCIPMonitorApp()
app.start()

# App runs monitors in background threads
# Config hot-reloads automatically
```

### Subscribe to Events
```python
phase_monitor = app.get_phase_monitor()

def my_callback(phase_num, old_state, new_state):
    print(f"Phase {phase_num}: {old_state} → {new_state}")

phase_monitor.on('phase_change', my_callback)
```

### Video Buffer Integration
```python
from ntcip_monitor.core import SignalState

def save_video_on_phase_2_red(phase, old, new):
    if phase == 2 and new == SignalState.RED:
        video_buffer.save_last_30_seconds(
            f"phase2_red_{datetime.now()}.mp4"
        )

phase_monitor.on('phase_change', save_video_on_phase_2_red)
```

### Control Functions
```python
controller = app.get_controller()

# Sync time
controller.sync_time_to_system()

# Place call
controller.place_vehicle_call(2, duration=5)  # 5 second call

# Control output
controller.set_output(5, True)   # Turn ON
controller.pulse_output(3, 2.0)  # Pulse 2 seconds
```

### Web UI
```python
from ntcip_monitor.ui import WebUI

web_ui = WebUI(app, port=5000)
web_ui.start()
# Open http://localhost:5000
```

## Troubleshooting

### Connection Issues
1. Port must be **501** (Econolite specific)
2. SNMP version must be **v1** (not v2c)
3. Community string = controller **username**

### Import Errors
```bash
pip uninstall -y pysnmp pysnmp-lextudio
pip install "pysnmp>=5.0.0,<6.0.0"
```

### Wrong Phase Order
Check bit ordering in `core/data_models.py`:
```python
# Econolite: Phase 1 = bit 0, Phase 8 = bit 7
bit = phase_num - 1
```

## Performance Tips

- **High-frequency monitoring**: Set `poll_interval=0.1` (10 Hz)
- **Low CPU usage**: Set `poll_interval=1.0` (1 Hz)
- **Detector-heavy**: Reduce `detector_range` to only needed detectors
- **Multiple controllers**: Run separate app instances

## API Endpoints (Web UI)

```
GET  /                      Dashboard
GET  /api/status            Current controller state
GET  /api/stats             SNMP client statistics
POST /api/control/time      Sync controller time
POST /api/control/vehicle_call  {"phase": 2}
POST /api/control/output    {"output": 5, "state": "on"}
```

## Extending the System

### Add Custom Monitor
```python
from ntcip_monitor.core import BaseMonitor

class MyMonitor(BaseMonitor):
    def _poll(self):
        # Read SNMP data
        value = self.snmp_client.get('oid')
        # Emit events
        self.emit('my_event', value)
```

### Add Custom Event Handler
```python
app = NTCIPMonitorApp()
app.start()

# Get any monitor
monitor = app.get_phase_monitor()

# Subscribe to events
monitor.on('phase_change', my_handler)
monitor.on('custom_event', another_handler)
```

## File Locations for Deployment

```
/opt/ntcip_monitor/          # Application root
    ├── ntcip_monitor/       # Python package
    ├── run.py               # Entry point
    ├── config.json          # Configuration
    └── logs/                # Log files (if enabled)
```

## Systemd Service (Linux)

```ini
[Unit]
Description=NTCIP Traffic Controller Monitor
After=network.target

[Service]
Type=simple
User=ntcip
WorkingDirectory=/opt/ntcip_monitor
ExecStart=/usr/bin/python3 /opt/ntcip_monitor/run.py
Restart=always

[Install]
WantedBy=multi-user.target
```

## Quick Commands

```bash
# Start with web UI
python3 run.py

# Start without web UI
python3 run.py --no-web

# Use custom config
python3 run.py --config /path/to/config.json

# Different web port
python3 run.py --web-port 8080

# Test SNMP connectivity
python3 -m ntcip_monitor.core.snmp_client
```
