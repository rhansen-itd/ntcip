# NTCIP Monitor - Project Summary

## ✅ What You Have

A complete, production-ready **event-driven NTCIP monitoring system** for Econolite Cobalt traffic controllers.

### Core Features Implemented

✅ **Phases 1-16** - Full monitoring with state change events  
✅ **Overlaps 1-8** - Full monitoring with state change events  
✅ **Detectors 1-64** - All 64 vehicle detectors with activation events  
✅ **Outputs 1-16** - All 16 digital outputs with state change events  
✅ **Event System** - Pub/sub callbacks for external module integration  
✅ **Web Dashboard** - Real-time visualization (Flask)  
✅ **Hot-Reload Config** - Change settings without restarting  
✅ **Control Functions** - Set time, place calls, control outputs  
✅ **Modular Design** - Clean architecture, easy to extend  

### What Makes This Special

1. **Event-Driven**: Subscribe to specific controller events (e.g., "Phase 2 goes RED")
2. **Video Buffer Ready**: Designed for easy integration with your video system
3. **Production Quality**: Proper threading, error handling, logging
4. **Econolite Optimized**: SNMPv1, Port 501, correct bit ordering

## 🚀 Quick Start (3 Steps)

```bash
# 1. Install
pip install -r requirements.txt

# 2. Configure
# Edit config.json - set your controller IP

# 3. Run
python3 run.py
```

Open http://localhost:5000 - you should see your phases updating in real-time!

## 📁 File Structure

```
ntcip_monitor/               ← Main Python package
├── core/                    ← Core functionality
│   ├── snmp_client.py       ← SNMP communication
│   ├── oid_definitions.py   ← NTCIP OIDs
│   ├── data_models.py       ← Data structures
│   └── event_monitor.py     ← Event system
├── monitors/                ← Monitor implementations
│   ├── phase_monitor.py     ← Phases & overlaps
│   ├── detector_monitor.py  ← Vehicle detectors
│   └── output_monitor.py    ← Digital outputs
├── utils/                   ← Utilities
│   ├── config_loader.py     ← Config management
│   └── controller_control.py ← Control functions
├── ui/                      ← Web interface
│   ├── web_ui.py            ← Flask server
│   └── templates/
│       └── dashboard.html   ← Dashboard UI
└── main.py                  ← Main orchestrator

run.py                       ← Entry point
config.json                  ← Configuration
requirements.txt             ← Dependencies
README.md                    ← Full documentation
QUICK_REFERENCE.md          ← API reference
ARCHITECTURE.md             ← System design
```

## 🎯 Most Important: Video Buffer Integration

This is the **key feature** you requested. Here's how to connect your video system:

```python
from ntcip_monitor import NTCIPMonitorApp
from ntcip_monitor.core import SignalState

# Start the monitor
app = NTCIPMonitorApp()
app.start()

# Get the phase monitor
phase_monitor = app.get_phase_monitor()

# Subscribe to phase changes
def save_video_on_phase_2_red(phase_num, old_state, new_state):
    # This runs EVERY TIME any phase changes
    # Filter for the specific transition you care about:
    
    if phase_num == 2 and old_state == SignalState.GREEN and new_state == SignalState.RED:
        print("Phase 2 went GREEN → RED!")
        
        # YOUR VIDEO BUFFER CODE HERE:
        # video_buffer.save_last_30_seconds(
        #     filename=f"phase2_red_{datetime.now()}.mp4"
        # )

# Register the callback
phase_monitor.on('phase_change', save_video_on_phase_2_red)

# The monitor runs in background - your callback gets called automatically!
```

### Multiple Triggers Example

```python
# Define all the transitions you want to capture
TRIGGERS = [
    (2, SignalState.GREEN, SignalState.RED),    # Phase 2: Green → Red
    (2, SignalState.YELLOW, SignalState.RED),   # Phase 2: Yellow → Red
    (6, SignalState.GREEN, SignalState.YELLOW), # Phase 6: Green → Yellow
]

def on_phase_change(phase_num, old_state, new_state):
    for trigger_phase, trigger_old, trigger_new in TRIGGERS:
        if (phase_num == trigger_phase and 
            old_state == trigger_old and 
            new_state == trigger_new):
            
            filename = f"phase{phase_num}_{new_state.name}.mp4"
            print(f"Saving: {filename}")
            # video_buffer.save(filename)

phase_monitor.on('phase_change', on_phase_change)
```

## 📝 Configuration File (config.json)

```json
{
  "controller": {
    "ip": "10.37.2.68",           ← Your controller IP
    "port": 501,                  ← Must be 501 for Econolite
    "community": "administrator"  ← Controller username
  },
  "monitors": {
    "phases": {
      "enabled": true,            ← Enable phase monitoring
      "poll_interval": 0.25,      ← 4 times per second
      "monitor_1_8": true,        ← Phases 1-8
      "monitor_9_16": false,      ← Phases 9-16 (if you have them)
      "monitor_overlaps": false   ← Overlaps (if you have them)
    },
    "detectors": {
      "enabled": false,           ← Enable if needed
      "poll_interval": 0.1,       ← 10 times per second
      "detector_range": [1, 65]   ← Detectors 1-64
    },
    "outputs": {
      "enabled": false,           ← Enable if needed
      "poll_interval": 0.25,
      "output_range": [1, 17]     ← Outputs 1-16
    }
  }
}
```

**Hot-reload**: Edit this file while the app is running - it will reload automatically!

## 🎮 Control Functions

```python
# Get controller interface
controller = app.get_controller()

# Sync controller clock to your system
controller.sync_time_to_system()

# Place a vehicle call (simulates detector)
controller.place_vehicle_call(phase_num=2)

# Control an output
controller.set_output(output_num=5, state=True)   # Turn ON
controller.set_output(output_num=5, state=False)  # Turn OFF
controller.pulse_output(output_num=3, duration=2.0)  # 2 second pulse
```

## 🌐 Web Dashboard

When you run the app, it starts a web server at http://localhost:5000

Features:
- Real-time phase status (colored circles)
- Detector grid (64 detectors)
- Output status
- Control buttons (sync time, place calls)
- Auto-updates 4 times per second

## 🔧 Econolite-Specific Details

**Critical settings for Econolite Cobalt:**
- Port: **501** (not the standard 161)
- SNMP Version: **v1** (not v2c)
- Community: **Controller username** (not "public")
- Bit Order: **Phase 1 = bit 0** (LSB to MSB)

These are all correctly configured in the code!

## 📊 Available Events

Subscribe to these events on monitors:

### Phase Monitor
- `'phase_change'` → `(phase_num, old_state, new_state)`
- `'phase_green_start'` → `(phase_num)`
- `'phase_red_start'` → `(phase_num)`
- `'phase_yellow_start'` → `(phase_num)`
- `'overlap_change'` → `(overlap_num, old_state, new_state)`

### Detector Monitor
- `'detector_change'` → `(detector_num, old_state, new_state)`
- `'detector_on'` → `(detector_num)`
- `'detector_off'` → `(detector_num)`

### Output Monitor
- `'output_change'` → `(output_num, old_state, new_state)`
- `'output_on'` → `(output_num)`
- `'output_off'` → `(output_num)`

## 🐛 Troubleshooting

### Connection Failed

1. **Ping the controller**: `ping 10.37.2.68`
2. **Check port**: Must be 501
3. **Check community**: Must match controller username
4. **Verify SNMP v1**: Not v2c

### Import Errors

```bash
pip uninstall -y pysnmp pysnmp-lextudio
pip install "pysnmp>=5.0.0,<6.0.0"
```

### Phases Showing Wrong Numbers

The bit ordering is correct for Econolite. If you see issues:
1. Check `core/data_models.py` line ~140
2. Verify: `bit = phase_num - 1` (Phase 1 = bit 0)

## 📚 Documentation Files

- **README.md** - Complete user guide
- **QUICK_REFERENCE.md** - API reference, code examples
- **ARCHITECTURE.md** - System design, data flow diagrams
- **This file** - Getting started summary

## 🎓 Learning Path

1. **Start here**: Run `python3 run.py` and open web dashboard
2. **Understand events**: Read the video buffer integration example above
3. **Customize**: Edit `config.json` to enable/disable monitors
4. **Extend**: Add your own event handlers
5. **Deep dive**: Read ARCHITECTURE.md to understand internals

## 💡 Pro Tips

1. **Development**: Use `poll_interval=1.0` for slower updates (easier to debug)
2. **Production**: Use `poll_interval=0.25` for responsive monitoring
3. **High Performance**: Use `poll_interval=0.1` for detectors (10 Hz)
4. **Logging**: Enable `logging.enabled` in config to write events to file
5. **Multiple Controllers**: Run separate app instances with different configs

## ⚠️ Important Notes

1. **Network**: Must have network access to controller (UDP port 501)
2. **Permissions**: SNMP SET commands control real traffic signals - be careful!
3. **Threading**: Event callbacks run in monitor threads - keep them fast
4. **State**: Monitors track state internally - accurate change detection

## 🚀 Next Steps

1. **Test the connection**: `python3 run.py --config config.json`
2. **Verify phases update**: Watch the web dashboard
3. **Add your video trigger**: Copy the example code above
4. **Test the trigger**: Manually change phases and verify callback fires
5. **Deploy to field computer**: Copy entire directory and run

## 📞 Support

- **NTCIP Standard**: Refer to NTCIP 1202 documentation
- **Econolite**: Refer to Cobalt SNMP MIB documentation
- **Python/PySNMP**: Check PySNMP documentation

## ✅ Verification Checklist

Before deploying:
- [ ] Can ping controller
- [ ] Web dashboard shows phases updating
- [ ] Event callback fires on phase change
- [ ] Video buffer integration tested
- [ ] Config hot-reload works
- [ ] All required monitors enabled

## 🎉 You're Ready!

You now have a complete, production-ready NTCIP monitoring system that's ready to integrate with your video buffer. The event-driven architecture makes it easy to trigger video saves on specific phase changes.

**Start with:** `python3 run.py`

Good luck with your traffic monitoring project!
