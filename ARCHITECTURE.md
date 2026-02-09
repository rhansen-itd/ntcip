# NTCIP Monitor - Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         USER LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │ Web Browser  │  │ Python App   │  │  External Modules    │ │
│  │  Dashboard   │  │   Script     │  │  (Video Buffer, etc) │ │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘ │
│         │                  │                     │              │
└─────────┼──────────────────┼─────────────────────┼──────────────┘
          │                  │                     │
          │ HTTP             │ Import              │ Event Callbacks
          │                  │                     │
┌─────────▼──────────────────▼─────────────────────▼──────────────┐
│                    APPLICATION LAYER                             │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              NTCIPMonitorApp (Orchestrator)             │    │
│  │  • Manages lifecycle of all components                  │    │
│  │  • Hot-reload configuration                             │    │
│  │  • Provides access to monitors and controller           │    │
│  └────────────┬───────────────────────────┬─────────────────┘   │
│               │                           │                      │
│  ┌────────────▼─────────┐   ┌────────────▼──────────┐          │
│  │     WebUI (Flask)     │   │   ConfigLoader        │          │
│  │  • Serves dashboard   │   │  • Watches config.json│          │
│  │  • REST API           │   │  • Auto-reload        │          │
│  └───────────────────────┘   └───────────────────────┘          │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                EVENT-DRIVEN MONITORS                      │  │
│  ├───────────────────────────────────────────────────────────┤  │
│  │                                                           │  │
│  │  ┌──────────────┐  ┌───────────────┐  ┌──────────────┐  │  │
│  │  │PhaseMonitor  │  │DetectorMonitor│  │OutputMonitor │  │  │
│  │  │• Phases 1-16 │  │• Detectors    │  │• Outputs     │  │  │
│  │  │• Overlaps 1-8│  │  1-64         │  │  1-16        │  │  │
│  │  │• Emit events │  │• Emit events  │  │• Emit events │  │  │
│  │  └──────┬───────┘  └───────┬───────┘  └──────┬───────┘  │  │
│  │         │                   │                 │          │  │
│  │         └───────────────────┴─────────────────┘          │  │
│  │                             │                            │  │
│  │           ┌─────────────────▼──────────────┐             │  │
│  │           │   BaseMonitor (ABC)            │             │  │
│  │           │  • EventEmitter mixin          │             │  │
│  │           │  • Threading support           │             │  │
│  │           │  • Polling loop                │             │  │
│  │           └────────────────────────────────┘             │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              ControllerControl (Actions)                  │  │
│  │  • sync_time_to_system()                                  │  │
│  │  • place_vehicle_call(phase_num)                          │  │
│  │  • set_output(output_num, state)                          │  │
│  └─────────────────────────────┬─────────────────────────────┘  │
│                                │                                 │
└────────────────────────────────┼─────────────────────────────────┘
                                 │
                                 │ SNMP GET/SET
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                     COMMUNICATION LAYER                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │            EconoliteSNMPClient                            │  │
│  │  • get(*oids) → values                                    │  │
│  │  • set(oid, value) → bool                                 │  │
│  │  • SNMPv1, Port 501, Community = username                 │  │
│  │  • Connection pooling, retry logic                        │  │
│  └─────────────────────────────┬─────────────────────────────┘  │
│                                │                                 │
│  ┌─────────────────────────────▼─────────────────────────────┐  │
│  │              OID Definitions                              │  │
│  │  • NTCIP 1201/1202 OIDs                                   │  │
│  │  • Helper functions (get_phase_oids, etc.)                │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              Data Models                                  │  │
│  │  • PhaseStatus, DetectorStatus, OutputStatus              │  │
│  │  • SignalState, DetectorState, OutputState (Enums)        │  │
│  │  • Parsing functions (bitmask → object list)              │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                   │
└────────────────────────────────┬──────────────────────────────────┘
                                 │
                                 │ SNMP Protocol
                                 │ UDP Port 501
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                    HARDWARE LAYER                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│               ┌──────────────────────────────┐                   │
│               │  Econolite Cobalt Controller │                   │
│               │  • NTCIP 1201/1202 Compliant │                   │
│               │  • SNMPv1 on Port 501        │                   │
│               │  • Phases, Detectors, Outputs│                   │
│               └──────────────────────────────┘                   │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

## Data Flow Examples

### Example 1: Phase Change Event

```
┌─────────────────┐
│ Controller      │ Phase 2 goes from Green → Yellow
│ (Cobalt)        │
└────────┬────────┘
         │
         │ SNMP Response (bitmask: 0b00000010)
         ▼
┌─────────────────┐
│ SNMP Client     │ Receives raw SNMP data
└────────┬────────┘
         │
         │ Returns integer value
         ▼
┌─────────────────┐
│ PhaseMonitor    │ Polls every 250ms
│                 │ • Compares new state vs old state
│                 │ • Detects change: GREEN → YELLOW
└────────┬────────┘
         │
         │ emit('phase_change', 2, GREEN, YELLOW)
         │ emit('phase_yellow_start', 2)
         ▼
┌─────────────────────────────────────────┐
│ Event Subscribers (parallel execution)  │
├─────────────────────────────────────────┤
│                                         │
│  ┌──────────────┐  ┌────────────────┐  │
│  │ Video Buffer │  │ Data Logger    │  │
│  │ saves clip   │  │ writes to file │  │
│  └──────────────┘  └────────────────┘  │
│                                         │
│  ┌──────────────┐                      │
│  │ Web UI       │                      │
│  │ updates view │                      │
│  └──────────────┘                      │
│                                         │
└─────────────────────────────────────────┘
```

### Example 2: Place Vehicle Call

```
┌─────────────────┐
│ User / Script   │ controller.place_vehicle_call(2)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ControllerControl│ Converts phase 2 → bitmask: 0b00000010
└────────┬────────┘
         │
         │ set(OID, 0b00000010)
         ▼
┌─────────────────┐
│ SNMP Client     │ Builds SNMP SET packet
└────────┬────────┘
         │
         │ UDP to port 501
         ▼
┌─────────────────┐
│ Controller      │ Receives vehicle call
│ (Cobalt)        │ Places call on Phase 2
└─────────────────┘
```

### Example 3: Config Hot-Reload

```
┌─────────────────┐
│ User edits      │ vim config.json
│ config.json     │ (enables detector monitor)
└────────┬────────┘
         │
         │ File modified
         ▼
┌─────────────────┐
│ ConfigLoader    │ Checks mtime every 5 seconds
│ (background     │ Detects change
│  thread)        │
└────────┬────────┘
         │
         │ Triggers reload
         ▼
┌─────────────────┐
│ NTCIPMonitorApp │
│ • Stops old monitors
│ • Reads new config
│ • Starts new monitors
└─────────────────┘
```

## Threading Model

```
Main Thread
├─ NTCIPMonitorApp
├─ Flask Web Server (daemon thread)
├─ ConfigLoader check loop (daemon thread)
└─ Monitors (daemon threads)
   ├─ PhaseMonitor polling loop
   ├─ DetectorMonitor polling loop
   └─ OutputMonitor polling loop

Event Callbacks
├─ Executed in monitor thread
├─ Should be fast (non-blocking)
└─ Can spawn own threads if needed
```

## State Management

```
┌──────────────────────────────────────┐
│   Phase Monitor State                │
├──────────────────────────────────────┤
│ _last_phases = {                     │
│   1: SignalState.RED,                │
│   2: SignalState.GREEN,              │
│   ...                                │
│ }                                    │
│                                      │
│ _last_overlaps = {                   │
│   1: SignalState.DARK,               │
│   ...                                │
│ }                                    │
└──────────────────────────────────────┘
         ▲              │
         │              │
    Read state     Emit events
    for queries     on changes
         │              │
         │              ▼
┌────────┴──────────────────────────────┐
│  External Modules                     │
│  • Read current state                 │
│  • Subscribe to changes               │
└───────────────────────────────────────┘
```

## Module Dependencies

```
run.py
 └─ NTCIPMonitorApp (main.py)
     ├─ ConfigLoader (utils/config_loader.py)
     ├─ EconoliteSNMPClient (core/snmp_client.py)
     │   └─ pysnmp (external)
     ├─ PhaseMonitor (monitors/phase_monitor.py)
     │   ├─ BaseMonitor (core/event_monitor.py)
     │   ├─ OID Definitions (core/oid_definitions.py)
     │   └─ Data Models (core/data_models.py)
     ├─ DetectorMonitor (monitors/detector_monitor.py)
     ├─ OutputMonitor (monitors/output_monitor.py)
     ├─ ControllerControl (utils/controller_control.py)
     └─ WebUI (ui/web_ui.py)
         └─ Flask (external)
```

## Design Patterns Used

1. **Observer Pattern**: Event emission and subscription
2. **Singleton Pattern**: SNMP client shared across monitors
3. **Strategy Pattern**: Different monitors for different data types
4. **Template Method**: BaseMonitor defines structure, subclasses implement _poll()
5. **Factory Pattern**: OID helper functions create appropriate OIDs
6. **Facade Pattern**: NTCIPMonitorApp provides simple interface to complex system

## Extension Points

```
1. Add Custom Monitor
   └─ Inherit from BaseMonitor
   └─ Implement _poll()
   └─ Emit custom events

2. Add Custom Event Handler
   └─ Subscribe to existing events
   └─ Process data externally

3. Add New OIDs
   └─ Add to oid_definitions.py
   └─ Update data models if needed

4. Add Web UI Endpoints
   └─ Add routes to web_ui.py
   └─ Update dashboard.html

5. Custom Configuration
   └─ Add keys to config.json
   └─ Read in ConfigLoader
```

## Security Considerations

- SNMP community strings are passwords (use secure strings)
- Web UI has no authentication (add reverse proxy with auth if exposed)
- SNMP SET operations can control traffic signals (restrict access)
- File permissions on config.json (should not be world-readable)
