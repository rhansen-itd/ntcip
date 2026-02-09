#!/usr/bin/env python3
"""
Example Usage Script - NTCIP Monitor

This script demonstrates common usage patterns for the NTCIP Monitor system.
"""

import time
from datetime import datetime
from ntcip_monitor import NTCIPMonitorApp
from ntcip_monitor.core import SignalState, DetectorState, OutputState


def example_1_basic_monitoring():
    """Example 1: Basic phase monitoring with event logging."""
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Phase Monitoring")
    print("="*60)
    
    # Create and start application
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    # Get phase monitor
    phase_monitor = app.get_phase_monitor()
    
    # Simple callback that logs all phase changes
    def log_phase_change(phase_num, old_state, new_state):
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{timestamp}] Phase {phase_num}: {old_state.name} → {new_state.name}")
    
    # Subscribe to phase change events
    phase_monitor.on('phase_change', log_phase_change)
    
    print("\nMonitoring all phase changes. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping...")
        app.stop()


def example_2_video_buffer_integration():
    """Example 2: Video buffer integration - save on specific transitions."""
    print("\n" + "="*60)
    print("EXAMPLE 2: Video Buffer Integration")
    print("="*60)
    
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    phase_monitor = app.get_phase_monitor()
    
    # Define transitions that should trigger video saves
    VIDEO_TRIGGERS = [
        (2, SignalState.GREEN, SignalState.RED),    # Phase 2: End of green
        (6, SignalState.GREEN, SignalState.RED),    # Phase 6: End of green
    ]
    
    def save_video_on_trigger(phase_num, old_state, new_state):
        # Check if this transition is one we care about
        for trigger_phase, trigger_old, trigger_new in VIDEO_TRIGGERS:
            if (phase_num == trigger_phase and 
                old_state == trigger_old and 
                new_state == trigger_new):
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"phase{phase_num}_{new_state.name}_{timestamp}.mp4"
                
                print(f"\n🎥 VIDEO SAVE TRIGGERED!")
                print(f"   Phase: {phase_num}")
                print(f"   Transition: {old_state.name} → {new_state.name}")
                print(f"   Filename: {filename}")
                
                # YOUR VIDEO BUFFER CODE HERE:
                # video_buffer.save_last_30_seconds(filename)
                # or
                # video_buffer.mark_event(filename)
    
    phase_monitor.on('phase_change', save_video_on_trigger)
    
    print("\nWaiting for phase 2 or 6 to go from GREEN → RED...")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.stop()


def example_3_detector_monitoring():
    """Example 3: Monitor specific detectors."""
    print("\n" + "="*60)
    print("EXAMPLE 3: Detector Monitoring")
    print("="*60)
    
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    detector_monitor = app.get_detector_monitor()
    
    if detector_monitor is None:
        print("ERROR: Detector monitoring not enabled in config.json")
        print("Enable it by setting monitors.detectors.enabled = true")
        return
    
    # Monitor specific detectors (e.g., approach detectors)
    APPROACH_DETECTORS = [1, 2, 5, 6]  # Example detector numbers
    
    def on_detector_activation(detector_num):
        if detector_num in APPROACH_DETECTORS:
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{timestamp}] 🚗 Detector {detector_num} ACTIVATED")
    
    def on_detector_deactivation(detector_num):
        if detector_num in APPROACH_DETECTORS:
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{timestamp}] ⚪ Detector {detector_num} CLEARED")
    
    detector_monitor.on('detector_on', on_detector_activation)
    detector_monitor.on('detector_off', on_detector_deactivation)
    
    print(f"\nMonitoring detectors: {APPROACH_DETECTORS}")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.stop()


def example_4_controller_control():
    """Example 4: Control functions - time sync and vehicle calls."""
    print("\n" + "="*60)
    print("EXAMPLE 4: Controller Control Functions")
    print("="*60)
    
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    controller = app.get_controller()
    
    # Example 1: Sync controller time
    print("\n1. Syncing controller time to system time...")
    try:
        controller.sync_time_to_system()
        print("   ✓ Time synced successfully")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Example 2: Place a vehicle call
    print("\n2. Placing vehicle call on Phase 2...")
    try:
        controller.place_vehicle_call(phase_num=2)
        print("   ✓ Vehicle call placed")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Example 3: Control an output
    print("\n3. Pulsing Output 5 for 3 seconds...")
    try:
        controller.pulse_output(output_num=5, duration=3.0)
        print("   ✓ Output pulsed")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    time.sleep(1)
    app.stop()


def example_5_query_current_state():
    """Example 5: Query current controller state."""
    print("\n" + "="*60)
    print("EXAMPLE 5: Query Current State")
    print("="*60)
    
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    # Give monitors time to poll once
    time.sleep(1)
    
    phase_monitor = app.get_phase_monitor()
    
    print("\nCurrent Phase States:")
    print("-" * 40)
    
    # Get all phases
    phases = phase_monitor.get_all_phases()
    
    for phase_num in sorted(phases.keys()):
        state = phases[phase_num]
        
        # Color code the output
        if state == SignalState.GREEN:
            color = '\033[92m'  # Green
        elif state == SignalState.YELLOW:
            color = '\033[93m'  # Yellow
        elif state == SignalState.RED:
            color = '\033[91m'  # Red
        else:
            color = '\033[90m'  # Gray
        
        reset = '\033[0m'
        
        print(f"  Phase {phase_num:2d}: {color}{state.name:7s}{reset}")
    
    # Get specific phase
    phase_2_state = phase_monitor.get_current_phase_state(2)
    print(f"\nPhase 2 is currently: {phase_2_state.name if phase_2_state else 'UNKNOWN'}")
    
    time.sleep(1)
    app.stop()


def example_6_multiple_monitors():
    """Example 6: Use multiple monitors simultaneously."""
    print("\n" + "="*60)
    print("EXAMPLE 6: Multiple Monitors")
    print("="*60)
    
    app = NTCIPMonitorApp('config.json')
    app.start()
    
    phase_monitor = app.get_phase_monitor()
    detector_monitor = app.get_detector_monitor()
    output_monitor = app.get_output_monitor()
    
    # Coordinated event handling
    def on_phase_yellow(phase_num):
        print(f"⚠️  Phase {phase_num} went YELLOW")
        
        # Check detector activity when phase goes yellow
        if detector_monitor:
            active = detector_monitor.get_active_detectors()
            if active:
                print(f"   Active detectors: {active}")
    
    def on_detector_on(detector_num):
        print(f"🚗 Detector {detector_num} activated")
        
        # Check which phases are green when detector activates
        if phase_monitor:
            phases = phase_monitor.get_all_phases()
            green_phases = [num for num, state in phases.items() 
                          if state == SignalState.GREEN]
            if green_phases:
                print(f"   Green phases: {green_phases}")
    
    if phase_monitor:
        phase_monitor.on('phase_yellow_start', on_phase_yellow)
    
    if detector_monitor:
        detector_monitor.on('detector_on', on_detector_on)
    else:
        print("Note: Detector monitoring not enabled in config")
    
    print("\nMonitoring phases and detectors simultaneously...")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        app.stop()


def show_menu():
    """Show example menu."""
    print("\n" + "="*60)
    print("NTCIP MONITOR - USAGE EXAMPLES")
    print("="*60)
    print("\nAvailable Examples:")
    print("  1. Basic Phase Monitoring")
    print("  2. Video Buffer Integration")
    print("  3. Detector Monitoring")
    print("  4. Controller Control Functions")
    print("  5. Query Current State")
    print("  6. Multiple Monitors")
    print("  0. Exit")
    print()


if __name__ == '__main__':
    examples = {
        '1': example_1_basic_monitoring,
        '2': example_2_video_buffer_integration,
        '3': example_3_detector_monitoring,
        '4': example_4_controller_control,
        '5': example_5_query_current_state,
        '6': example_6_multiple_monitors,
    }
    
    while True:
        show_menu()
        choice = input("Select example (0-6): ").strip()
        
        if choice == '0':
            print("\nGoodbye!")
            break
        
        if choice in examples:
            try:
                examples[choice]()
            except KeyboardInterrupt:
                print("\n\nExample interrupted.")
            except Exception as e:
                print(f"\n\nError running example: {e}")
                import traceback
                traceback.print_exc()
            
            input("\nPress Enter to continue...")
        else:
            print("Invalid choice. Please select 0-6.")
