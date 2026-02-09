#!/usr/bin/env python3
"""
NTCIP Monitor - Main Entry Point

Starts the NTCIP monitor application with optional web UI.
"""

import sys
import time
import argparse
from ntcip_monitor import NTCIPMonitorApp
from ntcip_monitor.ui import WebUI


def main():
    parser = argparse.ArgumentParser(
        description='NTCIP Traffic Controller Monitor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                    # Run with default config
  %(prog)s --no-web           # Run without web UI
  %(prog)s --config custom.json  # Use custom config file
        """
    )
    
    parser.add_argument('--config', default='config.json',
                       help='Configuration file path (default: config.json)')
    parser.add_argument('--no-web', action='store_true',
                       help='Disable web UI')
    parser.add_argument('--web-port', type=int, default=5000,
                       help='Web UI port (default: 5000)')
    
    args = parser.parse_args()
    
    print("="*60)
    print("NTCIP TRAFFIC CONTROLLER MONITOR")
    print("="*60)
    print()
    
    # Create and start application
    app = NTCIPMonitorApp(config_path=args.config)
    
    try:
        app.start()
        
        # Start web UI if enabled
        web_ui = None
        if not args.no_web:
            web_ui = WebUI(app, port=args.web_port)
            web_ui.start()
            print(f"\n→ Open http://localhost:{args.web_port} in your browser")
        
        print("\nPress Ctrl+C to stop")
        print("="*60)
        print()
        
        # Example: Subscribe to phase changes for logging
        if app.get_phase_monitor():
            def log_phase_change(phase_num, old_state, new_state):
                print(f"[EVENT] Phase {phase_num}: {old_state} → {new_state}")
            
            app.get_phase_monitor().on('phase_change', log_phase_change)
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        app.stop()
        if web_ui:
            web_ui.stop()
        print("Goodbye!")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
