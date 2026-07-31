#!/usr/bin/env python3
"""
NTCIP Monitor - Main Entry Point

Starts the NTCIP monitor application with optional web UI.
"""

import os
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
  %(prog)s --web-host 0.0.0.0    # Expose the web UI on the LAN (see note)

Note: the web UI binds to 127.0.0.1 by default. Exposing it with
--web-host also requires a control token (config web_ui.control_token or
$NTCIP_WEB_CONTROL_TOKEN) before /api/control/* will touch the hardware,
or before the /overlay page will serve camera imagery.
        """
    )

    parser.add_argument('--config', default='config.json',
                       help='Configuration file path (default: config.json)')
    parser.add_argument('--no-web', action='store_true',
                       help='Disable web UI')
    parser.add_argument('--web-host', default=None,
                       help='Web UI bind address (default: web_ui.host from '
                            'config, else 127.0.0.1). Use 0.0.0.0 to expose '
                            'on the network.')
    parser.add_argument('--web-port', type=int, default=None,
                       help='Web UI port (default: web_ui.port from config, '
                            'else 5000)')

    args = parser.parse_args()
    
    print("="*60)
    print("NTCIP TRAFFIC CONTROLLER MONITOR")
    print("="*60)
    print()
    
    # Create and start application
    app = NTCIPMonitorApp(config_path=args.config)
    web_ui = None

    try:
        app.start()

        # Start web UI if enabled. CLI flags win over config, config over the
        # (loopback-only) defaults.
        if not args.no_web:
            config = app.config_loader
            # `or` chains so an empty/null config value falls through to the
            # safe default rather than binding to every interface.
            web_host = args.web_host or config.get('web_ui.host') or '127.0.0.1'
            web_port = args.web_port or config.get('web_ui.port') or 5000
            # Env var keeps the shared secret out of a world-readable config.
            control_token = (os.environ.get('NTCIP_WEB_CONTROL_TOKEN')
                             or config.get('web_ui.control_token', ''))

            # Live video overlay (ROADMAP 11b); absent section => disabled.
            overlay_config = config.get('overlay') or {}

            web_ui = WebUI(app, host=web_host, port=web_port,
                           control_token=control_token,
                           overlay_config=overlay_config)
            web_ui.start()
            print(f"\n→ Open http://{web_host}:{web_port} in your browser")
            if overlay_config.get('enabled'):
                print(f"→ Video overlay at http://{web_host}:{web_port}/overlay")
        
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
