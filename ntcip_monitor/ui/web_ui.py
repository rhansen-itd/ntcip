"""
Flask Web UI for NTCIP Monitor

Provides a simple web dashboard to visualize:
- Phase status (colored circles)
- Detector status (grid)
- Controller settings
"""

from flask import Flask, render_template, jsonify, request
import threading
from ..core.data_models import SignalState, DetectorState, OutputState


class WebUI:
    """
    Flask-based web interface for NTCIP Monitor.
    
    Runs in a separate thread to avoid blocking the main application.
    """
    
    def __init__(self, app_instance, host='0.0.0.0', port=5000):
        """
        Initialize web UI.
        
        Args:
            app_instance: NTCIPMonitorApp instance
            host: Host to bind to
            port: Port to listen on
        """
        self.app_instance = app_instance
        self.host = host
        self.port = port
        
        # Create Flask app
        self.flask_app = Flask(__name__)
        self._setup_routes()
        
        self._thread = None
        self._running = False
    
    def _setup_routes(self):
        """Setup Flask routes."""
        
        @self.flask_app.route('/')
        def index():
            """Main dashboard."""
            return render_template('dashboard.html')
        
        @self.flask_app.route('/api/status')
        def get_status():
            """Get current controller status."""
            status = {
                'phases': {},
                'overlaps': {},
                'detectors': {},
                'outputs': {},
                'connected': True
            }
            
            # Get phase data
            if self.app_instance.phase_monitor:
                phases = self.app_instance.phase_monitor.get_all_phases()
                status['phases'] = {
                    num: state.name for num, state in phases.items()
                }
                
                overlaps = self.app_instance.phase_monitor.get_all_overlaps()
                status['overlaps'] = {
                    num: state.name for num, state in overlaps.items()
                }
            
            # Get detector data
            if self.app_instance.detector_monitor:
                detectors = self.app_instance.detector_monitor.get_all_detectors()
                status['detectors'] = {
                    num: state.name for num, state in detectors.items()
                }
            
            # Get output data
            if self.app_instance.output_monitor:
                outputs = self.app_instance.output_monitor.get_all_outputs()
                status['outputs'] = {
                    num: state.name for num, state in outputs.items()
                }
            
            return jsonify(status)
        
        @self.flask_app.route('/api/stats')
        def get_stats():
            """Get SNMP client statistics."""
            if self.app_instance.snmp_client:
                return jsonify(self.app_instance.snmp_client.get_stats())
            return jsonify({'error': 'No SNMP client'})
        
        @self.flask_app.route('/api/control/time', methods=['POST'])
        def sync_time():
            """Sync controller time to system time."""
            try:
                controller = self.app_instance.get_controller()
                if controller:
                    controller.sync_time_to_system()
                    return jsonify({'success': True})
                return jsonify({'success': False, 'error': 'Controller not available'})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})
        
        @self.flask_app.route('/api/control/vehicle_call', methods=['POST'])
        def place_vehicle_call():
            """Place vehicle call on a phase."""
            try:
                phase_num = int(request.json.get('phase'))
                controller = self.app_instance.get_controller()
                if controller:
                    controller.place_vehicle_call(phase_num)
                    return jsonify({'success': True})
                return jsonify({'success': False, 'error': 'Controller not available'})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})
        
        @self.flask_app.route('/api/control/output', methods=['POST'])
        def set_output():
            """Set output ON or OFF."""
            try:
                output_num = int(request.json.get('output'))
                state = request.json.get('state') == 'on'
                controller = self.app_instance.get_controller()
                if controller:
                    controller.set_output(output_num, state)
                    return jsonify({'success': True})
                return jsonify({'success': False, 'error': 'Controller not available'})
            except Exception as e:
                return jsonify({'success': False, 'error': str(e)})
    
    def start(self):
        """Start web UI in background thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._run_flask,
            daemon=True,
            name="WebUI"
        )
        self._thread.start()
        
        print(f"Web UI started at http://{self.host}:{self.port}")
    
    def _run_flask(self):
        """Run Flask server."""
        self.flask_app.run(
            host=self.host,
            port=self.port,
            debug=False,
            use_reloader=False
        )
    
    def stop(self):
        """Stop web UI."""
        self._running = False
        # Note: Flask doesn't have a clean shutdown method when run in thread
        # In production, use Werkzeug's Server.shutdown() or run in separate process
