"""
Flask Web UI for NTCIP Monitor

Provides a simple web dashboard to visualize:
- Phase status (colored circles)
- Detector status (grid)
- Controller settings

Network exposure (2026-07-31, ROADMAP 4f)
-----------------------------------------
The bind host defaults to ``127.0.0.1`` — the dashboard is a local operator
tool, not a service. Pass ``host='0.0.0.0'`` (``--web-host`` / the config's
``web_ui.host``) to deliberately expose it on the LAN.

The ``/api/control/*`` endpoints drive real signal hardware (time sync,
vehicle calls, output toggles), so they are gated by a shared-secret header
(``X-NTCIP-Control-Token``) instead of being open like the read-only
``/api/status`` and ``/api/stats`` routes. The policy is deliberately minimal
(no sessions, no users, no JWT — see the DESIGN_HISTORY entry):

* A token is configured -> every control request must carry a matching
  header, compared with :func:`hmac.compare_digest`; otherwise 401.
* No token and the bind host is loopback -> allowed. This is today's
  behavior for a local operator and keeps the dashboard button working.
* No token and the bind host is *not* loopback -> control refused with 403
  (reads still work), plus a startup warning. You cannot accidentally expose
  unauthenticated hardware control to the network.

Live video overlay (2026-07-31, ROADMAP 11b)
--------------------------------------------
``/overlay`` renders calibrated detector loops and stopbars on a ``<canvas>``
over a camera background, recolored from the live monitor state. Its four
``/api/overlay/*`` routes split deliberately:

* ``shapes`` (static, fetched once) and ``state`` (polled at the dashboard's
  250 ms interval) — **open**, like ``/api/status``: they are the same signal
  state the dashboard already publishes, in a different shape.
* ``background`` and ``stream`` — **gated by the same interlock as
  ``/api/control/*``**, a deliberate departure from the 4f policy of leaving
  reads open. Proxied camera imagery is a different category from "phase 3 is
  green": it is a live view of a public roadway, and an operator who passes
  ``--web-host 0.0.0.0`` to reach the dashboard would not expect to have
  published the camera with it. The two video routes additionally accept the
  token as a ``?token=`` query parameter, because an ``<img>`` tag — how the
  MJPEG stream is consumed — cannot set a request header.

With ``background: "live"`` (Item 11c) the camera is decoded once and shared:
every viewer of ``/api/overlay/stream`` and every ``/api/overlay/background``
hit attaches to the same decoder thread, which is opened on the first viewer
and torn down shortly after the last one leaves.
"""

from flask import Flask, Response, render_template, jsonify, request
import hmac
import ipaddress
import logging
import threading
import time
from .overlay import (
    ShapeConfig,
    create_background_source,
    resolve_all,
    shapes_payload,
)

logger = logging.getLogger(__name__)

#: Request header carrying the shared secret for ``/api/control/*``.
CONTROL_TOKEN_HEADER = 'X-NTCIP-Control-Token'

#: Multipart boundary for ``/api/overlay/stream``.
MJPEG_BOUNDARY = b'ntcipframe'

#: Hostnames that resolve to the loopback interface but aren't IP literals.
_LOOPBACK_NAMES = {'localhost', 'localhost.localdomain'}


def _is_loopback_host(host) -> bool:
    """Return True if binding to ``host`` reaches only the local machine.

    Args:
        host: Bind address as passed to ``flask_app.run()`` (IP literal or
            hostname). ``None``/empty is treated as "all interfaces".

    Returns:
        bool: True for loopback literals (``127.0.0.0/8``, ``::1``) and the
        well-known loopback hostnames; False for everything else, including
        unresolvable names — unknown means "assume exposed".
    """
    if not host:
        return False

    name = str(host).strip().lower()
    if name in _LOOPBACK_NAMES:
        return True

    try:
        return ipaddress.ip_address(name.strip('[]')).is_loopback
    except ValueError:
        return False


class WebUI:
    """
    Flask-based web interface for NTCIP Monitor.

    Runs in a separate thread to avoid blocking the main application.
    """

    def __init__(self, app_instance, host='127.0.0.1', port=5000,
                 control_token=None, overlay_config=None):
        """
        Initialize web UI.

        Args:
            app_instance: NTCIPMonitorApp instance
            host: Host to bind to. Defaults to loopback-only; pass
                ``'0.0.0.0'`` to deliberately expose the UI on the network.
            port: Port to listen on
            control_token: Shared secret required in the
                ``X-NTCIP-Control-Token`` header on ``/api/control/*``
                requests. ``None``/empty means no token is configured, in
                which case control is allowed only on a loopback bind (see
                the module docstring).
            overlay_config: The ``overlay`` section of ``config.json``
                (``enabled``, ``shapes_csv``, ``background``, ``image_path``,
                ``camera_url``, ``stream_fps``). ``None`` or ``enabled: false``
                leaves ``/overlay`` and the ``/api/overlay/*`` routes
                registered but answering 404.
        """
        self.app_instance = app_instance
        self.host = host
        self.port = port
        self.control_token = (control_token or '').strip() or None
        self._loopback_only = _is_loopback_host(host)

        self.overlay_config = dict(overlay_config or {})
        self.overlay_enabled = bool(self.overlay_config.get('enabled'))
        self._shape_config = None
        self._shape_error = None
        self._background_source = None
        self._background_error = None
        if self.overlay_enabled:
            self._init_overlay()

        # Create Flask app
        self.flask_app = Flask(__name__)
        self._setup_routes()

        self._thread = None
        self._running = False

    def _init_overlay(self):
        """Load the shape CSV and build the background source.

        Neither failure is fatal: a monitor that can't find its calibration
        file should still serve the dashboard. The offending piece is left as
        ``None`` and its routes answer 503 with the reason, which the overlay
        page shows to the operator.
        """
        shapes_csv = self.overlay_config.get('shapes_csv')
        try:
            if not shapes_csv:
                raise ValueError('overlay.shapes_csv is not set')
            self._shape_config = ShapeConfig.load(shapes_csv)
            logger.info(
                'Overlay shapes loaded: %d shape(s) at %sx%s from %s',
                len(self._shape_config.shapes),
                self._shape_config.video_width,
                self._shape_config.video_height,
                shapes_csv,
                extra={
                    'event': 'overlay_shapes_loaded',
                    'path': str(shapes_csv),
                    'shape_count': len(self._shape_config.shapes),
                },
            )
        except (OSError, ValueError) as e:
            self._shape_error = str(e)
            logger.error(
                'Overlay shapes unavailable: %s', e,
                extra={'event': 'overlay_shapes_failed', 'error': str(e)},
            )

        try:
            self._background_source = create_background_source(self.overlay_config)
        except (ValueError, NotImplementedError) as e:
            self._background_error = str(e)
            logger.error(
                'Overlay background source unavailable: %s', e,
                extra={'event': 'overlay_background_failed', 'error': str(e)},
            )

    def _check_shared_secret(self, supplied, scope, disabled_error):
        """Apply the token/loopback interlock to a privileged request.

        Implements the three-case policy documented in the module docstring.
        Both ``/api/control/*`` (4f) and the two video routes (11b) go through
        here so the policy has exactly one implementation.

        Args:
            supplied: The token the request carried, or ``''``.
            scope: Short label for the log event (``'control'`` / ``'video'``).
            disabled_error: Operator-facing message for the 403 case.

        Returns:
            tuple | None: ``None`` when the request is authorized, otherwise
            a ``(flask_response, status_code)`` tuple to return from the view.
        """
        if self.control_token is None:
            if self._loopback_only:
                return None
            logger.warning(
                '{"event":"%s_denied","reason":"no_token_non_loopback_bind",'
                '"host":"%s","path":"%s"}',
                scope, self.host, request.path,
            )
            return jsonify({'success': False, 'error': disabled_error}), 403

        # Compare as bytes: header values may contain non-ASCII, which
        # compare_digest rejects on str.
        if not hmac.compare_digest(supplied.encode('utf-8'),
                                   self.control_token.encode('utf-8')):
            logger.warning(
                '{"event":"%s_denied","reason":"%s","path":"%s"}',
                scope, 'missing_token' if not supplied else 'bad_token',
                request.path,
            )
            return jsonify({
                'success': False,
                'error': f'Missing or invalid {CONTROL_TOKEN_HEADER} header',
            }), 401

        return None

    def _check_control_access(self):
        """Authorize an ``/api/control/*`` request.

        Header only — a control action is a POST from the dashboard's own
        JavaScript, which can set headers, so there is no reason to also honor
        a token in the query string (where it would land in access logs).

        Returns:
            tuple | None: ``None`` when authorized, else ``(response, code)``.
        """
        return self._check_shared_secret(
            request.headers.get(CONTROL_TOKEN_HEADER, ''),
            'control',
            ('Control endpoints are disabled: the UI is bound to '
             f'{self.host} with no control token. Set '
             'web_ui.control_token (or bind to 127.0.0.1).'),
        )

    def _check_video_access(self):
        """Authorize an ``/api/overlay/background`` or ``/stream`` request.

        Same interlock as control, plus a ``?token=`` fallback: the MJPEG
        stream is consumed by an ``<img>`` tag, which cannot send a custom
        header.

        Returns:
            tuple | None: ``None`` when authorized, else ``(response, code)``.
        """
        supplied = (request.headers.get(CONTROL_TOKEN_HEADER)
                    or request.args.get('token', ''))
        return self._check_shared_secret(
            supplied,
            'video',
            ('Camera imagery is disabled: the UI is bound to '
             f'{self.host} with no control token. Set '
             'web_ui.control_token (or bind to 127.0.0.1).'),
        )

    def _build_status(self):
        """Collect the current controller state.

        Shared by ``/api/status`` and ``/api/overlay/state`` so the overlay
        can never drift from what the dashboard shows.

        Returns:
            dict: ``phases`` / ``overlaps`` / ``pedestrians`` / ``detectors`` /
            ``outputs`` dicts of number -> state name, plus ``connected``.
        """
        status = {
            'phases': {},
            'overlaps': {},
            'pedestrians': {},
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

            # Get pedestrian data
            pedestrians = self.app_instance.phase_monitor.get_all_pedestrians()
            status['pedestrians'] = {
                num: state.name for num, state in pedestrians.items()
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

        return status

    def _setup_routes(self):
        """Setup Flask routes."""
        
        @self.flask_app.route('/')
        def index():
            """Main dashboard."""
            return render_template(
                'dashboard.html',
                control_token_required=self.control_token is not None,
                control_token_header=CONTROL_TOKEN_HEADER,
            )
        
        @self.flask_app.route('/api/status')
        def get_status():
            """Get current controller status."""
            return jsonify(self._build_status())

        @self.flask_app.route('/overlay')
        def overlay_page():
            """Live video overlay page."""
            if not self.overlay_enabled:
                return (
                    'The live video overlay is disabled. Add an "overlay" '
                    'section with "enabled": true to config.json.',
                    404,
                )
            source_kind = (self._background_source.kind
                           if self._background_source else 'none')
            return render_template(
                'overlay.html',
                control_token_required=self.control_token is not None,
                control_token_header=CONTROL_TOKEN_HEADER,
                source_kind=source_kind,
                shapes_csv=self.overlay_config.get('shapes_csv', ''),
            )

        @self.flask_app.route('/api/overlay/shapes')
        def overlay_shapes():
            """Static shape geometry, fetched once per page load."""
            if not self.overlay_enabled:
                return jsonify({'error': 'Overlay is disabled'}), 404
            if self._shape_config is None:
                return jsonify({
                    'error': f'Shape config unavailable: {self._shape_error}',
                }), 503
            return jsonify(shapes_payload(self._shape_config))

        @self.flask_app.route('/api/overlay/state')
        def overlay_state():
            """Per-shape status, polled at the dashboard's update interval."""
            if not self.overlay_enabled:
                return jsonify({'error': 'Overlay is disabled'}), 404
            if self._shape_config is None:
                return jsonify({
                    'error': f'Shape config unavailable: {self._shape_error}',
                }), 503
            return jsonify({
                'statuses': resolve_all(self._shape_config, self._build_status()),
                'timestamp': time.time(),
            })

        @self.flask_app.route('/api/overlay/background')
        def overlay_background():
            """Single background image (still, or latest live frame)."""
            denied = self._check_video_access()
            if denied:
                return denied
            if not self.overlay_enabled:
                return jsonify({'error': 'Overlay is disabled'}), 404
            if self._background_source is None:
                return jsonify({
                    'error': ('Background source unavailable: '
                              f'{self._background_error}'),
                }), 503

            image = self._background_source.get_image()
            if image is None:
                return jsonify({'error': 'No background image available'}), 503

            data, content_type = image
            response = Response(data, mimetype=content_type)
            # The still is re-read on mtime change; never let a proxy or the
            # browser pin a stale calibration frame.
            response.headers['Cache-Control'] = 'no-store'
            return response

        @self.flask_app.route('/api/overlay/stream')
        def overlay_stream():
            """MJPEG stream — live source only.

            Holds its worker thread for the life of the connection; Flask's
            dev server runs threaded, so the dashboard's 250 ms polling is
            unaffected by a viewer sitting on this route.
            """
            denied = self._check_video_access()
            if denied:
                return denied
            if not self.overlay_enabled:
                return jsonify({'error': 'Overlay is disabled'}), 404
            if self._background_source is None or \
                    not self._background_source.supports_stream():
                return jsonify({
                    'error': ('Streaming is not available for this background '
                              'source; use /api/overlay/background'),
                }), 404

            # The source yields raw JPEG bytes; the multipart framing is HTTP
            # concern and stays here, so overlay/source.py needs no Flask.
            frames = self._background_source.mjpeg_frames()

            def generate():
                for jpeg in frames:
                    yield (b'--' + MJPEG_BOUNDARY + b'\r\n'
                           b'Content-Type: image/jpeg\r\n'
                           b'Content-Length: ' + str(len(jpeg)).encode() +
                           b'\r\n\r\n' + jpeg + b'\r\n')

            return Response(
                generate(),
                mimetype=('multipart/x-mixed-replace; boundary='
                          + MJPEG_BOUNDARY.decode()),
                headers={'Cache-Control': 'no-store'},
            )

        @self.flask_app.route('/api/stats')
        def get_stats():
            """Get SNMP client statistics."""
            if self.app_instance.snmp_client:
                return jsonify(self.app_instance.snmp_client.get_stats())
            return jsonify({'error': 'No SNMP client'})
        
        @self.flask_app.route('/api/control/time', methods=['POST'])
        def sync_time():
            """Sync controller time to system time."""
            denied = self._check_control_access()
            if denied:
                return denied
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
            denied = self._check_control_access()
            if denied:
                return denied
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
            denied = self._check_control_access()
            if denied:
                return denied
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

        if self.overlay_enabled:
            shapes = (len(self._shape_config.shapes)
                      if self._shape_config is not None else 0)
            print(f"  Video overlay at /overlay ({shapes} shapes, "
                  f"background: {self.overlay_config.get('background', 'file')})")
            if self._shape_config is None:
                print(f"    WARNING: shapes unavailable — {self._shape_error}")
            if self._background_source is None:
                print(f"    WARNING: background unavailable — {self._background_error}")

        if self.control_token is not None:
            print(f"  Control endpoints require the {CONTROL_TOKEN_HEADER} header")
        elif self._loopback_only:
            print("  Control endpoints: open (loopback-only bind, no token set)")
        else:
            warning = (
                f"WARNING: bound to {self.host} (not loopback) with no "
                "control token — /api/control/* and the overlay's camera "
                "imagery are DISABLED. Set web_ui.control_token to enable "
                "hardware control and video here."
            )
            print(f"  {warning}")
            logger.warning(
                '{"event":"control_disabled_on_exposed_bind","host":"%s"}',
                self.host,
            )
    
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
        if self._background_source is not None:
            # The file source has nothing to release; the live source of Item
            # 11c holds an RTSP session and a decoder thread.
            self._background_source.close()
        # Note: Flask doesn't have a clean shutdown method when run in thread
        # In production, use Werkzeug's Server.shutdown() or run in separate process
