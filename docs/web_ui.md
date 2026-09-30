# Web UI exposure and SSE push

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 645-713). Read before changing web_ui.py routes, auth, or ui/events.py. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L645-713 -->

## Web UI exposure (2026-07-31, ROADMAP 4f — load-bearing)

`ntcip_monitor/ui/web_ui.py` is an operator tool, not a service, and its
`/api/control/*` routes drive real signal hardware (time sync, vehicle calls,
output toggles). Two rules, both implemented:

- **Bind host defaults to `127.0.0.1`.** Override with `--web-host` (run.py) or
  `web_ui.host` in config — CLI beats config beats the default; `web_ui.port`
  resolves the same way. Don't restore a `0.0.0.0` default.
- **Control endpoints are gated by a shared secret** in the
  `X-NTCIP-Control-Token` header (`hmac.compare_digest`, compared as bytes),
  read from `$NTCIP_WEB_CONTROL_TOKEN` then `web_ui.control_token`. Policy:
  token set → header must match (401); no token + loopback bind → allowed;
  no token + non-loopback bind → **403, control disabled** plus a startup
  warning. The two rules interlock on purpose — exposing hardware control to
  the network takes both a host change and a secret. `/api/status`,
  `/api/stats` and the two SSE routes below stay open (read-only).

Both rules are implemented in one place: `_check_shared_secret()`, which
`_check_control_access()` and the overlay's `_check_video_access()` both call.

Deliberately not a session/user/JWT system — a reverse proxy owns real auth if
the deployment story changes. There's still no in-repo route test (a
Flask-test-client case is ROADMAP 4e), though `flask` and `pysnmp` were
installed here during 11b and the routes were verified from a scratch harness
(again in 15b, against a real Flask test client).

### Pushed state updates (2026-08-03, ROADMAP 15 — load-bearing)

Neither page polls any more. `/api/events` (raw state, dashboard) and
`/api/overlay/events` (resolved shape statuses, overlay) are Server-Sent
Events streams that push a change when the SNMP sweep detects it, removing the
0–250 ms poll phase from perceived latency; what is left is the ~0.33 s
sampling cycle. The plumbing is `ntcip_monitor/ui/events.py` —
**stdlib-only and Flask-free**, like `ui/overlay/*`, with the `EVENT_*` names
as string literals because `core/__init__.py` re-exports `snmp_client` and
would drag pysnmp into a bare-interpreter suite.

Six things are load-bearing:

- **The dev server runs `threaded=True`** (15a). It is single-threaded by
  default, so one MJPEG viewer or one SSE client — neither response ever
  finishes — would occupy the only worker and stall every other request.
  `remux`'s and the overlay source's docstrings had *assumed* this since 11c;
  it wasn't true until now. Don't remove it.
- **A monitor callback only enqueues.** `StateBroadcaster._dispatch` reads the
  enum's name and does one `put_nowait`; every payload (`_build_status()`,
  `resolve_all`) is built on the HTTP worker thread serving the stream. This is
  the "callbacks return in microseconds" rule from the NTCIP section — a
  browser's view of the world must never be a term in the sampling rate.
- **Overflow drops and resynchronises; it never back-pressures.** Each client
  owns a 256-entry queue. When it fills, pushes are dropped, the backlog is
  discarded, and a full snapshot is sent — the queued items are the *oldest*
  changes, so replaying them after a gap is worse than one snapshot.
- **Subscriptions attach once and are never detached** — deliberately unlike
  `overlay/source.py`'s ref-counted decoder, because an idle RTSP session is a
  real resource and an idle callback is a lock plus an empty list. Don't add
  ref-counting: its failure mode is a silently deaf stream.
- **The overlay gets its own route because resolution stays server-side.** The
  page consumes a positional `statuses` array from `overlay/status.py`; it does
  **not** map detectors to shapes in JS, and porting that table into the
  browser to consume raw deltas would duplicate the one piece of overlay logic
  the unit tests pin. The stream re-resolves per edge and sends the whole array;
  a change that resolves identically (a detector no shape maps) sends nothing.
- **Both pages keep the 250 ms poll as a fallback**, started on `onerror` and
  retired on `onopen`. `EventSource` reconnects itself and every connection
  opens with a full snapshot, so the fallback only covers the gap. A delta is
  shaped as the subset of a status payload it replaces, so `updateDisplay()`
  applies snapshot and delta through one path.
