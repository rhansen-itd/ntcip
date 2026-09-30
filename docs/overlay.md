# Live video overlay and deploy tools

Moved verbatim from `CLAUDE.md` on 2026-09-30 (source `93addce`, lines 715-804). Read before changing ui/overlay/, tools/, or calibration. Paths are relative to the repo root. This is the detailed reference; `CLAUDE.md` keeps a short digest. When a convention here changes, update both.

<!-- verbatim: CLAUDE.md@93addce L715-804 -->

## Live video overlay (2026-07-31, ROADMAP 11a–11c — load-bearing)

`GET /overlay` draws pyatspm-calibrated detector loops and stopbars on a
`<canvas>` over a camera image, recolored from the live monitor state. Config
lives in `config.json`'s `overlay` section (`enabled`, `shapes_csv`,
`background`, `image_path`, `camera_url`, `stream_fps`, plus the optional
`stream_quality` and `rtsp_transport`); absent or `enabled: false` means every
overlay route answers 404. Deployment data for intersection 201 is in
`overlay/` at the repo root. The shipped config uses `background: "file"`
because `camera_url` is empty until ROADMAP 11d authors it.

- **`ntcip_monitor/ui/overlay/` imports nothing heavy.** `shapes.py` (vendored
  from pyatspm — see the module docstring for the four deliberate deviations),
  `status.py`, and `source.py` are stdlib-only apart from one guarded
  `import av` in `source.py` (`try/except ImportError`, touched only on the
  live path): no Flask, no cv2, no `atspm`, no `video_engine`, no monitor
  imports. That is what keeps 86 unit tests runnable on a bare interpreter
  (`python3 ntcip_monitor/tests/test_overlay_shapes.py`) — the live source's
  three PyAV seams (`_open_container` / `_decode` / `_encode_jpeg`) are
  overridable precisely so its threading is testable without a camera. Flask
  lives only in `web_ui.py`, including the MJPEG multipart framing.
- **The live source shares one decoder per camera** (`RtspMjpegSource`,
  `background: "live"`). Viewers are **ref-counted subscribers** — a stream
  generator for its lifetime, a `/api/overlay/background` request for one
  frame — and the decoder thread opens on the first and retires
  `idle_grace_sec` (10 s) after the last. N tabs cost the intersection one
  RTSP session, an idle page costs none. Bookkeeping follows the same lock
  discipline as the remux manager (decide/collect under the lock, act after
  releasing; never hold it across a connect, decode, encode, or socket write),
  and each decoder thread carries a `_DecoderSession` liveness token so a
  retiring thread can never stop its successor. Frames are decoded at the
  source rate but encoded only at `stream_fps` — encoding is the expensive
  half. JPEG quality comes from the encoder's `qmin`/`qmax`
  (`overlay.stream_quality`, 1 best–31 worst, default 12); FFmpeg's
  `-q:v`/`qscale` options are ignored by this encoder (verified, don't retry
  them).
- **Shape CSV colors are BGR** (OpenCV order, as pyatspm authors them):
  `"255,0,0"` is *blue*. `shapes.bgr_to_rgb()` reverses the triple exactly
  once, inside `shapes_payload()` on the way to `/api/overlay/shapes`; the
  loaded shapes keep the authored order. Don't reverse again in the page.
- **Three routes are open, two are gated.** `/api/overlay/shapes` (static,
  fetched once), `/api/overlay/state` (the fallback poll) and
  `/api/overlay/events` (the SSE stream, ROADMAP 15) are open like
  `/api/status`. `/api/overlay/background` and `/api/overlay/stream` carry the
  **same interlock as `/api/control/*`** — a deliberate departure from 4f,
  because proxied camera video is a live view of a public roadway and
  `--web-host 0.0.0.0` shouldn't publish it by accident. The video routes also
  accept `?token=` (an `<img>` can't set a header); control is header-only.
- **The canvas does all the scaling.** `canvas.width/height` = the config's
  `video_width/video_height`; shapes are drawn in native calibration
  coordinates; canvas and background are stacked at `width:100%`. No
  coordinate math in the page — don't add any.
- **Every failure degrades to a 503 on one route**, never a crash: a missing
  CSV, an unreadable image, or an unreachable camera leaves the dashboard and
  the rest of the page working. `FileImageSource` re-reads on mtime/size
  change, so swapping the calibration still needs no restart; the live source
  reconnects with 1 s→30 s backoff and keeps re-sending the last good frame
  every 2 s so a viewer's `<img>` doesn't break mid-outage.
- The page **labels its own resolution** — SNMP sampling is ~0.33 s effective
  on intersection 201 post-4a (see the NTCIP rules above; the note said
  "1–1.5 s" until 2026-08-03), still far coarser than the video. Keep that
  caveat if you touch the template: since 15b the state is *pushed*, so the
  sampling cycle is the whole of what remains.

### Deploy-time tooling and the calibration workflow (ROADMAP 11d)

`tools/` at the repo root holds **deploy-time** scripts that belong to neither
package — the same role `video_engine/system_runner.py` plays at runtime.
They may import `ntcip_monitor`; they are never imported by it, and nothing in
them relaxes the rule that the two packages don't import each other.

- **`tools/sync_ui_config.py`** is the de-duplication mechanism for values that
  live in both config files. `video_engine/intersections/` is the
  authoring source; the script writes `controller.ip/port/community/chunk_size`
  and `overlay.camera_url` into `config.json`. **Dry run by default** (`--apply`
  to write), credentials masked in its output, atomic replace, idempotent.
  Poll intervals, timezone and `web_ui.*` are deliberately *not* synced — the
  monitor tunes four monitors separately, and bind host/port/token are
  properties of the host you run the UI on, not of the intersection.
- **`tools/grab_calibration_still.py`** saves one frame as a JPEG, resolving
  the URL from a `--intersection`/`--camera` pair or taking it directly. It
  grabs through the overlay's own `RtspMjpegSource`, so a successful grab is
  also proof the live overlay path can reach that camera.
- **Calibration workflow** (no ntcip code involved in step 2): grab a still →
  run pyatspm's `atspm video-calibrate-shapes --camera <name> --video <still>`
  against it (only the first frame is used; record a short clip with
  `video_engine/tools/__capture_rtsp.py` if OpenCV won't open the JPEG) → copy
  the CSV it writes to `overlay.shapes_csv`. 11a's reader accepts either format
  pyatspm produces. A browser-based calibrator would drop the pyatspm/Tkinter
  dependency entirely; it's parked in ROADMAP's Future section.
