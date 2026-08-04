"""Unit tests for ``config_manager``'s pure pieces (ROADMAP 4d) and for
``JsonFileConfigProvider``'s file-or-directory loading (ROADMAP 2).

Covers :class:`ConfigProviderError` — the module's single custom exception,
whose whole job is to carry a message and preserve the underlying cause so
callers need only catch one type — and the JSON provider's two accepted path
shapes, which need only a ``tempfile`` directory rather than a mock.

``SqliteCentralConfigProvider`` and the rest of the JSON provider's surface
remain ROADMAP 4e's work; this file is the place for them when that lands.

Run from anywhere:

    python3 video_engine/tests/test_config_manager.py
"""

from __future__ import annotations

import json
import pickle
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts and test_discrepancy_rules.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_manager import ConfigProviderError, JsonFileConfigProvider  # noqa: E402


class TestConfigProviderError(unittest.TestCase):
    """A RuntimeError subclass that optionally records its cause."""

    def test_is_a_runtime_error(self):
        # Callers catch this alongside KeyError; the base class is what makes
        # a bare `except RuntimeError` still work.
        self.assertTrue(issubclass(ConfigProviderError, RuntimeError))
        self.assertIsInstance(ConfigProviderError("x"), Exception)

    def test_message_reaches_args_and_str(self):
        exc = ConfigProviderError("config file is unreadable")
        self.assertEqual(exc.args, ("config file is unreadable",))
        self.assertEqual(str(exc), "config file is unreadable")

    def test_cause_defaults_to_none(self):
        self.assertIsNone(ConfigProviderError("no cause").__cause__)

    def test_cause_is_preserved_when_given(self):
        original = ValueError("bad json at line 3")
        exc = ConfigProviderError("could not parse config", original)
        self.assertIs(exc.__cause__, original)
        # The wrapper's own message must not be replaced by the cause's.
        self.assertEqual(str(exc), "could not parse config")

    def test_cause_accepts_any_base_exception(self):
        # The annotation is BaseException, so a KeyboardInterrupt-class cause
        # is legal and must not be coerced or dropped.
        cause = KeyboardInterrupt()
        self.assertIs(ConfigProviderError("interrupted", cause).__cause__, cause)

    def test_explicit_none_cause_is_accepted(self):
        self.assertIsNone(ConfigProviderError("x", None).__cause__)

    def test_raise_from_still_wins(self):
        # Constructor-set cause is a convenience, not a lock: an explicit
        # `raise ... from ...` at the call site must still take effect.
        outer = ConfigProviderError("wrapped", ValueError("first"))
        replacement = OSError("second")
        try:
            try:
                raise outer from replacement
            except ConfigProviderError as exc:
                self.assertIs(exc.__cause__, replacement)
        finally:
            pass

    def test_catchable_as_itself(self):
        with self.assertRaises(ConfigProviderError):
            raise ConfigProviderError("boom")

    def test_survives_pickling(self):
        # Central deployments may hand config errors across process
        # boundaries; a two-arg __init__ over a one-arg super() is the classic
        # way to break that, so pin it.
        restored = pickle.loads(pickle.dumps(ConfigProviderError("boom")))
        self.assertIsInstance(restored, ConfigProviderError)
        self.assertEqual(str(restored), "boom")


def _block(iid: str, camera: str = "cam1", detector: str = "1") -> dict:
    """Build a minimal intersection block that passes ``_validate_intersection``.

    Args:
        iid: Intersection ID, used for both the key and ``intersection_id``.
        camera: Camera ID to define and reference.
        detector: Detector ID to define.

    Returns:
        dict: One intersection block.
    """
    return {
        "intersection_id": iid,
        "controller_ip": "10.0.0.1",
        "snmp_port": 501,
        "timezone": "US/Mountain",
        "cameras": {camera: {"url": f"rtsp://10.0.0.2/{camera}"}},
        "detectors": {
            detector: {"type": "radar", "phase": 2, "camera_id": camera},
        },
    }


class TestJsonFileConfigProviderPaths(unittest.TestCase):
    """The provider accepts a single file or a directory of them (ROADMAP 2).

    Both shapes must produce the same namespace, and a directory must never
    resolve a duplicate intersection ID silently — an edge box loading the
    wrong site's controller IP because two files disagreed is exactly the
    failure the merge is supposed to prevent.
    """

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def _write(self, name: str, payload: dict) -> Path:
        path = self.tmp / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    # ── single file (the pre-existing shape) ──────────────────────────────

    def test_single_file_still_loads(self):
        path = self._write("intersections.json", {"201": _block("201")})
        provider = JsonFileConfigProvider(path)
        self.assertEqual(provider.list_intersection_ids(), ["201"])
        self.assertEqual(
            provider.get_intersection_config("201")["controller_ip"], "10.0.0.1"
        )

    def test_single_file_may_hold_several_blocks(self):
        path = self._write(
            "all.json", {"201": _block("201"), "701": _block("701")}
        )
        provider = JsonFileConfigProvider(path)
        self.assertEqual(provider.list_intersection_ids(), ["201", "701"])

    # ── directory ─────────────────────────────────────────────────────────

    def test_directory_merges_one_file_per_intersection(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "701.json").write_text(json.dumps({"701": _block("701")}))
        provider = JsonFileConfigProvider(d)
        self.assertEqual(provider.list_intersection_ids(), ["201", "701"])
        self.assertEqual(
            provider.get_intersection_config("701")["intersection_id"], "701"
        )

    def test_directory_and_single_file_agree(self):
        # The two shapes are alternate spellings of one namespace, not two
        # different features; pin that they cannot drift apart.
        d = self.tmp / "split"
        d.mkdir()
        (d / "a.json").write_text(json.dumps({"201": _block("201")}))
        (d / "b.json").write_text(json.dumps({"701": _block("701")}))
        merged = self._write("merged.json", {"201": _block("201"), "701": _block("701")})

        from_dir = JsonFileConfigProvider(d)
        from_file = JsonFileConfigProvider(merged)
        self.assertEqual(
            from_dir.list_intersection_ids(), from_file.list_intersection_ids()
        )
        for iid in from_dir.list_intersection_ids():
            self.assertEqual(
                from_dir.get_intersection_config(iid),
                from_file.get_intersection_config(iid),
            )

    def test_directory_is_not_recursive(self):
        # A nested directory is not a config namespace — a stray backup or an
        # unrelated data folder underneath must not be silently loaded.
        d = self.tmp / "intersections"
        (d / "nested").mkdir(parents=True)
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "nested" / "999.json").write_text(json.dumps({"999": _block("999")}))
        self.assertEqual(
            JsonFileConfigProvider(d).list_intersection_ids(), ["201"]
        )

    def test_directory_ignores_non_json_files(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "README.md").write_text("not config")
        (d / "old.json.bak").write_text("{ broken")
        self.assertEqual(
            JsonFileConfigProvider(d).list_intersection_ids(), ["201"]
        )

    # ── failure modes ─────────────────────────────────────────────────────

    def test_duplicate_intersection_across_files_raises(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "201_copy.json").write_text(json.dumps({"201": _block("201")}))
        with self.assertRaises(ConfigProviderError) as ctx:
            JsonFileConfigProvider(d)
        # Both filenames must appear — the whole point is to say which two.
        self.assertIn("201.json", str(ctx.exception))
        self.assertIn("201_copy.json", str(ctx.exception))

    def test_empty_directory_raises(self):
        d = self.tmp / "empty"
        d.mkdir()
        with self.assertRaises(ConfigProviderError):
            JsonFileConfigProvider(d)

    def test_missing_path_raises(self):
        with self.assertRaises(ConfigProviderError):
            JsonFileConfigProvider(self.tmp / "nope")

    def test_one_bad_file_in_a_directory_raises_naming_it(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "701.json").write_text("{ not json")
        with self.assertRaises(ConfigProviderError) as ctx:
            JsonFileConfigProvider(d)
        self.assertIn("701.json", str(ctx.exception))

    def test_validation_failure_names_the_offending_file(self):
        d = self.tmp / "intersections"
        d.mkdir()
        bad = _block("701")
        del bad["controller_ip"]
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "701.json").write_text(json.dumps({"701": bad}))
        with self.assertRaises(ConfigProviderError) as ctx:
            JsonFileConfigProvider(d)
        self.assertIn("701.json", str(ctx.exception))

    def test_failed_reload_leaves_previous_config_intact(self):
        # Nothing is published until every file parses, so a bad edit during
        # commissioning must not empty out a running provider.
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        provider = JsonFileConfigProvider(d)
        (d / "701.json").write_text("{ broken")
        with self.assertRaises(ConfigProviderError):
            provider.reload()
        self.assertEqual(provider.list_intersection_ids(), ["201"])

    # ── source tracking ───────────────────────────────────────────────────

    def test_source_path_reports_the_owning_file(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "701.json").write_text(json.dumps({"701": _block("701")}))
        provider = JsonFileConfigProvider(d)
        self.assertEqual(provider.source_path("701").name, "701.json")

    def test_source_path_on_a_single_file_is_that_file(self):
        path = self._write("all.json", {"201": _block("201"), "701": _block("701")})
        provider = JsonFileConfigProvider(path)
        self.assertEqual(provider.source_path("201"), path)
        self.assertEqual(provider.source_path("701"), path)

    def test_source_path_unknown_id_raises_keyerror(self):
        path = self._write("all.json", {"201": _block("201")})
        with self.assertRaises(KeyError):
            JsonFileConfigProvider(path).source_path("999")

    def test_reload_picks_up_a_new_file(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        provider = JsonFileConfigProvider(d)
        self.assertEqual(provider.list_intersection_ids(), ["201"])
        (d / "701.json").write_text(json.dumps({"701": _block("701")}))
        provider.reload()
        self.assertEqual(provider.list_intersection_ids(), ["201", "701"])

    def test_reload_drops_a_removed_file(self):
        d = self.tmp / "intersections"
        d.mkdir()
        (d / "201.json").write_text(json.dumps({"201": _block("201")}))
        (d / "701.json").write_text(json.dumps({"701": _block("701")}))
        provider = JsonFileConfigProvider(d)
        (d / "701.json").unlink()
        provider.reload()
        self.assertEqual(provider.list_intersection_ids(), ["201"])
        with self.assertRaises(KeyError):
            provider.source_path("701")


class TestShippedIntersectionConfigs(unittest.TestCase):
    """The repository's own ``video_engine/intersections/`` must load.

    A config directory that fails to load is a deployment outage, and it is
    exactly the kind of breakage a rename or a stray file introduces silently.
    """

    CONFIG_DIR = Path(__file__).resolve().parent.parent / "intersections"

    def test_shipped_directory_loads_both_sites(self):
        provider = JsonFileConfigProvider(self.CONFIG_DIR)
        self.assertEqual(provider.list_intersection_ids(), ["201", "701"])

    def test_each_site_lives_in_its_own_file(self):
        provider = JsonFileConfigProvider(self.CONFIG_DIR)
        for iid in provider.list_intersection_ids():
            self.assertEqual(provider.source_path(iid).name, f"{iid}.json")

    def test_every_pair_link_resolves(self):
        # A dangling paired_detector_id is not caught by schema validation but
        # silently costs a comparison the engine was configured to make.
        provider = JsonFileConfigProvider(self.CONFIG_DIR)
        for iid in provider.list_intersection_ids():
            detectors = provider.get_intersection_config(iid)["detectors"]
            for det_id, det in detectors.items():
                linked = det.get("paired_detector_id")
                if linked is None:
                    continue
                for other in ([linked] if isinstance(linked, (str, int)) else linked):
                    self.assertIn(
                        str(other), detectors,
                        f"{iid}: detector {det_id} pairs with unknown {other}",
                    )


if __name__ == "__main__":
    unittest.main()
