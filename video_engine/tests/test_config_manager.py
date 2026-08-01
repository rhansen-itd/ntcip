"""Unit tests for ``config_manager``'s pure pieces (ROADMAP 4d).

Currently covers :class:`ConfigProviderError` — the module's single custom
exception, whose whole job is to carry a message and preserve the underlying
cause so callers need only catch one type.

The provider classes themselves (``JsonFileConfigProvider``,
``SqliteCentralConfigProvider``) need fixtures and are ROADMAP 4e's work; this
file is the place for them when that lands.

Run from anywhere:

    python3 video_engine/tests/test_config_manager.py
"""

from __future__ import annotations

import pickle
import sys
import unittest
from pathlib import Path

# Bootstrap: make video_engine/ importable regardless of working directory
# (same pattern as the tools/ scripts and test_discrepancy_rules.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_manager import ConfigProviderError  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
