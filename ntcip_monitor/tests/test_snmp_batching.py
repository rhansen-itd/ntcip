"""Unit tests for SNMP chunking, the batched monitor poll loops (ROADMAP 4a),
and the effective-cycle self-measurement (ROADMAP 9 item A).

Runs without pysnmp installed: a minimal stub of ``pysnmp.hlapi`` is injected
into ``sys.modules`` before ``ntcip_monitor`` is imported, and the stub's
``getCmd`` records how many OIDs each PDU carried so chunking behavior is
directly observable.

Run from anywhere:

    python3 ntcip_monitor/tests/test_snmp_batching.py
"""

from __future__ import annotations

import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ---------------------------------------------------------------------------
# pysnmp.hlapi stub — must exist before ntcip_monitor imports resolve.
# ---------------------------------------------------------------------------

_PDU_LOG: list = []          # list of lists: the OID strings in each PDU sent
_OID_VALUES: dict = {}       # oid string -> int value the fake controller returns


class _ObjectIdentity:
    def __init__(self, oid):
        self.oid = str(oid)


class _ObjectType:
    def __init__(self, identity, value=None):
        self.identity = identity
        self.value = value


def _getCmd(engine, community, transport, context, *object_types):
    oids = [ot.identity.oid for ot in object_types]
    _PDU_LOG.append(oids)
    varbinds = [(oid, _OID_VALUES.get(oid, 0)) for oid in oids]
    yield None, 0, 0, varbinds  # (errorIndication, errorStatus, errorIndex, varBinds)


def _setCmd(*args, **kwargs):  # pragma: no cover - not exercised here
    yield None, 0, 0, []


def _install_stub():
    hlapi = types.ModuleType("pysnmp.hlapi")
    hlapi.getCmd = _getCmd
    hlapi.setCmd = _setCmd
    for name in ("CommunityData", "UdpTransportTarget", "ContextData",
                 "SnmpEngine", "Integer32", "Counter32", "Unsigned32",
                 "Gauge32"):
        setattr(hlapi, name, lambda *a, _n=name, **k: (_n, a, k))
    hlapi.ObjectType = _ObjectType
    hlapi.ObjectIdentity = _ObjectIdentity
    pysnmp = types.ModuleType("pysnmp")
    pysnmp.hlapi = hlapi
    sys.modules.setdefault("pysnmp", pysnmp)
    sys.modules["pysnmp.hlapi"] = hlapi


_install_stub()

from ntcip_monitor.core.snmp_client import EconoliteSNMPClient  # noqa: E402
from ntcip_monitor.core.oid_definitions import (  # noqa: E402
    DETECTOR_GROUPS, OUTPUT_OIDS,
)
from ntcip_monitor.core.event_monitor import _CYCLE_EMA_ALPHA  # noqa: E402
from ntcip_monitor.monitors.detector_monitor import DetectorMonitor  # noqa: E402
from ntcip_monitor.monitors.output_monitor import OutputMonitor  # noqa: E402


def _reset(values=None):
    _PDU_LOG.clear()
    _OID_VALUES.clear()
    if values:
        _OID_VALUES.update(values)


class TestClientChunking(unittest.TestCase):
    def test_default_chunk_is_one_oid_per_pdu(self):
        _reset({oid: i for i, oid in enumerate(DETECTOR_GROUPS)})
        client = EconoliteSNMPClient("1.2.3.4")
        values = client.get(*DETECTOR_GROUPS)
        self.assertEqual([len(p) for p in _PDU_LOG], [1] * 8)
        self.assertEqual(values, list(range(8)))  # order preserved

    def test_chunk_size_splits_and_preserves_order(self):
        _reset({oid: i * 10 for i, oid in enumerate(DETECTOR_GROUPS)})
        client = EconoliteSNMPClient("1.2.3.4", chunk_size=3)
        values = client.get(*DETECTOR_GROUPS)
        self.assertEqual([len(p) for p in _PDU_LOG], [3, 3, 2])
        self.assertEqual(values, [0, 10, 20, 30, 40, 50, 60, 70])

    def test_chunk_size_eight_is_single_pdu(self):
        _reset({oid: 1 for oid in DETECTOR_GROUPS})
        client = EconoliteSNMPClient("1.2.3.4", chunk_size=8)
        client.get(*DETECTOR_GROUPS)
        self.assertEqual([len(p) for p in _PDU_LOG], [8])

    def test_single_oid_returns_scalar(self):
        _reset({DETECTOR_GROUPS[0]: 42})
        client = EconoliteSNMPClient("1.2.3.4")
        self.assertEqual(client.get(DETECTOR_GROUPS[0]), 42)

    def test_invalid_chunk_size_clamps_to_one(self):
        client = EconoliteSNMPClient("1.2.3.4", chunk_size=0)
        self.assertEqual(client.chunk_size, 1)

    def test_reads_counts_calls_not_oids(self):
        _reset({oid: 0 for oid in DETECTOR_GROUPS})
        client = EconoliteSNMPClient("1.2.3.4")
        client.get(*DETECTOR_GROUPS)
        self.assertEqual(client.stats["reads"], 1)


class TestDetectorMonitorBatching(unittest.TestCase):
    def test_poll_issues_one_get_and_emits_events(self):
        # Detectors 1-16 -> groups 0,1. Group values: det 3 ON (bit 2),
        # det 9 ON (bit 0 of group 1).
        _reset({DETECTOR_GROUPS[0]: 0b100, DETECTOR_GROUPS[1]: 0b1})
        client = EconoliteSNMPClient("1.2.3.4")
        mon = DetectorMonitor(client, detector_range=(1, 17))
        seen = []
        mon.on("detector_on", lambda det: seen.append(det))
        mon._poll()
        # One client call, two single-OID PDUs (chunk stays 1 on the wire).
        self.assertEqual(client.stats["reads"], 1)
        self.assertEqual([len(p) for p in _PDU_LOG], [1, 1])
        self.assertEqual(sorted(seen), [3, 9])

    def test_range_limits_groups_polled(self):
        # Intersection 201 shape: detectors 2..46 -> groups 1..6 only.
        _reset({oid: 0 for oid in DETECTOR_GROUPS})
        client = EconoliteSNMPClient("1.2.3.4")
        mon = DetectorMonitor(client, detector_range=(2, 47))
        mon._poll()
        polled = [p[0] for p in _PDU_LOG]
        self.assertEqual(polled, DETECTOR_GROUPS[0:6])

    def test_off_edge_after_on(self):
        _reset({DETECTOR_GROUPS[0]: 0b1})
        client = EconoliteSNMPClient("1.2.3.4")
        mon = DetectorMonitor(client, detector_range=(1, 9))
        mon._poll()  # first poll broadcasts initial states; subscribe after
        offs = []
        mon.on("detector_off", lambda det: offs.append(det))
        _OID_VALUES[DETECTOR_GROUPS[0]] = 0
        mon._poll()
        self.assertEqual(offs, [1])


class TestOutputMonitorBatching(unittest.TestCase):
    def test_poll_issues_one_get_for_all_outputs(self):
        _reset({oid: 0 for oid in OUTPUT_OIDS})
        _OID_VALUES[OUTPUT_OIDS[4]] = 1  # output 5 ON
        client = EconoliteSNMPClient("1.2.3.4")
        mon = OutputMonitor(client)
        ons = []
        mon.on("output_on", lambda n: ons.append(n))
        mon._poll()
        self.assertEqual(client.stats["reads"], 1)
        self.assertEqual([len(p) for p in _PDU_LOG], [1] * 16)
        self.assertEqual(ons, [5])


class TestEffectiveCycleMeasurement(unittest.TestCase):
    """ROADMAP 9 item A — the monitor measures its own sampling cycle.

    ``poll_interval`` is only the sleep between sweeps; at chunk_size 1 the
    sweep itself is 8 sequential round trips, so the cycle a downstream
    consumer must respect is measured, not configured.
    """

    LOGGER = "ntcip_monitor.core.event_monitor"

    def _monitor(self, poll_interval=0.2):
        _reset({oid: 0 for oid in DETECTOR_GROUPS})
        client = EconoliteSNMPClient("1.2.3.4")
        return DetectorMonitor(
            client, poll_interval=poll_interval, detector_range=(1, 9)
        )

    def test_unmeasured_before_first_cycle(self):
        mon = self._monitor()
        # 0.0 is the documented "not measured yet" sentinel — consumers fall
        # back to their configured assumption rather than trusting it.
        self.assertEqual(mon.effective_cycle_sec(), 0.0)
        self.assertEqual(mon.get_stats()["cycles"], 0)

    def test_first_sample_seeds_ema(self):
        mon = self._monitor()
        mon._record_cycle(1.5)
        self.assertAlmostEqual(mon.effective_cycle_sec(), 1.5)

    def test_ema_blends_subsequent_samples(self):
        mon = self._monitor()
        mon._record_cycle(1.5)
        mon._record_cycle(2.5)
        expected = _CYCLE_EMA_ALPHA * 2.5 + (1.0 - _CYCLE_EMA_ALPHA) * 1.5
        self.assertAlmostEqual(mon.effective_cycle_sec(), expected)
        self.assertEqual(mon.get_stats()["cycles"], 2)

    def test_get_stats_exposes_measured_and_configured_rates(self):
        mon = self._monitor(poll_interval=0.2)
        mon._record_cycle(1.53)  # the 2026-07-19 measured median
        stats = mon.get_stats()
        self.assertEqual(stats["name"], "DetectorMonitor")
        self.assertEqual(stats["poll_interval_sec"], 0.2)
        self.assertAlmostEqual(stats["effective_cycle_sec"], 1.53)

    def test_slow_sweep_logs_once_per_interval(self):
        mon = self._monitor(poll_interval=0.2)  # budget = 0.4 s
        with self.assertLogs(self.LOGGER, level="INFO") as captured:
            mon._record_cycle(1.53)
            mon._record_cycle(1.53)  # rate-limited: must not log again
        self.assertEqual(len(captured.records), 1)
        record = captured.records[0]
        self.assertEqual(record.monitor, "DetectorMonitor")
        self.assertAlmostEqual(record.poll_interval_sec, 0.2)
        self.assertGreater(record.effective_cycle_sec, 0.4)

    def test_no_slow_sweep_log_within_budget(self):
        mon = self._monitor(poll_interval=1.0)  # budget = 2.0 s
        mon._record_cycle(1.53)
        # White-box on purpose: assertNoLogs needs Python 3.10+, and this
        # suite must run on whatever stdlib the edge box ships.
        self.assertEqual(mon._last_slow_sweep_log, 0.0)

    def test_run_loop_measures_real_cycles(self):
        mon = self._monitor(poll_interval=0.01)
        mon.start()
        try:
            time.sleep(0.15)
        finally:
            mon.stop()
        self.assertGreater(mon.get_stats()["cycles"], 0)
        self.assertGreater(mon.effective_cycle_sec(), 0.0)


if __name__ == "__main__":
    unittest.main()
