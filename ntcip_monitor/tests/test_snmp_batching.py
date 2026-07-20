"""Unit tests for SNMP chunking and the batched monitor poll loops (ROADMAP 4a).

Runs without pysnmp installed: a minimal stub of ``pysnmp.hlapi`` is injected
into ``sys.modules`` before ``ntcip_monitor`` is imported, and the stub's
``getCmd`` records how many OIDs each PDU carried so chunking behavior is
directly observable.

Run from anywhere:

    python3 ntcip_monitor/tests/test_snmp_batching.py
"""

from __future__ import annotations

import sys
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


if __name__ == "__main__":
    unittest.main()
