"""
Ring Monitor – monitors ringStatus for one or more controller rings.

Emits:
    ``EVENT_RING_STATE_CHANGE``  when the coded state (bits 0-2) transitions.
    ``EVENT_RING_TERMINATION``   on the **rising edge** (0->1) of any
                                 termination bit (bits 3-5) to avoid spam.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from ..core.data_models import RingState, RingStatus, TerminationReason, parse_ring_status
from ..core.event_monitor import (
    BaseMonitor,
    EVENT_RING_STATE_CHANGE,
    EVENT_RING_TERMINATION,
)
from ..core.oid_definitions import get_ring_status_oid
from ..core.snmp_client import SNMPError

logger = logging.getLogger(__name__)


class RingMonitor(BaseMonitor):
    """Monitor ringStatus for one or more NTCIP controller rings.

    Inherits the standard start/stop/poll lifecycle from
    :class:`~core.event_monitor.BaseMonitor` and uses the existing
    :class:`~core.snmp_client.EconoliteSNMPClient` with its CHUNK_SIZE=1
    chunking – no changes to the SNMP layer are required.

    Events emitted:

    ``ring_state_change``
        Fired when bits 0-2 (coded timing state) change for a ring.
        Callback signature: ``(ring_num: int, old_state: RingState,
        new_state: RingState)``

    ``ring_termination``
        Fired on the **0->1 edge** of any termination bit (gap-out, max-out,
        or force-off) so that subscribers receive exactly one notification per
        termination event and are not spammed while the bit remains high.
        Callback signature: ``(ring_num: int, reason: TerminationReason)``

    Example::

        monitor = RingMonitor(snmp_client, rings_to_monitor=[1, 2])
        monitor.on('ring_termination',
                   lambda ring, reason: print(f"Ring {ring}: {reason.name}"))
        monitor.start()
    """

    def __init__(
        self,
        snmp_client,
        poll_interval: float = 0.25,
        rings_to_monitor: Optional[List[int]] = None,
    ) -> None:
        """Initialise the ring monitor.

        Args:
            snmp_client: A live :class:`~core.snmp_client.EconoliteSNMPClient`
                instance shared with the other monitors.
            poll_interval: Seconds between SNMP polls.  Values below 0.5 s
                trigger a warning on low-power edge hardware.
            rings_to_monitor: List of ring numbers (1-4) to watch.  Defaults
                to ``[1, 2]`` (standard NEMA dual-ring layout).

        Raises:
            ValueError: If any value in ``rings_to_monitor`` is outside [1, 4].
        """
        super().__init__(snmp_client, poll_interval, name="RingMonitor")

        if rings_to_monitor is None:
            rings_to_monitor = [1, 2]

        invalid = [r for r in rings_to_monitor if not 1 <= r <= 4]
        if invalid:
            raise ValueError(
                f"rings_to_monitor contains invalid ring numbers: {invalid}"
            )

        self._rings: List[int] = sorted(set(rings_to_monitor))

        # Keyed by ring_num; None = not yet observed
        self._last_ring_states: Dict[int, Optional[RingState]] = {
            r: None for r in self._rings
        }
        self._last_termination_reasons: Dict[int, TerminationReason] = {
            r: TerminationReason.NONE for r in self._rings
        }

        if poll_interval < 0.5:
            logger.warning(
                '{"event":"poll_interval_warning","monitor":"RingMonitor",'
                '"poll_interval":%.3f,"message":"below 0.5s may saturate CPU'
                ' on low-power edge hardware"}',
                poll_interval,
            )

    # ------------------------------------------------------------------
    # Internal poll  (called by BaseMonitor._run_loop every poll_interval)
    # ------------------------------------------------------------------

    def _poll(self) -> None:
        """Fetch ringStatus for each monitored ring and emit change events.

        Each ring OID is fetched individually via the existing SNMP client,
        which already enforces CHUNK_SIZE=1 internally.  Any SNMPError is
        logged and skipped so a transient network hiccup does not abort the
        entire poll cycle.
        """
        for ring_num in self._rings:
            oid = get_ring_status_oid(ring_num)
            try:
                raw: int = self.snmp_client.get(oid)
            except SNMPError as exc:
                logger.error(
                    '{"event":"snmp_error","monitor":"RingMonitor",'
                    '"ring":%d,"error":"%s"}',
                    ring_num,
                    exc,
                )
                self.emit('error', exc)
                continue  # Try the next ring rather than aborting the poll

            status: RingStatus = parse_ring_status(ring_num, raw)
            self._process_ring(status)

        # Expose the latest coded states as _last_state for BaseMonitor consumers
        self._last_state = {r: self._last_ring_states[r] for r in self._rings}

    # ------------------------------------------------------------------
    # Change detection & event emission
    # ------------------------------------------------------------------

    def _process_ring(self, status: RingStatus) -> None:
        """Compare a freshly parsed ring status against the previous snapshot.

        Emits ``EVENT_RING_STATE_CHANGE`` when the coded state (bits 0-2)
        changes, and ``EVENT_RING_TERMINATION`` on the rising edge
        (NONE -> non-NONE) of a termination bit.

        Args:
            status: Freshly parsed :class:`~core.data_models.RingStatus`
                for a single ring.
        """
        ring_num  = status.ring_num
        new_state = status.state
        new_term  = status.termination_reason

        old_state = self._last_ring_states[ring_num]
        old_term  = self._last_termination_reasons[ring_num]

        # --- Coded state change (bits 0-2) ---
        if old_state is not None and new_state != old_state:
            logger.info(
                '{"event":"ring_state_change","ring":%d,'
                '"old":"%s","new":"%s"}',
                ring_num, old_state.name, new_state.name,
            )
            self.emit(EVENT_RING_STATE_CHANGE, ring_num, old_state, new_state)

        self._last_ring_states[ring_num] = new_state

        # --- Termination rising edge only (0->1) ---
        if new_term != TerminationReason.NONE and old_term == TerminationReason.NONE:
            logger.info(
                '{"event":"ring_termination","ring":%d,"reason":"%s"}',
                ring_num, new_term.name,
            )
            self.emit(EVENT_RING_TERMINATION, ring_num, new_term)

        self._last_termination_reasons[ring_num] = new_term

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def get_current_ring_state(self, ring_num: int) -> Optional[RingState]:
        """Return the last observed coded state for a ring.

        Args:
            ring_num: Ring number (must be in ``rings_to_monitor``).

        Returns:
            The current :class:`~core.data_models.RingState`, or ``None``
            if the ring has not yet been polled or is not monitored.
        """
        return self._last_ring_states.get(ring_num)

    def get_current_termination_reason(self, ring_num: int) -> Optional[TerminationReason]:
        """Return the last observed termination reason for a ring.

        Args:
            ring_num: Ring number (must be in ``rings_to_monitor``).

        Returns:
            The current :class:`~core.data_models.TerminationReason`, or
            ``None`` if the ring is not monitored.  Returns
            ``TerminationReason.NONE`` when the ring is monitored but no
            termination bit is currently set.
        """
        return self._last_termination_reasons.get(ring_num)

    @property
    def monitored_rings(self) -> List[int]:
        """Sorted list of ring numbers this instance is watching."""
        return list(self._rings)
