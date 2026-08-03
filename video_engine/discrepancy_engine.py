"""
discrepancy_engine.py — Co-located detector disagreement detection and trigger generation.

This module is the "Brain" of the integrated system.  It bridges the NTCIP
monitor (which reports raw detector state changes) and the video-buffer package
(which consumes trigger files from the Hot Folder).  It has **zero** imports
from either of those packages; all cross-boundary communication is handled via
atomic JSON files written to a spool directory.

Physical model
──────────────
Each detector *pair* represents **two different sensing technologies** watching
the **exact same physical zone** (e.g., a Radar unit and a Video detection
system both covering the same stop-bar).  The goal is not to measure upstream
vs. downstream travel time; it is to discover which system is misreporting.

Three discrepancy rules
───────────────────────

Rule 1 — Extended Holdover / Missed Call (Continuous Disagreement)
    If System A is ON and System B is OFF, a continuous disagreement timer
    starts at the moment of divergence.  If that exact A-ON / B-OFF state
    persists without interruption for > ``lag_threshold_sec``, a trigger fires.

    Unlike Rule 2, the duration of the disagreement is **not known at trigger
    time**.  The engine therefore:
      • Sends a ``"start"`` trigger with no cooldown engaged.
      • Actively tracks the pair in ``rt.active_trigger_id``.
      • Waits for both detectors to agree again (resolution).
      • Waits an additional ``post_roll_sec`` after resolution.
      • Sends a ``"stop"`` trigger, then engages cooldown.

    Catches:
      • One system picks up a vehicle the other completely misses.
      • One system drops its call while the other continues to hold.

    **No hysteresis, and that is a measured decision (2026-08-03, ROADMAP
    12B).**  Raising the fire threshold by one sampling cycle (5.0 → 5.33 s),
    or requiring a confirmation sample before firing, was evaluated against
    controller ground truth on both committed runs and **rejected**: on the
    2026-08-02 run it would have prevented only ~4–9 of rule 1's 39 false
    positives while pushing 22–53 genuine events below the bar (matched GT
    ``extended_disagreement`` shorter than 5.33 s: 53 on 08-02, 20 on 08-01) —
    net-negative by 3–6×.  The reason it does not work is that the FP
    mechanism is not marginal duration: it is sub-floor chatter *stitching*.
    The controller-truth continuous-XOR run at the FP event start has a median
    of **1.8 s**, so the engine's "continuous ≥ 5 s disagreement" was assembled
    across true agreements its sampling never saw — and its stale disagreement
    image then persists a median 7.2 s, so a one-cycle bump only delays the
    same fire.  Every consecutive-sample or agreement-confirmation variant is
    the same trade under a different name.  The genuine fix is finer sampling
    or per-pair suppression of chatter-prone channels (the existing
    ``suppress_high_duty_pairs`` family) — a deployment decision.  See
    SCOPE_partner_gate_dedup_window.md Item B.

Rule 2 — Orphan Pulse (Ghost Car)
    When System A turns OFF after a brief actuation whose total ON-duration was
    less than ``lag_threshold_sec``, we wait an additional ``lag_threshold_sec``
    window.  If System B was completely OFF during the entire window
    [A_on_time − threshold … A_off_time + threshold], a trigger fires.

    "Completely OFF" is decided against a bounded, time-windowed record of the
    partner's recent ON *intervals* (``_DetectorState.on_intervals``), not a
    single most-recent-edge scalar: the check fires only if **no** partner ON
    interval intersects the observation window **and** the partner is not
    currently ON.  (A scalar cannot represent an interval — it silently dropped
    legitimate orphans whenever the partner actuated *after* the window, and it
    missed mid-window overlaps that a newer partner ON had overwritten.)

    The verdict must also be **fresh**: if the evaluator only reaches the
    candidate more than ``_ORPHAN_DECISION_GRACE_SEC`` after the window closed
    (e.g. the pair sat in cooldown or inside an active Rule 1 recording), the
    candidate is discarded instead of fired — the RAM pre-roll for that moment
    is long gone, so a late trigger would record the wrong footage.

    Because the full orphan window has elapsed by the time the trigger fires,
    the **exact clip duration is known**: ``pre_roll_sec + disagreement_sec +
    post_roll_sec + threshold``.  This is passed as ``max_duration_sec`` in the
    start trigger and cooldown is engaged immediately — no stop trigger is sent.

    Catches:
      • A brief false-positive "ghost" pulse on one system that the other
        never saw at all.
      • Works symmetrically: A brief pulse on B that A never sees also fires.

    Rule 2 is additionally gated by the **sampling floor** (see below) on
    both sides of the comparison: a pulse shorter than
    ``min_pulse_floor_multiple × sampling_floor_sec`` is not registered as a
    candidate at all, and a candidate whose *partner* has recently produced
    ``partner_blip_max`` such sub-floor pulses is declined too — that
    partner's silence is not observable evidence.

Rule 3 — Chatter Exception (must NOT trigger)
    System A is solidly ON for 30 s; System B is chattering (ON 2 s / OFF 0.5 s
    / ON 2 s …) during the same 30 s.  The continuous disagreement (Rule 1)
    never exceeds ``lag_threshold_sec`` at any single moment, and neither system
    produces an isolated orphan pulse that the other never overlaps.  No trigger
    fires.

Rule 1 active-resolution state machine
───────────────────────────────────────

    ┌──────────────────────────────────────────────────────────────────┐
    │                      _evaluate_pair tick                         │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Cooldown guard ─────────────────────────────────────────────────┐
    │  cooldown_active? → return                                       │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Active Rule 1 tracking (if active_trigger_id is set) ───────────┐
    │                                                                  │
    │  Disagreement resolved? (a_is_on == b_is_on)                    │
    │       YES → set resolution_start_time (if not already set)      │
    │       NO  → clear resolution_start_time (still diverging)       │
    │                                                                  │
    │  Post-roll elapsed? (now − resolution_start_time >= post_roll)  │
    │       YES → _fire_trigger(..., action="stop") → cooldown         │
    │       NO  → return (keep waiting)                                │
    │                                                                  │
    │  CRITICAL: always return here; never evaluate new rules while    │
    │  active_trigger_id is set.                                       │
    └──────────────────────────────────────────────────────────────────┘
                │  (only reached when active_trigger_id is None)
                ▼
    ┌─ Rule 1 new-detection ────────────────────────────────────────────┐
    │  Disagreement > threshold → _fire_trigger(action="start")        │
    │  NOTE: cooldown NOT engaged; active_trigger_id set instead.      │
    └──────────────────────────────────────────────────────────────────┘
                │
                ▼
    ┌─ Rule 2 orphan detection ─────────────────────────────────────────┐
    │  Confirmed orphan → _fire_trigger(action="start",                 │
    │                                   duration_override=exact_dur)    │
    │  Cooldown engaged immediately.                                    │
    └──────────────────────────────────────────────────────────────────┘

Sampling floor (ROADMAP 9 / SCOPE_sampling_floor.md, 2026-07-30)
────────────────────────────────────────────────────────────────
**The engine must not evaluate evidence finer than its own sampling
resolution.**  The upstream detector source samples on a cycle it measures
itself (an NTCIP sweep at ``chunk_size=1`` is RTT-bound — measured median
1.53 s on 2026-07-19, catching only 7–42 % of true detector edges).  Evidence
below that resolution is aliasing, not signal: a *seen* sub-floor pulse fires
while the partner's equally short response pulse is simply *unseen* — the
exact shape of the false-positive storms observed on high-duty channels.

The floor is injected, never imported: ``system_runner`` (the composition
root that already wires both packages) calls :meth:`DiscrepancyMonitor.
set_sampling_floor` at startup from config and thereafter on a slow cadence
from the detector monitor's measured cycle.  ``discrepancy_engine`` still has
zero imports from ``ntcip_monitor``.  A float assignment is atomic under the
GIL — same pattern as ``cooldown_active``.

Three consequences, in order of how much they change behavior:

1. **Rule 2 is gated.**  An orphan candidate is registered only if its pulse
   duration ≥ ``min_pulse_floor_multiple × floor`` (config, default 2.0×).
   Shorter pulses bump a per-pair ``below_floor_suppressed`` counter, are
   dropped at DEBUG, and are recorded in the **suppression log** (below).
   Note the arithmetic: at the default 1.6 s floor the
   gate is 3.2 s, which *exceeds* a typical ``lag_threshold_sec`` of 2.0 s —
   so Rule 2 is effectively **off** until the sweep gets faster.  That is the
   intended reading of the measurement, not an accident.  After a green
   ``snmp_chunk_size: 8`` probe the floor drops to ~0.2 s and the gate to
   ~0.4 s, which suppresses almost nothing real.
2. **Rule 1 is not gated.**  Its threshold (seconds) is far above any floor.
   One residual imprecision is documented rather than coded: in the
   resolution state machine an *agreement* shorter than the floor is not
   reliable evidence that the disagreement resolved.  This is acceptable
   as-is because a re-divergence restarts the post-roll countdown, so a
   spurious agreement only delays the stop trigger — it never truncates the
   clip.
3. **High-duty pairs get an advisory, not suppression.**  A rolling ON-duty
   fraction per detector (over ``_DUTY_WINDOW_SEC``, computed on the
   evaluator thread from the same pruned ``on_intervals`` deque) drives a
   rate-limited structured WARNING when a pair's *minimum* duty exceeds
   ``high_duty_warn_fraction``: NTCIP data on such a channel is structurally
   unreliable regardless of the rules.  Setting ``suppress_high_duty_pairs``
   (default **false**) fully disables Rules 1+2 for those pairs — a
   deployment decision, off until the owner opts in.

Partner sub-floor-activity gate (ROADMAP 12A, 2026-08-03)
─────────────────────────────────────────────────────────
The floor gate above bounds the *orphan's* side of Rule 2.  This one bounds
the **partner's**, from the same principle: **the engine must not treat
partner silence as evidence when that partner's recent behavior shows it
produces pulses below the engine's own resolution.**

Ground truth says why.  Classifying every rule-2 false positive on the two
committed runs against the controller's own 82/81 edges, the dominant
population is not a bad orphan — the orphan pulse was real in 61 of 61 cases
checked — it is a partner that *did* respond, with a 0.1–0.4 s blip a ~0.33 s
sampling cycle cannot see.  The engine's "partner completely OFF during the
window" evidence is structurally blind to those, while the ground truth sees
them and refuses the anomaly.  Among rule-2 **true** positives the partner was
active in the window only ~1 % of the time (2/232 and 4/668), so the
separation is nearly perfect — but the separating variable is invisible to the
engine at event time.  Only a *statistical* signal can work, and the engine
already collects one: every candidate the floor gate declines is a recorded
sub-floor blip on a known detector.

So each detector keeps ``_DetectorState.below_floor_pulses``, a deque of the
``(on_ts, off_ts)`` windows its own pulses were declined at, deduped per
detector (a triangle declines one physical pulse once per pair) and pruned to
the gate's horizon.  A Rule 2 candidate whose partner has ≥ ``partner_blip_max``
(default **5**) entries inside the trailing ``partner_blip_window_sec``
(default **300**) is declined: it bumps a per-pair ``partner_blip_suppressed``
counter, logs at DEBUG, and writes a suppression row with reason
``partner_below_floor_activity``.  ``0`` on either key disables the gate.

**Order matters and is load-bearing**: the gate sits strictly *after* the
floor gate, so a below-floor pulse is always counted and reported as
``below_sampling_floor`` and the two populations stay disjoint.  The gate is
also blind to the *engine-visible* partner pulses Rule 2 already tests — that
check is the existing ``on_intervals`` overlap test and is unchanged.

Parameters are measured, not guessed (SCOPE_partner_gate_dedup_window.md,
Item A; counting distinct pulses over both committed runs): ≥5 in 300 s kills
6 FP / 5 TP on 2026-08-01 and 15 FP / 10 TP on 2026-08-02, taking those runs
to 98.0 % / 95.0 % overall and 98.7 % / 94.7 % on rule 2.  ≥3 keeps the same
FP kill at triple the TP cost; 600 s and 1800 s horizons are strictly worse.
The kills concentrate exactly where the mechanism says they should — the
26:33 pair, whose det 33 is also the #1 producer of below-floor candidates on
both runs by ~2.4×.  The gate is the software mitigation; a detector that
blips that much likely needs service, and being rolling rather than a static
"disable rule 2 on 26:33" is what lets it recover on its own once it is fixed.

Two rule-2 FP populations were examined and only this one is acted on.  The
other is a *threshold-boundary type flip*: the engine measures a pulse at
4.3–4.96 s (< 5.0 → rule 2) where the controller measures ≥ 5.0, so ground
truth files the same physical event as ``extended_disagreement``.  That is a
scoring artifact, not an engine defect, and suppressing the boundary zone is
net-negative (47 rule-2 TPs live there against 21 FPs on 2026-08-02) — so the
engine deliberately does nothing about it.

Decision log vs. recording log
──────────────────────────────
The engine appends one row to its own **decision log** (``engine_decisions.
csv``, path injected by ``system_runner``) for every trigger it successfully
emits — *before* anything downstream decides whether that trigger becomes a
clip.  This is deliberately **not** the same artifact as
``discrepancies_log.csv``, which the video-buffer backend writes only after
``_writer_semaphore.acquire()`` succeeds: a trigger dropped by the
``max_concurrent_writers`` cap, by a low-disk abort, or by an unmatched camera
leaves no row there at all.  Measuring engine recall against the recording log
therefore charges the engine for the buffer's back-pressure (on the 2026-07-31
run the cap was saturated 11.6 % of wall clock yet accounted for 43 % of the
apparent misses).  Score accuracy against *this* log; diff the two to measure
what the buffer dropped.

Rows carry the underlying event's start/end as exact Unix floats
(``event_start_ts`` / ``event_end_ts``) rather than leaving a consumer to
recover them from a 1-second local timestamp and a regex over the description.
Either may be blank where the rule does not define it — a Rule 1 ``start``
knows when the disagreement began but not when it ends, and a ``stop`` row
describes neither.

Writing is best-effort and strictly downstream of trigger delivery: a failed
append logs an ERROR and is otherwise swallowed, because a full or read-only
disk must never stop a clip from being recorded.

Suppression log (ROADMAP 9C3, 2026-08-01)
─────────────────────────────────────────
The decision log records what the engine *did*.  The **suppression log**
(``engine_suppressions.csv``, path injected the same way, ``None`` disables)
records what it deliberately **declined to do**, one row per distinct
suppressed candidate with a ``reason`` column.

``reason`` has two values today: ``below_sampling_floor`` — the Rule 2 gate in
consequence 1 above — and ``partner_below_floor_activity``, the partner gate
described in the section just above it.  Rows of the second kind carry
``partner_blip_count`` and ``partner_blip_window_sec`` (appended to the end of
the field tuple, the same discipline as every other column here); rows of the
first kind leave both blank, since the partner gate never sees a pulse the
floor gate already declined.  The floor columns exist because the accuracy report
currently *models* this population from the ground-truth side (it drops GT
pulses shorter than ``2 × poll``) rather than reading what the engine actually
suppressed, and those two selections are not the same events: the report
measures true durations from the controller's 0.1 s waveform, while the gate
measures the engine's own quantized observation, which carries up to ±1
sampling cycle on each edge.  A row per suppression turns that model into a
measurement.

Rows carry ``sampling_floor_sec`` and ``min_pulse_floor_multiple`` as separate
columns rather than only their product, so a consumer can recompute the gate
at other multiples and recover the counterfactual — "what would this run have
scored at 1.5× instead of 2.0×" — without another controller session.

**A suppressed row is not a would-have-fired trigger.**  Both gates sit at
candidate *registration*, ahead of Rule 2's partner-overlap test, so a
suppressed pulse never gets that far and might well have been rejected there
too.  Any recall attributed to either is therefore an **upper bound**.
Making it exact would mean arming below-floor pulses and gating at fire time,
which is a real behavior change — the ``orphan_watch_*`` slots hold one
candidate each, so arming junk would evict live candidates — and is
deliberately not done here.

``reason`` is a plain string precisely so new populations can land in this file
as new values, with no schema change and no second artifact — the partner gate
was the first to take that path, and the ones the accuracy report still models
rather than measures (cooldown suppression, a Rule 2 verdict discarded past
``_ORPHAN_DECISION_GRACE_SEC``, and ``suppress_high_duty_pairs``) can follow it.  Note the cross-pair duplicate rejection below deliberately does
**not** live here: it happens after a trigger is fully formed and keeps its
trigger ID, so it is a *decision* the engine made about a real detection, and a
consumer scoring accuracy must still credit it.  It is marked in the decision
log instead.

Cross-pair duplicate rejection (ROADMAP 9C4, 2026-08-01)
────────────────────────────────────────────────────────
Detectors are linked pairwise, but the links form larger structures: five of
intersection 201's groups are *triangles* (A→B, B→C, C→A), where one physical
event in which B disagrees with both A and C fires on pair ``A:B`` **and** pair
``B:C``, often on the same evaluator tick.  That is two clips of the same
moment, each burning one of only ``max_concurrent_writers`` (default 2) writer
slots.  On the 2026-08-01 run, 137 of 523 start decisions (26.2 %) were such
duplicates, while the writer cap dropped 174 decisions (33.6 %).

**Groups are derived, not configured.**  ``_build_structures`` computes the
connected components of the pair graph; a group is every detector reachable
from another through ``paired_detector_id`` links.  Both authoring styles
therefore work with no separate code path:

* explicit — ``paired_detector_id`` as a **list**: A ``[B, C]``, B ``[A, C]``,
  C ``[A, B]`` ("compare all of these");
* implicit — today's **ring** of scalars: A→B, B→C, C→A ("this cycle").

Pairs are the union of all normalized links, so for n = 3 the two forms yield
the *identical* 3 pairs.  **That coincidence is specific to n = 3** — a 4-ring
gives 4 edges where an explicit all-pairs list gives 6.  Neither is wrong, so a
group is a **dedup scope only**: it is never read as an instruction to evaluate
every internal pair, or a 4-ring config would silently grow comparisons nobody
asked for.  Pair generation stays link-driven.

The rejection itself: immediately before the tmp-write in :meth:`_fire_trigger`,
a ``start`` whose group fired another ``start`` less than one dedup window ago
is not written to the Hot Folder.  It is still
appended to the decision log, marked with ``suppressed_as_duplicate`` and
``duplicate_of_trigger_id``, because it is engine output that ground truth also
contains twice — dropping the row silently would score the sibling pair's GT
event as a *miss*.  Four properties are load-bearing:

1. **The window is anchored on emitted starts only.**  A suppressed row never
   updates the group's last-fire stamp, so a storm cannot roll the window
   forward indefinitely and suppress unboundedly.
2. **Cameras are part of the key.**  Two pairs in one group that resolve to
   different ``camera_id``s cover different footage and are not duplicates.
3. **``stop`` is never suppressed and never anchors.**  A ``stop`` closes a
   ``start`` the buffer has already seen; suppressing it would strand a
   recording, and letting it anchor would suppress the *next* genuine event.
4. **A suppressed Rule 1 ``start`` must not arm the resolution state machine.**
   Setting ``active_trigger_id`` for a trigger the buffer never received would
   later send it a ``stop`` for a recording that does not exist.  A suppressed
   start instead engages the pair cooldown (exactly what a Rule 2 start does),
   which also stops the pair re-firing on the same physical event one threshold
   later.

**The stop is an AND across every disagreement the clip stands for.**  Dropping
the duplicate *start* is safe — the sibling is already recording that instant —
but the *stop* is not symmetric: if the owner pair resolves at t+4 while the
folded pair keeps disagreeing until t+30, stopping on the owner alone ends the
footage before the event it was suppressed for is over.  So a suppressed
duplicate registers itself on the owner's ``held_pair_keys``, and the owner's
resolution state machine treats the disagreement as resolved only when its own
detectors agree **and** every held pair's do.  A re-divergence on any of them
restarts the post-roll countdown, exactly as one on the owner does.  Three
consequences:

* A held pair runs **no rules at all** while it is held (guard 0 in
  ``_evaluate_pair``) — the group is already recording that event, and its
  cooldown could otherwise be cleared early by the callback path.
* When the ``stop`` finally goes out, the held pairs are **released into a
  fresh cooldown** rather than straight back into service, so none of them
  fires again on the tail of the footage just recorded.
* Clips get longer, by design.  The buffer's ``max_duration_sec`` safety cap
  (300 s for Rule 1) still bounds them.

**The window is per rule, and the Rule 2 half carries a coverage guard**
(ROADMAP 14, 2026-08-03).  One number cannot serve both halves, because the
guarantee a fold rests on is not the same:

* ``dedup_window_rule1_sec`` (default **10.0**) governs a Rule 1 candidate
  folding into a Rule 1 owner.  The AND-stop above makes *any* width safe —
  the owner's recording is held open until the folded pair resolves too.
* ``dedup_window_sec`` (default **3.0**, raised from 1.0) governs a Rule 2
  candidate, which has no such lever: its pulse is over before it is
  evaluated, so it is folded only if the owner's footage already contains it.
  :meth:`DiscrepancyMonitor._owner_covers_event` is that check — the
  candidate's ``[event_start - pre_roll, event_end + post_roll]`` must sit
  inside a Rule 2 owner's fixed span (carried on the ``_GroupFire``), or the
  Rule 1 owner must still be recording (``active_trigger_id`` set).  A Rule 1
  owner that has already stopped is refused; that is unreachable at the
  defaults and exists so raising the window in config cannot silently lose
  footage.

Both widths are measured on clip *containment*, not the fire-time clustering
that sized the original 1.0 s — see the constants.  Replayed over the
committed 2026-08-02 decision log (the replay reproduces that run's own 457
suppression marks 457/457 at the shipped settings), the pair takes suppression
to **545 of 1553 starts (35.1 %)**, prevents **17 of the 38** same-group clips
the disk sweep had to delete as contained duplicates, and leaves **zero** Rule
2 pulse windows uncovered — the guard refusing 5 folds that would have lost
footage.  On the 2026-08-01 log: 164, up from 135, guard refusing 6.  The
remaining same-group deletions (Rule 1 into Rule 2 by design, three 38 s+ gap
outliers) and all 152 different-group ones stay with the disk sweep: this
narrows its diet, it does not replace it.

One exposure is widened rather than created: a detector stuck ON already holds
its own Rule 1 recording open indefinitely (the engine sends no ``stop`` until
the pair agrees again), and the AND now extends that to the held pairs as well.
The *footage* is still bounded — the buffer auto-stops at ``max_duration_sec``
— so what stalls is pair-level state, not disk.

**A Rule 1 start is never folded into a Rule 2 recording.**  A Rule 2 clip's
length is fixed at fire time and no ``stop`` is ever sent for it, so it cannot
be held open; folding an open-ended Rule 1 event into one would truncate it to
a length chosen for a brief pulse.  Such a start fires its own recording, which
it can close itself.  This is rare in practice — on the 2026-08-01 run 133 of
137 duplicates were same-rule (82 rule1→rule1, 51 rule2→rule2) and only 2 were
rule2→rule1.  Rule 2 duplicates need no holding either way: an orphan pulse is
complete before it is even evaluated.  Net effect on that run: of the 137
same-group starts inside the window, this code suppresses **135 (25.8 %)** and
lets the 2 rule2→rule1 cases through — replay it and expect 135, not 137.

All four ``_fire_trigger`` call sites are on the evaluator thread, so the
group bookkeeping needs no lock.

Setting a window to ``0`` disables its own path only: ``dedup_window_sec: 0``
still folds Rule 1 into Rule 1, and ``dedup_window_rule1_sec: 0`` still folds
Rule 2.  Zeroing both disables the mechanism.

Thread-safety contract
──────────────────────
``on_detector_on`` / ``on_detector_off`` are called from the NTCIP monitor's
event-dispatch thread and must return in microseconds.  They only acquire a
per-detector ``threading.Lock`` long enough to mutate four scalar fields plus,
on a falling edge, one O(1) ``deque.append`` recording the just-closed ON
interval.

The background evaluator thread acquires the same locks briefly to snapshot
each detector's state (pruning expired ON intervals while it holds the lock —
never from the callback path), then releases them before any comparison or I/O.

All ``_PairRuntimeState`` fields are written exclusively by the evaluator thread
except ``cooldown_active``, which the NTCIP callback thread may clear via the
early-reset path.  A boolean assignment is atomic under CPython's GIL and the
evaluator tolerates a one-tick stale read in the rare race window.  The
sampling floor is written by whatever thread calls
:meth:`DiscrepancyMonitor.set_sampling_floor` (in production, ``system_runner``'s
slow updater thread) and read by the evaluator — again a single atomic float
assignment, with a one-tick stale read tolerated by design.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
import pytz
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, List, NamedTuple, Optional, Sequence, Tuple

from config_manager import ConfigProvider, ConfigProviderError

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)

# Maximum staleness of a Rule 2 verdict: an orphan candidate whose observation
# window closed more than this many seconds ago is discarded instead of fired
# (the pre-roll footage for it no longer exists).  Sized to tolerate evaluator
# scheduling jitter (ticks are 0.1 s) while staying far below the cooldown.
_ORPHAN_DECISION_GRACE_SEC = 2.0

# Hard cap on the partner ON-interval history deque.  The time-based pruning in
# the evaluator is the real bound (now ``max(~3 × threshold, _DUTY_WINDOW_SEC)``
# — the duty computation reads the same deque); this maxlen is a
# belt-and-suspenders RAM cap.  At the 0.2 s NTCIP poll floor a detector can
# produce at most ~2.5 intervals/s, so 512 covers the 120 s duty window with
# ~1.7× headroom (and ~6× at today's measured 1.5 s floor) for ~8 KB/detector.
_PARTNER_INTERVAL_MAXLEN = 512

# ---------------------------------------------------------------------------
# Sampling-floor defaults (ROADMAP 9 — see the module docstring)
# ---------------------------------------------------------------------------

# Assumed effective sampling cycle until system_runner injects a measured one.
# 1.6 s = today's measured NTCIP reality (median sweep 1.53 s at chunk_size 1).
_DEFAULT_SAMPLING_FLOOR_SEC = 1.6

# Rule 2 trusts an orphan pulse only if it lasted at least this many sampling
# cycles.  2.0 is the Nyquist-flavoured minimum: below two samples the "pulse"
# and the partner's absence of one are equally likely to be aliasing.
_DEFAULT_MIN_PULSE_FLOOR_MULTIPLE = 2.0

# A pair whose *minimum* detector ON-duty exceeds this fraction is flagged as
# operating outside the regime where NTCIP sampling can resolve its edges.
_DEFAULT_HIGH_DUTY_WARN_FRACTION = 0.8

# ---------------------------------------------------------------------------
# Partner sub-floor-activity gate (ROADMAP 12A — see the module docstring)
# ---------------------------------------------------------------------------

# Rolling horizon over which a detector's *declined* below-floor pulses are
# remembered, and how many of them make that detector's silence untrustworthy
# as a Rule 2 partner.  Both measured against the committed 2026-08-01 and
# 2026-08-02 artifacts (SCOPE_partner_gate_dedup_window.md, Item A): counting
# distinct pulses, "≥5 in 300 s" kills 6 FP / 5 TP on 08-01 and 15 FP / 10 TP
# on 08-02 — the best FP:TP ratio of every (N, horizon) combination tried.
# Loosening N to 3 triples the TP cost for no extra FPs; 600 s and 1800 s
# horizons are strictly worse (same FP kill, 2–5× the TP kill).
_DEFAULT_PARTNER_BLIP_WINDOW_SEC = 300.0
_DEFAULT_PARTNER_BLIP_MAX = 5

# Hard cap on the per-detector below-floor-pulse deque.  The time-based pruning
# against ``partner_blip_window_sec`` is the real bound; this is a RAM
# backstop.  The worst detector on record (det 33) produced 731 distinct
# below-floor pulses over 11.9 h — ~5 per 300 s window — so 256 is ~50×
# headroom at ~4 KB/detector.
_BELOW_FLOOR_PULSE_MAXLEN = 256

# Rolling window over which ON-duty is measured.
_DUTY_WINDOW_SEC = 120.0

# Duty is an advisory statistic, not a per-tick decision input: recomputing it
# on every 0.1 s tick would walk the interval deques 10×/s per pair for no
# added fidelity on a 120 s window.  J1900-class CPUs care.
_DUTY_EVAL_INTERVAL_SEC = 5.0

# Minimum spacing between high-duty WARNINGs for the same pair.  The condition
# is a standing property of the channel, so this is a heartbeat, not an event.
_HIGH_DUTY_WARN_INTERVAL_SEC = 600.0

# ---------------------------------------------------------------------------
# Cross-pair duplicate rejection (ROADMAP 9C4 — see the module docstring)
# ---------------------------------------------------------------------------

@dataclass
class _GroupFire:
    """The last ``start`` a detector group actually emitted, for one camera set.

    Attributes:
        fire_ts: ``event_timestamp`` the trigger carried.
        trigger_id: Hex ID of that trigger.
        pair_key: The pair that fired it — the *owner* of the recording, which
            a later duplicate needs so it can attach itself to the owner's
            resolution state machine.
        rule: Rule that fired it.  A Rule 2 clip has a fixed length and no stop
            trigger, so it cannot be held open for a later Rule 1 duplicate;
            the rule is recorded here to make that decision (see
            :meth:`DiscrepancyMonitor._duplicate_within_group`).
        span_start: Beginning of the clip this trigger buys, in **event
            coordinates** — ``event_start_ts - pre_roll_sec``.  Set for Rule 2
            owners only (their length is fixed at fire time); ``None`` for
            Rule 1, whose span is open-ended and is judged by liveness instead.
        span_end: End of that span — ``span_start + max_duration_sec``, i.e.
            ``pulse_end + post_roll + threshold`` for a Rule 2 owner.  ``None``
            for Rule 1, same reason.
    """

    fire_ts: float
    trigger_id: str
    pair_key: str
    rule: str
    span_start: Optional[float] = None
    span_end: Optional[float] = None


# Seconds after a group's last emitted "start" during which another pair in the
# same group is treated as a duplicate of it.  **Two windows, because the
# footage guarantee differs by rule** (ROADMAP 14):
#
# * a Rule 1 candidate folded into a Rule 1 owner is safe at any width — the
#   AND-stop holds the owner's recording open until the folded pair's own
#   disagreement resolves, so the clip cannot end early;
# * a Rule 2 candidate is folded into a *fixed* span, so its width is bounded
#   by the coverage guard rather than by trust.
#
# Both values are measured against the committed 2026-08-02 run, on clip
# *containment* rather than the fire-time clustering that sized the original
# 1.0 s: of the 38 same-group clips the disk sweep deleted as wholly contained
# in a sibling's, the inter-start gap median is 1.62 s, 29 are within 3 s and
# 35 within 10 s.  10.0 (≈ this deployment's pre_roll + post_roll) sits just
# above the p90 preventable gap; 3.0 is the knee of the histogram, past which
# extra Rule 2 suppressions buy no further deletions.  The three 38 s+ outliers
# are deliberately left to the disk sweep — a window that wide would fold
# genuinely distinct events into one clip.  Either key set to 0 disables its
# own path only.
_DEFAULT_DEDUP_WINDOW_SEC = 3.0
_DEFAULT_DEDUP_WINDOW_RULE1_SEC = 10.0

# Slack on the Rule 2 coverage comparison, so a span that matches to the
# millisecond is not refused by float representation alone.  Deliberately tiny:
# this is not a tolerance for "close enough" footage.
_COVERAGE_EPSILON_SEC = 1e-6

# ---------------------------------------------------------------------------
# Decision log (ROADMAP 9C1 — see the module docstring)
# ---------------------------------------------------------------------------

# Column order of ``engine_decisions.csv``.  Append-only: readers key on the
# header, and an existing file is never rewritten, so new columns go on the
# END of this list or a resumed log's rows stop lining up with its header.
# ``event_timestamp`` doubles as the format discriminator for consumers that
# also accept the video-buffer's ``discrepancies_log.csv``.
_DECISION_LOG_FIELDS = (
    "event_timestamp",      # exact Unix float — when the engine decided
    "local_timestamp",      # same instant, intersection-local, for humans
    "intersection_id",
    "trigger_id",
    "action",               # "start" | "stop"
    "rule",
    "pair_key",
    "det_a",
    "det_b",
    "det_a_type",
    "det_b_type",
    "event_start_ts",       # exact Unix start of the underlying event, or ""
    "event_end_ts",         # exact Unix end of the underlying event, or ""
    "disagreement_sec",
    "max_duration_sec",
    "cameras",              # ";"-joined camera IDs, as sent in the trigger
    "description",
    # ── ROADMAP 9C4, appended 2026-08-01 ──
    "dedup_group",              # ":"-joined detector IDs of the derived group
    "suppressed_as_duplicate",  # "1" if it never reached the Hot Folder
    "duplicate_of_trigger_id",  # the trigger it duplicated, or ""
)

# ---------------------------------------------------------------------------
# Suppression log (ROADMAP 9C3 — see the module docstring)
# ---------------------------------------------------------------------------

# Reason codes for ``engine_suppressions.csv``.  A plain string column: new
# populations (cooldown, grace expiry, high-duty, cross-pair duplicate) become
# new values here, never new files or new columns.
_SUPPRESS_BELOW_FLOOR = "below_sampling_floor"
_SUPPRESS_PARTNER_BLIP = "partner_below_floor_activity"


class _OrphanSuppression(NamedTuple):
    """One Rule 2 candidate that ``_maybe_register_orphan`` declined to arm.

    The registration helper stays static and free of I/O; it reports what it
    declined through this value and the caller — which owns the instance and
    the log paths — writes the row.  Two gates can produce one, so the reason
    travels with the pulse rather than being inferred by the caller.

    Attributes:
        reason: One of the ``_SUPPRESS_*`` codes above.
        pulse: ``(on_ts, off_ts)`` of the declined pulse, exact Unix floats.
        partner_blip_count: Below-floor pulses counted on the *partner* inside
            the gate's horizon.  Meaningful only for
            ``_SUPPRESS_PARTNER_BLIP``; ``0`` otherwise.
    """

    reason: str
    pulse: Tuple[float, float]
    partner_blip_count: int = 0

# Column order of ``engine_suppressions.csv``.  Append-only for the same reason
# as _DECISION_LOG_FIELDS: a resumed log keeps its original header.
# ``event_start_ts`` / ``event_end_ts`` deliberately reuse the decision log's
# names — both files describe detector events on the same clock, so one parser
# and one correspondence matcher serve both.
_SUPPRESSION_LOG_FIELDS = (
    "event_timestamp",           # exact Unix float — when the engine declined
    "local_timestamp",           # same instant, intersection-local, for humans
    "intersection_id",
    "reason",                    # see the _SUPPRESS_* constants above
    "rule",                      # the rule that would have evaluated it
    "pair_key",
    "det_a",
    "det_b",
    "det_a_type",
    "det_b_type",
    "orphan_det",                # the detector whose pulse was suppressed
    "slot",                      # "a" | "b" — which orphan slot it would fill
    "event_start_ts",            # exact Unix ON edge of the suppressed pulse
    "event_end_ts",              # exact Unix OFF edge of the suppressed pulse
    "pulse_duration_sec",
    "min_pulse_sec",             # the gate in force at the moment of the call
    "sampling_floor_sec",        # ── the gate's two factors, kept separate so
    "min_pulse_floor_multiple",  #    a consumer can re-derive it at other × ──
    "description",
    # ── ROADMAP 12A, appended 2026-08-03 (end of the tuple, never mid-list) ──
    # The partner gate's two factors, same reasoning as the floor gate's: store
    # the count and the horizon it was counted over, not just the verdict, so a
    # finished run can be re-scored at other thresholds.  Blank on
    # ``below_sampling_floor`` rows, which the partner gate never sees.
    "partner_blip_count",
    "partner_blip_window_sec",
)

# ---------------------------------------------------------------------------
# Detector ID ordering helper
# ---------------------------------------------------------------------------

def _sort_detector_ids(det_ids: Sequence[str]) -> List[str]:
    """Order detector IDs numerically when they all look like numbers.

    Used only to build the *group* ID in the decision log and the startup
    banner, where ``"2:17:46"`` is legible and the plain lexicographic
    ``"17:2:46"`` is not.  Pair keys keep their existing lexicographic
    ``sorted()`` — this helper deliberately does not change them, because
    ``pair_key`` is already committed to in two log formats.

    Args:
        det_ids: Detector ID strings, in any order.

    Returns:
        A new sorted list: numerically if every ID is all-digits, otherwise
        lexicographically.
    """
    values = [str(d) for d in det_ids]
    if values and all(v.isdigit() for v in values):
        return sorted(values, key=int)
    return sorted(values)


# ---------------------------------------------------------------------------
# Config coercion helper
# ---------------------------------------------------------------------------

def _coerce_non_negative(raw: object, default: float) -> float:
    """Coerce a config value to a non-negative float, falling back on garbage.

    The posture every tuning knob in this module shares: a malformed or
    negative value falls back to the default rather than disabling the
    mechanism it belongs to, because a typo must never silently restore the
    behaviour the mechanism exists to prevent.  An explicit ``0`` is a
    legitimate value and is passed through — each caller documents what zero
    means for its own knob.

    Args:
        raw: The value as read from the intersection config.
        default: Value to fall back to when ``raw`` is not a non-negative
            number.

    Returns:
        ``float(raw)`` when that is a non-negative number, else ``default``.
    """
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    if value < 0.0 or value != value:  # NaN fails every comparison
        return default
    return value


# ---------------------------------------------------------------------------
# Timezone resolution helper
# ---------------------------------------------------------------------------

def _resolve_pytz(tz_name: str, fallback_log: logging.Logger) -> pytz.BaseTzInfo:
    """Resolve an IANA timezone name to a :mod:`pytz` timezone object.

    ``pytz`` carries its own full copy of the IANA timezone database, so this
    function has **no dependency on system zone files or the** ``tzdata``
    **package**.  ``US/Mountain``, ``America/Boise``, and all other canonical
    and legacy aliases are resolved directly from the bundled database.

    Falls back to ``pytz.utc`` (equivalent to UTC) and emits a structured
    warning when the name is genuinely unknown — i.e. not present in pytz's
    bundled database at all.

    Args:
        tz_name: IANA timezone name string (e.g. ``"America/Boise"`` or the
            legacy alias ``"US/Mountain"``).
        fallback_log: Logger used to emit the structured warning on failure.

    Returns:
        A :mod:`pytz` timezone object.  Always returns a valid object; never
        raises.
    """
    try:
        return pytz.timezone(tz_name)
    except pytz.exceptions.UnknownTimeZoneError:
        fallback_log.warning(
            "Unknown IANA timezone name; falling back to UTC",
            extra={"timezone": tz_name},
        )
        return pytz.utc


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------

@dataclass
class _DetectorState:
    """Mutable state for a single physical detector / sensing system.

    All fields are protected by ``lock`` and must not be read or written
    outside of a ``with self.lock`` block — with one documented exception,
    ``below_floor_pulses``, which no callback path touches at all (see below).

    Attributes:
        detector_id: String identifier matching the config schema.
        is_on: Current ON/OFF state.
        last_on_time: ``time.time()`` of the most recent rising edge (ON).
            Zero if the detector has never been seen ON.
        last_off_time: ``time.time()`` of the most recent falling edge (OFF).
            Zero if the detector has never been seen OFF.
        last_pulse_on_time: ``time.time()`` of the ON-edge that immediately
            preceded the most recent OFF-edge.  Updated on every rising edge
            so the evaluator can reconstruct the pulse window
            ``[last_pulse_on_time, last_off_time]`` even after the detector
            has already transitioned back to OFF.
        on_intervals: Bounded history of recently *completed* ON intervals as
            ``(on_ts, off_ts)`` tuples, appended on each falling edge.  This is
            what Rule 2 consults to decide whether this detector (as the
            partner) overlapped an orphan candidate's observation window — a
            deque because a single most-recent-edge scalar cannot represent an
            interval.  The evaluator thread prunes entries older than the
            largest window Rule 2 ever inspects; ``maxlen`` is only a RAM
            backstop.  The *current* ON (if any) is not in the deque — it is
            represented by ``is_on`` / ``last_on_time``.
        below_floor_pulses: Bounded history of this detector's ON pulses that
            the Rule 2 sampling-floor gate **declined**, as ``(on_ts, off_ts)``
            tuples (ROADMAP 12A).  This is the evidence the partner
            sub-floor-activity gate counts: a detector that keeps producing
            pulses too short for the engine to resolve is a detector whose
            *silence* cannot be trusted as proof it saw nothing.
            **Unlike every other field here it is written and read only on the
            evaluator thread** — the floor gate that fills it and the partner
            gate that reads it both run there — so it is deliberately not
            protected by ``lock``.  Pruned to ``partner_blip_window_sec``
            alongside ``on_intervals``; ``maxlen`` is only a RAM backstop.
            Entries are deduped against the deque's tail: a detector in two
            pairs has the same physical pulse declined once per pair.
    """

    detector_id: str
    is_on: bool = False
    last_on_time: float = 0.0
    last_off_time: float = 0.0
    last_pulse_on_time: float = 0.0
    on_intervals: Deque[Tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=_PARTNER_INTERVAL_MAXLEN)
    )
    below_floor_pulses: Deque[Tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=_BELOW_FLOOR_PULSE_MAXLEN)
    )
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class _PairRuntimeState:
    """Per-pair mutable runtime bookkeeping owned exclusively by the evaluator thread.

    Because only the evaluator thread writes these fields, no additional lock
    is needed beyond the GIL.  The single exception is ``cooldown_active``,
    which the NTCIP callback thread may clear via the early-reset path; a
    boolean assignment is atomic under CPython's GIL and the evaluator
    tolerates a one-tick stale read in the rare race window.

    Attributes:
        pair_key: Canonical sorted pair identifier, e.g. ``"1:2"``.
        disagreement_start: ``time.time()`` when continuous one-ON / one-OFF
            divergence began, or ``None`` when both detectors agree.
        cooldown_active: ``True`` while the pair is inside a post-trigger
            cooldown window.
        triggered_at: ``time.time()`` when the most recent trigger fired.
        orphan_watch_a: Pending Rule-2 candidate for detector A as a
            ``(pulse_on_time, pulse_off_time)`` tuple, or ``None``.
        orphan_watch_b: Same for detector B.
        active_trigger_id: Hex trigger ID of an in-progress Rule 1 recording,
            or ``None`` when no Rule 1 recording is active.  While this field
            is set the evaluator skips all new-detection logic and runs only
            the resolution state machine.
        resolution_start_time: ``time.time()`` when both detectors first agreed
            again after a Rule 1 disagreement, marking the start of the
            post-roll countdown.  ``None`` if the disagreement is still ongoing
            or if no Rule 1 recording is active.
        below_floor_suppressed: Count of orphan candidates rejected because the
            pulse was shorter than the sampling floor allows us to trust.  A
            large value relative to fired triggers means the pair is being
            sampled too slowly to judge — it is diagnostic, never an input to
            any rule.
        partner_blip_suppressed: Count of orphan candidates rejected because
            the *partner* detector has been producing too many below-floor
            pulses recently (ROADMAP 12A), i.e. its silence during the
            observation window is not trustworthy evidence.  Diagnostic in the
            same way ``below_floor_suppressed`` is; the two populations are
            disjoint (a below-floor pulse is declined before the partner gate
            ever sees it).
        last_duty_eval_ts: ``time.time()`` of the most recent ON-duty
            computation for this pair (throttled to ``_DUTY_EVAL_INTERVAL_SEC``).
        pair_min_duty: Most recently computed ``min(duty_a, duty_b)``.
        high_duty_active: Cached verdict of the last duty computation — whether
            ``pair_min_duty`` exceeded ``high_duty_warn_fraction``.
        last_high_duty_warn_ts: ``time.time()`` of the last high-duty WARNING
            emitted for this pair (rate limit).
        held_pair_keys: Pairs in the same detector group whose ``start`` was
            folded into **this pair's** in-progress Rule 1 recording as a
            cross-pair duplicate (ROADMAP 9C4).  Only meaningful while
            ``active_trigger_id`` is set: the resolution state machine will
            not send its ``stop`` until every one of them has *also* stopped
            disagreeing, so the clip covers all the disagreements it stands
            for and not just the one that happened to fire first.
        held_by_pair_key: The pair holding **this pair** inside its recording,
            or ``None``.  While set, the evaluator runs no rules for this pair
            — the group is already recording this event.
    """

    pair_key: str
    disagreement_start: Optional[float] = None
    cooldown_active: bool = False
    triggered_at: float = 0.0
    orphan_watch_a: Optional[Tuple[float, float]] = None
    orphan_watch_b: Optional[Tuple[float, float]] = None
    # Rule 1 active-resolution tracking (new in Session 5)
    active_trigger_id: Optional[str] = None
    resolution_start_time: Optional[float] = None
    # Cross-pair duplicate participation (ROADMAP 9C4).  Both are transient
    # Rule 1 tracking state and are therefore discarded by a config reload.
    held_pair_keys: List[str] = field(default_factory=list)
    held_by_pair_key: Optional[str] = None
    # Monotonic ON-edge timestamp of the most recent orphan pulse already
    # registered for each slot.  Prevents a single stale pulse from being
    # re-armed (and re-fired) after each cooldown expiry when the detector has
    # not actuated again.  See _maybe_register_orphan.
    last_handled_pulse_on_a: float = 0.0
    last_handled_pulse_on_b: float = 0.0
    # Sampling-floor bookkeeping (ROADMAP 9) and the partner gate (12A).
    below_floor_suppressed: int = 0
    partner_blip_suppressed: int = 0
    last_duty_eval_ts: float = 0.0
    pair_min_duty: float = 0.0
    high_duty_active: bool = False
    last_high_duty_warn_ts: float = 0.0


# ---------------------------------------------------------------------------
# Pure disagreement-logic functions (no side-effects — fully unit-testable)
# ---------------------------------------------------------------------------

def _check_rule1_continuous(
    disagreement_start: Optional[float],
    now: float,
    threshold: float,
) -> Tuple[bool, float]:
    """Rule 1: detect a continuous one-ON / one-OFF disagreement exceeding the threshold.

    Called on every evaluator tick while one detector is ON and the other is OFF.

    Args:
        disagreement_start: Wall-clock time when the current divergence began,
            or ``None`` if the detectors are not currently diverged.
        now: Current ``time.time()``.
        threshold: Allowable disagreement window in seconds
            (``lag_threshold_sec`` from detector config).

    Returns:
        A ``(should_fire, duration_seconds)`` tuple.  ``should_fire`` is
        ``True`` when the continuous disagreement has exceeded ``threshold``.
    """
    if disagreement_start is None:
        return False, 0.0
    duration = now - disagreement_start
    return duration > threshold, round(duration, 3)


def _check_rule2_orphan(
    pulse_on: float,
    pulse_off: float,
    other_intervals: Sequence[Tuple[float, float]],
    other_is_on: bool,
    now: float,
    threshold: float,
    decision_grace_sec: float = _ORPHAN_DECISION_GRACE_SEC,
) -> Tuple[bool, str]:
    """Rule 2: detect an orphan pulse — a brief actuation the partner never saw.

    The observation window is ``[pulse_on − threshold … pulse_off + threshold]``.
    The check is deferred until ``now >= pulse_off + threshold`` so the full
    post-pulse grace period has elapsed before a verdict is rendered, and it is
    abandoned once ``now > pulse_off + threshold + decision_grace_sec`` — a
    verdict that late (pair was in cooldown or inside an active Rule 1
    recording) would trigger a clip whose pre-roll footage no longer exists.

    Rule 3 protection is embedded here: if **any** completed partner ON
    interval intersects the observation window, or the partner is currently
    ON, the check is suppressed.  Interval intersection (rather than a single
    most-recent-edge comparison) is what makes a partner actuation *after* the
    window correctly non-suppressing, and a partner ON that merely straddles
    the window boundary correctly suppressing.

    Args:
        pulse_on: ``time.time()`` of the orphan candidate's ON-edge.
        pulse_off: ``time.time()`` of the orphan candidate's OFF-edge.
        other_intervals: Completed ON intervals of the partner detector as
            ``(on_ts, off_ts)`` tuples, most-recent-last.  Need only cover the
            observation window — older entries are ignored by the overlap test.
        other_is_on: Current ON state of the partner detector.
        now: Current ``time.time()``.
        threshold: Allowable disagreement window in seconds.
        decision_grace_sec: Maximum verdict staleness past the window end.

    Returns:
        A ``(should_fire, description)`` tuple.  ``should_fire`` is ``True``
        when the orphan condition is confirmed.  ``description`` is a
        human-readable string for trigger metadata.
    """
    if now < pulse_off + threshold:
        return False, ""

    window_start = pulse_on - threshold
    window_end   = pulse_off + threshold

    if now > window_end + decision_grace_sec:
        return False, ""

    if other_is_on:
        return False, ""

    for on_ts, off_ts in other_intervals:
        if on_ts <= window_end and off_ts >= window_start:
            return False, ""

    pulse_duration = round(pulse_off - pulse_on, 3)
    desc = (
        f"orphan pulse duration={pulse_duration}s "
        f"window=[{window_start:.3f}, {window_end:.3f}]"
    )
    return True, desc


def _compute_on_duty_fraction(
    intervals: Sequence[Tuple[float, float]],
    is_on: bool,
    last_on_time: float,
    now: float,
    window_sec: float,
) -> float:
    """Compute the fraction of the trailing window a detector spent ON.

    Reads the same ``on_intervals`` history Rule 2 consults, plus the
    still-open ON interval implied by ``is_on`` / ``last_on_time``.  Intervals
    are clipped to ``[now − window_sec, now]``, so partial overlaps at either
    edge contribute only the part that falls inside the window.  Completed
    intervals never overlap each other (they are consecutive ON periods of one
    detector), so clipped durations can simply be summed.

    The result is only as complete as the retained history: entries pruned by
    the evaluator or evicted by the deque's ``maxlen`` are gone, which biases
    the fraction *downwards*.  That is the safe direction — it can make the
    high-duty advisory miss, never make it fire spuriously.

    Args:
        intervals: Completed ON intervals as ``(on_ts, off_ts)`` tuples.
        is_on: Whether the detector is currently ON.
        last_on_time: ``time.time()`` of the most recent rising edge; used
            only when ``is_on`` is ``True``.
        now: Current ``time.time()``.
        window_sec: Length of the trailing window in seconds.

    Returns:
        ON-duty fraction in ``[0.0, 1.0]``.  Returns ``0.0`` for a
        non-positive ``window_sec``.
    """
    if window_sec <= 0:
        return 0.0

    window_start = now - window_sec
    on_time = 0.0

    for on_ts, off_ts in intervals:
        start = max(on_ts, window_start)
        end = min(off_ts, now)
        if end > start:
            on_time += end - start

    if is_on and last_on_time > 0.0:
        start = max(last_on_time, window_start)
        if now > start:
            on_time += now - start

    return min(1.0, on_time / window_sec)


# ---------------------------------------------------------------------------
# DiscrepancyMonitor
# ---------------------------------------------------------------------------

class DiscrepancyMonitor:
    """Monitors co-located detector pairs and writes trigger files on disagreement.

    Each pair represents two sensing systems covering the same physical zone.
    The monitor applies three rules (see module docstring) and writes atomic
    JSON trigger files to the Hot Folder.

    Rule 1 recordings are actively managed with paired ``"start"`` /
    ``"stop"`` triggers.  Rule 2 recordings are self-contained ``"start"``
    triggers with an exact ``max_duration_sec``.

    This class is entirely decoupled from both ``ntcip_monitor`` and
    ``video_buffer``.

    Args:
        intersection_id: Canonical intersection identifier (e.g.
            ``"1234_main"``).  Must exist in the ``ConfigProvider``.
        config_provider: A :class:`~config_manager.ConfigProvider` instance
            used to fetch detector pairing rules, thresholds, and camera IDs.
        trigger_dir: Path to the Hot Folder directory where trigger JSON
            files will be written.  Created automatically if absent.
        cooldown_sec: Seconds to suppress re-triggering the same pair after a
            trigger fully completes.
        evaluator_interval_sec: Sleep duration between evaluator ticks.
            Default 0.1 s (100 ms).
        pre_roll_sec: Pre-roll seconds embedded in every trigger file and
            used in the Rule 2 exact-duration calculation.
        post_roll_sec: Seconds to wait after Rule 1 resolution before
            sending the ``"stop"`` trigger.  Also embedded in every trigger.
        max_duration_sec: Hard recording cap used as ``max_duration_sec``
            for Rule 1 triggers only (video-buffer safety net if the stop
            trigger is somehow missed).
        decision_log_path: Optional CSV path receiving one row per emitted
            trigger — the engine's own record of what it decided, kept
            separate from the video buffer's record of what it managed to
            record (see the module docstring).  ``None`` disables it.
            Parent directories are created on first write.
        suppression_log_path: Optional CSV path receiving one row per
            candidate the engine deliberately declined to act on, tagged
            with a ``reason`` (see the module docstring).  ``None`` disables
            it.  Parent directories are created on first write.

    Sampling-floor configuration (read from the intersection config; see the
    module docstring for why these exist):

    ``min_pulse_floor_multiple``
        Rule 2 pulse-length gate, in multiples of the sampling floor.
        Default ``2.0``.
    ``high_duty_warn_fraction``
        Pair min-ON-duty above which the advisory WARNING fires.  Default
        ``0.8``.
    ``suppress_high_duty_pairs``
        When ``True``, Rules 1+2 are disabled entirely for pairs over that
        duty threshold.  Default ``False`` — advisory only.
    ``partner_blip_window_sec``
        Trailing horizon over which a detector's below-floor pulses are
        counted for the partner sub-floor-activity gate (ROADMAP 12A).
        Default ``300.0``; ``0`` disables the gate.
    ``partner_blip_max``
        Below-floor pulses on the partner inside that horizon that make its
        silence untrustworthy, declining the Rule 2 candidate.  Default
        ``5``; ``0`` disables the gate.

    Duplicate-rejection configuration (read from the intersection config; see
    the module docstring):

    ``dedup_window_sec``
        Seconds after a detector group's last emitted ``"start"`` during which
        a **Rule 2** candidate from another pair in the same group is rejected
        as a duplicate of it — subject to the coverage guard.  Default ``3.0``;
        ``0`` disables the Rule 2 path only.
    ``dedup_window_rule1_sec``
        The same window for a **Rule 1** candidate folding into a Rule 1 owner,
        where the AND-gated stop makes any width footage-safe.  Default
        ``10.0``; ``0`` disables the Rule 1 path only.

    The floor itself is **not** read from config here; it is injected by
    ``system_runner`` via :meth:`set_sampling_floor` (at startup from the
    config's ``sampling_floor_sec``, then from the detector monitor's measured
    cycle).  Until then the built-in default applies.

    Example::

        provider = JsonFileConfigProvider("/etc/traffic/intersections.json")
        monitor = DiscrepancyMonitor(
            intersection_id="1234_main",
            config_provider=provider,
            trigger_dir="./trigger_queue",
        )
        monitor.start()
    """

    def __init__(
        self,
        intersection_id: str,
        config_provider: ConfigProvider,
        trigger_dir: str | Path,
        cooldown_sec: float = 60.0,
        evaluator_interval_sec: float = 0.1,
        pre_roll_sec: float = 10.0,
        post_roll_sec: float = 20.0,
        max_duration_sec: float = 300.0,
        decision_log_path: Optional[str | Path] = None,
        suppression_log_path: Optional[str | Path] = None,
    ) -> None:
        self._intersection_id = intersection_id
        self._trigger_dir = Path(trigger_dir)
        self._cooldown_sec = cooldown_sec
        self._evaluator_interval_sec = evaluator_interval_sec
        self._pre_roll_sec = pre_roll_sec
        self._post_roll_sec = post_roll_sec
        self._max_duration_sec = max_duration_sec
        self._decision_log_path = (
            Path(decision_log_path) if decision_log_path else None
        )
        self._suppression_log_path = (
            Path(suppression_log_path) if suppression_log_path else None
        )
        self._log = logging.getLogger(f"{__name__}.{intersection_id}")

        self._trigger_dir.mkdir(parents=True, exist_ok=True)

        try:
            self._intersection_cfg = config_provider.get_intersection_config(
                intersection_id
            )
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Cannot load config for intersection '{intersection_id}': {exc}"
            ) from exc

        # ── Sampling floor (ROADMAP 9) ────────────────────────────────────
        # The floor itself is injected by the composition root; the tuning
        # knobs around it are engine-internal and come from the config block
        # the engine already owns.
        self._sampling_floor_sec = _DEFAULT_SAMPLING_FLOOR_SEC
        self._apply_floor_config()

        # ── Cross-pair duplicate rejection (ROADMAP 9C4, windows 14) ──────
        self._dedup_window_sec = _DEFAULT_DEDUP_WINDOW_SEC
        self._dedup_window_rule1_sec = _DEFAULT_DEDUP_WINDOW_RULE1_SEC
        self._apply_dedup_config()
        # (group_id, cameras_key) -> the last START actually written to the Hot
        # Folder.  Written and read only on the evaluator thread, so no lock.
        self._group_last_fire: Dict[Tuple[str, str], _GroupFire] = {}

        self._detector_states: Dict[str, _DetectorState] = {}
        self._pairs: Dict[str, Tuple[str, str]] = {}
        self._pair_runtime: Dict[str, _PairRuntimeState] = {}
        self._groups: Dict[str, List[str]] = {}
        self._pair_group: Dict[str, str] = {}
        self._build_structures()

        self._running = False
        self._evaluator_thread = threading.Thread(
            target=self._evaluator_loop,
            name=f"evaluator-{intersection_id}",
            daemon=True,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background evaluator thread.

        Idempotent — subsequent calls while running are silently ignored.
        """
        if self._running:
            return
        self._running = True
        self._evaluator_thread.start()
        self._log.info(
            "DiscrepancyMonitor started",
            extra={
                "intersection_id": self._intersection_id,
                "pairs": list(self._pairs.keys()),
                # Derived, not configured — log them so a transitively
                # over-grouped config is visible without reading the code.
                "groups": list(self._groups.keys()),
                "dedup_window_sec": self._dedup_window_sec,
                "dedup_window_rule1_sec": self._dedup_window_rule1_sec,
                "algorithm": "co-located-disagreement",
            },
        )

    def stop(self) -> None:
        """Signal the evaluator thread to exit and wait for it to join."""
        self._running = False
        self._evaluator_thread.join(timeout=self._evaluator_interval_sec * 5)
        self._log.info(
            "DiscrepancyMonitor stopped",
            extra={"intersection_id": self._intersection_id},
        )

    def reload(self, config_provider: ConfigProvider) -> None:
        """Hot-reload intersection configuration without stopping the monitor.

        Active cooldown state is preserved; transient fields (disagreement
        timers, orphan watches, Rule 1 active-tracking) are reset.

        Args:
            config_provider: Provider instance to fetch fresh config from.

        Raises:
            ValueError: If the intersection cannot be found or loaded.
        """
        try:
            new_cfg = config_provider.get_intersection_config(self._intersection_id)
        except (KeyError, ConfigProviderError) as exc:
            raise ValueError(
                f"Reload failed for intersection '{self._intersection_id}': {exc}"
            ) from exc
        self._intersection_cfg = new_cfg
        self._apply_floor_config()
        self._apply_dedup_config()
        self._build_structures(preserve_existing=True)
        self._log.info(
            "Configuration reloaded",
            extra={"intersection_id": self._intersection_id},
        )

    def _apply_floor_config(self) -> None:
        """Re-read the sampling-floor tuning knobs from the intersection config.

        Called from ``__init__`` and :meth:`reload`.  Does **not** touch
        ``_sampling_floor_sec`` itself — that value is owned by whoever calls
        :meth:`set_sampling_floor`, and a config reload must not clobber a
        measured floor with a stale assumption.
        """
        self._min_pulse_floor_multiple = float(
            self._intersection_cfg.get(
                "min_pulse_floor_multiple", _DEFAULT_MIN_PULSE_FLOOR_MULTIPLE
            )
        )
        self._high_duty_warn_fraction = float(
            self._intersection_cfg.get(
                "high_duty_warn_fraction", _DEFAULT_HIGH_DUTY_WARN_FRACTION
            )
        )
        self._suppress_high_duty_pairs = bool(
            self._intersection_cfg.get("suppress_high_duty_pairs", False)
        )
        # Partner sub-floor-activity gate (ROADMAP 12A).  Same validation
        # posture as _apply_dedup_config: a malformed or negative value falls
        # back to the default rather than silently disabling the gate; only an
        # explicit ``0`` on ``partner_blip_max`` turns it off.
        self._partner_blip_window_sec = _coerce_non_negative(
            self._intersection_cfg.get(
                "partner_blip_window_sec", _DEFAULT_PARTNER_BLIP_WINDOW_SEC
            ),
            _DEFAULT_PARTNER_BLIP_WINDOW_SEC,
        )
        self._partner_blip_max = int(
            _coerce_non_negative(
                self._intersection_cfg.get(
                    "partner_blip_max", _DEFAULT_PARTNER_BLIP_MAX
                ),
                float(_DEFAULT_PARTNER_BLIP_MAX),
            )
        )

    def _apply_dedup_config(self) -> None:
        """Re-read the cross-pair duplicate windows from the intersection config.

        Called from ``__init__`` and :meth:`reload`.  A malformed or negative
        value falls back to the default rather than disabling the mechanism —
        a typo must not silently restore the duplicate storm.  ``0`` disables
        one path, but only when written as an explicit zero: the two keys are
        independent, so ``dedup_window_sec: 0`` still leaves Rule 1 folding.
        """
        self._dedup_window_sec = _coerce_non_negative(
            self._intersection_cfg.get(
                "dedup_window_sec", _DEFAULT_DEDUP_WINDOW_SEC
            ),
            _DEFAULT_DEDUP_WINDOW_SEC,
        )
        self._dedup_window_rule1_sec = _coerce_non_negative(
            self._intersection_cfg.get(
                "dedup_window_rule1_sec", _DEFAULT_DEDUP_WINDOW_RULE1_SEC
            ),
            _DEFAULT_DEDUP_WINDOW_RULE1_SEC,
        )

    def set_sampling_floor(self, sec: float) -> None:
        """Set the effective sampling resolution of the upstream detector source.

        This is the engine's only channel for that information — the package
        boundary forbids importing ``ntcip_monitor``, so the composition root
        (``system_runner``) measures it there and pushes it here: once at
        startup from the config's ``sampling_floor_sec``, then periodically
        from ``DetectorMonitor.effective_cycle_sec()``.

        Thread-safe by construction: a single float assignment is atomic under
        CPython's GIL, and the evaluator tolerates reading the previous value
        for one tick.

        Args:
            sec: Measured (or configured) seconds between consecutive samples
                of a detector's state.  Non-positive values are ignored — an
                unmeasured source must not silently disable the gate.
        """
        try:
            value = float(sec)
        except (TypeError, ValueError):
            return
        if value <= 0.0:
            return

        previous = self._sampling_floor_sec
        self._sampling_floor_sec = value

        # Only log meaningful movement; the updater calls this once a minute.
        if abs(value - previous) >= 0.05:
            self._log.info(
                "Sampling floor updated",
                extra={
                    "intersection_id": self._intersection_id,
                    "sampling_floor_sec": round(value, 4),
                    "previous_sampling_floor_sec": round(previous, 4),
                    "min_orphan_pulse_sec": round(
                        value * self._min_pulse_floor_multiple, 4
                    ),
                },
            )

    def get_sampling_floor(self) -> float:
        """Get the sampling floor currently in force.

        Returns:
            Seconds between consecutive samples of a detector's state, as last
            set by :meth:`set_sampling_floor` (or the built-in default).
        """
        return self._sampling_floor_sec

    # ------------------------------------------------------------------
    # Non-blocking callbacks (called from NTCIP event thread)
    # ------------------------------------------------------------------

    def on_detector_on(self, detector_id: str) -> None:
        """Record a rising edge for ``detector_id``.

        Must execute in microseconds — no I/O, no blocking calls.
        Silently ignores unknown detector IDs.

        Args:
            detector_id: String detector identifier (coerced to ``str``).
        """
        key = str(detector_id)
        state = self._detector_states.get(key)
        if state is None:
            return
        with state.lock:
            state.is_on = True
            now = time.time()
            state.last_on_time = now
            state.last_pulse_on_time = now

    def on_detector_off(self, detector_id: str) -> None:
        """Record a falling edge for ``detector_id``.

        Must execute in microseconds — no I/O, no blocking calls.
        Also initiates an early-cooldown-reset check.

        Args:
            detector_id: String detector identifier (coerced to ``str``).
        """
        key = str(detector_id)
        state = self._detector_states.get(key)
        if state is None:
            return
        with state.lock:
            state.is_on = False
            now = time.time()
            state.last_off_time = now
            # last_pulse_on_time intentionally NOT updated here — it retains
            # the ON-edge value so the evaluator can form [pulse_on, pulse_off].
            if state.last_pulse_on_time > 0.0:
                # Close out the ON interval for the partner-overlap history.
                # O(1) append under the already-held lock; pruning is the
                # evaluator thread's job.
                state.on_intervals.append((state.last_pulse_on_time, now))

        self._maybe_reset_cooldown_early(key)

    # ------------------------------------------------------------------
    # Background evaluator
    # ------------------------------------------------------------------

    def _evaluator_loop(self) -> None:
        """Daemon loop: evaluate all configured pairs on every tick."""
        while self._running:
            for pair_key, (det_a_id, det_b_id) in self._pairs.items():
                try:
                    self._evaluate_pair(pair_key, det_a_id, det_b_id)
                except Exception:  # noqa: BLE001
                    self._log.exception(
                        "Unhandled error in evaluator loop",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key": pair_key,
                        },
                    )
            time.sleep(self._evaluator_interval_sec)

    def _evaluate_pair(
        self,
        pair_key: str,
        det_a_id: str,
        det_b_id: str,
    ) -> None:
        """Apply Rules 1–3 to a single co-located detector pair each tick.

        Execution order:

        0. **Held guard** — return immediately while this pair's trigger is
           folded into a sibling pair's open recording (ROADMAP 9C4).
        1. **Cooldown guard** — return immediately while in cooldown.
        2. **Rule 1 resolution state machine** — if ``rt.active_trigger_id``
           is set, manage the resolution countdown and send a ``"stop"``
           trigger when post-roll elapses — waiting for *every* held pair to
           resolve too, not just this one.  Always ``return`` after this block.
        3. **State snapshot** — read both detectors under brief locks.
        3b. **High-duty advisory** — recompute the pair's rolling ON-duty at
           most every ``_DUTY_EVAL_INTERVAL_SEC``; warn (rate-limited) when it
           exceeds ``high_duty_warn_fraction``, and return early only if
           ``suppress_high_duty_pairs`` is enabled.
        4. **Rule 1 new-detection** — start/continue disagreement timer; fire
           ``"start"`` trigger (no cooldown) if threshold exceeded.
        5. **Rule 2 orphan detection** — register candidates (subject to the
           sampling-floor gate); fire ``"start"`` trigger with exact
           ``duration_override`` when confirmed.

        Args:
            pair_key: Canonical pair key used for runtime state lookup.
            det_a_id: First detector ID in the pair.
            det_b_id: Second detector ID in the pair.
        """
        rt  = self._pair_runtime[pair_key]
        now = time.time()

        # ── 0. Held-by-another-pair guard ─────────────────────────────────
        # This pair's trigger was folded into a sibling pair's recording as a
        # cross-pair duplicate, and that recording is still open — it is
        # already capturing this disagreement, and that recording's stop is
        # waiting on this pair (ROADMAP 9C4).  Run no rules until it closes.
        # Ahead of the cooldown guard on purpose: the early-cooldown-reset
        # path can clear a cooldown from the callback thread, and a held pair
        # must stay quiet regardless.
        if rt.held_by_pair_key is not None:
            owner_rt = self._pair_runtime.get(rt.held_by_pair_key)
            if owner_rt is not None and owner_rt.active_trigger_id is not None:
                # Clear the timer as we go, for the same reason the high-duty
                # suppression does: a stale start would measure a disagreement
                # from before the hold and fire the moment the hold lifts.
                rt.disagreement_start = None
                return
            # The owner's recording ended without releasing us (it was
            # abandoned, or the config reloaded underneath it).  Resume.
            rt.held_by_pair_key = None

        # ── 1. Cooldown guard ─────────────────────────────────────────────
        if rt.cooldown_active:
            if now - rt.triggered_at < self._cooldown_sec:
                return
            rt.cooldown_active = False
            self._log.debug(
                "Pair cooldown expired",
                extra={
                    "intersection_id": self._intersection_id,
                    "pair_key": pair_key,
                },
            )

        # ── 2. Rule 1 active-resolution state machine ─────────────────────
        #
        # This block executes only when a Rule 1 "start" trigger has been
        # sent and we are waiting for the disagreement to resolve and the
        # post-roll to elapse before sending the "stop" trigger.
        if rt.active_trigger_id is not None:
            state_a = self._detector_states.get(det_a_id)
            state_b = self._detector_states.get(det_b_id)
            if state_a is None or state_b is None:
                # Detectors vanished during a reload; abandon the recording.
                rt.active_trigger_id     = None
                rt.resolution_start_time = None
                self._release_held_pairs(rt, now)
                return

            with state_a.lock:
                a_is_on = state_a.is_on
            with state_b.lock:
                b_is_on = state_b.is_on

            # Resolution = both detectors agree (both ON or both OFF) — and so
            # does every pair whose own trigger was folded into this recording
            # as a cross-pair duplicate.  The clip stands for all of them, so
            # the pair that happened to fire first does not get to decide alone
            # when the footage ends (ROADMAP 9C4).
            both_agree = (a_is_on == b_is_on) and self._held_pairs_agree(rt)

            if both_agree:
                # Start the post-roll countdown on the first tick of agreement.
                if rt.resolution_start_time is None:
                    rt.resolution_start_time = now
                    self._log.debug(
                        "Rule 1 disagreement resolved — post-roll countdown started",
                        extra={
                            "intersection_id":  self._intersection_id,
                            "pair_key":         pair_key,
                            "trigger_id":       rt.active_trigger_id,
                            "post_roll_sec":    self._post_roll_sec,
                        },
                    )

                # Send the "stop" trigger once post-roll has elapsed.
                post_roll_elapsed = now - rt.resolution_start_time
                if post_roll_elapsed >= self._post_roll_sec:
                    self._fire_trigger(
                        pair_key=pair_key,
                        det_a_id=det_a_id,
                        det_b_id=det_b_id,
                        rule="rule1_continuous_disagreement",
                        description=(
                            f"Rule 1 recording stopped after "
                            f"{post_roll_elapsed:.1f}s post-roll"
                        ),
                        disagreement_sec=0.0,   # not meaningful for stop
                        event_ts=now,
                        action="stop",
                        trigger_id_override=rt.active_trigger_id,
                    )
                    # State cleanup and cooldown engagement are handled inside
                    # _fire_trigger when action == "stop".
            else:
                # Still disagreeing — reset the post-roll countdown.  If the
                # detectors briefly agree then diverge again (tail-end chatter),
                # the countdown restarts from zero.
                if rt.resolution_start_time is not None:
                    self._log.debug(
                        "Rule 1 re-diverged during post-roll — timer reset",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key":        pair_key,
                            "trigger_id":      rt.active_trigger_id,
                        },
                    )
                rt.resolution_start_time = None

            # CRITICAL: always return here.  While a Rule 1 recording is
            # active we must not evaluate new Rule 1 or Rule 2 candidates —
            # doing so could fire a second "start" trigger for the same pair.
            return

        # ── 3. Snapshot both detector states ──────────────────────────────
        state_a = self._detector_states.get(det_a_id)
        state_b = self._detector_states.get(det_b_id)
        if state_a is None or state_b is None:
            return

        det_cfg   = self._intersection_cfg["detectors"].get(det_a_id, {})
        threshold = float(det_cfg.get("lag_threshold_sec", 2.0))

        # Rule 2 can never be intersected by an ON interval older than
        # 3×threshold + grace (window span plus verdict staleness), but the
        # ON-duty advisory reads the same deque over a much longer window, so
        # the retention horizon is the larger of the two.  Pruning happens
        # while the lock is held; the callback path never prunes.
        prune_before = now - max(
            3.0 * threshold + _ORPHAN_DECISION_GRACE_SEC + 1.0,
            _DUTY_WINDOW_SEC,
        )

        with state_a.lock:
            a_is_on         = state_a.is_on
            a_last_on       = state_a.last_on_time
            a_last_off      = state_a.last_off_time
            a_last_pulse_on = state_a.last_pulse_on_time
            while state_a.on_intervals and state_a.on_intervals[0][1] < prune_before:
                state_a.on_intervals.popleft()
            a_intervals     = tuple(state_a.on_intervals)

        with state_b.lock:
            b_is_on         = state_b.is_on
            b_last_on       = state_b.last_on_time
            b_last_off      = state_b.last_off_time
            b_last_pulse_on = state_b.last_pulse_on_time
            while state_b.on_intervals and state_b.on_intervals[0][1] < prune_before:
                state_b.on_intervals.popleft()
            b_intervals     = tuple(state_b.on_intervals)

        # Sub-floor blip history (ROADMAP 12A) lives on its own, much longer
        # horizon and is evaluator-thread-only, so it is pruned outside the
        # per-detector locks rather than inside them.
        blip_prune_before = now - self._partner_blip_window_sec
        for blip_state in (state_a, state_b):
            while (
                blip_state.below_floor_pulses
                and blip_state.below_floor_pulses[0][1] < blip_prune_before
            ):
                blip_state.below_floor_pulses.popleft()
        a_blips = tuple(state_a.below_floor_pulses)
        b_blips = tuple(state_b.below_floor_pulses)

        # ── 3b. High-duty advisory ────────────────────────────────────────
        if now - rt.last_duty_eval_ts >= _DUTY_EVAL_INTERVAL_SEC:
            rt.last_duty_eval_ts = now
            duty_a = _compute_on_duty_fraction(
                a_intervals, a_is_on, a_last_on, now, _DUTY_WINDOW_SEC
            )
            duty_b = _compute_on_duty_fraction(
                b_intervals, b_is_on, b_last_on, now, _DUTY_WINDOW_SEC
            )
            rt.pair_min_duty    = min(duty_a, duty_b)
            rt.high_duty_active = rt.pair_min_duty > self._high_duty_warn_fraction

            if (
                rt.high_duty_active
                and now - rt.last_high_duty_warn_ts >= _HIGH_DUTY_WARN_INTERVAL_SEC
            ):
                rt.last_high_duty_warn_ts = now
                self._log.warning(
                    "Pair operates above the NTCIP sampling-reliability regime",
                    extra={
                        "intersection_id":         self._intersection_id,
                        "pair_key":                pair_key,
                        "duty_a":                  round(duty_a, 3),
                        "duty_b":                  round(duty_b, 3),
                        "duty_window_sec":         _DUTY_WINDOW_SEC,
                        "high_duty_warn_fraction": self._high_duty_warn_fraction,
                        "sampling_floor_sec":      round(self._sampling_floor_sec, 4),
                        "suppressed":              self._suppress_high_duty_pairs,
                    },
                )

        if rt.high_duty_active and self._suppress_high_duty_pairs:
            # Opt-in deployment decision: this pair's edges are not resolvable
            # at the current sampling rate, so run no rules against it at all.
            # Clear the Rule 1 timer as we go: leaving it set would mean that
            # the first tick after the pair's duty falls back below the
            # threshold measures a disagreement from a timestamp before the
            # suppression began, and fires immediately.
            rt.disagreement_start = None
            return

        # ── 4. Rule 1 — Continuous Disagreement (new-detection path) ─────
        both_agree = (a_is_on == b_is_on)

        if both_agree:
            rt.disagreement_start = None
        else:
            if rt.disagreement_start is None:
                rt.disagreement_start = now

            fire, duration = _check_rule1_continuous(
                rt.disagreement_start, now, threshold
            )
            if fire:
                leading = det_a_id if a_is_on else det_b_id
                lagging = det_b_id if a_is_on else det_a_id
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule1_continuous_disagreement",
                    description=(
                        f"detector '{leading}' ON, detector '{lagging}' OFF "
                        f"for {duration}s (threshold={threshold}s)"
                    ),
                    disagreement_sec=duration,
                    event_ts=now,
                    action="start",
                    # No duration_override for Rule 1 — max_duration_sec is
                    # only a hard safety cap; the engine sends an explicit stop.
                    # The disagreement is still open, so it has no end yet.
                    event_window=(now - duration, None),
                )
                # Reset timer; active_trigger_id now set by _fire_trigger.
                rt.disagreement_start = None
                return  # skip Rule 2 on this tick

        # ── 5. Rule 2 — Orphan Pulse ──────────────────────────────────────
        # Sampling-floor gate: a pulse the source could not have resolved is
        # not evidence of anything (ROADMAP 9).  Read the floor once so both
        # slots are judged against the same value even if it changes mid-tick,
        # and so the suppression log records the gate that was actually applied
        # rather than whatever the floor updater has moved on to by log time.
        floor_sec     = self._sampling_floor_sec
        floor_multiple = self._min_pulse_floor_multiple
        min_pulse_sec = floor_sec * floor_multiple

        # The registration helper stays pure and static (it is exercised
        # directly by the unit tests); it reports what it suppressed and this
        # caller, which has the instance, does the I/O.
        # Each slot is judged against the *partner's* blip history: slot "a"'s
        # evidence is detector B's silence, so it is B's sub-floor behaviour
        # that decides whether that silence can be trusted (ROADMAP 12A).
        suppressed_a = self._maybe_register_orphan(
            rt, "a", a_is_on, a_last_pulse_on, a_last_off, threshold,
            min_pulse_sec,
            b_blips, now,
            self._partner_blip_window_sec, self._partner_blip_max,
        )
        suppressed_b = self._maybe_register_orphan(
            rt, "b", b_is_on, b_last_pulse_on, b_last_off, threshold,
            min_pulse_sec,
            a_blips, now,
            self._partner_blip_window_sec, self._partner_blip_max,
        )
        for slot, orphan_id, orphan_state, suppressed in (
            ("a", det_a_id, state_a, suppressed_a),
            ("b", det_b_id, state_b, suppressed_b),
        ):
            if suppressed is None:
                continue
            if suppressed.reason == _SUPPRESS_BELOW_FLOOR:
                # Remember the blip on the detector that produced it, deduped
                # against the tail: in a triangle the same physical pulse is
                # declined once per pair it participates in, and the gate
                # counts distinct pulses, not evaluations of them.
                blips = orphan_state.below_floor_pulses
                if not blips or blips[-1] != suppressed.pulse:
                    blips.append(suppressed.pulse)
            self._log_suppression(
                reason=suppressed.reason,
                pair_key=pair_key,
                det_a_id=det_a_id,
                det_b_id=det_b_id,
                slot=slot,
                orphan_det_id=orphan_id,
                pulse=suppressed.pulse,
                min_pulse_sec=min_pulse_sec,
                floor_sec=floor_sec,
                floor_multiple=floor_multiple,
                now=now,
                partner_blip_count=(
                    suppressed.partner_blip_count
                    if suppressed.reason == _SUPPRESS_PARTNER_BLIP else None
                ),
                partner_blip_window_sec=(
                    self._partner_blip_window_sec
                    if suppressed.reason == _SUPPRESS_PARTNER_BLIP else None
                ),
            )

        # Evaluate detector-A orphan candidate against detector-B history.
        if rt.orphan_watch_a is not None:
            pulse_on, pulse_off = rt.orphan_watch_a
            fire, desc = _check_rule2_orphan(
                pulse_on, pulse_off,
                b_intervals, b_is_on,
                now, threshold,
            )
            if fire:
                disagreement_sec = round(pulse_off - pulse_on, 3)
                # Exact clip duration: pre-roll already buffered in RAM +
                # the orphan pulse window + post-roll + the threshold we
                # waited to confirm the orphan.
                exact_duration = (
                    self._pre_roll_sec
                    + disagreement_sec
                    + self._post_roll_sec
                    + threshold
                )
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule2_orphan_pulse",
                    description=f"orphan on detector '{det_a_id}': {desc}",
                    disagreement_sec=disagreement_sec,
                    event_ts=now,
                    action="start",
                    duration_override=exact_duration,
                    event_window=(pulse_on, pulse_off),
                )
                rt.orphan_watch_a = None
                return
            if now >= pulse_off + threshold:
                rt.orphan_watch_a = None

        # Evaluate detector-B orphan candidate against detector-A history.
        if rt.orphan_watch_b is not None:
            pulse_on, pulse_off = rt.orphan_watch_b
            fire, desc = _check_rule2_orphan(
                pulse_on, pulse_off,
                a_intervals, a_is_on,
                now, threshold,
            )
            if fire:
                disagreement_sec = round(pulse_off - pulse_on, 3)
                exact_duration = (
                    self._pre_roll_sec
                    + disagreement_sec
                    + self._post_roll_sec
                    + threshold
                )
                self._fire_trigger(
                    pair_key=pair_key,
                    det_a_id=det_a_id,
                    det_b_id=det_b_id,
                    rule="rule2_orphan_pulse",
                    description=f"orphan on detector '{det_b_id}': {desc}",
                    disagreement_sec=disagreement_sec,
                    event_ts=now,
                    action="start",
                    duration_override=exact_duration,
                    event_window=(pulse_on, pulse_off),
                )
                rt.orphan_watch_b = None
                return
            if now >= pulse_off + threshold:
                rt.orphan_watch_b = None

    # ------------------------------------------------------------------
    # Orphan candidate registration
    # ------------------------------------------------------------------

    @staticmethod
    def _maybe_register_orphan(
        rt: _PairRuntimeState,
        which: str,
        is_on: bool,
        last_pulse_on: float,
        last_off: float,
        threshold: float,
        min_pulse_sec: float = 0.0,
        partner_blip_pulses: Sequence[Tuple[float, float]] = (),
        now: float = 0.0,
        partner_blip_window_sec: float = 0.0,
        partner_blip_max: int = 0,
    ) -> Optional[_OrphanSuppression]:
        """Register a new Rule-2 orphan candidate if one is not already tracked.

        A candidate is registered when ALL of the following hold:

        * The detector is currently OFF (the pulse has ended).
        * ``last_off`` is non-zero (an OFF edge has been observed).
        * ``last_pulse_on`` is non-zero (an ON edge has been observed).
        * The candidate differs from any already-registered candidate for
          this slot (prevents re-registering the same pulse each tick).
        * The pulse's ON-edge is newer than the last pulse already handled for
          this slot.  A pulse's ``last_pulse_on`` is monotonic (updated only on
          a fresh rising edge), so once a pulse has been armed it is never
          re-armed — even after its trigger fires and the post-trigger cooldown
          later expires while the detector's state is unchanged.  Without this
          guard a single stale ghost pulse re-fires once per cooldown period.
        * The pulse ON-duration is strictly less than ``threshold``.  Longer
          pulses are handled by Rule 1 and excluded here to avoid
          double-triggering.
        * The pulse ON-duration is at least ``min_pulse_sec`` — the sampling
          floor gate (ROADMAP 9).  A shorter pulse is below the resolution of
          the source that reported it, so its "the partner never saw it"
          counterpart is equally likely to be an unseen sample as a real
          absence.  Rejected pulses bump ``rt.below_floor_suppressed`` and are
          marked handled, so each distinct pulse is counted (and logged) once
          rather than on every 0.1 s tick it remains the detector's last pulse.
        * The **partner** has produced fewer than ``partner_blip_max``
          below-floor pulses in the trailing ``partner_blip_window_sec`` — the
          partner sub-floor-activity gate (ROADMAP 12A).  Rule 2's evidence is
          the partner's *silence*; a partner that keeps blipping below the
          engine's own resolution is one whose silence the engine cannot
          observe reliably, so the candidate is declined.  Rejected pulses bump
          ``rt.partner_blip_suppressed`` and are marked handled the same way.

        The two gates are ordered and the order is load-bearing: a below-floor
        pulse must be counted and reported as ``below_sampling_floor``, never
        as ``partner_below_floor_activity``.  The populations are therefore
        disjoint, and the second gate only ever judges pulses the engine
        considers resolvable.

        Args:
            rt: The pair runtime state object to update.
            which: ``"a"`` or ``"b"`` — which orphan slot to populate.
            is_on: Current ON state of the detector.
            last_pulse_on: ``last_pulse_on_time`` from the detector snapshot.
            last_off: ``last_off_time`` from the detector snapshot.
            threshold: Allowable disagreement window in seconds.
            min_pulse_sec: Sampling-floor gate in seconds
                (``floor × min_pulse_floor_multiple``).  ``0.0`` disables the
                gate — used only by callers that have no floor to apply.
            partner_blip_pulses: Snapshot of the **partner** detector's
                ``below_floor_pulses`` deque, as ``(on_ts, off_ts)`` tuples.
            now: Evaluator-tick timestamp, used only as the right edge of the
                partner-blip horizon.
            partner_blip_window_sec: Trailing horizon over which those pulses
                are counted.  ``0.0`` disables the partner gate.
            partner_blip_max: Number of partner below-floor pulses inside the
                horizon that makes the partner's silence untrustworthy.  ``0``
                disables the partner gate.

        Returns:
            An :class:`_OrphanSuppression` describing the pulse this call
            declined and which gate declined it, or ``None`` in every other
            case — including a successful registration and every early return.
            This method stays static and side-effect-free apart from ``rt``;
            the caller owns the suppression log, so the reporting seam is a
            return value rather than a write from here.  Because a suppressed
            pulse is marked handled, a given pulse is reported at most once,
            not once per tick.
        """
        if is_on or last_pulse_on == 0.0 or last_off == 0.0:
            return None

        pulse_duration = last_off - last_pulse_on
        if pulse_duration <= 0 or pulse_duration >= threshold:
            return None

        attr: str = f"orphan_watch_{which}"
        existing: Optional[Tuple[float, float]] = getattr(rt, attr)
        if existing is not None and existing[0] == last_pulse_on:
            return None  # Already watching this exact pulse.

        handled_attr = f"last_handled_pulse_on_{which}"
        if last_pulse_on <= getattr(rt, handled_attr):
            return None  # This pulse was already armed once; don't re-arm it
                         # after a cooldown while the detector state is
                         # unchanged.

        if min_pulse_sec > 0.0 and pulse_duration < min_pulse_sec:
            # Below the sampling floor — not evidence, aliasing.  Marked
            # handled so this pulse is accounted for exactly once.
            rt.below_floor_suppressed += 1
            setattr(rt, handled_attr, last_pulse_on)
            log.debug(
                "Orphan candidate below sampling floor — not registered",
                extra={
                    "pair_key": rt.pair_key,
                    "slot": which,
                    "pulse_duration_sec": round(pulse_duration, 3),
                    "min_pulse_sec": round(min_pulse_sec, 3),
                    "below_floor_suppressed": rt.below_floor_suppressed,
                },
            )
            return _OrphanSuppression(
                _SUPPRESS_BELOW_FLOOR, (last_pulse_on, last_off)
            )

        # Partner sub-floor-activity gate (ROADMAP 12A).  Strictly after the
        # floor gate above: the pulse in hand is one the engine can resolve,
        # and what is in question is whether the *partner's* silence means
        # anything.  Counts pulses, not ticks — the deque holds one entry per
        # distinct declined pulse, deduped by the caller.
        if partner_blip_max > 0 and partner_blip_window_sec > 0.0:
            horizon = now - partner_blip_window_sec
            blip_count = sum(
                1 for _blip_on, blip_off in partner_blip_pulses
                if blip_off > horizon
            )
            if blip_count >= partner_blip_max:
                rt.partner_blip_suppressed += 1
                setattr(rt, handled_attr, last_pulse_on)
                log.debug(
                    "Orphan candidate declined — partner blips below the "
                    "sampling floor too often for its silence to be evidence",
                    extra={
                        "pair_key": rt.pair_key,
                        "slot": which,
                        "pulse_duration_sec": round(pulse_duration, 3),
                        "partner_blip_count": blip_count,
                        "partner_blip_max": partner_blip_max,
                        "partner_blip_window_sec": partner_blip_window_sec,
                        "partner_blip_suppressed": rt.partner_blip_suppressed,
                    },
                )
                return _OrphanSuppression(
                    _SUPPRESS_PARTNER_BLIP, (last_pulse_on, last_off),
                    blip_count,
                )

        setattr(rt, attr, (last_pulse_on, last_off))
        setattr(rt, handled_attr, last_pulse_on)
        return None

    # ------------------------------------------------------------------
    # Early cooldown reset (called from NTCIP callback thread)
    # ------------------------------------------------------------------

    def _maybe_reset_cooldown_early(self, detector_id: str) -> None:
        """Lift the cooldown for any pair containing ``detector_id`` if both
        detectors are now OFF.

        Called from ``on_detector_off`` in the NTCIP event thread.  Must not
        perform any I/O.

        This method does **not** touch ``active_trigger_id`` or
        ``resolution_start_time``; those are owned exclusively by the
        evaluator thread's state machine.

        Args:
            detector_id: The detector that just transitioned to OFF.
        """
        for pair_key, (det_a_id, det_b_id) in self._pairs.items():
            if detector_id not in (det_a_id, det_b_id):
                continue

            partner_id = det_b_id if detector_id == det_a_id else det_a_id
            partner_state = self._detector_states.get(partner_id)
            if partner_state is None:
                continue

            with partner_state.lock:
                partner_is_on = partner_state.is_on

            if not partner_is_on:
                rt = self._pair_runtime.get(pair_key)
                if rt is not None and rt.cooldown_active:
                    rt.cooldown_active = False
                    self._log.debug(
                        "Cooldown reset early — both detectors OFF",
                        extra={
                            "intersection_id": self._intersection_id,
                            "pair_key": pair_key,
                        },
                    )

    # ------------------------------------------------------------------
    # Trigger file generation
    # ------------------------------------------------------------------

    def _fire_trigger(
        self,
        pair_key: str,
        det_a_id: str,
        det_b_id: str,
        rule: str,
        description: str,
        disagreement_sec: float,
        event_ts: float,
        action: str = "start",
        duration_override: Optional[float] = None,
        trigger_id_override: Optional[str] = None,
        event_window: Optional[Tuple[Optional[float], Optional[float]]] = None,
    ) -> None:
        """Write an atomic trigger JSON file and update pair runtime state.

        The file is written to a ``.tmp`` path first, then atomically renamed
        to ``.json`` so the video-buffer poller never reads a partial file.

        **State management after a successful write:**

        +------------------------------------------+----------------------------------+
        | Condition                                | Outcome                          |
        +==========================================+==================================+
        | ``action == "stop"``                     | Clear ``active_trigger_id`` and  |
        |                                          | ``resolution_start_time``;       |
        |                                          | engage cooldown.                 |
        +------------------------------------------+----------------------------------+
        | ``action == "start"`` and                | Set ``active_trigger_id``        |
        | ``rule == "rule1_continuous_…"``         | to new trigger ID; clear         |
        |                                          | ``resolution_start_time``.       |
        |                                          | Cooldown is NOT engaged.         |
        +------------------------------------------+----------------------------------+
        | ``action == "start"`` and any other rule | Engage cooldown immediately.     |
        | (e.g. ``"rule2_orphan_pulse"``)          | ``active_trigger_id`` unchanged. |
        +------------------------------------------+----------------------------------+

        Args:
            pair_key: Canonical pair key used for runtime state lookup.
            det_a_id: First detector ID.
            det_b_id: Second detector ID.
            rule: Rule identifier string (e.g. ``"rule1_continuous_disagreement"``).
            description: Human-readable description for trigger metadata.
            disagreement_sec: Measured disagreement duration in seconds.
            event_ts: Unix timestamp when the discrepancy was detected.
            action: ``"start"`` or ``"stop"``.  Defaults to ``"start"``.
            duration_override: When provided, overrides ``self._max_duration_sec``
                as the ``max_duration_sec`` field in the payload.  Pass the
                exact calculated clip duration for Rule 2 triggers.
            trigger_id_override: When provided (for ``action == "stop"``), use
                this hex string as ``trigger_id`` so the video-buffer layer can
                correlate the stop with its start.  A fresh UUID is generated
                when ``None`` (appropriate for ``"start"`` actions).
            event_window: ``(start_ts, end_ts)`` of the *underlying detector
                event* in Unix time, recorded verbatim in the decision log so a
                consumer never has to reconstruct it from a local timestamp and
                a description.  Either element may be ``None`` where the rule
                does not define it (Rule 1's ``start`` knows the beginning of
                the disagreement but not its end).  Not part of the trigger
                payload — the video buffer has no use for it, and the Hot
                Folder schema is deliberately hard to grow.

        A fourth outcome sits ahead of the table above: a ``start`` rejected as
        a **cross-pair duplicate** (ROADMAP 9C4) writes no trigger file at all.
        It is still recorded in the decision log, marked, and the pair engages
        cooldown — including for Rule 1, which must *not* arm
        ``active_trigger_id`` for a trigger the buffer never received.  See the
        module docstring.
        """
        rt = self._pair_runtime[pair_key]

        # For "stop" actions we must reuse the same trigger_id as the
        # corresponding "start" so the video-buffer can match the pair.
        trigger_id = trigger_id_override if trigger_id_override else uuid.uuid4().hex

        # Resolve the intersection-local timezone for the filename timestamp and
        # the payload field.  _resolve_pytz uses pytz's bundled IANA database
        # so it works regardless of system zone files or tzdata.
        tz_name: str = self._intersection_cfg.get("timezone", "UTC")
        local_tz = _resolve_pytz(tz_name, self._log)
        # Normalise tz_name in case _resolve_pytz fell back to UTC.
        if local_tz is pytz.utc:
            tz_name = "UTC"

        iso_ts    = datetime.fromtimestamp(event_ts, tz=local_tz).strftime(
            "%Y%m%dT%H%M%S%f"
        )
        stem      = f"trigger_{iso_ts}_{trigger_id[:8]}"
        tmp_path  = self._trigger_dir / f"{stem}.tmp"
        json_path = self._trigger_dir / f"{stem}.json"

        cameras   = self._cameras_for_pair(det_a_id, det_b_id)
        det_a_cfg = self._intersection_cfg["detectors"].get(det_a_id, {})
        det_b_cfg = self._intersection_cfg["detectors"].get(det_b_id, {})

        payload = {
            "trigger_id":       trigger_id,
            "action":           action,
            "event_timestamp":  event_ts,
            "reason":           "detector_disagreement",
            "intersection_id":  self._intersection_id,
            "timezone":         tz_name,
            "cameras":          cameras,
            "pre_roll_sec":     self._pre_roll_sec,
            "post_roll_sec":    self._post_roll_sec,
            "max_duration_sec": (
                duration_override if duration_override is not None
                else self._max_duration_sec
            ),
            "metadata": {
                "det_a":            det_a_id,
                "det_b":            det_b_id,
                "det_a_type":       det_a_cfg.get("type", "unknown"),
                "det_b_type":       det_b_cfg.get("type", "unknown"),
                "det_a_phase":      det_a_cfg.get("phase"),
                "det_b_phase":      det_b_cfg.get("phase"),
                "rule":             rule,
                "description":      description,
                "disagreement_sec": disagreement_sec,
            },
        }

        # ── Cross-pair duplicate rejection (ROADMAP 9C4) ──────────────────
        # Deliberately here: after the payload is fully formed (so the
        # decision log's row is identical to a delivered one bar the three
        # marker columns) and before the tmp-write (so nothing reaches the
        # Hot Folder).
        group_id    = self._pair_group.get(pair_key, pair_key)
        cameras_key = ";".join(cameras)
        duplicate   = self._duplicate_within_group(
            action, rule, group_id, cameras_key, event_ts, event_window
        )

        if duplicate is not None:
            held = self._join_owner_recording(pair_key, rt, rule, duplicate)
            self._log.info(
                "Duplicate trigger suppressed",
                extra={
                    "intersection_id":         self._intersection_id,
                    "trigger_id":              trigger_id,
                    "pair_key":                pair_key,
                    "dedup_group":             group_id,
                    "duplicate_of_trigger_id": duplicate.trigger_id,
                    "owner_pair_key":          duplicate.pair_key,
                    "rule":                    rule,
                    "cameras":                 cameras,
                    "dedup_window_sec":        (
                        self._dedup_window_rule1_sec
                        if rule == "rule1_continuous_disagreement"
                        else self._dedup_window_sec
                    ),
                    # True when the owner's stop now waits on this pair too.
                    "held_open_by_owner":      held,
                },
            )
            self._log_decision(
                payload, pair_key, event_window, local_tz,
                dedup_group=group_id, duplicate_of=duplicate.trigger_id,
            )
            # No trigger file exists, so the Rule 1 resolution state machine
            # must NOT be armed *here* — a later "stop" would reference a
            # recording the buffer never started.  Cooldown instead, exactly as
            # a Rule 2 start does: the group *is* recording this moment on
            # another pair, and the pair must not re-fire on the same physical
            # event.  When the owner holds an open Rule 1 recording, the line
            # above additionally makes that recording wait for this pair's
            # disagreement to resolve before it stops.
            rt.cooldown_active    = True
            rt.triggered_at       = time.time()
            rt.disagreement_start = None
            return

        try:
            tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.rename(tmp_path, json_path)
        except OSError as exc:
            self._log.error(
                "Failed to write trigger file",
                extra={
                    "intersection_id": self._intersection_id,
                    "pair_key":   pair_key,
                    "trigger_id": trigger_id,
                    "action":     action,
                    "error":      str(exc),
                },
            )
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
            return

        self._log.info(
            "Trigger sent",
            extra={
                "trigger_id":       trigger_id,
                "action":           action,
                "intersection_id":  self._intersection_id,
                "pair_key":         pair_key,
                "rule":             rule,
                "disagreement_sec": disagreement_sec,
                "max_duration_sec": payload["max_duration_sec"],
                "cameras":          cameras,
                "path":             str(json_path),
            },
        )

        # Record the decision now that it has actually been handed off.  This
        # is deliberately after the rename and before any state management:
        # what the log claims the engine emitted is exactly what reached the
        # Hot Folder, whether or not the buffer later finds a writer slot.
        self._log_decision(
            payload, pair_key, event_window, local_tz,
            dedup_group=group_id, duplicate_of=None,
        )

        # Anchor the group's duplicate window on *emitted* starts only.  A
        # suppressed row never lands here, so a storm cannot roll the window
        # forward indefinitely; a "stop" never lands here either, or closing
        # one recording would suppress the next genuine event.
        if action == "start":
            # A Rule 2 clip's span is fully determined here — the buffer starts
            # it pre_roll before the event and stops it max_duration_sec later
            # — so record it for the coverage guard.  Rule 1 leaves it None:
            # its clip is open-ended and is judged by liveness instead.
            span_start = span_end = None
            if rule != "rule1_continuous_disagreement":
                event_start = event_window[0] if event_window else None
                # Same fallback as the candidate side of the guard: without an
                # event window the trigger's own timestamp is the best anchor
                # available.  Production Rule 2 always supplies one.
                span_start = (
                    event_start if event_start is not None else event_ts
                ) - self._pre_roll_sec
                span_end = span_start + float(payload["max_duration_sec"])
            self._group_last_fire[(group_id, cameras_key)] = _GroupFire(
                fire_ts=event_ts, trigger_id=trigger_id,
                pair_key=pair_key, rule=rule,
                span_start=span_start, span_end=span_end,
            )

        # ── Post-write state management ───────────────────────────────────

        if action == "stop":
            # Clear active Rule 1 tracking and engage cooldown.
            rt.active_trigger_id     = None
            rt.resolution_start_time = None
            rt.cooldown_active       = True
            rt.triggered_at          = time.time()
            rt.disagreement_start    = None
            # The recording covered the held pairs' disagreements too; give
            # them their own cooldown rather than returning them to service on
            # the tail of the event that was just recorded.
            self._release_held_pairs(rt, rt.triggered_at)

        elif rule == "rule1_continuous_disagreement":
            # START for Rule 1: arm the resolution state machine.
            # Cooldown is deliberately NOT engaged here — the evaluator will
            # engage it after the paired "stop" trigger is sent.
            rt.active_trigger_id     = trigger_id
            rt.resolution_start_time = None
            # disagreement_start was already reset by the caller.

        else:
            # START for Rule 2 (or any future instantaneous rule): the clip
            # duration is fully self-contained, so engage cooldown immediately.
            rt.cooldown_active    = True
            rt.triggered_at       = time.time()
            rt.disagreement_start = None

    def _duplicate_within_group(
        self,
        action: str,
        rule: str,
        group_id: str,
        cameras_key: str,
        event_ts: float,
        event_window: Optional[Tuple[Optional[float], Optional[float]]] = None,
    ) -> Optional[_GroupFire]:
        """Return the group fire this trigger duplicates, or ``None`` to proceed.

        A ``start`` is a duplicate when another pair in the same derived
        detector group emitted a ``start`` for the same cameras recently enough
        — the triangle case where one physical event makes B disagree with both
        A and C (see the module docstring).  "Recently enough" is **per rule**
        (ROADMAP 14), because the guarantee a fold rests on is per rule:

        * a **Rule 1** candidate uses ``dedup_window_rule1_sec`` (default
          10.0).  Any width is footage-safe: the AND-stop holds the owner's
          recording open until the folded pair's own disagreement resolves.
        * a **Rule 2** candidate uses ``dedup_window_sec`` (default 3.0) *and*
          must pass :meth:`_owner_covers_event` — an orphan pulse is over
          before it is evaluated, so nothing can be held open for it and the
          owner's footage has to already contain it.

        ``stop`` actions are never duplicates: the buffer already holds the
        matching recording, and suppressing the stop would strand it until the
        ``max_duration_sec`` safety cap.

        **A Rule 1 start is never folded into a Rule 2 recording.**  A Rule 2
        clip is a fixed-length artifact — its duration is computed at fire time
        and no ``stop`` is ever sent — so there is no way to hold it open until
        the Rule 1 disagreement resolves, and folding would truncate an
        open-ended event into a clip sized for a brief pulse.  It fires its own
        recording instead, which it can close itself.  Measured cost on the
        2026-08-01 run: 2 of 137 duplicates (the other 133 are same-rule, 82
        rule1→rule1 and 51 rule2→rule2).

        The comparison is on ``abs()`` so a backwards clock step degrades to
        *more* suppression rather than none — the failure that matters is a
        duplicate clip burning a writer slot, not a missed one.

        Args:
            action: ``"start"`` or ``"stop"``.
            rule: Rule firing this candidate trigger.
            group_id: Derived group the firing pair belongs to.
            cameras_key: ``";"``-joined camera IDs from the trigger payload.
                Part of the key because two pairs in one group that resolve to
                different cameras cover different footage.
            event_ts: Timestamp the candidate trigger carries.
            event_window: ``(start_ts, end_ts)`` of the candidate's underlying
                detector event, used by the Rule 2 coverage guard.  Production
                Rule 2 always supplies both; a missing element falls back to
                ``event_ts`` (see :meth:`_owner_covers_event`).

        Returns:
            The :class:`_GroupFire` this trigger duplicates, or ``None`` if it
            should be written.
        """
        if action != "start":
            return None

        previous = self._group_last_fire.get((group_id, cameras_key))
        if previous is None:
            return None

        is_rule1 = rule == "rule1_continuous_disagreement"
        if is_rule1:
            if previous.rule != "rule1_continuous_disagreement":
                return None
            window = self._dedup_window_rule1_sec
        else:
            window = self._dedup_window_sec

        if window <= 0.0:
            return None

        if abs(event_ts - previous.fire_ts) > window:
            return None

        if not is_rule1 and not self._owner_covers_event(
            previous, event_ts, event_window
        ):
            return None

        return previous

    def _owner_covers_event(
        self,
        owner: _GroupFire,
        event_ts: float,
        event_window: Optional[Tuple[Optional[float], Optional[float]]],
    ) -> bool:
        """Does the owner's recording contain the candidate's whole event?

        The Rule 2 half of duplicate rejection has no AND-stop to fall back on
        — the pulse is already over — so folding one is only safe while the
        owner's clip provably covers it.  Everything is compared in **event
        coordinates**: a clip is ``[event_start - pre_roll, that +
        max_duration_sec]``, which is what the candidate's own clip would have
        been, so the test is "does the owner's footage reach at least as far,
        in both directions, as the clip this candidate would have bought".

        Two owner shapes, two arguments:

        * **Rule 2 owner** — its span is fixed at fire time and carried on the
          :class:`_GroupFire`; compare directly.
        * **Rule 1 owner** — open-ended, so liveness is the test: while its
          ``active_trigger_id`` is still set the recording is running and will
          run at least ``post_roll`` past any future resolution, so a pulse
          that is already over is inside it by construction.  An owner that has
          already stopped is refused — unreachable at the default windows (a
          Rule 1 owner cannot stop within ``dedup_window_sec`` of starting),
          but raising the window in config must not silently create footage
          loss.  Same conservative-by-construction posture as
          ``video_cleanup.plan_removals``.

        Args:
            owner: The group fire the candidate would be folded into.
            event_ts: The candidate's trigger timestamp, used as the fallback
                for a missing event-window element.
            event_window: The candidate's ``(start_ts, end_ts)``.

        Returns:
            ``True`` when the fold is footage-safe.
        """
        if owner.rule == "rule1_continuous_disagreement":
            owner_rt = self._pair_runtime.get(owner.pair_key)
            return (
                owner_rt is not None
                and owner_rt.active_trigger_id == owner.trigger_id
            )

        if owner.span_start is None or owner.span_end is None:
            return False

        start, end = event_window if event_window else (None, None)
        needed_start = (
            start if start is not None else event_ts
        ) - self._pre_roll_sec
        needed_end = (end if end is not None else event_ts) + self._post_roll_sec
        return (
            needed_start >= owner.span_start - _COVERAGE_EPSILON_SEC
            and needed_end <= owner.span_end + _COVERAGE_EPSILON_SEC
        )

    def _join_owner_recording(
        self,
        pair_key: str,
        rt: _PairRuntimeState,
        rule: str,
        duplicate: _GroupFire,
    ) -> bool:
        """Attach a suppressed duplicate to the owner's in-progress recording.

        The clip that gets written stands for *every* disagreement folded into
        it, so it must not stop while any of them is still open — otherwise the
        pair that happened to fire first also decides, arbitrarily, when the
        footage ends.  Registering here is what makes the owner's stop an AND
        across all participants (see :meth:`_held_pairs_agree`).

        Both sides have to be open-ended for a hold to mean anything:

        * The **owner** must be running Rule 1 — the only rule with an explicit
          ``stop`` to withhold.  A Rule 2 owner's clip length is fixed at fire
          time, which is also why ``_duplicate_within_group`` refuses to fold a
          Rule 1 start into one.
        * The **duplicate** must be Rule 1 too.  A Rule 2 orphan pulse is
          complete before it is even evaluated, so there is nothing left to
          wait for; holding on one would extend the clip for a disagreement
          that is already over.

        Args:
            pair_key: The suppressed pair.
            rt: That pair's runtime state.
            rule: Rule that produced the suppressed trigger.
            duplicate: The group fire it was suppressed against.

        Returns:
            ``True`` if the owner's recording will now wait for this pair.
        """
        if rule != "rule1_continuous_disagreement":
            return False

        owner_rt = self._pair_runtime.get(duplicate.pair_key)
        if owner_rt is None or owner_rt.active_trigger_id != duplicate.trigger_id:
            # The owner is a Rule 2 fire (no open recording) or its recording
            # has already closed — nothing to hold open.
            return False

        if pair_key not in owner_rt.held_pair_keys:
            owner_rt.held_pair_keys.append(pair_key)
        rt.held_by_pair_key = duplicate.pair_key
        return True

    def _held_pairs_agree(self, rt: _PairRuntimeState) -> bool:
        """Report whether every pair folded into this recording has resolved.

        Prunes held pairs whose detectors disappeared in a config reload: a
        pair that no longer exists cannot resolve, and leaving it in the list
        would hold the recording open until the buffer's ``max_duration_sec``
        safety cap.

        Args:
            rt: Runtime state of the pair that owns the recording.

        Returns:
            ``True`` when no held pair is still disagreeing (vacuously true
            when none are held).
        """
        if not rt.held_pair_keys:
            return True

        still_held: List[str] = []
        agree = True

        for held_key in rt.held_pair_keys:
            pair = self._pairs.get(held_key)
            if pair is None:
                continue
            state_a = self._detector_states.get(pair[0])
            state_b = self._detector_states.get(pair[1])
            if state_a is None or state_b is None:
                continue
            still_held.append(held_key)
            with state_a.lock:
                a_is_on = state_a.is_on
            with state_b.lock:
                b_is_on = state_b.is_on
            if a_is_on != b_is_on:
                agree = False

        rt.held_pair_keys = still_held
        return agree

    def _release_held_pairs(self, rt: _PairRuntimeState, now: float) -> None:
        """Release the pairs a finished recording was holding.

        Each released pair is put into a *fresh* cooldown rather than being
        returned to service immediately: the recording that just ended covered
        its disagreement, so firing again on the tail of the same event would
        recreate the duplicate this mechanism exists to remove.

        Args:
            rt: Runtime state of the pair that owned the recording.
            now: Timestamp to start the released pairs' cooldown from.
        """
        for held_key in rt.held_pair_keys:
            held_rt = self._pair_runtime.get(held_key)
            if held_rt is None:
                continue
            held_rt.held_by_pair_key = None
            held_rt.cooldown_active  = True
            held_rt.triggered_at     = now
            held_rt.disagreement_start = None
        rt.held_pair_keys = []

    def _log_decision(
        self,
        payload: dict,
        pair_key: str,
        event_window: Optional[Tuple[Optional[float], Optional[float]]],
        local_tz: "pytz.BaseTzInfo",
        dedup_group: str = "",
        duplicate_of: Optional[str] = None,
    ) -> None:
        """Append one row to the engine's decision log.

        Called from :meth:`_fire_trigger` for every trigger that reached the
        Hot Folder, **and** for every one the engine rejected as a cross-pair
        duplicate before writing it.  Unlike the video buffer's
        ``discrepancies_log.csv``, no downstream condition can suppress a row
        here — that difference is the whole point of the file (see the module
        docstring).  A duplicate is marked rather than dropped because ground
        truth contains the same event on both pairs of the group, so a
        consumer that never saw the row would score the sibling pair's event
        as a miss.

        Best-effort by contract: any write failure is logged and swallowed, so
        a full or read-only disk degrades measurement, never recording.  The
        engine is the single writer (all call sites are on the evaluator
        thread), so the append needs no lock.

        Args:
            payload: The trigger payload just written, used as the source of
                truth for the fields the two artifacts share.
            pair_key: Canonical pair key the decision was made for.
            event_window: ``(start_ts, end_ts)`` of the underlying detector
                event; either element may be ``None``, and ``None`` for the
                whole tuple means the rule/action defines neither.
            local_tz: Timezone already resolved by the caller, reused so the
                row's human-readable stamp cannot disagree with the trigger
                filename's.
            dedup_group: Derived detector group the pair belongs to, recorded
                on every row so a consumer can collapse a group's rows itself.
            duplicate_of: When set, the trigger ID this row duplicated — the
                row describes a decision that never reached the Hot Folder.
        """
        if self._decision_log_path is None:
            return

        meta = payload.get("metadata", {})
        event_ts = payload["event_timestamp"]
        start_ts, end_ts = event_window if event_window else (None, None)

        try:
            local_stamp = datetime.fromtimestamp(
                event_ts, tz=local_tz
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, OSError, OverflowError):
            local_stamp = ""

        row = {
            "event_timestamp":  f"{event_ts:.3f}",
            "local_timestamp":  local_stamp,
            "intersection_id":  payload["intersection_id"],
            "trigger_id":       payload["trigger_id"],
            "action":           payload["action"],
            "rule":             meta.get("rule", ""),
            "pair_key":         pair_key,
            "det_a":            meta.get("det_a", ""),
            "det_b":            meta.get("det_b", ""),
            "det_a_type":       meta.get("det_a_type", ""),
            "det_b_type":       meta.get("det_b_type", ""),
            "event_start_ts":   "" if start_ts is None else f"{start_ts:.3f}",
            "event_end_ts":     "" if end_ts is None else f"{end_ts:.3f}",
            "disagreement_sec": meta.get("disagreement_sec", ""),
            "max_duration_sec": payload["max_duration_sec"],
            "cameras":          ";".join(payload.get("cameras", [])),
            "description":      meta.get("description", ""),
            "dedup_group":      dedup_group,
            "suppressed_as_duplicate": "1" if duplicate_of else "0",
            "duplicate_of_trigger_id": duplicate_of or "",
        }

        self._append_csv_row(
            self._decision_log_path, _DECISION_LOG_FIELDS, row,
            "Failed to append to the engine decision log",
            {"trigger_id": payload["trigger_id"]},
        )

    def _log_suppression(
        self,
        reason: str,
        pair_key: str,
        det_a_id: str,
        det_b_id: str,
        slot: str,
        orphan_det_id: str,
        pulse: Tuple[float, float],
        min_pulse_sec: float,
        floor_sec: float,
        floor_multiple: float,
        now: float,
        partner_blip_count: Optional[int] = None,
        partner_blip_window_sec: Optional[float] = None,
    ) -> None:
        """Append one row to the engine's suppression log.

        The counterpart to :meth:`_log_decision`: that file records what the
        engine emitted, this one records what it deliberately declined to
        emit and why (see the module docstring).  Same best-effort contract —
        a write failure never affects rule evaluation.

        Args:
            reason: One of the ``_SUPPRESS_*`` reason codes.
            pair_key: Canonical pair key the candidate belonged to.
            det_a_id: First detector of the pair, in ``pair_key`` order.
            det_b_id: Second detector of the pair.
            slot: ``"a"`` or ``"b"`` — the orphan slot the candidate would
                have filled.
            orphan_det_id: The detector that produced the suppressed pulse.
            pulse: ``(on_ts, off_ts)`` of that pulse, exact Unix floats.
            min_pulse_sec: The gate that rejected it, as actually applied.
            floor_sec: Sampling floor the gate was derived from.
            floor_multiple: Multiplier the gate was derived with.  Kept
                separate from ``min_pulse_sec`` so a consumer can re-derive
                the gate at other multiples.
            now: Evaluator-tick timestamp the decision was taken at.
            partner_blip_count: Partner below-floor pulses counted inside the
                gate's horizon (ROADMAP 12A), or ``None`` for a row the
                partner gate did not produce — in which case both partner
                columns are written blank.
            partner_blip_window_sec: The horizon they were counted over, kept
                separate from the count for the same reason ``floor_sec`` is
                kept separate from ``min_pulse_sec``.
        """
        if self._suppression_log_path is None:
            return

        pulse_on, pulse_off = pulse
        duration = pulse_off - pulse_on
        tz_name: str = self._intersection_cfg.get("timezone", "UTC")
        local_tz = _resolve_pytz(tz_name, self._log)

        try:
            local_stamp = datetime.fromtimestamp(
                now, tz=local_tz
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, OSError, OverflowError):
            local_stamp = ""

        det_a_cfg = self._intersection_cfg["detectors"].get(det_a_id, {})
        det_b_cfg = self._intersection_cfg["detectors"].get(det_b_id, {})

        if reason == _SUPPRESS_PARTNER_BLIP:
            partner_det_id = det_b_id if slot == "a" else det_a_id
            description = (
                f"orphan candidate on detector '{orphan_det_id}' not "
                f"registered: partner '{partner_det_id}' produced "
                f"{partner_blip_count} below-floor pulses in the last "
                f"{partner_blip_window_sec:g}s"
            )
        else:
            description = (
                f"orphan candidate on detector '{orphan_det_id}' not "
                f"registered: pulse {duration:.3f}s < gate {min_pulse_sec:.3f}s"
            )

        row = {
            "event_timestamp":          f"{now:.3f}",
            "local_timestamp":          local_stamp,
            "intersection_id":          self._intersection_id,
            "reason":                   reason,
            "rule":                     "rule2_orphan_pulse",
            "pair_key":                 pair_key,
            "det_a":                    det_a_id,
            "det_b":                    det_b_id,
            "det_a_type":               det_a_cfg.get("type", "unknown"),
            "det_b_type":               det_b_cfg.get("type", "unknown"),
            "orphan_det":               orphan_det_id,
            "slot":                     slot,
            "event_start_ts":           f"{pulse_on:.3f}",
            "event_end_ts":             f"{pulse_off:.3f}",
            "pulse_duration_sec":       round(duration, 3),
            "min_pulse_sec":            round(min_pulse_sec, 4),
            "sampling_floor_sec":       round(floor_sec, 4),
            "min_pulse_floor_multiple": floor_multiple,
            "description":              description,
            "partner_blip_count": (
                "" if partner_blip_count is None else partner_blip_count
            ),
            "partner_blip_window_sec": (
                "" if partner_blip_window_sec is None
                else round(partner_blip_window_sec, 3)
            ),
        }

        self._append_csv_row(
            self._suppression_log_path, _SUPPRESSION_LOG_FIELDS, row,
            "Failed to append to the engine suppression log",
            {"pair_key": pair_key, "reason": reason},
        )

    def _append_csv_row(
        self,
        path: Path,
        fields: Tuple[str, ...],
        row: dict,
        error_message: str,
        error_context: dict,
    ) -> None:
        """Append one row to a CSV, writing the header only for a new file.

        Shared by :meth:`_log_decision` and :meth:`_log_suppression` so the
        two artifacts cannot drift apart on the restart-safety behavior that
        matters for both: an existing log is appended to and never
        re-headered, so a resumed file's rows keep lining up with the header
        it was created with.

        Best-effort by contract: any write failure is logged at ERROR and
        swallowed, because a full or read-only disk must degrade measurement,
        never recording.  The engine is the single writer (every call site is
        on the evaluator thread), so the append needs no lock.

        Args:
            path: Destination CSV.  Parent directories are created.
            fields: Column order — the file's header, and the DictWriter's
                field list.
            row: Mapping of column name to value; keys must match ``fields``.
            error_message: Log message used if the append fails.
            error_context: Extra structured fields for that error log,
                merged after the intersection ID and path.
        """
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Header only for a genuinely new (or truncated) file — the log
            # survives restarts and must not gain a header mid-stream.
            write_header = not path.exists() or path.stat().st_size == 0
            with path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(fields))
                if write_header:
                    writer.writeheader()
                writer.writerow(row)
        except OSError as exc:
            self._log.error(
                error_message,
                extra={
                    "intersection_id": self._intersection_id,
                    "path":            str(path),
                    "error":           str(exc),
                    **error_context,
                },
            )

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _build_structures(self, preserve_existing: bool = False) -> None:
        """Populate ``_detector_states``, ``_pairs``, ``_pair_runtime``, and
        the derived detector ``_groups`` / ``_pair_group`` map.

        ``paired_detector_id`` is accepted as a **scalar or a list**: pairs are
        the union of all normalized links, so a 3-way group written explicitly
        (A ``[B, C]``, B ``[A, C]``, C ``[A, B]``) and the same group written
        as a ring of scalars (A→B, B→C, C→A) produce the identical 3 pairs.

        Groups are the connected components of the resulting pair graph and are
        a **dedup scope only** — never an instruction to evaluate every pair
        inside them (see the module docstring).  A group spanning more than one
        ``phase`` is almost certainly a stray link merging two intended groups,
        so it is logged as a WARNING; the groups themselves are logged like
        ``_pairs`` already are.

        Args:
            preserve_existing: If ``True``, retains existing
                :class:`_DetectorState` objects for IDs that survive the
                reload, and carries forward active cooldown state.  Transient
                fields (disagreement timers, orphan watches, and Rule 1
                active-tracking fields) are intentionally discarded to
                guarantee a clean slate after a config change.
        """
        detectors_cfg: dict = self._intersection_cfg.get("detectors", {})

        new_states: Dict[str, _DetectorState] = {}
        for det_id in detectors_cfg:
            key = str(det_id)
            if preserve_existing and key in self._detector_states:
                new_states[key] = self._detector_states[key]
            else:
                new_states[key] = _DetectorState(detector_id=key)
        self._detector_states = new_states

        new_pairs: Dict[str, Tuple[str, str]] = {}
        seen: set = set()
        known_ids = {str(k) for k in detectors_cfg}

        for det_id, det_cfg in detectors_cfg.items():
            det_id_str = str(det_id)
            partner_id = det_cfg.get("paired_detector_id")
            if partner_id is None:
                continue
            # Scalar or list — one loop covers both authoring styles.
            partner_ids = (
                partner_id if isinstance(partner_id, (list, tuple))
                else [partner_id]
            )

            for raw_partner in partner_ids:
                if raw_partner is None:
                    continue
                partner_str = str(raw_partner)

                if partner_str not in known_ids:
                    self._log.warning(
                        "Detector references unknown paired_detector_id",
                        extra={
                            "intersection_id":    self._intersection_id,
                            "detector_id":        det_id_str,
                            "paired_detector_id": partner_str,
                        },
                    )
                    continue

                if partner_str == det_id_str:
                    self._log.warning(
                        "Detector is paired with itself; link ignored",
                        extra={
                            "intersection_id": self._intersection_id,
                            "detector_id":     det_id_str,
                        },
                    )
                    continue

                pair_key = ":".join(sorted([det_id_str, partner_str]))
                if pair_key in seen:
                    continue
                seen.add(pair_key)
                new_pairs[pair_key] = (det_id_str, partner_str)

        self._pairs = new_pairs
        self._build_groups(new_pairs, detectors_cfg)

        new_runtime: Dict[str, _PairRuntimeState] = {}
        for pair_key in new_pairs:
            if preserve_existing and pair_key in self._pair_runtime:
                old   = self._pair_runtime[pair_key]
                fresh = _PairRuntimeState(pair_key=pair_key)
                # Preserve only cooldown state; discard all transient fields
                # including any in-progress Rule 1 tracking.
                fresh.cooldown_active = old.cooldown_active
                fresh.triggered_at    = old.triggered_at
                new_runtime[pair_key] = fresh
            else:
                new_runtime[pair_key] = _PairRuntimeState(pair_key=pair_key)
        self._pair_runtime = new_runtime

        self._log.debug(
            "Structures built",
            extra={
                "intersection_id": self._intersection_id,
                "detectors":       list(self._detector_states.keys()),
                "pairs":           list(self._pairs.keys()),
                "groups":          list(self._groups.keys()),
            },
        )

    def _build_groups(
        self,
        pairs: Dict[str, Tuple[str, str]],
        detectors_cfg: dict,
    ) -> None:
        """Derive detector groups as connected components of the pair graph.

        Sets ``_groups`` (group ID → member detector IDs) and ``_pair_group``
        (pair key → group ID), and drops any duplicate-window bookkeeping for
        groups that no longer exist so a reload cannot leave a stale anchor
        behind.

        A group is a **dedup scope only**; it never adds a comparison.  Groups
        that span more than one ``phase`` are reported as a WARNING: detectors
        watching the same physical zone share a phase, so a multi-phase group
        means one stray ``paired_detector_id`` has transitively merged two
        intended groups — a mistake that is otherwise completely silent.

        Args:
            pairs: The pair map just built, ``pair_key -> (det_a, det_b)``.
            detectors_cfg: The intersection's ``detectors`` mapping, read for
                each member's ``phase``.
        """
        adjacency: Dict[str, set] = {}
        for det_a_id, det_b_id in pairs.values():
            adjacency.setdefault(det_a_id, set()).add(det_b_id)
            adjacency.setdefault(det_b_id, set()).add(det_a_id)

        group_of: Dict[str, str] = {}          # detector ID -> group ID
        new_groups: Dict[str, List[str]] = {}

        for root in adjacency:
            if root in group_of:
                continue
            members: List[str] = []
            visited = {root}
            stack = [root]
            while stack:
                node = stack.pop()
                members.append(node)
                for neighbour in adjacency[node]:
                    if neighbour not in visited:
                        visited.add(neighbour)
                        stack.append(neighbour)
            ordered  = _sort_detector_ids(members)
            group_id = ":".join(ordered)
            new_groups[group_id] = ordered
            for member in members:
                group_of[member] = group_id

        self._groups = new_groups
        self._pair_group = {
            pair_key: group_of[det_a_id]
            for pair_key, (det_a_id, _det_b_id) in pairs.items()
        }

        # A reload can dissolve a group; its anchor must not outlive it.
        self._group_last_fire = {
            key: value for key, value in self._group_last_fire.items()
            if key[0] in new_groups
        }

        for group_id, members in new_groups.items():
            phases = {
                str(detectors_cfg.get(m, {}).get("phase"))
                for m in members
            }
            if len(phases) > 1:
                self._log.warning(
                    "Derived detector group spans more than one phase — "
                    "check for a stray paired_detector_id link",
                    extra={
                        "intersection_id": self._intersection_id,
                        "dedup_group":     group_id,
                        "phases":          sorted(phases),
                        "detector_phases": {
                            m: detectors_cfg.get(m, {}).get("phase")
                            for m in members
                        },
                    },
                )

    def _cameras_for_pair(self, det_a_id: str, det_b_id: str) -> List[str]:
        """Return the deduplicated ordered camera IDs for a detector pair.

        Args:
            det_a_id: First detector ID.
            det_b_id: Second detector ID.

        Returns:
            List of camera ID strings, or ``["all"]`` if none are configured.
        """
        detectors_cfg = self._intersection_cfg.get("detectors", {})
        cameras: List[str] = []
        for det_id in (det_a_id, det_b_id):
            cam = detectors_cfg.get(det_id, {}).get("camera_id")
            if cam and cam not in cameras:
                cameras.append(cam)
        return cameras if cameras else ["all"]