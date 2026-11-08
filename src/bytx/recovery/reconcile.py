"""Recovery after restart: reconciling intents with the exchange by client_id.

For every open intent (``pending`` / ``unknown`` / ``placed``) Exchange:

1. looks the order up on the exchange: by id (if known), by client_id, then among the symbol's open
   orders (TP/SL legs — among open conditional orders, :meth:`Exchange.tp_sl_orders`);
2. **found** — adopts it: live → ``adopted``, finished while the process was down →
   ``finished``; the store and the position journal (if attached: role, position, level from the
   intent) receive its state;
3. **not found** (the client_id lookup answered "no" and it is not among open orders) — the order never
   reached the exchange. ``resubmit`` allows re-creating it **with the same request and the same client_id** (TP/SL
   legs — with a new client_id for the current position; no position — not placed); otherwise
   the intent is closed as ``abandoned``. A retry happens only after confirmed
   absence. The exchange rejects a repeated client_id only among **open** orders (Binance
   newClientOrderId, OKX clOrdId): a filled or cancelled order with the same client_id is accepted
   again, and the client_id lookup finds finished orders only for a limited time (OKX: cancelled
   unfilled orders — about 2 h, Binance — 3 days). So ``max_age`` (default 300 s)
   limits re-creation: an old intent is never retried; ``max_age=None`` — only if
   the exchange lookup is known to cover the intents' age. A TP/SL leg is re-created only if
   the same entry has no other leg of the same kind (a failure inside a previous recovery);
4. **could not verify** (network, the exchange doesn't search by client_id) — ``unverified``: the intent
   stays open, nothing is sent ("no" is not confused with "could not check").

Protection of an entry with a TP/SL plan (``place_order`` Binance/HL, :meth:`Exchange.open_position` — both
modes, attached and separate): the entry is found or re-created, but its legs are not in the store (the process crashed
between the entry and the protection) — before placing, the exchange's open TP/SL for the symbol and side
are re-read: a leg is already live (attached by the exchange, placed before the crash) — matched by
client_id where it is sent to the exchange, otherwise by kind + side + trigger price at the price step
(:func:`bytx.protection.find_leg`) — a second one is not placed; none — it is placed (``protect=True``, by
default: reduce-only, they don't increase the position). TP/SL list not read — ``unverified``, nothing
is placed. An entry with an attached SL still filling — ``unverified`` (the exchange attaches it itself).

Orphaned TP/SL — live legs without a position on their side (the bracket OCO emulation died with the
process, the position was closed bypassing the legs): a reduce-only leg would close the next position on that side.
``cancel_orphaned=True`` (default) cancels ONLY own legs (an intent in the store or the bot tag
``client_id_prefix``; foreign and manual ones — never) and ONLY when there is no position for the symbol and side:
positions are re-read from the exchange right before cancelling (read failure — no cancel); an entry on the same
side still open (Binance legs are placed before the fill) — no cancel. Cancellation goes through
:meth:`Exchange.cancel_tp_sl` (Binance/OKX algo, KuCoin stop orders); a failure on one leg does not break
recovery. Result — ``orphans_canceled`` / ``orphaned`` + ``orphan_reasons``.

Execution algorithms (``ex.algos``: chase, twap, iceberg, bracket, trailing, capped market) do not resume after a
restart — their progress lived in process memory (a safe policy instead of resuming). The algorithm's record in the
store (``running`` before the first child order, ``finished`` / ``unclean`` at the end) and ``algo_id`` in the intents
of its orders and TP/SL legs make the orders of an unfinished algorithm recognizable.
``cancel_orphaned_algos=True`` (default): **working** orders (the algorithm's regular orders: chase limit order,
twap / iceberg part, bracket entry, IOC) are canceled after the regular reconciliation — unattended, a limit order at
a stale price could fill at any time; the filled part stays (the position is not touched, the size is in the report);
child orders that never reached the exchange are not re-created even with ``resubmit=True``. **Position protection**
(the algorithm's TP/SL legs: bracket legs, trailing SL) stays; without a position such legs are canceled by the regular
orphan logic (above). The algorithm record is closed (``recovered``) — a repeated ``recover()`` doesn't touch it; an
unverified child order or a failed cancel keeps the record open, the next ``recover()`` retries. An algorithm running
in this same process is not touched. ``cancel_orphaned_algos=False`` — report only (as before: live orders are
adopted). Result — ``report.algos`` (:class:`AlgoRecovery`: what was found, what was canceled, what was kept and why).

**Bracket position protection from the plan.** A bracket writes its protection plan (position side, TP/SL prices,
``trigger_by``, size, client_ids of the recovery legs — ``AlgoRecord.plan``) BEFORE sending the entry and retires it
itself (``retired``) as soon as the legs are placed (``protected``), it is canceled or protection failed
(``on_protect_fail``): recover places no legs for a retired plan — a leg of a live bracket canceled by hand, triggered
or gone from the exchange history is not resurrected. The process died (or the task was stopped) between the entry
fill and the legs — ``protect=True`` places the missing legs after the reconciliation and cancelling the working entry.
This is not resuming the algorithm: position protection only, no OCO watching.

- The fill — from the algorithm's orders: finished ones are re-read from the exchange (a cancel response on Bybit /
  OKX / KuCoin / HL / Bitget has only the id, the intent size may have stayed 0), a read failure — ``unverified``; the
  entry still works (``cancel_orphaned_algos=False``, cancel failed) — ``unverified``: no legs for a partial size.
- A leg already exists if its intent of the algorithm is live (``placed``) or it is on the exchange: own — by the plan
  client_ids and the algorithm's legs; by kind + side + price (:func:`bytx.protection.find_leg`) — only a leg without
  a client_id (Bybit trading-stop, BingX) created no earlier than this bracket's entry (exchange time), one such leg —
  for one bracket per recovery; foreign client_ids (another bot, another own bracket) are not counted and not adopted.
  So a repeated recover and a ``kill -9`` inside it do not duplicate legs (the leg intent with the plan client_id is
  written before sending). The leg triggered — ``triggered``; canceled or gone from the exchange history (``abandoned``
  with an exchange id) — not re-created (``canceled``).
- No position on the plan's side — nothing (``no_position``); only the other side is open — nothing, a warning
  (``opposite_side``; in hedge — a separate position, not touched). Size — filled by the entry (minus what the
  algorithm closed), no more than the position: partly closed — the leg for the rest (the actual size, reduce-only — on
  all exchanges; "whole position" is not used: Bybit Full would replace a foreign stop of the position, Binance
  closePosition is one per side, and such a stop would protect a foreign size); below the minimum order —
  ``too_small``.
- The position exists, legs are missing, but the plan is older than ``protect_max_age`` (a day by default) since the
  last movement of the entry (fill / entry order update per the exchange response) — not applied: the position may
  have changed (``expired``).
- The price by the plan's ``trigger_by`` (mark — position mark / ticker, last — last trade;
  :meth:`Exchange._trigger_price`) is already past the trigger — the leg would trigger at once or be rejected: not
  placed (``price_passed``); the SDK does not close the position — the caller decides. Placement outcome unknown —
  ``unverified`` (``report.unverified`` has the leg intent), the next recover reconciles it by client_id.

The result per leg — ``AlgoRecovery.protection``, placed ones — ``AlgoRecovery.protected`` (and ``report.protected``).
``unverified`` and ``failed`` (exchange rejection) — the record stays open, the next recover retries (no longer than
``protect_max_age``); otherwise the record is closed, the decision is the caller's: a position that may have no stop —
``report.unprotected`` (the plan SL is not placed; without a plan — there is no SL of the side on the exchange). A
retired plan and records without a plan (schema 3) — the previous behaviour: only a note in ``notes``.

An entry's TP/SL plan (``place_order`` / ``open_position``) is closed instead of applied when it is stale
(``report.stale_plans``): 'superseded' — there is a newer entry of the same side of the symbol; 'expired' — older than
``protect_max_age``. An open entry with a ``trigger_by`` plan and no position yet — ``report.deferred`` (protection
after the fill, on the next recover).

Fresh intents (younger than ``settle`` seconds) wait ``settle`` on the Exchange clock: an order accepted
right before the crash may show up in the exchange lookup with a delay. Orders with the bot tag
(``client_id_prefix``) without an intent are ``untracked``: written to the store (role and position
from the journal, if it knows the cid) and adopted too. Exact tag match only, no heuristics.
"""
from __future__ import annotations

import contextlib
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Optional, Union

from loguru import logger

from bytx.errors import BytxError, ConfigError, OrderNotFound, OrderStatusUnknown
from bytx.recovery.store import OPEN_ALGO_STATES, AlgoRecord, Intent, IntentStore, _num, now_ms

if TYPE_CHECKING:
    from bytx.exchange import Exchange
    from bytx.protection import TpSlOrder
    from bytx.types import Order

ResubmitPolicy = Union[bool, Callable[[Intent], Any]]
_TERMINAL = frozenset({'closed', 'canceled', 'expired', 'rejected'})


@dataclass(slots=True)
class RecoveryReport:
    """Recovery result.

    ``adopted`` — live intent orders (adopted); ``finished`` — finished while the process
    was down; ``resubmitted`` — re-created (same client_id); ``protected`` — missing TP/SL of
    entries and bracket legs placed from the plan (also in ``AlgoRecovery.protected``); ``abandoned`` — dangling
    intents closed without sending (no order);
    ``unverified`` — could not verify (left open, retry later); ``failed`` —
    re-creation rejected by the exchange; ``untracked`` — bot-tagged orders without an intent (recorded);
    ``orphans_canceled`` — own live TP/SL legs without a position on their side, cancelled by recovery
    (``cancel_orphaned=True``); ``orphaned`` — such legs left live, reason in
    ``orphan_reasons`` (order id → text: ``cancel_orphaned=False``, positions not read, entry
    on that side still open, cancel failed). They don't affect ``ok``. ``algos`` — unfinished execution
    algorithms (:class:`AlgoRecovery`): their canceled working orders are not included in ``adopted``.
    ``unprotected`` — algorithms whose position may have no stop after recovery (``AlgoRecovery.unprotected``):
    ``ok`` is about reconciling intents, it does not show a naked position — check ``unprotected`` too.
    ``stale_plans`` — entries (not algorithms) whose TP/SL plan was closed by recovery instead of applied, and the
    reason: 'superseded' — there is a newer entry of the same side of the symbol; 'expired' — the plan is older than
    ``protect_max_age`` since the last movement of the entry, the side's position exists, legs are missing. Position
    protection is the caller's decision. A plan whose failure was reported to the caller (ProtectionNotPlaced,
    'protection_failed') is not touched by recover. ``deferred`` — entries with a trigger_by plan not filled yet (no
    position): legs are not placed, the entry is not marked protected — recover() after the fill; does not affect
    ``ok``.
    """

    adopted: list['Order'] = field(default_factory=list)
    finished: list['Order'] = field(default_factory=list)
    resubmitted: list['Order'] = field(default_factory=list)
    protected: list['Order'] = field(default_factory=list)
    abandoned: list[Intent] = field(default_factory=list)
    unverified: list[tuple[Intent, BaseException]] = field(default_factory=list)
    failed: list[tuple[Intent, BaseException]] = field(default_factory=list)
    untracked: list['Order'] = field(default_factory=list)
    orphaned: list['Order'] = field(default_factory=list)
    orphans_canceled: list['Order'] = field(default_factory=list)
    orphan_reasons: dict[str, str] = field(default_factory=dict)
    algos: list['AlgoRecovery'] = field(default_factory=list)
    pruned: int = 0
    stale_plans: list[tuple[Intent, str]] = field(default_factory=list)
    deferred: list[tuple[Intent, 'Order']] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """All intents reconciled: no ``unverified`` and no ``failed``."""
        return not self.unverified and not self.failed

    @property
    def unprotected(self) -> list['AlgoRecovery']:
        """Algorithms whose position may be left without a stop (the plan SL is not placed: the price passed,
        ``protect=False``, rejection, not checked, the plan is stale, size below the minimum, the SL was canceled;
        without a plan — no TP/SL of the side on the exchange): closing the position or placing a stop is the caller's
        decision."""
        return [a for a in self.algos if a.unprotected]

    @property
    def live(self) -> list['Order']:
        """All live orders after recovery: adopted, re-created, protection, without an intent."""
        return [o for o in (*self.adopted, *self.resubmitted, *self.protected, *self.untracked)
                if o.status not in _TERMINAL]


@dataclass(slots=True)
class AlgoRecovery:
    """An unfinished execution algorithm (record ``running`` / ``unclean``) found by recovery.

    ``canceled`` — its working orders canceled by recovery (the part filled before the cancel — the order's
    ``filled``, the position stays); ``finished`` — working orders that finished while the process was down (or filled
    between reconciliation and the cancel); ``kept`` — left live, why — ``reasons`` (order id → text): position
    protection (the algorithm's TP/SL), ``cancel_orphaned_algos=False``, cancel failed; ``abandoned`` — client_ids of
    children that never reached the exchange (not re-created); ``unverified`` — client_ids of children that couldn't be
    checked; ``filled`` — filled by the algorithm's orders in its direction, contracts (from intents; the position is
    not touched); ``closed`` — the record is closed (``recovered``), a repeated ``recover()`` doesn't touch the
    algorithm; False — it stays open (repeat ``recover()``); ``notes`` — explanations. ``state`` — the record state
    before recovery, ``size`` — the algorithm's requested size (base).

    Bracket position protection from the plan (see the module): ``protected`` — legs placed by recovery from the plan
    (also in ``report.protected``); ``protection`` — the result per plan leg ('stop_loss' / 'take_profit'): 'placed'
    (placed), 'present' (already there — the algorithm's intent is live or found on the exchange), 'triggered' (the
    algorithm's leg triggered), 'canceled' (removed — not re-created), 'no_position' (no position on the plan's side),
    'opposite_side' (only the other side is open), 'price_passed' (the price is already past the trigger — not placed;
    for an SL the position has no stop, whether to close it is the caller's decision), 'failed' (exchange rejection,
    the error — in ``report.failed``), 'unverified' (position / TP/SL / price / entry outcome not checked — in
    ``report.unverified``; the entry still works), 'disabled' (``protect=False``), 'expired' (the plan is older than
    ``protect_max_age`` — not applied), 'too_small' (own size on the exchange is below the minimum order). Empty — no
    plan (an old record, not a bracket), it was retired by the algorithm (legs placed, cancel, ``on_protect_fail``) or
    the entry is not filled. ``unprotected`` — the algorithm side's position may have no stop: plan SL 'price_passed' /
    'disabled' / 'failed' / 'unverified' / 'expired' / 'too_small' / 'canceled'; without a plan — the position is open
    and there is no SL of its side on the exchange (or it was not read).
    """

    algo_id: str
    kind: str
    symbol: str
    side: Optional[str]
    state: str
    started_ms: int
    size: Optional[float] = None
    canceled: list['Order'] = field(default_factory=list)
    finished: list['Order'] = field(default_factory=list)
    kept: list['Order'] = field(default_factory=list)
    reasons: dict[str, str] = field(default_factory=dict)
    abandoned: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)
    filled: float = 0.0
    closed: bool = False
    notes: list[str] = field(default_factory=list)
    protected: list['Order'] = field(default_factory=list)
    protection: dict[str, str] = field(default_factory=dict)
    unprotected: bool = False


class _Absent(Exception):
    """No order: the client_id lookup answered "not found", and it is not among open orders either."""


async def recover(ex: 'Exchange', *, resubmit: ResubmitPolicy = False, max_age: Optional[float] = 300.0,
                  settle: float = 3.0, protect: bool = True, adopt_untracked: bool = True,
                  symbols: Optional[Iterable[str]] = None, prune_after: Optional[float] = 7 * 86400.0,
                  clock: Any = None, cancel_orphaned: bool = True,
                  cancel_orphaned_algos: bool = True, protect_max_age: Optional[float] = 86400.0) -> RecoveryReport:
    """Reconciles the ``ex.intents`` intent store with the exchange (see module).

    ``resubmit`` — whether to re-create an order missing on the exchange: False (default) — no,
    the intent becomes ``abandoned``; True — yes; ``callable(intent) -> bool`` (may be async) — the caller
    decides. ``max_age`` (s) — older intents are never re-created (the market has moved);
    None — no limit. ``settle`` — how many seconds after recording to wait before looking up fresh
    intents. ``protect`` — place missing TP/SL for entries. ``adopt_untracked`` — adopt
    live bot-tagged orders without an intent (symbols from intents and ``symbols``).
    ``prune_after`` — delete closed intents older than this (s; None — don't delete). ``clock`` —
    a clock with ``now_ms()`` / ``sleep()`` (default — the paper clock, otherwise wall clock).
    ``cancel_orphaned`` — cancel own orphaned TP/SL (see module); False — report only.
    ``cancel_orphaned_algos`` — cancel the working orders of unfinished ``ex.algos`` algorithms (not position
    protection; see module); False — the ``report.algos`` report only. ``protect`` — also the missing bracket legs
    from the protection plan (the plan side's position exists, no legs; see module), False — report only
    (``disabled``, the record is closed). ``protect_max_age`` (s, a day by default) — an older protection plan (since
    the last movement of the entry: fill / order update per the exchange response) is not applied if the position
    exists and legs are missing: the position may have changed (result ``expired``; for an entry outside an algorithm
    — ``report.stale_plans``); None — no limit.
    """
    store: Optional[IntentStore] = getattr(ex, 'intents', None)
    if store is None:
        raise ConfigError('recover: Exchange has no intent store (create_exchange(intents=IntentStore(...)))',
                          exchange=ex.name)
    await ex.connect()
    clock = clock or getattr(ex, 'clock', None)
    report = RecoveryReport()
    pending = store.open_intents(ex)
    legacy = store.legacy_intents(ex)
    if legacy:
        # rows of schemas 1–4 without the key fingerprint: whose they are is unknown, recover does not touch them
        hint = ConfigError(f'intents without the key fingerprint (schema ≤ 4, namespace {legacy[0].namespace!r}): '
                           f'whose they are is unknown; if they belong to this key — store.claim_legacy(ex)',
                           exchange=ex.name)
        report.unverified.extend((it, hint) for it in legacy)
        logger.warning(f'{ex.name}: recover — {len(legacy)} open intents without the key fingerprint (namespace '
                       f'{legacy[0].namespace!r}) left untouched: move yours with IntentStore.claim_legacy(ex)')
    await _settle(ex, clock, pending, settle)
    cache = _Cache(ex)
    orders = [i for i in pending if i.kind == 'order']
    legs = [i for i in pending if i.kind != 'order']
    entries: list[tuple[Intent, 'Order']] = []
    stale_algos = store.algos(ex, states=OPEN_ALGO_STATES)
    # child orders of unfinished algorithms (not of this process) are not re-created: nobody would manage them
    no_resubmit = {r.algo_id for r in stale_algos if r.algo_id not in store.running_algos} \
        if cancel_orphaned_algos else ()
    for it in orders:
        await _recover_order(ex, store, it, cache, report, entries, resubmit, max_age, no_resubmit)
    for it in legs:
        await _recover_leg(ex, store, it, cache, report, resubmit, max_age)
    leg_ids = {i.client_id for i in legs}
    adopted_legs = [o for o in report.adopted if o.client_id in leg_ids]
    # entries whose TP/SL never got recorded (the process crashed between the entry and the protection)
    if protect:
        for it in store.intents(ex, states=('placed', 'final')):
            if it.kind == 'order' and it.tp_sl and not it.protected and not it.plan_retired and \
                    all(e.client_id != it.client_id for e, _ in entries):
                order = await _find_quiet(ex, it, cache)
                if order is not None:
                    entries.append((it, order))
        for it, order in entries:
            await _complete_protection(ex, store, it, order, report, cache, protect_max_age)
    scope = {i.symbol for i in pending} | set(symbols or ())
    if adopt_untracked and ex.client_id_prefix:
        await _adopt_untracked(ex, store, cache, scope, report)
    # working orders of unfinished algorithms — before orphans: a canceled entry on a side doesn't hold its legs
    for rec in stale_algos:
        await _recover_algo(ex, store, rec, cache, report, cancel_orphaned_algos, protect, protect_max_age)
    # orphaned legs are searched across all store symbols: legs attached by the exchange have no own intents
    await _orphans(ex, store, adopted_legs, scope | {i.symbol for i in store.intents(ex)}, report, cancel_orphaned)
    _algo_legs_orphaned(report)
    if prune_after is not None:
        report.pruned = store.prune(ex, older_than_ms=_now(ex, clock) - int(prune_after * 1000))
    _log(ex, report)
    return report


# ─── reconciling one intent ────────────────────────────────────────────────

async def _recover_order(ex: 'Exchange', store: IntentStore, it: Intent, cache: '_Cache', report: RecoveryReport,
                         entries: list, resubmit: ResubmitPolicy, max_age: Optional[float],
                         no_resubmit: Iterable[str] = ()) -> None:
    try:
        order = await _find(ex, it, cache)
    except _Absent:
        order = None
    except BytxError as e:
        report.unverified.append((it, e))
        return
    if order is not None:
        store._on_placed(ex, order, it.client_id)
        await _journal(ex, it, order)
        (report.adopted if order.status not in _TERMINAL else report.finished).append(order)
        if it.tp_sl and not it.protected and not it.plan_retired:
            entries.append((it, order))
        return
    if it.state == 'placed':
        # the exchange confirmed the order, but now it is nowhere (history archived) — not live
        store.mark(ex, it.client_id, 'abandoned', note='missing: confirmed earlier, not found on the exchange')
        report.abandoned.append(store.get(ex, it.client_id) or it)
        return
    if it.algo_id and it.algo_id in no_resubmit:
        store.mark(ex, it.client_id, 'abandoned',
                   note=f'not_sent: an order of unfinished algorithm {it.algo_id} is not re-created')
        report.abandoned.append(store.get(ex, it.client_id) or it)
        return
    if not await _may_resubmit(ex, it, resubmit, max_age):
        store.mark(ex, it.client_id, 'abandoned', note='not_sent: no order on the exchange, not re-created')
        report.abandoned.append(store.get(ex, it.client_id) or it)
        return
    request = it.request()
    try:
        with _bind(ex, it):
            if ex.risk is not None:   # risk guard, as in place_order
                m = await ex.market(it.symbol)
                price = it.price or await ex._reference_price(it.symbol, it.side)  # type: ignore[arg-type]
                await ex.risk.check(ex, it.symbol, it.side or '', (it.amount or 0.0) * m.contract_size, price,
                                     it.reduce_only)
            order = await ex._submit_order(request)
    except BytxError as e:
        report.failed.append((it, e))
        return
    report.resubmitted.append(order)
    if it.tp_sl:
        entries.append((store.get(ex, it.client_id) or it, order))


async def _recover_leg(ex: 'Exchange', store: IntentStore, it: Intent, cache: '_Cache', report: RecoveryReport,
                       resubmit: ResubmitPolicy, max_age: Optional[float]) -> None:
    try:
        order: Optional['Order'] = await _find_leg(ex, it, cache)
    except _Absent:
        order = None
    except BytxError as e:
        report.unverified.append((it, e))
        return
    if order is not None:
        store._on_placed(ex, order, it.client_id)
        await _journal(ex, it, order, tp_sl=True)
        (report.adopted if order.status not in _TERMINAL else report.finished).append(order)
        return
    side = it.position_side
    if it.state == 'placed' or it.kind not in ('take_profit', 'stop_loss') or side is None \
            or it.trigger_price is None or not await _may_resubmit(ex, it, resubmit, max_age):
        store.mark(ex, it.client_id, 'abandoned', note='not_sent: no TP/SL on the exchange, not re-created')
        report.abandoned.append(store.get(ex, it.client_id) or it)
        return
    twin = _replacement(store, ex, it)
    if twin is not None:
        # a previous recovery already re-created the leg and crashed before closing this intent
        store.mark(ex, it.client_id, 'abandoned', note=f'replaced: re-created as {twin.client_id}')
        report.abandoned.append(store.get(ex, it.client_id) or it)
        return
    # the new leg is a child of the entry (or of the old leg if there is no entry): a repeated recovery will see it
    placed = await _place_leg(ex, store, it, it.kind, it.trigger_price, side, it.amount, report,
                              parent=it.parent or it.client_id, trigger_by=it.trigger_by)
    if placed is not None:
        store.mark(ex, it.client_id, 'abandoned', note=f'replaced: re-created as {placed.client_id}')
        report.resubmitted.append(placed)


def _covers(c: Intent, kind: str) -> bool:
    """Leg ``c`` covers side ``kind``: placed or possibly placed (anything except
    an exchange rejection and one closed by recovery; a manually cancelled one is not re-created — nor one gone from the
    exchange history: ``abandoned`` with an exchange id). An OKX pair (kind 'tp_sl') — both sides."""
    return c.kind in (kind, 'tp_sl') and (c.state not in ('abandoned', 'rejected') or _was_placed(c))


def _was_placed(c: Intent) -> bool:
    """The exchange accepted the leg (its id is known), and now it is nowhere — canceled or triggered and gone from
    the history."""
    return c.state == 'abandoned' and bool(c.order_id)


def _replacement(store: IntentStore, ex: 'Exchange', it: Intent) -> Optional[Intent]:
    """A live (or possibly live) leg of the same kind on the same entry — re-created by a previous
    recovery in place of ``it``. Checked before any network call: a crash between placing and closing ``it``
    does not produce a second leg."""
    rows = store.children(ex, it.client_id)
    if it.parent:
        rows += store.children(ex, it.parent)
    return next((c for c in rows if c.client_id != it.client_id and c.created_ms >= it.created_ms
                 and _covers(c, it.kind)), None)


def _superseded(ex: 'Exchange', store: IntentStore, it: Intent) -> bool:
    """There is a newer entry (not reduce-only) of the same side of the symbol — this entry's plan no longer applies to
    the position."""
    return any(o.kind == 'order' and not o.reduce_only and o.side == it.side and o.client_id != it.client_id
               and o.created_ms > it.created_ms and o.state not in ('rejected', 'abandoned')
               for o in store.intents(ex, symbol=it.symbol))


def _stale(ex: 'Exchange', store: IntentStore, it: Intent, report: RecoveryReport, reason: str, why: str) -> None:
    store._retire_plan(ex, it.client_id, reason)
    report.stale_plans.append((it, reason))
    logger.warning(f'{ex.name}: the protection plan of entry {it.client_id} is not applied: {reason} ({why}) — check '
                   f'the protection of the {it.symbol} position')


def _entry_moved_ms(it: Intent, order: 'Order') -> int:
    """The last movement of the entry: fill / order update per the exchange response, otherwise the intent record."""
    raw = order.raw if isinstance(order.raw, dict) else {}
    stamps = [x for x in (raw.get('lastTradeTimestamp'), raw.get('lastUpdateTimestamp'), order.timestamp,
                          it.updated_ms, it.created_ms) if isinstance(x, (int, float)) and x > 0]
    return int(max(stamps)) if stamps else 0


async def _complete_protection(ex: 'Exchange', store: IntentStore, it: Intent, order: 'Order',
                               report: RecoveryReport, cache: '_Cache', max_age: Optional[float] = None) -> None:
    """Entry exists, its TP/SL are not in the store — reconcile with the exchange and place missing legs (as place_order).

    A leg counts as placed if its intent is not closed as ``abandoned`` / ``rejected``
    or it is already live on the exchange (:func:`bytx.protection.find_leg`: attached by the exchange,
    placed before the process crashed). Entry still open — legs for the entry amount, as right after
    place_order; filled — for the filled amount, no more than the position; cancelled unfilled —
    nothing to protect. The entry is marked protected only if all legs were placed (or there is no position)."""
    from bytx.protection import find_leg
    plan = it.tp_sl or {}
    # legs of this entry: a client_id reused after a closed intent is a new intent (created_ms anew); legs of the
    # previous entry with the same client_id (created earlier) do not cover it
    children = [c for c in store.children(ex, it.client_id) if c.created_ms >= it.created_ms]
    missing = {k: plan[k] for k in ('stop_loss', 'take_profit') if plan.get(k) is not None
               and not any(_covers(c, k) for c in children)}
    filled = order.filled_contracts if order.filled_contracts is not None else order.filled
    if not missing or (order.status in _TERMINAL and not filled):
        store._on_protected(ex, it.client_id)
        return
    if _superseded(ex, store, it):
        _stale(ex, store, it, report, 'superseded', 'there is a newer entry of the same side of the symbol')
        return
    attached = plan.get('mode') == 'attached'
    if attached and order.status not in _TERMINAL:
        report.unverified.append((it, BytxError(
            f'entry {it.client_id} is still filling: the attached stop is reconciled after the fill', exchange=ex.name)))
        return
    side = plan.get('side') or ('long' if it.side == 'buy' else 'short')
    try:   # before re-creating — a fresh exchange TP/SL list: a leg may have been placed without a store record
        rows = await cache.tp_sl(it.symbol, fresh=True)
    except BytxError as e:
        report.unverified.append((it, e))
        return
    cids = [plan.get('stop_client_id'), *(c.client_id for c in children)]
    for kind in list(missing):
        hit = find_leg(ex, rows, kind, side, float(missing[kind]), client_ids=cids)
        if hit is None:
            continue
        del missing[kind]
        if hit.kind == 'tp_sl':
            missing.pop('take_profit' if kind == 'stop_loss' else 'stop_loss', None)
        if hit.client_id and store.get(ex, hit.client_id) is None:
            store.adopt(ex, hit, kind=hit.kind, role='sl' if kind == 'stop_loss' else 'tp',
                        position_id=it.position_id, parent=it.client_id, note='adopted: protection found on the exchange')
        if all(o is not hit for o in report.adopted):
            report.adopted.append(hit)
    if not missing:
        store._on_protected(ex, it.client_id)
        return
    if max_age is not None and now_ms(ex) - _entry_moved_ms(it, order) > max_age * 1000:
        # the plan of an old entry is not applied to a position opened later: it may have changed; no position —
        # nothing to protect (as before), there is one — the plan is closed 'expired', the entry — in report.stale_plans
        try:
            pos = await ex.position(it.symbol, side)  # type: ignore[arg-type]
        except BytxError as e:
            report.unverified.append((it, e))
            return
        if pos is None or not pos.contracts:
            store._on_protected(ex, it.client_id)
            return
        _stale(ex, store, it, report, 'expired', f'older than {max_age / 3600:g} h since the last movement of the entry, '
                                                 f'the position may have changed')
        return
    trigger_by = plan.get('trigger_by')
    failed = len(report.failed) + len(report.unverified)
    cache.legs.pop(it.symbol, None)   # placing legs — the symbol's TP/SL cache is stale
    if not attached and order.status not in _TERMINAL and trigger_by is None:
        try:
            with _bind(ex, it):
                await ex._protect(order, {**it.request(), 'tp_sl': missing})
        except BytxError as e:
            report.failed.append((it, e))
            return
        report.protected.extend(order.raw.get('bytx_tp_sl') or [])
        return
    if order.status not in _TERMINAL:
        # the entry still works (a limit order in the book), legs go on the position (trigger_by): no position — legs
        # cannot be placed, and the entry is not "protected" (it would fill without a stop and the next recover would
        # not touch it) — report.deferred
        try:
            pos = await ex.position(it.symbol, side)  # type: ignore[arg-type]
        except BytxError as e:
            report.unverified.append((it, e))
            return
        if pos is None or not pos.contracts:
            report.deferred.append((it, order))
            logger.warning(f'{ex.name}: entry {it.client_id} ({it.symbol}) is open without a fill: protection from '
                           f'the plan — after the fill, call recover() or place TP/SL')
            return
    terminal = order.status in _TERMINAL
    # a partly filled entry — legs for the filled size; it becomes protected after it finishes (duplicate legs of the
    # next recover are excluded by looking the leg up on the exchange)
    size = filled if terminal or filled else it.amount
    for kind in ('stop_loss', 'take_profit'):   # SL first, as in protection.attach_tp_sl
        if kind in missing:
            await _place_leg(ex, store, it, kind, float(missing[kind]), side, size, report, parent=it.client_id,
                             into=report.protected, trigger_by=trigger_by)
    if len(report.failed) + len(report.unverified) == failed and terminal:   # a leg failed / outcome unclear /
        store._on_protected(ex, it.client_id)                                   # the entry still works — recover retries


async def _place_leg(ex: 'Exchange', store: IntentStore, it: Intent, kind: str, trigger: float, side: str,
                     contracts: Optional[float], report: RecoveryReport, *, parent: Optional[str] = None,
                     into: Optional[list] = None, trigger_by: Optional[str] = None,
                     client_id: Optional[str] = None) -> Optional['TpSlOrder']:
    """TP/SL leg for the current position (no larger than it); no position — not placed. A leg of an algorithm's
    order belongs to the algorithm too (the intent's ``algo_id``): a repeated recovery sees it among the algorithm's
    legs."""
    try:
        pos = await ex.position(it.symbol, side)  # type: ignore[arg-type]
        if pos is None or not pos.contracts:
            if it.kind != 'order':   # leg without a position: close the intent
                store.mark(ex, it.client_id, 'abandoned', note=f'{kind}: no {side} position — protection not needed')
                report.abandoned.append(store.get(ex, it.client_id) or it)
            return None
        size = min(contracts, pos.contracts) if contracts else pos.contracts
        with _bind(ex, it, role='tp' if kind == 'take_profit' else 'sl'), store.parent(parent or it.parent), \
                store.algo(it.algo_id):
            order = await ex.place_tp_sl(it.symbol, kind, trigger, position_side=side, contracts=size,
                                         client_id=client_id, trigger_by=trigger_by)  # type: ignore[arg-type]
    except OrderStatusUnknown as e:   # the leg may be placed: its intent stays open — the next recover checks it
        leg = store.get(ex, str(e.client_order_id)) if e.client_order_id else None
        report.unverified.append((leg or it, e))
        return None
    except BytxError as e:
        report.failed.append((it, e))
        return None
    if into is not None:
        into.append(order)
    return order


async def _orphans(ex: 'Exchange', store: IntentStore, adopted_legs: list['Order'], scope: set[str],
                   report: RecoveryReport, cancel: bool) -> None:
    """Own live TP/SL legs without a position on their side: cancel (``cancel``) or keep with a reason.

    Candidates — adopted intent legs and the exchange's open bot-tagged TP/SL on the recovery
    symbols; foreign ones (no intent and no tag) and those without client_id are not candidates at all."""
    from bytx.order_params import owns
    candidates: dict[str, list['Order']] = {}
    seen: set[str] = set()

    def add(o: 'Order') -> None:
        key = o.id or o.client_id or ''
        if key and key not in seen and o.status not in _TERMINAL and getattr(o, 'position_side', None):
            seen.add(key)
            candidates.setdefault(o.symbol, []).append(o)

    for o in adopted_legs:
        add(o)
    if ex.client_id_prefix and ex.market_type != 'spot':   # position TP/SL — derivatives only
        for symbol in sorted(scope | set(candidates)):
            try:
                rows = await ex.tp_sl_orders(symbol)
            except BytxError as e:
                logger.warning(f'{ex.name}: TP/SL {symbol} not received while looking for orphans: {e}')
                continue
            for t in rows:
                if t.client_id and (owns(t, ex.client_id_prefix, ex.id) or store.get(ex, t.client_id) is not None):
                    add(t)
    for symbol, legs in candidates.items():
        await _orphans_of(ex, store, symbol, legs, report, cancel)


async def _orphans_of(ex: 'Exchange', store: IntentStore, symbol: str, rows: list['Order'], report: RecoveryReport,
                      cancel: bool) -> None:
    def keep(orders: Iterable['Order'], reason: str) -> None:
        for o in orders:
            report.orphaned.append(o)
            report.orphan_reasons[o.id or o.client_id or ''] = reason
            logger.warning(f'{ex.name}: leg {getattr(o, "kind", "tp/sl")} {o.client_id or o.id} ({symbol}) without position '
                           f'{getattr(o, "position_side", "")} kept: {reason}')

    entry_sides: set[str] = set()
    if cancel:
        try:   # legs of a not-yet-filled entry (Binance places them at once) are not orphans
            opened = await ex.open_orders(symbol)
        except BytxError as e:
            keep(rows, f'open orders not read ({e}) — not cancelling')
            return
        entry_sides = {'long' if o.side == 'buy' else 'short' for o in opened
                       if not o.reduce_only and o.status not in _TERMINAL}
    try:   # positions — right before cancelling: "no" ≠ "could not check"
        sides = {p.side for p in await ex.positions([symbol]) if p.contracts}
    except BytxError as e:
        keep(rows, f'positions not read ({e}) — not cancelling')
        return
    orphans = [o for o in rows if getattr(o, 'position_side', None) not in sides]
    if not orphans:
        return
    if not cancel:
        keep(orphans, 'cancel_orphaned=False — a reduce-only leg will close the next position on this side')
        return
    waiting = [o for o in orphans if getattr(o, 'position_side', None) in entry_sides]
    keep(waiting, 'entry on this side is still open — legs wait for its fill')
    targets = [o for o in orphans if all(o is not w for w in waiting)]
    if not targets:
        return
    try:
        results = await ex._cancel_tp_sl_list(symbol, targets)  # type: ignore[arg-type]
    except BytxError as e:
        keep(targets, f'cancel failed: {e}')
        return
    for o, r in zip(targets, results):
        if r.status == 'error':
            keep([o], f'cancel failed: {r.error}')
            continue
        done = r.order or o
        if r.status == 'canceled':
            done.status = 'canceled'
            report.orphans_canceled.append(done)
            logger.warning(f'{ex.name}: orphaned leg {getattr(o, "kind", "tp/sl")} {o.client_id or o.id} '
                           f'({symbol}) cancelled: no {getattr(o, "position_side", "")} position')
        if r.status == 'canceled':
            await _record_orphan(ex, store, o, done, 'canceled', 'orphan_canceled: no position — cancelled during recovery')
        else:
            await _record_orphan(ex, store, o, done, done.status, 'orphan: already closed on the exchange')


async def _record_orphan(ex: 'Exchange', store: IntentStore, o: 'Order', done: 'Order', status: Optional[str],
                         note: str) -> None:
    """Cancelled (or already closed) orphaned leg — into the intent store and the position journal."""
    cid = o.client_id
    if cid:
        if store.get(ex, cid) is None:
            store.adopt(ex, done, kind=getattr(o, 'kind', 'stop_loss'), note=note)
        store.mark(ex, cid, 'final', order_id=o.id or None, status=status, note=note)
    if ex.journal is not None and status == 'canceled':
        await ex.journal._on_canceled(ex, done, True)


# ─── unfinished execution algorithms ─────────────────────────────────────

async def _recover_algo(ex: 'Exchange', store: IntentStore, rec: AlgoRecord, cache: '_Cache', report: RecoveryReport,
                        cancel: bool, protect: bool = True, protect_max_age: Optional[float] = None) -> None:
    """Unfinished algorithm: cancel its working orders (already reconciled above), keep protection, report it all; a
    bracket with a plan — the missing legs from the plan (after cancelling the entry: the fill is known). The record is
    closed only if no working orders are left, all of them were checked and the plan protection is done."""
    a = AlgoRecovery(algo_id=rec.algo_id, kind=rec.kind, symbol=rec.symbol, side=rec.side, state=rec.state,
                     started_ms=rec.created_ms, size=rec.size)
    report.algos.append(a)
    if rec.algo_id in store.running_algos:
        a.notes.append('the algorithm runs in this process — it manages its orders itself, not touched')
        return
    seen = [*report.adopted, *report.finished, *report.resubmitted]
    unverified = {i.client_id for i, _ in report.unverified}
    abandoned = {i.client_id for i in report.abandoned}
    working_left = protection = stop_kept = False
    for it in store.algo_children(ex, rec.algo_id):
        order = _seen(seen, it)
        if it.client_id in unverified or it.state in ('pending', 'unknown'):
            a.unverified.append(it.client_id)
            working_left = working_left or it.kind == 'order'   # an unverified protection leg doesn't hold the record
            continue
        if order is None:
            if it.client_id in abandoned:
                a.abandoned.append(it.client_id)
            continue
        if order.status in _TERMINAL:
            if any(order is o for o in report.finished):
                a.finished.append(order)
            continue
        key = order.id or order.client_id or it.client_id
        if it.kind != 'order':
            a.kept.append(order)
            a.reasons[key] = 'position protection: the algorithm\'s TP/SL is not canceled (no position — orphan logic)'
            protection = True
            stop_kept = stop_kept or it.kind in ('stop_loss', 'tp_sl')
            continue
        if not cancel:
            a.kept.append(order)
            a.reasons[key] = 'cancel_orphaned_algos=False — the working order is left without the algorithm'
            working_left = True
            continue
        try:
            done = await _cancel_algo_order(ex, store, it, order)
        except BytxError as e:
            a.kept.append(order)
            a.reasons[key] = f'cancel failed: {e}'
            working_left = True
            logger.warning(f'{ex.name}: working order {it.client_id} of algorithm {rec.algo_id} not canceled: {e}')
            continue
        report.adopted = [o for o in report.adopted if o is not order]
        report.resubmitted = [o for o in report.resubmitted if o is not order]
        if done.status == 'canceled':
            a.canceled.append(done)
            logger.warning(f'{ex.name}: working order {it.client_id} ({order.id}) of unfinished algorithm '
                           f'{rec.algo_id} canceled by recovery')
        else:   # filled (expired) between reconciliation and the cancel — nothing to cancel
            a.finished.append(done)
            report.finished.append(done)
    for it in store.algo_children(ex, rec.algo_id):    # filled — from intents after the cancels
        if it.kind == 'order' and it.side == rec.side and it.filled:
            a.filled += float(it.filled)
    if a.filled > 0:
        a.notes.append(f'filled {a.filled:g} contracts — the position is not touched')
    plan_left = False
    if rec.kind == 'bracket' and rec.plan and not rec.plan.get('retired'):
        plan_left = not await _bracket_protect(ex, store, rec, a, cache, report, protect, protect_max_age)
    elif rec.kind == 'bracket' and a.filled > 0 and not stop_kept:
        if rec.plan:
            why = rec.plan['retired']
            a.notes.append('bracket: the protection plan is done — the algorithm placed the legs itself, the plan '
                           'places none' if why == 'protected' else
                           f'bracket: the protection plan was retired by the algorithm ({why}) — the plan places no '
                           f'legs')
        await _bracket_note(ex, rec, a, cache)
    if cancel and not working_left and not plan_left:
        a.closed = True
        what = f'canceled {len(a.canceled)}' + (f', protection kept {len(a.kept)}' if a.kept else '') + \
            (f', legs from the plan {len(a.protected)}' if a.protected else '')
        store.mark_algo(ex, rec.algo_id, 'recovered', note=f'recover: {what}')


def _seen(orders: Iterable['Order'], it: Intent) -> Optional['Order']:
    """The intent's order among those reconciled by this recovery (by id, then by client_id)."""
    cid = it.client_id.lower()
    for o in orders:
        if (it.order_id and o.id == it.order_id) or (o.client_id and o.client_id.lower() == cid):
            return o
    return None


async def _cancel_algo_order(ex: 'Exchange', store: IntentStore, it: Intent, order: 'Order') -> 'Order':
    """Cancels an algorithm's working order the SDK way (intent and journal — as with cancel_order); the filled part —
    from the re-read order (a cancel response often has no size). Filled before the cancel — the final status from the
    exchange (``closed``, as in cancel_order). Status after the cancel not final — BytxError."""
    with _bind(ex, it):
        done = await ex.cancel_order(it.symbol, order.id)
    try:
        fresh = await ex.order(it.symbol, order.id)
    except BytxError:
        fresh = None
    if fresh is not None and fresh.status in _TERMINAL:
        done = fresh
    if done.status not in _TERMINAL:
        raise BytxError(f'order {order.id}: status after cancel {done.status!r}', exchange=ex.name)
    note = (f'algo_canceled: algorithm {it.algo_id} unfinished — canceled' if done.status == 'canceled' else
            f'algo: algorithm {it.algo_id} unfinished — the order finished ({done.status}) before the cancel')
    store.mark(ex, it.client_id, 'final', order_id=done.id or order.id, status=done.status,
               filled=_num(done.filled_contracts), note=note)
    return done


async def _bracket_note(ex: 'Exchange', rec: AlgoRecord, a: AlgoRecovery, cache: '_Cache') -> None:
    """The bracket entry is filled, no live algorithm legs with a client_id (the process died before the protection,
    or legs without a client_id — Bybit trading-stop, BingX): the position is open, and there is no TP/SL on its side
    on the exchange — an explanation (the legs themselves are not placed: algorithms are not resumed)."""
    side = 'long' if rec.side == 'buy' else 'short'
    try:
        pos = await ex.position(rec.symbol, side)  # type: ignore[arg-type]
        if pos is None or not pos.contracts:
            return
        rows = await cache.tp_sl(rec.symbol, fresh=True)
    except BytxError as e:
        a.notes.append(f'bracket: position or TP/SL not read ({e}) — check the protection of the {side} position')
        a.unprotected = True
        return
    live = [x for x in rows if getattr(x, 'position_side', None) in (side, None)
            and getattr(x, 'state', None) in (None, 'open')]
    a.unprotected = not any(getattr(x, 'kind', None) in ('stop_loss', 'tp_sl') for x in live)
    if live:
        a.notes.append(f'bracket: no algorithm legs with a client_id; TP/SL of the {side} side on the exchange: '
                       f'{len(live)} — not touched (legs without a client_id — Bybit trading-stop, BingX — can\'t be '
                       f'recognized from intents)')
    else:
        a.notes.append(f'bracket: the {side} position is open, no TP/SL on its side on the exchange — the position is '
                       f'unprotected')


#: bracket plan legs — SL first, as in protection.attach_tp_sl
_PLAN_KINDS = ('stop_loss', 'take_profit')
#: plan protection results that keep the algorithm record open: the next recover() retries (no longer than
#: ``protect_max_age``); the others — the record is closed, the decision is the caller's (``report.unprotected``)
_PLAN_RETRY = frozenset({'unverified', 'failed'})
#: SL results with which the position may be left without a stop (``AlgoRecovery.unprotected``)
_NAKED = frozenset({'price_passed', 'disabled', 'failed', 'unverified', 'expired', 'too_small', 'canceled'})
_KIND_NAMES = {'stop_loss': 'SL', 'take_profit': 'TP'}
_EPS = 1e-12


async def _bracket_protect(ex: 'Exchange', store: IntentStore, rec: AlgoRecord, a: AlgoRecovery, cache: '_Cache',
                           report: RecoveryReport, protect: bool, max_age: Optional[float]) -> bool:
    """Position protection from the bracket plan (see the module): missing legs for the filled size, no more than the
    position of the plan's side. The result per leg — ``a.protection``; True — protection is done or not needed (the
    record can be closed)."""
    children = store.algo_children(ex, rec.algo_id)
    entry = next((c for c in children if c.kind == 'order' and not c.reduce_only), None)
    plan = rec.plan or {}
    kinds = [k for k in _PLAN_KINDS if plan.get(k) is not None]
    if entry is None or not kinds:
        return True
    job = _PlanProtection(ex, store, rec, a, cache, report, entry)
    await job.run(kinds, protect, max_age)
    a.unprotected = a.protection.get('stop_loss') in _NAKED
    return not any(v in _PLAN_RETRY for v in a.protection.values())


class _PlanProtection:
    """Missing plan legs of one bracket (see the module): outcome and fill of the algorithm's orders (the entry is
    re-read) → position of the plan's side → the algorithm's leg intents → open TP/SL on the exchange (own — by
    client_id; by price — only legs without a client_id created no earlier than the entry) → plan age → price by
    ``trigger_by`` → placing with the plan client_id."""

    def __init__(self, ex: 'Exchange', store: IntentStore, rec: AlgoRecord, a: AlgoRecovery, cache: '_Cache',
                 report: RecoveryReport, entry: Intent) -> None:
        self.ex, self.store, self.rec, self.a, self.cache, self.report = ex, store, rec, a, cache, report
        self.entry = entry
        self.plan: dict = rec.plan or {}
        self.side: str = self.plan.get('side') or ('long' if rec.side == 'buy' else 'short')
        # the last known moment of the entry by the exchange clock (fill / order update — the re-read response)
        self.entry_ts: Optional[int] = None

    def outcome(self, kinds: Iterable[str], value: str, note: str, *, warn: bool = False) -> None:
        for k in kinds:
            self.a.protection[k] = value
        self.a.notes.append(f'bracket: {note}')
        if warn:
            logger.warning(f'{self.ex.name}: bracket {self.rec.algo_id} ({self.rec.symbol} {self.side}): {note}')

    def unverified(self, kinds: Iterable[str], what: str, error: BaseException) -> None:
        self.report.unverified.append((self.entry, error))
        self.outcome(kinds, 'unverified', f'{what} not read ({error}) — protection from the plan is not placed, repeat '
                                          f'recover()', warn=True)

    def children(self) -> tuple[list[Intent], list[Intent]]:
        rows = self.store.algo_children(self.ex, self.rec.algo_id)
        return [c for c in rows if c.kind == 'order'], [c for c in rows if c.kind != 'order']

    def expired(self, kinds: list[str], max_age: Optional[float]) -> bool:
        """The plan is older than ``max_age`` since the last known moment of the entry (a limit entry may have rested
        longer than a day and filled just now) — not applied: the position may have changed. Checked only when the
        position of the plan's side exists and legs are missing (no position — no "naked position")."""
        if max_age is None:
            return False
        since = max(self.rec.created_ms, self.entry_ts or self.entry.updated_ms or 0)
        if now_ms(self.ex) - since <= max_age * 1000:
            return False
        self.outcome(kinds, 'expired', f'the protection plan is older than {max_age / 3600:g} h (since the last movement '
                                       f'of the entry) — not applied (the position may have changed): check the '
                                       f'protection of the {self.side} {self.rec.symbol} position', warn=True)
        return True

    async def run(self, kinds: list[str], protect: bool, max_age: Optional[float]) -> None:
        rec = self.rec
        orders, _ = self.children()
        checked = {i.client_id for i, _ in self.report.unverified}
        if any(c.state in ('pending', 'unknown') or c.client_id in checked for c in orders):
            self.outcome(kinds, 'unverified', 'an algorithm order outcome is not checked — protection from the plan is '
                                              'not placed, repeat recover()')
            return
        known = await self.refresh(orders, kinds)
        if known is None:
            return
        orders, legs = self.children()
        entry = next(c for c in orders if c.client_id == self.entry.client_id)
        self.entry = entry
        # filled by the entry minus what the algorithm closed itself (reduce-only close), contracts
        filled = sum(float(c.filled or 0.0) for c in orders if not c.reduce_only and c.side == rec.side) \
            - sum(float(c.filled or 0.0) for c in orders if c.reduce_only)
        if entry.state == 'placed' and (filled > _EPS or not known):
            self.outcome(kinds, 'unverified', 'the entry still works (cancel_orphaned_algos=False or the cancel failed) '
                                              '— no legs for a partial size, repeat recover() after it finishes',
                         warn=True)
            return
        if known and filled <= _EPS:
            return            # the entry is not filled — nothing to protect
        if any(c.state == 'final' and c.status == 'closed' for c in legs):
            self.outcome(kinds, 'triggered', 'an algorithm leg triggered — protection from the plan is not needed')
            return
        pos = await self.position(kinds)
        if pos is None:
            return
        if not known:
            if not self.expired(kinds, max_age):
                self.outcome(kinds, 'unverified', 'the entry fill is unknown (the order is not in the exchange history, '
                                                  'the size is not in the store) — protection from the plan is not '
                                                  'placed', warn=True)
            return
        missing = []
        for k in kinds:
            own = [c for c in legs if c.kind in (k, 'tp_sl')]
            if any(c.state in ('pending', 'unknown') or c.client_id in checked for c in own):
                self.outcome([k], 'unverified', f'the algorithm {_KIND_NAMES[k]} leg is not checked — not placed, '
                                                f'repeat recover()')
            elif any(c.state == 'placed' for c in own):
                self.a.protection[k] = 'present'
            elif any(c.state == 'final' or _was_placed(c) for c in own):
                self.outcome([k], 'canceled', f'the algorithm {_KIND_NAMES[k]} leg was canceled (by hand or by the '
                                              f'exchange) — not re-created', warn=True)
            else:
                missing.append(k)
        if missing:
            await self.place(missing, legs, min(filled, pos.contracts), pos, protect, max_age)

    async def refresh(self, orders: list[Intent], kinds: list[str]) -> Optional[bool]:
        """Finished orders of the algorithm are re-read from the exchange: a cancel response on Bybit / OKX / KuCoin / HL
        / Bitget has only the id, and ``ex.order()`` does not update the store — the entry fill in the intent may have
        stayed 0. True — the fill is known; False — the order is not in the history and the entry size is not recorded;
        None — a read failure
        ('unverified')."""
        known = True
        for c in orders:
            if _was_placed(c) and not c.reduce_only and not c.filled:
                known = False      # the exchange accepted the entry and it is gone from its history, the size is not recorded
                continue
            if c.state != 'final' or not c.order_id:
                continue
            try:
                o = await self.ex.order(self.rec.symbol, c.order_id)
            except OrderNotFound:
                o = None
            except BytxError as e:
                self.unverified(kinds, 'fills of the algorithm orders', e)
                return None
            got = o.filled_contracts if o is not None else None
            if o is not None and c.client_id == self.entry.client_id:
                raw = o.raw if isinstance(o.raw, dict) else {}
                stamps = [x for x in (raw.get('lastTradeTimestamp'), raw.get('lastUpdateTimestamp'), o.timestamp)
                          if isinstance(x, (int, float)) and x > 0]
                self.entry_ts = int(max(stamps)) if stamps else None
            if got is None:
                known = known and (c.reduce_only or bool(c.filled))
                continue
            if got >= float(c.filled or 0.0) - _EPS:
                self.store._on_placed(self.ex, o, c.client_id)  # type: ignore[arg-type]
        return known

    async def position(self, kinds: list[str]) -> Optional[Any]:
        """The position of the plan's side; none (or only the other side) — the result for all legs and None."""
        ex, rec, side = self.ex, self.rec, self.side
        try:
            rows = [p for p in await ex.positions([rec.symbol]) if p.symbol == rec.symbol and p.contracts]
        except BytxError as e:
            self.unverified(kinds, 'positions', e)
            return None
        pos = next((p for p in rows if p.side == side), None)
        if pos is None:
            other = sorted({p.side or '' for p in rows if p.side != side})
            if other:
                self.outcome(kinds, 'opposite_side', f'no {side} position, a position of the other side is open '
                                                     f'({", ".join(other)}) — the plan legs are not placed, the '
                                                     f'position is not touched', warn=True)
            else:
                self.outcome(kinds, 'no_position', f'no {side} position — protection from the plan is not needed')
        return pos

    async def place(self, missing: list[str], legs: list[Intent], size: float, pos: Any, protect: bool,
                    max_age: Optional[float]) -> None:
        ex, rec, side = self.ex, self.rec, self.side
        try:   # a fresh list before placing: a leg may be placed without a record (Bybit / BingX — no client_id)
            listed = await self.cache.tp_sl(rec.symbol, fresh=True)
        except BytxError as e:
            self.unverified(missing, 'TP/SL', e)
            return
        self.found(listed, legs, missing)
        if not missing or self.expired(missing, max_age):
            return
        if not protect:
            self.outcome(missing, 'disabled', f'{side} position without the plan legs '
                                              f'{"/".join(_KIND_NAMES[k] for k in missing)}: protect=False — not '
                                              f'placed, the caller decides', warn=True)
            return
        minimum = _min_contracts(ex, rec.symbol)
        if size < minimum * (1 - 1e-9):
            self.outcome(missing, 'too_small', f'own size {size:g} is below the minimum order {minimum:g} — legs are '
                                               f'not placed, the caller decides', warn=True)
            return
        try:
            price = await ex._trigger_price(rec.symbol, self.plan.get('trigger_by'), mark=pos.mark_price)
        except BytxError as e:
            self.unverified(missing, 'price', e)
            return
        for k in missing:
            await self.place_leg(k, size, price)

    def found(self, listed: list['TpSlOrder'], legs: list[Intent], missing: list[str]) -> None:
        """Plan legs already on the exchange without a live intent — 'present', removed from ``missing``. An own leg — by
        the plan client_id or the algorithm's legs; by kind + side + price — only a leg without a client_id (Bybit
        trading-stop, BingX), and one such leg — for one bracket per recovery (another own bracket with the same
        prices, a foreign leg of another bot). Foreign client_ids are not counted and not adopted."""
        from bytx.protection import find_leg
        cids = {str(c).lower() for c in (self.plan.get('stop_loss_client_id'), self.plan.get('take_profit_client_id'),
                                        *(c.client_id for c in legs)) if c}
        claimed = self.cache.claimed
        ours = [o for o in listed if (o.client_id and o.client_id.lower() in cids)
                or (not o.client_id and (o.id or '') not in claimed and not self.older(o))]
        for k in list(missing):
            if k not in missing:
                continue
            hit = find_leg(self.ex, ours, k, self.side, float(self.plan[k]), client_ids=cids)
            if hit is None:
                continue
            for x in [x for x in missing if x == k or hit.kind == 'tp_sl']:
                missing.remove(x)
                self.a.protection[x] = 'present'
            if hit.client_id:
                if self.store.get(self.ex, hit.client_id) is None:
                    self.store.adopt(self.ex, hit, kind=hit.kind, role='sl' if k == 'stop_loss' else 'tp',
                                     position_id=self.entry.position_id, parent=self.entry.client_id,
                                     note='adopted: plan protection found on the exchange', algo_id=self.rec.algo_id)
            elif hit.id:
                claimed.add(hit.id)
            how = 'by the plan / algorithm leg client_id' if hit.client_id else 'no client_id — by kind, side and price'
            self.a.notes.append(f'bracket: leg {hit.kind} {hit.client_id or hit.id} is already on the exchange ({how}) '
                                f'— a second one is not placed')

    def older(self, o: 'TpSlOrder') -> bool:
        """A leg without a client_id created before this bracket's entry cannot be its leg (legs are placed after the
        entry fills): the leg's exchange time against the entry's exchange time (1 s slack), without it — against the
        algorithm start by the local clock (5 s slack for clock skew). The exchange gives no leg creation time — not
        excluded."""
        ts = o.timestamp
        if not isinstance(ts, (int, float)) or ts <= 0:
            return False
        return ts < (self.entry_ts - 1000 if self.entry_ts else self.rec.created_ms - 5000)

    async def place_leg(self, kind: str, size: float, price: float) -> None:
        trigger, name = float(self.plan[kind]), _KIND_NAMES[kind]
        if _passed(kind, self.side, trigger, price):
            self.outcome([kind], 'price_passed',
                         f'price {price:g} passed {name} {trigger:g} — the leg is not placed (it would trigger at once or '
                         f'be rejected by the exchange)' + (', the position has no stop: whether to close it is the '
                                                            'caller\'s decision' if kind == 'stop_loss' else ''),
                         warn=True)
            return
        failed, unknown = len(self.report.failed), len(self.report.unverified)
        leg = await _place_leg(self.ex, self.store, self.entry, kind, trigger, self.side, size, self.report,
                               parent=self.entry.client_id, into=self.report.protected,
                               trigger_by=self.plan.get('trigger_by'), client_id=self.plan.get(f'{kind}_client_id'))
        if leg is not None:
            self.a.protected.append(leg)
            if leg.id and not leg.client_id:
                self.cache.claimed.add(leg.id)
            qty = f' for {leg.contracts:g} contracts' if leg.contracts is not None else ''
            self.outcome([kind], 'placed', f'leg {name} {trigger:g}{qty} placed from the plan '
                                           f'({leg.client_id or leg.id})', warn=True)
        elif len(self.report.unverified) > unknown:
            self.outcome([kind], 'unverified', f'the outcome of leg {name} {trigger:g} is unknown: '
                                               f'{self.report.unverified[-1][1]} — the next recover() checks it by '
                                               f'client_id', warn=True)
        elif len(self.report.failed) > failed:
            self.outcome([kind], 'failed', f'leg {name} {trigger:g} not placed: {self.report.failed[-1][1]}',
                         warn=True)
        else:
            self.outcome([kind], 'no_position', f'no {self.side} position (closed before placing {name}) — '
                                                f'protection is not needed')


def _min_contracts(ex: 'Exchange', symbol: str) -> float:
    """The market's minimum order in contracts (size step, ``min_amount``); market not read — 0 (the exchange checks)."""
    try:
        from bytx.markets import to_market
        m = to_market(ex.ccxt.market(symbol), ex.ccxt.id)
    except Exception:  # noqa: BLE001
        return 0.0
    return max(float(m.amount_step or 0.0), float(m.min_amount or 0.0))


def _passed(kind: str, side: str, trigger: float, price: float) -> bool:
    """The price (by the leg's ``trigger_by`` — :meth:`Exchange._trigger_price`) is already past the leg trigger (it
    would trigger at once): TP long and SL short — on a rise, SL long and TP short — on a fall."""
    up = (kind == 'take_profit') == (side == 'long')
    return price >= trigger if up else price <= trigger


def _algo_legs_orphaned(report: RecoveryReport) -> None:
    """Algorithm legs canceled by the orphan logic (no position) — from ``kept`` to notes."""
    gone = {o.id for o in report.orphans_canceled if o.id}
    for a in report.algos:
        for o in [x for x in a.kept if x.id in gone]:
            a.kept.remove(o)
            a.reasons.pop(o.id or o.client_id or '', None)
            a.notes.append(f'leg {getattr(o, "kind", "tp/sl")} {o.client_id or o.id} canceled as orphaned '
                           f'(no position) — orphans_canceled')


# ─── exchange lookup ─────────────────────────────────────────────────────────

class _Cache:
    """Open orders / TP/SL of a symbol — one request per symbol per recovery."""

    def __init__(self, ex: 'Exchange') -> None:
        self.ex = ex
        self.orders: dict[str, list['Order']] = {}
        self.legs: dict[str, list['TpSlOrder']] = {}
        # legs without a client_id already counted (or placed) for one bracket — not counted by price for another
        self.claimed: set[str] = set()

    async def open_orders(self, symbol: str) -> list['Order']:
        if symbol not in self.orders:
            self.orders[symbol] = await self.ex.open_orders(symbol)
        return self.orders[symbol]

    async def tp_sl(self, symbol: str, *, fresh: bool = False) -> list['TpSlOrder']:
        if fresh or symbol not in self.legs:
            self.legs[symbol] = await self.ex.tp_sl_orders(symbol)
        return self.legs[symbol]


def _match(rows: Iterable['Order'], it: Intent) -> Optional['Order']:
    cid = it.client_id.lower()
    for o in rows:
        if (it.order_id and o.id == it.order_id) or (o.client_id and o.client_id.lower() == cid):
            return o
    return None


async def _find(ex: 'Exchange', it: Intent, cache: _Cache) -> 'Order':
    """The intent's order or _Absent (confirmed absent); other errors — "not verified"."""
    if it.order_id:
        try:
            return await ex.order(it.symbol, it.order_id)
        except OrderNotFound:
            pass
    try:
        return _not_older(ex, it, await ex.order(it.symbol, client_id=it.client_id))
    except OrderNotFound:
        pass
    hit = _match(await cache.open_orders(it.symbol), it)
    if hit is not None:
        return _not_older(ex, it, hit)
    raise _Absent(it.client_id)


#: exchange/host clock slack: an order found by client_id older than the intent by more than this is not its order
_REUSED_CID_SLACK_MS = 5000


def _not_older(ex: 'Exchange', it: Intent, order: 'Order') -> 'Order':
    """An order found by client_id was created before the intent (exchange clock, 5 s slack) — an order of a previous
    entry with a reused client_id, not this one: "not checked", neither "filled" nor "re-create"."""
    ts = order.timestamp
    # order time — epoch ms (ccxt timestamp); 0 / stubs without time are not compared
    if isinstance(ts, (int, float)) and ts >= 10 ** 12 and it.created_ms and ts < it.created_ms - _REUSED_CID_SLACK_MS:
        raise BytxError(f'client_id {it.client_id} was reused: found order {order.id} of a previous entry (created '
                            f'{int(ts)}, intent — {it.created_ms}); the outcome of the new order is not checked',
                            exchange=ex.name, details={'client_id': it.client_id, 'order_id': order.id})
    return order


async def _find_quiet(ex: 'Exchange', it: Intent, cache: _Cache) -> Optional['Order']:
    try:
        return await _find(ex, it, cache)
    except (_Absent, BytxError):
        return None


async def _find_leg(ex: 'Exchange', it: Intent, cache: _Cache) -> 'Order':
    """TP/SL leg: open conditional orders; none — final status by id / client_id."""
    hit = _match(await cache.tp_sl(it.symbol), it)
    if hit is not None:
        return hit
    if it.order_id:
        try:
            return await ex.tp_sl_order(it.symbol, it.order_id)
        except OrderNotFound:
            pass
    try:
        return await ex.order(it.symbol, client_id=it.client_id)
    except OrderNotFound:
        raise _Absent(it.client_id) from None


async def _adopt_untracked(ex: 'Exchange', store: IntentStore, cache: _Cache, symbols: set[str],
                           report: RecoveryReport) -> None:
    from bytx.order_params import owns
    for symbol in sorted(symbols):
        try:
            rows = await cache.open_orders(symbol)
        except BytxError as e:
            logger.warning(f'{ex.name}: open orders {symbol} not received during recovery: {e}')
            continue
        for o in rows:
            if not o.client_id or not owns(o, ex.client_id_prefix, ex.id) or store.get(ex, o.client_id) is not None:
                continue
            role, position_id = await _journal_ref(ex, o)
            store.adopt(ex, o, role=role, position_id=position_id)
            report.untracked.append(o)
            it = store.get(ex, o.client_id)
            if it is not None:
                await _journal(ex, it, o)


# ─── journal, policy, time ────────────────────────────────────────────────

@contextlib.contextmanager
def _bind(ex: 'Exchange', it: Intent, *, role: Optional[str] = None) -> Iterator[None]:
    journal = ex.journal
    if journal is None:
        yield
        return
    with journal.bind(position_id=it.position_id, role=role or it.role, level_id=it.level_id):
        yield


async def _journal(ex: 'Exchange', it: Intent, order: 'Order', *, tp_sl: bool = False) -> None:
    """Adopted order — into the position journal with the intent's role and position (idempotent)."""
    journal = ex.journal
    if journal is None:
        return
    with _bind(ex, it):
        if tp_sl:
            await journal._on_tp_sl(ex, [order])
        else:
            await journal._on_placed(ex, order, {'reduce_only': it.reduce_only, 'journal_role': it.role})


async def _journal_ref(ex: 'Exchange', order: 'Order') -> tuple[Optional[str], Optional[str]]:
    journal = ex.journal
    if journal is None or not order.client_id:
        return None, None
    try:
        events = await journal.store.find_order(bot_id=journal.bot_id, client_order_id=order.client_id)
    except Exception as e:  # the service journal is unavailable — adopt without a role
        logger.warning(f'{ex.name}: journal did not respond for {order.client_id}: {e}')
        return None, None
    for ev in events:
        return ev.role, ev.position_id
    return None, None


async def _may_resubmit(ex: 'Exchange', it: Intent, policy: ResubmitPolicy, max_age: Optional[float]) -> bool:
    if not policy or (it.kind == 'order' and not it.resubmittable):
        return False
    if max_age is not None and now_ms(ex) - it.created_ms > max_age * 1000:
        return False
    if policy is True:
        return True
    result = policy(it)  # type: ignore[operator]
    if inspect.isawaitable(result):
        result = await result
    return bool(result)


def _now(ex: 'Exchange', clock: Any) -> int:
    return int(clock.now_ms()) if clock is not None and hasattr(clock, 'now_ms') else now_ms(ex)


async def _settle(ex: 'Exchange', clock: Any, intents: list[Intent], settle: float) -> None:
    fresh = [i.created_ms for i in intents if i.state in ('pending', 'unknown')]
    if not fresh or settle <= 0:
        return
    wait_ms = max(fresh) + int(settle * 1000) - _now(ex, clock)
    if wait_ms <= 0:
        return
    if clock is not None and hasattr(clock, 'sleep'):
        await clock.sleep(wait_ms / 1000)
    else:
        import asyncio
        await asyncio.sleep(wait_ms / 1000)


def _log(ex: 'Exchange', r: RecoveryReport) -> None:
    parts = [f'{name} {len(getattr(r, name))}' for name in
             ('adopted', 'finished', 'resubmitted', 'protected', 'abandoned', 'unverified', 'failed', 'untracked',
              'orphans_canceled', 'orphaned', 'algos')
             if getattr(r, name)]
    canceled = sum(len(a.canceled) for a in r.algos)
    if canceled:
        parts.append(f'algorithm orders canceled {canceled}')
    naked = r.unprotected
    if naked:
        parts.append(f'positions without a stop {len(naked)} ({", ".join(f"{a.symbol} {a.algo_id}" for a in naked)})')
    if parts:
        log = logger.warning if not r.ok or naked else logger.info
        log(f'{ex.name}: intent recovery — ' + ', '.join(parts))


__all__ = ['AlgoRecovery', 'RecoveryReport', 'ResubmitPolicy', 'recover']
