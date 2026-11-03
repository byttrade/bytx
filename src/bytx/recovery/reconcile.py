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

from bytx.errors import BytxError, ConfigError, OrderNotFound
from bytx.recovery.store import Intent, IntentStore, now_ms

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
    entries; ``abandoned`` — dangling intents closed without sending (no order);
    ``unverified`` — could not verify (left open, retry later); ``failed`` —
    re-creation rejected by the exchange; ``untracked`` — bot-tagged orders without an intent (recorded);
    ``orphans_canceled`` — own live TP/SL legs without a position on their side, cancelled by recovery
    (``cancel_orphaned=True``); ``orphaned`` — such legs left live, reason in
    ``orphan_reasons`` (order id → text: ``cancel_orphaned=False``, positions not read, entry
    on that side still open, cancel failed). They don't affect ``ok``.
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
    pruned: int = 0

    @property
    def ok(self) -> bool:
        """All intents reconciled: no ``unverified`` and no ``failed``."""
        return not self.unverified and not self.failed

    @property
    def live(self) -> list['Order']:
        """All live orders after recovery: adopted, re-created, protection, without an intent."""
        return [o for o in (*self.adopted, *self.resubmitted, *self.protected, *self.untracked)
                if o.status not in _TERMINAL]


class _Absent(Exception):
    """No order: the client_id lookup answered "not found", and it is not among open orders either."""


async def recover(ex: 'Exchange', *, resubmit: ResubmitPolicy = False, max_age: Optional[float] = 300.0,
                  settle: float = 3.0, protect: bool = True, adopt_untracked: bool = True,
                  symbols: Optional[Iterable[str]] = None, prune_after: Optional[float] = 7 * 86400.0,
                  clock: Any = None, cancel_orphaned: bool = True) -> RecoveryReport:
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
    """
    store: Optional[IntentStore] = getattr(ex, 'intents', None)
    if store is None:
        raise ConfigError('recover: Exchange has no intent store (create_exchange(intents=IntentStore(...)))',
                          exchange=ex.name)
    await ex.connect()
    clock = clock or getattr(ex, 'clock', None)
    report = RecoveryReport()
    pending = store.open_intents(ex)
    await _settle(ex, clock, pending, settle)
    cache = _Cache(ex)
    orders = [i for i in pending if i.kind == 'order']
    legs = [i for i in pending if i.kind != 'order']
    entries: list[tuple[Intent, 'Order']] = []
    for it in orders:
        await _recover_order(ex, store, it, cache, report, entries, resubmit, max_age)
    for it in legs:
        await _recover_leg(ex, store, it, cache, report, resubmit, max_age)
    leg_ids = {i.client_id for i in legs}
    adopted_legs = [o for o in report.adopted if o.client_id in leg_ids]
    # entries whose TP/SL never got recorded (the process crashed between the entry and the protection)
    if protect:
        for it in store.intents(ex, states=('placed', 'final')):
            if it.kind == 'order' and it.tp_sl and not it.protected and \
                    all(e.client_id != it.client_id for e, _ in entries):
                order = await _find_quiet(ex, it, cache)
                if order is not None:
                    entries.append((it, order))
        for it, order in entries:
            await _complete_protection(ex, store, it, order, report, cache)
    scope = {i.symbol for i in pending} | set(symbols or ())
    if adopt_untracked and ex.client_id_prefix:
        await _adopt_untracked(ex, store, cache, scope, report)
    # orphaned legs are searched across all store symbols: legs attached by the exchange have no own intents
    await _orphans(ex, store, adopted_legs, scope | {i.symbol for i in store.intents(ex)}, report, cancel_orphaned)
    if prune_after is not None:
        report.pruned = store.prune(ex, older_than_ms=_now(ex, clock) - int(prune_after * 1000))
    _log(ex, report)
    return report


# ─── reconciling one intent ────────────────────────────────────────────────

async def _recover_order(ex: 'Exchange', store: IntentStore, it: Intent, cache: '_Cache', report: RecoveryReport,
                         entries: list, resubmit: ResubmitPolicy, max_age: Optional[float]) -> None:
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
        if it.tp_sl and not it.protected:
            entries.append((it, order))
        return
    if it.state == 'placed':
        # the exchange confirmed the order, but now it is nowhere (history archived) — not live
        store.mark(ex, it.client_id, 'abandoned', note='missing: confirmed earlier, not found on the exchange')
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
    an exchange rejection and one closed by recovery; a manually cancelled one is not re-created). An OKX pair (kind
    'tp_sl') — both sides."""
    return c.kind in (kind, 'tp_sl') and c.state not in ('abandoned', 'rejected')


def _replacement(store: IntentStore, ex: 'Exchange', it: Intent) -> Optional[Intent]:
    """A live (or possibly live) leg of the same kind on the same entry — re-created by a previous
    recovery in place of ``it``. Checked before any network call: a crash between placing and closing ``it``
    does not produce a second leg."""
    rows = store.children(ex, it.client_id)
    if it.parent:
        rows += store.children(ex, it.parent)
    return next((c for c in rows if c.client_id != it.client_id and c.created_ms >= it.created_ms
                 and _covers(c, it.kind)), None)


async def _complete_protection(ex: 'Exchange', store: IntentStore, it: Intent, order: 'Order',
                               report: RecoveryReport, cache: '_Cache') -> None:
    """Entry exists, its TP/SL are not in the store — reconcile with the exchange and place missing legs (as place_order).

    A leg counts as placed if its intent is not closed as ``abandoned`` / ``rejected``
    or it is already live on the exchange (:func:`bytx.protection.find_leg`: attached by the exchange,
    placed before the process crashed). Entry still open — legs for the entry amount, as right after
    place_order; filled — for the filled amount, no more than the position; cancelled unfilled —
    nothing to protect. The entry is marked protected only if all legs were placed (or there is no position)."""
    from bytx.protection import find_leg
    plan = it.tp_sl or {}
    children = store.children(ex, it.client_id)
    missing = {k: plan[k] for k in ('stop_loss', 'take_profit') if plan.get(k) is not None
               and not any(_covers(c, k) for c in children)}
    filled = order.filled_contracts if order.filled_contracts is not None else order.filled
    if not missing or (order.status in _TERMINAL and not filled):
        store._on_protected(ex, it.client_id)
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
    trigger_by = plan.get('trigger_by')
    failed = len(report.failed)
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
    size = filled if order.status in _TERMINAL else it.amount
    for kind in ('stop_loss', 'take_profit'):   # SL first, as in protection.attach_tp_sl
        if kind in missing:
            await _place_leg(ex, store, it, kind, float(missing[kind]), side, size, report, parent=it.client_id,
                             into=report.protected, trigger_by=trigger_by)
    if len(report.failed) == failed:   # a leg failed — the entry is unprotected: the next recover() retries
        store._on_protected(ex, it.client_id)


async def _place_leg(ex: 'Exchange', store: IntentStore, it: Intent, kind: str, trigger: float, side: str,
                     contracts: Optional[float], report: RecoveryReport, *, parent: Optional[str] = None,
                     into: Optional[list] = None, trigger_by: Optional[str] = None) -> Optional['TpSlOrder']:
    """TP/SL leg for the current position (no larger than it); no position — not placed."""
    try:
        pos = await ex.position(it.symbol, side)  # type: ignore[arg-type]
        if pos is None or not pos.contracts:
            if it.kind != 'order':   # leg without a position: close the intent
                store.mark(ex, it.client_id, 'abandoned', note=f'{kind}: no {side} position — protection not needed')
                report.abandoned.append(store.get(ex, it.client_id) or it)
            return None
        size = min(contracts, pos.contracts) if contracts else pos.contracts
        with _bind(ex, it, role='tp' if kind == 'take_profit' else 'sl'), store.parent(parent or it.parent):
            order = await ex.place_tp_sl(it.symbol, kind, trigger, position_side=side, contracts=size,
                                         trigger_by=trigger_by)  # type: ignore[arg-type]
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


# ─── exchange lookup ─────────────────────────────────────────────────────────

class _Cache:
    """Open orders / TP/SL of a symbol — one request per symbol per recovery."""

    def __init__(self, ex: 'Exchange') -> None:
        self.ex = ex
        self.orders: dict[str, list['Order']] = {}
        self.legs: dict[str, list['TpSlOrder']] = {}

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
        return await ex.order(it.symbol, client_id=it.client_id)
    except OrderNotFound:
        pass
    hit = _match(await cache.open_orders(it.symbol), it)
    if hit is not None:
        return hit
    raise _Absent(it.client_id)


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
              'orphans_canceled', 'orphaned')
             if getattr(r, name)]
    if parts:
        log = logger.warning if not r.ok else logger.info
        log(f'{ex.name}: intent recovery — ' + ', '.join(parts))


__all__ = ['RecoveryReport', 'ResubmitPolicy', 'recover']
