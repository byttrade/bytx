"""Position journal collector.

Opt-in: ``create_exchange(..., journal=PositionJournal(...))`` or
``ex.journal = journal``. Without a journal, Exchange and trackers work as before: each
write point costs one ``ex.journal is not None`` check.

What is written (all into the journal of the position the order belongs to):

- ``place_order`` / ``ws_place_order`` / ``place_orders`` / ``close_position`` /
  ``reduce_position`` / ``close_all``: ``intent`` **before sending** (right after the
  ``pre_submit`` hook), then ``placed`` (and ``filled`` if the order filled immediately) or ``rejected``
  (an explicit reject; on an unknown outcome the ``intent`` stays — reconciliation by cid finds it);
- ``set_tp_sl`` / ``place_tp_sl`` / TP/SL of a Binance/HL main order: ``intent`` for each leg
  and ``placed`` with role ``tp``/``sl``; ``modify_tp_sl``: ``replaced`` (``replaces`` — the old id);
  TP/SL without a client_id (Bybit trading-stop, Gate price_orders) — ``tp_sl_set`` before the request: role,
  position side, trigger. A Bybit conditional order without ``orderLinkId`` (stopOrderType TakeProfit /
  StopLoss / Partial…) with the same symbol, side, role and trigger (``price_to_precision``) is ours,
  and moving it to our ``tp_sl_set`` price is not an ``external_amend``;
- ``amend_order`` / ``ws_amend_order``: ``replaced``;
- ``cancel_order`` / ``ws_cancel_order`` / ``cancel_orders`` / ``cancel_all``: ``canceled``
  (or the final status if the order was already closed);
- ``OrderTracker``: ``placed`` / ``partially_filled`` / ``filled`` / ``canceled`` / ``expired`` /
  ``rejected`` on every order update (WS repeats and REST reconciliation are not duplicated — the
  idempotency key);
- ``PositionTracker``: ``position_opened`` / ``increased`` / ``reduced`` / ``closed``.

Order's position: ``journal.bind(position_id=..., role=..., level_id=...)`` (context, e.g. the bot
cycle uuid), otherwise the journal's open position ``(exchange, symbol, side)``, otherwise
a new key ``account:exchange:symbol:side:ts``. Role: ``bind`` > call kind (close, TP/SL) >
role from a next-generation client_id. Origin: ours (bot tag, a recorded
``intent``, a known order id) — ``own``; otherwise the exchange rules per its docs
(:func:`bytx.journal.origin.classify_origin`: liquidation, ADL, exchange UI, TP/SL child
order, platform format ``platform_formats``, foreign cid, order without cid).

An intervention is an ``intervention`` event (kind in ``kind``) plus the ``on_intervention`` callback:
an order of foreign origin in an open journal position (``foreign_order`` / ``liquidation`` /
``adl`` / ``settlement``), our order canceled (``external_cancel``) or amended (``external_amend``)
not through this Exchange (via tracker WS; cancels/amends via the SDK are marked before the request and for
``own_op_grace`` seconds after), a size discrepancy — :meth:`PositionJournal.reconcile`
(``position_drift``). Rebuilding the journal from exchange history — :meth:`PositionJournal.recover`.
"""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import re
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator, Optional, Pattern, Union

from loguru import logger

from bytx.journal.client_id import (
    LEVEL_ROLES,
    ROLE_CODES,
    bot_tag as make_bot_tag,
    make_client_id,
    parse_client_id,
)
from bytx.journal.models import ROLES, JournalEvent, PositionRecord
from bytx.order_params import _ccxt_id, numeric_client_id
from bytx.journal.origin import classify_origin, rules_for
from bytx.journal.store import AppendResult, InMemoryJournalStore, JournalStore

if TYPE_CHECKING:
    from bytx.exchange import Exchange
    from bytx.types import Order, Position

_KIND_ROLES = {'take_profit': 'tp', 'stop_loss': 'sl', 'tp_sl': 'tp'}
_STATUS_EVENTS = {'closed': 'filled', 'canceled': 'canceled', 'expired': 'expired', 'rejected': 'rejected'}
_REL_TOL = 1e-6
_OURS = frozenset({'own', 'bot_legacy'})
# how many of our TP/SL orders to re-read to recognize a fill of one of them in an unknown order
_FILL_LOOKUPS = 4
_SYSTEM_KINDS = {'liquidation': 'liquidation', 'adl': 'adl', 'settlement': 'settlement'}


@dataclass(frozen=True, slots=True)
class Binding:
    """Call binding within a ``journal.bind(...)`` context."""

    position_id: Optional[str] = None
    role: Optional[str] = None
    level_id: Optional[int] = None


_BINDING: contextvars.ContextVar[Optional[Binding]] = contextvars.ContextVar('bytx_journal_binding',
                                                                             default=None)


def current_binding() -> Binding:
    return _BINDING.get() or Binding()


@dataclass(slots=True)
class _OrderRef:
    position_id: str
    role: Optional[str]
    level_id: Optional[int]
    origin: str
    position_side: Optional[str]
    increases: Optional[bool]
    filled: float = 0.0
    price: Optional[float] = None
    amount: Optional[float] = None
    reason: Optional[str] = None
    confidence: Optional[str] = None
    flagged: bool = False      # an intervention for this order is already recorded


def _position_side(side: Optional[str], reduce_only: bool) -> Optional[str]:
    """Order's position side: an opening buy — long; a closing sell — long too."""
    if side not in ('buy', 'sell'):
        return None
    opens_long = (side == 'buy') != bool(reduce_only)
    return 'long' if opens_long else 'short'


class PositionJournal:
    """Position journal on top of a :class:`JournalStore` (in memory by default).

    ``bot_id`` — journal owner (idempotency key and the ``(bot_id, position_id, seq)`` index);
    ``tag`` — the bot tag in client_id (default ``d`` + sha1(bot_id)[:12], :func:`~bytx.journal.client_id.bot_tag`;
    without ``bot_id`` and ``tag`` — the exchange's ``client_id_prefix``). If the Exchange has a
    ``client_id_prefix``, cids are generated with it (case as is — ``owns`` and ``cancel_all`` rely on it),
    and the journal tag must match it (case-insensitively), otherwise
    :class:`~bytx.errors.ConfigError`; ``account`` — part of the position key
    without ``position_id``; ``platform_formats`` — platform cid regexes (class
    ``platform``); ``strict`` — a store failure while writing ``intent`` stops the order from being sent
    (default — WARNING in the log, the order goes out); ``on_intervention(event)`` — callback for a new
    ``intervention`` event (sync or async; a callback error — WARNING, the journal is unaffected);
    ``own_op_grace`` — how many seconds after a cancel/amend via the SDK a WS update is considered
    ours (WS may arrive after the REST response).
    """

    def __init__(self, store: Optional[JournalStore] = None, *, bot_id: Optional[str] = None,
                 tag: Optional[str] = None, account: Optional[str] = None,
                 platform_formats: Iterable[Union[str, Pattern[str]]] = (), strict: bool = False,
                 clock: Optional[Callable[[], int]] = None, max_cached_orders: int = 10_000,
                 on_intervention: Optional[Callable[[JournalEvent], Any]] = None,
                 own_op_grace: float = 30.0) -> None:
        self.store: JournalStore = store if store is not None else InMemoryJournalStore()
        self.bot_id = str(bot_id) if bot_id is not None else None
        self.tag = tag.lower() if tag else (make_bot_tag(bot_id) if bot_id is not None else None)
        self.account = account
        self.platform_formats = tuple(re.compile(p) if isinstance(p, str) else p for p in platform_formats)
        self.strict = bool(strict)
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._max_orders = int(max_cached_orders)
        self._orders: OrderedDict[str, _OrderRef] = OrderedDict()
        self._active: dict[tuple[str, str, str], str] = {}
        self._sizes: dict[str, float] = {}
        self._own_size: dict[str, float] = {}
        self._tasks: set[asyncio.Task] = set()
        self._lock: Optional[asyncio.Lock] = None
        self.errors = 0
        self.on_intervention = on_intervention
        self.own_op_grace = float(own_op_grace)
        self._cb_tasks: set[asyncio.Task] = set()
        # listeners for new journal events (notifications); empty — nothing is done
        self._listeners: list[Callable[[JournalEvent], Any]] = []
        self._ops: dict[tuple[str, str], list[float]] = {}     # (exchange, symbol) → [in flight, end]
        self._op_ids: dict[str, float] = {}                     # order id/cid → operation end
        # position TP/SL without cid placed by us: (exchange, symbol, side) → [(role, trigger, position)]
        self._tp_sl_sets: dict[tuple[str, str, str], list[tuple[str, float, str]]] = {}

    # ─── public API ─────────────────────────────────────────────────────────

    def subscribe(self, callback: Callable[[JournalEvent], Any]) -> Callable[[], None]:
        """Listener for every **new** journal event (a repeat by key is not), including
        ``intervention``. Callback is sync or async; its error — WARNING, the journal is unaffected.
        Returns an unsubscribe function."""
        self._listeners.append(callback)

        def remove() -> None:
            if callback in self._listeners:
                self._listeners.remove(callback)

        return remove

    @contextlib.contextmanager
    def bind(self, *, position_id: Optional[str] = None, role: Optional[str] = None,
             level_id: Optional[int] = None) -> Iterator[Binding]:
        """Context: orders inside belong to ``position_id`` with role ``role`` (level ``level_id``).

        A nested ``bind`` extends the outer one. Role — from :data:`~bytx.journal.models.ROLES`.
        """
        if role is not None and role not in ROLES:
            from bytx.errors import ConfigError
            raise ConfigError(f'journal role: one of {sorted(ROLES)}, not {role!r}')
        outer = current_binding()
        b = Binding(position_id if position_id is not None else outer.position_id,
                    role if role is not None else outer.role,
                    level_id if level_id is not None else outer.level_id)
        token = _BINDING.set(b)
        try:
            yield b
        finally:
            _BINDING.reset(token)

    def client_id(self, exchange: Any, role: Optional[str] = None, *, level_id: Optional[int] = None,
                  market_type: str = 'swap', tag: Optional[str] = None) -> Optional[str]:
        """Next-generation client_id for ``role`` (or the role from ``bind``); None — the role is not encoded
        (no role, a role without a code, TP/SL without a level) or there is no tag."""
        b = current_binding()
        role = role or b.role
        level = level_id if level_id is not None else b.level_id
        t = self.tag_for(tag)
        if not t or role not in ROLE_CODES or (role in LEVEL_ROLES and level is None):
            return None
        if numeric_client_id(_ccxt_id(exchange), market_type):
            return None     # HTX USDT-M: client_order_id is an integer, role — only in the journal
        return make_client_id(exchange, t, role, level_id=level if role in LEVEL_ROLES else None,
                              market_type=market_type)

    def tag_for(self, prefix: Optional[str]) -> Optional[str]:
        """Cid tag for an Exchange with ``client_id_prefix`` = ``prefix``: the prefix itself (case as is —
        ``owns`` / ``cancel_all`` rely on it), otherwise the journal tag. A journal tag that is set and differs from
        the prefix (ignoring case and the HL ``0x``) — :class:`~bytx.errors.ConfigError`."""
        if not prefix:
            return self.tag
        value = str(prefix)
        body = value[2:] if value[:2].lower() == '0x' else value
        if self.tag and body.lower() != self.tag:
            from bytx.errors import ConfigError
            raise ConfigError(f'journal tag {self.tag!r} does not match the Exchange client_id_prefix {value!r}: '
                              'cids would get a foreign tag, and owns() / cancel_all() by prefix would not find them. '
                              'Set client_id_prefix = the journal tag (bot_tag(bot_id)) or '
                              'PositionJournal(tag=client_id_prefix)',
                              details={'journal_tag': self.tag, 'client_id_prefix': value})
        return value

    def classify(self, client_id: Optional[str], exchange_order_id: Optional[str] = None) -> str:
        """Order origin by client_id and known orders, without requests."""
        ref = self._cached(exchange_order_id, client_id)
        if ref is not None:
            return ref.origin
        if not client_id:
            return 'unknown'
        info = parse_client_id(client_id, tag=self.tag if self.tag and not _is_ts_tag(self.tag) else None)
        if self.tag and info.bot_tag == self.tag:
            return 'own'
        cid = str(client_id)
        if any(p.match(cid) for p in self.platform_formats):
            return 'platform'
        return 'foreign_api'

    async def record(self, event: JournalEvent) -> Optional[AppendResult]:
        """Writes an event (``bot_id`` and ``ts`` are filled in if not set)."""
        async with self._guard():
            return await self._append(event)

    async def read(self, position_id: str, *, since_seq: int = 0) -> list[JournalEvent]:
        """Position events in ascending ``seq``."""
        await self.flush()
        return await self.store.read(position_id, bot_id=self.bot_id, since_seq=since_seq)

    async def summary(self, position_id: str) -> PositionRecord:
        """Position summary from events (:class:`PositionRecord`)."""
        return PositionRecord.from_events(await self.read(position_id))

    async def record_funding(self, position_id: str, amount: float, *, ts: Optional[int] = None,
                             symbol: Optional[str] = None, trade_id: Optional[str] = None,
                             raw: Optional[dict] = None) -> Optional[AppendResult]:
        """Funding for the position (sign as on the exchange: plus — received)."""
        return await self.record(JournalEvent(position_id, 'funding', 'funding', ts=ts or self._clock(),
                                              bot_id=self.bot_id, symbol=symbol, amount=amount,
                                              trade_id=trade_id, raw=dict(raw or {})))

    async def record_liquidation(self, position_id: str, *, ts: Optional[int] = None, adl: bool = False,
                                 symbol: Optional[str] = None, price: Optional[float] = None,
                                 amount: Optional[float] = None, trade_id: Optional[str] = None,
                                 raw: Optional[dict] = None) -> Optional[AppendResult]:
        """Liquidation (or ADL) of the position — by the exchange marker on the caller's side."""
        kind = 'adl' if adl else 'liquidated'
        return await self.record(JournalEvent(position_id, kind, kind if adl else 'liquidation',
                                              ts=ts or self._clock(), bot_id=self.bot_id, symbol=symbol,
                                              role='liquidation', price=price, amount=amount,
                                              contracts_after=0.0, trade_id=trade_id, raw=dict(raw or {})))

    def position_for(self, exchange: str, symbol: str, side: str) -> Optional[str]:
        """The journal's open position ``(exchange, symbol, side)``."""
        return self._active.get((exchange, symbol, side))

    async def flush(self) -> None:
        """Wait for tracker events to be written (they are written in the background) and for async intervention callbacks."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        me = asyncio.current_task()
        pending = [t for t in self._cb_tasks if t is not me]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def reconcile(self, position_id: str, exchange_size: float, *, ts: Optional[int] = None,
                        symbol: Optional[str] = None) -> Optional[JournalEvent]:
        """Reconcile the exchange position size with the sum of our fills.

        ``exchange_size`` — exchange position size in base coin (like ``Position.size``).
        Our size — the sum of fills of the journal's ``own`` / ``bot_legacy`` orders (opening plus,
        closing minus), :func:`own_size`. A discrepancy — ``drift`` and ``intervention`` events
        (``kind='position_drift'``, ``amount`` — absolute difference) and the callback; the same discrepancy
        is not written twice. Match — None.
        """
        events = await self.read(position_id)
        expected = own_size(events)
        size = abs(float(exchange_size))
        if abs(expected - size) <= _REL_TOL * max(abs(expected), size, 1e-12):
            return None
        first = events[0] if events else None
        async with self._guard():
            stamp = ts or self._clock()
            common: dict[str, Any] = dict(
                bot_id=self.bot_id, exchange=first.exchange if first else None,
                symbol=symbol or (first.symbol if first else None),
                position_side=first.position_side if first else None, amount=abs(size - expected),
                contracts_after=size, kind='position_drift', confidence='heuristic',
                note=f'exchange size {size} ≠ sum of our fills {expected}')
            base = f'{self.bot_id or ""}|pos:{position_id}'
            state = f'position_drift:own={expected}:after={size}'
            drift = JournalEvent(position_id, 'drift', 'unknown', ts=stamp, key=f'{base}|drift|{state}', **common)
            await self._append(drift)
            inter = JournalEvent(position_id, 'intervention', 'unknown', ts=stamp,
                                 key=f'{base}|intervention|{state}', **common)
            res = await self._append(inter)
            if res is not None and res.created:
                self._notify(res.event)
            return res.event if res is not None else None

    async def recover(self, ex: Any, symbol: str, side: str, *, since: int, until: Optional[int] = None,
                      position_id: Optional[str] = None, source: Any = None, funding: bool = True,
                      exchange_size: Optional[float] = None) -> Any:
        """Rebuild the position journal from exchange history: :func:`bytx.journal.recovery.recover_position`."""
        from bytx.journal.recovery import recover_position
        return await recover_position(self, ex, symbol, side, since=since, until=until, position_id=position_id,
                                      source=source, funding=funding, exchange_size=exchange_size)

    # ─── operations via the SDK (not an intervention) ───────────────────────

    @contextlib.contextmanager
    def own_op(self, ex: Any, symbol: Optional[str], ids: Iterable[Optional[str]] = ()) -> Iterator[None]:
        """A cancel/amend through this Exchange: WS updates for ``ids`` (or for the symbol if
        the list is unknown — mass cancel) during the operation and ``own_op_grace`` seconds after are ours."""
        keys = [str(i) for i in ids if i]
        slot = None
        if not keys:   # ids unknown (mass cancel): mark the whole symbol
            slot = self._ops.setdefault((getattr(ex, 'name', str(ex)), symbol or '*'), [0, -1e18])
            slot[0] += 1
        for i in keys:
            self._op_ids[i] = float('inf')   # in flight
        try:
            yield
        finally:
            end = time.monotonic()
            if slot is not None:
                slot[0] -= 1
                slot[1] = end
            for i in keys:
                self._op_ids[i] = end
            if len(self._op_ids) > self._max_orders:
                cutoff = end - self.own_op_grace
                for i in [k for k, v in self._op_ids.items() if v < cutoff]:
                    self._op_ids.pop(i, None)

    def _is_own_op(self, ex: Any, order: Any) -> bool:
        now = time.monotonic()
        for i in (order.id, order.client_id):
            mark = self._op_ids.get(str(i)) if i else None
            if mark is not None and (mark == float('inf') or now - mark <= self.own_op_grace):
                return True
        name = getattr(ex, 'name', str(ex))
        for key in ((name, order.symbol), (name, '*')):
            slot = self._ops.get(key)
            if slot is not None and (slot[0] > 0 or now - slot[1] <= self.own_op_grace):
                return True
        return False

    # ─── Exchange write points ──────────────────────────────────────────────

    async def _on_intent(self, ex: 'Exchange', request: dict, kind: str, extra: dict) -> None:
        cid = request.get('client_id')
        if not cid:
            # without a cid the order cannot be linked to an intent (Bybit trading-stop) — placed is written; position
            # TP/SL — tp_sl_set: it lets the exchange's conditional order without cid be recognized as ours
            if kind in ('take_profit', 'stop_loss') and extra.get('trigger_price') is not None:
                await self._on_tp_sl_set(ex, request, kind, extra)
            return
        async with self._guard():
            b = current_binding()
            info = parse_client_id(cid, tag=self._parse_tag(ex))
            role = (b.role or request.get('journal_role') or _KIND_ROLES.get(kind)
                    or (info.role if info.generation in ('v2', 'legacy_algo') else None))
            level = b.level_id if b.level_id is not None else info.level_id
            reduce_only = bool(request.get('reduce_only'))
            pos_side = _position_side(request.get('side'), reduce_only)
            symbol = request.get('symbol')
            ts = self._clock()
            pid = self._position_id(ex, symbol, pos_side, ts)
            ref = _OrderRef(pid, role, level, 'own', pos_side, not reduce_only)
            self._remember(None, cid, ref)
            price = request.get('price') if request.get('price') is not None else extra.get('trigger_price')
            await self._append(JournalEvent(
                pid, 'intent', 'own', ts=ts, bot_id=self.bot_id, role=role, level_id=level, exchange=ex.name,
                symbol=symbol, position_side=pos_side, client_order_id=cid, side=request.get('side'),
                price=price, amount=_base(ex, symbol, request.get('amount')), note=kind if kind != 'order' else None),
                strict=True)
            attach = request.get('attach_client_id')
            if attach:
                # attached OKX TP/SL (attachAlgoClOrdId): its child order will come with algoClOrdId
                a_role = request.get('attach_role')
                self._remember(None, attach, _OrderRef(pid, a_role, 0, 'own', pos_side, False))
                close_side = {'buy': 'sell', 'sell': 'buy'}.get(str(request.get('side')))
                await self._append(JournalEvent(
                    pid, 'intent', 'own', ts=ts, bot_id=self.bot_id, role=a_role, level_id=0, exchange=ex.name,
                    symbol=symbol, position_side=pos_side, client_order_id=attach, side=close_side,
                    price=request.get('attach_trigger'), amount=_base(ex, symbol, request.get('amount')),
                    note='attached_tp_sl'), strict=True)

    async def _on_tp_sl_set(self, ex: 'Exchange', request: dict, kind: str, extra: dict) -> None:
        async with self._guard():
            b = current_binding()
            role = _KIND_ROLES[kind]
            pos_side = _position_side(request.get('side'), True)
            symbol = request.get('symbol')
            ts = self._clock()
            pid = self._position_id(ex, symbol, pos_side, ts)
            trigger = float(extra['trigger_price'])
            self._remember_tp_sl_set(ex.name, symbol, pos_side, role, trigger, pid)
            params = request.get('params') or {}
            await self._append(JournalEvent(
                pid, 'tp_sl_set', 'own', ts=ts, bot_id=self.bot_id, role=b.role if b.role in ('tp', 'sl') else role,
                level_id=b.level_id, exchange=ex.name, symbol=symbol, position_side=pos_side,
                side=request.get('side'), price=trigger, trigger_price=trigger,
                amount=_base(ex, symbol, request.get('amount')), reduce_only=True,
                note=f"{kind}: position TP/SL without client_id (trigger_by={extra.get('trigger_by')}, "
                     f"tpslMode={params.get('tpslMode')})",
                raw={'params': {k: v for k, v in params.items() if k != 'symbol'}}), strict=True)

    def _remember_tp_sl_set(self, exchange: str, symbol: Optional[str], pos_side: Optional[str], role: str,
                            trigger: float, pid: str) -> None:
        sets = self._tp_sl_sets.setdefault((exchange, symbol or '', pos_side or ''), [])
        sets.append((role, trigger, pid))
        del sets[:-32]      # latest placements: the exchange has already replaced older triggers

    async def _tp_sl_set_match(self, ex: Any, symbol: Optional[str], pos_side: Optional[str], role: str,
                               trigger: Optional[float]) -> Optional[str]:
        """Position of our ``tp_sl_set`` with the same role and trigger (``price_to_precision``); None — none."""
        if trigger is None:
            return None
        key = (getattr(ex, 'name', str(ex)), symbol or '', pos_side or '')
        cands = list(self._tp_sl_sets.get(key, ()))
        pid = self._active.get(key)
        if not cands and pid is not None:    # after a restart — from the open position's store
            try:
                evs = await self.store.read(pid, bot_id=self.bot_id)
            except Exception:
                evs = []
            cands = [(e.role or '', float(e.trigger_price), pid) for e in evs
                     if e.event == 'tp_sl_set' and e.trigger_price is not None
                     and e.symbol == symbol and e.position_side == pos_side]
        for c_role, c_trigger, c_pid in reversed(cands):
            if c_role == role and _same_trigger(ex, symbol, c_trigger, trigger):
                return c_pid
        return None

    async def _own_position_tp_sl(self, ex: Any, order: 'Order') -> Optional[_OrderRef]:
        """A position TP/SL conditional order without cid (Bybit trading-stop) is ours if it matches a ``tp_sl_set``."""
        if order.client_id:
            return None
        try:
            rules = rules_for(ex)
        except Exception:
            return None
        role_of = getattr(rules, 'position_tp_sl_role', None)
        info = _info_of(order)
        role = role_of(info) if role_of is not None else None
        if role is None:
            return None
        pos_side = rules.fill_position_side(info) or _position_side(order.side, True)
        trigger = order.trigger_price
        if trigger is None and hasattr(rules, 'trigger_price_of'):
            trigger = rules.trigger_price_of(info)
        pid = await self._tp_sl_set_match(ex, order.symbol, pos_side, role, trigger)
        if pid is None:
            return None
        ref = _OrderRef(pid, role, None, 'own', pos_side, False)
        ref.reason, ref.confidence = 'position TP/SL without client_id: matches our tp_sl_set', 'heuristic'
        return ref

    async def _unknown_ref(self, ex: Any, order: 'Order') -> Optional[_OrderRef]:
        """An order not in the journal: our position TP/SL without cid, a fill order of our TP/SL,
        otherwise a foreign order in the position."""
        ref = await self._own_position_tp_sl(ex, order)
        if ref is None:
            ref = await self._own_tp_sl_fill(ex, order)
        return ref if ref is not None else self._foreign_ref(ex, order)

    async def _own_tp_sl_fill(self, ex: Any, order: 'Order') -> Optional[_OrderRef]:
        """Fill order of our triggered TP/SL: the exchange fills TP/SL with a DIFFERENT order, the link exists only
        in the TP/SL record (``TpSlOrder.fill_order_ids``: Binance actualOrderId, OKX ordIdList, Gate trade_id, MEXC
        placeOrderId, HTX relation_order_id). Our TP/SL orders on the same symbol are re-read (at most
        ``_FILL_LOOKUPS``)."""
        from bytx.exchanges import tp_sl_fills_by_other_order
        if not order.id or not tp_sl_fills_by_other_order(getattr(getattr(ex, '_ex', None), 'id', None) or ''):
            return None
        seen: set[str] = set()
        cands: list[tuple[str, _OrderRef]] = []
        for key, ref in reversed(self._orders.items()):
            oid = key[2:]
            if key.startswith('o:') and ref.role in ('tp', 'sl') and ref.origin in _OURS and oid not in seen \
                    and self._active.get((ex.name, order.symbol, ref.position_side or '')) == ref.position_id:
                seen.add(oid)
                cands.append((oid, ref))
        for oid, ref in cands[:_FILL_LOOKUPS]:
            try:
                tp_sl = await ex.tp_sl_order(order.symbol, oid)
            except Exception as e:
                logger.debug(f'position journal: TP/SL {oid} not read: {type(e).__name__}: {e}')
                continue
            fills = getattr(tp_sl, 'fill_order_ids', ()) or (getattr(tp_sl, 'fill_order_id', None),)
            if str(order.id) in fills:              # OKX ordIdList: a TP/SL fill may be several orders
                found = replace(ref, filled=0.0, price=None, amount=None, flagged=False)
                found.reason, found.confidence = f'fill of our TP/SL {oid} (fill_order_id)', 'exchange'
                return found
        return None

    async def _on_placed(self, ex: 'Exchange', order: 'Order', request: Optional[dict] = None) -> None:
        async with self._guard():
            ref = await self._resolve(order.id, order.client_id)
            if ref is None:
                req = request or {}
                b = current_binding()
                reduce_only = bool(req.get('reduce_only', order.reduce_only))
                pos_side = _position_side(order.side, reduce_only)
                ref = _OrderRef(self._position_id(ex, order.symbol, pos_side, self._clock()),
                                b.role or req.get('journal_role'), b.level_id, 'own', pos_side, not reduce_only)
            self._remember(order.id, order.client_id, ref)
            _baseline(ref, order)
            await self._append(self._order_event(ex, 'placed', order, ref, ts=order.timestamp))
            await self._progress(ex, order, ref, skip_open=True)

    async def _on_failed(self, ex: 'Exchange', request: dict, error: BaseException) -> None:
        from bytx.errors import OrderStatusUnknown
        if isinstance(error, OrderStatusUnknown):
            return  # outcome unknown: the intent stays, reconciliation by cid finds the order
        async with self._guard():
            for cid in (request.get('client_id'), request.get('attach_client_id')):
                ref = await self._resolve(None, cid) if cid else None
                if ref is None:
                    continue   # attached OKX TP/SL rejected together with the order
                await self._append(JournalEvent(
                    ref.position_id, 'rejected', ref.origin, ts=self._clock(), bot_id=self.bot_id, role=ref.role,
                    level_id=ref.level_id, exchange=ex.name, symbol=request.get('symbol'),
                    position_side=ref.position_side, client_order_id=cid, side=request.get('side'),
                    note=f'{type(error).__name__}: {error}'[:300]))

    async def _on_batch(self, ex: 'Exchange', requests: list[dict], results: list) -> None:
        from bytx.types import Order
        for request, result in zip(requests, results):
            if isinstance(result, Order):
                await self._on_placed(ex, result, request)
            elif isinstance(result, BaseException):
                await self._on_failed(ex, request, result)

    async def _on_tp_sl(self, ex: 'Exchange', orders: Iterable[Any]) -> None:
        async with self._guard():
            for o in orders:
                ref = await self._resolve(o.id, o.client_id)
                if ref is None:
                    b = current_binding()
                    pos_side = getattr(o, 'position_side', None)
                    ref = _OrderRef(self._position_id(ex, o.symbol, pos_side, self._clock()),
                                    b.role or _KIND_ROLES.get(getattr(o, 'kind', '')), b.level_id, 'own',
                                    pos_side, False)
                self._remember(o.id, o.client_id, ref)
                note = 'tp_sl' if getattr(o, 'kind', None) == 'tp_sl' else None
                await self._append(self._order_event(ex, 'placed', o, ref, ts=o.timestamp, note=note,
                                                     price=getattr(o, 'trigger_price', None) or o.price))

    async def _on_replaced(self, ex: 'Exchange', old_id: str, order: 'Order', *,
                           old_canceled: bool = False) -> None:
        async with self._guard():
            old = await self._resolve(str(old_id), None)
            ref = await self._resolve(order.id, order.client_id) if order.id != str(old_id) else None
            if ref is None and old is not None:
                ref = replace(old, filled=0.0, price=None, amount=None)
            elif ref is None:
                pos_side = _position_side(order.side, bool(order.reduce_only))
                ref = _OrderRef(self._position_id(ex, order.symbol, pos_side, self._clock()), None, None, 'own',
                                pos_side, not order.reduce_only)
            self._remember(order.id, order.client_id, ref)
            _baseline(ref, order)
            if old_canceled and old is not None and order.id != str(old_id):
                await self._append(JournalEvent(
                    old.position_id, 'canceled', old.origin, ts=self._clock(), bot_id=self.bot_id, role=old.role,
                    level_id=old.level_id, exchange=ex.name, symbol=order.symbol, position_side=old.position_side,
                    exchange_order_id=str(old_id), note='replaced'))
            await self._append(self._order_event(ex, 'replaced', order, ref, ts=self._clock(), replaces=str(old_id),
                                                 price=getattr(order, 'trigger_price', None) or order.price))

    async def _on_canceled(self, ex: 'Exchange', order: 'Order', ours: bool) -> None:
        async with self._guard():
            ref = await self._resolve(order.id, order.client_id)
            if ref is None:
                ref = await self._unknown_ref(ex, order)
                if ref is None:
                    return
                self._remember(order.id, order.client_id, ref)
            if ours:
                await self._append(self._order_event(ex, 'canceled', order, ref, ts=order.timestamp,
                                                     note='own_cancel'))
            else:
                await self._progress(ex, order, ref, skip_open=True)

    # ─── tracker write points (sync → ordered background write) ──────────────

    def _on_order_update(self, ex: 'Exchange', order: 'Order') -> None:
        self._spawn(self._order_update(ex, order))

    def _on_position_update(self, ex: 'Exchange', pos: 'Position') -> None:
        self._spawn(self._position_update(ex, pos))

    async def _order_update(self, ex: 'Exchange', order: 'Order') -> None:
        async with self._guard():
            ref = await self._resolve(order.id, order.client_id)
            if ref is None:
                ref = await self._unknown_ref(ex, order)
                if ref is None:
                    return  # order outside journal positions
                self._remember(order.id, order.client_id, ref)
            await self._progress(ex, order, ref, skip_open=False)
            await self._check_intervention(ex, order, ref)

    # ─── intervention ───────────────────────────────────────────────────────

    async def _check_intervention(self, ex: Any, order: 'Order', ref: _OrderRef) -> None:
        info = _info_of(order)
        if ref.origin not in _OURS:
            if not ref.flagged:
                kind = _SYSTEM_KINDS.get(ref.origin, 'foreign_order')
                ref.flagged = await self._intervene(ex, kind, ref.origin, ref, order, ref.reason, ref.confidence)
            return
        try:
            rules = rules_for(ex)
        except Exception:
            rules = None
        if order.status == 'canceled':
            if self._is_own_op(ex, order) or await self._own_cancel_recorded(order):
                return
            sig = rules.cancel_signal(info) if rules is not None else None
            if sig is not None and sig.expected:
                return
            if sig is not None and sig.origin:
                await self._intervene(ex, 'external_cancel', sig.origin, ref, order, sig.reason, sig.confidence)
            else:
                reason = (sig.reason + ': ' if sig is not None else '') + 'canceled not through this Exchange'
                await self._intervene(ex, 'external_cancel', 'unknown', ref, order, reason, 'heuristic')
            return
        if order.status != 'open':
            return
        price = order.price if order.trigger_price is None else order.trigger_price
        moved = (ref.price is not None and price is not None
                 and abs(price - ref.price) > _REL_TOL * max(abs(price), abs(ref.price)))
        resized = (ref.amount is not None and order.size is not None
                   and abs(order.size - ref.amount) > _REL_TOL * max(abs(order.size), abs(ref.amount)))
        amended = rules is not None and hasattr(rules, 'is_amendment') and rules.is_amendment(info)
        if moved and not resized and ref.role in ('tp', 'sl') and not order.client_id \
                and await self._own_position_tp_sl(ex, order) is not None:
            moved = False   # our repeated trading-stop (Full) moved the trigger in place
        if (moved or resized or (amended and (ref.price is not None or ref.amount is not None))) \
                and not self._is_own_op(ex, order):
            what = ', '.join(x for x, on in (('price', moved), ('size', resized), ('AMENDMENT', amended)) if on)
            await self._intervene(ex, 'external_amend', 'unknown', ref, order,
                                  f'order amended not through this Exchange ({what})', 'heuristic',
                                  key_extra=f':px={price}:sz={order.size}')
        _baseline(ref, order)

    async def _own_cancel_recorded(self, order: 'Order') -> bool:
        try:
            found = await self.store.find_order(bot_id=self.bot_id, exchange_order_id=order.id or None,
                                                client_order_id=None if order.id else order.client_id)
        except Exception:
            return False
        return any(e.event == 'canceled' and e.note in ('own_cancel', 'replaced') for e in found)

    async def _intervene(self, ex: Any, kind: str, origin: str, ref: _OrderRef, order: Any,
                         reason: Optional[str], confidence: Optional[str], *, key_extra: str = '') -> bool:
        event = JournalEvent(
            ref.position_id, 'intervention', origin, ts=getattr(order, 'timestamp', None) or self._clock(),
            bot_id=self.bot_id, role=ref.role, level_id=ref.level_id, exchange=getattr(ex, 'name', None),
            symbol=order.symbol, position_side=ref.position_side, exchange_order_id=order.id or None,
            client_order_id=order.client_id, side=order.side,
            price=order.trigger_price if order.trigger_price is not None else (order.average or order.price),
            amount=order.size, filled=order.filled, kind=kind, confidence=confidence, note=reason,
            order_type=order.type or None)
        if key_extra:
            event.key = event.default_key() + key_extra
        res = await self._append(event)
        if res is not None and res.created:
            self._notify(res.event)
        return res is not None

    def _notify(self, event: JournalEvent) -> None:
        cb = self.on_intervention
        if cb is None:
            return
        self._call_listener(cb, event, 'on_intervention')

    def _call_listener(self, cb: Callable[[JournalEvent], Any], event: JournalEvent, what: str) -> None:
        try:
            out = cb(event)
        except Exception as e:
            logger.warning(f'position journal: {what} callback failed: {type(e).__name__}: {e}')
            return
        if asyncio.iscoroutine(out):
            try:
                task = asyncio.get_running_loop().create_task(out)
            except RuntimeError:
                out.close()
                return
            self._cb_tasks.add(task)

            def done(t: asyncio.Task) -> None:
                self._cb_tasks.discard(t)
                if not t.cancelled() and t.exception() is not None:
                    logger.warning(f'position journal: {what} callback failed: {t.exception()}')

            task.add_done_callback(done)

    async def _position_update(self, ex: 'Exchange', pos: 'Position') -> None:
        async with self._guard():
            size = abs(float(pos.size or 0.0))
            keys = ([(ex.name, pos.symbol, pos.side)] if pos.side
                    else [k for k in self._active if k[0] == ex.name and k[1] == pos.symbol])
            if size == 0:
                for key in keys:
                    pid = self._active.get(key)
                    if pid is None:
                        continue
                    prev = self._sizes.get(pid, 0.0)
                    if prev == 0:
                        self._drop(key, pid)
                        continue
                    await self._append(self._position_event(ex, 'closed', pid, key[2], pos, 0.0, prev))
                    self._drop(key, pid)
                return
            if not pos.side:
                return
            key = (ex.name, pos.symbol, pos.side)
            pid = self._position_id(ex, pos.symbol, pos.side, pos.timestamp or self._clock())
            prev = self._sizes.get(pid, 0.0)
            if abs(size - prev) <= _REL_TOL * max(size, prev):
                return
            event = 'position_opened' if prev == 0 else 'increased' if size > prev else 'reduced'
            self._sizes[pid] = size
            await self._append(self._position_event(ex, event, pid, key[2], pos, size, prev))

    # ─── internals ──────────────────────────────────────────────────────────

    def _spawn(self, coro: Any) -> None:
        try:
            task = asyncio.get_running_loop().create_task(coro)
        except RuntimeError:  # no loop — writing is impossible
            coro.close()
            return
        self._tasks.add(task)

        def done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                self.errors += 1
                logger.warning(f'position journal: tracker event not written: {t.exception()}')

        task.add_done_callback(done)

    @contextlib.asynccontextmanager
    async def _guard(self) -> Any:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            yield

    async def _append(self, event: JournalEvent, *, strict: bool = False) -> Optional[AppendResult]:
        auto_key = event.key == event.default_key()
        if event.bot_id is None and self.bot_id is not None:
            event.bot_id = self.bot_id
        if not event.ts:
            event.ts = self._clock()
        if auto_key:
            event.key = event.default_key()
        try:
            res = await self.store.append(event)
        except Exception as e:
            self.errors += 1
            if strict and self.strict:
                raise
            logger.warning(f'position journal: event {event.event} ({event.client_order_id or event.exchange_order_id})'
                           f' not written: {e}')
            return None
        if self._listeners and res is not None and res.created:
            self._dispatch(res.event)
        return res

    def _dispatch(self, event: JournalEvent) -> None:
        for cb in list(self._listeners):
            self._call_listener(cb, event, 'subscribe')

    def _parse_tag(self, ex: Any) -> Optional[str]:
        tag = self.tag or getattr(ex, 'client_id_prefix', None)
        return tag if tag and not _is_ts_tag(tag) else None

    def _cached(self, oid: Optional[str], cid: Optional[str]) -> Optional[_OrderRef]:
        if oid and f'o:{oid}' in self._orders:
            return self._orders[f'o:{oid}']
        if cid and f'c:{cid}' in self._orders:
            return self._orders[f'c:{cid}']
        return None

    def _remember(self, oid: Optional[str], cid: Optional[str], ref: _OrderRef) -> None:
        for key in ((f'o:{oid}' if oid else None), (f'c:{cid}' if cid else None)):
            if key:
                self._orders[key] = ref
                self._orders.move_to_end(key)
        while len(self._orders) > self._max_orders:
            self._orders.popitem(last=False)

    async def _resolve(self, oid: Optional[str], cid: Optional[str]) -> Optional[_OrderRef]:
        """Order → position: process cache, then the store (after a restart)."""
        ref = self._cached(oid, cid)
        if ref is not None:
            return ref
        if not oid and not cid:
            return None
        try:
            found = await self.store.find_order(bot_id=self.bot_id, exchange_order_id=oid, client_order_id=cid)
        except Exception as e:
            logger.warning(f'position journal: order lookup {oid or cid} failed: {e}')
            return None
        if not found:
            return None
        first = found[0]
        role = next((e.role for e in found if e.role), None)
        level = next((e.level_id for e in found if e.level_id is not None), None)
        filled = max((e.filled or 0.0 for e in found), default=0.0)
        increases = None
        if first.side and first.position_side:
            increases = _position_side(first.side, False) == first.position_side
        ref = _OrderRef(first.position_id, role, level, first.origin, first.position_side, increases, filled)
        self._remember(oid or first.exchange_order_id, cid or first.client_order_id, ref)
        return ref

    def _position_id(self, ex: Any, symbol: Optional[str], pos_side: Optional[str], ts: int) -> str:
        b = current_binding()
        key = (ex.name, symbol or '', pos_side or '')
        if b.position_id:
            self._active[key] = b.position_id
            return b.position_id
        pid = self._active.get(key)
        if pid is None:
            pid = f'{self.account or "-"}:{ex.name}:{symbol}:{pos_side}:{ts}'
            self._active[key] = pid
        return pid

    def _drop(self, key: tuple[str, str, str], pid: str) -> None:
        self._active.pop(key, None)
        self._sizes.pop(pid, None)
        self._own_size.pop(pid, None)

    def _foreign_ref(self, ex: Any, order: 'Order') -> Optional[_OrderRef]:
        """An order the journal did not place: attached to the symbol's open position, if any."""
        pos_side = _position_side(order.side, bool(order.reduce_only))
        pid = self._active.get((ex.name, order.symbol, pos_side or ''))
        if pid is None:
            # one-way: a closing order without reduce_only — to the symbol's only position
            same = [v for k, v in self._active.items() if k[0] == ex.name and k[1] == order.symbol]
            if len(set(same)) != 1:
                return None
            pid = same[0]
            pos_side = next(k[2] for k, v in self._active.items() if v == pid)
        verdict = self.classify_order(ex, order)
        role = verdict.role if verdict.is_ours else None
        ref = _OrderRef(pid, role, verdict.level_id if role else None, verdict.origin, pos_side,
                        _position_side(order.side, False) == pos_side)
        ref.reason, ref.confidence = verdict.reason, verdict.confidence
        return ref

    def classify_order(self, ex: Any, order: Any, *, kind: str = 'order') -> Any:
        """Order / fill origin by exchange rules (:func:`~bytx.journal.origin.classify_origin`)
        with the bot tag, platform formats, broker prefix and orders known to the journal."""
        ref = self._cached(getattr(order, 'id', None) if kind == 'order' else getattr(order, 'order_id', None),
                           getattr(order, 'client_id', None))
        return classify_origin(
            ex, client_id=getattr(order, 'client_id', None), info=_info_of(order), kind=kind,
            tag=self._parse_tag(ex) or self.tag, platform_formats=self.platform_formats,
            broker_prefixes=_broker_prefixes(ex), known=ref.origin if ref is not None else None,
            resolve_parent=self._parent_origin)

    def _parent_origin(self, parent_id: str) -> Optional[str]:
        ref = self._cached(parent_id, parent_id)
        return ref.origin if ref is not None else None

    async def _progress(self, ex: Any, order: 'Order', ref: _OrderRef, *, skip_open: bool) -> None:
        """Order update → fill/final event; own position size — for the position origin."""
        filled = float(order.filled or 0.0)
        status = order.status
        if status in _STATUS_EVENTS:
            event = _STATUS_EVENTS[status]
        elif status == 'open' and filled > 0:
            event = 'partially_filled'
        elif status == 'open':
            if skip_open:
                return
            event = 'placed'
        else:
            return
        delta = filled - ref.filled
        if delta > 0:
            ref.filled = filled
            if ref.origin in _OURS and ref.increases is not None:
                own = self._own_size.get(ref.position_id, 0.0)
                self._own_size[ref.position_id] = own + (delta if ref.increases else -delta)
        await self._append(self._order_event(ex, event, order, ref, ts=order.timestamp,
                                             price=order.average or order.price))

    def _order_event(self, ex: Any, event: str, order: Any, ref: _OrderRef, *, ts: Optional[int],
                     note: Optional[str] = None, price: Optional[float] = None,
                     replaces: Optional[str] = None) -> JournalEvent:
        return JournalEvent(
            ref.position_id, event, ref.origin, ts=ts or self._clock(), bot_id=self.bot_id, role=ref.role,
            level_id=ref.level_id, exchange=ex.name, symbol=order.symbol, position_side=ref.position_side,
            exchange_order_id=order.id or None, client_order_id=order.client_id, side=order.side,
            price=price if price is not None else order.price, amount=order.size,
            filled=order.filled if event in ('filled', 'partially_filled', 'canceled', 'expired') else None,
            fee=order.fee if event == 'filled' else None, fee_currency=order.fee_currency if event == 'filled' else None,
            replaces=replaces, note=note, order_type=getattr(order, 'type', None) or None,
            trigger_price=getattr(order, 'trigger_price', None), reduce_only=getattr(order, 'reduce_only', None))

    def _position_event(self, ex: Any, event: str, pid: str, side: str, pos: 'Position', size: float,
                        prev: float) -> JournalEvent:
        expected = self._own_size.get(pid)
        origin = 'unknown'
        if expected is not None and abs(expected - size) <= _REL_TOL * max(abs(expected), size, 1e-12):
            origin = 'own'
        return JournalEvent(pid, event, origin, ts=pos.timestamp or self._clock(), bot_id=self.bot_id,
                            exchange=ex.name, symbol=pos.symbol, position_side=side or None,
                            price=pos.entry_price, amount=abs(size - prev), contracts_after=size,
                            confidence='exact' if origin == 'own' else None)


def _baseline(ref: _OrderRef, order: Any) -> None:
    """Last known price (trigger for conditional orders) and order size — the baseline for external_amend."""
    trig = getattr(order, 'trigger_price', None)
    price = trig if trig is not None else getattr(order, 'price', None)
    if price is not None:
        ref.price = float(price)
    if getattr(order, 'size', None) is not None:
        ref.amount = float(order.size)


def _same_trigger(ex: Any, symbol: Optional[str], a: float, b: float) -> bool:
    """Triggers are equal within the exchange price step (``price_to_precision``, as the SDK rounds)."""
    if abs(a - b) <= _REL_TOL * max(abs(a), abs(b)):
        return True
    try:
        c = getattr(ex, 'ccxt', ex)
        return float(c.price_to_precision(symbol, a)) == float(c.price_to_precision(symbol, b))
    except Exception:   # market not loaded — exact match only
        return False


def _info_of(obj: Any) -> dict:
    raw = getattr(obj, 'raw', None)
    if isinstance(raw, dict):
        info = raw.get('info')
        if isinstance(info, dict):
            return info
    return {}


def _broker_prefixes(ex: Any) -> tuple[str, ...]:
    """The client's Binance broker prefixes ``x-<ID>`` (for physical cids from history)."""
    try:
        from bytx.order_params import exchange_binance_cid_config
        cfg = exchange_binance_cid_config(getattr(ex, 'ccxt', ex))
        return tuple(cfg.known) if cfg is not None else ()
    except Exception:
        return ()


def own_size(events: Iterable[JournalEvent]) -> float:
    """Position size from our fills: the sum of ``filled`` of ``own`` / ``bot_legacy`` orders
    (opening plus, closing minus; per order — the last cumulative ``filled``,
    for trades with ``trade_id`` — the sum of trade amounts)."""
    per_order: dict[str, float] = {}
    per_trade: dict[str, float] = {}
    sign: dict[str, int] = {}
    for e in events:
        if e.origin not in _OURS or e.event not in ('filled', 'partially_filled') or not e.side:
            continue
        ref = e.exchange_order_id or f'cid:{e.client_order_id}'
        if e.position_side:
            sign[ref] = 1 if _position_side(e.side, False) == e.position_side else -1
        if e.trade_id:
            per_trade[f'{ref}|{e.trade_id}'] = float(e.amount or 0.0)
        elif e.filled is not None:
            per_order[ref] = max(per_order.get(ref, 0.0), float(e.filled))
    total = 0.0
    traded: dict[str, float] = {}
    for k, v in per_trade.items():
        ref = k.rsplit('|', 1)[0]
        traded[ref] = traded.get(ref, 0.0) + v
    for ref in set(per_order) | set(traded):
        size = traded.get(ref, per_order.get(ref, 0.0))
        total += sign.get(ref, 1) * size
    return total


def _is_ts_tag(tag: str) -> bool:
    from bytx.journal.client_id import BOT_TAG_RE
    return bool(BOT_TAG_RE.fullmatch(tag))


def _base(ex: Any, symbol: Optional[str], contracts: Any) -> Optional[float]:
    """Exchange contracts → base coin (like Order size)."""
    if contracts is None:
        return None
    try:
        size = float(contracts)
        market = ex.ccxt.market(symbol) if symbol else None
        cs = float((market or {}).get('contractSize') or 1.0)
        return size * cs
    except Exception:
        return None


__all__ = ['Binding', 'PositionJournal', 'current_binding', 'own_size']
