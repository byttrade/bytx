"""Rebuilding the position journal from exchange history.

The source of truth is fills. A position is a "zero to zero" episode per (symbol, side):
the first opening fill in the ``[since, until]`` window opens it, the size returning to zero
closes it. Linked to the position are:

- the episode's fills (hedge mode — only its own side: Binance/BingX ``positionSide``,
  Bybit ``positionIdx``, OKX ``posSide``, KuCoin ``positionSide``);
- orders: those that filled the episode, and those placed/canceled/open during the position (TP/SL,
    grid limit orders) — with status "fired / canceled / open";
- funding during the position, liquidations and ADL (exchange markers, the forceOrders endpoint on BingX).

Each order's origin — :func:`bytx.journal.origin.classify_origin` (if the journal knows
the order — its origin and role). A foreign order or fill — an ``intervention`` event; our order
canceled not by us — ``external_cancel`` (by the exchange marker; without a marker — only if the journal
tracked the order live and recorded no cancel of its own). Incomplete history (page limit, request error,
position opened before the window) — a ``drift`` event of kind ``history_incomplete``: summary ``incomplete``.

Writes are idempotent (event keys from order and trade ids): a repeated recovery and
events already written live are not duplicated. ``position_id`` — the given one, otherwise the one
the journal already linked an episode order to, otherwise ``account:exchange:symbol:side:opened_at``.

History source — :class:`HistorySource` (default :class:`ExchangeHistorySource`: Binance —
7-day windows of ``/fapi/v1/userTrades``, Bybit — raw ``/v5/execution/list`` without ``execType``,
Hyperliquid — ``userFillsByTime`` (reconstructors ``exchanges/<exchange>/position_history.py``);
OKX / BingX / KuCoin — paginated ``fetch_my_trades``; funding — :mod:`bytx.exchanges._funding_history`).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

from loguru import logger

from bytx.journal.models import JournalEvent, PositionRecord
from bytx.journal.origin import OriginVerdict, classify_origin, rules_for

if TYPE_CHECKING:
    from bytx.journal.journal import PositionJournal
    from bytx.types import Fill, Order

_EPS = 1e-12
HISTORY = 'history'   # raw['source'] of events rebuilt from exchange history
_OURS = frozenset({'own', 'bot_legacy'})
_SYSTEM_KINDS = {'liquidation': 'liquidation', 'adl': 'adl', 'settlement': 'settlement'}
_FINAL = {'closed': 'filled', 'canceled': 'canceled', 'expired': 'expired', 'rejected': 'rejected'}
# order of events with equal ts: placement → fill → position → final → intervention → funding
_RANK = {'placed': 0, 'partially_filled': 1, 'position': 2, 'final': 3, 'intervention': 4, 'funding': 5}


@runtime_checkable
class HistorySource(Protocol):
    """Exchange history source for recovery. Each method returns (data, completeness)."""

    async def fills(self, symbol: str, since: int, until: int) -> tuple[list['Fill'], bool]:
        """Symbol fills over [since, until] in ascending time."""
        ...

    async def orders(self, symbol: str, since: int, until: int) -> tuple[list['Order'], bool]:
        """Symbol orders (closed, canceled, open) placed since ``since``."""
        ...

    async def funding(self, symbol: str, since: int, until: int) -> tuple[list[dict], bool]:
        """Funding ``[{'ts', 'amount', 'currency'}]`` (+ received / − paid)."""
        ...

    async def forced_orders(self, symbol: str, since: int, until: int) -> tuple[dict[str, str], bool]:
        """Liquidation / ADL order ids if the exchange returns them via a separate endpoint: {id: origin}."""
        ...


@dataclass(slots=True)
class RecoveryResult:
    """Recovery result: position, summary, number of events written, history completeness."""

    position_id: str
    record: PositionRecord
    opened_at: Optional[int]
    closed_at: Optional[int]
    created: int = 0
    existing: int = 0
    complete: bool = True
    reasons: list[str] = field(default_factory=list)


class ExchangeHistorySource:
    """Exchange history via :class:`~bytx.Exchange` (read-only requests)."""

    def __init__(self, ex: Any, *, bot_id: Optional[str] = None, page_limit: int = 100, max_pages: int = 50) -> None:
        self.ex = ex
        self.bot_id = bot_id or 'journal'
        self.page_limit = int(page_limit)
        self.max_pages = int(max_pages)

    async def _ready(self, symbol: str) -> str:
        await self.ex.connect()
        return self.ex.symbol(symbol)

    async def fills(self, symbol: str, since: int, until: int) -> tuple[list['Fill'], bool]:
        from bytx import convert
        s = await self._ready(symbol)
        c = self.ex.ccxt
        complete = True
        if c.id == 'binance':
            from bytx.exchanges.binance.position_history import BinancePositionHistoryReconstructor
            hedge = bool(getattr(self.ex, '_hedge', False))
            raw = await BinancePositionHistoryReconstructor(c, s, self.bot_id, hedge, False)._fetch_all_trades(since)
        elif c.id == 'bybit':
            from bytx.exchanges.bybit.position_history import BybitPositionHistoryMatcher
            m = BybitPositionHistoryMatcher(c, s, self.bot_id)
            raw, complete = await m._fetch_executions(since, until, m.new_budget())
        elif c.id == 'hyperliquid':
            from bytx.exchanges.hyperliquid.position_history import HyperliquidPositionHistoryReconstructor
            r = HyperliquidPositionHistoryReconstructor(c, s, self.bot_id)
            raw, complete = await r._fetch_fills(since, until, r.new_budget())
        else:
            raw, complete = await self._paged_trades(s, since, until)
        market = self.ex._market_or_none(s)
        out = [convert.fill(t, market, c.id) for t in raw if (t.get('timestamp') or 0) <= until
               and (t.get('timestamp') or 0) >= since]
        out.sort(key=lambda f: (f.timestamp or 0, str(f.id)))
        return out, complete

    async def _paged_trades(self, s: str, since: int, until: int) -> tuple[list[dict], bool]:
        """fetch_my_trades forward by pages: cursor — the last trade time, duplicates — by id."""
        seen: dict[Any, dict] = {}
        cursor = int(since)
        for _ in range(self.max_pages):
            batch = await self.ex._call(self.ex.ccxt.fetch_my_trades, s, cursor, self.page_limit) or []
            new = 0
            for t in batch:
                key = t.get('id') if t.get('id') is not None else (t.get('timestamp'), t.get('order'), t.get('amount'))
                if key not in seen:
                    seen[key] = t
                    new += 1
            if len(batch) < self.page_limit:
                return list(seen.values()), True
            last = max((t.get('timestamp') or 0) for t in batch)
            if last > until:
                return list(seen.values()), True
            cursor = last if last > cursor else cursor + 1
            if new == 0:
                cursor += 1
        logger.warning(f'position journal: trade history {s} truncated at {self.max_pages} pages')
        return list(seen.values()), False

    async def orders(self, symbol: str, since: int, until: int) -> tuple[list['Order'], bool]:
        s = await self._ready(symbol)
        c = self.ex.ccxt
        has = c.has
        calls = []
        if has.get('fetchOrders') is True:
            calls.append(c.fetch_orders)
        else:
            if has.get('fetchCanceledAndClosedOrders') is True:
                calls.append(c.fetch_canceled_and_closed_orders)
            else:
                if has.get('fetchClosedOrders'):
                    calls.append(c.fetch_closed_orders)
                if has.get('fetchCanceledOrders'):
                    calls.append(c.fetch_canceled_orders)
            calls.append(c.fetch_open_orders)
        found: dict[str, 'Order'] = {}
        complete = True
        for fn in calls:
            try:
                raw = await self.ex._call(fn, s, since, None) if fn is not c.fetch_open_orders \
                    else await self.ex._call(fn, s)
            except Exception as e:
                complete = False
                logger.warning(f'position journal: {getattr(fn, "__name__", fn)} {s}: {e}')
                continue
            for r in raw or []:
                o = self.ex._order(r)
                if o.id:
                    found[o.id] = o
        return list(found.values()), complete

    async def funding(self, symbol: str, since: int, until: int) -> tuple[list[dict], bool]:
        s = await self._ready(symbol)
        res = await _funding_adapter(self.ex, s, self.bot_id).fetch_funding_paid(s, since, until)
        return list(res.get('items') or []), bool(res.get('complete'))

    async def forced_orders(self, symbol: str, since: int, until: int) -> tuple[dict[str, str], bool]:
        s = await self._ready(symbol)
        c = self.ex.ccxt
        if c.id != 'bingx':
            return {}, True   # on other exchanges the marker is in the order / fill itself
        rules = rules_for('bingx')
        market_id = c.market(s)['id']
        out: dict[str, str] = {}
        complete = True
        for close_type, origin in rules.FORCE_ORDER_TYPES.items():
            params = {'symbol': market_id, 'autoCloseType': close_type, 'startTime': int(since),
                      'endTime': int(until), 'limit': rules.FORCE_ORDERS_LIMIT}
            try:
                resp = await self.ex._call(c.swapV2PrivateGetTradeForceOrders, params)
            except Exception as e:
                complete = False
                logger.warning(f'position journal: BingX forceOrders {close_type} {s}: {e}')
                continue
            rows = ((resp or {}).get('data') or {}).get('orders') or []
            for row in rows:
                if row.get('orderId') is not None:
                    out[str(row['orderId'])] = origin
            if len(rows) >= rules.FORCE_ORDERS_LIMIT:
                complete = False   # page is full — older records were not fetched
        return out, complete


def _funding_adapter(ex: Any, symbol: str, bot_id: str) -> Any:
    """Minimal "Client" for the ``FundingHistory`` mixin on top of Exchange."""
    from bytx.exchanges._funding_history import FundingHistory

    class _FundingAdapter(FundingHistory):
        def __init__(self) -> None:
            self.exchange = ex.ccxt
            self.symbol = symbol
            self.bot_id = bot_id

        async def _load_markets_cached(self) -> None:
            await ex.connect()

    return _FundingAdapter()


# ─── recovery ───────────────────────────────────────────────────────────────

@dataclass(slots=True)
class _Step:
    fill: 'Fill'
    delta: float
    after: float


def _signed(fill: 'Fill', side: str) -> float:
    size = abs(float(fill.size or 0.0))
    return size if (fill.side == 'buy') == (side == 'long') else -size


def _episode(ex: Any, fills: list['Fill'], side: str) -> tuple[list[_Step], Optional[int], Optional[int], bool]:
    """Position episode: steps, opened, closed, whether there were closing fills before the open."""
    rules = rules_for(ex)
    steps: list[_Step] = []
    net = 0.0
    opened = closed = None
    pre_existing = False
    for f in sorted(fills, key=lambda x: (x.timestamp or 0, str(x.id))):
        info = f.raw.get('info') if isinstance(f.raw.get('info'), dict) else {}
        ps = rules.fill_position_side(info)
        if ps is not None and ps != side:
            continue
        delta = _signed(f, side)
        if opened is None:
            if delta <= 0:
                pre_existing = pre_existing or ps is not None  # hedge: this side was closed before the window
                continue
            opened = f.timestamp
        if net + delta < -_EPS:      # one-way: reversal — the episode ended at zero
            delta = -net
        net += delta
        steps.append(_Step(f, delta, max(net, 0.0)))
        if net <= _EPS * max(1.0, abs(delta)):
            closed = f.timestamp
            break
    if opened is not None and not pre_existing:
        # one-way: a closing fill before the first opening one — the position existed before the window
        first_rules_side = None
        for f in sorted(fills, key=lambda x: (x.timestamp or 0, str(x.id))):
            if (f.timestamp or 0) >= opened:
                break
            info = f.raw.get('info') if isinstance(f.raw.get('info'), dict) else {}
            first_rules_side = rules.fill_position_side(info)
            if first_rules_side in (None, side) and _signed(f, side) < 0:
                pre_existing = True
                break
    return steps, opened, closed, pre_existing


def _info(obj: Any) -> dict:
    raw = getattr(obj, 'raw', None)
    info = raw.get('info') if isinstance(raw, dict) else None
    return info if isinstance(info, dict) else {}


def _order_ts(o: 'Order') -> int:
    return int(o.timestamp or 0)


def _final_ts(o: 'Order') -> int:
    raw = o.raw if isinstance(o.raw, dict) else {}
    return int(raw.get('lastUpdateTimestamp') or raw.get('lastTradeTimestamp') or o.timestamp or 0)


async def recover_position(journal: 'PositionJournal', ex: Any, symbol: str, side: str, *, since: int,
                           until: Optional[int] = None, position_id: Optional[str] = None,
                           source: Optional[HistorySource] = None, funding: bool = True,
                           exchange_size: Optional[float] = None) -> RecoveryResult:
    """Rebuild the ``(symbol, side)`` position journal from exchange history (see the module).

    ``since`` / ``until`` — the search window (ms; ``until`` defaults to now); ``side`` —
    ``long`` / ``short``; ``exchange_size`` — the open position size on the exchange (base) for reconciliation
    (:meth:`~bytx.journal.PositionJournal.reconcile`). Returns :class:`RecoveryResult`.
    """
    from bytx.errors import ConfigError
    if side not in ('long', 'short'):
        raise ConfigError(f"side: 'long' or 'short', not {side!r}")
    until = int(until if until is not None else time.time() * 1000)
    src = source if source is not None else ExchangeHistorySource(ex, bot_id=journal.bot_id)
    name = getattr(ex, 'name', str(ex))
    try:
        sym = ex.symbol(symbol)
    except Exception:
        sym = symbol
    reasons: list[str] = []

    fills, ok = await src.fills(sym, int(since), until)
    if not ok:
        reasons.append('fill history is incomplete')
    steps, opened, closed, pre_existing = _episode(ex, fills, side)
    if pre_existing:
        reasons.append('the window has closing fills before the first opening one — the position was opened earlier')
    end = closed if closed is not None else until
    fill_order_ids = {s.fill.order_id for s in steps if s.fill.order_id}
    orders: list['Order'] = []
    forced: dict[str, str] = {}
    fund: list[dict] = []
    if opened is not None:
        got, ok = await src.orders(sym, int(since), end)
        if not ok:
            reasons.append('order history is incomplete')
        first_ts = min([opened] + [_order_ts(o) for o in got if o.id in fill_order_ids and o.timestamp])
        rules = rules_for(ex)
        for o in got:
            if o.symbol and o.symbol != sym:
                continue
            ps = rules.fill_position_side(_info(o))
            if ps is not None and ps != side:
                continue
            if o.id in fill_order_ids or first_ts <= _order_ts(o) <= end:
                orders.append(o)
        forced, ok = await src.forced_orders(sym, int(opened), end)
        if not ok:
            reasons.append('liquidation history is incomplete')
        if funding:
            fund, ok = await src.funding(sym, int(opened), end)
            if not ok:
                reasons.append('funding history is incomplete')

    # position: the given one, one already linked by the journal, otherwise a stable key
    known: dict[str, list[JournalEvent]] = {}
    for oid in {o.id for o in orders} | fill_order_ids:
        try:
            evs = await journal.store.find_order(bot_id=journal.bot_id, exchange_order_id=oid)
        except Exception:
            evs = []
        if evs:
            known[oid] = evs
    for o in orders:
        if o.id not in known and o.client_id:
            try:
                evs = await journal.store.find_order(bot_id=journal.bot_id, client_order_id=o.client_id)
            except Exception:
                evs = []
            if evs:
                known[o.id] = evs
    pid = position_id
    if pid is None:
        linked = sorted((e for evs in known.values() for e in evs), key=lambda e: (e.ts, e.seq or 0))
        pid = linked[0].position_id if linked else f'{journal.account or "-"}:{name}:{sym}:{side}:{opened}'

    def verdict(oid: Optional[str], cid: Optional[str], info: dict, kind: str = 'order') -> OriginVerdict:
        if oid and oid in forced:
            return OriginVerdict(forced[oid], 'exact', f'{name}: order is in the forced list ({forced[oid]})')
        evs = known.get(oid or '')
        if evs:
            role = next((e.role for e in evs if e.role), None)
            level = next((e.level_id for e in evs if e.level_id is not None), None)
            return OriginVerdict(evs[0].origin, 'exact', 'order is recorded in the journal', role, level)
        return classify_origin(ex, client_id=cid, info=info, kind=kind, tag=journal._parse_tag(ex) or journal.tag,
                               platform_formats=journal.platform_formats,
                               broker_prefixes=_prefixes(ex), resolve_parent=_parent(known))

    events: list[tuple[int, int, JournalEvent]] = []

    def add(ts: int, rank: str, ev: JournalEvent) -> None:
        events.append((int(ts or 0), _RANK[rank], ev))

    common = dict(bot_id=journal.bot_id, exchange=name, symbol=sym, position_side=side)
    order_verdicts: dict[str, OriginVerdict] = {}
    by_id = {o.id: o for o in orders}
    for o in orders:
        info = _info(o)
        v = verdict(o.id, o.client_id, info)
        order_verdicts[o.id] = v
        trig = o.trigger_price
        base = dict(common, role=v.role, level_id=v.level_id, exchange_order_id=o.id, client_order_id=o.client_id,
                    side=o.side, amount=o.size, order_type=o.type or None, trigger_price=trig,
                    reduce_only=o.reduce_only)
        add(_order_ts(o), 'placed', JournalEvent(pid, 'placed', v.origin, ts=_order_ts(o),
                                                price=trig if trig is not None and o.price is None else o.price,
                                                confidence=v.confidence, note=v.reason[:300], **base))
        final = _FINAL.get(o.status or '')
        if final is not None:
            fee = None if o.id in fill_order_ids else o.fee   # fees come from trades, no double counting
            add(_final_ts(o), 'final', JournalEvent(pid, final, v.origin, ts=_final_ts(o), price=o.average or o.price,
                                                    filled=o.filled, fee=fee if final == 'filled' else None,
                                                    fee_currency=o.fee_currency if fee else None, **base))
        if not v.is_ours:
            add(_order_ts(o), 'intervention', JournalEvent(
                pid, 'intervention', v.origin, ts=_order_ts(o), kind=_SYSTEM_KINDS.get(v.origin, 'foreign_order'),
                confidence=v.confidence, note=v.reason[:300], price=o.average or o.price, filled=o.filled, **base))
        elif o.status == 'canceled':
            sig = rules_for(ex).cancel_signal(info)
            live = [e for e in known.get(o.id, []) if (e.raw or {}).get('source') != HISTORY]
            own_cancel = any(e.event == 'canceled' and e.note in ('own_cancel', 'replaced') for e in (live or []))
            if sig is not None and sig.origin and not sig.expected:
                origin, conf, why = sig.origin, sig.confidence, sig.reason
            elif live and not own_cancel and not (sig is not None and sig.expected):
                origin, conf = 'unknown', 'heuristic'
                why = (sig.reason + ': ' if sig is not None else '') + 'the journal tracked the order, no own cancel recorded'
            else:
                origin = None
            if origin is not None:
                add(_final_ts(o), 'intervention', JournalEvent(
                    pid, 'intervention', origin, ts=_final_ts(o), kind='external_cancel', confidence=conf,
                    note=why[:300], **base))

    # fills and position events
    synthesized: dict[str, list[_Step]] = {}
    for i, st in enumerate(steps):
        f = st.fill
        info = _info(f)
        ov = order_verdicts.get(f.order_id or '')
        fv = verdict(f.order_id, f.client_id, info, kind='fill')
        if ov is not None and (fv.origin == 'unknown' or fv.origin in _OURS):
            v = ov
        elif ov is None and fv.origin == 'unknown' and f.order_id in known:
            v = verdict(f.order_id, None, {}, kind='order')
        else:
            v = fv
        raw_extra = {'contracts': f.contracts, 'taker_or_maker': None if f.maker is None else
                     ('maker' if f.maker else 'taker'), 'realized_pnl': _pnl(info), 'info': info}
        base = dict(common, role=v.role, level_id=v.level_id, exchange_order_id=f.order_id, client_order_id=f.client_id,
                    side=f.side, price=f.price, trade_id=str(f.id) if f.id is not None else None)
        add(f.timestamp or 0, 'partially_filled', JournalEvent(
            pid, 'partially_filled', v.origin, ts=f.timestamp or 0, amount=abs(f.size), fee=f.fee,
            fee_currency=f.fee_currency, confidence=v.confidence, raw=raw_extra, **base))
        if i == 0:
            pev = 'position_opened'
        elif v.event in ('liquidated', 'adl') or v.origin in ('liquidation', 'adl'):
            pev = 'adl' if (v.event == 'adl' or v.origin == 'adl') else 'liquidated'
        elif st.after <= _EPS:
            pev = 'closed'
        else:
            pev = 'increased' if st.delta > 0 else 'reduced'
        add(f.timestamp or 0, 'position', JournalEvent(
            pid, pev, v.origin, ts=f.timestamp or 0, amount=abs(st.delta), contracts_after=st.after,
            confidence=v.confidence, role='liquidation' if pev in ('liquidated', 'adl') else v.role,
            **{k: val for k, val in base.items() if k != 'role'}))
        if f.order_id and f.order_id not in by_id:
            synthesized.setdefault(f.order_id, []).append(st)
            if not v.is_ours and len(synthesized[f.order_id]) == 1:
                add(f.timestamp or 0, 'intervention', JournalEvent(
                    pid, 'intervention', v.origin, ts=f.timestamp or 0, kind=_SYSTEM_KINDS.get(v.origin, 'foreign_order'),
                    confidence=v.confidence, note=v.reason[:300], amount=abs(f.size),
                    **{k: val for k, val in base.items() if k != 'trade_id'}))
    for oid, sts in synthesized.items():
        last = sts[-1].fill
        filled = sum(abs(s.fill.size) for s in sts)
        first_v = verdict(oid, last.client_id, _info(last), kind='fill')
        if first_v.origin == 'unknown' and oid in known:
            first_v = verdict(oid, None, {}, kind='order')
        ev_origin = first_v.origin
        add(last.timestamp or 0, 'final', JournalEvent(
            pid, 'filled', ev_origin, ts=last.timestamp or 0, role=first_v.role, level_id=first_v.level_id,
            exchange_order_id=oid, client_order_id=last.client_id, side=last.side, price=last.price, amount=filled,
            filled=filled, note='order not found in history: status from fills', **common))

    for item in fund:
        ts = int(item.get('ts') or 0)
        add(ts, 'funding', JournalEvent(pid, 'funding', 'funding', ts=ts, amount=float(item.get('amount') or 0.0),
                                        fee_currency=item.get('currency'), trade_id=f'funding:{ts}', **common))

    if opened is None:
        reasons.append(f'no opening {side} fills in the window')
    if reasons:
        add(opened or int(since), 'intervention', JournalEvent(
            pid, 'drift', 'unknown', ts=opened or int(since), kind='history_incomplete', note='; '.join(reasons)[:300],
            key=f'{journal.bot_id or ""}|pos:{pid}|drift|history_incomplete:{"; ".join(reasons)}', **common))

    created = existing = 0
    for _, _, ev in sorted(events, key=lambda x: (x[0], x[1])):
        ev.raw = {**(ev.raw or {}), 'source': HISTORY}   # distinguishes from events written live
        res = await journal.record(ev)
        if res is None:
            continue
        if res.created:
            created += 1
            if res.event.event == 'intervention':
                journal._notify(res.event)
        else:
            existing += 1
    if closed is None and exchange_size is not None and opened is not None:
        await journal.reconcile(pid, exchange_size, ts=until, symbol=sym)
    evs = await journal.read(pid)
    record = PositionRecord.from_events(evs) if evs else PositionRecord(position_id=pid, incomplete=True)
    return RecoveryResult(pid, record, opened, closed, created, existing, not reasons, reasons)


def _pnl(info: dict) -> Optional[float]:
    for key in ('realizedPnl', 'closedPnl', 'pnl', 'rp', 'profit', 'closedSize_pnl'):
        value = info.get(key)
        if value not in (None, ''):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _prefixes(ex: Any) -> tuple[str, ...]:
    from bytx.journal.journal import _broker_prefixes
    return _broker_prefixes(ex)


def _parent(known: dict[str, list[JournalEvent]]) -> Any:
    def resolve(parent_id: str) -> Optional[str]:
        evs = known.get(parent_id)
        return evs[0].origin if evs else None
    return resolve


__all__ = ['ExchangeHistorySource', 'HistorySource', 'RecoveryResult', 'recover_position']
