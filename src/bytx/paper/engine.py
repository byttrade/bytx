"""Paper execution core: order book matching, positions, balance, PnL, funding.

Pure synchronous code with no network and no clock of its own (the clock is passed in): the same
sequence of inputs (order books, trades, tickers, rates, orders) yields the same
fills, ids and timestamps. :class:`bytx.paper.PaperExchange` wraps it in the
:class:`bytx.Exchange` interface.

Execution model:

- market — walks the book levels (taker, each level's price); not enough liquidity —
  the rest is ``expired``; no liquidity at all — :class:`~bytx.errors.InvalidOrder`;
- a limit crossing the book on placement — taker across levels up to its price, the rest rests
  in the book (GTC) or is ``expired`` (IOC; FOK — all or nothing); ``post_only`` on crossing —
  :class:`~bytx.errors.InvalidOrder` (the order is not placed);
- a resting limit fills as maker **at its own price** when a new book crosses it (amount —
  the opposite liquidity beyond the price) or a public trade goes **through** the price (at the
  price itself — no: queue position is unknown); hence partial fills;
- own fills "consume" levels of the current book until the next snapshot;
- reduce-only: no position — :class:`~bytx.errors.PositionAlreadyClosed`; market — amount
  no more than the position; limit — fills no more than the position; once the position closes, reduce-only
  orders and TP/SL of that side are cancelled;
- TP/SL — triggered by mark (default; ticker ``mark``, otherwise book mid) or last
  (last trade, otherwise mid), executed as a market reduce-only against the book;
  TP/SL attached to an order are placed on its first fill;
- funding — at the latest :meth:`PaperEngine.on_funding` rate at ``next_funding_time``
  (long pays when the rate > 0), once per settlement time;
- liquidation is **simplified** (not the exchange model): price
  ``entry × (1 ∓ 1/leverage ± mmr)`` per position separately (like isolated, also in cross),
  closed at mark; marked ``raw['paper']['liquidation_model'] = 'simplified'``.

Derivatives balance — in the market's settle currency: ``total`` = wallet + unrealized PnL,
``used`` = position margin (entry × amount / leverage) + open order margin, ``free`` = total − used.
Spot — per currency, fee in quote (simplification: some exchanges charge it in the received coin).
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from bytx.errors import (
    ConfigError,
    DuplicateOrderId,
    InsufficientFunds,
    InvalidOrder,
    OrderNotFound,
    PositionAlreadyClosed,
)
from bytx.paper.fees import PaperFees
from bytx.protection import TpSlOrder
from bytx.types import (
    Balance,
    BalanceItem,
    Fill,
    FundingRate,
    Market,
    Order,
    OrderBook,
    Position,
    Ticker,
    Trade,
)

EPS = 1e-9
LIQUIDATION_MODEL = 'simplified'
_TIF = frozenset({'GTC', 'IOC', 'FOK', 'PO'})


@dataclass(slots=True)
class PaperRequest:
    """Normalized order for the engine. ``contracts`` — in exchange contracts (spot — base).

    ``take_profit`` / ``stop_loss`` — attached triggers (placed on the first fill),
    ``tp_client_id`` / ``sl_client_id`` — their client_id.
    """

    symbol: str
    side: str
    type: str
    contracts: float
    price: Optional[float] = None
    client_id: Optional[str] = None
    post_only: bool = False
    time_in_force: Optional[str] = None
    reduce_only: bool = False
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    tp_client_id: Optional[str] = None
    sl_client_id: Optional[str] = None
    trigger_by: Optional[str] = None
    margin_mode: Optional[str] = None


@dataclass(slots=True)
class PaperFunding:
    """Funding accrual on a position (sign: plus — received)."""

    symbol: str
    side: str
    amount: float
    rate: float
    mark_price: float
    timestamp: int
    currency: Optional[str] = None


@dataclass(slots=True)
class PaperLiquidation:
    """Simplified liquidation of a position (see module)."""

    symbol: str
    side: str
    price: float
    contracts: float
    order: Order
    timestamp: int
    model: str = LIQUIDATION_MODEL


@dataclass(slots=True)
class _O:
    id: str
    client_id: Optional[str]
    symbol: str
    side: str
    type: str
    price: Optional[float]
    contracts: float
    ts: int
    post_only: bool = False
    tif: Optional[str] = None
    reduce_only: bool = False
    pos_side: Optional[str] = None
    margin_mode: Optional[str] = None
    filled: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    status: str = 'open'
    updated: int = 0
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    tp_cid: Optional[str] = None
    sl_cid: Optional[str] = None
    trigger_by: Optional[str] = None
    attached_done: bool = False
    liquidation: bool = False

    @property
    def remaining(self) -> float:
        return max(self.contracts - self.filled, 0.0)


@dataclass(slots=True)
class _T:
    id: str
    client_id: Optional[str]
    symbol: str
    kind: str
    pos_side: str
    trigger_price: float
    trigger_by: str
    contracts: Optional[float]
    ts: int
    state: str = 'open'
    filled: float = 0.0
    cost: float = 0.0
    fee: float = 0.0
    updated: int = 0
    parent_id: Optional[str] = None


@dataclass(slots=True)
class _P:
    symbol: str
    side: str
    contracts: float = 0.0
    entry: float = 0.0
    realized: float = 0.0
    funding: float = 0.0
    leverage: float = 1.0
    margin_mode: str = 'cross'
    ts: int = 0


@dataclass(slots=True)
class _Book:
    book: OrderBook
    consumed: dict = field(default_factory=dict)   # (book side, price) → base consumed


class PaperEngine:
    """Paper execution engine for one account on one exchange (see module).

    ``market`` — function symbol → :class:`~bytx.types.Market` (step, contract_size, settle
    currency); ``balances`` — starting funds {currency: amount}; ``clock`` — time function, ms;
    ``hedge`` — position mode; ``maintenance_margin_rate`` — rate for simplified liquidation;
    ``trigger_by`` — 'mark' / 'last' for TP/SL by default; ``fill_on_trades`` — fill
    resting limits on trades through the price; ``default_leverage`` — leverage without :meth:`set_leverage`.
    """

    def __init__(self, *, exchange: str, market_type: str, market: Callable[[str], Market], fees: PaperFees,
                 balances: dict[str, float], clock: Callable[[], int], hedge: bool = False,
                 maintenance_margin_rate: float = 0.005, trigger_by: str = 'mark', fill_on_trades: bool = True,
                 default_leverage: float = 1.0, liquidation: bool = True) -> None:
        if trigger_by not in ('mark', 'last'):
            raise ConfigError(f"trigger_by: 'mark' or 'last', not {trigger_by!r}")
        self.exchange = exchange
        self.market_type = market_type
        self.spot = market_type == 'spot'
        self._market = market
        self.fees = fees
        self.clock = clock
        self.hedge = bool(hedge)
        self.mmr = float(maintenance_margin_rate)
        self.trigger_by = trigger_by
        self.fill_on_trades = fill_on_trades
        self.default_leverage = float(default_leverage)
        self.liquidation = liquidation
        from bytx.exchanges import market_no_liquidity, post_only_cross
        # what the exchange does with a post-only crossing the book and a market with no liquidity — per its docs
        self.post_only_cross = post_only_cross(exchange)[0]
        self.market_no_liquidity = market_no_liquidity(exchange)[0]
        self._wallet: dict[str, float] = {k.upper(): float(v) for k, v in balances.items()}
        self._ids = itertools.count(1)
        self._fill_ids = itertools.count(1)
        self._orders: dict[str, _O] = {}
        self._open: dict[str, dict[str, _O]] = {}            # symbol → open orders (by id)
        self._triggers: dict[str, _T] = {}
        self._positions: dict[tuple[str, str], _P] = {}
        self._fills: list[Fill] = []
        self._books: dict[str, _Book] = {}
        self._mark: dict[str, float] = {}
        self._last: dict[str, float] = {}
        self._leverage: dict[str, float] = {}
        self._funding_rate: dict[str, tuple[float, Optional[int]]] = {}
        self._funding_done: dict[str, int] = {}
        self._events: list[tuple[str, Any]] = []
        self._balance_dirty = False
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self.funding_paid = 0.0
        self.liquidations = 0

    # ─── market data ────────────────────────────────────────────────────

    def on_book(self, book: OrderBook) -> None:
        """New order book snapshot: fill crossed limits, check triggers and liquidation."""
        s = book.symbol
        self._books[s] = _Book(book)
        for o in list(self._open.get(s, {}).values()):
            if o.type == 'limit' and o.status == 'open':
                self._match_resting_book(o)
        self._after_price(s)

    def on_trade(self, trade: Trade) -> None:
        """Public trade: last price; resting limits — on a trade through the price (``fill_on_trades``)."""
        s = trade.symbol
        self._last[s] = float(trade.price)
        if self.fill_on_trades:
            left = float(trade.size)
            for o in list(self._open.get(s, {}).values()):
                if left <= EPS:
                    break
                if o.type != 'limit' or o.price is None or o.status != 'open':
                    continue
                through = trade.price < o.price if o.side == 'buy' else trade.price > o.price
                if not through:
                    continue
                cs = self._cs(s)
                qty = self._cap(o, min(o.remaining, left / cs))
                if qty > EPS:
                    self._fill(o, qty, o.price, maker=True)
                    left -= qty * cs
                    self._settle_order(o)
        self._after_price(s)

    def on_ticker(self, ticker: Ticker) -> None:
        """Ticker: ``mark`` (for mark triggers, PnL, liquidation) and ``last``."""
        s = ticker.symbol
        if ticker.mark:
            self._mark[s] = float(ticker.mark)
        if ticker.last:
            self._last[s] = float(ticker.last)
        self._after_price(s)

    def on_funding(self, rate: FundingRate) -> None:
        """Funding rate and settlement time (``next_funding_time``); mark if present."""
        s = rate.symbol
        if rate.mark_price:
            self._mark[s] = float(rate.mark_price)
        if rate.rate is not None:
            self._funding_rate[s] = (float(rate.rate), rate.next_funding_time)
        self.tick()

    def tick(self) -> None:
        """Time-based event check (funding settlement). Called on every input."""
        now = self.clock()
        for s, (rate, when) in list(self._funding_rate.items()):
            if when is None or now < when or self._funding_done.get(s, -1) >= when:
                continue
            self._funding_done[s] = when
            self.settle_funding(s, rate, ts=when)
        self._flush_balance()

    def settle_funding(self, symbol: str, rate: float, *, mark: Optional[float] = None,
                       ts: Optional[int] = None) -> list[PaperFunding]:
        """Accrues funding on the symbol's positions now (``mark`` — otherwise the current mark)."""
        price = mark if mark is not None else self._mark_price(symbol)
        out: list[PaperFunding] = []
        if price is None or self.spot:
            return out
        cs = self._cs(symbol)
        cur = self._settle_cur(symbol)
        for side in ('long', 'short'):
            p = self._positions.get((symbol, side))
            if p is None or p.contracts <= EPS:
                continue
            amount = -(1 if side == 'long' else -1) * p.contracts * cs * price * rate
            self._wallet[cur] = self._wallet.get(cur, 0.0) + amount
            p.funding += amount
            self.funding_paid += amount
            ev = PaperFunding(symbol, side, amount, rate, price, ts if ts is not None else self.clock(), cur)
            out.append(ev)
            self._events.append(('funding', ev))
            self._events.append(('positions', self._position_model(p)))
            self._balance_dirty = True
        self._flush_balance()
        return out

    # ─── trading ───────────────────────────────────────────────────────────

    def set_leverage(self, symbol: str, leverage: float) -> None:
        if leverage <= 0:
            raise ConfigError(f'leverage must be > 0, not {leverage!r}')
        self._leverage[symbol] = float(leverage)
        for side in ('long', 'short'):
            p = self._positions.get((symbol, side))
            if p is not None:
                p.leverage = float(leverage)

    def leverage(self, symbol: str) -> float:
        return self._leverage.get(symbol, self.default_leverage)

    def submit(self, req: PaperRequest) -> Order:
        """Places an order; immediately fills whatever crosses the book. Returns the order state."""
        s = req.symbol
        if req.side not in ('buy', 'sell'):
            raise ConfigError(f"side: 'buy' or 'sell', not {req.side!r}")
        if req.contracts <= EPS:
            raise InvalidOrder(f'order amount must be > 0 ({s})', exchange=self.exchange)
        tif = req.time_in_force.upper() if req.time_in_force else None
        if tif is not None and tif not in _TIF:
            raise ConfigError(f'time_in_force: {sorted(_TIF)}, not {req.time_in_force!r}')
        post_only = req.post_only or tif == 'PO'
        if req.type not in ('market', 'limit'):
            raise InvalidOrder(f'paper: order type {req.type!r} is not supported (market / limit)',
                               exchange=self.exchange)
        if req.type == 'limit' and req.price is None:
            raise ConfigError(f'limit order requires price ({s})')
        if req.client_id and any(o.client_id == req.client_id for o in self._open.get(s, {}).values()):
            raise DuplicateOrderId(f'client_id {req.client_id} already belongs to an open order', exchange=self.exchange,
                                   details={'client_id': req.client_id, 'symbol': s})
        if req.reduce_only and self.spot:
            raise ConfigError(f'reduce_only is for derivatives only ({s})')
        pos_side = self._target_side(req.side, req.reduce_only)
        contracts = float(req.contracts)
        if req.reduce_only:
            reducible = self._reducible(s, req.side)
            if reducible <= EPS:
                raise PositionAlreadyClosed(f'reduce-only: no {s} position for {req.side}', exchange=self.exchange,
                                            details={'symbol': s, 'side': req.side})
            if req.type == 'market':
                contracts = min(contracts, reducible)
        book = self._books.get(s)
        crossing = self._crossing(s, req.side, req.price) if req.type == 'limit' else 0.0
        cancel = False   # the exchange accepts the order and cancels it at once (table exchanges/<exchange>/matching.py)
        if req.type == 'market' and self._available(s, req.side, None) <= EPS:
            if book is None or self.market_no_liquidity != 'cancel':
                why = 'no order book' if book is None else 'no liquidity in the order book'
                raise InvalidOrder(f'paper: {why} for market order {s}', exchange=self.exchange,
                                   details={'symbol': s})
            cancel = True
        if post_only and crossing > EPS:
            if self.post_only_cross != 'cancel':
                raise InvalidOrder(f'post-only order {s} would cross the book (would fill as taker)',
                                   exchange=self.exchange, code='post_only_would_take', details={'symbol': s})
            cancel = True
        if cancel:
            now = self.clock()
            o = _O(f'P{next(self._ids)}', req.client_id, s, req.side, req.type,
                   float(req.price) if req.price is not None else None, contracts, now, post_only=post_only,
                   tif=tif, reduce_only=req.reduce_only, pos_side=pos_side, margin_mode=req.margin_mode,
                   updated=now)
            o.status = 'canceled'
            self._orders[o.id] = o
            self._emit_order(o)
            return self._order_model(o)
        if not req.reduce_only:
            self._check_funds(s, req.side, contracts, req.price)
        now = self.clock()
        o = _O(f'P{next(self._ids)}', req.client_id, s, req.side, req.type,
               float(req.price) if req.price is not None else None, contracts, now, post_only=post_only,
               tif=tif, reduce_only=req.reduce_only, pos_side=pos_side, margin_mode=req.margin_mode,
               updated=now, take_profit=req.take_profit, stop_loss=req.stop_loss, tp_cid=req.tp_client_id,
               sl_cid=req.sl_client_id, trigger_by=req.trigger_by)
        self._orders[o.id] = o
        if tif == 'FOK' and self._available(s, req.side, o.price) < contracts - EPS:
            o.status = 'expired'
            self._emit_order(o)
            self._flush_balance()
            return self._order_model(o)
        self._take(o, o.price)
        if o.status == 'open':
            if o.remaining <= EPS:
                o.status = 'closed'
            elif o.type == 'market' or tif in ('IOC', 'FOK'):
                o.status = 'expired'
            else:
                self._open.setdefault(s, {})[o.id] = o
        self._emit_order(o)
        self._after_fill_effects(o)
        self._flush_balance()
        return self._order_model(o)

    def amend(self, symbol: str, order_id: str, *, price: Optional[float] = None,
              contracts: Optional[float] = None) -> Order:
        """Changes the price and/or amount of an open order (same id). If it crosses the book, it fills as
        taker (post-only — :class:`InvalidOrder`, the order stays unchanged)."""
        o = self._find(symbol, order_id, None)
        if o.status != 'open' or o.id not in self._open.get(symbol, {}):
            raise InvalidOrder(f'order {order_id} is not open ({o.status})', exchange=self.exchange)
        new_price = float(price) if price is not None else o.price
        if contracts is not None and contracts < o.filled - EPS:
            raise InvalidOrder(f'new amount {contracts} is less than filled {o.filled}', exchange=self.exchange)
        if o.post_only and self._crossing(symbol, o.side, new_price) > EPS:
            raise InvalidOrder(f'post-only order {symbol} would cross the book', exchange=self.exchange,
                               code='post_only_would_take')
        o.price = new_price
        if contracts is not None:
            o.contracts = float(contracts)
        o.updated = self.clock()
        self._take(o, o.price)
        if o.remaining <= EPS:
            o.status = 'closed'
            self._open[symbol].pop(o.id, None)
        self._emit_order(o)
        self._after_fill_effects(o)
        self._flush_balance()
        return self._order_model(o)

    def cancel(self, symbol: str, order_id: Optional[str] = None, *,
               client_id: Optional[str] = None) -> tuple[Order, bool]:
        """Cancel → (order, whether we cancelled it). Already closed — final status and False;
        unknown id — :class:`OrderNotFound`. Works for TP/SL too."""
        t = self._find_trigger(symbol, order_id, client_id)
        if t is not None:
            if t.state != 'open':
                return self._trigger_model(t), False
            t.state = 'canceled'
            t.updated = self.clock()
            self._emit_trigger(t)
            return self._trigger_model(t), True
        o = self._find(symbol, order_id, client_id)
        if o.status != 'open':
            return self._order_model(o), False
        o.status = 'canceled'
        o.updated = self.clock()
        self._open.get(symbol, {}).pop(o.id, None)
        self._emit_order(o)
        self._flush_balance()
        return self._order_model(o), True

    def submit_trigger(self, symbol: str, kind: str, trigger_price: float, *, position_side: str,
                       contracts: Optional[float] = None, client_id: Optional[str] = None,
                       trigger_by: Optional[str] = None, parent_id: Optional[str] = None) -> TpSlOrder:
        """TP/SL: reduce-only market on trigger. ``contracts`` None — the whole position side."""
        if kind not in ('take_profit', 'stop_loss'):
            raise ConfigError(f"kind: 'take_profit' or 'stop_loss', not {kind!r}")
        if position_side not in ('long', 'short'):
            raise ConfigError(f"position_side: 'long' or 'short', not {position_side!r}")
        if self.spot:
            raise ConfigError(f'paper TP/SL is for derivatives only ({symbol})')
        by = trigger_by or self.trigger_by
        if by not in ('mark', 'last'):
            raise ConfigError(f"trigger_by: 'mark' or 'last', not {by!r}")
        if client_id and any(t.client_id == client_id and t.state == 'open' for t in self._triggers.values()):
            raise DuplicateOrderId(f'client_id {client_id} already belongs to an open TP/SL', exchange=self.exchange,
                                   details={'client_id': client_id, 'symbol': symbol})
        now = self.clock()
        t = _T(f'P{next(self._ids)}', client_id, symbol, kind, position_side, float(trigger_price), by,
               float(contracts) if contracts is not None else None, now, updated=now, parent_id=parent_id)
        self._triggers[t.id] = t
        self._emit_trigger(t)
        self._check_triggers(symbol)
        self._flush_balance()
        return self._trigger_model(t)

    # ─── state ──────────────────────────────────────────────────────────

    def drain(self) -> list[tuple[str, Any]]:
        """Accumulated events: ('orders', Order|TpSlOrder), ('fills', Fill), ('positions', Position),
        ('balance', Balance), ('funding', PaperFunding), ('liquidation', PaperLiquidation)."""
        events, self._events = self._events, []
        return events

    def order(self, symbol: str, order_id: Optional[str] = None, *, client_id: Optional[str] = None) -> Order:
        t = self._find_trigger(symbol, order_id, client_id)
        if t is not None:
            return self._trigger_model(t)
        return self._order_model(self._find(symbol, order_id, client_id))

    def orders(self, symbol: Optional[str] = None, *, open_only: bool = False) -> list[Order]:
        rows = (o for o in self._orders.values() if symbol is None or o.symbol == symbol)
        return [self._order_model(o) for o in rows if not open_only or o.status == 'open']

    def open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        books = [self._open.get(symbol, {})] if symbol else list(self._open.values())
        rows = sorted((o for b in books for o in b.values()), key=lambda o: int(o.id[1:]))
        return [self._order_model(o) for o in rows]

    def triggers(self, symbol: Optional[str] = None, *, side: Optional[str] = None,
                 open_only: bool = True) -> list[TpSlOrder]:
        return [self._trigger_model(t) for t in self._triggers.values()
                if (symbol is None or t.symbol == symbol) and (side is None or t.pos_side == side)
                and (not open_only or t.state == 'open')]

    def trigger(self, symbol: str, order_id: str) -> TpSlOrder:
        t = self._find_trigger(symbol, order_id, None)
        if t is None:
            raise OrderNotFound(f'TP/SL {order_id} not found ({symbol})', exchange=self.exchange)
        return self._trigger_model(t)

    def fills(self, symbol: Optional[str] = None) -> list[Fill]:
        return [f for f in self._fills if symbol is None or f.symbol == symbol]

    def positions(self, symbol: Optional[str] = None) -> list[Position]:
        return [self._position_model(p) for (s, _), p in self._positions.items()
                if p.contracts > EPS and (symbol is None or s == symbol)]

    def balance(self) -> Balance:
        items: dict[str, BalanceItem] = {}
        upnl: dict[str, float] = {}
        used: dict[str, float] = {}
        for p in self._positions.values():
            if p.contracts <= EPS:
                continue
            cur = self._settle_cur(p.symbol)
            upnl[cur] = upnl.get(cur, 0.0) + self._upnl(p)
            used[cur] = used.get(cur, 0.0) + p.entry * p.contracts * self._cs(p.symbol) / p.leverage
        for book in self._open.values():
            for o in book.values():
                for cur, amount in self._reserved(o):
                    used[cur] = used.get(cur, 0.0) + amount
        for cur in sorted(set(self._wallet) | set(used)):
            total = self._wallet.get(cur, 0.0) + upnl.get(cur, 0.0)
            u = used.get(cur, 0.0)
            items[cur] = BalanceItem(cur, total - u, u, total)
        return Balance(items, timestamp=self.clock(), raw={'paper': True})

    def summary(self) -> dict:
        """Account summary: realized and unrealized PnL, fees, funding, liquidations."""
        return {'realized_pnl': self.realized_pnl, 'unrealized_pnl': sum(self._upnl(p) for p in self._positions.values()
                                                                         if p.contracts > EPS),
                'fees': self.fees_paid, 'funding': self.funding_paid, 'liquidations': self.liquidations,
                'fills': len(self._fills), 'orders': len(self._orders), 'fees_source': self.fees.source,
                'fees_verified': self.fees.verified}

    def mark_price(self, symbol: str) -> Optional[float]:
        return self._mark_price(symbol)

    def best(self, symbol: str, side: str) -> Optional[float]:
        """Best opposite price: buy — ask, sell — bid; None — no book."""
        b = self._books.get(symbol)
        if b is None:
            return None
        levels = b.book.asks if side == 'buy' else b.book.bids
        return float(levels[0][0]) if levels else None

    def has_book(self, symbol: str) -> bool:
        return symbol in self._books

    def book(self, symbol: str) -> Optional[OrderBook]:
        """Latest order book snapshot (not accounting for levels consumed by own fills)."""
        b = self._books.get(symbol)
        return b.book if b is not None else None

    def last_price(self, symbol: str) -> Optional[float]:
        return self._last.get(symbol)

    # ─── matching ──────────────────────────────────────────────────────

    def _levels(self, s: str, side: str, limit: Optional[float]) -> Iterable[tuple[float, float, tuple]]:
        """Opposite levels (price, available contracts, key) up to ``limit`` inclusive."""
        b = self._books.get(s)
        if b is None:
            return
        cs = self._cs(s)
        book_side = 'ask' if side == 'buy' else 'bid'
        for price, size in (b.book.asks if side == 'buy' else b.book.bids):
            if limit is not None and (price > limit + EPS if side == 'buy' else price < limit - EPS):
                break
            key = (book_side, float(price))
            avail = (float(size) - b.consumed.get(key, 0.0)) / cs
            if avail > EPS:
                yield float(price), avail, key

    def _available(self, s: str, side: str, limit: Optional[float]) -> float:
        return sum(a for _, a, _ in self._levels(s, side, limit))

    def _crossing(self, s: str, side: str, price: Optional[float]) -> float:
        return self._available(s, side, price) if price is not None else 0.0

    def _consume(self, s: str, key: tuple, contracts: float) -> None:
        b = self._books[s]
        b.consumed[key] = b.consumed.get(key, 0.0) + contracts * self._cs(s)

    def _take(self, o: _O, limit: Optional[float]) -> None:
        """Taker fill across levels up to ``limit`` (None — market)."""
        for price, avail, key in list(self._levels(o.symbol, o.side, limit)):
            qty = self._cap(o, min(o.remaining, avail))
            if qty <= EPS:
                break
            self._consume(o.symbol, key, qty)
            self._fill(o, qty, price, maker=False)

    def _match_resting_book(self, o: _O) -> None:
        avail = self._crossing(o.symbol, o.side, o.price)
        if avail <= EPS:
            return
        qty = self._cap(o, min(o.remaining, avail))
        if qty <= EPS:
            return
        left = qty
        for _, a, key in list(self._levels(o.symbol, o.side, o.price)):
            take = min(a, left)
            self._consume(o.symbol, key, take)
            left -= take
            if left <= EPS:
                break
        self._fill(o, qty, o.price, maker=True)   # type: ignore[arg-type]
        self._settle_order(o)

    def _settle_order(self, o: _O) -> None:
        if o.status == 'open' and o.remaining <= EPS:
            o.status = 'closed'
            self._open.get(o.symbol, {}).pop(o.id, None)
        self._emit_order(o)
        self._after_fill_effects(o)

    def _cap(self, o: _O, qty: float) -> float:
        if not o.reduce_only:
            return qty
        return min(qty, self._reducible(o.symbol, o.side))

    def _after_fill_effects(self, o: _O) -> None:
        if o.filled > EPS and not o.attached_done and (o.take_profit is not None or o.stop_loss is not None):
            o.attached_done = True
            pos_side = o.pos_side or ('long' if o.side == 'buy' else 'short')
            if o.take_profit is not None:
                self.submit_trigger(o.symbol, 'take_profit', o.take_profit, position_side=pos_side,
                                    contracts=o.contracts, client_id=o.tp_cid, trigger_by=o.trigger_by,
                                    parent_id=o.id)
            if o.stop_loss is not None:
                self.submit_trigger(o.symbol, 'stop_loss', o.stop_loss, position_side=pos_side,
                                    contracts=o.contracts, client_id=o.sl_cid, trigger_by=o.trigger_by,
                                    parent_id=o.id)

    def _fill(self, o: _O, qty: float, price: float, *, maker: bool) -> Fill:
        s = o.symbol
        cs = self._cs(s)
        notional = qty * cs * price
        fee = notional * (self.fees.maker if maker else self.fees.taker)
        cur = self._fee_cur(s)
        realized = self._apply(s, o.side, qty, price, o.reduce_only, o.pos_side, o.margin_mode, fee)
        now = self.clock()
        o.filled += qty
        o.cost += notional
        o.fee += fee
        o.updated = now
        self.fees_paid += fee
        fill = Fill(f'F{next(self._fill_ids)}', o.id, o.client_id, s, o.side, price, qty * cs, fee, cur, maker,  # type: ignore[arg-type]
                    now, raw={'paper': {'realized_pnl': realized, 'liquidation': o.liquidation}}, contracts=qty,
                    position_side=o.pos_side if o.pos_side in ('long', 'short') else None)  # type: ignore[arg-type]
        self._fills.append(fill)
        self._events.append(('fills', fill))
        self._balance_dirty = True
        return fill

    # ─── positions and money ───────────────────────────────────────────────────

    def _target_side(self, side: str, reduce_only: bool) -> Optional[str]:
        if self.spot:
            return None
        if self.hedge:
            return 'long' if (side == 'buy') != reduce_only else 'short'
        return None

    def _reducible(self, s: str, side: str) -> float:
        p = self._positions.get((s, 'long' if side == 'sell' else 'short'))
        return p.contracts if p is not None else 0.0

    def _apply(self, s: str, side: str, qty: float, price: float, reduce_only: bool, pos_side: Optional[str],
               margin_mode: Optional[str], fee: float) -> float:
        if self.spot:
            m = self._market(s)
            base, quote = m.base.upper(), m.quote.upper()
            amount = qty * self._cs(s)
            sign = 1 if side == 'buy' else -1
            self._wallet[base] = self._wallet.get(base, 0.0) + sign * amount
            self._wallet[quote] = self._wallet.get(quote, 0.0) - sign * amount * price - fee
            return 0.0
        realized = 0.0
        left = qty
        reduce_side = 'long' if side == 'sell' else 'short'
        open_side = 'long' if side == 'buy' else 'short'
        hedge_open = self.hedge and not reduce_only
        if not hedge_open:   # one-way — reduce the opposite side first; hedge reduce-only — own side
            p = self._positions.get((s, reduce_side))
            if p is not None and p.contracts > EPS:
                q = min(left, p.contracts)
                pnl = (price - p.entry if reduce_side == 'long' else p.entry - price) * q * self._cs(s)
                p.realized += pnl
                p.contracts -= q
                realized += pnl
                left -= q
                self._position_changed(p)
        if left > EPS and not reduce_only:
            key = (s, open_side)
            p = self._positions.get(key)
            if p is None or p.contracts <= EPS:
                p = self._positions[key] = _P(s, open_side, leverage=self.leverage(s),
                                              margin_mode=margin_mode or 'cross', ts=self.clock())
            p.entry = (p.entry * p.contracts + price * left) / (p.contracts + left)
            p.contracts += left
            self._position_changed(p)
        cur = self._settle_cur(s)
        self._wallet[cur] = self._wallet.get(cur, 0.0) + realized - fee
        self.realized_pnl += realized
        return realized

    def _position_changed(self, p: _P) -> None:
        if p.contracts <= EPS:
            p.contracts = 0.0
            self._events.append(('positions', self._position_model(p)))
            del self._positions[(p.symbol, p.side)]
            self._on_flat(p.symbol, p.side)
        else:
            self._events.append(('positions', self._position_model(p)))

    def _on_flat(self, s: str, side: str) -> None:
        """Position closed: cancel reduce-only orders and TP/SL of that side."""
        close_side = 'sell' if side == 'long' else 'buy'
        now = self.clock()
        for o in list(self._open.get(s, {}).values()):
            if o.reduce_only and o.side == close_side:
                o.status = 'canceled'
                o.updated = now
                self._open[s].pop(o.id, None)
                self._emit_order(o)
        for t in self._triggers.values():
            if t.symbol == s and t.pos_side == side and t.state == 'open':
                t.state = 'canceled'
                t.updated = now
                self._emit_trigger(t)

    def _check_funds(self, s: str, side: str, contracts: float, price: Optional[float]) -> None:
        est = price if price is not None else self.best(s, side)
        if est is None:
            return
        cs = self._cs(s)
        if self.spot:
            m = self._market(s)
            if side == 'buy':
                cur, need = m.quote.upper(), contracts * cs * est * (1 + max(self.fees.taker, 0.0))
            else:
                cur, need = m.base.upper(), contracts * cs
        else:
            opening = contracts
            if not self.hedge:
                opening = max(contracts - self._reducible(s, side), 0.0)
            if opening <= EPS:
                return
            cur = self._settle_cur(s)
            need = opening * cs * est * (1 / self.leverage(s) + max(self.fees.taker, 0.0))
        free = self.balance()[cur].free
        if need > free + EPS:
            raise InsufficientFunds(f'paper: need {need:.8g} {cur}, free {free:.8g} ({s})',
                                    exchange=self.exchange, details={'symbol': s, 'need': need, 'free': free,
                                                                     'currency': cur})

    def _reserved(self, o: _O) -> list[tuple[str, float]]:
        if o.price is None or o.reduce_only:
            return []
        rem = o.remaining * self._cs(o.symbol)
        if self.spot:
            m = self._market(o.symbol)
            return [(m.quote.upper(), rem * o.price)] if o.side == 'buy' else [(m.base.upper(), rem)]
        return [(self._settle_cur(o.symbol), rem * o.price / self.leverage(o.symbol))]

    def _upnl(self, p: _P) -> float:
        mark = self._mark_price(p.symbol)
        if mark is None:
            return 0.0
        return (mark - p.entry if p.side == 'long' else p.entry - mark) * p.contracts * self._cs(p.symbol)

    def _liq_price(self, p: _P) -> Optional[float]:
        if not self.liquidation or p.contracts <= EPS:
            return None
        inv = 1 / p.leverage
        price = p.entry * (1 - inv + self.mmr) if p.side == 'long' else p.entry * (1 + inv - self.mmr)
        return price if price > 0 else None

    # ─── prices, triggers, liquidation ─────────────────────────────────────────

    def _mid(self, s: str) -> Optional[float]:
        b = self._books.get(s)
        return b.book.mid if b is not None else None

    def _mark_price(self, s: str) -> Optional[float]:
        return self._mark.get(s) or self._mid(s) or self._last.get(s)

    def _ref(self, s: str, by: str) -> Optional[float]:
        if by == 'last':
            return self._last.get(s) or self._mid(s)
        return self._mark_price(s)

    def _after_price(self, s: str) -> None:
        self._check_triggers(s)
        self._check_liquidation(s)
        self.tick()

    def _check_triggers(self, s: str) -> None:
        for t in list(self._triggers.values()):
            if t.symbol != s or t.state != 'open':
                continue
            ref = self._ref(s, t.trigger_by)
            if ref is None:
                continue
            up = (t.kind == 'take_profit') == (t.pos_side == 'long')
            if (ref >= t.trigger_price) if up else (ref <= t.trigger_price):
                self._fire(t, ref)

    def _fire(self, t: _T, ref: float) -> None:
        close_side = 'sell' if t.pos_side == 'long' else 'buy'
        pos = self._positions.get((t.symbol, t.pos_side))
        t.updated = self.clock()
        if pos is None or pos.contracts <= EPS:
            t.state = 'canceled'
            self._emit_trigger(t)
            return
        t.state = 'triggered'
        qty = min(t.contracts if t.contracts is not None else pos.contracts, pos.contracts)
        o = _O(t.id, t.client_id, t.symbol, close_side, 'market', None, qty, t.ts, reduce_only=True,
               pos_side=t.pos_side if self.hedge else None, updated=t.updated)
        self._take(o, None)
        left = self._cap(o, o.remaining)
        if left > EPS:   # the book was not enough — the rest at the trigger price (simplification)
            self._fill(o, left, ref, maker=False)
        t.filled, t.cost, t.fee = o.filled, o.cost, o.fee
        self._emit_trigger(t)

    def _check_liquidation(self, s: str) -> None:
        mark = self._mark_price(s)
        if mark is None:
            return
        for side in ('long', 'short'):
            p = self._positions.get((s, side))
            liq = self._liq_price(p) if p is not None else None
            if p is None or liq is None:
                continue
            if (mark <= liq) if side == 'long' else (mark >= liq):
                self._liquidate(p, mark)

    def _liquidate(self, p: _P, mark: float) -> None:
        now = self.clock()
        qty = p.contracts
        o = _O(f'P{next(self._ids)}', None, p.symbol, 'sell' if p.side == 'long' else 'buy', 'market', None, qty,
               now, reduce_only=True, pos_side=p.side if self.hedge else None, updated=now, liquidation=True)
        self._orders[o.id] = o
        self._fill(o, qty, mark, maker=False)
        o.status = 'closed'
        self.liquidations += 1
        self._emit_order(o)
        model = self._order_model(o)
        self._events.append(('liquidation', PaperLiquidation(p.symbol, p.side, mark, qty, model, now)))

    # ─── models ─────────────────────────────────────────────────────────────

    def _cs(self, s: str) -> float:
        m = self._market(s)
        return float(m.contract_size or 1.0) if m.is_contract else 1.0

    def _settle_cur(self, s: str) -> str:
        m = self._market(s)
        return (m.settle or m.quote).upper()

    def _fee_cur(self, s: str) -> str:
        m = self._market(s)
        return m.quote.upper() if self.spot else (m.settle or m.quote).upper()

    def _find(self, s: str, order_id: Optional[str], client_id: Optional[str]) -> _O:
        if order_id is not None:
            o = self._orders.get(str(order_id))
            if o is not None and o.symbol == s:
                return o
        elif client_id:
            found = [o for o in self._orders.values() if o.symbol == s and o.client_id == client_id]
            if found:
                open_ = [o for o in found if o.status == 'open']
                return (open_ or found)[-1]
        else:
            raise ConfigError(f'order_id or client_id is required ({s})')
        raise OrderNotFound(f'order {order_id or client_id} not found ({s})', exchange=self.exchange,
                            details={'symbol': s, 'order_id': order_id, 'client_id': client_id})

    def _find_trigger(self, s: str, order_id: Optional[str], client_id: Optional[str]) -> Optional[_T]:
        if order_id is not None:
            t = self._triggers.get(str(order_id))
            return t if t is not None and t.symbol == s else None
        if client_id:
            found = [t for t in self._triggers.values() if t.symbol == s and t.client_id == client_id]
            return found[-1] if found else None
        return None

    def _emit_order(self, o: _O) -> None:
        self._events.append(('orders', self._order_model(o)))

    def _emit_trigger(self, t: _T) -> None:
        self._events.append(('orders', self._trigger_model(t)))

    def _flush_balance(self) -> None:
        if self._balance_dirty:
            self._balance_dirty = False
            self._events.append(('balance', self.balance()))

    def _order_model(self, o: _O) -> Order:
        cs = self._cs(o.symbol)
        avg = o.cost / (o.filled * cs) if o.filled > EPS else None
        paper: dict[str, Any] = {'liquidation': o.liquidation}
        if o.liquidation:
            paper['liquidation_model'] = LIQUIDATION_MODEL
        return Order(id=o.id, client_id=o.client_id, symbol=o.symbol, side=o.side, type=o.type,  # type: ignore[arg-type]
                     status=o.status, price=o.price, average=avg, size=o.contracts * cs,  # type: ignore[arg-type]
                     filled=o.filled * cs, remaining=o.remaining * cs, reduce_only=o.reduce_only,
                     post_only=o.post_only, time_in_force=o.tif, trigger_price=None, fee=o.fee,
                     fee_currency=self._fee_cur(o.symbol), timestamp=o.ts,
                     raw={'paper': paper, 'lastUpdateTimestamp': o.updated},
                     contracts=o.contracts, filled_contracts=o.filled, remaining_contracts=o.remaining)

    def _trigger_model(self, t: _T) -> TpSlOrder:
        cs = self._cs(t.symbol)
        pos = self._positions.get((t.symbol, t.pos_side))
        contracts = t.contracts if t.contracts is not None else (t.filled or (pos.contracts if pos else 0.0))
        status = {'open': 'open', 'triggered': 'closed', 'canceled': 'canceled'}[t.state]
        return TpSlOrder(id=t.id, client_id=t.client_id, symbol=t.symbol,
                         side='sell' if t.pos_side == 'long' else 'buy',
                         type='take_profit_market' if t.kind == 'take_profit' else 'stop_market',
                         status=status, price=None,  # type: ignore[arg-type]
                         average=t.cost / (t.filled * cs) if t.filled > EPS else None, size=contracts * cs,
                         filled=t.filled * cs, remaining=max(contracts - t.filled, 0.0) * cs, reduce_only=True,
                         post_only=False, time_in_force=None, trigger_price=t.trigger_price, fee=t.fee,
                         fee_currency=self._fee_cur(t.symbol), timestamp=t.ts,
                         raw={'paper': {'trigger_by': t.trigger_by, 'parent_id': t.parent_id},
                              'lastUpdateTimestamp': t.updated},
                         contracts=contracts, filled_contracts=t.filled,
                         remaining_contracts=max(contracts - t.filled, 0.0), kind=t.kind, position_side=t.pos_side,
                         take_profit=t.trigger_price if t.kind == 'take_profit' else None,
                         stop_loss=t.trigger_price if t.kind == 'stop_loss' else None, state=t.state)

    def _position_model(self, p: _P) -> Position:
        cs = self._cs(p.symbol)
        mark = self._mark_price(p.symbol)
        notional = p.contracts * cs * (mark if mark is not None else p.entry)
        margin = p.entry * p.contracts * cs / p.leverage
        return Position(symbol=p.symbol, side=p.side, size=p.contracts * cs, contracts=p.contracts,  # type: ignore[arg-type]
                        entry_price=p.entry or None, mark_price=mark, liquidation_price=self._liq_price(p),
                        leverage=p.leverage, margin_mode=p.margin_mode, unrealized_pnl=self._upnl(p),
                        realized_pnl=p.realized, notional=notional, margin=margin, timestamp=self.clock(),
                        raw={'paper': {'liquidation_model': LIQUIDATION_MODEL, 'funding': p.funding,
                                       'opened_at': p.ts}},
                        hedged=self.hedge)


__all__ = ['PaperEngine', 'PaperRequest', 'PaperFunding', 'PaperLiquidation', 'LIQUIDATION_MODEL']
