"""Typed response models of :class:`bytx.Exchange`.

Amounts (``size``, ``filled``…) are always in the BASE coin, even on derivatives, where
the exchange counts in contracts. The contract count is separate (``contracts``, ``filled_contracts``,
``remaining_contracts``). Models have no unitless ``amount`` field.
The original response is in ``raw``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

Side = Literal['buy', 'sell']
PositionSide = Literal['long', 'short']
OrderStatus = Literal['open', 'closed', 'canceled', 'expired', 'rejected']
MarketType = Literal['spot', 'swap', 'future']
MarginMode = Literal['cross', 'isolated']
Level = tuple[float, float]  # (price, amount in base coin)


def _bps(spread: Optional[float], mid: Optional[float]) -> Optional[float]:
    if spread is None or not mid:
        return None
    return spread / mid * 10_000


@dataclass(slots=True)
class Market:
    symbol: str
    id: str
    base: str
    quote: str
    settle: Optional[str]
    type: str
    contract_size: float
    amount_step: Optional[float]
    price_step: Optional[float]
    min_amount: Optional[float]
    min_cost: Optional[float]
    max_leverage: Optional[float]
    maker_fee: Optional[float]
    taker_fee: Optional[float]
    active: bool
    raw: dict = field(repr=False, default_factory=dict)

    @property
    def is_contract(self) -> bool:
        return self.type != 'spot'


@dataclass(slots=True)
class Ticker:
    symbol: str
    bid: Optional[float]
    ask: Optional[float]
    last: Optional[float]
    mark: Optional[float] = None
    index: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    base_volume: Optional[float] = None
    quote_volume: Optional[float] = None
    change_pct: Optional[float] = None
    timestamp: Optional[int] = None
    raw: dict = field(repr=False, default_factory=dict)

    @property
    def mid(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> Optional[float]:
        if self.bid is None or self.ask is None:
            return None
        return self.ask - self.bid

    @property
    def spread_bps(self) -> Optional[float]:
        return _bps(self.spread, self.mid)


@dataclass(slots=True)
class OrderBook:
    symbol: str
    bids: list[Level]
    asks: list[Level]
    timestamp: Optional[int] = None
    nonce: Optional[int] = None

    @property
    def best_bid(self) -> Optional[Level]:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[Level]:
        return self.asks[0] if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2

    @property
    def spread(self) -> Optional[float]:
        if not self.bids or not self.asks:
            return None
        return self.asks[0][0] - self.bids[0][0]

    @property
    def spread_bps(self) -> Optional[float]:
        return _bps(self.spread, self.mid)

    @property
    def microprice(self) -> Optional[float]:
        """Price weighted by top-level sizes: closer to the side with the smaller size
        (where price is more likely to move). (bid·ask_size + ask·bid_size) / (bid_size + ask_size)."""
        if not self.bids or not self.asks:
            return None
        (bid, bid_size), (ask, ask_size) = self.bids[0], self.asks[0]
        total = bid_size + ask_size
        return (bid + ask) / 2 if total == 0 else (bid * ask_size + ask * bid_size) / total

    def imbalance(self, levels: int = 5) -> Optional[float]:
        """(bid_vol - ask_vol) / (bid_vol + ask_vol) over the first ``levels`` levels, from -1 to 1."""
        bid_vol = sum(size for _, size in self.bids[:levels])
        ask_vol = sum(size for _, size in self.asks[:levels])
        total = bid_vol + ask_vol
        return None if total == 0 else (bid_vol - ask_vol) / total

    def depth(self, side: Side, pct: float) -> float:
        """Amount (base) within ``pct`` percent of the best price on one side of the book.

        side='buy' — ask volume (what can be bought), 'sell' — bid volume.
        """
        levels = self.asks if side == 'buy' else self.bids
        if not levels:
            return 0.0
        best = levels[0][0]
        limit = best * (1 + pct / 100) if side == 'buy' else best * (1 - pct / 100)
        inside = (lambda p: p <= limit) if side == 'buy' else (lambda p: p >= limit)
        return sum(size for price, size in levels if inside(price))

    def fill_price(self, side: Side, *, size: Optional[float] = None,
                   notional: Optional[float] = None) -> Optional[float]:
        """Average fill price of a market order for ``size`` base or ``notional`` quote.

        None — not enough liquidity in the book.
        """
        if (size is None) == (notional is None):
            raise ValueError('exactly one of size / notional is required')
        levels = self.asks if side == 'buy' else self.bids
        need = float(size if size is not None else notional)  # type: ignore[arg-type]
        if need <= 0:
            raise ValueError('size / notional must be > 0')
        remaining = need
        base = cost = 0.0
        for price, amount in levels:
            if size is not None:
                take = min(amount, remaining)
                base += take
                cost += take * price
            else:
                take = min(amount * price, remaining)
                base += take / price
                cost += take
            remaining -= take
            if remaining <= need * 1e-12:
                return cost / base
        return None


@dataclass(slots=True)
class Trade:
    """Public trade from the tape."""

    id: Optional[str]
    symbol: str
    side: Optional[Side]
    price: float
    size: float
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)

    @property
    def cost(self) -> float:
        return self.price * self.size


@dataclass(slots=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(slots=True)
class BalanceItem:
    currency: str
    free: float
    used: float
    total: float


@dataclass(slots=True)
class Balance:
    items: dict[str, BalanceItem]
    timestamp: Optional[int] = None
    raw: dict = field(repr=False, default_factory=dict)

    def __getitem__(self, currency: str) -> BalanceItem:
        return self.items.get(currency.upper()) or BalanceItem(currency.upper(), 0.0, 0.0, 0.0)

    def __contains__(self, currency: str) -> bool:
        return currency.upper() in self.items

    def free(self, currency: str) -> float:
        return self[currency].free

    def total(self, currency: str) -> float:
        return self[currency].total


@dataclass(slots=True)
class Position:
    """Position. ``side`` — 'long'/'short', also in hedge mode on all exchanges; None — only
    for a zero one-way position (``positions(include_zero=True)``). ``hedged`` — hedge-mode position
    (None — the exchange does not report it). ``margin_mode`` — 'cross'/'isolated' (if the exchange reports it),
    ``raw`` — ccxt response (``raw['info']`` — the exchange's)."""

    symbol: str
    side: Optional[PositionSide]
    size: float
    contracts: float
    entry_price: Optional[float]
    mark_price: Optional[float]
    liquidation_price: Optional[float]
    leverage: Optional[float]
    margin_mode: Optional[str]
    unrealized_pnl: Optional[float]
    realized_pnl: Optional[float]
    notional: Optional[float]
    margin: Optional[float]
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)
    hedged: Optional[bool] = None

    @property
    def pnl_pct(self) -> Optional[float]:
        if self.unrealized_pnl is None or not self.margin:
            return None
        return self.unrealized_pnl / self.margin * 100


@dataclass(slots=True)
class Order:
    id: str
    client_id: Optional[str]
    symbol: str
    side: Optional[Side]
    type: str
    status: Optional[OrderStatus]
    price: Optional[float]
    average: Optional[float]
    size: Optional[float]
    filled: Optional[float]
    remaining: Optional[float]
    reduce_only: Optional[bool]
    post_only: Optional[bool]
    time_in_force: Optional[str]
    trigger_price: Optional[float]
    fee: Optional[float]
    fee_currency: Optional[str]
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)
    # the same amounts in exchange units (contracts; on spot = base).
    # size / filled / remaining = contracts / filled_contracts / … × Market.contract_size
    contracts: Optional[float] = None
    filled_contracts: Optional[float] = None
    remaining_contracts: Optional[float] = None

    @property
    def is_open(self) -> bool:
        return self.status == 'open'

    @property
    def is_filled(self) -> bool:
        return self.status == 'closed'


@dataclass(slots=True)
class Fill:
    """Own trade (order fill)."""

    id: Optional[str]
    order_id: Optional[str]
    client_id: Optional[str]
    symbol: str
    side: Optional[Side]
    price: float
    size: float
    fee: Optional[float]
    fee_currency: Optional[str]
    maker: Optional[bool]
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)
    contracts: Optional[float] = None  # size in exchange units


@dataclass(slots=True)
class FundingRate:
    symbol: str
    rate: Optional[float]
    next_funding_time: Optional[int]
    mark_price: Optional[float]
    index_price: Optional[float]
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)


@dataclass(slots=True)
class FundingPayment:
    symbol: str
    rate: float
    timestamp: int


@dataclass(slots=True)
class OpenInterest:
    symbol: str
    size: Optional[float]
    value: Optional[float]
    timestamp: Optional[int]
    raw: dict = field(repr=False, default_factory=dict)


@dataclass(slots=True)
class TradingFee:
    symbol: str
    maker: Optional[float]
    taker: Optional[float]


@dataclass(slots=True)
class LeverageTier:
    """Risk tier: leverage and maintenance margin rate for a position range.

    Tier bound — in quote notional (``min_notional`` / ``max_notional``: Binance, Bybit,
    KuCoin, Hyperliquid) or in base (``min_size`` / ``max_size``: OKX — ``minSz``/``maxSz`` in
    contracts × ctVal). What the exchange does not report — None. ``raw`` — the exchange record.
    """

    tier: int
    max_leverage: Optional[float]
    maintenance_margin_rate: Optional[float]
    min_notional: Optional[float] = None
    max_notional: Optional[float] = None
    min_size: Optional[float] = None
    max_size: Optional[float] = None
    source: str = 'exchange'
    raw: dict = field(repr=False, default_factory=dict)


@dataclass(slots=True)
class Transfer:
    id: Optional[str]
    currency: str
    amount: float
    from_account: str
    to_account: str
    status: Optional[str]
    raw: dict = field(repr=False, default_factory=dict)


CancelStatus = Literal['canceled', 'already_closed', 'error']


@dataclass(slots=True)
class CancelResult:
    """Outcome of cancelling one order.

    ``status``: ``'canceled'`` — we cancelled it; ``'already_closed'`` — the order was no longer in the book
    (``order`` — its final state from the exchange, if known); ``'error'`` — not cancelled,
    reason in ``error``. ``kind``: ``'order'`` — regular order, ``'tp_sl'`` — conditional TP/SL.
    """

    id: Optional[str]
    symbol: str
    status: CancelStatus
    client_id: Optional[str] = None
    kind: str = 'order'
    order: Optional[Order] = None
    error: Optional[Exception] = None


@dataclass(slots=True)
class CancelReport:
    """Report of :meth:`Exchange.cancel_all`: outcome per order."""

    results: list[CancelResult] = field(default_factory=list)

    @property
    def canceled(self) -> list[CancelResult]:
        return [r for r in self.results if r.status == 'canceled']

    @property
    def already_closed(self) -> list[CancelResult]:
        return [r for r in self.results if r.status == 'already_closed']

    @property
    def errors(self) -> list[CancelResult]:
        return [r for r in self.results if r.status == 'error']

    @property
    def ok(self) -> bool:
        """No errors: everything cancelled or already closed."""
        return not self.errors

    def by_id(self) -> dict[Optional[str], CancelResult]:
        return {r.id: r for r in self.results}


@dataclass(slots=True)
class FlattenReport:
    """Report of :meth:`Exchange.flatten` by step.

    ``orders`` — cancelling own limit orders, ``tp_sl`` — own TP/SL, ``close`` — the close order
    (None — there was no position or it closed on its own), ``flat`` — position confirmed closed,
    ``errors`` — {step: error} (``'orders'`` / ``'tp_sl'`` / ``'close'`` / ``'wait'``).
    """

    symbol: str
    side: str
    orders: CancelReport = field(default_factory=CancelReport)
    tp_sl: CancelReport = field(default_factory=CancelReport)
    close: Optional[Order] = None
    flat: bool = False
    errors: dict[str, Exception] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.flat and not self.errors


__all__ = [
    'Side', 'PositionSide', 'OrderStatus', 'MarketType', 'MarginMode', 'Level',
    'Market', 'Ticker', 'OrderBook', 'Trade', 'Candle', 'BalanceItem', 'Balance', 'Position',
    'Order', 'Fill', 'FundingRate', 'FundingPayment', 'OpenInterest', 'TradingFee', 'Transfer', 'LeverageTier',
    'CancelStatus', 'CancelResult', 'CancelReport', 'FlattenReport',
]
