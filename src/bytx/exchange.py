"""Public async exchange client.

    async with create_exchange('okx', api_key=..., secret=..., password=..., demo=True) as ex:
        book = await ex.order_book('BTC/USDT')
        order = await ex.buy('BTC/USDT', notional=50, price=book.best_bid[0], post_only=True)

Sizes are in the base coin (``size``), in the quote (``notional``) or in contracts
(``contracts``); the SDK converts to exchange contracts, rounds and checks minimums itself.
"""
from __future__ import annotations

import asyncio
import contextlib
import importlib
import inspect
import json
import math
import time
import weakref
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Literal, Optional, TypeVar, Union, cast

import ccxt

from bytx import batch_cancel as _batch_cancel, convert, sizing
from bytx.net._sig.opts import BrokerConfig
from bytx.exchanges.binance.ccxt_fixes import algo_as_order as binance_algo_as_order
from bytx.exchanges.gate.leverage import leverage_params as gate_leverage_params
from bytx.events import ExchangeEvent, Observers, check_kinds, request_view
from bytx.errors import (
    BadSymbol,
    BatchError,
    BytxError,
    ConfigError,
    ExchangeError,
    InsufficientFunds,
    InvalidOrder,
    NetworkError,
    NotSupported,
    OrderNotFound,
    OrderStatusUnknown,
    OrderTooSmall,
    PositionAlreadyClosed,
    PositionCovered,
    PositionOpenError,
    ProtectionNotPlaced,
    RateLimited,
    ReduceExceedsPosition,
    RequestTimeout,
    TimestampError,
    UnknownOutcome,
    ccxt_secrets,
    from_ccxt,
)
from bytx.markets import ensure_markets, to_market
from bytx.outcome import classify as classify_outcome, order_not_open, position_error, refine as refine_error
from bytx.net import MetricsSnapshot, NetLayer, deadline as net_deadline, deadline_left
from bytx.net.timesync import SHARED_MAX_AGE_SEC
from bytx.order_params import (
    cancel_client_id_params,
    EXACT_CLIENT_ID_LOOKUP,
    MY_TRADES_ORDER_FILTER,
    MY_TRADES_SPOT_SPAN,
    MY_TRADES_WINDOW,
    check_client_id_prefix,
    amend_client_id,
    client_id_params,
    generate_client_id,
    owns,
    position_params,
)
from bytx import protection as _protection
from bytx.protection import DeadManSwitch, RiskGuard, TpSlModifyResult, TpSlOrder, TriggerBy
from bytx.trackers import OrderTracker, PositionTracker
from bytx.transport import EXCHANGES, DemoArg, Proxy, build_ccxt, demo_env
from bytx.types import (
    Balance,
    CancelReport,
    CancelResult,
    Candle,
    FlattenReport,
    Fill,
    FundingPayment,
    FundingRate,
    MarginMode,
    Market,
    OpenInterest,
    Order,
    OrderBook,
    Position,
    PositionSide,
    Side,
    Ticker,
    Trade,
    LeverageTier,
    TradingFee,
    Transfer,
)

if TYPE_CHECKING:
    from bytx.journal import PositionJournal
    from bytx.journal.origin import CloseVerdict
    from bytx.paper.exchange import PaperConfig
    from bytx.data import HistoryLoader
    from bytx.health import HealthReport
    from bytx.key_check import KeyInfo
    from bytx.portfolio import AccountReport, FundingPaid
    from bytx.recovery import IntentStore, RecoveryReport
    from bytx.risk_sizing import OpenPositionResult, RiskPlan

T = TypeVar('T')
_TIF = {'GTC', 'IOC', 'FOK', 'PO'}
_NO_FOK = frozenset({'hyperliquid', 'kucoinfutures'})       # no FOK per docs
_PO_AS_FLAG = frozenset({'hyperliquid', 'kucoinfutures'})   # PO is not a TIF but postOnly
_INT_LEVERAGE = frozenset({'binance', 'bingx', 'hyperliquid', 'kucoinfutures', 'mexc', 'htx'})
# no TP/SL attached to the order: placed as separate conditional orders
_SEPARATE_TP_SL = frozenset({'binance', 'hyperliquid', 'gate'})
# Margin mode is sent with every order (OKX tdMode, KuCoin marginMode), so it must be explicit.
# Bitget marginMode is required in Place Order; MEXC openType is required; on HTX the mode selects the endpoint
# s: price used only for the minimum cost check (min_cost) before sending — from a cache, not a ticker per order
_CHECK_PRICE_TTL = 5.0
# s: how long to wait for a market order fill from the ``ex.stream.orders()`` stream before reading it over REST
_WS_FILL_WAIT = 0.5
_MODE_IN_ORDER = frozenset({'okx', 'kucoinfutures', 'bitget', 'mexc', 'htx', 'blofin'})
# Leverage is sent with the order (KuCoin leverage, MEXC leverage "must be provided when opening", HTX lever_rate is
# required and equals the open position's leverage); set_leverage only stores the value, so does set_margin_mode,
# except on KuCoin: there the mode also belongs to the symbol (Switch Margin Mode, exchanges/kucoin/margin_mode.py)
_LEVERAGE_IN_ORDER = frozenset({'kucoinfutures', 'mexc', 'htx'})
# Exchanges with a single position mode (one-way) where hedge is impossible.
# Gate dual mode (close via auto_size / dual_side) is not yet supported by the SDK: one-way only.
_ONE_WAY_ONLY = {'hyperliquid', 'kucoinfutures', 'gate'}
# The SDK trades one-way only, but the exchange has hedge (KuCoin Futures since 2025.09): the account mode is read
# from the exchange before the first order; hedge raises ConfigError (only the user can switch the mode)
_ONE_WAY_CHECKED = {'kucoinfutures'}
# set_leverage(side=): exchanges where leverage is set for one side of a hedge position (Set leverage docs) —
# OKX posSide (isolated, long/short), BingX side LONG/SHORT, Bybit buyLeverage/sellLeverage
_SIDE_LEVERAGE = frozenset({'okx', 'bingx', 'bybit', 'blofin'})


def _leverage_str(value: float) -> str:
    """Leverage as a string without trailing zeros: 10.0 → '10', 2.5 → '2.5' (Bybit buyLeverage/sellLeverage — String)."""
    return f'{float(value):.10g}'


def _check_margin_mode(mode: Any, exchange: Optional[str]) -> MarginMode:
    """'cross' / 'isolated' (case-insensitive), otherwise ConfigError."""
    value = mode.lower() if isinstance(mode, str) else mode
    if value not in ('cross', 'isolated'):
        raise ConfigError(f"margin_mode: 'cross' or 'isolated', not {mode!r}", exchange=exchange,
                          details={'margin_mode': mode})
    return cast(MarginMode, value)


def _applied_leverage(exchange_id: str, response: Any) -> Optional[float]:
    """Applied leverage from the set_leverage response per exchange docs; None if the exchange does not report it."""
    if exchange_id == 'binance':
        from bytx.exchanges.binance.leverage import applied_leverage
    elif exchange_id == 'okx':
        from bytx.exchanges.okx.leverage import applied_leverage
    elif exchange_id == 'bingx':
        from bytx.exchanges.bingx.leverage import applied_leverage
    elif exchange_id == 'bitget':
        from bytx.exchanges.bitget.leverage import applied_leverage
    elif exchange_id == 'gate':
        from bytx.exchanges.gate.leverage import applied_leverage
    elif exchange_id == 'blofin':
        from bytx.exchanges.blofin.leverage import applied_leverage
    else:
        return None
    return applied_leverage(response)


def _ccxt_amount(request: dict) -> float:
    """Amount for ccxt create_order. A spot buy for a quote amount without a price (amount None): the amount — ccxt
    sends it (``params['cost']``: Binance quoteOrderQty, Bybit qty+marketUnit, OKX sz+tgtCcy, KuCoin funds), and
    the amount only passes its rounding."""
    amount = request['amount']
    return float(request['params']['cost']) if amount is None else amount


class Exchange:
    """Async client for one exchange and one account (not bound to a symbol)."""

    #: True on :class:`bytx.paper.PaperExchange` (orders are filled by the paper engine)
    is_paper = False

    @classmethod
    def paper(cls, name: str, **kwargs: Any) -> 'Exchange':
        """Paper account on the live exchange order book: ``create_exchange(name, paper=True, **kwargs)``."""
        return create_exchange(name, paper=True, **kwargs)

    def __init__(self, ccxt_exchange: Any, *, name: str, market_type: str = 'swap',
                 demo: DemoArg = False, proxy: Optional[Proxy] = None,
                 margin_mode: Optional[MarginMode] = None, hedge: Optional[bool] = None,
                 client_id_prefix: Optional[str] = None, client_id_strict: bool = False,
                 pre_submit: Optional[Callable[[dict], Any]] = None,
                 journal: Optional['PositionJournal'] = None,
                 intents: Optional['IntentStore'] = None) -> None:
        self._ex = ccxt_exchange
        self.name = name
        # bot tag at the start of every generated client_id
        self.client_id_prefix = check_client_id_prefix(ccxt_exchange.id, client_id_prefix, market_type)
        # a client_id that does not fit in 36 with the Binance broker prefix raises
        # InvalidOrder before sending (False: the cid is deterministically shortened, the prefix is kept)
        self.client_id_strict = bool(client_id_strict)
        # hook before the placement network call (see create_exchange)
        self.pre_submit = pre_submit
        # position journal (opt-in). None: nothing is written (a single check at the write point)
        if journal is not None:
            journal.tag_for(self.client_id_prefix)   # journal tag ≠ client_id_prefix raises ConfigError right away
        self.journal = journal
        # intent store (opt-in): the order is written before sending, the outcome after; None: nothing
        self.intents = intents
        # Binance: conditional orders live in the Algo API (own algoId); remember those created here
        # so that order/cancel_order go to /fapi/v1/algoOrder instead of /fapi/v1/order
        self._binance_algo_ids: set[str] = set()
        # symbol → (price, time.monotonic()): a fresh ticker or the average of a filled market order (_CHECK_PRICE_TTL)
        self._check_prices: dict[str, tuple[float, float]] = {}
        self.market_type = market_type
        # demo: bool (as before); environment via demo_env: None (live), 'demo' or 'testnet' (Binance only)
        self.demo_env: Optional[str] = demo_env(name, demo)
        self.demo = self.demo_env is not None
        self.proxy = proxy
        # default margin mode; set_margin_mode(symbol) changes the mode of one symbol.
        # None means not set: where the mode goes into the request (OKX, KuCoin, OKX/HL leverage),
        # missing mode raises ConfigError instead of a silent 'cross'
        self.margin_mode: Optional[MarginMode] = (
            _check_margin_mode(margin_mode, name) if margin_mode is not None else None)
        self._margin_modes: dict[str, MarginMode] = {}
        if ccxt_exchange.id == 'htx':
            # HTX: order/position endpoint depends on the symbol's margin mode (exchanges/htx/ccxt_fixes)
            ccxt_exchange.bytx_margin = lambda: (self.margin_mode, self._margin_modes)
        # None: position mode is fetched from the exchange when first needed
        self._hedge: Optional[bool] = (
            None if ccxt_exchange.id in _ONE_WAY_CHECKED else False if ccxt_exchange.id in _ONE_WAY_ONLY else hedge)
        # Bybit: position mode is per symbol (symbol > coin > default), cached per symbol; '' means no symbol
        self._hedge_by_symbol: dict[str, bool] = {}
        # BloFin: Multi-Position on the account (seen in connect) — every order is refused
        self._blofin_mode_error: Optional[ConfigError] = None
        self._leverage: dict[str, float] = {}
        self._connected = False
        self._close_gen = 0      # bumped on every close(): a connect started before close does not revive background tasks
        # shared per-IP limiter, retries, metrics, time sync (configured by create_exchange)
        self._net = NetLayer(ccxt_exchange, proxy=proxy)
        # running trackers (OrderTracker / PositionTracker): live state without REST
        self._trackers: 'weakref.WeakSet[Any]' = weakref.WeakSet()
        # event listeners (ex.subscribe); empty means no events are created
        self._observers = Observers()
        self._stream_state_unsub: Optional[Callable[[], None]] = None
        #: account name (AccountPool sets its own), for notifications and metrics
        self.account: Optional[str] = None

    # ─── lifecycle ──────────────────────────────────────────────────────────

    @property
    def ccxt(self) -> Any:
        """Raw ccxt.pro instance (escape hatch; broker tags are already set on it)."""
        return self._ex

    @property
    def id(self) -> str:
        return self._ex.id

    async def connect(self) -> 'Exchange':
        """Warm-up: markets from the shared cache and time sync. Called lazily.

        After :meth:`close` you can connect again: warm-up and background time
        sync restart (``ex.stream`` WS streams stay closed).
        If ``close()`` is called while connect is still running, raises :class:`ConfigError`.
        """
        if self._connected:
            return self
        gen = self._close_gen
        await self._call(ensure_markets, self._ex)
        self._check_not_closed(gen)
        # markets from the shared cache, without a single REST request from this instance: ccxt has not called open(), and
        # asyncio_loop/session are empty. A WS client created in that state gets loop=None:
        # demo Bybit 2026-10-26: the process's second Exchange received neither pong nor auth reply,
        # the private stream was silent. open() is idempotent (loop, SSL, aiohttp session)
        opener = getattr(self._ex, 'open', None)
        if callable(opener):
            opener()
        await self._net_warmup()  # time sync + background resync
        if self._ex.id == 'hyperliquid':
            # key is an API wallet: info requests use the main account address (otherwise balance and positions are empty)
            from bytx.exchanges.hyperliquid.ccxt_fixes import resolve_master_address
            await resolve_master_address(self._ex)
        if self._ex.id == 'bybit' and self._ex.apiKey:
            # account type (UTA): ccxt learns it with two requests (query-api, account/info) on the first trading
            # call — done during warm-up; if it fails, ccxt retries there as before
            try:
                await self._call(self._ex.is_unified_enabled)
            except BytxError as e:
                from loguru import logger
                logger.debug(f'{self.name}: Bybit account type not learned during warm-up: {e.message or e}')
        if self._ex.id == 'blofin' and self._ex.apiKey:
            # key type (query-apikey) decides whether brokerId is sent; on failure the type is unknown, brokerId is sent
            from bytx.exchanges.blofin.api_key import load_key_type
            await load_key_type(self)
        if self._ex.id == 'blofin' and self._ex.apiKey and self.market_type != 'spot':   # position mode — futures only
            from bytx.exchanges.blofin.leverage import position_mode as blofin_position_mode
            # account mode during warm-up: Multi-Position is remembered and refuses every order (also with hedge
            # given to create_exchange); reads (balance, positions) work
            try:
                hedge = await blofin_position_mode(self)
                if self._hedge is None:
                    self._hedge = hedge
            except ConfigError as e:
                self._blofin_mode_error = e
            except BytxError:
                pass   # not read: is_hedge_mode asks before the order
        if gen != self._close_gen:
            await self._net.close()  # close() already ran: do not leave background tasks
            self._check_not_closed(gen)
        self._connected = True
        return self

    def _check_not_closed(self, gen: int) -> None:
        if gen != self._close_gen:
            raise ConfigError('exchange.close() called during connect()', exchange=self.name)

    async def close(self) -> None:
        """Stops WS streams and background time sync, closes ccxt sessions."""
        self._connected = False   # the next connect() warms up and starts time sync again
        self._close_gen += 1
        if getattr(self, '_streams', None) is not None:  # stop WS streams
            await self._streams.close()
        await self._net.close()  # stop background time sync
        await self._ex.close()

    async def __aenter__(self) -> 'Exchange':
        return await self.connect()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __repr__(self) -> str:
        mode = 'demo' if self.demo else 'live'
        return f'<Exchange {self.name} {self.market_type} {mode} proxy={self.proxy!r}>'

    async def _call(self, fn: Callable[..., Awaitable[T]], *args: Any, safe: bool = True,
                    **kwargs: Any) -> T:
        """ccxt call: errors → BytxError; ``safe`` (idempotent) calls are retried.

        ``safe=False``: order placement/amendment and transfers, no retries (see bytx.net.retry).
        """
        return await self._net.call(fn, args, kwargs, safe=safe, exchange=self.name)

    # ─── events for integrations ──────────────────────────────────

    def subscribe(self, callback: Callable[[ExchangeEvent], Any], *,
                  kinds: Optional[Iterable[str]] = None) -> Callable[[], None]:
        """Event listener (:mod:`bytx.events`): order placement/rejection/cancellation, TP/SL,
        WS stream state, integration errors. ``kinds``: kinds or groups ('order', 'stream');
        an unknown one raises ConfigError. The callback is sync or async; its exceptions are logged and
        never break trading. Returns an unsubscribe function. Without listeners no events are created."""
        wanted = check_kinds(kinds)
        remove = self._observers.add(callback, wanted)
        if self._stream_state_unsub is None and self._observers.wants('stream.state'):
            self._stream_state_unsub = self.stream.on_state(self._on_stream_state)

        def unsubscribe() -> None:
            remove()
            if self._stream_state_unsub is not None and not self._observers.wants('stream.state'):
                self._stream_state_unsub()
                self._stream_state_unsub = None

        return unsubscribe

    def emit(self, kind: str, *, symbol: Optional[str] = None, order: Any = None,
             error: Optional[BaseException] = None, state: Any = None, data: Optional[dict] = None) -> None:
        """Sends an event to ``subscribe`` listeners (this is how integrations report their errors: ``'error'``)."""
        if not self._observers:
            return
        self._observers.dispatch(ExchangeEvent(
            kind=kind, exchange=self.name, account=self.account, demo=bool(self.demo), paper=self.is_paper,
            symbol=symbol, order=order, error=error, state=state, data=data or {}))

    def _on_stream_state(self, ev: Any) -> None:
        self.emit('stream.state', symbol=ev.symbol, state=ev, error=ev.error)

    def _emit_failed(self, request: dict, error: BaseException) -> None:
        kind = 'order.unknown' if isinstance(error, OrderStatusUnknown) else 'order.rejected'
        self.emit(kind, symbol=request.get('symbol'), error=error, data={'request': request_view(request)})

    def _require(self, capability: str) -> None:
        if not self._ex.has.get(capability):
            raise NotSupported(f'{capability} is not supported', exchange=self.name)

    # ─── markets ────────────────────────────────────────────────────────────

    def symbol(self, symbol: str) -> str:
        """Normalizes a symbol: 'BTC/USDT', 'BTCUSDT', 'btc-usdt-swap' → the unified symbol of this market."""
        markets = self._ex.markets or {}
        if symbol in markets and self._fits(markets[symbol]):
            return symbol
        resolved = convert.resolve_symbol(markets, getattr(self._ex, 'markets_by_id', None) or {},
                                          symbol, self.market_type, self._ex.id)
        if resolved is None:
            raise BadSymbol(f'symbol {symbol!r} not found on {self.name} {self.market_type}',
                            exchange=self.name)
        return resolved

    def _fits(self, m: dict) -> bool:
        return (m.get('type') == 'spot') == (self.market_type == 'spot')

    async def market(self, symbol: str) -> Market:
        await self.connect()
        return to_market(self._ex.market(self.symbol(symbol)), self._ex.id)

    async def markets(self, *, quote: Optional[str] = None, active_only: bool = True) -> list[Market]:
        await self.connect()
        result = []
        for m in self._ex.markets.values():
            if not self._fits(m) or (active_only and m.get('active') is False):
                continue
            if quote and m.get('quote') != quote.upper():
                continue
            result.append(to_market(m, self._ex.id))
        return result

    async def reload_markets(self) -> None:
        await self._call(ensure_markets, self._ex, reload=True)

    # ─── market data ────────────────────────────────────────────────────────

    async def ticker(self, symbol: str) -> Ticker:
        await self.connect()
        s = self.symbol(symbol)
        return convert.ticker(await self._call(self._ex.fetch_ticker, s), self._market_or_none(s))

    async def tickers(self, symbols: Optional[Iterable[str]] = None) -> dict[str, Ticker]:
        await self.connect()
        syms = [self.symbol(s) for s in symbols] if symbols else None
        raw = await self._call(self._ex.fetch_tickers, syms)
        return {s: convert.ticker(t, self._market_or_none(s))
                for s, t in raw.items() if syms or self._fits(self._ex.markets.get(s, {}))}

    async def order_book(self, symbol: str, depth: Optional[int] = None) -> OrderBook:
        await self.connect()
        s = self.symbol(symbol)
        raw = await self._call(self._ex.fetch_order_book, s, depth)
        return convert.order_book(raw, to_market(self._ex.market(s), self._ex.id))

    async def trades(self, symbol: str, *, since: Optional[int] = None,
                     limit: Optional[int] = None) -> list[Trade]:
        """Public trades. ``since`` (ms) and ``limit`` are keyword-only, as in :meth:`candles`."""
        await self.connect()
        if self._ex.id == 'hyperliquid':
            # HL has no public REST tape; ccxt fetch_trades returns the wallet's userFills
            raise NotSupported('public trades are available only via stream.trades', exchange=self.name)
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        return [convert.trade(t, m) for t in await self._call(self._ex.fetch_trades, s, since, limit)]

    async def candles(self, symbol: str, timeframe: str = '1m', *, since: Optional[int] = None,
                      limit: Optional[int] = None) -> list[Candle]:
        await self.connect()
        s = self.symbol(symbol)
        raw = await self._call(self._ex.fetch_ohlcv, s, timeframe, since, limit)
        m = self._market_or_none(s)
        return [convert.candle(c, self._ex.id, m) for c in raw]

    async def funding_rate(self, symbol: str) -> FundingRate:
        await self.connect()
        s = self.symbol(symbol)
        if not self._ex.has.get('fetchFundingRate') and self._ex.has.get('fetchFundingRates'):
            rates = await self.funding_rates([s])  # Hyperliquid: only the batch metaAndAssetCtxs
            if s not in rates:
                raise BadSymbol(f'no funding rate for {s}', exchange=self.name)
            return rates[s]
        self._require('fetchFundingRate')
        return convert.funding_rate(await self._call(self._ex.fetch_funding_rate, s))

    async def funding_rates(self, symbols: Optional[Iterable[str]] = None) -> dict[str, FundingRate]:
        await self.connect()
        syms = [self.symbol(s) for s in symbols] if symbols else None
        if self._ex.has.get('fetchFundingRates'):
            raw = await self._call(self._ex.fetch_funding_rates, syms)
            return {s: convert.funding_rate(r) for s, r in raw.items()}
        if not syms:
            raise NotSupported('fetchFundingRates is not supported: pass symbols', exchange=self.name)
        rates = await asyncio.gather(*(self.funding_rate(s) for s in syms))
        return {r.symbol: r for r in rates}

    async def funding_history(self, symbol: str, *, since: Optional[int] = None,
                              limit: Optional[int] = None) -> list[FundingPayment]:
        await self.connect()
        self._require('fetchFundingRateHistory')
        raw = await self._call(self._ex.fetch_funding_rate_history, self.symbol(symbol), since, limit)
        return [FundingPayment(r['symbol'], float(r['fundingRate']), int(r['timestamp'])) for r in raw]

    async def open_interest(self, symbol: str) -> OpenInterest:
        await self.connect()
        self._require('fetchOpenInterest')
        s = self.symbol(symbol)
        raw = await self._call(self._ex.fetch_open_interest, s)
        return convert.open_interest(raw, to_market(self._ex.market(s), self._ex.id))

    async def server_time(self) -> int:
        self._require('fetchTime')
        return int(await self._call(self._ex.fetch_time))

    # ─── account ────────────────────────────────────────────────────────────

    async def balance(self) -> Balance:
        await self.connect()
        return convert.balance(await self._call(self._ex.fetch_balance))

    async def leverage_tiers(self, symbol: str) -> list[LeverageTier]:
        """Symbol risk tiers: leverage and maintenance margin rate by bracket.

        Sources per exchange docs (``bytx.exchanges.<exchange>.tiers``), keyless wherever
        the exchange publishes tiers: Bybit /v5/market/risk-limit, OKX /public/position-tiers, KuCoin
        /contracts/risk-limit, Hyperliquid meta (MMR = 1 / (2 · maxLeverage)). Binance: with keys,
        /fapi/v1/leverageBracket (USER_DATA); without keys or permissions, the exchange's fallback public
        web endpoint (``source='binance_public'``, outside the official docs). BingX does not publish
        tiers: :class:`NotSupported`; same for spot.
        """
        await self.connect()
        s = self.symbol(symbol)
        market = self._ex.market(s)
        package = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'kucoinfutures': 'kucoin',
                   'hyperliquid': 'hyperliquid', 'bitget': 'bitget', 'gate': 'gate',
                   'mexc': 'mexc', 'blofin': 'blofin'}.get(self._ex.id)
        if package is None or not market.get('contract'):
            raise NotSupported(f'leverage tiers for {s} are not published by the exchange', exchange=self.name)
        module = importlib.import_module(f'bytx.exchanges.{package}.tiers')
        tiers: list[LeverageTier] = await module.leverage_tiers(self, s, market)
        if not tiers:
            raise ExchangeError(f'exchange returned no tiers for {s}', exchange=self.name, details={'symbol': s})
        return tiers

    async def maintenance_margin_rate(self, symbol: str, *, notional: Optional[float] = None,
                                      size: Optional[float] = None) -> float:
        """Symbol maintenance margin rate from :meth:`leverage_tiers`.

        Without ``notional`` / ``size``, tier 1; otherwise the tier the
        position falls into (``size`` in base, for OKX; ``notional`` in quote). Beyond the last
        tier, the last one. No rate from the exchange raises :class:`ExchangeError`.
        """
        tiers = await self.leverage_tiers(symbol)
        pick = tiers[0]
        if notional is not None or size is not None:
            for t in tiers:
                value, upper = ((size, t.max_size) if size is not None and t.max_size is not None
                                else (notional, t.max_notional))
                pick = t
                if value is None or upper is None or value <= upper:
                    break
        if pick.maintenance_margin_rate is None:
            raise ExchangeError(f'no maintenance margin rate for {symbol}', exchange=self.name)
        return pick.maintenance_margin_rate

    async def free_collateral(self, symbol: str) -> float:
        """Free collateral in the symbol's settle currency: ``Market.settle``
        (quote on spot). Hyperliquid: USDC, USDT perps: USDT.

        A read failure raises. A currency missing from the exchange response raises :class:`ExchangeError`
        (``details``: ``currency``, ``currencies``), not 0: "not read" ≠ "no money".
        KuCoin Futures returns the balance of one currency (``/api/v1/account-overview?currency=``),
        so the symbol's settle currency is requested.
        """
        await self.connect()
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        currency = (m.settle if m.is_contract and m.settle else m.quote).upper()
        # KuCoin: docs parameter currency (currency id); ccxt's 'code' would go into the request as an extra field
        params = {'currency': m.raw.get('settleId') or currency} if self._ex.id == 'kucoinfutures' else {}
        bal = convert.balance(await self._call(self._ex.fetch_balance, params))
        if currency not in bal and self._ex.id == 'bingx' and self.demo and 'VST' in bal:
            currency = 'VST'   # VST (BingX demo): virtual settle currency of USDT perps is "VST"
        if currency not in bal:
            raise ExchangeError(f'no {currency} in the account balance (settle currency of {s}); available: {sorted(bal.items)}',
                                exchange=self.name,
                                details={'symbol': s, 'currency': currency, 'currencies': sorted(bal.items)})
        return bal[currency].free

    async def positions(self, symbols: Optional[Iterable[str]] = None, *,
                        include_zero: bool = False) -> list[Position]:
        """Open positions (zero ones filtered out).

        ``include_zero=True`` also returns zero rows the exchange sent (``size=0``;
        hedge with a side, one-way with ``side=None``): "all positions are zero" can be checked without
        parsing ``info``. Hedge side is normalized on all exchanges, see :class:`Position`.
        """
        await self.connect()
        syms = [self.symbol(s) for s in symbols] if symbols else None
        if self._ex.id == 'bybit' and not syms:
            raw = await self._bybit_account_positions()
        else:
            raw = await self._call(self._ex.fetch_positions, syms)
        result = []
        for p in raw:
            pos = convert.position(p, self._market_or_none(p.get('symbol')), include_zero=include_zero)
            if pos is not None:
                result.append(pos)
        return result

    async def _bybit_account_positions(self) -> list[dict]:
        """Bybit: account positions — all pages of /v5/position/list (docs: ``limit`` 1..200, default 20;
        ``cursor`` ← ``nextPageCursor``). ccxt 4.4.65 without a symbol reads one page of 20: ``close_everything``
        closed 20 positions of one settleCoin, the account ``PositionTracker`` resync declared the rest closed.
        Walks every settleCoin of the linear markets (USDT, USDC) and inverse, if loaded; each page is a separate call
        through the limiter; a cut-off walk (the cursor did not end within MAX_PAGES) raises ExchangeError."""
        from bytx.exchanges.bybit import orders as bybit_orders
        return await bybit_orders.account_pages(
            lambda params: self._call(self._ex.fetch_positions, None, params), self._ex.markets,
            bybit_orders.POSITIONS_PAGE_LIMIT, key=bybit_orders.position_key, what='position/list')

    async def position(self, symbol: str, side: Optional[PositionSide] = None) -> Optional[Position]:
        """Position for a symbol; None if there is no position.

        ``side``: position side ``'long'``/``'short'``, anything else raises :class:`ConfigError`.
        Without ``side`` while both long and short are open (hedge), raises :class:`ConfigError`: pass the side.
        """
        if side is not None and side not in ('long', 'short'):
            raise ConfigError(f"side: 'long' or 'short' (position side), not {side!r}", exchange=self.name)
        s = self.symbol(symbol)
        found = [p for p in await self.positions([s]) if p.symbol == s and (side is None or p.side == side)]
        if len(found) > 1:
            raise ConfigError(f'both long and short are open on {s} (hedge): pass side', exchange=self.name)
        return found[0] if found else None

    async def open_orders(self, symbol: Optional[str] = None) -> list[Order]:
        """Open orders of the symbol (without a symbol — of the account): all pages of the exchange list — Bybit
        (cursor), OKX (``after``), KuCoin Futures (``currentPage``); each page is a separate call through the limiter."""
        await self.connect()
        s = self.symbol(symbol) if symbol else None
        if s is None:
            self._ex.options['warnOnFetchOpenOrdersWithoutSymbol'] = False
        if self._ex.id == 'bybit':
            return await (self._bybit_open_orders(s) if s is not None else self._bybit_account_open_orders())
        if self._ex.id == 'okx':
            from bytx.exchanges.okx import orders as okx_orders
            raw = await okx_orders.open_pages(
                lambda params: self._call(self._ex.fetch_open_orders, s, None, None, params), s or '')
            return [self._order(o) for o in raw]
        if self._ex.id == 'kucoinfutures':
            return await self._kucoin_open_orders(s)
        raw = await self._call(self._ex.fetch_open_orders, s)
        return [self._order(o) for o in raw]

    async def _bybit_account_open_orders(self) -> list[Order]:
        """Bybit without a symbol: pages of /v5/order/realtime by 50 with a cursor (ccxt — one page of 20) for every
        settleCoin of the linear markets (USDT, USDC); a cut-off walk raises ExchangeError."""
        from bytx.exchanges.bybit import orders as bybit_orders
        fetch = lambda params: self._call(self._ex.fetch_open_orders, None, None, None, params)  # noqa: E731
        if self.market_type == 'swap':   # docs: linear without symbol needs settleCoin — each one
            raw = await bybit_orders.account_pages(fetch, self._ex.markets, bybit_orders.OPEN_PAGE_LIMIT,
                                                   key=lambda o: str(o.get('id')), what='order/realtime',
                                                   inverse=False)
        else:
            raw = await bybit_orders.ccxt_pages(fetch, bybit_orders.OPEN_PAGE_LIMIT, key=lambda o: str(o.get('id')),
                                                what='order/realtime')
        return [self._order(o) for o in raw]

    async def _kucoin_open_orders(self, s: Optional[str]) -> list[Order]:
        """KuCoin Futures: all pages of GET /api/v1/orders?status=active (ccxt — one page of 50); parsing — ccxt."""
        from bytx.exchanges.kucoin import orders as kucoin_orders
        market = self._ex.market(s) if s is not None else None
        request: dict = {'status': 'active'}
        if market is not None:
            request['symbol'] = market['id']
        rows = await kucoin_orders.open_rows(lambda req: self._call(self._ex.futuresPrivateGetOrders, req), request)
        return [self._order(o) for o in self._ex.parse_orders(rows, market)]

    async def _bybit_open_orders(self, s: str) -> list[Order]:
        """Bybit: all pages of the symbol's open orders (50 each, ``nextPageCursor``) — ccxt returns only the first
        20. Each page is a separate call through the rate limiter."""
        from bytx.exchanges.bybit import orders as bybit_orders
        market = self._ex.market(s)
        rows = await bybit_orders.open_rows(lambda req: self._call(self._ex.private_get_v5_order_realtime, req), market)
        return [self._order(self._ex.parse_order(r, market)) for r in rows]

    async def closed_orders(self, symbol: Optional[str] = None, *, since: Optional[int] = None,
                            limit: Optional[int] = None) -> list[Order]:
        await self.connect()
        s = self.symbol(symbol) if symbol else None
        raw = await self._call(self._ex.fetch_closed_orders, s, since, limit)
        return [self._order(o) for o in raw]

    async def order(self, symbol: str, order_id: Optional[str] = None, *,
                    client_id: Optional[str] = None) -> Order:
        """Order by exchange id or by your own client_id."""
        await self.connect()
        s = self.symbol(symbol)
        if order_id and str(order_id) in self._binance_algo_ids:
            return self._order(await self._binance_algo('GET', s, str(order_id)))
        if self._ex.id == 'bybit' and (order_id or client_id):
            return await self._bybit_order(s, order_id, client_id)
        if self._ex.id == 'htx' and order_id and self.market_type == 'swap':
            from bytx.exchanges.htx.orders import order_by_id
            return await order_by_id(self, s, str(order_id))   # finished orders are removed from order_info
        if order_id:
            try:
                found = self._found(await self._call(self._ex.fetch_order, order_id, s, self._fetch_order_params()))
                return await self._hl_with_average(s, found) if self._ex.id == 'hyperliquid' else found
            except OrderNotFound:
                if self._ex.id == 'gate':
                    # Gate: TP/SL is a price-triggered order with its own id (GET price_orders/{order_id})
                    trigger = await self._find_gate_price_order(s, str(order_id))
                    if trigger is None:
                        raise
                    return self._order(trigger)
                # id not found among regular orders: may be an algoId (TP/SL not created here)
                algo = await self._find_binance_algo(s, str(order_id))
                if algo is None:
                    raise
                self._binance_algo_ids.add(str(order_id))
                return self._order(self._ex.parse_order(binance_algo_as_order(algo), self._ex.market(s)))
        if not client_id:
            raise ConfigError(f'order_id or client_id is required ({s})', exchange=self.name, details={'symbol': s})
        return await self._order_by_client_id(s, client_id)

    async def _hl_with_average(self, s: str, order: Order) -> Order:
        """Hyperliquid: orderStatus carries no avgPx — the average price of a filled order comes from its trades
        (userFillsByTime from the order time, filtered by oid). If the trades cannot be read, average stays None (the
        order itself is read)."""
        if order.average is not None or not order.filled_contracts or not order.id or not order.timestamp:
            return order
        try:
            fills = await self.my_trades(s, since=order.timestamp, order_id=order.id)
        except BytxError:
            return order
        size = sum(f.size or 0 for f in fills)
        if size > 0:
            order.average = sum((f.price or 0) * (f.size or 0) for f in fills) / size
            if isinstance(order.raw, dict):
                order.raw['average'] = order.average
        return order

    async def find_order(self, symbol: str, client_id: str, *, timeout: float = 8.0,
                         min_scans: int = 2) -> Optional[Order]:
        """Looks up an order by your own ``client_id`` within a time budget.

        For reconciliation after :class:`~bytx.errors.OrderStatusUnknown` and after a restart.
        Scans (lookup by client_id, then open and recent closed orders) run with pauses of
        0.3 / 0.7 / 1.5 / 2 s, the first immediately; everything fits into ``timeout`` (and the overall
        :meth:`deadline`, if shorter).

        Returns the order (in any status) or None if it is absent for ``min_scans`` successful
        scans in a row at the end of the window. If absence could not be confirmed (lookup failure,
        fewer than ``min_scans`` scans), raises :class:`~bytx.errors.RequestTimeout` with
        ``details['client_id']``: "not found" and "could not check" are never conflated.
        Absence is not proof: the engine may have accepted the order after the window.
        """
        await self.connect()
        if not client_id:
            raise ConfigError('find_order: client_id is required', exchange=self.name)
        if timeout <= 0 or min_scans < 1:
            raise ConfigError('find_order: timeout > 0, min_scans >= 1', exchange=self.name,
                              details={'timeout': timeout, 'min_scans': min_scans})
        s = self.symbol(symbol)
        order, ok_scans, scan_error = await self._reconcile(s, client_id, timeout=timeout, first_delay=False)
        if order is not None:
            return order
        if scan_error is None and ok_scans >= min_scans:
            return None
        why = f'lookup failed: {scan_error.message or scan_error}' if scan_error is not None \
            else f'{ok_scans} of {min_scans} successful scans in {timeout} s'
        err = RequestTimeout(f'order with client_id {client_id} not verified ({why})', exchange=self.name,
                             details={'client_id': client_id, 'symbol': s, 'scans': ok_scans})
        raise err from scan_error

    @property
    def client_id_lookup_exact(self) -> bool:
        """Whether the lookup by client_id is exact: «not found» from :meth:`find_order` / :meth:`order` proves there
        is no such order (within the exchange's retention). False — the lookup also scans the open and recently closed
        order lists, so «not found» is not a proof. Exchanges — :data:`bytx.order_params.EXACT_CLIENT_ID_LOOKUP`;
        spot is not verified — False."""
        # shortcut: spot is not verified against the docs — False; verify before relying on it for spot
        return self.market_type != 'spot' and self._ex.id in EXACT_CLIENT_ID_LOOKUP

    @staticmethod
    def deadline(seconds: float) -> Any:
        """Overall call deadline: ``with ex.deadline(5): await ex.place_order(...)``.

        Requests, retries of safe calls, pauses and order reconciliation fit into ``seconds``.
        A request that does not make it raises :class:`~bytx.errors.RequestTimeout` (for an order, reconciliation in
        the remaining time, then :class:`~bytx.errors.OrderStatusUnknown`). A nested
        deadline does not extend the outer one. Applies to child tasks too (``asyncio.gather``).
        """
        return net_deadline(seconds)

    def _fetch_order_params(self) -> dict:
        # Bybit UTA: fetch_order without acknowledged raises ArgumentsRequired (searches only the last 500)
        return {'acknowledged': True} if self._ex.id == 'bybit' else {}

    def _found(self, raw: dict) -> Order:
        info = raw.get('info') if isinstance(raw.get('info'), dict) else {}
        # Hyperliquid orderStatus {"status": "unknownOid"}: ccxt parses the missing
        # response.order and returns an empty order without info
        if self._ex.id == 'hyperliquid' and (not info or info.get('status') == 'unknownOid'):
            raise OrderNotFound('order not found', exchange=self.name)
        return self._order(raw)

    async def _bybit_order(self, s: str, order_id: Optional[str], client_id: Optional[str]) -> Order:
        """Bybit: realtime (open + last 500 closed) → history."""
        from bytx.exchanges.bybit import orders as bybit_orders
        market = self._ex.market(s)
        req = bybit_orders.order_request(market, order_id, client_id)
        for endpoint in bybit_orders.ENDPOINTS:
            row = bybit_orders.find_row(await self._call(getattr(self._ex, endpoint), dict(req)), order_id, client_id)
            if row is not None:
                return self._order(self._ex.parse_order(row, market))
        raise OrderNotFound(f'order {order_id or client_id} not found in realtime or history', exchange=self.name,
                            details={'symbol': s, 'order_id': order_id, 'client_id': client_id})

    async def _bitget_order_by_client_id(self, s: str, client_id: str) -> Order:
        """Bitget Get Order Detail / spot Get Order Info by ``clientOid`` (docs: "Either orderId or
        clientOid is required"). ccxt 4.4.65 sends clientOrderId and orderId=None, giving 40017/40020."""
        m = self._ex.market(s)
        if m.get('spot'):
            resp = await self._call(self._ex.private_spot_get_v2_spot_trade_orderinfo, {'clientOid': client_id})
            rows = resp.get('data') if isinstance(resp, dict) else None
            row = rows[0] if isinstance(rows, list) and rows else None
        else:
            product_type, _ = self._ex.handle_product_type_and_params(m, {})
            resp = await self._call(self._ex.private_mix_get_v2_mix_order_detail,
                                    {'symbol': m['id'], 'productType': product_type, 'clientOid': client_id})
            row = resp.get('data') if isinstance(resp, dict) else None
        if not isinstance(row, dict) or not row:
            # spot: no order gives data [] (demo 2026-10-26); swap gives 40109 (OrderNotFound from ccxt)
            raise OrderNotFound(f'order with client_id {client_id} not found', exchange=self.name,
                                details={'symbol': s, 'client_id': client_id})
        return self._order(self._ex.parse_order(row, m))

    async def _order_by_client_id(self, s: str, client_id: str) -> Order:
        if self._ex.id == 'hyperliquid':
            # orderStatus accepts cloid in the oid field (ccxt tells it apart by length); shared by order(client_id=)
            # and find_order — otherwise find_order called fetch_order(None, clientOrderId) and missed a canceled order
            return await self._hl_with_average(s, self._found(await self._call(self._ex.fetch_order, client_id, s)))
        if self._ex.id == 'bybit':
            return await self._bybit_order(s, None, client_id)
        if self._ex.id == 'bitget':
            return await self._bitget_order_by_client_id(s, client_id)
        if self.market_type == 'spot' and self._ex.id in ('okx', 'kucoin'):
            return await self._spot_order_by_client_id(s, client_id)
        params = {'clientOrderId': client_id, **self._fetch_order_params()}
        lookup_error: Optional[BytxError] = None
        try:
            return self._order(await self._call(self._ex.fetch_order, None, s, params))
        except OrderNotFound:
            # Gate: by text ("user custom ID") a finished order is found only for 60 s; after that, via the list
            if self._ex.id != 'gate':
                raise
        except NotSupported:
            raise
        except BytxError as e:
            lookup_error = e
        # fallback: open and recently closed orders
        for o in await self.open_orders(s) + await self.closed_orders(s, limit=100):
            if o.client_id == client_id:
                return o
        if lookup_error is not None and lookup_error.retryable:
            # the exact lookup did not answer (timeout / 5xx / 429 — retryable) and the order is not in the lists: the
            # lists hold open orders and the last 100 closed ones, an order filled earlier is not in them. "Not found"
            # is not "could not check" — the original error, not OrderNotFound: otherwise find_order returned None and
            # recover re-created the entry. A definite exchange answer (not a network failure) — the usual path:
            # OrderNotFound after the lists
            raise lookup_error
        raise OrderNotFound(f'order with client_id {client_id} not found', exchange=self.name)

    async def _spot_order_by_client_id(self, s: str, client_id: str) -> Order:
        """Spot OKX / KuCoin: an order by client_id in any status within the exchange's retention.

        OKX: /trade/order?clOrdId → orders-history (7 days) → orders-history-archive (3 months), pages by ``after``.
        KuCoin: the account mode (GET /api/v1/hf/accounts/opened); HF — hf/orders/client-order/{clientOid}, then
        hf/orders/done (cursor ``lastId``); classic — /api/v1/order/client-order/{clientOid} (active orders only).
        An empty response (``data: null``) is «not found». A failure of any step is its exception, not OrderNotFound."""
        market = self._ex.market(s)
        if self._ex.id == 'kucoin' and self._ex.options.get('hf') is None:
            await self._call(self._ex.load_migration_status)
            if self._ex.options.get('hf') is None:
                raise ExchangeError('kucoin spot: the account mode (hf/classic) is unknown', exchange=self.name)
        try:
            raw = await self._call(self._ex.fetch_order, None, s, {'clientOrderId': client_id})
            if raw.get('id'):
                return self._order(raw)
        except OrderNotFound:
            pass
        if self._ex.id == 'okx':
            from bytx.exchanges.okx import orders as okx_orders
            found = await okx_orders.spot_history_by_client_id(self._call, self._ex, market, client_id)
        elif self._ex.options.get('hf'):
            from bytx.exchanges.kucoin import orders as kucoin_orders
            row = await kucoin_orders.spot_hf_done_by_client_id(
                lambda p: self._call(self._ex.privateGetHfOrdersDone, p), market['id'], client_id)
            found = self._ex.parse_order(row, market) if row is not None else None
        else:
            found = None
        if found is None:
            raise OrderNotFound(f'order with client_id {client_id} not found', exchange=self.name)
        return self._order(found)

    async def my_trades(self, symbol: Optional[str] = None, *, since: Optional[int] = None,
                        limit: Optional[int] = None, order_id: Optional[str] = None,
                        client_id_prefix: Optional[str] = None, until: Optional[int] = None) -> list[Fill]:
        """Your own trades. ``order_id`` — only this order's trades: filtered by the exchange where the docs have the
        parameter (:data:`bytx.order_params.MY_TRADES_ORDER_FILTER`), otherwise by ``Fill.order_id`` within the
        ``since`` / ``limit`` window (set ``since`` no later than the order). ``client_id_prefix`` — trades of your own
        orders (bot tag, :func:`~bytx.order_params.owns`): by the trade's client_id, or, if the exchange does not
        return it in trades, by the order's client_id (one :meth:`order` per order, the client_id is copied into the
        trade). Both filters require ``symbol``. ``until`` (ms, inclusive) — all trades of the window ``[since,
        until]``: the window end goes into the request, pages run to the end of the window
        (:data:`~bytx.order_params.MY_TRADES_WINDOW`); if the whole window cannot be read, an error is raised instead
        of returning part of it. Only with ``symbol`` and ``since``, without ``limit``."""
        await self.connect()
        if (order_id is not None or client_id_prefix is not None) and not symbol:
            raise ConfigError('my_trades: order_id / client_id_prefix require symbol', exchange=self.name)
        if until is not None and (not symbol or since is None or limit is not None or since > until):
            raise ConfigError('my_trades: until requires symbol and since ≤ until, without limit (whole window)',
                              exchange=self.name)
        s = self.symbol(symbol) if symbol else None
        order_id = None if order_id is None else str(order_id)
        key = MY_TRADES_ORDER_FILTER.get(self._ex.id) if order_id is not None else None
        raw = None
        history = getattr(self._ex, 'bytx_fetch_fill_history', None)
        if s is not None and history is not None:
            # BingX: fillHistory with tradeId (= WS "td"), so the trade id is the same in REST and the stream; allFillOrders
            # returns no id, a synthetic id caused double counting (demo VST 2026-10-29)
            raw = await self._call(history, s, since, limit, *(() if until is None else (until,)))
        if raw is None and s is not None and self._ex.id == 'bybit':
            raw = await self._bybit_my_trades(s, since, limit, until)
        params = {key: order_id} if key else {}
        if raw is None and until is not None:
            raw = await self._my_trades_window(s, since, until, params)    # type: ignore[arg-type]
        if raw is None:
            raw = await self._call(self._ex.fetch_my_trades, s, since, limit, params)
        if until is not None:
            raw = [t for t in raw or () if (t.get('timestamp') or 0) <= until]
        fills = [convert.fill(t, self._market_or_none(t.get('symbol')), self._ex.id) for t in raw or ()]
        if order_id is not None:
            fills = [f for f in fills if f.order_id == order_id]
        if client_id_prefix is not None and s is not None:
            fills = await self._own_fills(s, fills, client_id_prefix)
        return fills

    async def _my_trades_window(self, s: str, since: int, until: int, params: dict) -> list[dict]:
        """All trades of the window ``[since, until]``: the window end goes into the request, a full page splits the
        window in half (see :data:`~bytx.order_params.MY_TRADES_WINDOW`). More than a page of trades within one ms —
        ExchangeError."""
        if self._ex.id not in MY_TRADES_WINDOW:
            raise NotSupported(f'my_trades(until=): the {self._ex.id} trade window is not verified against the docs',
                               exchange=self.name)
        page, span = MY_TRADES_WINDOW[self._ex.id]
        if self.market_type == 'spot':
            span = MY_TRADES_SPOT_SPAN.get(self._ex.id, span)
        stack, start = [], since
        while start <= until:                                   # a request (window ± 1 ms) is not wider than the docs span
            end = until if span is None else min(until, start + span - 3)
            stack.append((start, end))
            start = end + 1
        rows: dict = {}
        while stack:
            a, b = stack.pop()
            # bounds with a margin (strict on some exchanges, not on others; Gate — seconds, ccxt drops ms), extra rows
            # are filtered out
            end_param = b + (1000 if self._ex.id == 'gate' else 1)
            got = await self._call(self._ex.fetch_my_trades, s, a - 1, page, {**params, 'until': end_param})
            if len(got) >= page:
                if a >= b:
                    raise ExchangeError(f'my_trades {s}: at least {page} trades within {a} ms — the window was not '
                                        'read completely', exchange=self.name)
                m = (a + b) // 2
                stack += [(a, m), (m + 1, b)]
                continue
            for t in got:
                if a <= (t.get('timestamp') or 0) <= b:
                    rows[t.get('id') or id(t)] = t
        return sorted(rows.values(), key=lambda t: t.get('timestamp') or 0)

    async def _own_fills(self, s: str, fills: list[Fill], prefix: str) -> list[Fill]:
        """Trades of your own orders: by the trade's client_id, otherwise by the order's client_id (one request per
        order)."""
        cids: dict[str, Optional[str]] = {}
        for f in fills:
            if not f.client_id and f.order_id and f.order_id not in cids:
                cids[f.order_id] = (await self.order(s, f.order_id)).client_id
        out = []
        for f in fills:
            if not f.client_id and f.order_id:
                f.client_id = cids.get(f.order_id)
            if owns(f, prefix, self._ex.id):
                out.append(f)
        return out

    async def _bybit_my_trades(self, s: str, since: Optional[int], limit: Optional[int],
                               until: Optional[int] = None) -> list[dict]:
        """Bybit: the symbol's trades — all pages (100 each, cursor) over 7-day windows up to now (with ``until`` — up
        to it, endTime in every window; not enough pages — ExchangeError); ccxt returns one page (50) of one window.
        Each page is a separate call through the rate limiter; parsing — ccxt parse_trades."""
        import time as _time

        from bytx.exchanges.bybit import orders as bybit_orders
        market = self._ex.market(s)
        rows: list[dict] = []
        seen: set[str] = set()
        page_limit = min(bybit_orders.TRADES_PAGE_LIMIT, limit) if limit else bybit_orders.TRADES_PAGE_LIMIT
        windows = bybit_orders.trade_windows(since, int(_time.time() * 1000) if until is None else until)
        if until is not None:
            windows[0] = (windows[0][0], until)          # the newest window ends at until, not open-ended
        for start, end in windows:
            cursor: Optional[str] = None
            for _ in range(bybit_orders.TRADES_MAX_PAGES):
                resp = await self._call(self._ex.private_get_v5_execution_list,
                                        bybit_orders.trades_request(market, start, end, cursor, page_limit))
                page, cursor = bybit_orders.open_page(resp)
                new = [r for r in page if str(r.get('execId')) not in seen]
                seen.update(str(r.get('execId')) for r in new)
                rows += new
                if not cursor or not new or (limit is not None and len(rows) >= limit):
                    break
            else:
                if cursor and until is not None:
                    raise ExchangeError(f'my_trades {s}: {bybit_orders.TRADES_MAX_PAGES} pages in window {start}..{end}'
                                        ' — the window was not read completely', exchange=self.name)
                if cursor and (limit is None or len(rows) < limit):
                    from loguru import logger
                    logger.warning(f'{self.name}: my_trades {s} — {bybit_orders.TRADES_MAX_PAGES} pages in window '
                                   f'{start}..{end}: older trades of the window were not read')
            if limit is not None and len(rows) >= limit:
                break
        if limit is not None:
            rows = rows[:limit]                          # rows are newest first: the latest ``limit`` trades
        return self._ex.parse_trades(rows, market, since)

    async def classify_close(self, close: Union[Order, Fill], *, tag: Optional[str] = None) -> 'CloseVerdict':
        """What closed the position: a closing order or fill → liquidation / ADL / settlement / our bot / triggered
        conditional order / manual (:func:`bytx.journal.origin.classify_close`). ``tag`` — the bot tag (defaults to
        the client's ``client_id_prefix``). BingX: the order carries no marker — the id is looked up in forceOrders
        (``autoCloseType`` LIQUIDATION / ADL) within an hour around the order time."""
        from bytx.journal.origin import classify_close
        is_fill = isinstance(close, Fill)
        raw = close.raw.get('info') if isinstance(close.raw, dict) else None
        tag = tag if tag is not None else self.client_id_prefix
        verdict = classify_close(self._ex.id, info=raw if isinstance(raw, dict) else {}, client_id=close.client_id,
                                 tag=tag, kind='fill' if is_fill else 'order')
        order_id = close.order_id if isinstance(close, Fill) else close.id
        if self._ex.id == 'bingx' and verdict.reason in ('manual', 'unknown') and order_id and close.timestamp:
            from bytx.journal.recovery import ExchangeHistorySource
            hour = 3_600_000
            forced, _ = await ExchangeHistorySource(self).forced_orders(close.symbol, close.timestamp - hour,
                                                                         close.timestamp + hour)
            origin = forced.get(str(order_id))
            if origin is not None:
                return type(verdict)(origin, 'exact', f'bingx: forceOrders autoCloseType ({origin})', verdict.origin)
        return verdict

    async def trading_fee(self, symbol: str) -> TradingFee:
        await self.connect()
        s = self.symbol(symbol)
        raw = await self._call(self._ex.fetch_trading_fee, s)
        return TradingFee(s, raw.get('maker'), raw.get('taker'))

    async def leverage(self, symbol: str, side: Optional[PositionSide] = None, *,
                       margin_mode: Optional[MarginMode] = None) -> Optional[float]:
        """Symbol leverage. None if the exchange does not report it ("not measured").

        ``side``: position side 'long'/'short' (leverage may differ: Bybit hedge
        isolated, OKX isolated long/short); anything else raises ConfigError. Without ``side`` when the sides'
        leverage differs, raises ConfigError: pass the side (previously long or short was picked silently).
        ``margin_mode``: the margin bucket (OKX and KuCoin store leverage separately for cross and
        isolated), defaults to :meth:`margin_mode_of`; on OKX without a mode, ConfigError.
        Hyperliquid: asset leverage from ``activeAssetData``; if the asset is in a different mode, None.
        """
        if side is not None and side not in ('long', 'short'):
            raise ConfigError(f"side: 'long' or 'short' (position side), not {side!r}", exchange=self.name)
        await self.connect()
        s = self.symbol(symbol)
        mode = _check_margin_mode(margin_mode, self.name) if margin_mode is not None else self.margin_mode_of(s)
        if self._ex.id == 'hyperliquid':
            return await self._hl_leverage(s, mode)
        if self._ex.id == 'bybit':
            from bytx.exchanges.bybit.leverage import category, leverage_by_side
            raw = await self._call(self._ex.private_get_v5_position_list,
                                   {'category': category(self._ex.market(s)), 'symbol': self._ex.market(s)['id']})
            by_side = leverage_by_side(raw)
            return self._pick_leverage(s, by_side.get('long'), by_side.get('short'), side)
        if self._ex.id == 'htx':
            if mode is None:
                raise self._no_margin_mode(s)
            from bytx.exchanges.htx.leverage import current_leverage
            return await current_leverage(self, s, mode)
        if self._ex.id == 'kucoinfutures':
            if mode is None:
                raise self._no_margin_mode(s)
            if mode == 'isolated':
                return self._leverage.get(s)  # ccxt 4.4.65 reads only cross leverage
        elif self._ex.id in ('okx', 'blofin') and mode is None:
            raise self._no_margin_mode(s)
        elif self._ex.id == 'gate':
            # ccxt fetch_leverage for swap is NotSupported; leverage comes from the position (leverage / cross_leverage_limit)
            from bytx.exchanges.gate.leverage import current as gate_leverage
            return (await gate_leverage(self, s))[1]
        if not self._ex.has.get('fetchLeverage'):
            return self._leverage.get(s)
        params = {'marginMode': mode} if self._ex.id in ('okx', 'kucoinfutures', 'blofin') else {}
        raw = await self._call(self._ex.fetch_leverage, s, params)
        if self._ex.id == 'bitget':
            # Get Single Account holds both pockets: cross — crossedMarginLeverage, isolated — per side;
            # no SDK mode set — the symbol's mode from the response (marginMode)
            from bytx.exchanges.bitget.leverage import leverage_pocket
            _, long, short = leverage_pocket(raw.get('info'), mode)
            return self._pick_leverage(s, long, short, side)
        return self._pick_leverage(s, raw.get('longLeverage'), raw.get('shortLeverage'), side)

    def _pick_leverage(self, s: str, long: Any, short: Any, side: Optional[str]) -> Optional[float]:
        values = {'long': float(long) if long else None, 'short': float(short) if short else None}
        if side is not None:
            return values[side]
        if values['long'] and values['short'] and values['long'] != values['short']:
            raise ConfigError(f'{s} leverage differs for long {values["long"]} and short {values["short"]}: pass side',
                              exchange=self.name, details={'symbol': s, **values})
        return values['long'] or values['short']

    async def _hl_leverage(self, s: str, mode: Optional[str] = None) -> Optional[float]:
        # ccxt 4.4.65 cannot fetchLeverage on HL. Docs (info-endpoint/perpetuals,
        # "Retrieve User's active asset data"): activeAssetData → leverage {type, value} —
        # asset setting, available even without a position
        market = self._ex.market(s)
        user, _ = self._ex.handle_public_address('fetchLeverage', {})
        coin = market['base'] if market.get('swap') else market['id']
        raw = await self._call(self._ex.publicPostInfo, {'type': 'activeAssetData', 'user': user, 'coin': coin})
        lev = (raw or {}).get('leverage') or {}
        value = lev.get('value')
        if value in (None, ''):
            return self._leverage.get(s)
        if mode is not None and lev.get('type') not in (None, mode):
            return None  # asset in a different margin mode: leverage of the requested bucket is not measured
        self._leverage[s] = float(value)
        return float(value)

    def margin_mode_of(self, symbol: str) -> Optional[MarginMode]:
        """Symbol margin mode: the one set by :meth:`set_margin_mode`, otherwise the client's ``margin_mode``.

        None if set in neither place.
        """
        return self._margin_modes.get(self.symbol(symbol), self.margin_mode)

    def _no_margin_mode(self, s: str) -> ConfigError:
        return ConfigError(f"margin mode for {s} is not set: create_exchange(margin_mode='cross'|'isolated') "
                           f'or set_margin_mode(symbol, mode)', exchange=self.name, details={'symbol': s})

    def _need_margin_mode(self, s: str, fallback: Optional[str] = None) -> MarginMode:
        """Margin mode sent to the exchange; not set raises ConfigError.

        ``fallback``: the open position's mode (close and reduce: the position is already in its bucket).
        """
        mode = self._margin_modes.get(s) or self.margin_mode
        if mode is None and fallback:
            mode = _check_margin_mode(fallback, self.name)
        if mode is None:
            raise self._no_margin_mode(s)
        return mode

    async def is_hedge_mode(self, symbol: Optional[str] = None, *, refresh: bool = False) -> bool:
        """Whether hedge mode is on (two long/short positions per symbol).

        The answer is cached; ``refresh=True`` re-reads it from the exchange (the mode may have
        been changed outside the process). :meth:`set_position_mode` updates/resets the cache itself.
        KuCoin Futures: a hedge account raises :class:`ConfigError` (the SDK trades KuCoin one-way only).
        """
        if self._ex.id in _ONE_WAY_CHECKED:
            return await self._one_way_checked(refresh=refresh)
        if self._ex.id in _ONE_WAY_ONLY:
            return False
        if self._hedge is not None and not refresh:
            return self._hedge
        if self._ex.id == 'bybit':
            return await self._bybit_hedge_mode(symbol, refresh=refresh)
        await self.connect()
        if self._ex.id == 'bitget':
            self._hedge = await self._bitget_hedge_mode(symbol)
            return self._hedge
        if self._ex.id == 'blofin':
            # Get Position Mode is per account; Multi-Position — ConfigError before trading
            from bytx.exchanges.blofin.leverage import position_mode as blofin_position_mode
            self._hedge = await blofin_position_mode(self)
            return self._hedge
        if self._ex.id == 'htx' and self.market_type == 'swap':
            from bytx.exchanges.htx.leverage import position_mode
            self._hedge = await position_mode(self) == 'dual_side'
            return self._hedge
        # ccxt okx 4.4.65: has a fetch_position_mode method but no has['fetchPositionMode'] flag
        if not self._ex.has.get('fetchPositionMode') and self._ex.id != 'okx':
            self._hedge = False
            return False
        s = self.symbol(symbol) if symbol else None
        params: dict = {}
        if s is None and self._ex.id == 'binance':
            # Binance position mode is account-wide for USDⓈ-M; without a symbol ccxt requires subType
            params['subType'] = 'linear'
        raw = await self._call(self._ex.fetch_position_mode, s, params)
        self._hedge = bool(raw.get('hedged'))
        return self._hedge

    async def _one_way_checked(self, *, refresh: bool) -> bool:
        """KuCoin Futures: GET /api/v2/position/getPositionMode (positionMode 0 — one-way, 1 — hedge).

        The mode is account-wide ("applies to all futures trading pairs"), so the cache has no symbol. Hedge raises
        :class:`ConfigError` before sending an order (otherwise an order without ``positionSide`` is rejected by the exchange);
        the mode is not switched: that is an action on the user's account.
        """
        if self.market_type != 'swap':
            return False
        if self._hedge is None or refresh:
            await self.connect()
            from bytx.exchanges.kucoin.position_mode import read
            self._hedge = None
            self._hedge = await read(self)
        if self._hedge:
            from bytx.exchanges.kucoin.position_mode import hedge_account_error
            raise hedge_account_error(self)
        return False

    async def _bybit_hedge_mode(self, symbol: Optional[str], *, refresh: bool) -> bool:
        """Bybit V5: mode from GET /v5/position/list (positionIdx 0: one-way, 1/2: hedge).

        ccxt 4.4.65 has no fetchPositionMode for Bybit; V5 has no dedicated mode endpoint either.
        The mode is set symbol > coin > default (Switch Position Mode docs), so it is cached per symbol.
        Without a symbol: settleCoin=USDT (only positions with size > 0); if empty, the BTCUSDT reference symbol
        (the USDT coin mode, unless the symbol has its own). Unreadable raises ExchangeError, not one-way.
        """
        if self.market_type != 'swap':
            return False
        await self.connect()
        from bytx.exchanges.bybit.leverage import category, position_mode
        s = self.symbol(symbol) if symbol else None
        key = s or ''
        if not refresh and key in self._hedge_by_symbol:
            return self._hedge_by_symbol[key]
        if refresh:
            self._hedge = None   # re-read from the exchange: the value from create_exchange/cache is no longer valid
        hedge: Optional[bool] = None
        if s is not None:
            m = self._ex.market(s)
            hedge = position_mode(await self._call(self._ex.private_get_v5_position_list,
                                                   {'category': category(m), 'symbol': m['id']}))
        else:
            hedge = position_mode(await self._call(self._ex.private_get_v5_position_list,
                                                   {'category': 'linear', 'settleCoin': 'USDT'}))
            if hedge is None:
                hedge = position_mode(await self._call(self._ex.private_get_v5_position_list,
                                                       {'category': 'linear', 'symbol': 'BTCUSDT'}))
        if hedge is None:
            raise ExchangeError(f'position mode for {s or "USDT"} undetermined: /v5/position/list returned no rows',
                                exchange=self.name, details={'symbol': s})
        self._hedge_by_symbol[key] = hedge
        return hedge

    async def _bitget_hedge_mode(self, symbol: Optional[str]) -> bool:
        """Bitget: ``data.posMode`` Get Single Account (GET /api/v2/mix/account/account).

        ccxt 4.4.65: has['fetchPositionMode']=False. The mode is per productType (Change Position
        Mode: productType + posMode); without a symbol, via BTC/USDT:USDT (USDT-FUTURES). Unreadable posMode
        raises ExchangeError instead of a silent one-way (which sent orders without tradeSide, and a
        hedge account replied 40774).
        """
        if self.market_type != 'swap':
            return False
        if symbol:
            m = self._ex.market(self.symbol(symbol))
        else:
            m = self._ex.markets.get('BTC/USDT:USDT') or next(
                (x for x in self._ex.markets.values() if x.get('swap') and x.get('settle') == 'USDT'), None)
            if m is None:
                raise ExchangeError('position mode: no USDT-FUTURES market to query', exchange=self.name)
        product_type, _ = self._ex.handle_product_type_and_params(m, {})
        resp = await self._call(self._ex.private_mix_get_v2_mix_account_account,
                                {'symbol': m['id'], 'productType': product_type, 'marginCoin': m['settleId']})
        data = resp.get('data') if isinstance(resp, dict) else None
        pos_mode = data.get('posMode') if isinstance(data, dict) else None
        if pos_mode not in ('hedge_mode', 'one_way_mode'):
            raise ExchangeError(f'position mode for {m["symbol"]} undetermined: posMode={pos_mode!r}',
                                exchange=self.name, details={'symbol': m['symbol']})
        return pos_mode == 'hedge_mode'

    def _not_modified(self, error: BytxError) -> bool:
        """An "already set" rejection (Bybit 110025 mode, 110043 leverage) means success for a settings change."""
        if self._ex.id != 'bybit':
            return False
        from bytx.exchanges.bybit.error_codes import NOT_MODIFIED
        return str(error.code or '') in NOT_MODIFIED

    # ─── trading settings ───────────────────────────────────────────────────

    async def set_leverage(self, symbol: str, leverage: float, *, side: Optional[PositionSide] = None,
                           allow_open_position: bool = False) -> Optional[float]:
        """Sets symbol leverage.

        Returns the leverage the exchange reported as applied (Binance ``leverage``, OKX
        ``data[].lever``: the lower of the sides, BingX ``data.leverage``); lower than requested means
        capped by the symbol's tier. None if the exchange does not report it (Bybit, Hyperliquid, KuCoin); re-measure via
        :meth:`leverage`. An exchange rejection raises.

        If a position is open on the symbol, raises :class:`bytx.errors.PositionOpenError` and nothing
        is sent. The check costs one
        positions request; ``allow_open_position=True`` skips it.
        OKX and Hyperliquid: a margin mode is required (:meth:`margin_mode_of`), otherwise ConfigError.

        ``side`` ('long' / 'short'): leverage of this hedge position side only (the other side — e.g. another
        strategy on the symbol — is not changed; only this side's position is checked). Where the exchange sets
        leverage per side: OKX — isolated in long/short mode (``posSide``), BingX — hedge (``side`` LONG/SHORT),
        Bybit — hedge (``buyLeverage`` / ``sellLeverage``, the other side keeps its current leverage; in cross Bybit
        requires them equal — otherwise the exchange rejects with 10001). Otherwise
        :class:`~bytx.errors.NotSupported` before any request: Binance (one leverage per symbol), OKX cross / net,
        one-way, other exchanges. Silently changing both sides is not an option — it would change the other side's
        leverage too. Without ``side`` — as before (both sides).
        """
        if side is not None and side not in ('long', 'short'):
            raise ConfigError(f"side: 'long' or 'short' (position side), not {side!r}", exchange=self.name)
        await self.connect()
        s = self.symbol(symbol)
        if self._ex.id in _INT_LEVERAGE:
            if leverage != int(leverage):
                raise ConfigError(f'leverage on {self.name} must be an integer: {leverage}', exchange=self.name)
            leverage = int(leverage)
        mode = (self._need_margin_mode(s) if self._ex.id in ('okx', 'hyperliquid', 'blofin')
                else self.margin_mode_of(s))
        if side is not None:
            await self._check_side_leverage(s, side, mode)
        if not allow_open_position and any(p.symbol == s and (side is None or p.side == side)
                                           for p in await self.positions([s])):
            raise PositionOpenError(f'position open on {s}: leverage not changed', symbol=s,
                                    exchange=self.name)
        if self._ex.id in _LEVERAGE_IN_ORDER:
            self._leverage[s] = leverage
            return None  # KuCoin / MEXC / HTX: leverage is sent with every order (order_params.position_params)
        params: dict = {}
        calls: list[dict] = []
        if side is not None:
            calls = [await self._side_leverage_params(s, leverage, side, mode)]
        elif self._ex.id == 'okx':
            params['marginMode'] = mode
            params['lever'] = str(leverage)  # docs: lever is a String (ccxt 4.4.65 sends a number)
            if mode == 'isolated' and await self.is_hedge_mode(s):
                # set-leverage docs, example 11: isolated + long/short sets leverage per side
                calls = [{**params, 'posSide': pos_side} for pos_side in ('long', 'short')]
        elif self._ex.id == 'hyperliquid':
            params['marginMode'] = mode
        elif self._ex.id == 'blofin':
            # Set Leverage: marginMode is required; positionSide — «only isolated in long/short mode»: per side
            params['marginMode'] = mode
            if mode == 'isolated' and await self.is_hedge_mode(s):
                calls = [{**params, 'positionSide': pos_side} for pos_side in ('long', 'short')]
        elif self._ex.id == 'gate':
            # Position.leverage: "0 means cross margin; positive number means isolated margin", no marginMode
            # ccxt sends leverage N (the position goes isolated); with no mode set, the position's mode
            if mode is None:
                from bytx.exchanges.gate.leverage import current as gate_leverage
                mode = cast(Optional[MarginMode], (await gate_leverage(self, s))[0])
            if mode is None:
                raise self._no_margin_mode(s)
            params = gate_leverage_params(mode, leverage)
        elif self._ex.id == 'bingx':
            # ccxt 4.4.65 requires side on BingX; in hedge, set it for both sides
            calls = [{'side': side} for side in (('LONG', 'SHORT') if await self.is_hedge_mode(s) else ('BOTH',))]
        applied: list[float] = []
        for call_params in calls or [params]:
            try:
                raw = await self._call(self._ex.set_leverage, leverage, s, call_params)
            except BytxError as e:
                if not self._not_modified(e):
                    raise
                raw = None   # Bybit 110043: leverage already equals the requested one
            value = _applied_leverage(self._ex.id, raw)
            if value is not None:
                applied.append(value)
        result = min(applied) if applied else None
        self._leverage[s] = result if result is not None else leverage
        return result

    async def _check_side_leverage(self, s: str, side: str, mode: Optional[str]) -> None:
        """Can the exchange set leverage for one side of ``s`` (see :meth:`set_leverage`); no — NotSupported."""
        ex_id = self._ex.id
        reason: Optional[str] = None
        if ex_id not in _SIDE_LEVERAGE:
            reason = ('one leverage per symbol for both sides (POST /fapi/v1/leverage has no side)'
                      if ex_id == 'binance' else 'per-side leverage is not supported by the SDK')
        elif ex_id in ('okx', 'blofin') and mode != 'isolated':
            reason = 'in cross the instrument leverage is shared by both sides (posSide — isolated long/short only)'
        elif not await self.is_hedge_mode(s):
            reason = 'the symbol is in one-way mode: the position has a single side'
        if reason is not None:
            raise NotSupported(f'{self.name}: leverage of the {side} side of {s} cannot be set — {reason}; compare '
                               f'leverage({s!r}, {side!r}) with the required one or set leverage without side',
                               exchange=self.name, details={'symbol': s, 'side': side, 'margin_mode': mode})

    async def _side_leverage_params(self, s: str, leverage: float, side: str, mode: Optional[str]) -> dict:
        """ccxt ``set_leverage`` params for one hedge position side (the exchange is checked by ``_check_side_leverage``)."""
        if self._ex.id == 'okx':
            # set-leverage docs: posSide «Only required when margin mode is isolated in long/short mode»
            return {'marginMode': mode, 'lever': str(leverage), 'posSide': side}
        if self._ex.id == 'blofin':
            return {'marginMode': mode, 'positionSide': side}
        if self._ex.id == 'bingx':
            # Set Leverage docs: side «In the Hedge mode, LONG for long positions, SHORT for short positions»
            return {'side': side.upper()}
        # Bybit: buyLeverage and sellLeverage are both required — the other side keeps its own leverage
        other = 'short' if side == 'long' else 'long'
        current = await self.leverage(s, cast(PositionSide, other))
        if current is None:
            raise ExchangeError(f'leverage of the {other} side of {s} not read: /v5/position/list has no entry for '
                                'the side', exchange=self.name, details={'symbol': s, 'side': other})
        own, keep = _leverage_str(leverage), _leverage_str(current)
        return {'buyLeverage': own if side == 'long' else keep, 'sellLeverage': keep if side == 'long' else own}

    async def set_margin_mode(self, symbol: str, mode: MarginMode, *,
                              leverage: Optional[float] = None) -> None:
        """Margin mode of one symbol: 'cross' or 'isolated'.

        Changes only ``symbol``; other symbols use the client's ``margin_mode``
        (see :meth:`margin_mode_of`). Stored after a successful exchange response.

        ``leverage``: the leverage OKX, Hyperliquid and Gate switch the mode with (on these exchanges the mode
        change goes in the same request as leverage). If not set, the symbol's current leverage is used
        (:meth:`leverage`); if it cannot be determined, ConfigError; 1x is never substituted.

        KuCoin: the mode is sent with every order and also belongs to the symbol (an order in another mode is
        rejected, 330005): the symbol's mode is read (Get Margin Mode) and, if different, switched
        (Switch Margin Mode). MEXC, HTX: only stored (the mode goes in the order / selects the endpoint).
        """
        mode = _check_margin_mode(mode, self.name)
        await self.connect()
        s = self.symbol(symbol)
        if self._ex.id == 'kucoinfutures':
            from bytx.exchanges.kucoin import margin_mode as kucoin_margin_mode
            await kucoin_margin_mode.switch(self, s, mode)
            self._margin_modes[s] = mode
            return
        if self._ex.id in _LEVERAGE_IN_ORDER or self._ex.id == 'blofin':
            self._margin_modes[s] = mode
            # set on every order (MEXC openType, HTX endpoint, BloFin marginMode — an order with a mode different
            # from the account's is accepted)
            return
        params: dict = {}
        if self._ex.id in ('okx', 'hyperliquid', 'gate'):
            lev = leverage if leverage is not None else self._leverage.get(s)
            if not lev:
                try:
                    # current bucket; with no mode set, HL returns the asset leverage, OKX the target bucket
                    current = self.margin_mode_of(s) or (mode if self._ex.id == 'okx' else None)
                    lev = await self.leverage(s, margin_mode=current)
                except ConfigError:
                    lev = None  # sides have different leverage: the caller decides which to use
            if not lev:
                raise ConfigError(f'{s} leverage unknown: pass set_margin_mode(..., leverage=)',
                                  exchange=self.name, details={'symbol': s, 'margin_mode': mode})
            if self._ex.id == 'hyperliquid':
                if lev != int(lev):
                    raise ConfigError(f'leverage on {self.name} must be an integer: {lev}', exchange=self.name,
                                      details={'symbol': s, 'leverage': lev})
                lev = int(lev)
            params['leverage'] = lev
        if self._ex.id == 'gate':
            # ccxt has no set_margin_mode for gate: the mode goes via the same POST positions/{contract}/leverage (cross: 0 +
            # cross_leverage_limit, isolated — leverage N)
            await self._call(self._ex.set_leverage, lev, s, gate_leverage_params(mode, lev))
        else:
            await self._call(self._ex.set_margin_mode, mode, s, params)
        self._margin_modes[s] = mode
        if 'leverage' in params:
            self._leverage[s] = params['leverage']

    async def set_position_mode(self, hedge: bool, symbol: Optional[str] = None) -> None:
        """hedge=True: two positions (long and short) per symbol; False: one-way."""
        if self._ex.id in _ONE_WAY_ONLY:
            if hedge:
                raise NotSupported('the exchange supports one-way only', exchange=self.name)
            return
        await self.connect()
        self._require('setPositionMode')
        s = self.symbol(symbol) if symbol else None
        self._hedge = None  # on failure the mode is unknown: the next is_hedge_mode asks the exchange
        self._hedge_by_symbol.clear()
        try:
            await self._call(self._ex.set_position_mode, hedge, s)
        except BytxError as e:
            if not self._not_modified(e):
                raise
            # Bybit 110025 "Position mode is not modified": the mode is already set
        if self._ex.id == 'bybit':
            # Bybit mode is per symbol (symbol > coin > default): without a symbol the coin mode changed,
            # symbols with their own mode keep it, so other symbols will ask the exchange
            if s is not None:
                self._hedge_by_symbol[s] = hedge
            return
        self._hedge = hedge

    # ─── trading ────────────────────────────────────────────────────────────

    async def buy(self, symbol: str, *, size: Optional[float] = None, notional: Optional[float] = None,
                  contracts: Optional[float] = None, price: Optional[float] = None,
                  **kwargs: Any) -> Order:
        """Buy: market without price, limit with price. See :meth:`place_order`.

        With an external timeout (``asyncio.wait_for``, task cancellation), pass your own
        ``client_id``: the order can be found by it via :meth:`order`.
        """
        return await self.place_order(symbol, 'buy', size=size, notional=notional,
                                      contracts=contracts, price=price, **kwargs)

    async def sell(self, symbol: str, *, size: Optional[float] = None, notional: Optional[float] = None,
                   contracts: Optional[float] = None, price: Optional[float] = None,
                   **kwargs: Any) -> Order:
        """Sell: market without price, limit with price. See :meth:`place_order`.

        With an external timeout (``asyncio.wait_for``, task cancellation), pass your own
        ``client_id``: the order can be found by it via :meth:`order`.
        """
        return await self.place_order(symbol, 'sell', size=size, notional=notional,
                                      contracts=contracts, price=price, **kwargs)

    async def sell_all(self, symbol: str, *, client_id: Optional[str] = None) -> Optional[Order]:
        """Spot: a market sell of the whole free balance of the base coin.

        ``free`` from :meth:`balance` is rounded down strictly to the lot step (Binance market — also to
        ``MARKET_LOT_SIZE``); below the step, the minimum size or the minimum cost at the reference price it is
        «dust»: no order is sent, a WARNING is logged and None is returned. Otherwise — :meth:`place_order` with all
        its protection (reconciliation by ``client_id`` on an unknown outcome). Not spot — :class:`ConfigError`.
        """
        if self.market_type != 'spot':
            raise ConfigError("sell_all is for market='spot' only", exchange=self.name,
                              details={'symbol': symbol, 'market': self.market_type})
        await self.connect()
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        free = balance_free = (await self.balance()).free(m.base)
        if m.amount_step and self._ex.precisionMode == ccxt.TICK_SIZE:
            # strictly down: snapping to a whole lot would raise the size above free — an exchange rejection (OKX 51008)
            step = Decimal(str(m.amount_step))
            free = float((Decimal(str(free)) / step).to_integral_value(rounding=ROUND_DOWN) * step)
        try:
            if free <= 0:
                raise OrderTooSmall(f'no free {m.base}', exchange=self.name)
            return await self.place_order(s, 'sell', size=free, client_id=client_id)
        except OrderTooSmall as e:
            from loguru import logger
            logger.warning(f'{self.name}: sell_all {s} — dust: free={balance_free}, min={m.min_amount} {m.base}, '
                           f'min_cost={m.min_cost}, step={m.amount_step} ({e.message})')
            return None

    async def place_order(self, symbol: str, side: Side, *, size: Optional[float] = None,
                          notional: Optional[float] = None, contracts: Optional[float] = None,
                          margin: Optional[float] = None, leverage: Optional[float] = None,
                          price: Optional[float] = None, type: Optional[str] = None,
                          post_only: bool = False, time_in_force: Optional[str] = None,
                          reduce_only: bool = False, client_id: Optional[str] = None,
                          take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                          params: Optional[dict] = None, reconcile: bool = True,
                          max_round_up: Optional[float] = None) -> Order:
        """Places an order.

        Exactly one of: ``size`` (base), ``notional`` (quote, e.g. USDT; full
        notional without leverage), ``contracts``, ``margin`` (quote, as margin; size = margin ×
        leverage). ``leverage`` only with ``margin``; without it, the leverage from
        :meth:`set_leverage` on this instance; without that too, :class:`ConfigError`. ``margin``
        is converted to contracts per :meth:`contracts_for_margin`
        at the execution price: limit uses its own price after
        rounding, market uses the reference price (best book price/ticker) rounded to the tick.
        A ``margin`` size below the minimum lot is rounded up to the minimum; ``max_round_up`` caps how many times
        the minimum lot may exceed the requested size (above it — :class:`OrderTooSmall`; None — no cap, a WARNING
        at 2× and more).
        Default ``type``: 'limit' with ``price``, otherwise 'market'.
        ``post_only``: maker only; ``time_in_force``: 'GTC' / 'IOC' / 'FOK';
        ``take_profit`` / ``stop_loss``: trigger prices attached to the order (where
        the exchange supports it). ``client_id`` is generated if not set and
        lets you find the order after a timeout (:meth:`order`).

        Rounding to exchange steps: size (``size`` / ``notional`` / ``contracts``) always
        down (below one step raises :class:`OrderTooSmall`); limit price toward the passive
        side: buy down, sell up, i.e. never worse than the user's price (100.06 with a
        0.1 tick: buy → 100.0, sell → 100.1). Market order and TP/SL prices round to the nearest tick.
        Error outcome follows the exchange code table (:mod:`bytx.outcome`):

        - explicit rejection (funds, parameters, rate limit, clock): the original error, no reconciliation;
          reduce-only without a position (Bybit 110017, OKX 51169, BingX 101205, KuCoin
          300009) raises :class:`~bytx.errors.PositionAlreadyClosed`; do not retry;
        - timeout, disconnect, "status unknown" (Binance -1006/-1007, Bybit 10000/10016, OKX
          51149/50004, BingX 109500, KuCoin 408000), duplicate client_id (Binance -4116, Bybit
          110072/10014, OKX 51016, KuCoin 300018: an order with it already exists): ``reconcile`` looks up
          by ``client_id`` for up to ~8 s; if not found, :class:`~bytx.errors.OrderStatusUnknown`
          (``retryable=False``, fields ``client_order_id``/``symbol``/``side``/``amount``);
        - any other exchange error without a known code: reconciliation; ≥ 2 successful empty scans mean
          rejection (the original error), otherwise :class:`~bytx.errors.OrderStatusUnknown`.

        ``reconcile=False``: no reconciliation; an unclear outcome raises ``OrderStatusUnknown`` right away.
        The placement request itself is never retried.

        Time budget: the request takes up to the client ``timeout`` (10 s by default),
        reconciliation up to 8 s more; up to ~18 s in total. Overall cap: ``with ex.deadline(sec):``.

        External cancellation (``asyncio.wait_for``, ``task.cancel()``) interrupts reconciliation too:
        the order may have been placed, and the generated ``client_id`` is not visible to the caller.
        If you use your own timeout, pass your own ``client_id`` and look the order up by it.

        Binance / Hyperliquid: TP/SL are placed as separate orders after the main one; if
        they fail, raises :class:`~bytx.errors.ProtectionNotPlaced` (the main order
        is placed and is in ``details['order']``, placed TP/SL in ``details['placed']``).
        """
        request = await self._build_order(symbol, side, size=size, notional=notional,
                                          contracts=contracts, margin=margin, leverage=leverage,
                                          price=price, type=type,
                                          post_only=post_only, time_in_force=time_in_force,
                                          reduce_only=reduce_only, client_id=client_id,
                                          take_profit=take_profit, stop_loss=stop_loss,
                                          params=params, max_round_up=max_round_up)
        order = await self._submit_order(request, reconcile=reconcile)
        if request.get('tp_sl'):
            try:
                await self._protect(order, request)
            except ProtectionNotPlaced:
                # the protection failure is reported to the caller — the plan is retired: recover() will not place
                # these legs later on another position of the same side
                if self.intents is not None and request.get('client_id'):
                    self.intents._retire_plan(self, request['client_id'], 'protection_failed')
                raise
        return order

    async def _attach_tp_sl(self, order: Order, request: dict) -> None:
        """Binance / Hyperliquid: the order has no attached TP/SL, so separate
        reduce-only conditional orders for the same size are placed right after the main one.

        Failure raises :class:`ProtectionNotPlaced` (not the original error class: the main order is already
        placed), ``details``: order, placed (placed TP/SL), client_ids."""
        try:
            orders = await _protection.attach_tp_sl(
                self, request['symbol'], position_side='long' if request['side'] == 'buy' else 'short',
                contracts=request['amount'],
                # HL: the market entry's reference price also works for TP/SL, no second request
                ref_price=request['price'] if request['type'] == 'market' else None,
                **request['tp_sl'])
        except BytxError as e:
            got = e.details if isinstance(e, ProtectionNotPlaced) and isinstance(e.details, dict) else {}
            if got.get('placed'):
                await self._tp_sl_placed(got['placed'])
            cause = e.__cause__ if isinstance(e, ProtectionNotPlaced) and e.__cause__ is not None else e
            msg = cause.message if isinstance(cause, BytxError) else str(cause)
            raise ProtectionNotPlaced(
                f'order {order.id} placed, but TP/SL not placed: {msg}', exchange=self.name,
                code=getattr(cause, 'code', None),
                details={'order': order, 'placed': got.get('placed', []),
                         'client_ids': got.get('client_ids', [])}) from cause
        order.raw['bytx_tp_sl'] = orders
        await self._tp_sl_placed(orders)

    async def place_orders(self, orders: list[dict], *, reconcile: bool = True) -> list[Order]:
        """Batch placement. Each item is :meth:`place_order` kwargs (symbol, side, …).

        All items are validated before sending: an error in one means none is sent.
        The batch goes as one request (createOrders), otherwise in parallel one by one: an exchange without
        a batch endpoint or a market without batches (Binance spot: the Spot API places only via
        ``POST /api/v3/order``; Gate and MEXC futures: ccxt 4.4.65 createOrders does not support them).
        As in :meth:`place_order`: the request is not retried; on timeout/network disconnect
        (``reconcile``) each order is looked up by its ``client_id``; TP/SL on
        Binance / Hyperliquid are placed as separate orders after placement.

        Returns orders in request order. If any item failed, raises
        :class:`~bytx.errors.BatchError`; already placed orders are not lost, ``details``:
        ``results``: per request index, an :class:`Order` or an error (TP/SL not placed:
        :class:`~bytx.errors.ProtectionNotPlaced`, order in its ``details['order']``),
        ``orders``: placed orders, ``errors``: {index: error},
        ``client_ids``: client_ids of all requests (for reconciliation via :meth:`order`).
        An unclear item outcome is handled as in :meth:`place_order`: reconciliation, then
        :class:`~bytx.errors.OrderStatusUnknown` in ``errors``.
        """
        requests = [await self._build_order(**o) for o in orders]
        if not requests:
            return []
        results: list[Union[Order, BytxError]]
        if _batch_endpoint(self._ex, requests):
            # Batch in chunks by the exchange limit (Binance: "Max 5 orders", -4082 on 8, demo
            # 2026-10-29). A chunk rejected as a whole yields errors for its items; other chunks still go.
            # With a single chunk, the previous behavior: its error is re-raised.
            size = _BATCH_MAX.get(self._ex.id) or len(requests)
            chunks = [requests[i:i + size] for i in range(0, len(requests), size)]
            results = []
            rejected: set[int] = set()        # indexes of chunks rejected as a whole (already journaled)
            for chunk in chunks:
                try:
                    results.extend(await self._submit_batch(chunk, reconcile=reconcile))
                except BytxError as e:
                    if self.journal is not None:
                        await self.journal._on_batch(self, chunk, [e] * len(chunk))
                    self._intents_batch(chunk, [e] * len(chunk))
                    if self._observers:
                        for request in chunk:
                            self._emit_failed(request, e)
                    if len(chunks) == 1:
                        raise
                    rejected.update(range(len(results), len(results) + len(chunk)))
                    results.extend([e] * len(chunk))
                    continue
            ok = [(r, x) for i, (r, x) in enumerate(zip(requests, results)) if i not in rejected]
            if self.journal is not None and ok:
                await self.journal._on_batch(self, [r for r, _ in ok], [x for _, x in ok])
            self._intents_batch([r for r, _ in ok], [x for _, x in ok])
            if self._observers:
                for request, result in ok:
                    if isinstance(result, Order):
                        self.emit('order.placed', symbol=result.symbol, order=result,
                                  data={'request': request_view(request)})
                    else:
                        self._emit_failed(request, result)
        else:
            gathered = await asyncio.gather(*(self._submit_order(r, reconcile=reconcile) for r in requests),
                                            return_exceptions=True)
            results = [x if isinstance(x, Order) else self._as_error(x) for x in gathered]
        protect = [i for i, (x, r) in enumerate(zip(results, requests)) if isinstance(x, Order) and r.get('tp_sl')]
        if protect:
            attached = await asyncio.gather(*(self._protect(results[i], requests[i])  # type: ignore[arg-type]
                                              for i in protect), return_exceptions=True)
            for i, a in zip(protect, attached):
                if a is not None:
                    results[i] = self._as_error(a)
        errors = {i: x for i, x in enumerate(results) if isinstance(x, BytxError)}
        if errors:
            placed = [x for x in results if isinstance(x, Order)]
            placed += [x.details['order'] for x in errors.values()
                       if isinstance(x, ProtectionNotPlaced) and isinstance(x.details, dict) and x.details.get('order')]
            first = next(iter(errors.values()))
            raise BatchError(f'place_orders: {len(errors)} of {len(requests)} failed: {first.message or first}',
                             exchange=self.name,
                             details={'results': results, 'orders': placed, 'errors': errors,
                                      'client_ids': [r['client_id'] for r in requests]}) from first
        return results  # type: ignore[return-value]

    def _intents_batch(self, requests: list[dict], results: list) -> None:
        if self.intents is None:
            return
        for request, result in zip(requests, results):
            if isinstance(result, Order):
                self.intents._on_placed(self, result, request['client_id'])
            elif isinstance(result, BaseException):
                self.intents._on_failed(self, request, result)

    def _as_error(self, error: BaseException) -> BytxError:
        """Exception from gather(return_exceptions=True) → SDK error; cancellation is not swallowed."""
        if not isinstance(error, Exception):
            raise error
        return from_ccxt(error, self.name, secrets=ccxt_secrets(self._ex))

    async def _submit_batch(self, requests: list[dict], *, reconcile: bool) -> list[Union[Order, BytxError]]:
        """createOrders in one request; a result per item (Order or error)."""
        cause: Optional[BytxError] = None
        kind = 'unknown'
        for r in requests:
            await self._pre_submit(r)
        try:
            raw = await self._call(self._ex.create_orders, [
                {'symbol': r['symbol'], 'type': r['type'], 'side': r['side'], 'amount': _ccxt_amount(r),
                 'price': r['price'], 'params': r['params']} for r in requests], safe=False)
        except BytxError as e:
            kind = classify_outcome(self._ex.id, e)
            if kind == 'rejected':
                raise  # batch rejected as a whole before execution: nothing to reconcile
            raw, cause = [], e  # outcome unknown for all items
        results: list[Union[Order, BytxError, None]] = []
        for o, r in zip(_match_batch(raw, requests), requests):
            if isinstance(o, dict) and o.get('id'):
                self._remember_algo(o)
                results.append(self._order(o, fallback=r))
                continue
            reason = _element_error(o) if isinstance(o, dict) else None
            if reason is not None:  # exchange rejected the item: no order, nothing to reconcile
                element: BytxError = self._element_rejection(reason, r)
                # "no position" / "exceeds position" / funds, by the item's code
                element = position_error(self._ex.id, element, reduce_only=bool(r.get('reduce_only'))) \
                    or refine_error(self._ex.id, element)
                if isinstance(element, PositionAlreadyClosed) and r.get('reduce_only'):
                    element = await self._verify_position_closed(r, element)   # "covered" is not "closed"
                results.append(element)
            else:
                results.append(None)  # outcome unknown: reconcile by client_id
        unknown = [i for i, x in enumerate(results) if x is None]
        if unknown:
            failure = cause or UnknownOutcome('batch response lacks this order', exchange=self.name)
            if not reconcile:
                for i in unknown:
                    results[i] = self._status_unknown(requests[i], failure, 'reconciliation disabled (reconcile=False)')
                return results  # type: ignore[return-value]
            found = await asyncio.gather(*(self._reconcile(requests[i]['symbol'], requests[i]['client_id'])
                                           for i in unknown))
            for i, (order, ok_scans, scan_error) in zip(unknown, found):
                if order is not None:
                    results[i] = order
                elif kind == 'exchange_error' and cause is not None and ok_scans >= 2 and scan_error is None:
                    results[i] = cause  # error without a known code and ≥ 2 empty scans: rejection
                else:
                    why = ('order not found by client_id within the reconciliation window' if scan_error is None
                           else f'reconciliation failed: {scan_error.message or scan_error}')
                    results[i] = self._status_unknown(requests[i], failure, why)
        return results  # type: ignore[return-value]

    def _element_rejection(self, reason: tuple[Optional[str], str], request: dict) -> BytxError:
        """Batch item rejection → InvalidOrder; a code ccxt knows as insufficient funds
        (Bitget 43012 / 40762) → InsufficientFunds, as with a single place_order. A rate limit (Bybit item 10006 —
        the part of the batch over the limit) is refined by the caller: refine → RateLimited."""
        code, message = reason
        details = {'client_id': request['client_id'], 'symbol': request['symbol']}
        if code:
            exact = (getattr(self._ex, 'exceptions', None) or {}).get('exact') or {}
            cls = exact.get(code)
            if isinstance(cls, type) and issubclass(cls, ccxt.InsufficientFunds):
                return InsufficientFunds(message, exchange=self.name, code=code, details=details)
        return InvalidOrder(message, exchange=self.name, code=code, details=details)

    def _amend_replace_key(self, m: Market) -> Optional[str]:
        """ccxt ``edit_order`` params key for the NEW order's client_id if the exchange amends via cancel
        + new order (new id); None for in-place amendment (order id and client_id are preserved).

        In place: Binance USDⓈ-M (PUT /fapi/v1/order, Modify Order), Bybit (v5/order/amend),
        OKX (trade/amend-order), Gate (PATCH orders/{order_id}). Cancel + new order: Binance spot
        (POST /api/v3/order/cancelReplace, newClientOrderId "Used to identify the new order"),
        BingX (cancelReplace: spot newClientOrderId, swap clientOrderID), Bitget (contracts modify-order,
        spot cancel-replace-order, both newClientOid), Hyperliquid (batchModify: cloid ``c`` of the new order). Without
        our id ccxt sets its own: the bot tag is lost and the order stops being ours.

        KuCoin spot (hf/orders/alter: "Cancel the order and place a new order"): the new order's client_id
        cannot be set and the docs do not promise to keep clientOid, so NotSupported; KuCoin Futures
        inherits the same spot alter in ccxt, so NotSupported. Without editOrder in ccxt (MEXC) or with
        ccxt emulation (HTX: ``'emulated'``: cancel_order, then create_order without our id and without
        atomicity): NotSupported before any network call.
        """
        ex_id = self._ex.id
        if self._ex.has.get('editOrder') is not True:
            raise NotSupported(f'amend_order: {ex_id} does not amend orders on the exchange (ccxt has no editOrder or '
                               'emulates it via cancel + new order without the bot tag); cancel and place '
                               'again', exchange=self.name)
        if ex_id in ('kucoin', 'kucoinfutures'):
            raise NotSupported('amend_order: KuCoin amends via cancel and a new order (hf/orders/alter, '
                               "spot only), the new order's client_id cannot be set, so the bot tag would be lost; "
                               'cancel and place again', exchange=self.name)
        if ex_id == 'binance':
            return None if m.is_contract else 'newClientOrderId'
        if ex_id == 'bingx':
            return 'clientOrderID' if m.is_contract else 'newClientOrderId'
        if ex_id == 'bitget':
            return 'newClientOid'
        if ex_id == 'hyperliquid':
            return 'clientOrderId'
        return None

    async def _amend_prepare(self, s: str, m: Market, order_id: str, side: Optional[Side],
                             size: Optional[float]
                             ) -> tuple[Optional[Side], Optional[float], dict, Optional[tuple[str, str]]]:
        """Side, size, params and (old client_id, new order client_id) for edit_order
        (see _amend_replace_key); None if the id does not change."""
        key = self._amend_replace_key(m)
        side, amount, current = await self._amend_side_amount(s, m, order_id, side, size, need_order=key is not None)
        params: dict = {}
        if self._ex.id == 'bingx' and m.is_contract and current is not None:
            # cancelReplace: positionSide is mandatory (hedge: LONG/SHORT); ccxt without hedged set BOTH,
            # so in hedge the original order was canceled and the new one rejected (109400, VST 2026-10-28)
            position_side = str(((current.raw or {}).get('info') or {}).get('positionSide') or '').upper()
            if position_side in ('BOTH', 'LONG', 'SHORT'):
                params['positionSide'] = position_side
        if key is None or current is None or not current.client_id:
            return side, amount, params, None   # in place, or no tag: nothing to lose
        new_cid = amend_client_id(self._ex, current.client_id, self.market_type, self.client_id_prefix)
        if new_cid is None:
            raise NotSupported(f'amend_order: client_id {current.client_id!r} of order {order_id} cannot be continued '
                               'with a new id in the exchange format; the tag would be lost; cancel and place again',
                               exchange=self.name, details={'symbol': s, 'order_id': order_id,
                                                            'client_id': current.client_id})
        return side, amount, {**params, key: new_cid}, (current.client_id, new_cid)

    async def _amend_done(self, s: str, order_id: str, raw: dict, cids: Optional[tuple[str, str]],
                          order: Optional[Order] = None) -> Order:
        """Order from the edit_order response (or found by reconciliation: ``order``); a new order updates the journal
        (replaced, the old one canceled) and intents (old one closed, new one with the same binding)."""
        new_cid = cids[1] if cids else None
        if order is None:
            order = self._order(raw, fallback={'client_id': new_cid, 'symbol': s} if new_cid else None)
        if order is not None and not order.id and new_cid:
            # Bitget mix modify-order (demo 2026-10-27): {"orderId": "", "clientOid": <new>},
            # the new order is identified only by cid
            order = await self._order_by_new_cid(s, new_cid) or order
        if new_cid and not order.client_id:
            order.client_id = new_cid   # response without id (Hyperliquid statuses): ours was sent
        replaced = cids is not None and bool(order.id) and str(order.id) != str(order_id)
        if self.intents is not None and cids:
            self.intents._on_replaced(self, str(order_id), cids[0], order)
        if self.journal is not None:
            await self.journal._on_replaced(self, order_id, order, old_canceled=replaced)
        return order

    async def _order_by_new_cid(self, s: str, cid: str, attempts: int = 3) -> Optional[Order]:
        for i in range(attempts):
            try:
                return await self.order(s, client_id=cid)
            except OrderNotFound:
                if i + 1 < attempts:
                    await asyncio.sleep(0.3)
        return None

    async def _amend_side_amount(self, s: str, m: Market, order_id: str, side: Optional[Side],
                                 size: Optional[float], *, need_order: bool = False
                                 ) -> tuple[Optional[Side], Optional[float], Optional[Order]]:
        """Side and size (contracts) for edit_order; whatever is missing comes from the exchange order.

        Binance Modify Order (USDⓈ-M): "Both quantity and price must be sent"; without size ccxt
        sent quantity=None (-1102). quantity is the full order size (origQty): a partially filled order with
        quantity <= executedQty is canceled by the exchange. Spot (cancelReplace): new order for the remainder.
        Bitget Modify Order (mix, /api/v2/mix/order/modify-order): newSize and newPrice only together
        (40925 "price or size must be passed in together", demo 2026-10-27).
        BingX swap (cancelReplace): new order with quantity; without size ccxt crashed on
        amount_to_precision(None) (AttributeError, VST 2026-10-28).
        Hyperliquid modify: "order" is the full order spec (OrderRequest with s); without size ccxt
        crashed on Decimal(None) (testnet 2026-10-28).
        ``need_order``: the order is needed anyway (client_id for the new order on cancel + new).
        """
        amount = convert.contracts_from_size(self._ex, m, size) if size is not None else None
        need_amount = amount is None and (self._ex.id in ('binance', 'bingx', 'hyperliquid') or self._both_price_size(m))
        current: Optional[Order] = None
        if side is None or need_amount or need_order:
            current = await self.order(s, order_id)
            side = side or current.side
            if need_amount:
                amount = current.contracts if m.is_contract else current.remaining_contracts
                if not amount:
                    raise ConfigError(f'size of order {order_id} unknown: pass size ({s})',
                                      exchange=self.name, details={'symbol': s, 'order_id': order_id})
        return side, amount, current

    def _both_price_size(self, m: Market) -> bool:
        """Amending an order requires both price and size (Bitget mix modify-order)."""
        return self._ex.id == 'bitget' and m.is_contract

    async def amend_order(self, symbol: str, order_id: str, *, price: Optional[float] = None,
                          size: Optional[float] = None, side: Optional[Side] = None,
                          type: str = 'limit') -> Order:
        """Amends the price and/or size of an open order (without cancelling, where the exchange supports it).

        Rounding as in :meth:`place_order`: size down, limit price toward the passive side.
        If the exchange amends via cancel + new order (new id), the new order gets a client_id with the same
        bot tag (see :meth:`_amend_replace_key`, ``order_params.amend_client_id``); if amending while
        keeping the tag is impossible, raises :class:`~bytx.errors.NotSupported` before any network call.
        """
        if price is None and size is None:
            raise ConfigError(f'price and/or size required ({symbol}, order {order_id})', exchange=self.name,
                              details={'symbol': symbol, 'order_id': order_id})
        await self.connect()
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        side, amount, params, cids = await self._amend_prepare(s, m, order_id, side, size)
        if price is None and self._both_price_size(m):
            price = (await self.order(s, order_id)).price   # size only: keep the old price
        # limit price toward the passive side, as in place_order
        p = (convert.price_to_precision(self._ex, m, price, side if type == 'limit' else None)
             if price is not None else None)
        with self._journal_op(s, [order_id, cids[1] if cids else None]):
            try:
                raw = await self._call(self._ex.edit_order, order_id, s, type, side, amount, p, params, safe=False)
            except BytxError as e:
                kind = classify_outcome(self._ex.id, e, has_client_id=True)
                if kind == 'rejected':
                    raise
                found = await self._amend_reconcile(s, m, order_id, p, amount, cids, e)
                if found is None:
                    if kind == 'exchange_error':
                        raise    # exchange response with an unknown code and no visible change: rejection as is
                    raise self._amend_unknown(s, order_id, amount, cids, e) from e
                return await self._amend_done(s, order_id, found.raw or {}, cids, order=found)
        return await self._amend_done(s, order_id, raw, cids)

    async def _amend_reconcile(self, s: str, m: Market, order_id: str, price: Optional[float],
                               amount: Optional[float], cids: Optional[tuple[str, str]],
                               error: BytxError) -> Optional[Order]:
        """No or unclear response to the amendment: determine the outcome from the exchange. Replacement: the new order by
        our client_id; in place: the order with the requested price/size. Not found: None."""
        if cids is not None:
            found, _, _ = await self._reconcile(s, cids[1])
            if found is not None:
                return found
        else:
            for delay in self._RECONCILE_DELAYS[:3]:
                await asyncio.sleep(delay)
                try:
                    current = await self.order(s, order_id)
                except BytxError:
                    continue
                same_price = price is None or (current.price is not None
                                               and math.isclose(current.price, price, rel_tol=1e-9))
                same_amount = amount is None or (current.contracts is not None
                                                 and math.isclose(current.contracts, amount, rel_tol=1e-9))
                if same_price and same_amount:
                    return current
                if current.status not in ('open', None):
                    break
        return None

    def _amend_unknown(self, s: str, order_id: str, amount: Optional[float], cids: Optional[tuple[str, str]],
                       error: BytxError) -> OrderStatusUnknown:
        """A blind retry on replacing exchanges would create a second order, hence "outcome unknown"."""
        unknown = OrderStatusUnknown(
            f'outcome of amending order {order_id} unknown ({s}): no response received, no change visible '
            f'on the exchange; check the order before retrying: {error.message or error}', exchange=self.name,
            code=error.code, details={'symbol': s, 'order_id': order_id,
                                      'client_id': cids[1] if cids else None},
            client_order_id=cids[1] if cids else None, exchange_order_id=str(order_id), symbol=s,
            amount=amount)
        unknown.__cause__ = error
        return unknown

    async def cancel_order(self, symbol: str, order_id: Optional[str] = None, *,
                           client_id: Optional[str] = None, strict: bool = False) -> Order:
        """Cancels an order by exchange id or by your own ``client_id``.

        If the order is no longer in the book (filled, canceled earlier, expired), this is not
        an exception: returns the order with its **final status** from the exchange (``'closed'`` means filled,
        ``'canceled'``, ``'expired'``, ``'rejected'``). A "not open" rejection is recognized by
        :class:`~bytx.errors.OrderNotFound` and doc codes (Binance -2011/-2013, Bybit
        110001/110008/110010, OKX 51400–51402/51603, BingX 109421, KuCoin 100004). If
        the order is not found afterwards either, the original error (OrderNotFound) is raised.

        ``strict=True`` — no status read: if the order is no longer in the book, the exchange error is raised at once
        (:class:`~bytx.errors.OrderNotFound` / :class:`~bytx.errors.InvalidOrder` with its ``code``, e.g. Binance
        -2011).

        Binance: an algoId (TP/SL) is canceled via the Algo API even after a restart (without process memory).
        """
        await self.connect()
        order, _ = await self._cancel(self.symbol(symbol), order_id, client_id, strict=strict)
        return order

    async def _cancel(self, s: str, order_id: Optional[str], client_id: Optional[str], *,
                      strict: bool = False) -> tuple[Order, bool]:
        """Cancel → (order, whether we canceled it). False means the order was already in a final status."""
        with self._journal_op(s, [order_id, client_id]):
            order, ours = await self._cancel_one(s, order_id, client_id, strict=strict)
        if self.intents is not None:
            self.intents._on_update(self, order)
        if self.journal is not None:
            await self.journal._on_canceled(self, order, ours)
        if self._observers:
            self.emit('order.canceled', symbol=order.symbol or s, order=order, data={'ours': ours})
        return order, ours

    async def _cancel_one(self, s: str, order_id: Optional[str], client_id: Optional[str], *,
                          strict: bool = False) -> tuple[Order, bool]:
        oid = str(order_id) if order_id else None
        if oid and oid in self._binance_algo_ids:
            try:
                raw = await self._binance_algo('DELETE', s, oid)
            except BytxError as e:
                if strict or not order_not_open(self._ex.id, e):
                    raise
                return await self._already_closed(s, oid, None, e), False
            self._binance_algo_ids.discard(oid)
        elif oid:
            try:
                raw = await self._call(self._ex.cancel_order, oid, s)
            except BytxError as e:
                if not order_not_open(self._ex.id, e):
                    raise
                # Binance: may be an algoId (TP/SL not created here or before a restart); only
                # if no such id exists among regular orders and an algo order with it exists for this symbol
                if not await self._is_unregistered_algo(s, oid):
                    if strict:
                        raise
                    return await self._already_closed(s, oid, None, e), False
                self._binance_algo_ids.add(oid)
                return await self._cancel_one(s, oid, None, strict=strict)  # now the Algo API branch
        elif client_id:
            try:
                raw = await self._call(self._ex.cancel_order, None, s, cancel_client_id_params(self._ex, client_id))
            except BytxError as e:
                if strict or not order_not_open(self._ex.id, e):
                    raise
                return await self._already_closed(s, None, client_id, e), False
        else:
            raise ConfigError(f'order_id or client_id is required ({s})', exchange=self.name, details={'symbol': s})
        return self._canceled(raw, s, oid, client_id), True

    async def _already_closed(self, s: str, order_id: Optional[str], client_id: Optional[str],
                              error: BytxError) -> Order:
        """Cancel replied "not open": final status from the exchange; if unknown, the original error."""
        try:
            order = await self.order(s, order_id, client_id=client_id)
        except BytxError as lookup:
            raise error from lookup
        if order.status in (None, 'open'):
            raise error  # exchange sees the order as open but the cancel was rejected: unclear outcome, caller decides
        return order

    def _canceled(self, raw: dict, s: str, order_id: Optional[str] = None,
                  client_id: Optional[str] = None) -> Order:
        # cancel responses often lack id/symbol (KuCoin cancelledOrderIds, HL statuses: ["success"])
        order = self._order(raw, fallback={'id': order_id, 'client_id': client_id, 'symbol': s})
        if order.status in (None, 'success'):
            order.status = 'canceled'  # exchange confirmed the cancel without an order status
        return order

    async def cancel_orders(self, symbol: str, order_ids: list[str]) -> list[Order]:
        """Cancels orders by id. Returns the canceled ones in ``order_ids`` order.

        Binance algo ids (TP/SL) go to the Algo API, as in :meth:`cancel_order`. Orders
        are canceled independently: if some fail, raises :class:`~bytx.errors.BatchError`,
        ``details``: ``results``: per index, Order or error, ``orders``: canceled ones,
        ``errors``: {order_id: error}. An order that is no longer in the book is a "not open" error
        (:class:`~bytx.errors.OrderNotFound`, :func:`bytx.outcome.order_not_open`).

        Batch cancel (:mod:`bytx.batch_cancel`, per the docs): Bybit (linear/spot) — 10 per request, OKX — 20,
        Binance USDⓈ-M — 10; the response is parsed per order (item code: "no such order", a rate limit — RateLimited;
        an item rejected by the rate limit is retried once by a single cancel). Other exchanges — a ccxt batch
        (``cancelOrders``) or one by one.
        """
        await self.connect()
        s = self.symbol(symbol)
        ids = [str(i) for i in order_ids]
        results: dict[int, Union[Order, BytxError]] = {}
        single = [i for i, oid in enumerate(ids) if oid in self._binance_algo_ids]
        batch = [i for i in range(len(ids)) if i not in set(single)]
        market = self._ex.market(s)
        size = _batch_cancel.batch_size(self._ex.id, market) if batch and self._ex.has.get('cancelOrders') else 0
        chunks: list[list[int]] = []
        if size:
            chunks, lane = _batch_cancel.plan(batch, size)
            single += lane
        elif batch and self._ex.has.get('cancelOrders') and self._ex.id not in _batch_cancel.BATCH_MAX:
            await self._cancel_ccxt_batch(s, ids, batch, results)
        else:
            single += batch

        async def chunk(idx: list[int]) -> None:
            for i, x in zip(idx, await self._cancel_chunk(s, market, [ids[i] for i in idx])):
                results[i] = x

        gathered = await asyncio.gather(*(chunk(c) for c in chunks),
                                        *(self.cancel_order(s, ids[i]) for i in single), return_exceptions=True)
        for i, x in zip(single, gathered[len(chunks):]):
            results[i] = x if isinstance(x, Order) else self._as_error(cast(BaseException, x))
        for c, x in zip(chunks, gathered[:len(chunks)]):
            if isinstance(x, BaseException):  # parsing does not raise; just in case — an error for each order of the batch
                for i in c:
                    results.setdefault(i, self._as_error(x))
        # a batch item rejected by the rate limit (Bybit 10006: «the part that exceeds the limit will fail»; demo
        # 2026-11-04 — 3 of 129 at 9 orders/s) — retried once by a single cancel: the exchange limits it separately
        limited = [i for c in chunks for i in c if isinstance(results.get(i), RateLimited)]
        if limited:
            again = await asyncio.gather(*(self.cancel_order(s, ids[i]) for i in limited), return_exceptions=True)
            for i, x in zip(limited, again):
                results[i] = x if isinstance(x, Order) else self._as_error(cast(BaseException, x))
        ordered = [results[i] for i in range(len(ids))]
        errors = {ids[i]: x for i, x in enumerate(ordered) if isinstance(x, BytxError)}
        if errors:
            first = next(iter(errors.values()))
            raise BatchError(f'cancel_orders {s}: {len(errors)} of {len(ids)} not canceled: {first.message or first}',
                             exchange=self.name,
                             details={'results': ordered, 'orders': [x for x in ordered if isinstance(x, Order)],
                                      'errors': errors}) from first
        return ordered  # type: ignore[return-value]

    async def _cancel_chunk(self, s: str, market: dict, ids: list[str]) -> list[Union[Order, BytxError]]:
        """One batch cancel request (:mod:`bytx.batch_cancel`): the outcome per id — Order or an error."""
        method, params = _batch_cancel.request(self._ex.id, market, ids)
        error: Optional[BytxError] = None
        try:
            with self._journal_op(s, ids):
                raw = await self._call(getattr(self._ex, method), params)
        except BytxError as e:
            raw, error = _batch_cancel.error_body(e), e     # OKX code 1: the item codes are in the response body
        matched = _batch_cancel.match(ids, _batch_cancel.elements(self._ex.id, raw))
        out: list[Union[Order, BytxError]] = []
        for oid, item in zip(ids, matched):
            if item is None:
                out.append(error or ExchangeError(f'order {oid}: no cancel response', exchange=self.name,
                                                  details={'symbol': s, 'order_id': oid}))
                continue
            _, code, msg, row = item
            if code is not None:
                out.append(self._element_cancel_error(code, msg or code, s, oid, market))
                continue
            order = self._canceled(self._ex.parse_order(row, market), s, oid)
            out.append(order)
            if self.intents is not None:
                self.intents._on_update(self, order)
            if self.journal is not None:
                await self.journal._on_canceled(self, order, True)
            if self._observers:
                self.emit('order.canceled', symbol=order.symbol or s, order=order, data={'ours': True})
        return out

    def _element_cancel_error(self, code: str, msg: str, s: str, oid: str, market: dict) -> BytxError:
        """A batch cancel item rejection by the exchange code — the same class as a single cancel with this code
        (the market's ccxt ``exceptions`` table — Binance has its own for linear/inverse/spot, then the shared one; the
        SDK refinement — RateLimited etc.); "the order is not in the book" (ORDER_NOT_OPEN table) — always
        OrderNotFound."""
        details = {'symbol': s, 'order_id': oid}
        table = self._ex.exceptions if isinstance(getattr(self._ex, 'exceptions', None), dict) else {}
        kind = 'inverse' if market.get('inverse') else 'linear' if market.get('contract') else 'spot'
        by_market: dict = table.get(kind) if isinstance(table.get(kind), dict) else {}  # type: ignore[assignment]
        cls = (by_market.get('exact') or {}).get(code) or (table.get('exact') or {}).get(code) or ccxt.ExchangeError
        err = from_ccxt(cls(json.dumps({'code': code, 'msg': msg})), self.name)
        err.message, err.code, err.details = msg, code, details
        if order_not_open(self._ex.id, err) and not isinstance(err, OrderNotFound):
            return OrderNotFound(msg, exchange=self.name, code=code, details=details)
        return err

    async def _cancel_ccxt_batch(self, s: str, ids: list[str], batch: list[int],
                                 results: dict[int, Union[Order, BytxError]]) -> None:
        """A ccxt ``cancel_orders`` batch as a whole (exchanges without their own batch cancel in
        :mod:`bytx.batch_cancel`)."""
        error: Optional[BytxError] = None
        try:
            with self._journal_op(s, [ids[i] for i in batch]):
                raw = await self._call(self._ex.cancel_orders, [ids[i] for i in batch], s)
        except BytxError as e:
            raw, error = [], e
        rows = [o for o in raw if isinstance(o, dict)] if isinstance(raw, list) else []
        if rows and all(o.get('id') for o in rows):
            # all items have an id: match by id (BingX returns only successful ones)
            by_id = {str(o['id']): o for o in rows}
            matched: list[Optional[dict]] = [by_id.get(ids[i]) for i in batch]
        else:
            matched = [o for _, o in zip(batch, [*rows, *[None] * len(batch)])]
        market = self._ex.market(s)
        for i, o in zip(batch, matched):
            # Bitget batch-cancel-orders: a failureList record has orderId and errorCode (exchanges/bitget/ccxt_fixes)
            check = isinstance(o, dict) and (not o.get('id') or self._ex.id == 'bitget')
            reason = _element_error(o) if check else None
            if error is not None:
                results[i] = error
            elif reason is not None and reason[0]:
                # item code — the same class as a single cancel; "no such order" (ORDER_NOT_OPEN, Bitget 40819) —
                # OrderNotFound, as cancel_orders promises and as _cancel_chunk parses it
                results[i] = self._element_cancel_error(reason[0], reason[1], s, ids[i], market)
            elif reason is not None:
                results[i] = refine_error(self._ex.id, ExchangeError(reason[1], exchange=self.name))
            elif not isinstance(o, dict):
                results[i] = ExchangeError(f'order {ids[i]}: no cancel response', exchange=self.name)
            else:
                results[i] = self._canceled(o, s, ids[i])
                if self.intents is not None:
                    self.intents._on_update(self, results[i])  # type: ignore[arg-type]
                if self.journal is not None:
                    await self.journal._on_canceled(self, results[i], True)  # type: ignore[arg-type]

    async def cancel_all(self, symbol: Optional[str] = None, *, client_id_prefix: Optional[str] = None,
                         order_ids: Optional[Iterable[str]] = None, all_orders: bool = False) -> CancelReport:
        """Cancels **your own** orders and TP/SL with a per-order report.

        Exactly one mode:

        - ``client_id_prefix`` (default: the client's ``client_id_prefix``): only orders and
          TP/SL whose client_id starts with the bot tag (:func:`bytx.owns`); without ``symbol``,
          across all symbols. TP/SL without a client_id (Bybit trading-stop) are not considered ours;
        - ``order_ids`` (requires ``symbol``): only these ids, regular orders and TP/SL, including
          Binance/OKX algo orders by ids from a DB after a restart;
        - ``all_orders=True``: blind cancellation of the whole book (and all TP/SL), explicit only.

        Without a tag, ``order_ids`` or ``all_orders``, raises :class:`ConfigError` and nothing is canceled.
        TP/SL outside the open orders list (Binance Algo, OKX algo, KuCoin stop) are canceled separately.

        Returns :class:`~bytx.types.CancelReport`: per order ``canceled`` /
        ``already_closed`` (already filled/canceled, final status) / ``error``. Orders
        are canceled independently — in batches where the exchange supports them (:meth:`cancel_orders`),
        otherwise one by one; in ``all_orders`` mode exchanges with a "cancel all by symbol" endpoint clear
        the book in one request (statuses from the open orders snapshot).

        If there are errors, raises :class:`~bytx.errors.BatchError` after all cancellations, ``details``:
        ``report``, ``done``: {symbol: canceled}, ``failed``: {symbol: first error}
        (``'*'``: the TP/SL list was not received).
        """
        await self.connect()
        ids = [str(i) for i in order_ids] if order_ids is not None else None
        if sum((ids is not None, client_id_prefix is not None, bool(all_orders))) > 1:
            raise ConfigError('cancel_all: pass exactly one of client_id_prefix / order_ids / all_orders',
                              exchange=self.name)
        if ids is not None and symbol is None:
            raise ConfigError('cancel_all(order_ids=...): symbol is required', exchange=self.name)
        prefix = client_id_prefix if client_id_prefix is not None else (
            None if ids is not None or all_orders else self.client_id_prefix)
        if ids is None and not prefix and not all_orders:
            raise ConfigError('cancel_all: whose orders to cancel? client_id_prefix= (or create_exchange('
                              'client_id_prefix=)), order_ids= or explicitly all_orders=True', exchange=self.name)
        s = self.symbol(symbol) if symbol else None
        if ids is not None:
            report = await self._cancel_ids(cast(str, s), ids)
        else:
            report = await self._cancel_listed(s, prefix)
        if report.errors:
            failed: dict[str, BytxError] = {}
            for r in report.errors:
                failed.setdefault(r.symbol, self._as_error(r.error or ExchangeError('?', exchange=self.name)))
            done: dict[str, int] = {}
            for r in report.canceled:
                done[r.symbol] = done.get(r.symbol, 0) + 1
            first = next(iter(failed.values()))
            raise BatchError(f'cancel_all: {len(report.errors)} of {len(report.results)} not canceled '
                             f'({", ".join(failed)}): {first.message or first}', exchange=self.name,
                             details={'report': report, 'done': done, 'failed': failed}) from first
        return report

    async def _cancel_each(self, s: str, items: list[tuple[str, Optional[str]]]) -> list[CancelResult]:
        """Cancel one by one (id, client_id) with a final status; at most 8 at a time."""
        gate = asyncio.Semaphore(8)

        async def one(oid: str, cid: Optional[str]) -> CancelResult:
            async with gate:
                try:
                    order, ours = await self._cancel(s, oid, None)
                except Exception as e:
                    return CancelResult(oid, s, 'error', cid, error=self._as_error(e))
            return CancelResult(oid, s, 'canceled' if ours else 'already_closed', cid or order.client_id,
                                kind='tp_sl' if order.trigger_price else 'order', order=order)

        return list(await asyncio.gather(*(one(oid, cid) for oid, cid in items)))

    async def _cancel_many(self, s: str, items: list[tuple[str, Optional[str]]]) -> list[CancelResult]:
        """Cancel own orders (id, client_id) in batches (:meth:`cancel_orders`); "the order is not in the book" — the
        final status from the exchange, as in a one-by-one cancel. Without a batch cancel per the docs — one by one
        (:meth:`_cancel_each`)."""
        market = self._ex.market(s)
        if len(items) < 2 or not self._ex.has.get('cancelOrders') or not _batch_cancel.batch_size(self._ex.id, market):
            return await self._cancel_each(s, items)
        ids = [oid for oid, _ in items]
        try:
            got: list[Union[Order, BytxError]] = list(await self.cancel_orders(s, ids))
        except BatchError as e:
            got = list((e.details or {}).get('results') or [])
            if len(got) != len(ids):
                return await self._cancel_each(s, items)
        out: list[CancelResult] = []
        for (oid, cid), x in zip(items, got):
            if isinstance(x, Order):
                out.append(CancelResult(oid, s, 'canceled', cid or x.client_id, order=x))
            elif order_not_open(self._ex.id, x):
                try:
                    order = await self._already_closed(s, oid, None, x)
                    out.append(CancelResult(oid, s, 'already_closed', cid or order.client_id,
                                            kind='tp_sl' if order.trigger_price else 'order', order=order))
                except Exception as e:  # noqa: BLE001 — the final status is unknown: the original error
                    out.append(CancelResult(oid, s, 'error', cid, error=self._as_error(e)))
            else:
                out.append(CancelResult(oid, s, 'error', cid, error=x))
        return out

    async def _cancel_tp_sl_list(self, s: str, orders: list[TpSlOrder]) -> list[CancelResult]:
        out = []
        with self._journal_op(s, [o.id for o in orders] + [o.client_id for o in orders]):
            cancelled = await _protection.cancel_tp_sl_each(self, s, orders)
        for o, r in zip(orders, cancelled):
            if r is None:
                o.status = 'canceled'
                out.append(CancelResult(o.id, s, 'canceled', o.client_id, kind='tp_sl', order=o))
            elif isinstance(r, OrderNotFound):
                out.append(CancelResult(o.id, s, 'already_closed', o.client_id, kind='tp_sl'))
            else:
                out.append(CancelResult(o.id, s, 'error', o.client_id, kind='tp_sl', error=self._as_error(r)))
        return out

    async def _cancel_listed(self, symbol: Optional[str], prefix: Optional[str]) -> CancelReport:
        """cancel_all by tag (prefix) or the whole book (prefix=None)."""
        orders = await self.open_orders(symbol)
        mine = [o for o in orders if prefix is None or owns(o, prefix, self._ex.id)]
        by_symbol: dict[str, list[Order]] = {}
        for o in mine:
            by_symbol.setdefault(o.symbol, []).append(o)
        report = CancelReport()
        try:
            conditional = await _protection.conditional_symbols(self, symbol)
        except BytxError as e:
            conditional = set()
            report.results.append(CancelResult(None, '*', 'error', kind='tp_sl', error=e))
        bulk = prefix is None and bool(self._ex.has.get('cancelAllOrders'))

        async def one(s: str) -> list[CancelResult]:
            out: list[CancelResult] = []
            listed = by_symbol.get(s, [])
            if listed and bulk:
                try:
                    with self._journal_op(s):
                        await self._call(self._ex.cancel_all_orders, s)
                    out += [CancelResult(o.id, s, 'canceled', o.client_id, order=o) for o in listed]
                except BytxError as e:
                    out += [CancelResult(o.id, s, 'error', o.client_id, error=e) for o in listed]
            elif listed:
                out += await self._cancel_many(s, [(o.id, o.client_id) for o in listed])
            if s in conditional:  # phases are independent: TP/SL are canceled even if regular orders failed
                try:
                    tps = [t for t in await _protection.tp_sl_orders(self, s)
                           if t.id and (prefix is None or owns(t, prefix, self._ex.id))]
                except BytxError as e:
                    return out + [CancelResult(None, s, 'error', kind='tp_sl', error=e)]
                out += await self._cancel_tp_sl_list(s, tps)
            return out

        symbols = list(dict.fromkeys([*by_symbol, *sorted(conditional)]))
        for s, got in zip(symbols, await asyncio.gather(*(one(s) for s in symbols), return_exceptions=True)):
            if isinstance(got, BaseException):
                report.results.append(CancelResult(None, s, 'error', error=self._as_error(got)))
            else:
                report.results.extend(got)
        return report

    async def _cancel_ids(self, s: str, ids: list[str]) -> CancelReport:
        """cancel_all(order_ids=): regular and conditional orders by exchange id, in ``ids`` order."""
        tp_by_id: dict[str, TpSlOrder] = {}
        if s in await _protection.conditional_symbols(self, s):
            try:
                tp_by_id = {t.id: t for t in await _protection.tp_sl_orders(self, s) if t.id}
            except BytxError:
                tp_by_id = {}  # list not received: ids go through regular cancel (a Binance algo is found by id)
        results: dict[str, CancelResult] = {}
        tps = [tp_by_id[i] for i in dict.fromkeys(ids) if i in tp_by_id]
        for r in await self._cancel_tp_sl_list(s, tps):
            results[cast(str, r.id)] = r
        regular = [i for i in dict.fromkeys(ids) if i not in tp_by_id]
        for r in await self._cancel_each(s, [(i, None) for i in regular]):
            if r.status == 'error' and isinstance(r.error, OrderNotFound) and self._ex.id in ('okx', 'kucoinfutures'):
                # algo/stop order no longer among active ones: final status by id
                try:
                    tp = await _protection.tp_sl_order(self, s, cast(str, r.id))
                except BytxError:
                    tp = None
                if tp is not None and tp.status not in (None, 'open'):
                    r = CancelResult(r.id, s, 'already_closed', tp.client_id, kind='tp_sl', order=tp)
            results[cast(str, r.id)] = r
        return CancelReport([results[i] for i in dict.fromkeys(ids)])

    async def close_position(self, symbol: str, side: Optional[PositionSide] = None, *,
                             client_id: Optional[str] = None) -> Optional[Order]:
        """Closes the position with a market reduce-only order. None if there is no position.

        ``side`` as in :meth:`position`: an invalid side or hedge with long and short
        without ``side`` raises :class:`ConfigError` and nothing is sent. A reduce-only rejection
        that may leave the position (OKX 51170, 51205) raises :class:`InvalidOrder`, not None.
        An ambiguous "no position" answer (Binance -2022, Bybit 110017, BingX 101212, HTX 1048 — it also means "the
        side is fully covered by resting closing orders") is re-checked by reading the position (as for any
        reduce-only, :meth:`_unplaced`): the position remains → :class:`~bytx.errors.PositionCovered`, closed — None,
        read failure — :class:`InvalidOrder` with ``details['position_unverified']``.
        """
        pos = await self.position(symbol, side)
        if pos is None:
            return None
        order_side: Side = 'sell' if pos.side == 'long' else 'buy'
        try:
            request = await self._build_order(pos.symbol, order_side, contracts=pos.contracts, reduce_only=True,
                                              client_id=client_id, position_margin_mode=pos.margin_mode,
                                              journal_role='close')
            return await self._submit_order(request)
        except PositionAlreadyClosed:
            return None  # closed between reading and the order (normal race, do not retry)

    async def reduce_position(self, symbol: str, side: Optional[PositionSide] = None, *,
                              size: Optional[float] = None, contracts: Optional[float] = None,
                              client_id: Optional[str] = None, reconcile: bool = True) -> Order:
        """Reduces the position with a market reduce-only order, checking the size before sending.

        Exactly one of ``size`` (base) / ``contracts``. ``side`` as in :meth:`position`.
        No position raises :class:`~bytx.errors.PositionAlreadyClosed`; a size (after rounding
        to the step) larger than the position raises :class:`~bytx.errors.ReduceExceedsPosition`
        (``details``: ``requested``, ``position``, in contracts); nothing is sent.
        A size equal to the position closes it fully. A position closed between reading and the order
        also gives ``PositionAlreadyClosed`` (by exchange code). Otherwise as :meth:`place_order`.
        """
        pos = await self.position(symbol, side)
        if pos is None:
            raise PositionAlreadyClosed(f'no {symbol} position ({side or "any side"}); nothing to reduce',
                                        exchange=self.name, details={'symbol': symbol, 'side': side})
        order_side: Side = 'sell' if pos.side == 'long' else 'buy'
        request = await self._build_order(pos.symbol, order_side, size=size, contracts=contracts,
                                          reduce_only=True, client_id=client_id,
                                          position_margin_mode=pos.margin_mode)
        if request['amount'] > pos.contracts * (1 + 1e-9):
            raise ReduceExceedsPosition(
                f'size {request["amount"]} contracts exceeds position {pos.contracts} ({pos.symbol} {pos.side})',
                exchange=self.name, details={'symbol': pos.symbol, 'side': pos.side,
                                             'requested': request['amount'], 'position': pos.contracts})
        # snapping to a whole lot never pushes the size past the position, even by float error
        request['amount'] = min(request['amount'], pos.contracts)
        return await self._submit_order(request, reconcile=reconcile)

    def _check_position_side(self, side: Optional[str]) -> None:
        if side is not None and side not in ('long', 'short'):
            raise ConfigError(f"side: 'long' or 'short' (position side), not {side!r}", exchange=self.name)

    async def _close_positions(self, positions: list[Position], what: str) -> list[Order]:
        """Closes positions independently; partial failure raises BatchError (orders, failed)."""

        async def close(p: Position) -> Optional[Order]:
            request = await self._build_order(p.symbol, 'sell' if p.side == 'long' else 'buy', contracts=p.contracts,
                                              reduce_only=True, position_margin_mode=p.margin_mode,
                                              journal_role='close')
            try:
                return await self._submit_order(request)
            except PositionAlreadyClosed:
                return None  # closed between reading and the order: normal race

        gathered = await asyncio.gather(*(close(p) for p in positions), return_exceptions=True)
        orders = [x for x in gathered if isinstance(x, Order)]
        failed = {(p.symbol, p.side): self._as_error(x) for p, x in zip(positions, gathered)
                  if isinstance(x, BaseException)}
        if failed:
            first = next(iter(failed.values()))
            raise BatchError(f'{what}: {len(failed)} of {len(positions)} not closed: {first.message or first}',
                             exchange=self.name, details={'orders': orders, 'failed': failed}) from first
        return orders

    async def close_all(self, symbol: str, side: Optional[PositionSide] = None) -> list[Order]:
        """Closes positions of **one symbol** with market reduce-only orders.

        ``side``: position side 'long'/'short' (None: both sides in hedge); anything else raises
        ConfigError. All account positions only explicitly, via :meth:`close_everything`.
        Positions are closed independently: if some fail, raises :class:`BatchError`,
        ``details``: ``orders``: close orders sent, ``failed``: {(symbol, side): error}.
        """
        self._check_position_side(side)
        s = self.symbol(symbol)
        positions = [p for p in await self.positions([s]) if p.symbol == s and (side is None or p.side == side)]
        return await self._close_positions(positions, 'close_all')

    async def close_everything(self) -> list[Order]:
        """Closes **all** account positions on all symbols (formerly ``close_all()``).

        Errors as in :meth:`close_all`.
        """
        return await self._close_positions(await self.positions(), 'close_everything')

    async def kill_switch(self, symbol: str, side: Optional[PositionSide] = None, *,
                          client_id_prefix: Optional[str] = None, all_orders: bool = False) -> dict:
        """Emergency stop for a symbol: cancel orders and TP/SL, close the position.

        Cancellation as in :meth:`cancel_all` (``client_id_prefix`` / client tag or explicit
        ``all_orders=True``; without them, ConfigError before any action). ``side`` limits only
        position closing. Whole account: :meth:`kill_everything`.

        Phases are independent: positions are closed even if cancellation failed. Returns
        ``{'cancelled': count, 'closed': count, 'report': CancelReport}``. If a phase failed, raises
        :class:`~bytx.errors.BatchError` after both, ``details``: ``cancelled``, ``closed``
        (close orders), ``report``, ``errors``: {'cancel_all' / 'close_all': error}.
        """
        self._check_position_side(side)
        return await self._kill(lambda: self.cancel_all(symbol, client_id_prefix=client_id_prefix,
                                                        all_orders=all_orders),
                                lambda: self.close_all(symbol, side), 'kill_switch')

    async def kill_everything(self) -> dict:
        """Emergency stop for the whole account (formerly ``kill_switch()``): blind cancellation of all
        orders and TP/SL (``cancel_all(all_orders=True)``) and closing of all positions
        (:meth:`close_everything`). Report and errors as in :meth:`kill_switch`."""
        return await self._kill(lambda: self.cancel_all(all_orders=True), self.close_everything, 'kill_everything')

    async def _kill(self, cancel: Callable[[], Awaitable[CancelReport]],
                    close: Callable[[], Awaitable[list[Order]]], what: str) -> dict:
        errors: dict[str, BytxError] = {}
        report = CancelReport()
        closed: list[Order] = []
        try:
            report = await cancel()
        except ConfigError:
            raise  # invalid call: nothing done, positions are left alone
        except Exception as e:
            errors['cancel_all'] = err = from_ccxt(e, self.name, secrets=ccxt_secrets(self._ex))
            if isinstance(err, BatchError) and isinstance(err.details, dict):
                report = err.details.get('report') or report
        try:
            closed = await close()
        except Exception as e:
            errors['close_all'] = err = from_ccxt(e, self.name, secrets=ccxt_secrets(self._ex))
            if isinstance(err, BatchError) and isinstance(err.details, dict):
                closed = list(err.details.get('orders', []))
        cancelled = len(report.canceled)
        if errors:
            first = next(iter(errors.values()))
            raise BatchError(f'{what}: failed {", ".join(errors)}: {first.message or first}',
                             exchange=self.name,
                             details={'cancelled': cancelled, 'closed': closed, 'report': report,
                                      'errors': errors}) from first
        return {'cancelled': cancelled, 'closed': len(closed), 'report': report}

    async def flatten(self, symbol: str, side: PositionSide, *, client_id_prefix: Optional[str] = None,
                      timeout: float = 10.0, poll: float = 0.5) -> FlattenReport:
        """Step-by-step position exit with a report.

        1. ``orders``: cancel **your own** regular orders on the symbol (by ``client_id_prefix`` or the client
           tag; without a tag, ConfigError before any action);
        2. ``tp_sl``: cancel your own TP/SL for ``side``;
        3. ``close``: close the ``side`` position with a reduce-only order (:meth:`close_position`);
        4. wait until the position is gone (REST, every ``poll`` s, up to ``timeout`` s).

        Steps are independent: a step failure is recorded in ``errors`` and does not stop the next ones
        (the position is closed even if cancellation failed). Does not raise on exchange failures:
        check :attr:`FlattenReport.ok` (``flat`` and no errors).
        """
        if side not in ('long', 'short'):
            raise ConfigError(f"flatten: side is the position side 'long' or 'short', not {side!r}",
                              exchange=self.name)
        prefix = client_id_prefix if client_id_prefix is not None else self.client_id_prefix
        if not prefix:
            raise ConfigError('flatten cancels only your own orders: client_id_prefix= is required '
                              '(or create_exchange(client_id_prefix=))', exchange=self.name)
        await self.connect()
        s = self.symbol(symbol)
        report = FlattenReport(symbol=s, side=side)
        try:
            mine = [o for o in await self.open_orders(s) if owns(o, prefix, self._ex.id)
                    and _protection._classify(self._ex.id, o.raw if isinstance(o.raw, dict) else {}) is None]
            report.orders = CancelReport(await self._cancel_each(s, [(o.id, o.client_id) for o in mine]))
            if report.orders.errors:
                report.errors['orders'] = cast(Exception, report.orders.errors[0].error)
        except Exception as e:
            report.errors['orders'] = self._as_error(e)
        if self.market_type != 'spot':
            try:
                tps = [t for t in await _protection.tp_sl_orders(self, s, side) if t.id and owns(t, prefix, self._ex.id)]
                report.tp_sl = CancelReport(await self._cancel_tp_sl_list(s, tps))
                if report.tp_sl.errors:
                    report.errors['tp_sl'] = cast(Exception, report.tp_sl.errors[0].error)
            except NotSupported:
                pass
            except Exception as e:
                report.errors['tp_sl'] = self._as_error(e)
        try:
            report.close = await self.close_position(s, side)
        except Exception as e:
            report.errors['close'] = self._as_error(e)
        loop = asyncio.get_running_loop()
        until = loop.time() + max(timeout, 0.0)
        while True:
            try:
                if await self.position(s, side) is None:
                    report.flat = True
                    report.errors.pop('wait', None)
                    break
            except Exception as e:
                report.errors['wait'] = self._as_error(e)
            if loop.time() + poll > until:
                break
            await asyncio.sleep(poll)
        if not report.flat and 'wait' not in report.errors:
            report.errors['wait'] = RequestTimeout(f'position {s} {side} not closed within {timeout} s',
                                                   exchange=self.name, details={'symbol': s, 'side': side})
        return report

    async def transfer(self, currency: str, amount: float, from_account: str, to_account: str) -> Transfer:
        """Transfer between exchange accounts (e.g. 'spot' → 'swap')."""
        await self.connect()
        self._require('transfer')
        raw = await self._call(self._ex.transfer, currency.upper(), amount, from_account, to_account,
                               safe=False)
        return Transfer(raw.get('id'), currency.upper(), amount, from_account, to_account,
                        raw.get('status'), raw)

    # ═══ position protection: TP/SL, dead-man switch, risk guard ═════
    # Implemented in bytx.protection; only thin methods here.

    risk: Optional[RiskGuard] = None  # create_exchange(risk=RiskGuard(...))

    async def set_tp_sl(self, symbol: str, *, take_profit: Optional[float] = None,
                        stop_loss: Optional[float] = None, side: Optional[str] = None,
                        size: Optional[float] = None, trigger_by: Optional[TriggerBy] = None,
                        take_profit_client_id: Optional[str] = None,
                        stop_loss_client_id: Optional[str] = None,
                        contracts: Optional[float] = None,
                        position_side: Optional[str] = None) -> list[TpSlOrder]:
        """Places reduce-only TP and/or SL (market execution) on the current position.

        side: position side ('long'/'short'; defaults to the only open one),
        the position is read from the exchange. ``position_side``: side supplied by the caller: together
        with ``size`` / ``contracts`` the position is NOT read (multi-level TP without races or extra
        requests), the size is not checked against the position (exchange reduce-only keeps it within), Bybit
        places the leg in Partial mode; OKX/KuCoin without a margin mode (create_exchange / set_margin_mode)
        raise ConfigError. Without a size the position is read, as with ``side``.
        size: size in the base coin, or contracts: in exchange contracts
        (at most one; default: the whole position; larger than the position raises InvalidOrder), trigger_by:
        'mark' or 'last'; None (default) uses the exchange default: Binance and OKX 'last',
        Bybit, BingX, KuCoin, Hyperliquid 'mark'. Returns the created orders (:class:`TpSlOrder`), TP before SL.

        ``take_profit_client_id`` / ``stop_loss_client_id``: your own id for each
        leg, sent as is (Binance ``clientAlgoId``, OKX ``algoClOrdId``, Hyperliquid cloid,
        KuCoin ``clientOid``); invalid format raises InvalidOrder before any request, identical ones raise
        ConfigError. OKX with ids set places TP and SL as separate algo orders (a pair has one id).
        Bybit (position TP/SL) and BingX (``clientOrderID`` only on LIMIT/MARKET, Place order):
        ids are not sent, orders have ``has_client_id=False``. Without ids, they are generated.

        SL is placed first. If a failure happens after some orders were placed, raises
        :class:`~bytx.errors.ProtectionNotPlaced`, ``details``: ``placed``: already
        placed ones, ``client_ids``: client_ids of all sent (for reconciliation after a
        timeout). If nothing was placed, the original error, with ``client_ids`` in its ``details``.
        """
        try:
            orders = await _protection.set_tp_sl(self, symbol, take_profit=take_profit, stop_loss=stop_loss,
                                                 side=side, size=size, trigger_by=trigger_by,  # type: ignore[arg-type]
                                                 take_profit_client_id=take_profit_client_id,
                                                 stop_loss_client_id=stop_loss_client_id, contracts=contracts,
                                                 position_side=position_side)  # type: ignore[arg-type]
        except ProtectionNotPlaced as e:
            if isinstance(e.details, dict) and e.details.get('placed'):
                await self._tp_sl_placed(e.details['placed'])
            raise
        await self._tp_sl_placed(orders)
        return orders

    async def place_tp_sl(self, symbol: str, kind: str, trigger: float, *, position_side: str,
                          size: Optional[float] = None, contracts: Optional[float] = None,
                          client_id: Optional[str] = None,
                          trigger_by: Optional[TriggerBy] = None) -> TpSlOrder:
        """One protection leg: ``kind`` 'take_profit' / 'stop_loss' at the ``trigger`` price.

        Returns a single :class:`TpSlOrder` or raises; no lists. ``position_side`` is required;
        with ``size`` / ``contracts`` the position is not read (see :meth:`set_tp_sl`), without them the size
        is the whole position on that side. ``client_id`` is sent as is, ``trigger_by`` as in set_tp_sl.
        """
        order = await _protection.place_tp_sl(self, symbol, kind, trigger, position_side=position_side,  # type: ignore[arg-type]
                                              size=size, contracts=contracts, client_id=client_id,
                                              trigger_by=trigger_by)
        await self._tp_sl_placed([order])
        return order

    async def modify_tp_sl(self, symbol: str, order_id: str, *, take_profit: Optional[float] = None,
                           stop_loss: Optional[float] = None, size: Optional[float] = None,
                           contracts: Optional[float] = None, client_id: Optional[str] = None,
                           trigger_by: Optional[TriggerBy] = None) -> TpSlModifyResult:
        """Changes the trigger price (and size) of a TP/SL by exchange id, without process memory.

        The old order is read from the exchange (:meth:`tp_sl_order`); side and size are taken from it
        (the position is not read); ``size`` / ``contracts`` set the new size. Only this order's leg
        changes: ``take_profit`` for TP, ``stop_loss`` for SL; for an OKX pair (kind 'tp_sl') either,
        the missing price stays as before. ``client_id``: id of the new order, sent as is. **Exception —
        Bitget, a whole-position TP/SL** (``pos_loss`` / ``pos_profit``) without a new size: moved in place
        (``modify-tpsl-order``), the order keeps the PREVIOUS ``client_id`` (``result.order.client_id``, a warning in
        the log) — the exchange doesn't create a second such order per side but replaces the previous one (demo
        2026-11-04), cancel-first would leave the position without a stop; no window without a stop takes priority.
        A size ``size`` / ``contracts`` ≤ 0 — ConfigError before any action.

        Without a window without a stop where the exchange allows it (per the exchange docs,
        :func:`bytx.exchanges.tp_sl_modify`, ``strategy`` in the result):

        - ``in_place`` — the trigger amended in place in one request: OKX ``amend-algos`` (the same algoId; not
          with a new ``client_id`` / ``trigger_by`` — the amend doesn't change them), Bybit whole-position TP/SL
          (``/v5/position/trading-stop``, tpslMode=Full, without a new size), Bitget ``modify-tpsl-order``
          (``pos_loss`` / ``pos_profit`` without a new size — with any ``client_id``; with a size — the same
          ``client_id``);
        - ``make_before_break`` — the new one is placed FIRST, the previous one is removed after: Binance (algo with
          quantity), Bybit Partial, OKX (otherwise), BingX, KuCoin, Hyperliquid, Bitget ``loss_plan`` /
          ``profit_plan`` (a new ``client_id`` or size; «whole position» → a size). The new one rejected — the previous
          one is untouched (``old='alive'``, ``new='rejected'``); outcome unknown — the same (look up by
          ``client_ids``); a failed removal of the previous one — the new one is not lost: ``old='alive'``,
          ``both_alive`` — both are on the exchange, the caller decides; the previous one fired in time —
          ``old='triggered'`` with the new one ``created``;
        - ``cancel_first`` — cancel, then the new one (a window without a stop — the time of one request): Binance
          ``closePosition=true`` without a new size (one per side, -4130), Gate / MEXC / HTX (two TP/SL per
          position not verified). The new one is placed only if the old one is definitely gone; the new one
          rejected — the previous trigger is placed again (``restored``).

        Returns :class:`~bytx.protection.TpSlModifyResult`: ``old`` canceled | alive |
        triggered | unknown, ``new`` created | unknown | rejected | None, ``order``: the new order.
        Raises only before any change (order not found, invalid arguments).
        """
        with self._journal_op(symbol, [order_id]):
            result = await _protection.modify_tp_sl(self, symbol, order_id, take_profit=take_profit,
                                                    stop_loss=stop_loss, size=size, contracts=contracts,
                                                    client_id=client_id, trigger_by=trigger_by)
        if self.journal is not None and result.order is not None:
            await self.journal._on_replaced(self, order_id, result.order, old_canceled=result.old == 'canceled')
        return result

    async def tp_sl_orders(self, symbol: str, side: Optional[str] = None) -> list[TpSlOrder]:
        """Active TP/SL for a symbol (side: position side)."""
        return await _protection.tp_sl_orders(self, symbol, side)  # type: ignore[arg-type]

    async def tp_sl_order(self, symbol: str, order_id: str) -> TpSlOrder:
        """One TP/SL by exchange id in any status, without process memory.

        Works after a restart with ids from a DB: Binance via Algo API by algoId, OKX via algo order
        by algoId, others via order lookup by id. Not found or not a TP/SL raises OrderNotFound.
        """
        return await _protection.tp_sl_order(self, symbol, order_id)

    async def cancel_tp_sl(self, symbol: str, *, side: Optional[str] = None, kind: Optional[str] = None,
                           order_ids: Optional[list[str]] = None) -> int:
        """Cancels TP/SL (kind: 'take_profit' / 'stop_loss' / None for all). Returns the count.

        ``order_ids``: only these (ids from a DB work after a restart too: the active TP/SL list
        is fetched from the exchange). Per-id report: :meth:`cancel_all` (``order_ids=``).
        """
        with self._journal_op(symbol, order_ids or ()):
            return await _protection.cancel_tp_sl(self, symbol, side=side, kind=kind,  # type: ignore[arg-type]
                                                  order_ids=order_ids)

    async def cancel_all_after(self, seconds: float, symbol: Optional[str] = None) -> Optional[int]:
        """Exchange dead-man switch: cancel all orders after seconds (0 clears the timer).

        Binance: futures only and symbol is required; KuCoin: not supported.
        Returns the trigger time (ms) or None.
        """
        return await _protection.cancel_all_after(self, seconds, symbol)

    def dead_man_switch(self, seconds: float, refresh_every: Optional[float] = None, *,
                        symbol: Optional[str] = None) -> DeadManSwitch:
        """``async with ex.dead_man_switch(60): ...``: the timer is extended in the background."""
        return DeadManSwitch(self, seconds, refresh_every, symbol=symbol)

    # ═══ end of position protection ═══════════════════════════════════════════

    # ─── broker tags ──────────────────────────────────────

    async def check_broker_prefix(self) -> list[str]:
        """Startup check: open Binance orders with an ``x-<ID>`` broker prefix
        that is not configured (neither your own nor ``BINANCE_BROKER_ID_LEGACY``) are logged as WARNING.

        Such orders cannot be found by client_id (risk of duplicates after a rollback or mode change).
        1–2 private requests; [] on other exchanges. Returns the client_ids of the orders found.
        """
        from bytx.exchanges.binance.broker_cid import scan_prefixed_orders
        if self._ex.id != 'binance':
            return []
        return await self._call(scan_prefixed_orders, self._ex, self.market_type)

    def _require_hyperliquid(self) -> None:
        if self._ex.id != 'hyperliquid':
            raise NotSupported('builder fee exists only on Hyperliquid', exchange=self.name)

    async def builder_fee_status(self, *, refresh: bool = True,
                                 timeout: Optional[float] = 5.0) -> dict:
        """Hyperliquid builder fee: user approval status.

        ``{enabled, builder, fee_tenths_bp, max_fee_rate, approved_tenths_bp, approved, checked,
        network, main_address, error}``. ``refresh=False``: from the process cache without requests;
        otherwise info ``maxBuilderFee`` (failure does not raise: ``checked=False``, ``error``). Builder
        is enabled only explicitly (``HL_BUILDER_ADDRESS`` / ``BrokerConfig``).
        Not Hyperliquid: NotSupported.
        """
        from bytx.exchanges.hyperliquid import builder as hl_builder
        self._require_hyperliquid()
        if not refresh:
            return cast(dict, hl_builder.cached_status(self._ex))
        return cast(dict, await hl_builder.refresh_status(self._ex, timeout=timeout))

    async def check_builder_fee(self, *, require: Optional[bool] = None,
                                timeout: Optional[float] = 5.0) -> dict:
        """Builder fee approval check at bot/loop start; status as in :meth:`builder_fee_status`.

        Approved or builder disabled: returns the status. Not approved: ``require`` (None means
        ``HL_BUILDER_REQUIRE_APPROVAL``) → :class:`~bytx.HyperliquidBuilderNotApprovedError`
        (HL replied "too low/none"), and if approval could not be checked (info unavailable, cache
        empty), :class:`~bytx.HyperliquidBuilderCheckUnavailableError` (retry later; this is
        not a user refusal). Without ``require``: WARNING, trading without the fee. Ongoing orders,
        reduce, close and SL do not trigger this check.
        """
        from bytx.exchanges.hyperliquid import builder as hl_builder
        status = await self.builder_fee_status(refresh=True, timeout=timeout)
        return cast(dict, hl_builder.check_start_approval(status, require=require))

    @property
    def on_builder_fallback(self) -> Optional[Callable[[dict], Any]]:
        """Callback for HL rejections due to builder fee: ``callable(info)`` (may be async).

        Called when HL rejected an action due to builder and the SDK retried it without builder
        (service metric/alert). A callback error does not break the order. Global variant:
        :func:`bytx.hl_set_builder_fallback_hook`.
        """
        return getattr(self._ex, '_bytx_hl_builder_on_fallback', None)

    @on_builder_fallback.setter
    def on_builder_fallback(self, hook: Optional[Callable[[dict], Any]]) -> None:
        self._require_hyperliquid()
        self._ex._bytx_hl_builder_on_fallback = hook

    def builder_approval_request(self, *, nonce: Optional[int] = None, chain_id: Optional[int] = None,
                                 max_fee_rate: Optional[str] = None) -> dict:
        """EIP-712 ApproveBuilderFee template for signing with the user's main wallet.

        Network follows the client (demo → testnet). Builder not configured raises ConfigError.
        ``max_fee_rate='0%'`` revokes approval.
        """
        from bytx.exchanges.hyperliquid import builder as hl_builder
        self._require_hyperliquid()
        if hl_builder.config_for_exchange(self._ex) is None:
            raise ConfigError('HL builder fee not configured (HL_BUILDER_ADDRESS / network / attribution off)',
                              exchange=self.name)
        return hl_builder.builder_approval_typed_data(testnet=self.demo, nonce=nonce, chain_id=chain_id,
                                                      max_fee_rate=max_fee_rate, exchange=self._ex)

    # ─── internals ──────────────────────────────────────────────────────────

    def _market_or_none(self, symbol: Optional[str]) -> Optional[Market]:
        if symbol and self._ex.markets and symbol in self._ex.markets:
            return to_market(self._ex.markets[symbol], self._ex.id)
        return None

    def _order(self, raw: dict, fallback: Optional[dict] = None) -> Order:
        m = self._market_or_none(raw.get('symbol') or (fallback or {}).get('symbol'))
        return convert.order(raw, m, fallback)

    def _ws_price(self, s: str, side: Optional[Side]) -> Optional[float]:
        """Price from the fresh WS cache (book / ticker), no request; None if there is no cache."""
        streams = getattr(self, '_streams', None)
        if streams is None:
            return None
        book = streams.last_book(s)
        if book is not None and book.bids and book.asks:
            if side is None:
                return book.mid
            return book.asks[0][0] if side == 'buy' else book.bids[0][0]
        t = streams.last_ticker(s)
        if t is None:
            return None
        if side is None:
            return t.mark or t.mid or t.last
        return (t.ask if side == 'buy' else t.bid) or t.last or t.mark

    def _cached_price(self, s: str, side: Optional[Side]) -> Optional[float]:
        """Price without a request: a fresh WS cache or the minimum-check cache younger than ``_CHECK_PRICE_TTL``;
        None — none."""
        price = self._ws_price(s, side)
        if price:
            return price
        hit = self._check_prices.get(s)
        return hit[0] if hit is not None and time.monotonic() - hit[1] < _CHECK_PRICE_TTL else None

    async def _check_price(self, s: str, side: Optional[Side]) -> float:
        """Price for the minimum cost check: from the cache (:meth:`_cached_price`), otherwise the reference price —
        cached."""
        price = self._cached_price(s, side)
        if price is None:
            price = await self._reference_price(s, side)
            self._check_prices[s] = (price, time.monotonic())
        return price

    async def _reference_price(self, s: str, side: Optional[Side]) -> float:
        """Reference price: buy uses ask, sell uses bid, None uses mark/mid.

        First the fresh WS cache ``ex.stream``; Hyperliquid: a single ``info l2Book`` (weight 2 per
        docs) instead of a ticker (ccxt fetch_ticker = fetch_tickers for all markets, weight 20+20);
        others: REST ticker.
        """
        price = self._ws_price(s, side)
        if not price and self._ex.id == 'hyperliquid':
            book = await self.order_book(s)
            if book.bids and book.asks:
                price = book.mid if side is None else (book.asks[0][0] if side == 'buy' else book.bids[0][0])
            else:
                level = book.best_ask or book.best_bid
                price = level[0] if level else None
        elif not price:
            t = await self.ticker(s)
            if side is None:
                price = t.mark or t.last or t.mid
            else:
                price = (t.ask if side == 'buy' else t.bid) or t.last or t.mark
        if not price:
            raise InvalidOrder(f'no price for {s}: pass price', exchange=self.name)
        return float(price)

    async def _trigger_price(self, s: str, trigger_by: Optional[str] = None, *, mark: Optional[float] = None) -> float:
        """The price the exchange compares the TP/SL trigger with: ``trigger_by`` 'mark' / 'last' (None — the exchange
        default, as in :meth:`set_tp_sl`). mark — the caller's ``mark`` (position mark), otherwise the ticker (WS cache,
        then REST); the ticker lacks the needed price — its other one, then the reference price
        (:meth:`_reference_price`)."""
        from bytx.exchanges import tp_sl_trigger_by
        by = trigger_by or tp_sl_trigger_by(self._ex.id)
        if by == 'mark' and mark:
            return float(mark)
        streams = getattr(self, '_streams', None)
        t = streams.last_ticker(s) if streams is not None else None
        if t is None and self._ex.id == 'hyperliquid':
            # HL REST ticker is fetch_tickers of all markets (weight 20+20): the reference price — WS cache / one l2Book
            return await self._reference_price(s, None)
        if t is None:
            t = await self.ticker(s)
        price = (t.mark or t.last) if by == 'mark' else (t.last or t.mark)
        return float(price) if price else await self._reference_price(s, None)

    def _spot_quote_buy(self, s: str, m: Any, notional: float, ref_price: Optional[float]) -> Optional[dict]:
        """Spot market buy for a quote amount with the exchange's own parameter: Binance ``quoteOrderQty``
        (if ``quoteOrderQtyMarketAllowed``), Bybit ``marketUnit=quoteCoin``, OKX ``tgtCcy=quote_ccy``, KuCoin
        ``funds``. The amount is rounded down to the quote step (Binance quoteAssetPrecision, Bybit quotePrecision,
        KuCoin quoteIncrement); below the minimum cost (NOTIONAL / minOrderAmt / minFunds, quoteMinSize) or, on OKX
        without a minimum cost, below ``minSz`` at the reference price — :class:`OrderTooSmall`. Other exchanges —
        None (converted to base)."""
        info = m.raw.get('info') or {}
        ex_id = self._ex.id
        if ex_id not in ('binance', 'bybit', 'okx', 'kucoin', 'blofin') or \
                (ex_id == 'binance' and info.get('quoteOrderQtyMarketAllowed') is False):
            return None
        step = {'binance': (m.raw.get('precision') or {}).get('quote'),
                'bybit': (info.get('lotSizeFilter') or {}).get('quotePrecision'),
                'kucoin': info.get('quoteIncrement')}.get(ex_id)
        quote = Decimal(str(notional))
        if step and Decimal(str(step)) > 0:
            quote = (quote / Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step))
        minimum = max([float(x) for x in (m.min_cost, info.get('quoteMinSize') if ex_id == 'kucoin' else None)
                       if x], default=0.0)
        if quote <= 0 or quote < Decimal(str(minimum)):
            raise OrderTooSmall(f'amount {notional} is below the minimum {minimum} or the step {step} ({s})',
                                exchange=self.name, details={'symbol': s, 'notional': notional})
        if ex_id in ('okx', 'blofin') and m.min_amount and ref_price and float(quote) / ref_price < m.min_amount:
            raise OrderTooSmall(f'amount {notional} at price {ref_price} is below minSz {m.min_amount} ({s})',
                                exchange=self.name, details={'symbol': s, 'notional': notional})
        out: dict = {'cost': format(quote.normalize(), 'f')}
        if ex_id == 'bybit':
            out['marketUnit'] = 'quoteCoin'
        elif ex_id == 'okx':
            out['tgtCcy'] = 'quote_ccy'
        elif ex_id == 'blofin':
            out['targetCurrency'] = 'quote_currency'   # S#place-order; size is the quote amount (spot.create_order_request)
        return out

    async def _build_order(self, symbol: str, side: Side, *, size: Optional[float] = None,
                           notional: Optional[float] = None, contracts: Optional[float] = None,
                           margin: Optional[float] = None, leverage: Optional[float] = None,
                           price: Optional[float] = None, type: Optional[str] = None,
                           post_only: bool = False, time_in_force: Optional[str] = None,
                           reduce_only: bool = False, client_id: Optional[str] = None,
                           take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                           params: Optional[dict] = None, position_margin_mode: Optional[str] = None,
                           journal_role: Optional[str] = None, max_round_up: Optional[float] = None) -> dict:
        """Order request without sending. ``position_margin_mode``: margin mode of the open position
        (close/reduce): used to close if the symbol's mode is not set explicitly."""
        if side not in ('buy', 'sell'):
            raise ConfigError(f"side: 'buy' or 'sell', not {side!r} ({symbol})", exchange=self.name,
                              details={'symbol': symbol, 'side': side})
        if sum(x is not None for x in (size, notional, contracts, margin)) != 1:
            raise ConfigError(f'exactly one of size / notional / contracts / margin is required ({symbol})',
                              exchange=self.name,
                              details={'symbol': symbol, 'size': size, 'notional': notional,
                                       'contracts': contracts, 'margin': margin})
        if leverage is not None and margin is None:
            raise ConfigError(f'leverage only together with margin ({symbol})', exchange=self.name,
                              details={'symbol': symbol, 'leverage': leverage})
        await self.connect()
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        order_type = type or ('limit' if price is not None else 'market')
        if order_type == 'limit' and price is None:
            raise ConfigError(f'limit order requires price ({s})', exchange=self.name, details={'symbol': s})
        if post_only and order_type != 'limit':
            raise ConfigError(f'post_only only for limit, not {order_type} ({s})', exchange=self.name,
                              details={'symbol': s, 'type': order_type})

        ref_price = price
        quote_buy = notional is not None and order_type == 'market' and side == 'buy' and self.market_type == 'spot'
        if quote_buy and ref_price is None:
            # the quote amount is compared with the minimum without a price; the price only from WS / the cache
            # (amount estimate, OKX minSz). The risk guard and the paper engine need the amount — a ticker for them
            ref_price = (await self._reference_price(s, side) if self.risk is not None or self.is_paper
                         else self._cached_price(s, side))
        quote_params = self._spot_quote_buy(s, m, cast(float, notional), ref_price) if quote_buy else None
        needs_ref = ((notional is not None and quote_params is None) or margin is not None
                     or (order_type == 'market' and self._ex.id == 'hyperliquid'))
        if ref_price is None and needs_ref:
            ref_price = await self._reference_price(s, side)
        elif ref_price is None and quote_params is None and m.min_cost and not reduce_only:
            # only the minimum cost check (reduce-only is not checked): WS / TTL cache / ticker
            ref_price = await self._check_price(s, side)

        if quote_params is not None:
            # estimate: the exchange sizes by the amount; without a price — 0, None in the request (_ccxt_amount)
            amount = notional / ref_price if ref_price else 0.0  # type: ignore[operator]
        elif margin is not None:
            amount = self._margin_contracts(s, m, side, margin, leverage, price, ref_price, order_type,
                                            max_round_up=max_round_up)
        elif contracts is not None:
            # round down to the step, like size: ccxt hyperliquid rounds to the nearest, and below one step
            # ccxt raises its own InvalidOrder
            amount = convert.contracts_to_precision(self._ex, m, contracts)
        else:
            base = size if size is not None else notional / ref_price  # type: ignore[operator]
            amount = convert.contracts_from_size(self._ex, m, base)
        if order_type == 'market' and self._ex.id == 'binance' and quote_params is None:
            from bytx.exchanges.binance.limits import apply_market_lot
            amount = apply_market_lot(m.raw, amount)
        if quote_params is not None:
            pass  # quote minimums are checked in _spot_quote_buy
        elif amount <= 0 or (m.min_amount and amount < m.min_amount):
            raise OrderTooSmall(f'size {amount} contracts is below the minimum {m.min_amount} ({s})',
                                exchange=self.name)
        elif not reduce_only and m.min_cost and ref_price:
            cost = amount * m.contract_size * ref_price
            if cost < m.min_cost:
                raise OrderTooSmall(f'notional {cost:.4f} is below the minimum {m.min_cost} ({s})',
                                    exchange=self.name)

        # limit toward the passive side (buy down, sell up): never worse than the user's price
        out_price = (convert.price_to_precision(self._ex, m, price, side if order_type == 'limit' else None)
                     if price is not None else None)
        if out_price is None and self._ex.id == 'hyperliquid':
            out_price = ref_price  # HL computes market order slippage from the price

        p: dict = {}
        if self._ex.id == 'blofin':
            if self.market_type == 'spot' and (take_profit is not None or stop_loss is not None):
                raise NotSupported(f'BloFin Spot API has no TP/SL in the order ({s})', exchange=self.name)
            if self._blofin_mode_error is not None:
                raise self._blofin_mode_error   # Multi-Position on the account
            if not reduce_only:
                from bytx.exchanges.blofin.ccxt_fixes import check_open_allowed
                check_open_allowed(self, m)
        if m.is_contract:
            # OKX (tdMode) and KuCoin (marginMode) send the mode with every order:
            # with no mode set, ConfigError instead of a silent cross
            mode = (self._need_margin_mode(s, position_margin_mode) if self._ex.id in _MODE_IN_ORDER
                    else self.margin_mode_of(s))
            leverage = self._leverage.get(s)
            if not leverage and (self._ex.id == 'htx' or (self._ex.id == 'mexc' and not reduce_only)):
                # HTX: lever_rate is mandatory and equals the position leverage (ccxt would substitute 1); MEXC: on open
                leverage = await self.leverage(s)
                if not leverage:
                    raise ConfigError(f'{s} leverage unknown: call set_leverage(symbol, x) '
                                      f'({self.name} takes leverage from every order)', exchange=self.name,
                                      details={'symbol': s})
                self._leverage[s] = leverage
            if self._ex.id == 'kucoinfutures' and not reduce_only and not leverage:
                # without leverage ccxt substitutes 1x (regression c3db30d): do not send
                raise ConfigError(f'{s} leverage not set on this instance: call set_leverage(symbol, x) '
                                  f'(KuCoin takes leverage from every order)', exchange=self.name,
                                  details={'symbol': s})
            hedge = await self.is_hedge_mode(s)
            p.update(position_params(self._ex, side=side, hedge=hedge, reduce_only=reduce_only,
                                     margin_mode=mode, leverage=leverage))
        elif reduce_only:
            raise ConfigError(f'reduce_only only for derivatives ({s})', exchange=self.name,
                              details={'symbol': s})
        if time_in_force:
            tif = time_in_force.upper()
            if tif not in _TIF:
                raise ConfigError(f'time_in_force: {sorted(_TIF)}, not {time_in_force!r} ({s})',
                                  exchange=self.name, details={'symbol': s, 'time_in_force': time_in_force})
            if tif == 'FOK' and self._ex.id in _NO_FOK:
                raise NotSupported('time_in_force FOK is not supported by the exchange', exchange=self.name)
            if tif == 'PO' and self._ex.id in _PO_AS_FLAG:
                # per docs HL tif ∈ Alo|Ioc|Gtc, KuCoin Futures GTC|IOC: post-only is a flag
                if order_type != 'limit':
                    raise ConfigError(f'post_only only for limit, not {order_type} ({s})', exchange=self.name,
                                      details={'symbol': s, 'type': order_type})
                post_only = True
            else:
                p['timeInForce'] = tif
        if post_only:
            p['postOnly'] = True
        if (take_profit is not None or stop_loss is not None) and self._ex.id not in _SEPARATE_TP_SL:
            if take_profit is not None:
                p['takeProfit'] = {'triggerPrice': float(self._ex.price_to_precision(s, take_profit))}
            if stop_loss is not None:
                p['stopLoss'] = {'triggerPrice': float(self._ex.price_to_precision(s, stop_loss))}
        cid = client_id or self._new_client_id(role=journal_role)
        p.update(client_id_params(self._ex, cid, market_type=self.market_type, strict=self.client_id_strict))
        attach_cid, attach_role = None, None
        if (self.journal is not None and self._ex.id == 'okx'
                and (take_profit is not None or stop_loss is not None)
                and not (params or {}).get('attachAlgoClOrdId')):
            # OKX: an attached TP/SL is a separate algo order; its child order carries algoClOrdId =
            # attachAlgoClOrdId (Place order → attachAlgoOrds). Our own id with the bot tag lets the journal
            # recognize a triggered TP/SL as ours, not foreign. One leg for TP and SL: role tp if TP is set.
            attach_role = 'tp' if take_profit is not None else 'sl'
            attach_cid = self.journal.client_id(self, attach_role, level_id=0, market_type=self.market_type,
                                                tag=self.client_id_prefix)
            if attach_cid is not None:
                p['attachAlgoClOrdId'] = attach_cid
        if quote_params:
            p.update(quote_params)
        if params:
            p.update(params)
        if self.risk is not None:  # client-side risk guard before sending
            await self.risk.check(self, s, side, amount * m.contract_size, out_price or ref_price, reduce_only)
        separate = None
        if (take_profit is not None or stop_loss is not None) and self._ex.id in _SEPARATE_TP_SL:
            if reduce_only or not m.is_contract:
                raise ConfigError(f'take_profit/stop_loss only for an opening order on derivatives ({s})',
                                  exchange=self.name, details={'symbol': s})
            # check TP against SL before sending: after placement it is too late
            _protection._check_prices('long' if side == 'buy' else 'short', take_profit, stop_loss, self.name)
            separate = {'take_profit': take_profit, 'stop_loss': stop_loss}
        return {'symbol': s, 'type': order_type, 'side': side,
                'amount': None if quote_params is not None and not amount else amount, 'price': out_price,
                'params': p, 'client_id': cid, 'submitted_at': int(time.time() * 1000),
                'tp_sl': separate, 'reduce_only': reduce_only, 'journal_role': journal_role,
                'attach_client_id': attach_cid, 'attach_role': attach_role if attach_cid else None,
                'attach_trigger': (take_profit if attach_role == 'tp' else stop_loss) if attach_cid else None}

    # ═══ WebSocket: ex.stream and orders via WS ═════════════════════
    # Implemented in bytx.streams.

    @property
    def stream(self) -> Any:
        """WS streams with a reconnect supervisor (:class:`bytx.streams.Streams`)."""
        streams = getattr(self, '_streams', None)
        if streams is None:
            from bytx.streams import Streams
            streams = self._streams = Streams(self)
        return streams

    def _ws_route(self, op: str, *, ws: bool) -> bool:
        from bytx.streams.private import ws_order_route
        if not ws:
            return False
        use_ws, _reason = ws_order_route(self._ex, op)
        return use_ws

    async def _ws_timed(self, op: str, use_ws: bool, fn: Callable[..., Awaitable[T]], *args: Any) -> T:
        started = time.perf_counter()
        ok = False
        try:
            # order placement and amendment are not retried (duplicate); cancel is idempotent
            result = await self._call(fn, *args, safe=(op == 'cancel'))
            ok = True
            return result
        finally:
            self.stream._record_order(op, 'ws' if use_ws else 'rest', time.perf_counter() - started, ok)

    async def ws_place_order(self, symbol: str, side: Side, *, size: Optional[float] = None,
                             notional: Optional[float] = None, contracts: Optional[float] = None,
                             margin: Optional[float] = None, leverage: Optional[float] = None,
                             price: Optional[float] = None, type: Optional[str] = None,
                             post_only: bool = False, time_in_force: Optional[str] = None,
                             reduce_only: bool = False, client_id: Optional[str] = None,
                             take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                             params: Optional[dict] = None, ws: bool = True) -> Order:
        """Like :meth:`place_order`, but via WebSocket (ccxt ``create_order_ws``) where the exchange
        supports it and broker attribution is preserved; otherwise REST. Latency is in
        ``ex.stream.stats()['orders']`` ('create:ws' / 'create:rest').

        A WS network error is NOT automatically retried via REST (possible duplicate) and is not
        reconciled (latency path): an unclear outcome raises :class:`~bytx.errors.OrderStatusUnknown`
        with ``client_order_id``; look the order up via :meth:`find_order`. Explicit rejection as in
        :meth:`place_order` (reduce-only without a position: :class:`~bytx.errors.PositionAlreadyClosed`). TP/SL on Binance / Hyperliquid
        are placed as separate orders after placement, as in :meth:`place_order`.
        """
        request = await self._build_order(symbol, side, size=size, notional=notional,
                                          contracts=contracts, margin=margin, leverage=leverage,
                                          price=price, type=type,
                                          post_only=post_only, time_in_force=time_in_force,
                                          reduce_only=reduce_only, client_id=client_id,
                                          take_profit=take_profit, stop_loss=stop_loss,
                                          params=params)
        await self._pre_submit(request)
        use_ws = self._ws_route('create', ws=ws)
        if use_ws:
            from bytx.streams.private import ws_create_params
            fn, order_params = self._ex.create_order_ws, ws_create_params(self._ex, request['params'])
        else:
            fn, order_params = self._ex.create_order, request['params']
        try:
            raw = await self._ws_timed('create', use_ws, fn, request['symbol'], request['type'],
                                       request['side'], _ccxt_amount(request), request['price'], order_params)
        except BytxError as e:
            # no reconciliation (latency path): unclear outcome raises OrderStatusUnknown, the caller reconciles
            failure = await self._unplaced(request, e, reconcile=False)
            if self.journal is not None:
                await self.journal._on_failed(self, request, failure if isinstance(failure, BytxError) else e)
            if self.intents is not None:
                self.intents._on_failed(self, request, failure if isinstance(failure, BytxError) else e)
            if self._observers:
                self._emit_failed(request, failure if isinstance(failure, BytxError) else e)
            if failure is e or not isinstance(failure, BytxError):
                raise
            raise failure from failure.__cause__
        self._remember_algo(raw)
        order = self._order(raw, fallback=request)
        if self.market_type == 'spot' and request['type'] == 'market':
            order = await self._complete_market(order, request['symbol'])   # same completeness as place_order
        if self.journal is not None:
            await self.journal._on_placed(self, order, request)
        if self.intents is not None:
            self.intents._on_placed(self, order, request['client_id'])
        if self._observers:
            self.emit('order.placed', symbol=order.symbol, order=order, data={'request': request_view(request)})
        if request.get('tp_sl'):
            await self._protect(order, request)
        return order

    async def ws_amend_order(self, symbol: str, order_id: str, *, price: Optional[float] = None,
                             size: Optional[float] = None, side: Optional[Side] = None,
                             type: str = 'limit', ws: bool = True) -> Order:
        """Like :meth:`amend_order`, via WS (ccxt ``edit_order_ws``) with a REST fallback.

        Pass ``side`` to avoid a REST request to look it up.
        """
        if price is None and size is None:
            raise ConfigError(f'price and/or size required ({symbol}, order {order_id})', exchange=self.name,
                              details={'symbol': symbol, 'order_id': order_id})
        await self.connect()
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        side, amount, params, cids = await self._amend_prepare(s, m, order_id, side, size)
        if price is None and self._both_price_size(m):
            price = (await self.order(s, order_id)).price   # size only: keep the old price
        # limit price toward the passive side, as in place_order
        p = (convert.price_to_precision(self._ex, m, price, side if type == 'limit' else None)
             if price is not None else None)
        use_ws = self._ws_route('edit', ws=ws)
        fn = self._ex.edit_order_ws if use_ws else self._ex.edit_order
        with self._journal_op(s, [order_id, cids[1] if cids else None]):
            raw = await self._ws_timed('edit', use_ws, fn, order_id, s, type, side, amount, p, params)
        return await self._amend_done(s, order_id, raw, cids)

    async def ws_cancel_order(self, symbol: str, order_id: Optional[str] = None, *,
                              client_id: Optional[str] = None, ws: bool = True) -> Order:
        """Like :meth:`cancel_order`, via WS (ccxt ``cancel_order_ws``) with a REST fallback."""
        await self.connect()
        s = self.symbol(symbol)
        use_ws = self._ws_route('cancel', ws=ws)
        fn = self._ex.cancel_order_ws if use_ws else self._ex.cancel_order
        if not order_id and not client_id:
            raise ConfigError(f'order_id or client_id is required ({s})', exchange=self.name, details={'symbol': s})
        with self._journal_op(s, [order_id, client_id]):
            if order_id:
                raw = await self._ws_timed('cancel', use_ws, fn, order_id, s, {})
            else:
                # cancel key by client_id, as in REST cancel_order (Binance: origClientOrderId)
                raw = await self._ws_timed('cancel', use_ws, fn, None, s,
                                           cancel_client_id_params(self._ex, cast(str, client_id)))
        order = self._order(raw, fallback={'id': order_id, 'client_id': client_id, 'symbol': s})
        if self.intents is not None:
            self.intents._on_update(self, order)
        if self.journal is not None:
            await self.journal._on_canceled(self, order, True)
        return order

    # ─── recovery after restart ────────────────────────────────────

    async def recover(self, *, resubmit: Any = False, max_age: Optional[float] = 300.0, settle: float = 3.0,
                      protect: bool = True, adopt_untracked: bool = True,
                      symbols: Optional[Iterable[str]] = None,
                      prune_after: Optional[float] = 7 * 86400.0,
                      cancel_orphaned: bool = True, cancel_orphaned_algos: bool = True,
                      protect_max_age: Optional[float] = 86400.0) -> 'RecoveryReport':
        """Reconciles the intent store (``intents=``) with the exchange at process start.

        Live intent orders are picked up, finished ones are marked, missing ones are
        re-created with the same client_id (``resubmit=True`` or ``callable(intent) -> bool``; no older than
        ``max_age`` s) or closed as ``abandoned``; "could not check" gives ``unverified``,
        and nothing is sent. Missing entry TP/SL (Binance/HL, open_position stop plan,
        attached and separate) are placed after reconciling with the exchange's open TP/SL (``protect``): if one already exists,
        no second one is placed. Your own orphaned TP/SL (no position on that side, re-read before cancelling)
        are canceled (``cancel_orphaned``; foreign ones never). Working orders of unfinished ``ex.algos`` algorithms
        (the process died in the middle of chase / twap / iceberg / bracket …) are canceled, position protection (the
        algorithm's TP/SL) stays, what was filled is not touched — ``report.algos`` (``cancel_orphaned_algos``;
        algorithms are not resumed). A bracket that died between the entry and its legs gets the missing legs from the
        protection plan written before the entry (``protect``; the plan side's position exists, size — the filled
        one, no more than the position; a price past the trigger — the leg is not placed, the position is not closed;
        a plan older than ``protect_max_age`` is not applied): ``report.algos[i].protection`` / ``protected``,
        positions without a stop — ``report.unprotected``. Details: :func:`bytx.recovery.recover`. Without a store,
        raises :class:`ConfigError`.
        """
        from bytx.recovery import recover
        return await recover(self, resubmit=resubmit, max_age=max_age, settle=settle, protect=protect,
                             adopt_untracked=adopt_untracked, symbols=symbols, prune_after=prune_after,
                             cancel_orphaned=cancel_orphaned, cancel_orphaned_algos=cancel_orphaned_algos,
                             protect_max_age=protect_max_age)

    # ─── portfolio, key check, exchange health ─────────────────────

    async def account_report(self, *, since: int, until: Optional[int] = None,
                             symbols: Optional[Iterable[str]] = None, keep_items: bool = False) -> 'AccountReport':
        """Account portfolio over ``[since, until]`` (ms): equity, unrealized PnL of positions,
        realized PnL, fees and funding from the exchange's ledger (per docs, parsed natively).
        Incomplete data gives ``complete=False`` with reasons in ``issues`` (not an exception). ``symbols=`` —
        a report for these symbols (ledger, positions, unrealized PnL; equity — of the whole account); the symbol of
        ledger records is the SDK symbol. Details: :mod:`bytx.portfolio`; across all accounts:
        :meth:`AccountPool.portfolio`."""
        from bytx.portfolio import account_report
        return await account_report(self, since=since, until=until, symbols=symbols, keep_items=keep_items)

    async def funding_paid(self, symbol: str, *, since: int, until: Optional[int] = None) -> 'FundingPaid':
        """Funding paid by the account for a symbol over ``[since, until]`` (ms), on every exchange: entries and
        ``total`` (+ received / − paid), ``complete`` / ``issues`` — completeness. Source — the exchange's account ledger
        (:func:`bytx.portfolio.funding_paid`)."""
        from bytx.portfolio import funding_paid
        return await funding_paid(self, symbol, since=since, until=until)

    async def check_key(self, *, withdraw: Literal['reject', 'warn', 'allow'] = 'reject') -> 'KeyInfo':
        """API key permissions from the exchange's key permissions endpoint (per docs): trading, withdrawal, transfers, IP
        binding, account type (unified/classic …), position mode, UID: :class:`~bytx.key_check.KeyInfo`.

        ``withdraw='reject'`` (default): a key with withdrawal permission raises
        :class:`~bytx.errors.UnsafeApiKey`; ``'warn'`` adds a warning to ``info.warnings``;
        ``'allow'`` stays silent. Anything the exchange does not report is ``None`` with the reason in ``info.unknown``."""
        from bytx.key_check import check_key
        return await check_key(self, withdraw=withdraw)

    async def account_uid(self) -> str:
        """Exchange account UID (Hyperliquid: wallet address), via the same endpoint as :meth:`check_key`."""
        from bytx.key_check import account_uid
        return await account_uid(self)

    async def health(self, *, private: Optional[bool] = None) -> 'HealthReport':
        """Exchange health: status/maintenance from the public endpoint in the exchange docs, REST latency and
        clock skew (server time probe), REST and WS stream metrics, limiter state.
        Works without keys; with keys (``private=None`` means if keys are set) also a private probe
        (balance). :class:`~bytx.health.HealthReport`; ``report.healthy`` means trading is possible."""
        from bytx.health import health
        return await health(self, private=private)

    # ─── state trackers and scalping helpers ───────────────────────

    @property
    def algos(self) -> Any:
        """Execution algorithms: ``ex.algos.chase / twap / iceberg / bracket /
        trailing_stop / market``; see :mod:`bytx.algos`. Created on first access:
        until then the module is not imported and the order hot path is unchanged."""
        algos = self.__dict__.get('_algos')
        if algos is None:
            from bytx.algos import Algos
            algos = self._algos = Algos(self)
        return algos

    @property
    def history(self) -> 'HistoryLoader':
        """History loader: ``await ex.history.candles(symbol, '1h', since=t0)``, ``trades``,
        ``funding``, with an on-disk cache (:func:`bytx.data.default_cache_dir`) and gap filling;
        custom directory/format: :class:`bytx.data.HistoryLoader`. The module is imported on first
        access."""
        loader = self.__dict__.get('_history')
        if loader is None:
            from bytx.data import HistoryLoader
            loader = self._history = HistoryLoader(self)
        return loader

    def order_tracker(self, symbol: Optional[str] = None, *, keep_closed: int = 1000) -> 'OrderTracker':
        """Local state of your own orders (WS + REST reconciliation after a disconnect).

        ``async with ex.order_tracker('BTC/USDT') as t: await t.wait(order.id)``
        """
        return OrderTracker(self, symbol, keep_closed=keep_closed)

    def position_tracker(self, symbol: Optional[str] = None) -> 'PositionTracker':
        """Local position state (WS + REST reconciliation after a disconnect)."""
        return PositionTracker(self, symbol)

    def round_price(self, symbol: str, price: float, *, side: Optional[Side] = None) -> float:
        """Price rounded to the market's price tick (markets must be loaded: ``await ex.connect()``).

        ``side`` rounds toward the passive side so post-only does not cross the spread:
        'buy' down, 'sell' up; without side, to the nearest tick.
        """
        s = self.symbol(symbol)
        step = to_market(self._ex.market(s), self._ex.id).price_step
        if not step:
            return float(self._ex.price_to_precision(s, price))
        ticks = price / step
        n = math.floor(ticks + 1e-9) if side == 'buy' else math.ceil(ticks - 1e-9) if side == 'sell' \
            else round(ticks)
        return float(self._ex.price_to_precision(s, n * step))

    def round_size(self, symbol: str, size: float) -> float:
        """Size (base) rounded down to the market's amount step: what actually goes into the order."""
        s = self.symbol(symbol)
        m = to_market(self._ex.market(s), self._ex.id)
        return convert.contracts_from_size(self._ex, m, size) * (m.contract_size if m.is_contract else 1.0)

    def contracts_for_margin(self, symbol: str, margin: float, leverage: float, price: float) -> Optional[float]:
        """Margin (quote) × leverage → contracts at ``price``.

        Bit-for-bit ``bytx.utils.calculate_contract_size(usdt_volume=margin * leverage, …)``: division
        in Decimal, snapping to a whole lot, down to the exchange's lot step; below the minimum,
        one step up if that is not below the minimum. None if still below the minimum after the step up
        (or the market has no lot rules). Step and minimum per exchange docs
        (``bytx.exchanges.<exchange>.limits``). Markets must be loaded (``await ex.connect()``).
        """
        s = self.symbol(symbol)
        return sizing.contracts_for_margin(self._ex.id, self._ex.market(s), margin, leverage, price)

    # ─── risk-based position sizing ─────────────────────────────────

    async def risk_size(self, symbol: str, side: str, *, risk_usdt: float, stop: float,
                        leverage: Optional[float] = None, price: Optional[float] = None,
                        take_profit: Optional[float] = None, fee_rate: Optional[float] = None,
                        slippage_bps: float = 0.0) -> 'RiskPlan':
        """Position size from stop-based risk, without orders (the :meth:`open_position` calculation).

        ``side``: 'long' / 'short' ('buy' / 'sell' also work). ``risk_usdt``: how much to lose when
        the stop triggers (symbol's settle currency), ``stop``: stop trigger (rounded to the price tick).
        Entry: without ``price``, market at the best opposite price (WS cache or one REST request);
        with ``price``, limit at that price. Loss per base unit = ``|entry − exit| + fee_rate × (entry + exit)``,
        ``slippage_bps`` worsens the market entry and the stop exit; ``fee_rate`` defaults to the exchange's
        default-tier taker fee from its docs (paper: the account rate). Contracts are rounded down to the exchange step
        (actual risk ≤ ``risk_usdt``), accounting for ``contractSize``. If the minimum size (``min amount``,
        ``min_cost``) risks more, raises :class:`~bytx.errors.MinSizeExceedsRisk`; a stop or TP on the wrong
        side of the entry raises :class:`~bytx.errors.InvalidOrder`. ``leverage`` (otherwise the leverage from
        :meth:`set_leverage`) gives the margin and an isolated liquidation estimate. Returns
        :class:`~bytx.risk_sizing.RiskPlan`. Derivatives only.
        """
        from bytx.risk_sizing import risk_size
        return await risk_size(self, symbol, side, risk_usdt=risk_usdt, stop=stop, leverage=leverage, price=price,
                               take_profit=take_profit, fee_rate=fee_rate, slippage_bps=slippage_bps)

    async def open_position(self, symbol: str, side: str, *, risk_usdt: float, stop: float,
                            leverage: Optional[float] = None, take_profit: Optional[float] = None,
                            price: Optional[float] = None, fee_rate: Optional[float] = None,
                            slippage_bps: float = 0.0, stop_mode: Literal['auto', 'separate'] = 'auto',
                            trigger_by: Optional[TriggerBy] = None,
                            on_stop_fail: Literal['close', 'keep'] = 'close', check_margin: bool = True,
                            client_id: Optional[str] = None,
                            position_id: Optional[str] = None) -> 'OpenPositionResult':
        """Opens a **new** position sized from risk (:meth:`risk_size`) and immediately protects it with a stop.

        Before sending: if a position on the symbol already exists (in hedge, on this side), :class:`PositionOpenError`;
        ``leverage`` is set (:meth:`set_leverage`, if the instance has a different one), otherwise read;
        isolated: a stop beyond the liquidation estimate raises :class:`InvalidOrder`; ``check_margin``: margin
        (notional / leverage + fee) above free collateral raises :class:`InsufficientFunds`.
        Nothing is sent on :class:`~bytx.errors.MinSizeExceedsRisk` either.

        Entry is market (``price``: limit IOC, no worse than the price, unfilled part canceled). Stop:

        - ``stop_mode='auto'``, market entry, and the exchange supports SL in the entry order per its docs
          (:func:`bytx.exchanges.attached_sl`: Bybit, OKX, BingX, KuCoin): SL (and TP) go
          **in the same request** (atomic; trigger uses the exchange default);
        - otherwise (Binance, Hyperliquid, limit entry, ``stop_mode='separate'``, ``trigger_by`` set):
          :meth:`set_tp_sl` reduce-only on the filled size right after entry. If SL fails,
          ``on_stop_fail='close'``: a placed TP is canceled, the position is closed with a reduce-only market,
          :class:`ProtectionNotPlaced` (``details``: ``order``, ``rolled_back``, ``close``, ``close_error``,
          ``plan``); ``'keep'``: the same error, the position stays. If only TP fails, the position is open,
          with a warning in ``warnings``.

        Position journal (if attached): entry has role ``entry``, SL/TP ``sl``/``tp``, rollback ``close``;
        ``position_id``: journal position. Returns :class:`~bytx.risk_sizing.OpenPositionResult`
        (plan, entry order, stop, actual risk by fill). Derivatives only.
        """
        from bytx.risk_sizing import open_position
        return await open_position(self, symbol, side, risk_usdt=risk_usdt, stop=stop, leverage=leverage,
                                   take_profit=take_profit, price=price, fee_rate=fee_rate,
                                   slippage_bps=slippage_bps, stop_mode=stop_mode, trigger_by=trigger_by,
                                   on_stop_fail=on_stop_fail, check_margin=check_margin, client_id=client_id,
                                   position_id=position_id)

    def _margin_contracts(self, s: str, m: Market, side: Side, margin: float, leverage: Optional[float],
                          price: Optional[float], ref_price: Optional[float], order_type: str,
                          max_round_up: Optional[float] = None) -> float:
        """``place_order(margin=)``: contracts at the execution price, or OrderTooSmall.

        A size below the minimum lot is rounded up to the minimum. ``max_round_up`` — how many times the minimum lot
        may exceed the requested size: more — :class:`OrderTooSmall` (``details``: ``requested``, ``min`` —
        contracts); None — no cap (the default), but a WARNING at 2× and more (5 USDT of margin on BTC → 100 USDT)."""
        known = self._leverage.get(s)
        if leverage is not None and known is not None and float(leverage) != float(known):
            # margin is computed with one leverage, but the position would open with another: wrong size
            raise ConfigError(f'leverage={leverage}, but {known} is set on the instance for {s} (set_leverage)',
                              exchange=self.name, details={'symbol': s, 'leverage': leverage, 'instance': known})
        lev = leverage if leverage is not None else known
        if lev is None and not m.is_contract:
            lev = 1.0
        if lev is None or lev <= 0:
            raise ConfigError(f'margin= without leverage: pass leverage= or call set_leverage ({s})',
                              exchange=self.name, details={'symbol': s, 'margin': margin})
        if price is not None:
            exec_price = convert.price_to_precision(self._ex, m, price, side if order_type == 'limit' else None)
        else:
            exec_price = convert.price_to_precision(self._ex, m, float(ref_price))  # type: ignore[arg-type]
        contracts = sizing.contracts_for_margin(self._ex.id, m.raw, margin, lev, exec_price)
        if contracts is None:
            raise OrderTooSmall(f'margin {margin} × leverage {lev} at price {exec_price} is below the minimum lot ({s})',
                                exchange=self.name, details={'symbol': s, 'margin': margin, 'leverage': lev})
        raw = m.raw if isinstance(m.raw, dict) else {}
        try:
            requested = float(margin) * float(lev) / (float(exec_price) * float(raw.get('contractSize') or 1.0))
        except (TypeError, ValueError, ZeroDivisionError):
            requested = 0.0
        if requested > 0 and contracts > requested * (1 + 1e-9):
            ratio = contracts / requested
            details = {'symbol': s, 'margin': margin, 'leverage': lev, 'requested': requested, 'min': contracts,
                       'ratio': ratio}
            if max_round_up is not None and ratio > float(max_round_up):
                raise OrderTooSmall(f'margin {margin} × leverage {lev}: {requested:g} contracts is below the minimum '
                                    f'lot {contracts:g} by {ratio:.1f}x (max_round_up={max_round_up}) ({s})',
                                    exchange=self.name, details=details)
            if ratio >= 2:
                from loguru import logger
                logger.warning(f'{self.name}: margin={margin} × {lev} on {s}: {requested:g} contracts rounded up to '
                               f'the minimum lot {contracts:g} — {ratio:.1f}x the requested size '
                               f'(cap it with place_order(max_round_up=…))')
        # snap to the exchange step grid (for a step that is not a power of 10, Decimal.quantize misses the grid)
        return convert.contracts_to_precision(self._ex, m, contracts)

    # ═══ end of section ═══════════════════════════════════════════════


    # ═══ network: metrics, order reconciliation, time sync, proxy check ══
    # Network layer code lives in bytx.net; only public Exchange methods here.

    _RECONCILE_TIMEOUT_SEC = 8.0
    _RECONCILE_DELAYS = (0.3, 0.7, 1.5, 2.0)  # after that, the last pause repeats until the deadline

    def metrics(self) -> MetricsSnapshot:
        """REST metrics snapshot: per-endpoint latency (p50/p90/p99), errors, retries,
        exchange rate-limit headers (e.g. ``x-mbx-used-weight-1m``), clock offset."""
        return self._net.snapshot()

    @property
    def latency_ms(self) -> Optional[float]:
        """Latency of the last HTTP request, ms (None if there have been no requests yet)."""
        return self._net.metrics.last_latency_ms

    @property
    def rate_limit_key(self) -> str:
        """Shared request limiter key (accounts with the same key share the limit)."""
        return self._net.limiter_key

    async def sync_time(self) -> Optional[float]:
        """Syncs the clock with the exchange; returns the offset in ms (None if the exchange does not report time)."""
        return await self._call(self._net.timesync.sync, force=True)

    async def check_proxy(self) -> float:
        """Channel (proxy) check: response time of the exchange's public time endpoint, ms.

        No retries; any error raises :class:`NetworkError`.
        """
        started = time.perf_counter()
        try:
            if self._ex.has.get('fetchTime'):
                await self._call(self._ex.fetch_time, safe=False)
            elif self._ex.id == 'hyperliquid':
                await self._call(self._ex.public_post_info, {'type': 'exchangeStatus'}, safe=False)
            else:
                raise NotSupported('no public time endpoint', exchange=self.name)
        except NetworkError:
            raise
        except BytxError as e:
            raise NetworkError(f'channel check failed: {e.message or e}', exchange=self.name,
                               details={'proxy': repr(self.proxy)}) from e
        return (time.perf_counter() - started) * 1000.0

    async def _net_warmup(self) -> None:
        try:
            # a neighbor's offset for the same key (fresher than SHARED_MAX_AGE_SEC), no request
            await self._net.timesync.sync(max_age=SHARED_MAX_AGE_SEC)
        except Exception:  # time sync must not block startup
            pass
        self._net.timesync.start()

    def _journal_op(self, symbol: Optional[str], ids: Iterable[Optional[str]] = ()) -> Any:
        """Cancel/amend via the SDK: marks it for the journal (this is not an intervention)."""
        if self.journal is None:
            return contextlib.nullcontext()
        if symbol is not None:
            try:
                symbol = self.symbol(symbol)
            except BytxError:
                pass   # markets not loaded yet: mark by the symbol as is
        return self.journal.own_op(self, symbol, ids)

    async def _pre_submit(self, request: dict, kind: str = 'order', **extra: Any) -> None:
        """``pre_submit`` hook before the placement network call.

        The hook receives a copy of the request: ``kind`` ('order' / 'take_profit' / 'stop_loss' / 'tp_sl'),
        ``symbol``, ``type``, ``side``, ``amount`` (contracts), ``price``, ``client_id`` (final,
        sent to the exchange; None if the order has none, Bybit trading-stop), ``reduce_only``,
        ``params`` (ccxt/exchange params) and for TP/SL ``trigger_price`` and ``trigger_by``. A hook exception
        is not caught: the request is not sent.
        """
        hook = self.pre_submit
        if hook is not None:
            view = {'kind': kind, **{k: request.get(k) for k in ('symbol', 'type', 'side', 'amount', 'price',
                                                                'client_id', 'reduce_only')},
                    'params': dict(request.get('params') or {}), **extra}
            result = hook(view)
            if inspect.isawaitable(result):
                await result
        if self.journal is not None:  # intent before sending, after the hook
            await self.journal._on_intent(self, request, kind, extra)
        if self.intents is not None:  # intent in sqlite, last before the network
            self.intents._on_intent(self, request, kind, extra)

    def _new_client_id(self, role: Optional[str] = None, level_id: Optional[int] = None) -> str:
        """client_id the SDK generates itself: with a journal and a known role, the new generation
        (tag + role), otherwise as before (``client_id_prefix`` tag + random tail)."""
        if self.journal is not None:
            cid = self.journal.client_id(self, role, level_id=level_id, market_type=self.market_type,
                                         tag=self.client_id_prefix)
            if cid is not None:
                return cid
        return generate_client_id(self._ex, self.market_type, self.client_id_prefix)

    async def _submit_order(self, request: dict, *, reconcile: bool = True) -> Order:
        """Sends the order (see :meth:`_send_order`); with a journal, ``placed`` / ``rejected``,
        with an intent store, the intent outcome."""
        if self.journal is None and self.intents is None and not self._observers:
            return await self._send_order(request, reconcile=reconcile)
        try:
            order = await self._send_order(request, reconcile=reconcile)
        except BytxError as e:
            if self._observers:
                self._emit_failed(request, e)
            if self.journal is not None:
                await self.journal._on_failed(self, request, e)
            if self.intents is not None:
                self.intents._on_failed(self, request, e)
            raise
        # intent outcome last: if we crash before it, recovery finds the order and completes the journal
        if self.journal is not None:
            await self.journal._on_placed(self, order, request)
        if self.intents is not None:
            self.intents._on_placed(self, order, request['client_id'])
        if self._observers:
            self.emit('order.placed', symbol=order.symbol, order=order, data={'request': request_view(request)})
        return order

    async def _protect(self, order: Order, request: dict) -> None:
        """TP/SL as separate orders after entry; with an intent store the legs are children of the entry,
        and after placement the entry is marked protected (recovery does not place them again)."""
        try:
            if self.intents is None:
                await self._attach_tp_sl(order, request)
                return
            with self.intents.parent(request['client_id']):
                await self._attach_tp_sl(order, request)
        except ProtectionNotPlaced as e:
            if self._observers:
                self.emit('protection.failed', symbol=order.symbol, order=order, error=e)
            raise
        self.intents._on_protected(self, request['client_id'])

    async def _tp_sl_placed(self, orders: Iterable[Any]) -> None:
        """Placed TP/SL: journal (role tp/sl), then the legs' intent outcomes."""
        orders = list(orders)
        if self._observers and orders:
            self.emit('tp_sl.placed', symbol=getattr(orders[0], 'symbol', None), data={'orders': orders})
        if self.journal is not None:
            await self.journal._on_tp_sl(self, orders)
        if self.intents is not None:
            for o in orders:
                self.intents._on_placed(self, o)

    async def _send_order(self, request: dict, *, reconcile: bool = True) -> Order:
        """Sends the order (no retries). Unclear outcome: reconcile by client_id.

        Error outcome follows the exchange code table (:mod:`bytx.outcome`): an explicit rejection is raised
        as is (reduce-only "no position" raises :class:`PositionAlreadyClosed`); duplicate client_id,
        timeout, "status unknown" and other unclear cases are reconciled; not found raises :class:`OrderStatusUnknown`.
        """
        await self._pre_submit(request)
        try:
            raw = await self._call(self._ex.create_order, request['symbol'], request['type'],
                                   request['side'], _ccxt_amount(request), request['price'],
                                   request['params'], safe=False)
        except BytxError as e:
            result = await self._unplaced(request, e, reconcile=reconcile)
            if isinstance(result, Order):
                return result
            if result is e:
                raise
            raise result from result.__cause__
        self._remember_algo(raw)
        order = self._order(raw, fallback=request)
        if self.market_type == 'spot' and request['type'] == 'market':
            order = await self._complete_market(order, request['symbol'])
        return order

    async def _complete_market(self, order: Order, s: str) -> Order:
        """Spot market: a placement response without ``filled`` (Bybit, OKX, KuCoin — only the id) — the final order
        from the ``ex.stream.orders()`` stream if it is subscribed (up to ``_WS_FILL_WAIT`` s, no request), otherwise
        one :meth:`order` (filled, average, fee); if it cannot be read — the response as is with
        ``partial_info=True``. ``average`` goes to the minimum-check price cache (the next order needs no ticker)."""
        streams = getattr(self, '_streams', None)
        if order.filled is None and order.id and streams is not None:
            ws = await streams.final_order(order.id, _WS_FILL_WAIT)
            if ws is not None and ws.filled is not None:
                order = ws
        if order.filled is None and order.id:
            try:
                order = await self.order(s, order.id)
            except BytxError:
                order.partial_info = True
                return order
        if order.average:
            self._check_prices[s] = (order.average, time.monotonic())
        return order

    async def _unplaced(self, request: dict, error: BytxError, *, reconcile: bool) -> Union[Order, BytxError]:
        """create_order error → an error for the caller or an order found by reconciliation."""
        kind = classify_outcome(self._ex.id, error, has_client_id=bool(request.get('client_id')))
        if kind == 'rejected':
            typed = position_error(self._ex.id, error, reduce_only=bool(request.get('reduce_only')))
            if isinstance(typed, PositionAlreadyClosed) and request.get('reduce_only'):
                return await self._verify_position_closed(request, typed)
            return typed or error
        if not reconcile:
            return self._status_unknown(request, error, 'reconciliation disabled (reconcile=False)')
        found, ok_scans, scan_error = await self._reconcile(request['symbol'], request['client_id'])
        if found is not None:
            return found
        if kind == 'exchange_error' and ok_scans >= 2 and scan_error is None:
            # exchange replied with an error without a known code, ≥ 2 successful scans did not find the order:
            # rejection (heuristic); the original error as is
            return error
        reason = ('order not found by client_id within the reconciliation window' if scan_error is None
                  else f'reconciliation failed: {scan_error.message or scan_error}')
        return self._status_unknown(request, error, reason)

    async def _verify_position_closed(self, request: dict, typed: BytxError) -> BytxError:
        """Reduce-only rejected with a "no position" code that also means "the side is covered by resting closing
        orders" (POSITION_CLOSED_IF_REDUCE: Binance -2022, Bybit 110017, BingX 101212, HTX 1048) — the position is
        re-read: open → :class:`PositionCovered`; none → PositionAlreadyClosed; read failure → InvalidOrder with
        ``details['position_unverified']`` (not "closed"). Unambiguous "no position" codes are not re-read."""
        from bytx.outcome import position_close_ambiguous
        if not position_close_ambiguous(self._ex.id, typed):
            return typed
        s = request['symbol']
        side = 'long' if request.get('side') == 'sell' else 'short'   # a reduce-only sell reduces a long
        try:
            rows = await self.positions([s])
        except BytxError as read_error:
            details = dict(typed.details) if isinstance(typed.details, dict) else {}
            details.update(symbol=s, side=side, position_unverified=True)
            unverified = InvalidOrder(f'{typed.message or typed} — the position was not read after the rejection '
                                      f'({read_error.message or read_error}): whether it is closed is unknown',
                                      exchange=self.name, code=typed.code, details=details)
            unverified.__cause__ = read_error
            return unverified
        # hedge — only its own side; one-way — any non-zero position of the symbol
        open_rows = [p for p in rows if p.symbol == s and (p.contracts or 0) > 0
                     and (p.side == side or not p.hedged)]
        if not open_rows:
            return typed
        contracts = sum(float(p.contracts or 0) for p in open_rows)
        from loguru import logger
        logger.error(f'{self.name}: close rejected: {s} {side} covered by resting reduce orders ({typed.code}) '
                     f'contracts={contracts}')
        covered = PositionCovered(
            f'position {s} {side} is open ({contracts} contracts), but the exchange answered "no position": '
            f'{typed.message or typed} — the side is covered by resting closing orders (cancel them and retry)',
            exchange=self.name, code=typed.code,
            details={'symbol': s, 'side': side, 'contracts': contracts, 'code': typed.code})
        covered.__cause__ = typed
        return covered

    def _status_unknown(self, request: dict, error: BytxError, reason: str) -> OrderStatusUnknown:
        """An "outcome unknown" error with order fields; ``__cause__`` is the create_order error."""
        unknown = OrderStatusUnknown(
            f'order outcome unknown ({reason}), client_id {request["client_id"]}: {error.message or error}',
            exchange=self.name, code=error.code,
            details={'client_id': request['client_id'], 'symbol': request['symbol']},
            client_order_id=request['client_id'], symbol=request['symbol'], side=request['side'],
            amount=request['amount'])
        unknown.__cause__ = error
        return unknown

    def _remember_algo(self, raw: Any) -> None:
        """Binance: the order went to the Algo API; remember the algoId for order/cancel_order."""
        if self._ex.id == 'binance' and isinstance(raw, dict) and isinstance(raw.get('info'), dict) \
                and 'algoId' in raw['info']:
            self._binance_algo_ids.add(str(raw['info']['algoId']))

    def _algo_space(self) -> bool:
        return self._ex.id == 'binance' and self.market_type == 'swap'

    async def _find_gate_price_order(self, s: str, order_id: str) -> Optional[dict]:
        """Gate: price-triggered order (TP/SL) of this contract with this id (as a ccxt order) or None.

        GET /futures/{settle}/price_orders/{order_id}: "Query single auto order details"; regular
        orders (GET orders/{order_id}) have ids from a different space."""
        m = self._ex.market(s)
        if not m.get('swap') or not order_id.isdigit():
            return None
        try:
            raw = await self._call(self._ex.fetch_order, order_id, s, {'trigger': True})
        except OrderNotFound:
            return None
        info = raw.get('info') if isinstance(raw, dict) else None
        info = info if isinstance(info, dict) else {}
        initial = info.get('initial')
        if not isinstance(initial, dict) or initial.get('contract') != m['id'] or str(info.get('id')) != order_id:
            return None
        return raw

    async def _find_binance_algo(self, s: str, algo_id: str) -> Optional[dict]:
        """GET /fapi/v1/algoOrder: algo order of THIS symbol with this algoId, or None."""
        if not self._algo_space() or not algo_id.isdigit():
            return None
        mid = self._ex.market(s)['id']
        try:
            # Query Algo Order docs: params algoId / clientAlgoId (no symbol), so the symbol
            # is checked against the response
            resp = await self._call(self._ex.request, 'algoOrder', 'fapiPrivate', 'GET', {'algoId': algo_id})
        except OrderNotFound:
            return None  # -2013 "Order does not exist.": confirmed absence
        # other errors (network, 5xx, -1000) are not "no order": the caller decides
        if not isinstance(resp, dict) or str(resp.get('algoId')) != algo_id or resp.get('symbol') != mid:
            return None
        return resp

    async def _is_unregistered_algo(self, s: str, order_id: str) -> bool:
        """Whether ``order_id`` can be canceled as an algoId: no regular order with this id exists,
        and an algo order with it exists for this symbol. Any ambiguity means no (foreign orders are left alone)."""
        if not self._algo_space() or not order_id.isdigit():
            return False
        try:
            await self._call(self._ex.fetch_order, order_id, s)
        except OrderNotFound:
            pass
        except BytxError:
            return False
        else:
            return False  # a regular order (filled/canceled): not an algo
        try:
            return await self._find_binance_algo(s, order_id) is not None
        except BytxError:
            return False

    async def _binance_algo(self, method: str, s: str, algo_id: str) -> dict:
        """GET/DELETE /fapi/v1/algoOrder by algoId → ccxt order (docs: algoId only, no symbol)."""
        resp = await self._call(self._ex.request, 'algoOrder', 'fapiPrivate', method, {'algoId': algo_id},
                                safe=(method == 'GET'))
        return self._ex.parse_order(binance_algo_as_order(resp), self._ex.market(s))

    async def _reconcile_order(self, s: str, client_id: str) -> Optional[Order]:
        """Looks up an order by client_id several times with pauses until the deadline; None if not found."""
        found, _, _ = await self._reconcile(s, client_id)
        return found

    async def _reconcile(self, s: str, client_id: str, *, timeout: Optional[float] = None,
                         first_delay: bool = True) -> tuple[Optional[Order], int, Optional[BytxError]]:
        """Scans by client_id with pauses until the deadline → (order, successful empty scans, error
        of the last scan or None). Deadline: the lesser of ``timeout`` (default
        ``_RECONCILE_TIMEOUT_SEC``) and the overall call deadline (:meth:`deadline`)."""
        loop = asyncio.get_running_loop()
        budget = self._RECONCILE_TIMEOUT_SEC if timeout is None else timeout
        left = deadline_left()
        if left is not None:
            budget = min(budget, left)
        deadline = loop.time() + budget
        planned = 0.0  # sum of pauses: caps the number of attempts even if the loop clock is frozen
        attempt = ok_scans = 0
        last_error: Optional[BytxError] = None
        scan_time = 0.0  # duration of the last completed scan
        while True:
            delay = 0.0 if attempt == 0 and not first_delay else \
                self._RECONCILE_DELAYS[min(attempt, len(self._RECONCILE_DELAYS) - 1)]
            attempt += 1
            planned += delay
            # do not start a scan that cannot finish before the deadline: its timeout would overwrite
            # the result of earlier scans (Bitget: scan ≈ 0.9 s, the 4th started 0.8 s before the deadline)
            if planned >= budget or loop.time() + delay + scan_time >= deadline:
                break
            if delay:
                await asyncio.sleep(delay)
            remaining = max(deadline - loop.time(), 0.05)
            started = loop.time()
            try:
                order = await asyncio.wait_for(self._order_by_client_id(s, client_id), remaining)
            except OrderNotFound:
                ok_scans += 1
                last_error = None
                scan_time = loop.time() - started
                continue
            except NetworkError as e:
                last_error = e
                continue
            except asyncio.TimeoutError:
                if ok_scans >= 2:
                    break  # the deadline is ours; ≥ 2 scans already did not find the order, their result stands
                last_error = RequestTimeout('client_id scan did not fit within the deadline', exchange=self.name)
                continue
            except BytxError as e:
                last_error = e  # the exchange cannot search by client_id: no point retrying
                break
            self._net.metrics.reconciled += 1
            return order, ok_scans, None
        return None, ok_scans, last_error


def _match_batch(raw: Any, requests: list[dict]) -> list[Optional[dict]]:
    """createOrders response items matched to requests.

    The ccxt response order cannot be trusted: ``parse_orders`` sorts by timestamp, and a rejection
    without a time (Binance ``{code, msg}``) moves to the front; BingX omits rejections entirely.
    So items with ``clientOrderId`` are matched by it (with a broker prefix,
    by suffix). The rest (rejections, responses without client_id: Hyperliquid) are assigned
    in order to unmatched requests, only if their counts match. Otherwise None:
    outcome unknown, reconcile by client_id.
    """
    rows = [o for o in raw if isinstance(o, dict)] if isinstance(raw, list) else []
    out: list[Optional[dict]] = [None] * len(requests)
    cids = [str(r['client_id']).lower() for r in requests]
    rest: list[dict] = []
    for o in rows:
        got = str(o.get('clientOrderId') or '').lower()
        if not (o.get('id') and got):
            rest.append(o)
            continue
        free = [i for i, c in enumerate(cids) if out[i] is None and got.endswith(c)]
        # an exact match beats a suffix match (one request's cid may be the tail of another's)
        i = next((i for i in free if got == cids[i]), free[0] if len(free) == 1 else None)
        if i is not None:
            out[i] = o
        # client_id is not ours or ambiguous: the item is attributed to nobody
    unmatched = [i for i, o in enumerate(out) if o is None]
    if rest and len(rest) == len(unmatched):
        for i, o in zip(unmatched, rest):
            out[i] = o
    return out


def _element_error(raw: Any) -> Optional[tuple[Optional[str], str]]:
    """Is the batch response item an exchange rejection? (code, text) or None.

    Binance/Bybit/KuCoin — ``code``/``msg``, OKX — ``sCode``/``sMsg``, Hyperliquid — ``error``.
    """
    if not isinstance(raw, dict):
        return None
    info: dict = raw['info'] if isinstance(raw.get('info'), dict) else {}
    if info.get('error'):
        return None, str(info['error'])
    s_code = info.get('sCode')
    if s_code not in (None, '', '0', 0):
        return str(s_code), str(info.get('sMsg') or s_code)
    code = info.get('code')
    if code not in (None, '', '0', 0, '200', '200000', 200):
        return str(code), str(info.get('msg') or code)
    # Bitget Batch Order: a failureList item is {clientOid, errorCode, errorMsg}
    if info.get('errorCode') not in (None, '', '00000'):
        return str(info['errorCode']), str(info.get('errorMsg') or info['errorCode'])
    if raw.get('status') == 'rejected':
        return None, str(info or 'rejected by the exchange')
    return None


# Markets where ccxt 4.4.65 createOrders refuses before the network (NotSupported) despite has['createOrders']:
# Binance "*contract only*" (the Spot API has no batch placement), Gate "does not support futures
# or options markets", MEXC "only supported for spot markets". There place_orders goes one by one.
# Max orders per batch request (exchange docs): Binance USDⓈ-M Place Multiple Orders:
# "Max 5 orders"; OKX batch-orders: up to 20. Bybit (linear up to 20 per v5 docs) and BingX: with a margin,
# 10 and 5: a smaller chunk is always allowed. Not in the table: the whole batch at once.
_BATCH_MAX: dict[str, int] = {'binance': 5, 'binanceusdm': 5, 'okx': 20, 'bybit': 10, 'bingx': 5, 'blofin': 20}
# A batch holds one symbol only (BloFin Place Multiple Orders: 152007 «same instId and marginMode»); otherwise one by one
_BATCH_ONE_SYMBOL = frozenset({'blofin'})

_NO_BATCH: dict[str, Callable[[dict], bool]] = {
    'binance': lambda m: bool(m.get('spot')),
    'gate': lambda m: not m.get('spot'),
    'mexc': lambda m: not m.get('spot'),
}


def _batch_endpoint(ex: Any, requests: list[dict]) -> bool:
    """Whether a single-request batch (createOrders) is possible for the ``requests`` markets."""
    if not ex.has.get('createOrders'):
        return False
    if ex.id in _BATCH_ONE_SYMBOL and len({r['symbol'] for r in requests}) > 1:
        return False
    rule = _NO_BATCH.get(ex.id)
    if rule is None:
        return True
    for r in requests:
        try:
            market = ex.market(r['symbol'])
        except Exception:
            return False
        if rule(market):
            return False
    return True


def create_exchange(name: str, *, api_key: Optional[str] = None, secret: Optional[str] = None,
                    password: Optional[str] = None, wallet_address: Optional[str] = None,
                    private_key: Optional[str] = None, demo: DemoArg = False, market: str = 'swap',
                    proxy: Optional[str | Proxy] = None, broker: Optional[BrokerConfig] = None,
                    margin_mode: Optional[MarginMode] = None, hedge: Optional[bool] = None,
                    timeout: float = 10.0, recv_window_ms: Optional[int] = None,
                    options: Optional[dict] = None, rate_limit_key: Optional[str] = None,
                    rate_limit: Optional[float] = None, retries: int = 3,
                    backoff: tuple[float, float] = (0.2, 5.0),
                    time_sync_interval: Optional[float] = None,
                    risk: Optional[RiskGuard] = None,
                    client_id_prefix: Optional[str] = None, client_id_strict: bool = False,
                    pre_submit: Optional[Callable[[dict], Any]] = None,
                    journal: Optional['PositionJournal'] = None, paper: bool = False,
                    paper_config: Optional['PaperConfig'] = None,
                    intents: Optional['IntentStore'] = None) -> Exchange:
    """Creates an exchange client.

    name: binance, bybit, okx, bingx, hyperliquid, kucoin.
    market: 'swap' (perpetual futures) or 'spot'.
    demo: True for the exchange's demo/testnet (no real money). Binance: True (= 'demo') is
    Demo Mode demo.binance.com (prices, filters and limits as on the live exchange; spot and futures with one key);
    'testnet' is the legacy testnet (testnet.binance.vision / testnet.binancefuture.com, independent
    prices and books). Environment: :attr:`Exchange.demo_env`; other values raise ConfigError.
    proxy: 'http://user:pass@host:port' or 'socks5://…', for REST and WebSocket.
    hyperliquid: wallet_address + private_key (or api_key + secret).

    Network:
    rate_limit_key: shared request limiter key; default "exchange + proxy"
    (accounts behind one proxy/IP share the limit). Set the same key if different
    proxies exit through one IP.
    rate_limit: optional requests-per-second cap for this account.
    retries, backoff=(base, max): retries of safe requests (reads, cancel by id)
    with exponential backoff and jitter; orders are never retried.
    time_sync_interval: seconds between background clock syncs (None: only
    on connect and after a time error).
    risk: :class:`bytx.protection.RiskGuard`, pre-send order checks.
    margin_mode: default margin mode, 'cross' or 'isolated' (case-insensitive);
    anything else raises ConfigError. Per-symbol mode: :meth:`Exchange.set_margin_mode`.
    None (default) means not set: OKX/KuCoin orders and OKX/HL leverage
    require it explicitly (ConfigError instead of a silent 'cross'); close and reduce use the
    open position's mode. Binance/Bybit/BingX keep the mode on the exchange and do not need it.
    retries=0: no read retries (for services with their own circuit breaker and time budget).
    client_id_prefix: bot tag (letters/digits up to 14; Hyperliquid: hex, BingX: no uppercase):
    every client_id the SDK generates itself (entry, close, reduce, batch) starts
    with it, so your orders can be found after a timeout and restart. Invalid raises ConfigError.
    client_id_strict: a client_id that together with the Binance broker prefix
    ``x-<ID>`` exceeds 36 raises InvalidOrder before sending (by default it is deterministically shortened and
    keeps the prefix; what is sent — :func:`bytx.exchange_client_id`). Limit:
    :func:`bytx.max_client_id_len`. An invalid client_id format always raises InvalidOrder: the SDK
    never replaces the caller's id.
    pre_submit: ``callable(request: dict)`` (may be async), called before
    every placement network call with the final client_id: persist the intent before sending.
    A hook exception cancels placement (the request is not sent). See :meth:`Exchange.place_order`.
    journal: :class:`bytx.journal.PositionJournal`, position journal (opt-in):
    placements, cancellations, amendments, TP/SL and tracker events are written to the position journal; client_ids
    the SDK generates itself for a known role are the new generation (tag + role).
    paper: True gives :class:`bytx.paper.PaperExchange`: the same interface, orders
    are filled in memory against the exchange's live order book (virtual balance, default-tier fees
    per exchange docs); keys are not needed and not passed to ccxt. ``paper_config``:
    :class:`bytx.paper.PaperConfig` (balance, fees, leverage, funding). Paper does not replace
    the exchange's demo/testnet (simplified liquidation).
    intents: :class:`bytx.recovery.IntentStore`: every order is written to sqlite before
    sending, the outcome after; at startup :meth:`Exchange.recover` reconciles them with the exchange (pick up
    live ones, re-create missing ones with the same client_id, close dangling ones).
    """
    if market not in ('swap', 'spot'):
        raise ConfigError("market: 'swap' or 'spot'", exchange=name.lower(), details={'market': market})
    if margin_mode is not None:
        margin_mode = _check_margin_mode(margin_mode, name.lower())
    proxy_obj = Proxy(proxy) if isinstance(proxy, str) else proxy
    if paper:
        from bytx.paper.exchange import build_paper
        paper_ex = build_paper(name, market=market, demo=demo, proxy=proxy_obj, timeout=timeout, options=options,
                               config=paper_config, margin_mode=margin_mode, hedge=hedge,
                               client_id_prefix=client_id_prefix, client_id_strict=client_id_strict,
                               pre_submit=pre_submit, journal=journal, intents=intents)
        paper_ex._net.configure(rate_limit_key=rate_limit_key, rate_limit=rate_limit, retries=retries,
                                backoff=backoff, time_sync_interval=time_sync_interval)
        if risk is not None:
            paper_ex.risk = risk
        return paper_ex
    ex = build_ccxt(name, market_type=market, api_key=api_key, secret=secret, password=password,
                    wallet_address=wallet_address, private_key=private_key, demo=demo,
                    proxy=proxy_obj, broker=broker, timeout_ms=int(timeout * 1000),
                    recv_window_ms=recv_window_ms, options=options)
    exchange = Exchange(ex, name=name.lower(), market_type=market, demo=demo, proxy=proxy_obj,
                        margin_mode=margin_mode, hedge=hedge, client_id_prefix=client_id_prefix,
                        client_id_strict=client_id_strict, pre_submit=pre_submit, journal=journal,
                        intents=intents)
    exchange._net.configure(rate_limit_key=rate_limit_key, rate_limit=rate_limit, retries=retries,
                            backoff=backoff, time_sync_interval=time_sync_interval)
    if risk is not None:
        exchange.risk = risk
    return exchange


__all__ = ['Exchange', 'create_exchange', 'EXCHANGES']
