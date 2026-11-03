"""Public WS streams: order book, ticker, best prices, trades, candles.

Each function builds a :class:`FeedSpec` — how to call ccxt ``watch_*`` and how to parse
the response into :mod:`bytx.types` models (amounts in base coin, via :mod:`bytx.convert`).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx import convert
from bytx.errors import NotSupported
from bytx.streams.orderbook import check_book, reset_book_state, to_book
from bytx.streams.supervisor import Feed, FeedSpec
from bytx.types import Market


def symbol_tokens(symbol: str, market: Optional[Market]) -> tuple[str, ...]:
    """Substrings used to find the stream's ccxt client in ``exchange.clients``.

    Numeric ids (hyperliquid: '0', '1'…) and too-short strings are skipped —
    they would match other subscriptions.
    """
    tokens = {symbol}
    if market is not None and market.id:
        for mid in (market.id, market.id.lower()):
            if len(mid) >= 3 and any(c.isalpha() for c in mid):
                tokens.add(mid)
    return tuple(sorted(tokens))


def _unwatch(ccxt_ex: Any, method: str, *args: Any) -> Any:
    """Topic unsubscribe via ccxt ``un_watch_*`` (None — ccxt has none for this exchange).

    ccxt base class stubs (raising NotSupported) do not count as an unsubscribe.
    """
    fn = getattr(ccxt_ex, method, None)
    if fn is None:
        return None
    if method not in getattr(ccxt_ex, '__dict__', {}):
        owner = next((k for k in type(ccxt_ex).__mro__ if method in k.__dict__), None)
        if owner is None or owner.__module__.startswith(('ccxt.base.', 'ccxt.async_support.base.')):
            return None
    return lambda: fn(*args)


def _require(ccxt_ex: Any, capability: str, name: str) -> None:
    if not ccxt_ex.has.get(capability):
        raise NotSupported(f'{capability} is not supported', exchange=name)


def order_book(hub: Any, symbol: str, market: Optional[Market], *, limit: Optional[int] = None,
               depth: Optional[int] = None, params: Optional[dict] = None) -> FeedSpec:
    """Order book. ``limit`` goes to ccxt (exchange subscription depth, values differ per exchange);
    the consumer depth (``depth``) is sliced locally, and consumers with different
    depths share one subscription."""
    ex = hub.ccxt
    _require(ex, 'watchOrderBook', hub.name)
    p = dict(params or {})
    if limit is None and depth is not None:
        limit = ws_book_limit(ex.id, market, depth)

    def watch() -> Any:
        return ex.watch_order_book(symbol, limit, p)

    def conv(raw: Any, feed: Feed) -> list:
        return [to_book(raw, market, feed.depth)]

    return FeedSpec(kind='order_book', key=('order_book', symbol, limit, _frozen(p)), symbol=symbol,
                    watch=watch, convert=conv, policy='latest', tokens=symbol_tokens(symbol, market),
                    check=check_book, on_reconnect=reset_book_state,
                    unwatch=_unwatch(ex, 'un_watch_order_book', symbol, p))


def ws_book_limit(exchange_id: str, market: Optional[Market], depth: int) -> Optional[int]:
    """Exchange subscription depth for the depth the consumer needs (None — ccxt default)."""
    if exchange_id == 'bybit':
        from bytx.exchanges.bybit.ccxt_fixes import ws_book_limit as bybit_limit
        return bybit_limit(depth, option=bool(market and market.raw.get('option')))
    return None


def ticker(hub: Any, symbol: str, market: Optional[Market], *, params: Optional[dict] = None) -> FeedSpec:
    ex = hub.ccxt
    _require(ex, 'watchTicker', hub.name)
    p = dict(params or {})

    def watch() -> Any:
        return ex.watch_ticker(symbol, p)

    def conv(raw: Any, feed: Feed) -> list:
        return [convert.ticker(raw, market)] if raw else []

    return FeedSpec(kind='ticker', key=('ticker', symbol, _frozen(p)), symbol=symbol, watch=watch,
                    convert=conv, policy='latest', tokens=symbol_tokens(symbol, market),
                    unwatch=_unwatch(ex, 'un_watch_ticker', symbol, p))


def best_bid_ask(hub: Any, symbol: str, market: Optional[Market], *,
                 params: Optional[dict] = None) -> FeedSpec:
    """Best bid/ask: ``watch_bids_asks`` (bookTicker / bbo) where the exchange supports it, otherwise the ticker."""
    ex = hub.ccxt
    p = dict(params or {})
    if not ex.has.get('watchBidsAsks'):
        _require(ex, 'watchTicker', hub.name)
        unwatch = _unwatch(ex, 'un_watch_ticker', symbol, p)

        def watch() -> Any:
            return ex.watch_ticker(symbol, p)

        def conv(raw: Any, feed: Feed) -> list:
            return [convert.ticker(raw, market)] if raw else []
    else:
        unwatch = _unwatch(ex, 'un_watch_bids_asks', [symbol], p)

        def watch() -> Any:
            return ex.watch_bids_asks([symbol], p)

        def conv(raw: Any, feed: Feed) -> list:
            t = (raw or {}).get(symbol)
            return [convert.ticker(t, market)] if t else []

    return FeedSpec(kind='best_bid_ask', key=('best_bid_ask', symbol, _frozen(p)), symbol=symbol,
                    watch=watch, convert=conv, policy='latest', tokens=symbol_tokens(symbol, market),
                    unwatch=unwatch)


def trades(hub: Any, symbol: str, market: Optional[Market], *, params: Optional[dict] = None) -> FeedSpec:
    """Trade tape. ccxt (newUpdates) returns only trades new since the previous call."""
    ex = hub.ccxt
    _require(ex, 'watchTrades', hub.name)
    p = dict(params or {})

    def watch() -> Any:
        return ex.watch_trades(symbol, None, None, p)

    def conv(raw: Any, feed: Feed) -> list:
        return [convert.trade(t, market) for t in raw or []]

    return FeedSpec(kind='trades', key=('trades', symbol, _frozen(p)), symbol=symbol, watch=watch,
                    convert=conv, policy='queue', tokens=symbol_tokens(symbol, market),
                    unwatch=_unwatch(ex, 'un_watch_trades', symbol, p))


def candles(hub: Any, symbol: str, market: Optional[Market], *, timeframe: str = '1m',
            params: Optional[dict] = None) -> FeedSpec:
    """Candles: every update of the current candle (and the close of the previous one)."""
    ex = hub.ccxt
    _require(ex, 'watchOHLCV', hub.name)
    p = dict(params or {})

    def watch() -> Any:
        return ex.watch_ohlcv(symbol, timeframe, None, None, p)

    def conv(raw: Any, feed: Feed) -> list:
        return [convert.candle(c, ex.id, market) for c in raw or []]

    return FeedSpec(kind='candles', key=('candles', symbol, timeframe, _frozen(p)), symbol=symbol,
                    watch=watch, convert=conv, policy='queue', tokens=symbol_tokens(symbol, market),
                    timestamp=lambda c: None,   # candle open time is not the message time
                    unwatch=_unwatch(ex, 'un_watch_ohlcv', symbol, timeframe, p))


def _frozen(params: dict) -> Optional[tuple]:
    return tuple(sorted((k, repr(v)) for k, v in params.items())) or None


BUILDERS = {
    'order_book': order_book,
    'ticker': ticker,
    'best_bid_ask': best_bid_ask,
    'trades': trades,
    'candles': candles,
}

__all__ = ['BUILDERS', 'order_book', 'ticker', 'best_bid_ask', 'trades', 'candles', 'symbol_tokens']
