"""ccxt 4.4.65 fixes for kucoin (see tests/ground_truth).

Reference — the official KuCoin spec (github.com/Kucoin/kucoin-universal-sdk, which
www.kucoin.com/docs-new is built from). Patches only for kucoinfutures (all SDK trading
goes through it); spot ccxt.kucoin is left alone.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

_NS_PER_MS = 1_000_000
_APPLIED = '_bytx_kucoin_fixes'


def _ns_to_ms(value: Any) -> Optional[int]:
    """Nanoseconds → ms as an integer (ccxt's float × 1e-6 loses 1 ms at 1.7e18)."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value) // _NS_PER_MS
    except (TypeError, ValueError):
        return None


def _set_ts(exchange: Any, result: Any, ts: Optional[int]) -> Any:
    if ts is not None and isinstance(result, dict):
        result['timestamp'] = ts
        result['datetime'] = exchange.iso8601(ts)
    return result


def _wrap(exchange: Any, name: str, make: Callable[[Callable], Callable]) -> None:
    orig = getattr(exchange, name, None)
    if orig is not None:
        setattr(exchange, name, make(orig))


def _fix_funding_rate(exchange: Any) -> None:
    # Get Current Funding Rate (docs-new/api-3470265): timePoint — the moment the
    # current rate was computed, the next computation — fundingTime. ccxt puts timePoint into
    # fundingTimestamp (= next funding time in the unified structure).
    def make(orig):
        def parse_funding_rate(data, market=None):
            result = orig(data, market)
            nxt = exchange.safe_integer(data, 'fundingTime') if isinstance(data, dict) else None
            if nxt is not None and isinstance(result, dict):
                result['fundingTimestamp'] = nxt
                result['fundingDatetime'] = exchange.iso8601(nxt)
            return result
        return parse_funding_rate
    _wrap(exchange, 'parse_funding_rate', make)


def _fix_trade_time(exchange: Any) -> None:
    # Get Trade History / Fills (GET /api/v1/fills): trade time is tradeTime (ns),
    # createdAt is the order creation time. ts (ns) of public trades and WS execution
    # is converted to ms as an integer.
    def make(orig):
        def parse_trade(trade, market=None):
            result = orig(trade, market)
            if not isinstance(trade, dict):
                return result
            ts = _ns_to_ms(trade.get('ts'))
            if ts is None:
                ts = _ns_to_ms(trade.get('tradeTime'))
            return _set_ts(exchange, result, ts)
        return parse_trade
    _wrap(exchange, 'parse_trade', make)


def _fix_ns_timestamps(exchange: Any) -> None:
    # WS tickerV1/tickerV2 (ts, ns) and Order change pushes (orderTime, ns):
    # ccxt computes via safe_integer_product(×1e-6) in float → minus 1 ms.
    def make_ticker(orig):
        def parse_ticker(ticker, market=None):
            result = orig(ticker, market)
            ts = _ns_to_ms(ticker.get('ts')) if isinstance(ticker, dict) else None
            return _set_ts(exchange, result, ts)
        return parse_ticker

    def make_bid_ask(orig):
        def parse_ws_bid_ask(ticker, market=None):
            result = orig(ticker, market)
            data = ticker.get('data') if isinstance(ticker, dict) else None
            ts = _ns_to_ms(data.get('ts')) if isinstance(data, dict) else None
            return _set_ts(exchange, result, ts)
        return parse_ws_bid_ask

    def make_ws_order(orig):
        def parse_ws_order(order, market=None):
            result = orig(order, market)
            ts = _ns_to_ms(order.get('orderTime')) if isinstance(order, dict) else None
            return _set_ts(exchange, result, ts)
        return parse_ws_order

    _wrap(exchange, 'parse_ticker', make_ticker)
    _wrap(exchange, 'parse_ws_bid_ask', make_bid_ask)
    _wrap(exchange, 'parse_ws_order', make_ws_order)


def _handle_wallet_balance(exchange: Any) -> Callable:
    # Private WS /contractAccount/wallet, subject walletBalance.change: free =
    # availableBalance, total = equity (like accountEquity in REST account-overview,
    # which ccxt uses for fetch_balance) — used = total − free.
    def handle_wallet_balance(client, message):
        data = exchange.safe_dict(message, 'data', {})
        exchange.balance['info'] = data
        code = exchange.safe_currency_code(exchange.safe_string(data, 'currency'))
        account = exchange.account()
        account['free'] = exchange.safe_string(data, 'availableBalance')
        account['total'] = exchange.safe_string(data, 'equity')
        exchange.balance[code] = account
        exchange.balance = exchange.safe_balance(exchange.balance)
        client.resolve(exchange.balance, 'balance')
    return handle_wallet_balance


def _fix_ws_subjects(exchange: Any) -> None:
    # Private WS (spec ws/openapi-futures-private.json): the order push on
    # /contractMarket/tradeOrders[:symbol] comes with subject 'symbolOrderChange',
    # balance — 'walletBalance.change'. ccxt knows only orderChange/orderUpdated and
    # the deprecated availableBalance.change — such pushes were silently lost.
    extra = {
        'symbolOrderChange': lambda client, message: exchange.handle_order(client, message),
        'walletBalance.change': _handle_wallet_balance(exchange),
    }

    def make(orig):
        def handle_subject(client, message):
            method = extra.get(message.get('subject')) if isinstance(message, dict) else None
            if method is not None:
                return method(client, message)
            return orig(client, message)
        return handle_subject
    _wrap(exchange, 'handle_subject', make)


def _fix_ws_candles(ex: Any) -> None:
    """WS candles /contractMarket/limitCandle: field order and time units.

    Push: candles = [start (SECONDS), open, CLOSE, HIGH, LOW, turnover (quote), volume (lots)]
    — verified against live data 2026-10-25 with REST /api/v1/kline/query of the same interval
    (there [ms, open, high, low, close, lots, turnover]). ccxt 4.4.65 reads [1..4] as
    o/h/l/c and the time as ms → high/low/close mixed up, a candle «from the 1970s».
    Reordered into the order ccxt expects, before it parses (volume — index 6, as in ccxt).
    """
    if not hasattr(ex, 'handle_ohlcv'):
        return
    original = ex.handle_ohlcv

    def handle_ohlcv(client, message):
        data = message.get('data') if isinstance(message, dict) else None
        raw = data.get('candles') if isinstance(data, dict) else None
        if isinstance(raw, list) and len(raw) >= 7:
            start = ex.safe_integer(raw, 0)
            fixed = [None if start is None else str(start * 1000), raw[1], raw[3], raw[4], raw[2], raw[5], raw[6]]
            message = {**message, 'data': {**data, 'candles': fixed}}
        return original(client, message)

    ex.handle_ohlcv = handle_ohlcv


def _fix_ws_token(ex: Any) -> None:
    """WS: a new token for every new connection.

    Docs (Classic WebSocket → Introduction, «2. Apply Connect Token»): «The token is valid for only
    24 hours, and a single connection is expected to be disconnected after 24 hours». ccxt 4.4.65
    caches the token URL in ``options['urls'][connectId]`` and resets it only on the message
    ``token is expired``: after a disconnect (including the scheduled 24 h one) it reconnected with the old token.
    If there is no longer a connection for the cached URL (ccxt removed the client on close/error), the URL
    is reset and ``negotiate`` requests bullet-public/private again.
    """
    if not hasattr(ex, 'negotiate'):
        return
    original = ex.negotiate

    async def negotiate(privateChannel, params={}):
        urls = ex.options.get('urls') if isinstance(ex.options, dict) else None
        connect_id = 'private' if privateChannel else 'public'
        cached = urls.get(connect_id) if isinstance(urls, dict) else None
        if cached is not None and hasattr(cached, 'done') and cached.done():
            url = None if cached.cancelled() or cached.exception() is not None else cached.result()
            if url not in (ex.clients or {}):
                del urls[connect_id]
        return await original(privateChannel, params)

    ex.negotiate = negotiate


def apply(exchange: Any) -> None:
    """ccxt kucoin instance patches."""
    if getattr(exchange, 'id', '') != 'kucoinfutures' or getattr(exchange, _APPLIED, False):
        return
    setattr(exchange, _APPLIED, True)  # a repeated apply doesn't wrap methods twice
    _fix_funding_rate(exchange)
    _fix_trade_time(exchange)
    _fix_ns_timestamps(exchange)
    _fix_ws_subjects(exchange)
    _fix_ws_candles(exchange)
    _fix_ws_token(exchange)
