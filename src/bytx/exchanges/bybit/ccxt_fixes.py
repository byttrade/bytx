"""ccxt 4.4.65 fixes for bybit (see tests/ground_truth).

Reference — Bybit V5 docs (https://bybit-exchange.github.io/docs/v5):

- market/instrument: ``lotSizeFilter.minNotionalValue`` (linear/inverse) — minimum
  order notional; ccxt sets ``limits.cost.min = None``;
- market/orderbook, websocket/public/orderbook: ``u`` — Update ID; ccxt doesn't fill
  the book ``nonce`` in either REST or WS;
- order/open-order: for ``linear``/``spot`` ``cumExecFee`` is Deprecated, the fee is
  in ``cumFeeDetail`` ({coin: amount}); ccxt reads only ``cumExecFee``;
- position/position: realized PnL of the current position is ``curRealisedPnl``; ccxt takes
  ``closedPnl``, which position/list and WS position lack (it exists only in closed-pnl);
- websocket/private/position: in hedge mode ``positionIdx`` 1 is the Buy side, 2 — Sell; an empty side has
  ``side`` = ``""``. ccxt treats an empty ``side`` as closing BOTH sides and zeroes in the cache the long that
  just arrived in the same push (demo 2026-10-30: an open 0.01 was always 0 in the stream — the Bybit position
  stream «didn't respond», 4 answers out of 378 reads).
"""
from __future__ import annotations

from typing import Any

_PATCHED_ATTR = '_bytx_bybit_fixes'


def apply(exchange: Any) -> None:
    """ccxt bybit instance patches (repeated call is a no-op)."""
    if getattr(exchange, _PATCHED_ATTR, False):
        return
    _patch_future_markets(exchange)
    _patch_order_book_nonce(exchange)
    _patch_order_fee(exchange)
    _patch_position_realized_pnl(exchange)
    _patch_ws_order_book_levels(exchange)
    _patch_ticker_time(exchange)
    _patch_ws_hedge_position_side(exchange)
    setattr(exchange, _PATCHED_ATTR, True)


def _patch_future_markets(ex: Any) -> None:
    """limits.cost.min ← lotSizeFilter.minNotionalValue (market/instrument, linear/inverse)."""
    orig = ex.fetch_future_markets

    async def fetch_future_markets(params):
        markets = await orig(params)
        for m in markets:
            lot = (m.get('info') or {}).get('lotSizeFilter') or {}
            min_notional = ex.safe_number(lot, 'minNotionalValue')
            if min_notional is None:
                continue
            cost = m.setdefault('limits', {}).setdefault('cost', {})
            if cost.get('min') is None:
                cost['min'] = min_notional
        return markets

    ex.fetch_future_markets = fetch_future_markets


def _patch_order_book_nonce(ex: Any) -> None:
    """Book nonce ← ``u`` (Update ID) — REST market/orderbook and WS orderbook."""
    orig_parse = ex.parse_order_book

    def parse_order_book(orderbook, *args, **kwargs):
        result = orig_parse(orderbook, *args, **kwargs)
        if result.get('nonce') is None and isinstance(orderbook, dict):
            result['nonce'] = ex.safe_integer(orderbook, 'u')
        return result

    ex.parse_order_book = parse_order_book

    if not hasattr(ex, 'handle_order_book'):  # REST instance without ccxt.pro
        return
    orig_handle = ex.handle_order_book

    def handle_order_book(client, message):
        # snapshot: nonce arrives via parse_order_book → reset(snapshot);
        # delta: ccxt doesn't touch nonce — set it before resolve inside the handler
        data = message.get('data')
        symbol = None
        if message.get('type') == 'delta' and isinstance(data, dict):
            market_type = 'spot' if 'spot' in (client.url or '') else 'contract'
            symbol = ex.safe_market(data.get('s'), None, None, market_type)['symbol']
            book = ex.orderbooks.get(symbol)
            if book is not None:
                book['nonce'] = ex.safe_integer(data, 'u')
        orig_handle(client, message)
        if symbol is not None and symbol in ex.orderbooks:
            ex.orderbooks[symbol]['nonce'] = ex.safe_integer(data, 'u')

    ex.handle_order_book = handle_order_book


def _patch_order_fee(ex: Any) -> None:
    """linear/spot order fee ← cumFeeDetail (order/open-order: cumExecFee Deprecated)."""
    orig = ex.parse_order

    def parse_order(order, market=None):
        result = orig(order, market)
        detail = order.get('cumFeeDetail') if isinstance(order, dict) else None
        if not isinstance(detail, dict) or not detail:
            return result
        category = order.get('category')
        if category is None:
            m = ex.safe_market(order.get('symbol'), market)
            category = 'spot' if m.get('spot') else 'linear' if m.get('linear') else None
        if category not in ('linear', 'spot'):
            return result  # inverse/option: cumExecFee is the current field
        fees = [{'cost': ex.parse_number(cost), 'currency': ex.safe_currency_code(coin)}
                for coin, cost in detail.items() if cost not in (None, '')]
        if not fees:
            return result
        result['fees'] = fees
        result['fee'] = dict(fees[0]) if len(fees) == 1 else None
        return result

    ex.parse_order = parse_order


def _patch_position_realized_pnl(ex: Any) -> None:
    """Position realizedPnl ← curRealisedPnl (position/list, WS position)."""
    orig = ex.parse_position

    def parse_position(position, market=None):
        result = orig(position, market)
        # closed-pnl (fetch_positions_history) — closedPnl is documented there, left alone
        if isinstance(position, dict) and 'closedPnl' not in position and 'curRealisedPnl' in position:
            result['realizedPnl'] = ex.safe_number(position, 'curRealisedPnl')
        return result

    ex.parse_position = parse_position


# ─── WS order book: depth levels per current docs ───────────────────────────

# Docs websocket/public/orderbook: linear/inverse and spot — 1, 50, 200, 1000; option — 25, 100.
# ccxt 4.4.65 subscribes contracts to 500 by default (the level was removed — the exchange answers
# «handler not found») and doesn't allow 1000.
WS_BOOK_LEVELS = (1, 50, 200, 1000)
WS_BOOK_LEVELS_OPTION = (25, 100)


def ws_book_limit(depth: Any, option: bool = False) -> int:
    """Subscription level for the requested depth: the smallest ≥ depth (default 50 / 25)."""
    levels = WS_BOOK_LEVELS_OPTION if option else WS_BOOK_LEVELS
    if depth is None:
        return levels[0] if option else 50
    for level in levels:
        if level >= int(depth):
            return level
    return levels[-1]


def _patch_ws_order_book_levels(ex: Any) -> None:
    if not hasattr(ex, 'watch_order_book_for_symbols'):
        return  # REST instance

    async def watch_order_book_for_symbols(symbols, limit=None, params={}):  # noqa: B006 — ccxt signature
        await ex.load_markets()
        if not symbols:
            raise ex.ArgumentsRequired(ex.id + ' watchOrderBookForSymbols() requires a non-empty array of symbols') \
                if hasattr(ex, 'ArgumentsRequired') else ValueError('at least one symbol is required')
        symbols = ex.market_symbols(symbols)
        url = await ex.get_url_by_market_type(symbols[0], False, 'watchOrderBook', params)
        params = ex.clean_params(params)
        market = ex.market(symbols[0])
        option = bool(market.get('option'))
        levels = WS_BOOK_LEVELS_OPTION if option else WS_BOOK_LEVELS
        if limit is None or int(limit) not in levels:
            limit = ws_book_limit(limit, option)
        topics = [f'orderbook.{limit}.{ex.market_id(s)}' for s in symbols]
        hashes = [f'orderbook:{s}' for s in symbols]
        orderbook = await ex.watch_topics(url, hashes, topics, params)
        return orderbook.limit()

    ex.watch_order_book_for_symbols = watch_order_book_for_symbols


def _patch_ticker_time(ex: Any) -> None:
    """Ticker time: market/tickers returns it at the response root («time»), not in the list
    items — ccxt reads ticker['time'] and leaves timestamp=None (live check)."""
    original = getattr(ex, 'publicGetV5MarketTickers', None)
    if original is None:
        return

    async def public_get_v5_market_tickers(params={}, *args, **kwargs):  # noqa: B006 — ccxt signature
        response = await original(params, *args, **kwargs)
        stamp = response.get('time') if isinstance(response, dict) else None
        items = ((response or {}).get('result') or {}).get('list') if isinstance(response, dict) else None
        if stamp is not None and isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and item.get('time') is None:
                    item['time'] = stamp
        return response

    ex.publicGetV5MarketTickers = public_get_v5_market_tickers
    ex.public_get_v5_market_tickers = public_get_v5_market_tickers


_HEDGE_IDX_SIDE = {1: 'Buy', 2: 'Sell'}


def _patch_ws_hedge_position_side(ex: Any) -> None:
    """WS position: empty ``side`` of a hedge side ← side by ``positionIdx`` (1 Buy, 2 Sell)."""
    if not hasattr(ex, 'handle_positions'):  # REST instance without ccxt.pro
        return
    orig_handle = ex.handle_positions

    def handle_positions(client, message):
        for row in (message.get('data') or []) if isinstance(message, dict) else []:
            if isinstance(row, dict) and not row.get('side'):
                side = _HEDGE_IDX_SIDE.get(ex.safe_integer(row, 'positionIdx'))
                if side is not None:
                    row['side'] = side
        orig_handle(client, message)

    ex.handle_positions = handle_positions
