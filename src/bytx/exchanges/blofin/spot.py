"""BloFin Spot API (since 2026-08-27) on the ``blofin`` ccxt instance — ccxt 4.4.65 does not know BloFin spot.

Reference — https://docs.blofin.com/spot.html (``S#anchor``), demo responses —
``tests/ground_truth/blofin/demo_spot_*.json``. Installed on top of the futures fixes (:mod:`.ccxt_fixes`) on a
``market='spot'`` client:

- **Requests** (S#general-information-on-endpoints): the same hosts and signature; market data, orders and trades —
  ``/api/v1/spot/…``, all with ``instType=SPOT`` (in batches — in every element). ccxt methods (``fetch_ticker``,
  ``create_order``, ``fetch_open_orders`` …) build a futures request — the ``request`` wrapper moves the path to spot
  and drops fields spot does not have (marginMode, positionSide, reduceOnly). Other futures endpoints (leverage,
  positions, TP/SL, amend) — NotSupported before the network. Shared — ``asset/*`` (balance
  ``asset/balances?accountType=spot``, S#get-balance), ``user/*``.
- **Markets** (S#get-instruments): ``minSize`` / ``lotSize`` are in the base currency; spot has no settlement currency.
- **Market orders** (S#place-order, demo 2026-11-09): ``targetCurrency`` has no default — without it 152001; the SDK
  sends it explicitly: a buy for an amount — ``quote_currency`` (size is the quote amount), otherwise
  ``base_currency``. In market-buy-for-amount responses ``size`` = 0, filled — ``filledSize`` (base) and
  ``filledAmount`` (quote).
- **Fee**: the order and trade ``feeCurrency`` is ``base_currency`` / ``quote_currency`` (demo: buy — in base, sell —
  in quote), not a currency code; ccxt sets USDT / settle.
- **WS** (S#general-info): ``/ws/spot/{public,private}``; ``instType: SPOT`` in the subscription; balance — the
  ``spot-account`` channel (S#ws-spot-account-channel). The book ``seqId`` check is shared with futures
  (:mod:`.ccxt_fixes`).
- **Not on spot**: amend, TP/SL in the order and standalone, trailing, leverage, positions — NotSupported before the
  network.
"""
from __future__ import annotations

from typing import Any

from ccxt.base.errors import NotSupported as CcxtNotSupported

INST_TYPE = 'SPOT'
# ccxt / SDK futures request path → spot endpoint (S#…)
PATHS = {p: f'spot/{p}' for p in (
    'market/instruments', 'market/tickers', 'market/books', 'market/trades', 'market/candles',
    'trade/order', 'trade/batch-orders', 'trade/cancel-order', 'trade/cancel-batch-orders', 'trade/order-detail',
    'trade/orders-pending', 'trade/orders-history', 'trade/fills-history')}
_COMMON = ('asset/', 'user/')
_FUTURES_ONLY = ('marginMode', 'positionSide', 'reduceOnly')
_TP_SL = ('tpTriggerPrice', 'slTriggerPrice', 'tpOrderPrice', 'slOrderPrice')


def _fix_request(exchange: Any) -> None:
    original = exchange.request

    async def request(path: str, api: Any = 'public', method: str = 'GET', params: Any = None, *args: Any,
                      **kwargs: Any) -> Any:
        if path.startswith('spot/') or path.startswith(_COMMON):
            return await original(path, api, method, {} if params is None else params, *args, **kwargs)
        if path not in PATHS:
            raise CcxtNotSupported(f'{exchange.id} spot: {path} — futures only (the Spot API has no such endpoint)')
        entries = params if isinstance(params, list) else [params or {}]
        out = [{**{k: v for k, v in e.items() if k not in _FUTURES_ONLY}, 'instType': INST_TYPE} for e in entries]
        return await original(PATHS[path], api, method, out if isinstance(params, list) else out[0], *args, **kwargs)

    exchange.request = request


def _fix_markets(exchange: Any) -> None:
    original_parse = exchange.parse_market

    def parse_market(market: dict) -> dict:
        result = original_parse(market)
        if result.get('spot'):
            # ccxt: settle = quote, margin — from maxLeverage; spot has neither a settlement currency nor margin
            result.update(settle=None, settleId=None, margin=False)
            result['limits'] = {**result['limits'], 'leverage': {'min': None, 'max': None}}
        return result

    async def fetch_markets(params: Any = None) -> list:
        response = await exchange.request('market/instruments', 'public', 'GET', params or {})
        return [m for m in (parse_market(r) for r in (response.get('data') or []) if isinstance(r, dict))
                if m.get('spot')]

    exchange.parse_market = parse_market
    exchange.fetch_markets = fetch_markets


def _fee_currency(exchange: Any, row: dict, market: Any) -> Any:
    value = row.get('feeCurrency')
    if value == 'base_currency':
        return market.get('base')
    if value == 'quote_currency':
        return market.get('quote')
    return exchange.safe_currency_code(value) if value else market.get('quote')


def _fix_orders(exchange: Any) -> None:
    from ccxt.base.precise import Precise

    original_request = exchange.create_order_request
    original_order = exchange.parse_order
    original_trade = exchange.parse_trade

    def create_order_request(symbol: str, type: str, side: str, amount: float, price: Any = None,
                             params: Any = None) -> dict:
        params = dict(params or {})
        if params.get('stopLoss') or params.get('takeProfit') or any(params.get(k) for k in _TP_SL):
            raise CcxtNotSupported(f'{exchange.id} spot: the Spot API has no TP/SL in the order (S#place-order)')
        cost = params.pop('cost', None)
        request = original_request(symbol, type, side, amount, price, params)
        for key in _FUTURES_ONLY:
            request.pop(key, None)
        if request.get('orderType') == 'market':
            if cost is not None:
                request['size'] = str(cost)                   # quote amount (Exchange._spot_quote_buy — rounded to step)
                request['targetCurrency'] = 'quote_currency'
            request.setdefault('targetCurrency', 'base_currency')   # no default — 152001 (demo)
        else:
            request.pop('targetCurrency', None)
        return request

    def parse_order(order: Any, market: Any = None) -> dict:
        result = original_order(order, market)
        if not isinstance(order, dict):
            return result
        m = exchange.safe_market(order.get('instId'), market)
        filled = exchange.safe_string(order, 'filledSize')
        average = exchange.safe_string(order, 'averagePrice')
        amount_quote = exchange.safe_string_2(order, 'filledAmount', 'filled_amount')
        cost = amount_quote if Precise.string_gt(amount_quote or '0', '0') else (
            Precise.string_mul(filled, average) if filled and average else None)
        result['cost'] = exchange.parse_number(cost) if cost is not None else result.get('cost')
        if order.get('targetCurrency') == 'quote_currency':
            # size is the quote amount (0 in order-detail after the fill): the order amount is the filled base amount
            result['amount'] = result.get('filled') if result.get('status') != 'open' else None
            result['remaining'] = 0.0 if result.get('status') != 'open' else None
        if result.get('fee'):
            result['fee'] = {**result['fee'], 'currency': _fee_currency(exchange, order, m)}
            result['fees'] = [result['fee']]
        return result

    def parse_trade(trade: dict, market: Any = None) -> dict:
        result = original_trade(trade, market)
        if isinstance(trade, dict) and result.get('fee'):
            m = exchange.safe_market(trade.get('instId'), market)
            result['fee'] = {**result['fee'], 'currency': _fee_currency(exchange, trade, m)}
            result['fees'] = [result['fee']]
        return result

    async def edit_order(*args: Any, **kwargs: Any) -> Any:
        raise CcxtNotSupported(f'{exchange.id} spot: the Spot API has no amend')

    async def fetch_balance(params: Any = None) -> dict:
        await exchange.load_markets()
        response = await exchange.request('asset/balances', 'private', 'GET', {'accountType': 'spot', **(params or {})})
        return exchange.parse_funding_balance(response)

    exchange.create_order_request = create_order_request
    exchange.parse_order = parse_order
    exchange.parse_trade = parse_trade
    exchange.edit_order = edit_order
    exchange.fetch_balance = fetch_balance
    exchange.has = {**exchange.has, 'editOrder': False, 'fetchPositions': False, 'setLeverage': False,
                    'setPositionMode': False, 'setMarginMode': False}


def _fix_ws(exchange: Any) -> None:
    if not hasattr(exchange, 'watch_multiple_wrapper'):
        return
    ws = exchange.urls['api']['ws']
    spot = {k: v.replace('/ws/', '/ws/spot/', 1) for k, v in ws['swap'].items()}
    # ccxt takes the WS (and login) URL only from ws['swap'] — spot goes there too
    ws['swap'] = ws['spot'] = spot
    original_wrapper = exchange.watch_multiple_wrapper
    original_watch = exchange.watch
    original_multiple = exchange.watch_multiple
    original_message = exchange.handle_message

    def _with_inst_type(message: Any) -> Any:
        if isinstance(message, dict) and message.get('op') in ('subscribe', 'unsubscribe'):
            args = [dict(a, instType=INST_TYPE) if isinstance(a, dict) else a for a in message.get('args') or []]
            return {**message, 'args': args}
        return message

    async def watch(url: Any, message_hash: Any, message: Any = None, *args: Any, **kwargs: Any) -> Any:
        return await original_watch(url, message_hash, _with_inst_type(message), *args, **kwargs)

    async def watch_multiple(url: Any, hashes: Any, message: Any = None, *args: Any, **kwargs: Any) -> Any:
        return await original_multiple(url, hashes, _with_inst_type(message), *args, **kwargs)

    async def watch_multiple_wrapper(is_public: bool, channel: str, caller: str, symbols: Any = None,
                                     params: Any = None) -> Any:
        # ccxt only allows 'swap'; the ws['swap'] URL here is the spot one
        return await original_wrapper(is_public, channel, caller, symbols, {**(params or {}), 'type': 'swap'})

    async def watch_balance(params: Any = None) -> Any:
        await exchange.load_markets()
        await exchange.authenticate()
        request = {'op': 'subscribe', 'args': [{'channel': 'spot-account'}]}
        return await exchange.watch(spot['private'], 'swap:balance', request, 'spot-account')

    def handle_message(client: Any, message: Any) -> Any:
        arg = message.get('arg') if isinstance(message, dict) else None
        if isinstance(arg, dict) and arg.get('channel') == 'spot-account' and 'data' in message:
            return exchange.handle_balance(client, message)
        return original_message(client, message)

    exchange.watch = watch
    exchange.watch_multiple = watch_multiple
    exchange.watch_multiple_wrapper = watch_multiple_wrapper
    exchange.watch_balance = watch_balance
    exchange.handle_message = handle_message
    exchange.has = {**exchange.has, 'watchPositions': False}


def apply(exchange: Any) -> None:
    _fix_request(exchange)
    _fix_markets(exchange)
    _fix_orders(exchange)
    _fix_ws(exchange)

