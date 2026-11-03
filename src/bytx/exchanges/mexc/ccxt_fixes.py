"""ccxt 4.4.65 fixes for MEXC (Contract API futures, spot v3), based on official docs.

Reference — https://www.mexc.com/api-docs/futures/ (new docs: «2026-03-31 Futures Trading API maintenance ended;
trading access reopened») and https://mexcdevelop.github.io/apidocs/contract_v1_en/ (previous), fixtures
``tests/ground_truth/mexc/``. ccxt 4.4.65 follows the previous docs, where futures orders were «Under maintenance»
since 2022-07-25. Patches on the instance only.

- **Place Order** (``POST /api/v1/private/order/create``): ccxt sends to ``order/submit`` (absent from the new docs),
  market — ``type 6`` (the new docs have types 1–5, market = 5), response ``data`` is an object ``{orderId, ts}`` (ccxt
  reads a string — the order id breaks). ``positionMode`` — «Default Hedge Mode» — ccxt doesn't send it in one-way;
  in hedge, for a close (reduceOnly) ccxt flips the side and sends ``side`` 1/3 (open!) instead of 4/2.
  ``leverage`` — «must be provided when opening a position» (ccxt requires it only for isolated).
  TP/SL on entry — ``stopLossPrice`` / ``takeProfitPrice`` (ccxt doesn't know the ``stopLoss``/``takeProfit`` dicts
  and would send them as is).
- ccxt ``options['unavailableContracts']`` — BTC/ETH/LTC USDT-M «does not support yet» (legacy of the 2022
  pause): the docs have no such restriction — ccxt doesn't send orders for these contracts; the SDK sends them itself.
- Contract **apiAllowed** — «whether support api»: an order for such a contract is NotSupported before sending.
- **Futures WS push.deal** — ``"data": [ … ]`` (array of trades, docs WebSocket → Deal); ccxt expects an object and
  parses the array as one trade without a price; trade id — field ``i`` («Transaction ID»).
- **Spot WS** — «The current websocket push uses the protobuf format» (channels ``….pb@…``); ccxt 4.4.65
  subscribes to the old JSON channels, no data — MEXC spot streams: NotSupported (until protobuf parsing exists).
- **Cancel Orders** (``order/cancel``): a per-order failure — ``errorCode`` 2040 «order not exist» / 2041 «order
  state cannot be cancelled» inside ``data`` — exception with the code (ccxt gives text without the code).
- **Place Order**, update log 2026-10-23: ``sideType`` — «string | true | buy (open long / close short), sell
  (open short / close long)»; neither ccxt nor the previous SDK sent it.
- **Contract order** (order/get, order/external, history_orders, WS push.personal.order): ``side`` «1 open long,
  2 close short, 3 open short, 4 close long» (ccxt: 2 → sell, 3/4 — raw), ``state`` «1 pending, 2 unfilled,
  3 filled, 4 canceled, 5 invalid» (ccxt: 1 and 5 raw), ``externalOid`` (ccxt reads clientOrderId — empty),
  ``orderType`` «1 limit, 2 Post Only, 3 IOC, 4 FOK, 5 market, 6 market-to-limit» (ccxt reads type — empty).
- **Contract trade** (order_deals, deal_details, WS push.personal.order.deal): ccxt parses it as a contract
  only if ``positionMode`` is in the record (the deal_details example lacks it — a trade without amount or side), the side
  from ``side`` 1–4 is wrong, taker — ``isTaker`` (ccxt reads ``taker``), own id — ``externalOid``.
- **Position** (open_positions): margin mode — ``openType`` «1 isolated 2 cross» (ccxt reads the nonexistent
  ``margin_mode`` — always cross).
- **Get Order by External ID** — GET ``order/external/{symbol}/{external_oid}`` (ccxt sends order/get/None);
  **Cancel by External Order ID** — POST ``order/cancel_with_external`` ``[{"symbol", "externalOid"}]`` (ccxt
  sends order/cancel ``[null]``). Empty ``data`` from an order lookup — OrderNotFound.
- **Error Code**: 510 «Requests are too frequent» — RateLimitExceeded, 2040 «order not exist» — OrderNotFound
  (ccxt — bare ExchangeError).
- **WS login**: «Failure: return error info, channel = rs.error» — ccxt doesn't parse rs.error, the private
  watch hangs; an error before ``rs.login`` rejects private waiters and drops the login subscription (login retry).
"""
from __future__ import annotations

from typing import Any

import ccxt

ORDER_PATH = 'order/create'
TYPES = {'limit': 1, 'post_only': 2, 'ioc': 3, 'fok': 4, 'market': 5}
OPEN_TYPES = {'isolated': 1, 'cross': 2}
TREND = {'last': 1, 'mark': 2, 'index': 3}     # lossTrend / profitTrend: 1 latest, 2 fair, 3 index
# sideType «buy: open long / close short; sell: open short / close long»
SIDE_TYPE = {1: 'buy', 2: 'buy', 3: 'sell', 4: 'sell'}
# state «1 pending, 2 unfilled, 3 filled, 4 canceled, 5 invalid»
ORDER_STATES = {'1': 'open', '2': 'open', '3': 'closed', '4': 'canceled', '5': 'rejected'}
# orderType (WS Order): «1 limit, 2 Post Only, 3 IOC, 4 FOK, 5 market, 6 market-to-limit» → (type, timeInForce)
ORDER_TYPES = {'1': ('limit', 'GTC'), '2': ('limit', 'PO'), '3': ('limit', 'IOC'), '4': ('limit', 'FOK'),
               '5': ('market', None), '6': ('market', None)}


def order_side(side: str, *, reduce_only: bool, hedged: bool) -> int:
    """MEXC side: 1 open long, 2 close short, 3 open short, 4 close long. Close (reduce) — the order side
    is opposite to the position: sell closes long (4), buy closes short (2); same in hedge and one-way."""
    if reduce_only:
        return 4 if side == 'sell' else 2
    return 1 if side == 'buy' else 3


def _trigger(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get('triggerPrice')
    return value


def build_order_request(exchange: Any, market: dict, type: str, side: str, amount: float, price: Any,
                        margin_mode: Any, params: dict) -> dict:
    p = dict(params or {})
    info = market.get('info') or {}
    if info.get('apiAllowed') is False:
        raise ccxt.NotSupported(f"mexc: contract {market['id']} has no API trading (apiAllowed=false)")
    mode = margin_mode or p.pop('marginMode', None)
    open_type = OPEN_TYPES.get(str(mode).lower()) if mode else p.pop('openType', None)
    if open_type is None:
        raise ccxt.ArgumentsRequired('mexc: openType (marginMode cross|isolated) is required')
    hedged = bool(p.pop('hedged', False))
    reduce_only = bool(p.pop('reduceOnly', False))
    tif = str(p.pop('timeInForce', '') or '').upper()
    post_only = bool(p.pop('postOnly', False)) or tif == 'PO'
    kind = 'market' if type == 'market' else 'post_only' if post_only else {'IOC': 'ioc', 'FOK': 'fok'}.get(tif, 'limit')
    side_code = order_side(side, reduce_only=reduce_only, hedged=hedged)
    request: dict = {'symbol': market['id'], 'vol': float(exchange.amount_to_precision(market['symbol'], amount)),
                     'side': side_code, 'sideType': SIDE_TYPE[side_code], 'type': TYPES[kind],
                     'openType': open_type, 'positionMode': 1 if hedged else 2}
    if kind != 'market':
        request['price'] = float(exchange.price_to_precision(market['symbol'], price))
    leverage = p.pop('leverage', None)
    if leverage is not None:
        request['leverage'] = int(leverage)
    elif not reduce_only:
        raise ccxt.ArgumentsRequired('mexc: leverage — «must be provided when opening a position»')
    if reduce_only and not hedged:
        request['reduceOnly'] = True      # «only applicable in one-way mode»
    cid = p.pop('clientOrderId', None) or p.pop('externalOid', None)
    if cid is not None:
        request['externalOid'] = str(cid)
    for key, field, trend in (('stopLoss', 'stopLossPrice', 'lossTrend'), ('takeProfit', 'takeProfitPrice', 'profitTrend')):
        value = _trigger(p.pop(key, None))
        value = value if value is not None else p.pop(field, None)
        if value is not None:
            request[field] = float(exchange.price_to_precision(market['symbol'], value))
    request.update(p)
    return request


def _fix_create_swap_order(exchange: Any) -> None:
    original = exchange.create_swap_order

    async def create_swap_order(market, type, side, amount, price=None, marginMode=None, params={}):
        if exchange.safe_value_2(params, 'triggerPrice', 'stopPrice') is not None:
            return await original(market, type, side, amount, price, marginMode, params)  # plan order — stock ccxt
        await exchange.load_markets()
        request = build_order_request(exchange, market, type, side, amount, price, marginMode, params)
        response = await exchange.request(ORDER_PATH, ['contract', 'private'], 'POST', request)
        data = response.get('data') if isinstance(response, dict) else None
        oid = data.get('orderId') if isinstance(data, dict) else data
        return exchange.safe_order({'id': None if oid is None else str(oid), 'clientOrderId': request.get('externalOid'),
                                    'symbol': market['symbol'], 'type': type, 'side': side, 'amount': amount,
                                    'price': price, 'reduceOnly': request.get('reduceOnly'), 'info': response}, market)

    exchange.create_swap_order = create_swap_order


# Cancel Orders: errorCode in data[] — «2040 order not exist», «2041 order state cannot be cancelled»
CANCEL_NOT_OPEN = frozenset({2040, 2041})


def _fix_cancel(exchange: Any) -> None:
    original = exchange.contractPrivatePostOrderCancel

    async def cancel(params=None, *args, **kwargs):
        response = await original(params, *args, **kwargs)
        data = response.get('data') if isinstance(response, dict) else None
        for row in data if isinstance(data, list) else []:
            code = exchange.safe_integer(row, 'errorCode')
            if code:
                text = f'mexc {{"code":{code},"msg":"{row.get("errorMsg") or ""}","orderId":"{row.get("orderId")}"}}'
                raise (ccxt.OrderNotFound if code in CANCEL_NOT_OPEN else ccxt.InvalidOrder)(text)
        return response

    exchange.contractPrivatePostOrderCancel = cancel


def _fix_ws_deals(exchange: Any) -> None:
    original = getattr(exchange, 'handle_trades', None)
    if original is None:
        return

    def handle_trades(client, message):
        data = message.get('data') if isinstance(message, dict) else None
        if isinstance(data, list):           # push.deal: array of trades — one at a time (ccxt expects an object)
            for row in data:
                original(client, dict(message, data=row))
                market = exchange.safe_market(exchange.safe_string(message, 'symbol'))
                stored = (exchange.trades or {}).get(market['symbol'])
                if stored and isinstance(row, dict) and row.get('i') is not None:
                    stored[-1]['id'] = str(row['i'])   # «i — Transaction ID» (ccxt doesn't read the id)
            return None
        return original(client, message)

    exchange.handle_trades = handle_trades


def _contract(exchange: Any, row: Any, market: Any) -> bool:
    if not isinstance(row, dict) or 'vol' not in row or str(row.get('side')) not in ('1', '2', '3', '4'):
        return False
    m = exchange.safe_market(exchange.safe_string(row, 'symbol'), market)
    return bool(m.get('contract'))


def _client_id(row: dict) -> Any:
    value = row.get('externalOid')
    return value if isinstance(value, str) and value else None


def _fix_parse_order(exchange: Any) -> None:
    original = exchange.parse_order

    def parse_order(order, market=None):
        parsed = original(order, market)
        if not _contract(exchange, order, market):
            return parsed
        side = int(order['side'])
        kind = ORDER_TYPES.get(str(order.get('orderType')))
        status = ORDER_STATES.get(str(order.get('state')))
        parsed['side'] = 'buy' if side in (1, 2) else 'sell'
        parsed['reduceOnly'] = side in (2, 4)
        parsed['clientOrderId'] = _client_id(order)
        if status is not None:
            parsed['status'] = status
        if kind is not None:
            parsed['type'], parsed['timeInForce'] = kind
            parsed['postOnly'] = kind[1] == 'PO'
        amount, filled = parsed.get('amount'), parsed.get('filled')
        if amount is not None and filled is not None:
            parsed['remaining'] = max(float(amount) - float(filled), 0.0)
        return parsed

    exchange.parse_order = parse_order


def _fix_parse_trade(exchange: Any) -> None:
    original = exchange.parse_trade

    def parse_trade(trade, market=None):
        parsed = original(trade, market)
        if not _contract(exchange, trade, market):
            return parsed
        m = exchange.safe_market(exchange.safe_string(trade, 'symbol'), market)
        side = int(trade['side'])
        taker = trade.get('isTaker', trade.get('taker'))
        ts = exchange.safe_integer(trade, 'timestamp')
        parsed.update({
            'symbol': m['symbol'], 'side': 'buy' if side in (1, 2) else 'sell',
            'amount': exchange.safe_number(trade, 'vol'), 'price': exchange.safe_number(trade, 'price'),
            'order': exchange.safe_string(trade, 'orderId'), 'timestamp': ts, 'datetime': exchange.iso8601(ts),
            'takerOrMaker': None if taker is None else 'taker' if taker else 'maker',
            'fee': {'cost': exchange.safe_number(trade, 'fee'),
                    'currency': exchange.safe_currency_code(exchange.safe_string(trade, 'feeCurrency'))},
            'clientOrderId': _client_id(trade)})
        if parsed.get('amount') is not None and parsed.get('price') is not None:
            parsed['cost'] = parsed['amount'] * parsed['price'] * float(m.get('contractSize') or 1)
        if trade.get('id') is not None:
            parsed['id'] = str(trade['id'])
        return parsed

    exchange.parse_trade = parse_trade


def _fix_parse_position(exchange: Any) -> None:
    original = exchange.parse_position

    def parse_position(position, market=None):
        parsed = original(position, market)
        mode = {'1': 'isolated', '2': 'cross'}.get(str((position or {}).get('openType')))
        if mode is not None:
            parsed['marginMode'] = mode     # «openType | 1 isolated 2 cross» (ccxt reads margin_mode)
        return parsed

    exchange.parse_position = parse_position


def _swap(exchange: Any, symbol: Any) -> bool:
    return symbol is not None and bool(exchange.market(symbol).get('swap'))


def _fix_fetch_order(exchange: Any) -> None:
    original = exchange.fetch_order

    async def fetch_order(id, symbol=None, params={}):
        p = dict(params or {})
        if symbol is not None:
            await exchange.load_markets()
        if not _swap(exchange, symbol):
            return await original(id, symbol, params)
        cid = p.pop('clientOrderId', None) or p.pop('externalOid', None)
        market = exchange.market(symbol)
        if cid is not None:
            # Get Order by External ID: GET /api/v1/private/order/external/{symbol}/{external_oid}
            response = await exchange.contractPrivateGetOrderExternalSymbolExternalOid(
                exchange.extend({'symbol': market['id'], 'external_oid': str(cid)}, p))
        else:
            response = await exchange.contractPrivateGetOrderGetOrderId(exchange.extend({'order_id': id}, p))
        data = response.get('data') if isinstance(response, dict) else None
        if not isinstance(data, dict) or not data:
            raise ccxt.OrderNotFound(f'mexc fetchOrder: order {cid or id} not found ({response})')
        return exchange.parse_order(data, market)

    exchange.fetch_order = fetch_order


def _fix_cancel_by_client_id(exchange: Any) -> None:
    original = exchange.cancel_order

    async def cancel_order(id, symbol=None, params={}):
        p = dict(params or {})
        cid = p.pop('clientOrderId', None) or p.pop('externalOid', None)
        if symbol is not None:
            await exchange.load_markets()
        if cid is None or id is not None or not _swap(exchange, symbol):
            return await original(id, symbol, params)
        market = exchange.market(symbol)
        # Cancel by External Order ID: [{"symbol": …, "externalOid": …}]; errorCode — in data[]
        response = await exchange.contractPrivatePostOrderCancelWithExternal(
            [{'symbol': market['id'], 'externalOid': str(cid)}])
        rows = response.get('data') if isinstance(response, dict) else None
        row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
        code = exchange.safe_integer(row, 'errorCode')
        if code:
            text = f'mexc {{"code":{code},"msg":"{row.get("errorMsg") or ""}","externalOid":"{cid}"}}'
            raise (ccxt.OrderNotFound if code in CANCEL_NOT_OPEN else ccxt.InvalidOrder)(text)
        return exchange.safe_order({'id': exchange.safe_string(row, 'orderId'), 'clientOrderId': str(cid),
                                    'symbol': market['symbol'], 'status': 'canceled', 'info': response}, market)

    exchange.cancel_order = cancel_order


# Error Code: 510 «Requests are too frequent, please try again later», 2040 «order not exist»;
# 604 «Sorry, this feature is under maintenance», 801 «System under maintenance» — maintenance:
# temporary unavailability with retry (ExchangeUnavailable), not «trading closed» (TRADING_UNAVAILABLE)
EXACT_ERRORS = {'510': ccxt.RateLimitExceeded, '2040': ccxt.OrderNotFound,
                '604': ccxt.ExchangeNotAvailable, '801': ccxt.ExchangeNotAvailable}


def _fix_exceptions(exchange: Any) -> None:
    exceptions = dict(exchange.exceptions or {})
    exceptions['exact'] = {**(exceptions.get('exact') or {}), **EXACT_ERRORS}
    exchange.exceptions = exceptions


_PRIVATE_HASHES = ('orders', 'myTrades', 'balance')

# Futures API update log, 2026-01-19: «futures API base domain changed to https://api.mexc.com».
# WS stays at wss://contract.mexc.com/edge (update log 2024-01-31 / 2026-10-23). Paths are the same:
# public responses from the old and new domain matched on 2026-10-27 (ping, detail, ticker, depth, kline,
# funding_rate, index/fair_price, risk_reverse, funding_rate/history).
CONTRACT_REST = {'public': 'https://api.mexc.com/api/v1/contract',
                 'private': 'https://api.mexc.com/api/v1/private'}


def _fix_contract_domain(exchange: Any) -> None:
    api = exchange.urls.get('api') if isinstance(exchange.urls, dict) else None
    if isinstance(api, dict) and isinstance(api.get('contract'), dict):
        api['contract'] = {**api['contract'], **CONTRACT_REST}


def _fix_ws_login(exchange: Any) -> None:
    original = getattr(exchange, 'handle_message', None)
    if original is None:
        return

    def handle_message(client, message):
        channel = message.get('channel') if isinstance(message, dict) else None
        if channel == 'rs.login':
            client._bytx_mexc_login = True
        elif (channel == 'rs.error' and 'login' in (getattr(client, 'subscriptions', None) or {})
              and not getattr(client, '_bytx_mexc_login', False)):
            # «Failure: return error info, channel = rs.error» — login failed: private waiters get the error,
            # the login subscription is dropped (the next watch sends login again)
            error = ccxt.AuthenticationError(f'mexc login: {message}')
            client.subscriptions.pop('login', None)
            for key in list(getattr(client, 'futures', {}) or {}):
                if str(key).split(':')[0] in _PRIVATE_HASHES:
                    client.reject(error, key)
            return None
        return original(client, message)

    exchange.handle_message = handle_message


SPOT_WS = ('watch_ticker', 'watch_tickers', 'watch_bids_asks', 'watch_order_book', 'watch_trades', 'watch_ohlcv',
           'watch_orders', 'watch_my_trades', 'watch_balance')
SPOT_WS_DOC = 'https://mexcdevelop.github.io/apidocs/spot_v3_en/#protocol-buffers-integration'


def _fix_spot_ws(exchange: Any) -> None:
    if exchange.safe_string(exchange.options, 'defaultType') != 'spot':
        return

    def unsupported(name: str) -> Any:
        async def watch(*args: Any, **kwargs: Any) -> Any:
            raise ccxt.NotSupported(f'mexc {name}: MEXC spot WS is protobuf («The current websocket push uses the '
                                    f'protobuf format», {SPOT_WS_DOC}); ccxt 4.4.65 JSON channels send no data')
        return watch

    for name in SPOT_WS:
        if callable(getattr(exchange, name, None)):
            setattr(exchange, name, unsupported(name))


def apply(exchange: Any) -> None:
    _fix_contract_domain(exchange)
    _fix_create_swap_order(exchange)
    _fix_cancel(exchange)
    _fix_parse_order(exchange)
    _fix_parse_trade(exchange)
    _fix_parse_position(exchange)
    _fix_fetch_order(exchange)
    _fix_cancel_by_client_id(exchange)
    _fix_exceptions(exchange)
    _fix_ws_login(exchange)
    _fix_ws_deals(exchange)
    _fix_spot_ws(exchange)
