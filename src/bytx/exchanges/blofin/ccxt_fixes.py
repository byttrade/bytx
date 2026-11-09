"""ccxt 4.4.65 fixes for BloFin (Futures API v1), per the official documentation.

Reference — https://docs.blofin.com/index.html (``F#anchor``), fixtures ``tests/ground_truth/blofin/``, verified on
demo. Patches are per instance only.

- **Markets** (F#get-instruments): the settlement currency is ``settleCurrency`` (USDT-M ``BTC-USDT``, USDC-M
  ``BTC-USDC``, coin-M ``BTC-USD`` — settle BTC); ccxt takes settle from ``quoteCurrency`` (coin-M came out as
  ``BTC/USD:USD`` linear). The SDK trades USDT-M only: USDC-M and coin-M from the shared response are not loaded.
- **Place Order** (F#place-order): ``orderType`` — market | limit | post_only | fok | ioc; PO / IOC / FOK — only as the
  orderType value. ccxt writes post_only into ``request['type']`` (orderType stays limit — demo 2026-11-09: the order
  rested as a plain limit), and loses limit + IOC/FOK. ``reduceOnly`` — the string "true"/"false".
- **Batch** (F#place-multiple-orders): up to 20 (152006), ccxt ``features.createOrders.max`` — 10.
- **Errors** (F#errors): HTTP 200, the per-order code — ``data[i].code`` or ``data.code`` when ``data`` is an object
  (order-tpsl, amend-*, cancel-algo; demo 2026-11-09: cancel-algo — ``code 0`` + ``data.code 102068``); ccxt reads
  only the list. Batch: ``code 1`` — all failed, ``2`` — partially: the SDK parses the elements. Classes: 150004 /
  103003 — funds (ccxt: InvalidOrder / none), 102068 / 102067 / 1000 — «no such order», 102002 / 150003 — duplicate
  client id, 1524xx — key / signature / time, 429 — rate limit (ccxt httpExceptions: ExchangeNotAvailable).
- **Amend** (F#amend-order, ccxt has no editOrder): ``POST /trade/amend-order`` — ``newPrice`` / ``newSize`` of the
  same ``orderId`` (the id is kept), ``requestId`` — idempotency key.
- **Order by id** (F#get-order-detail, ccxt has no fetchOrder): ``GET /trade/order-detail`` — ``orderId`` |
  ``clientOrderId``; on demo it returns both filled and canceled orders.
- **Leverage** (F#set-leverage, F#get-multiple-leverage): the limit is the instrument's ``maxLeverage`` (ccxt — 1..125;
  above maxLeverage the exchange answers 152002 — the same code as for size, demo 2026-11-09), ``leverage`` — a string,
  ``positionSide`` — from params; reading — ``batch-leverage-info`` (ccxt — the deprecated ``leverage-info``).
- **Position mode** (F#set-position-mode): ``POST /account/set-position-mode`` — not in ccxt.
- **Balance** (F#get-futures-account-balance): ``productType=USDT-FUTURES`` explicitly.
- **Signature** (F#signature): ``ACCESS-NONCE`` must be unique (UUID; a repeat within 60 s — 152407, demo 2026-11-09);
  ccxt sets nonce = timestamp — parallel requests in the same ms are rejected. The timestamp is corrected by
  ``timeDifference``.
- **Server time**: there is no endpoint (``public/time`` — 401); ``fetch_time`` — the ``Date`` header of the
  ``market/tickers`` response (1 s precision) — for the SDK clock sync.
- **has**: ``fetchOrderTrades`` / ``fetchFundingHistory`` are declared without methods; ``fetchLedger`` — ``asset/bills``,
  transfer history, not the account journal (the journal — exchanges/blofin/portfolio).
- **WS books** (F#ws-order-book-channel, F#sequence-id): snapshot — ``prevSeqId`` 0, increment — ``prevSeqId`` = the
  previous ``seqId``; no checksum. ccxt does not check gaps — a missed increment: the book is reset, the subscription is
  rejected (resubscribe — a new snapshot).
- **WS trades**: there is no channel — ``watch_my_trades``, on growth of ``filledSize`` in WS orders, reads the order's
  trades from REST fills-history (real tradeId). ``watchBalance`` (account channel) exists in ccxt, the has flag does not.
- **WS orders-algo** (F#ws-algo-orders-channel): TP/SL and trigger orders have their own channel (not in ccxt);
  ``watch_orders`` with ``params={'trigger': True}`` subscribes to it, orders carry ids ``tpslId`` / ``algoId``.
"""
from __future__ import annotations

import uuid
from typing import Any

from ccxt.base.errors import (
    AuthenticationError,
    DuplicateOrderId,
    ExchangeError,
    InsufficientFunds,
    InvalidNonce,
    InvalidOrder,
    OrderNotFound,
    PermissionDenied,
    RateLimitExceeded,
)

from bytx.errors import NotSupported

#: settlement currency of the SDK markets
SETTLE = 'USDT'
BATCH_MAX = 20
# F#errors codes that ccxt 4.4.65 does not know or maps to the wrong class
_EXACT = {
    '150004': InsufficientFunds, '103003': InsufficientFunds,
    '102068': OrderNotFound, '102067': OrderNotFound, '1000': OrderNotFound,
    '102002': DuplicateOrderId, '150003': DuplicateOrderId,
    '152401': AuthenticationError, '152402': AuthenticationError, '152408': AuthenticationError,
    '152409': AuthenticationError, '152404': PermissionDenied, '152406': PermissionDenied,
    '152405': InvalidNonce, '152407': InvalidNonce, '152410': InvalidNonce,
    '429': RateLimitExceeded,
}
_TIF_TYPES = {'IOC': 'ioc', 'FOK': 'fok', 'PO': 'post_only'}


def _fix_markets(exchange: Any) -> None:
    original_parse = exchange.parse_market
    original_fetch = exchange.fetch_markets

    def parse_market(market: dict) -> dict:
        result = original_parse(market)
        settle_id = exchange.safe_string(market, 'settleCurrency')
        if not settle_id or not result.get('contract'):
            return result
        settle = exchange.safe_currency_code(settle_id)
        result.update(settle=settle, settleId=settle_id, symbol=f"{result['base']}/{result['quote']}:{settle}",
                      linear=result['quoteId'] == settle_id, inverse=result['baseId'] == settle_id)
        return result

    async def fetch_markets(params: Any = None) -> list:
        markets = await original_fetch(params or {})
        return [m for m in markets if m.get('settle') == SETTLE]

    exchange.parse_market = parse_market
    exchange.fetch_markets = fetch_markets


def _fix_order_request(exchange: Any) -> None:
    original = exchange.create_order_request

    def create_order_request(symbol: str, type: str, side: str, amount: float, price: Any = None,
                             params: Any = None) -> dict:
        params = dict(params or {})
        tif = str(params.pop('timeInForce', '') or '').upper()
        post_only = bool(params.pop('postOnly', False)) or type == 'post_only' or tif == 'PO'
        request = original(symbol, 'limit' if type in ('post_only', 'ioc', 'fok') else type, side, amount, price,
                           params)
        request.pop('type', None)                    # ccxt: postOnly → 'type' (no such field in the docs)
        if request.get('orderType') != 'market':
            request['orderType'] = 'post_only' if post_only else _TIF_TYPES.get(tif) or (
                type if type in ('ioc', 'fok') else 'limit')
        for leg in ('tp', 'sl'):
            if request.get(f'{leg}TriggerPrice') is not None and not request.get(f'{leg}TriggerPriceType'):
                # TP/SL on entry: explicit price type (exchange default — last), as for a separate SDK TP/SL
                from bytx.exchanges.blofin.tp_sl import DEFAULT_TRIGGER_BY
                request[f'{leg}TriggerPriceType'] = DEFAULT_TRIGGER_BY
        if isinstance(request.get('reduceOnly'), bool):
            request['reduceOnly'] = 'true' if request['reduceOnly'] else 'false'
        return request

    exchange.create_order_request = create_order_request
    features = exchange.features.get('default') if isinstance(exchange.features, dict) else None
    if isinstance(features, dict):
        features['createOrders'] = {'max': BATCH_MAX}


def _fix_errors(exchange: Any) -> None:
    exceptions = dict(exchange.exceptions)
    exceptions['exact'] = {**exceptions.get('exact', {}), **_EXACT}
    exchange.exceptions = exceptions
    exchange.httpExceptions = {**exchange.httpExceptions, '429': RateLimitExceeded}
    original = exchange.handle_errors

    def handle_errors(code: int, reason: str, url: str, method: str, headers: Any, body: str, response: Any,
                      request_headers: Any, request_body: Any) -> Any:
        if isinstance(response, dict):
            top = exchange.safe_string(response, 'code')
            data = response.get('data')
            feedback = f'{exchange.id} {body}'
            if top in ('1', '2') and isinstance(data, list) and 'batch-orders' in str(url):
                return None                          # batch element rejections are parsed by the SDK (data[i].code)
            if top in ('1', '2') and isinstance(data, list):
                # single cancel as an array (cancel-tpsl, cancel-batch-orders): class by the element code (demo: 102068)
                inner = next((exchange.safe_string(d, 'code') for d in data if isinstance(d, dict)
                              and exchange.safe_string(d, 'code') not in (None, '', '0')), None)
                if inner is not None:
                    exchange.throw_exactly_matched_exception(exchange.exceptions['exact'], inner, feedback)
            if top in (None, '0') and isinstance(data, dict):
                inner = exchange.safe_string(data, 'code')
                if inner not in (None, '', '0'):
                    exchange.throw_exactly_matched_exception(exchange.exceptions['exact'], inner, feedback)
                    raise ExchangeError(feedback)
        return original(code, reason, url, method, headers, body, response, request_headers, request_body)

    exchange.handle_errors = handle_errors


def _fix_order_endpoints(exchange: Any) -> None:
    exchange.has = {**exchange.has, 'editOrder': True, 'fetchOrder': True, 'fetchOrderTrades': False}

    async def fetch_order(id: Any, symbol: Any = None, params: Any = None) -> dict:
        await exchange.load_markets()
        market = exchange.market(symbol)
        params = dict(params or {})
        request = {'instId': market['id']}
        cid = params.pop('clientOrderId', None)
        if cid:
            request['clientOrderId'] = cid
        else:
            request['orderId'] = str(id)
        response = await exchange.request('trade/order-detail', 'private', 'GET', {**request, **params})
        data = response.get('data') if isinstance(response, dict) else None
        row = data[0] if isinstance(data, list) and data else data
        if not isinstance(row, dict) or not row.get('orderId'):
            raise OrderNotFound(f'{exchange.id} order-detail: no order {cid or id}')
        return exchange.parse_order(row, market)

    async def edit_order(id: Any, symbol: Any, type: Any, side: Any, amount: Any = None, price: Any = None,
                         params: Any = None) -> dict:
        await exchange.load_markets()
        market = exchange.market(symbol)
        params = dict(params or {})
        request = {'instId': market['id'], 'orderId': str(id), 'requestId': params.pop('requestId', None)
                   or uuid.uuid4().hex}
        if price is not None:
            request['newPrice'] = exchange.price_to_precision(symbol, price)
        if amount is not None:
            request['newSize'] = exchange.amount_to_precision(symbol, amount)
        response = await exchange.request('trade/amend-order', 'private', 'POST', {**request, **params})
        data = response.get('data') if isinstance(response, dict) else None
        row = data[0] if isinstance(data, list) and data else data
        row = row if isinstance(row, dict) else {}
        try:
            # the amend response has no clientOrderId ("" even for an order with own id, demo 2026-11-09): order from order-detail
            return await fetch_order(row.get('orderId') or str(id), symbol)
        except Exception:  # noqa: BLE001 — the amend is accepted; order not read — amend response as is
            pass
        order = exchange.parse_order({'orderId': row.get('orderId') or str(id),
                                      'clientOrderId': row.get('clientOrderId') or None, 'instId': market['id'],
                                      'side': side, 'orderType': type, 'state': 'live',
                                      'price': request.get('newPrice'), 'size': request.get('newSize')}, market)
        order['info'] = row
        return order

    exchange.fetch_order = fetch_order
    exchange.edit_order = edit_order


def _fix_account(exchange: Any) -> None:
    exchange.has = {**exchange.has, 'setPositionMode': True}
    original_balance = exchange.fetch_balance

    async def set_leverage(leverage: Any, symbol: Any = None, params: Any = None) -> dict:
        await exchange.load_markets()
        market = exchange.market(symbol)
        params = dict(params or {})
        top = ((market.get('limits') or {}).get('leverage') or {}).get('max')
        if not leverage or float(leverage) < 1 or (top and float(leverage) > float(top)):
            raise InvalidOrder(f'{exchange.id} set-leverage: leverage {leverage} outside 1..{top} (maxLeverage {market["id"]})')
        request = {'instId': market['id'], 'leverage': f'{float(leverage):.10g}',
                   'marginMode': params.pop('marginMode', None) or 'cross', **params}
        return await exchange.request('account/set-leverage', 'private', 'POST', request)

    async def fetch_leverage(symbol: Any, params: Any = None) -> dict:
        await exchange.load_markets()
        market = exchange.market(symbol)
        params = dict(params or {})
        request = {'instId': market['id'], 'marginMode': params.pop('marginMode', None) or 'cross', **params}
        response = await exchange.request('account/batch-leverage-info', 'private', 'GET', request)
        rows = [r for r in (response.get('data') or []) if isinstance(r, dict) and r.get('instId') == market['id']]
        by_side = {str(r.get('positionSide')): exchange.safe_number(r, 'leverage') for r in rows}
        net = by_side.get('net')
        return {'info': rows, 'symbol': market['symbol'], 'marginMode': request['marginMode'],
                'longLeverage': by_side.get('long', net), 'shortLeverage': by_side.get('short', net)}

    async def set_position_mode(hedged: bool, symbol: Any = None, params: Any = None) -> dict:
        request = {'positionMode': 'long_short_mode' if hedged else 'net_mode', **(params or {})}
        return await exchange.request('account/set-position-mode', 'private', 'POST', request)

    async def fetch_balance(params: Any = None) -> dict:
        params = dict(params or {})
        if not any(k in params for k in ('accountType', 'type', 'productType')):
            params['productType'] = 'USDT-FUTURES'
        return await original_balance(params)

    exchange.set_leverage = set_leverage
    exchange.fetch_leverage = fetch_leverage
    exchange.set_position_mode = set_position_mode
    exchange.fetch_balance = fetch_balance


def _fix_sign(exchange: Any) -> None:
    """Signature per F#signature with a unique nonce: prehash = requestPath(+query for GET) + METHOD + timestamp + nonce +
    body; HMAC-SHA256 → hex → Base64. The request (path, query, body — with wrapper edits such as brokerId) is built by
    ccxt; only the nonce (UUID instead of timestamp), the timestamp (corrected by timeDifference) and the signature are
    replaced here."""
    import hashlib
    from urllib.parse import urlsplit

    original = exchange.sign

    def sign(path: str, api: Any = 'public', method: str = 'GET', params: Any = None, *args: Any,
             **kwargs: Any) -> dict:
        result = original(path, api, method, {} if params is None else params, *args, **kwargs)
        headers = result.get('headers')
        if api != 'private' or not isinstance(headers, dict) or 'ACCESS-SIGN' not in headers:
            return result
        parts = urlsplit(result['url'])
        request = parts.path + (f'?{parts.query}' if parts.query else '')
        diff = exchange.safe_integer(exchange.options, 'timeDifference', 0) or 0
        timestamp = str(exchange.milliseconds() - diff)
        nonce = uuid.uuid4().hex
        auth = request + method + timestamp + nonce + (result.get('body') or '' if method != 'GET' else '')
        headers.update({'ACCESS-TIMESTAMP': timestamp, 'ACCESS-NONCE': nonce,
                        'ACCESS-SIGN': exchange.string_to_base64(exchange.hmac(
                            exchange.encode(auth), exchange.encode(exchange.secret), hashlib.sha256))})
        return result

    exchange.sign = sign

    async def fetch_time(params: Any = None) -> int:
        from email.utils import parsedate_to_datetime
        await exchange.request('market/tickers', 'public', 'GET', {'instId': 'BTC-USDT', **(params or {})})
        date = (exchange.last_response_headers or {}).get('Date') or (exchange.last_response_headers or {}).get('date')
        if not date:
            raise ExchangeError(f'{exchange.id}: no Date header — server time unknown')
        return int(parsedate_to_datetime(date).timestamp() * 1000)

    exchange.fetch_time = fetch_time
    exchange.has = {**exchange.has, 'fetchTime': True, 'fetchFundingHistory': False, 'fetchLedger': False}


def _fix_ws(exchange: Any) -> None:
    if not hasattr(exchange, 'handle_order_book'):
        return
    from ccxt.async_support.base.ws.cache import ArrayCacheBySymbolById
    original_book = exchange.handle_order_book
    original_message = exchange.handle_message
    original_watch_orders = exchange.watch_orders
    seqs: dict = {}

    def handle_order_book(client: Any, message: Any) -> Any:
        arg = message.get('arg') if isinstance(message, dict) else None
        data = message.get('data') if isinstance(message, dict) else None
        if not isinstance(arg, dict) or not isinstance(data, dict):
            return original_book(client, message)
        symbol = exchange.safe_market(arg.get('instId'))['symbol']
        seq, prev = exchange.safe_integer(data, 'seqId'), exchange.safe_integer(data, 'prevSeqId')
        message_hash = f"{arg.get('channel')}:{symbol}"
        if message.get('action') != 'snapshot' and seqs.get(symbol) is not None and prev != seqs[symbol]:
            expected = seqs.pop(symbol)
            exchange.orderbooks.pop(symbol, None)
            subs = getattr(client, 'subscriptions', None)
            if isinstance(subs, dict):
                subs.pop(message_hash, None)
            client.reject(InvalidNonce(f'{exchange.id} {symbol}: order book sequence gap (prevSeqId {prev} '
                                       f'≠ seqId {expected}) — book reset, resubscribe needed'), message_hash)
            return None
        if seq is not None:
            seqs[symbol] = seq
        result = original_book(client, message)
        book = exchange.orderbooks.get(symbol)
        if book is not None and seq is not None:
            book['nonce'] = seq
        return result

    def handle_orders_algo(client: Any, message: Any) -> None:
        cache = getattr(exchange, '_bytx_algo_orders', None)
        if cache is None:
            cache = ArrayCacheBySymbolById(exchange.safe_integer(exchange.options, 'ordersLimit', 1000))
            exchange._bytx_algo_orders = cache
        for row in message.get('data') or []:
            if isinstance(row, dict):
                row = dict(row, tpslId=row.get('tpslId') or row.get('algoId'))
                cache.append(exchange.parse_order(row))
        client.resolve(cache, 'orders-algo')

    def handle_message(client: Any, message: Any) -> Any:
        arg = message.get('arg') if isinstance(message, dict) else None
        if isinstance(arg, dict) and arg.get('channel') == 'orders-algo' and 'data' in message:
            return handle_orders_algo(client, message)
        return original_message(client, message)

    async def watch_orders(symbol: Any = None, since: Any = None, limit: Any = None, params: Any = None) -> Any:
        params = dict(params or {})
        if not (params.pop('trigger', False) or params.pop('stop', False)):
            return await original_watch_orders(symbol, since, limit, params)
        await exchange.load_markets()
        await exchange.authenticate()
        url = exchange.implode_hostname(exchange.urls['api']['ws']['swap']['private'])
        request = {'op': 'subscribe', 'args': [{'channel': 'orders-algo'}]}
        orders = await exchange.watch(url, 'orders-algo', request, 'orders-algo')
        return exchange.filter_by_symbol_since_limit(orders, symbol, since, limit, True)

    # Trades: there is no channel (F#ws-order-channel) — growth of an order's filledSize in WS orders → its trades from REST
    # fills-history (real tradeId, as in my_trades: no double counting). Empty response (index lagging) — up to 3 retries.
    import asyncio
    original_orders = exchange.handle_orders
    filled: dict = {}
    seen: set = set()

    async def fetch_fills(client: Any, row: dict, want: float) -> None:
        market = exchange.safe_market(row.get('instId'))
        for attempt in range(3):
            try:
                response = await exchange.request('trade/fills-history', 'private', 'GET',
                                                  {'instId': row.get('instId'), 'orderId': row.get('orderId')})
            except Exception as e:  # noqa: BLE001 — the error goes to the trades subscriber
                client.reject(e, 'myTrades')
                return
            trades = exchange.parse_trades(response.get('data') or [], market)
            new = [t for t in trades if t['id'] not in seen]
            if new or sum(float(t['amount'] or 0) for t in trades) >= want or attempt == 2:
                break
            await asyncio.sleep(0.5)
        if not new:
            return
        cache = getattr(exchange, 'myTrades', None)
        if cache is None:
            cache = ArrayCacheBySymbolById(exchange.safe_integer(exchange.options, 'tradesLimit', 1000))
            exchange.myTrades = cache
        for t in new:
            seen.add(t['id'])
            cache.append(t)
        client.resolve(cache, 'myTrades')

    def handle_orders(client: Any, message: Any) -> Any:
        result = original_orders(client, message)
        for row in (message.get('data') or []) if isinstance(message, dict) else []:
            if not isinstance(row, dict) or not row.get('orderId'):
                continue
            size = exchange.safe_number(row, 'filledSize') or 0.0
            if size > filled.get(row['orderId'], 0.0):
                filled[row['orderId']] = size
                asyncio.ensure_future(fetch_fills(client, row, size))
        return result

    async def watch_my_trades(symbol: Any = None, since: Any = None, limit: Any = None, params: Any = None) -> Any:
        await exchange.load_markets()
        await exchange.authenticate()
        url = exchange.implode_hostname(exchange.urls['api']['ws']['swap']['private'])
        request = {'op': 'subscribe', 'args': [{'channel': 'orders'}]}
        trades = await exchange.watch(url, 'myTrades', request, 'orders')
        if exchange.newUpdates:
            limit = trades.getLimit(symbol, limit)
        return exchange.filter_by_symbol_since_limit(trades, symbol, since, limit, True)

    # Positions (F#ws-positions-channel): a message is a snapshot of all positions (last one closed — data: []); ccxt
    # resolves only positions:<symbol> — watch_positions() without symbols stayed silent (demo 2026-11-09). A position
    # gone from the snapshot is closed: an event with zero size (no side in net, as for a one-way close).
    from ccxt.async_support.base.ws.cache import ArrayCacheBySymbolBySide
    open_keys: dict = {}

    def handle_positions(client: Any, message: Any) -> None:
        if exchange.positions is None:
            exchange.positions = ArrayCacheBySymbolBySide()
        rows = [r for r in (message.get('data') or []) if isinstance(r, dict)]
        now = {(r.get('instId'), str(r.get('positionSide') or 'net')): r for r in rows}
        flats = [{'instId': inst, 'positionSide': side, 'positions': '0', 'marginMode': prev.get('marginMode'),
                  'leverage': prev.get('leverage')} for (inst, side), prev in open_keys.items() if (inst, side) not in now]
        open_keys.clear()
        open_keys.update({k: v for k, v in now.items() if exchange.safe_number(v, 'positions')})
        parsed = [exchange.parse_position(r) for r in rows + flats]
        for p in parsed:
            if p.get('side') is not None:             # ccxt cache is per side; a net close has no side
                exchange.positions.append(p)
            client.resolve([p], f"positions:{p['symbol']}")
        if parsed:
            client.resolve(parsed, 'positions')

    exchange.handle_order_book = handle_order_book
    exchange.handle_message = handle_message
    exchange.handle_positions = handle_positions
    exchange.handle_orders = handle_orders
    exchange.watch_orders = watch_orders
    exchange.watch_my_trades = watch_my_trades
    # watch_balance (account channel) exists in ccxt, the has flag does not
    exchange.has = {**exchange.has, 'watchMyTrades': True, 'watchBalance': True}


def check_open_allowed(ex: Any, market: Any) -> None:
    """F#api-trading-permission: ``apiPermission=reduce_only`` — opening via API is closed (the exchange answers 102167),
    closing, cancels and reads work: refused before the network."""
    info = ((market.raw or {}).get('info') or {}) if hasattr(market, 'raw') else {}
    if str(info.get('apiPermission') or '').lower() == 'reduce_only':
        raise NotSupported(f'{ex.name}: {market.symbol} — opening via API is closed by the exchange (apiPermission='
                           'reduce_only); closing the position is allowed', exchange=ex.name, code='102167',
                           details={'symbol': market.symbol})


def apply(exchange: Any) -> None:
    _fix_sign(exchange)
    _fix_markets(exchange)
    _fix_order_request(exchange)
    _fix_errors(exchange)
    _fix_order_endpoints(exchange)
    _fix_account(exchange)
    _fix_ws(exchange)
