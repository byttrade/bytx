"""ccxt 4.4.65 fixes for Bitget (classic API v2), based on official docs.

Reference — https://www.bitget.com/docs/classic/ (Classic Account, Futures/Spot), fixtures
``tests/ground_truth/bitget/``. Patches on the instance only.

- **Demo Trading** (``demo-trading/rest-api``, ``demo-trading/websocket``): demo is a Demo API Key and the
  ``paptrading: 1`` header on the same host and same symbols; WS — ``wss://wspap.bitget.com``.
  ccxt in sandbox maps symbols to ``S…/SUSDT`` (productType SUSDT-FUTURES — not in docs, empty on the
  exchange), forbids spot, sends ``PAPTRADING`` without ``Content-Type`` (exchange answers 40007).
- **Futures amount step** (Get Contract Config): «the quantity of the order must be greater than
  minTradeNum and is a multiple of sizeMultiplier» — ccxt takes the step from ``minTradeNum``.
- **WS Depth Channel** (``books``): the docs have no ``checksum`` field — integrity via ``seq`` / ``pseq`` («The serial
  number of the previous push … non-zero when pushing incremental update data … Can be used to determine whether
  there is packet loss»). ccxt 4.4.65 checks crc32 against an undocumented field — the live book failed with
  ChecksumError (checked 2026-10-26); the ccxt check is disabled, a gap where pseq ≠ previous seq resets the book.
- **Spot: order edit** — Cancel an Existing Order and Send a New Order
  (``POST /api/v2/spot/trade/cancel-replace-order``: symbol, price, size — required; orderId/clientOid;
  newClientOid; response ``data`` — orderId, clientOid, success ``success``/``failure``, msg). ccxt 4.4.65
  ``edit_order`` on spot supports only plan orders (NotSupported for a limit order, demo 2026-10-26). Price/amount
  not passed in the call are taken from the order (amount — remaining). Response ``data.orderId`` is the PREVIOUS order's id (demo
  2026-10-26); the new order is found via Get Order Info by newClientOid (without one — our own uuid).
- **Spot: WS Order Channel** — ``instId``: «Product ID, e.g. ETHUSDT / ``default``: subscribe all symbols».
  ccxt 4.4.65 ``watch_orders()`` without a symbol on spot fails with ``TypeError`` (joining the subscription hash with None;
  demo 2026-10-26) — we subscribe with ``instId=default`` ourselves.
- **Place Order** (futures and spot): ``force`` — ``ioc`` / ``fok`` / ``gtc`` / ``post_only``, ``tradeSide`` —
  ``open`` / ``close`` (lowercase); ccxt sends ``GTC``/``IOC``/``FOK`` and ``Open``/``Close``.
- **Hedge: trade side** — ``side`` in hedge is the position direction (Place Order), closing a long is ``buy`` +
  ``tradeSide close``: ccxt reported REST trades and WS closing orders as buys (demo 2026-11-04) — the trade side
  comes from ``tradeSide``.
- **Leverage** (Get Single Account) — the pocket of the symbol's mode: cross — ``crossedMarginLeverage`` (ccxt —
  always isolated*).
"""
from __future__ import annotations

import asyncio
from typing import Any

from ccxt.base.errors import InvalidOrder, OrderNotFound

DEMO_WS = {'public': 'wss://wspap.bitget.com/v2/ws/public', 'private': 'wss://wspap.bitget.com/v2/ws/private'}
DEMO_HEADER = 'paptrading'
# /api/v2/public/time with paptrading — 40404 (live check 2026-10-26; ccxt excludes it too)
_NO_DEMO_HEADER_PATHS = frozenset({'v2/public/time'})


def is_demo(exchange: Any) -> bool:
    return bool(getattr(exchange, '_bytx_bitget_demo', False))


def _fix_demo(exchange: Any) -> None:
    """Demo per docs: without ccxt sandbox mode (S… symbols), we set the paptrading: 1 header ourselves."""
    if not exchange.safe_bool(exchange.options, 'sandboxMode', False):
        return
    exchange.options['sandboxMode'] = False          # ccxt: no S-symbols and no spot ban
    exchange._bytx_bitget_demo = True
    # fills-history (market trade history) in demo — 40404 «Request URL NOT FOUND» (live check 2026-10-26);
    # recent trades — «Get Recent Transactions» (/market/fills) works
    exchange.options['fetchTrades'] = {'spot': {'method': 'publicSpotGetV2SpotMarketFills'},
                                       'swap': {'method': 'publicMixGetV2MixMarketFills'}}
    ws = exchange.urls.get('api', {}).get('ws')
    if isinstance(ws, dict):
        exchange.urls['api']['ws'] = dict(DEMO_WS)
    original_sign = exchange.sign

    def sign(path, api=[], method='GET', params={}, *args, **kwargs):
        request = original_sign(path, api, method, params, *args, **kwargs)
        if path not in _NO_DEMO_HEADER_PATHS:
            headers = dict(request.get('headers') or {})
            headers[DEMO_HEADER] = '1'
            headers.setdefault('Content-Type', 'application/json')
            request['headers'] = headers
        return request

    exchange.sign = sign


def _fix_market_step(exchange: Any) -> None:
    original = exchange.parse_market

    def parse_market(market: dict) -> dict:
        result = original(market)
        step = exchange.safe_number(market, 'sizeMultiplier')
        if result.get('contract') and step:
            result['precision'] = dict(result.get('precision') or {}, amount=step)
        return result

    exchange.parse_market = parse_market


_LOWER = {'force': frozenset({'GTC', 'IOC', 'FOK'}), 'tradeSide': frozenset({'Open', 'Close'})}


def _fix_order_enums(exchange: Any) -> None:
    original = exchange.create_order_request

    def create_order_request(symbol, type, side, amount, price=None, params={}):
        request = original(symbol, type, side, amount, price, params)
        for key, values in _LOWER.items():
            if request.get(key) in values:
                request[key] = request[key].lower()
        return request

    exchange.create_order_request = create_order_request


def _fix_ws_order_book(exchange: Any) -> None:
    if not hasattr(exchange, 'handle_order_book'):
        return
    from ccxt.base.errors import InvalidNonce
    exchange.options.setdefault('watchOrderBook', {})['checksum'] = False
    original = exchange.handle_order_book
    seqs: dict = {}

    def handle_order_book(client, message):
        arg = message.get('arg') if isinstance(message, dict) else None
        data = (message.get('data') or [None])[0] if isinstance(message, dict) else None
        if not isinstance(arg, dict) or arg.get('channel') != 'books' or not isinstance(data, dict):
            return original(client, message)
        market_type = 'spot' if arg.get('instType') == 'SPOT' else 'contract'
        symbol = exchange.safe_market(arg.get('instId'), None, None, market_type)['symbol']
        seq, pseq = exchange.safe_integer(data, 'seq'), exchange.safe_integer(data, 'pseq')
        if message.get('action') == 'snapshot':
            exchange.orderbooks.pop(symbol, None)          # snapshot — start from scratch
        elif pseq and seqs.get(symbol) is not None and pseq != seqs[symbol]:
            expected = seqs.pop(symbol)
            exchange.orderbooks.pop(symbol, None)
            message_hash = 'orderbook:' + symbol
            subs = getattr(client, 'subscriptions', None)
            if isinstance(subs, dict):
                subs.pop(message_hash, None)
            client.reject(InvalidNonce(f'{exchange.id} {symbol}: order book sequence gap '
                                       f'(pseq {pseq} ≠ seq {expected}) — book reset, resubscribe required'),
                          message_hash)
            return None
        if seq is not None:
            seqs[symbol] = seq
        result = original(client, message)
        book = exchange.orderbooks.get(symbol)
        if book is not None and seq is not None:
            book['nonce'] = seq
        return result

    exchange.handle_order_book = handle_order_book


def _fix_batch_cancel(exchange: Any) -> None:
    """Futures Batch Cancel Orders (POST /api/v2/mix/order/batch-cancel-orders): ``data.successList`` and
    ``data.failureList`` (orderId, clientOid, errorCode, errorMsg). ccxt 4.4.65 ``cancel_orders`` parses only
    successList — an item rejection was lost (the SDK reported «no cancel response»; demo 2026-11-04: a removed limit
    order — failureList with errorCode 40819 «The order does not exist!»). failureList records go into the response
    with the exchange code (parsed by the SDK per item)."""
    original = exchange.cancel_orders

    async def cancel_orders(ids, symbol=None, params={}):
        params = dict(params or {})
        await exchange.load_markets()
        market = exchange.market(symbol) if symbol is not None else None
        if market is None or market['spot'] or exchange.safe_value_2(params, 'stop', 'trigger') \
                or 'marginMode' in params:
            return await original(ids, symbol, params)
        product, rest = exchange.handle_product_type_and_params(market, params)
        request = {'symbol': market['id'], 'productType': product,
                   'orderIdList': [{'orderId': str(i)} for i in ids]}
        response = await exchange.privateMixPostV2MixOrderBatchCancelOrders(exchange.extend(request, rest))
        data = exchange.safe_dict(response, 'data', {})
        rows = list(exchange.safe_list(data, 'successList', []))
        rows += [dict(r, errorCode=r.get('errorCode') or 'failureList') for r in exchange.safe_list(data, 'failureList', [])
                 if isinstance(r, dict)]
        return exchange.parse_orders(rows, market)

    exchange.cancel_orders = cancel_orders


def _fix_batch_failure(exchange: Any) -> None:
    """Batch Order: a ``data.failureList`` element (clientOid, errorCode, errorMsg — Batch Order docs;
    demo 2026-10-26: 40774, 43012). ccxt 4.4.65 parses it into an order with status='rejected' and no symbol, and
    ``parse_orders`` drops it with the symbol filter — create_orders returned []. The symbol is the batch's
    market: the SDK sees the element's rejection by clientOid, not «a batch response missing this order»."""
    original = exchange.parse_order

    def parse_order(order: dict, market: Any = None) -> dict:
        result = original(order, market)
        if isinstance(order, dict) and order.get('errorCode') not in (None, '', '00000') \
                and result.get('symbol') is None and isinstance(market, dict):
            result['symbol'] = market.get('symbol')
        return result

    exchange.parse_order = parse_order


def _spot(exchange: Any, symbol: Any) -> bool:
    try:
        return exchange.market(symbol)['type'] == 'spot'
    except Exception:
        return False


def _fix_spot_edit_order(exchange: Any) -> None:
    original = exchange.edit_order

    async def edit_order(id, symbol, type, side, amount=None, price=None, params={}):
        params = dict(params or {})
        trigger, rest = exchange.is_trigger_order(params)
        if trigger or type != 'limit' or not _spot(exchange, symbol):
            return await original(id, symbol, type, side, amount, price, params)
        await exchange.load_markets()
        market = exchange.market(symbol)
        if amount is None or price is None:
            current = await exchange.fetch_order(id, symbol)
            amount = current.get('remaining') if amount is None else amount
            price = current.get('price') if price is None else price
        request = {'symbol': market['id'], 'orderId': str(id),
                   'price': exchange.price_to_precision(symbol, price),
                   'size': exchange.amount_to_precision(symbol, amount)}
        # newClientOid always: response data.orderId is the PREVIOUS order's id (demo 2026-10-26), the new
        # order can only be found by our clientOid
        new_cid = exchange.safe_string_2(rest, 'newClientOid', 'clientOrderId') or exchange.uuid().replace('-', '')
        request['newClientOid'] = new_cid
        rest = exchange.omit(rest, ['newClientOid', 'clientOrderId'])
        response = await exchange.request('v2/spot/trade/cancel-replace-order', ['private', 'spot'], 'POST',
                                          exchange.extend(request, rest))
        data = exchange.safe_dict(response, 'data', {})
        if data.get('success') != 'success':
            raise InvalidOrder(f"{exchange.id} cancel-replace-order {id}: {data.get('msg') or data}")
        for attempt in range(3):
            info = await exchange.privateSpotGetV2SpotTradeOrderInfo({'clientOid': new_cid})
            rows = exchange.safe_list(info, 'data', [])
            if rows:
                return exchange.parse_order(rows[0], market)
            await asyncio.sleep(0.3 * (attempt + 1))
        # docs: on a duplicate newClientOid «the old order to be successfully canceled but the new order
        # placement to fail»
        raise OrderNotFound(f'{exchange.id} cancel-replace-order {id}: previous order canceled, new one '
                            f'(clientOid {new_cid}) not found — check open_orders')

    exchange.edit_order = edit_order


def _fix_ws_spot_orders(exchange: Any) -> None:
    if not hasattr(exchange, 'watch_private'):
        return
    original = exchange.watch_orders

    async def watch_orders(symbol=None, since=None, limit=None, params={}):
        params = params or {}
        if symbol is not None or 'marginMode' in params:
            return await original(symbol, since, limit, params)
        type_, _ = exchange.handle_market_type_and_params('watchOrders', None, params)
        trigger, rest = exchange.is_trigger_order(params)
        if type_ != 'spot' or trigger:
            return await original(symbol, since, limit, params)
        await exchange.load_markets()
        args = {'instType': 'SPOT', 'channel': 'orders', 'instId': 'default'}
        orders = await exchange.watch_private('order', 'order:trades:default:SPOT', args, rest)
        if exchange.newUpdates:
            limit = orders.getLimit(None, limit)
        return exchange.filter_by_symbol_since_limit(orders, None, since, limit, True)

    exchange.watch_orders = watch_orders


def _fix_ws_positions(exchange: Any) -> None:
    """The positions channel always sends a full snapshot (action=snapshot); a closed position just disappears
    from ``data`` (closing the last one gives ``data: []``, demo 2026-10-27). ccxt 4.4.65 rebuilds the cache and
    emits nothing — the subscriber never sees the close. Positions missing from the snapshot get a zero row."""
    if not hasattr(exchange, 'handle_positions'):
        return
    original = exchange.handle_positions

    def handle_positions(client, message):
        if isinstance(message, dict) and message.get('action') == 'snapshot':
            arg = message.get('arg') if isinstance(message.get('arg'), dict) else {}
            cache = (exchange.positions or {}).get(arg.get('instType', ''))
            rows = message.get('data') if isinstance(message.get('data'), list) else []
            present = {(r.get('instId'), r.get('holdSide')) for r in rows if isinstance(r, dict)}
            gone = []
            for pos in list(cache or []):
                info = pos.get('info') if isinstance(pos, dict) else None
                if not isinstance(info, dict) or (info.get('instId'), info.get('holdSide')) in present:
                    continue
                if exchange.safe_number(info, 'total'):
                    gone.append({**info, 'total': '0', 'available': '0', 'frozen': '0',
                                 'unrealizedPL': '0', 'marginSize': '0'})
            if gone:
                message = {**message, 'data': rows + gone}
        return original(client, message)

    exchange.handle_positions = handle_positions


_CLOSE_TRADE_SIDES = ('reduce_close_', 'burst_close_', 'offset_close_', 'delivery_close_', 'dte_sys_adl_close_')


def hedge_trade_side(info: Any) -> Any:
    """Trade direction (buy / sell) of a hedge-mode order or trade; not hedge / no fields — None.

    Place Order: ``side`` — «buy: Buy(one-way-mode); Long position direction(hedge-mode)», Batch Order: «For close
    long, side fill in "buy"; tradeSide should be "close"». ``tradeSide`` (Get Order Detail / Fills / Order Channel):
    open / close and ``*_close_long`` / ``*_close_short`` (liquidation, ADL, delivery — hedge). Closing a long is a sell.
    """
    if not isinstance(info, dict) or str(info.get('posMode') or '') != 'hedge_mode':
        return None
    side = str(info.get('side') or '').lower()
    trade = str(info.get('tradeSide') or '').lower()
    if trade.startswith(_CLOSE_TRADE_SIDES):
        return 'sell' if trade.endswith('_long') else 'buy' if trade.endswith('_short') else None
    if side not in ('buy', 'sell'):
        return None
    if trade == 'close':
        return 'sell' if side == 'buy' else 'buy'
    return side if trade == 'open' else None


def _fix_hedge_sides(exchange: Any) -> None:
    """Hedge: the response ``side`` is the position direction, a close trades the other way (``hedge_trade_side``).
    ccxt 4.4.65: REST trades (Get Order Fill Details) — side as is (closing a long — buy; demo 2026-11-04),
    WS Order Channel — side as is and without reduceOnly, a REST order — flipped only by reduceOnly. The WS Fill
    Channel already sends the trade direction (closing a long — sell, demo 2026-11-04) — left alone."""
    def wrap(name: str, *, reduce_only: bool) -> None:
        original = getattr(exchange, name, None)
        if original is None:
            return

        def parse(raw: Any, market: Any = None) -> Any:
            result = original(raw, market)
            side = hedge_trade_side(raw)
            if side is not None and isinstance(result, dict):
                result['side'] = side
                if reduce_only and result.get('reduceOnly') is None and raw.get('reduceOnly') not in (None, ''):
                    result['reduceOnly'] = str(raw['reduceOnly']).lower() == 'yes'
            return result

        setattr(exchange, name, parse)

    wrap('parse_trade', reduce_only=False)
    wrap('parse_order', reduce_only=True)
    wrap('parse_ws_order', reduce_only=True)


def _fix_leverage(exchange: Any) -> None:
    """Get Single Account: the leverage of the symbol's mode pocket (``data.marginMode``) — cross:
    ``crossedMarginLeverage`` for both sides, isolated: ``isolatedLongLever`` / ``isolatedShortLever``. ccxt 4.4.65
    always returns isolated* (``exchanges.bitget.leverage``)."""
    from bytx.exchanges.bitget.leverage import leverage_pocket

    def parse_leverage(leverage: dict, market: Any = None) -> dict:
        mode, long, short = leverage_pocket(leverage)
        return {'info': leverage, 'symbol': (market or {}).get('symbol'), 'marginMode': mode,
                'longLeverage': exchange.safe_integer({'v': long}, 'v'),
                'shortLeverage': exchange.safe_integer({'v': short}, 'v')}

    exchange.parse_leverage = parse_leverage


def apply(exchange: Any) -> None:
    _fix_demo(exchange)
    _fix_leverage(exchange)
    _fix_hedge_sides(exchange)
    _fix_batch_cancel(exchange)
    _fix_batch_failure(exchange)
    _fix_ws_order_book(exchange)
    _fix_market_step(exchange)
    _fix_order_enums(exchange)
    _fix_spot_edit_order(exchange)
    _fix_ws_spot_orders(exchange)
    _fix_ws_positions(exchange)
