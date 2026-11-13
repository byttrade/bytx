"""ccxt 4.4.65 fixes for binance (see tests/ground_truth/test_binance.py).

Patches on the instance only (method wrappers, ``urls``); repeated ``apply`` is a no-op.

- **WS USDⓈ-M: split endpoints.** «Important WebSocket Change Notice»: legacy
  ``wss://fstream.binance.com/ws`` and ``/stream`` were retired 2026-04-23; without a route
  the connection only gets ``/public`` data. High-frequency public streams
  (book, bookTicker) — ``/public/ws``, other market streams — ``/market/ws``, user data
  — ``/private/ws/<listenKey>`` (CAAS «User Data Streams»). Demo Mode
  (``wss://demo-fstream.binance.com``) — the same routes; testnet has no routes in the
  docs — its URL is left unchanged.
- **Demo Mode** (``demo=True``) — URL table and ccxt fixes for demo hosts: ``demo.py``.
- **USDⓈ-M conditional orders** (STOP/TAKE_PROFIT(_MARKET)/TRAILING_STOP_MARKET): since
  2025-12-09 ``POST /fapi/v1/order`` returns -4120 STOP_ORDER_SWITCH_ALGO; the request
  is redirected to ``POST /fapi/v1/algoOrder`` (New Algo Order: algoType=CONDITIONAL,
  triggerPrice, activatePrice, clientAlgoId). The ``clientAlgoId`` broker prefix
  is added by the transport (``exchanges.binance.broker_cid``) — the request goes through ``exchange.request``.
- **parse_ws_order USDⓈ-M**: ORDER_TRADE_UPDATE has no ``O`` (creation time), ccxt
  substitutes ``T`` only for x=NEW/CANCELED/AMENDMENT — for EXPIRED/TRADE the time
  was lost. We always take ``T`` (Order Trade Time) when ``O`` is missing.
- **ALGO_UPDATE USDⓈ-M**: conditional TP/SL arrive as a separate event that ccxt doesn't
  parse — the event goes into the shared ``watch_orders`` cache (statuses as in the library's REST path).
- **Spot user data** (Spot CHANGELOG 2026-01-21): ``POST /api/v3/userDataStream``
  (listenKey) was removed; subscription — ``userDataStream.subscribe.signature`` in the WebSocket API,
  events arrive wrapped as ``{"subscriptionId", "event"}``.
- **un_watch_ticker(s)**: ccxt waits for the reply on the data hash — the exchange reply "rejects" it with
  UnsubscribeError, and the subscription key (``ethusdt@ticker``) stays in ``client.subscriptions``: a repeated
  ``watch_ticker`` of the same symbol sends no SUBSCRIBE and stays silent. Fixed: an own unsubscribe hash, the
  subscription keys are removed after the reply.
- **Packing subscriptions into connections** (:func:`pack_streams`, the market hub): ccxt spreads subscriptions
  round-robin over ``streamLimits`` (50) connections — 50 symbols = 50 WS. With packing a subscription goes to
  the lowest-numbered connection that has room.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import Any

from bytx.exchanges.binance import demo

_APPLIED_ATTR = '_bytx_binance_fixes'

# ─── WS USDⓈ-M: /public, /market, /private ────────────────────────────────

_FSTREAM = 'wss://fstream.binance.com'
_LEGACY_WS = _FSTREAM + '/ws'
_PUBLIC_WS = _FSTREAM + '/public/ws'
_MARKET_WS = _FSTREAM + '/market/ws'
_PRIVATE_WS = _FSTREAM + '/private/ws'
# ccxt streamHash for /public streams (per docs: bookTicker, !bookTicker, depth, depth<levels>)
_PUBLIC_STREAM_HASHES = ('multipleOrderbook', 'bookTicker')
# public stream id tag in the ccxt URL (base/market/ws/p<N>) — stripped in client()
_PUBLIC_MARK = 'p'


def _is_stream_id(value: str) -> bool:
    """ccxt stream id is a short number (connection index), listenKey is a long string."""
    return value.isdigit() and len(value) <= 4


# Demo Mode USDⓈ-M (wss://demo-fstream.binance.com): same routes as live — all three
# accept connections and serve streams (live check without keys 2026-10-26)
_FSTREAM_BASES = (_FSTREAM, 'wss://demo-fstream.binance.com')


def _route_future_url(url: Any) -> Any:
    """ccxt URL like ``<base>/market/ws/<id|p<id>|listenKey>`` → docs endpoint."""
    if not isinstance(url, str):
        return url
    for base in _FSTREAM_BASES:
        market = base + '/market/ws'
        if not url.startswith(market + '/'):
            continue
        rest = url[len(market) + 1:]
        if _is_stream_id(rest):
            return url
        if rest.startswith(_PUBLIC_MARK) and _is_stream_id(rest[len(_PUBLIC_MARK):]):
            return f'{base}/public/ws/{rest[len(_PUBLIC_MARK):]}'
        return f'{base}/private/ws/{rest}'  # user data: listenKey in the path
    return url


def _patch_ws_routes(exchange: Any) -> None:
    ws = exchange.urls.get('api', {}).get('ws')
    legacy = {base + '/ws': base for base in _FSTREAM_BASES}
    if not isinstance(ws, dict) or ws.get('future') not in legacy:
        return  # testnet or already a different URL
    ws['future'] = legacy[ws['future']] + '/market/ws'
    original_stream = exchange.stream

    def stream(type, subscriptionHash, numSubscriptions=1):
        sid = original_stream(type, subscriptionHash, numSubscriptions)
        if type == 'future' and str(subscriptionHash).startswith(_PUBLIC_STREAM_HASHES):
            return _PUBLIC_MARK + sid
        return sid

    exchange.stream = stream


# ─── Spot user data: WebSocket API instead of listenKey ─────────────────────

# Tag instead of listenKey: ccxt builds the URL urls.ws.spot + '/' + listenKey → client() routes
# it to a dedicated WebSocket API connection (one account subscription per connection).
_SPOT_UDS_KEY = 'bytx-ws-api-user-data'
_SPOT_UDS_TASK_ATTR = '_bytx_uds_subscription'


def _spot_uds_url(exchange: Any) -> str:
    # returnRateLimits=false is a documented WS API connection parameter; it also
    # separates the user data connection from the shared ws-api (handle_balance takes the account type from
    # the client's first subscription — a client shared with fetch*Ws would break it)
    base = exchange.urls['api']['ws']['ws-api']['spot']
    return base + ('&' if '?' in base else '?') + 'returnRateLimits=false'


def _spot_uds_placeholder(exchange: Any) -> str:
    return exchange.urls['api']['ws']['spot'] + '/' + _SPOT_UDS_KEY


async def _spot_uds_subscribe(exchange: Any, url: str) -> Any:
    """userDataStream.subscribe.signature (Spot WebSocket API, «User Data Stream requests»)."""
    client = exchange.client(url)
    request_id = str(exchange.request_id(url))
    message = {
        'id': request_id,
        'method': 'userDataStream.subscribe.signature',
        'params': exchange.sign_params({}),  # apiKey, timestamp, signature
    }

    def handle(ws_client, response):  # {"id", "status": 200, "result": {"subscriptionId": 0}}
        ws_client.resolve(response, request_id)

    try:
        return await exchange.watch(url, request_id, message, request_id, {'method': handle})
    finally:
        # the subscription response must not stay in client.subscriptions: the first key there
        # must be the account type ('spot'), handle_balance expects it
        client.subscriptions.pop(request_id, None)


async def _ensure_spot_uds(exchange: Any) -> None:
    url = _spot_uds_url(exchange)
    client = exchange.client(url)
    task = getattr(client, _SPOT_UDS_TASK_ATTR, None)
    if task is None:  # new connection — subscribe once (concurrent watches wait for it)
        task = asyncio.ensure_future(_spot_uds_subscribe(exchange, url))
        setattr(client, _SPOT_UDS_TASK_ATTR, task)
    try:
        await asyncio.shield(task)
    except BaseException:
        if getattr(client, _SPOT_UDS_TASK_ATTR, None) is task and task.done():
            setattr(client, _SPOT_UDS_TASK_ATTR, None)  # the next watch retries
        raise


def _is_plain_spot_auth(exchange: Any, params: dict) -> bool:
    """authenticate for plain Spot (not margin, not portfolio margin)."""
    params = dict(params or {})
    type_, params = exchange.handle_market_type_and_params('authenticate', None, params)
    if type_ != 'spot':
        return False
    papi, params = exchange.handle_option_and_params_2(params, 'authenticate', 'papi',
                                                       'portfolioMargin', False)
    margin_mode, _ = exchange.handle_margin_mode_and_params('authenticate', params)
    return not papi and margin_mode is None


def _patch_spot_user_data(exchange: Any) -> None:
    original_auth = exchange.authenticate
    original_message = exchange.handle_message

    async def authenticate(params={}):
        if not _is_plain_spot_auth(exchange, params):
            return await original_auth(params)
        exchange.check_required_credentials()
        spot = exchange.safe_dict(exchange.options, 'spot', {})
        exchange.options['spot'] = exchange.extend(spot, {'listenKey': _SPOT_UDS_KEY})
        await _ensure_spot_uds(exchange)

    def handle_message(client, message):
        # WS API: {"subscriptionId": 0, "event": {...}} → event as before in the listenKey stream
        if isinstance(message, dict) and 'subscriptionId' in message and isinstance(message.get('event'), dict):
            message = message['event']
        return original_message(client, message)

    exchange.authenticate = authenticate
    exchange.handle_message = handle_message


def _patch_client_routing(exchange: Any) -> None:
    original_client = exchange.client

    def client(url):
        if url == _spot_uds_placeholder(exchange):
            url = _spot_uds_url(exchange)
        else:
            url = _route_future_url(url)
        return original_client(url)

    exchange.client = client


# ─── USDⓈ-M conditional orders → Algo Order API ─────────────────────────────

_CONDITIONAL_TYPES = frozenset({'STOP', 'STOP_MARKET', 'TAKE_PROFIT', 'TAKE_PROFIT_MARKET',
                                'TRAILING_STOP_MARKET'})
# /fapi/v1/order → /fapi/v1/algoOrder (New Algo Order): parameter names
_ALGO_RENAME = {'stopPrice': 'triggerPrice', 'activationPrice': 'activatePrice',
                'newClientOrderId': 'clientAlgoId'}
_ALGO_MARK = '_bytx_algo_raw'


def _algo_request(params: dict) -> dict:
    req = {_ALGO_RENAME.get(k, k): v for k, v in params.items()}
    req['algoType'] = 'CONDITIONAL'
    return req


def _algo_as_order(response: Any) -> Any:
    """New Algo Order response → New Order response fields parsed by ccxt parse_order."""
    if not isinstance(response, dict) or 'algoId' not in response:
        return response
    r = response
    return {
        'orderId': r.get('algoId'), 'clientOrderId': r.get('clientAlgoId'),
        'symbol': r.get('symbol'), 'side': r.get('side'), 'positionSide': r.get('positionSide'),
        'type': r.get('orderType'), 'origType': r.get('orderType'),
        'status': r.get('algoStatus'), 'timeInForce': r.get('timeInForce'),
        'origQty': r.get('quantity'), 'executedQty': '0', 'cumQuote': '0',
        'price': r.get('price'), 'stopPrice': r.get('triggerPrice'),
        'activatePrice': r.get('activatePrice'), 'priceRate': r.get('callbackRate'),
        'reduceOnly': r.get('reduceOnly'), 'closePosition': r.get('closePosition'),
        'workingType': r.get('workingType'), 'priceProtect': r.get('priceProtect'),
        'selfTradePreventionMode': r.get('selfTradePreventionMode'),
        'goodTillDate': r.get('goodTillDate'),
        'time': r.get('createTime'), 'updateTime': r.get('updateTime'),
        _ALGO_MARK: r,
    }


# for Exchange.order/cancel_order by algoId (GET/DELETE /fapi/v1/algoOrder)
algo_as_order = _algo_as_order


def _patch_conditional_orders(exchange: Any) -> None:
    original_post = exchange.fapiPrivatePostOrder
    original_parse = exchange.parse_order

    async def fapi_private_post_order(params={}, *args, **kwargs):
        if str((params or {}).get('type', '')).upper() not in _CONDITIONAL_TYPES:
            return await original_post(params, *args, **kwargs)
        # via exchange.request — it carries the clientAlgoId broker prefix wrapper
        response = await exchange.request('algoOrder', 'fapiPrivate', 'POST', _algo_request(params))
        return _algo_as_order(response)

    def parse_order(order, market=None):
        if isinstance(order, dict) and _ALGO_MARK in order:
            raw = order[_ALGO_MARK]
            result = original_parse({k: v for k, v in order.items() if k != _ALGO_MARK}, market)
            result['info'] = raw  # info — raw exchange response (algoId, algoStatus…)
            return result
        return original_parse(order, market)

    exchange.fapiPrivatePostOrder = fapi_private_post_order
    exchange.fapiprivate_post_order = fapi_private_post_order
    exchange.parse_order = parse_order


# ─── parse_ws_order: ORDER_TRADE_UPDATE time ───────────────────────────────

def _patch_ws_order_timestamp(exchange: Any) -> None:
    original = exchange.parse_ws_order

    def parse_ws_order(order, market=None):
        result = original(order, market)
        # futures ('ps' exists only there): no O, the order event time is T
        if result.get('timestamp') is None and isinstance(order, dict) and 'ps' in order:
            ts = exchange.safe_integer(order, 'T')
            if ts is not None:
                result['timestamp'] = ts
                result['datetime'] = exchange.iso8601(ts)
        return result

    exchange.parse_ws_order = parse_ws_order


# ─── ALGO_UPDATE (USDⓈ-M): conditional TP/SL in the orders stream ───────────

# algoStatus → order status; same as the library's REST path (``binance/tp_sl.py``):
# TRIGGERING is not in the book yet — open; TRIGGERED/FINISHED — fired
_ALGO_WS_STATUS = {
    'NEW': 'open', 'TRIGGERING': 'open',
    'TRIGGERED': 'closed', 'FINISHED': 'closed',
    'CANCELED': 'canceled', 'EXPIRED': 'canceled', 'REJECTED': 'canceled',
}


def _parse_algo_update(exchange: Any, message: dict) -> dict:
    """ALGO_UPDATE (User Data Streams «Event: Algo Order Update») → ccxt order.

    Fields per a live demo event 2026-10-29: o.aid — algoId, o.caid — clientAlgoId, o.o — type
    (STOP_MARKET / TAKE_PROFIT_MARKET / …), o.tp — trigger price, o.X — algoStatus,
    o.ai — id of the book order after triggering, o.R — reduceOnly, T — time.
    """
    o = exchange.safe_dict(message, 'o', {})
    market = exchange.safe_market(exchange.safe_string(o, 's'), None, None, 'swap')
    order_type = exchange.safe_string_upper(o, 'o') or ''
    trigger = exchange.safe_number(o, 'tp')
    price = exchange.safe_number(o, 'p')
    timestamp = exchange.safe_integer_2(message, 'T', 'E')
    amount = exchange.safe_number(o, 'q')
    status = _ALGO_WS_STATUS.get(exchange.safe_string_upper(o, 'X') or '', 'open')
    return exchange.safe_order({
        'info': o,
        'id': exchange.safe_string(o, 'aid'),
        'clientOrderId': exchange.safe_string(o, 'caid'),
        'symbol': market['symbol'],
        'type': order_type.lower() or None,
        'side': exchange.safe_string_lower(o, 'S'),
        'amount': amount,
        'price': price if price else None,
        'triggerPrice': trigger,
        'stopPrice': trigger,
        'takeProfitPrice': trigger if order_type.startswith('TAKE_PROFIT') else None,
        'stopLossPrice': trigger if order_type.startswith('STOP') else None,
        'status': status,
        'reduceOnly': exchange.safe_bool(o, 'R'),
        'timeInForce': exchange.safe_string(o, 'f'),
        'postOnly': False,
        'timestamp': timestamp,
        'datetime': exchange.iso8601(timestamp),
        'lastTradeTimestamp': None,
        'filled': None,
        'remaining': None,
        'cost': None,
        'average': None,
        'trades': None,
        'fee': None,
    }, market)


def _patch_algo_update(exchange: Any) -> None:
    """ccxt 4.4.65 doesn't know ALGO_UPDATE: conditional TP/SL (Algo Service since 2025-12-09) never reached
    watch_orders — demo 2026-10-29: 0 TP/SL events in the orders stream vs 4 ALGO_UPDATE in raw
    WS. The event is put into the shared order cache and wakes orders / orders:<symbol> subscribers."""
    original = exchange.handle_message

    def handle_message(client, message):
        if isinstance(message, dict) and message.get('e') == 'ALGO_UPDATE':
            parsed = _parse_algo_update(exchange, message)
            if exchange.orders is None:
                from ccxt.async_support.base.ws.cache import ArrayCacheBySymbolById
                exchange.orders = ArrayCacheBySymbolById(exchange.safe_integer(exchange.options, 'ordersLimit', 1000))
            exchange.orders.append(parsed)
            client.resolve(exchange.orders, 'orders')
            client.resolve(exchange.orders, 'orders:' + parsed['symbol'])
            return None
        return original(client, message)

    exchange.handle_message = handle_message


# ─── WS: packing subscriptions into connections (market hub) ──────────────────

_PACKER_ATTR = '_bytx_stream_packer'


class _StreamPacker:
    """Connection number of a subscription: the lowest one that holds at most ``per_connection`` after it.

    ccxt (``stream()``) hands out numbers round-robin ``streamIndex % streamLimits[type]``: the first 50
    subscriptions — 50 connections. Here a connection is filled up and the next one opens only when the current
    ones have no room; :meth:`release` (unsubscribe) frees a place. The group is the ccxt market type and the
    USDⓈ-M route (``/public`` and ``/market`` are different connections with the same number).
    """

    __slots__ = ('per_connection', 'by_hash', 'load')

    def __init__(self, per_connection: int) -> None:
        self.per_connection = max(1, int(per_connection))
        self.by_hash: dict[str, tuple[tuple, int, int]] = {}
        self.load: dict[tuple, dict[int, int]] = {}

    def allocate(self, group: tuple, subscription_hash: str, count: int = 1) -> int:
        known = self.by_hash.get(subscription_hash)
        if known is not None:
            return known[1]  # resubscribe (disconnect, topic unsubscribe) — the same connection
        count = max(1, int(count or 1))
        load = self.load.setdefault(group, {})
        index = 0
        while load.get(index, 0) and load.get(index, 0) + count > self.per_connection:
            index += 1
        load[index] = load.get(index, 0) + count
        self.by_hash[subscription_hash] = (group, index, count)
        return index

    def release(self, subscription_hash: str) -> None:
        known = self.by_hash.pop(subscription_hash, None)
        if known is None:
            return
        group, index, count = known
        load = self.load.get(group, {})
        left = load.get(index, 0) - count
        if left > 0:
            load[index] = left
        else:
            load.pop(index, None)

    def connections(self) -> int:
        """How many connections are in use (for metrics and tests)."""
        return sum(len(v) for v in self.load.values())


def _patch_stream_alloc(exchange: Any) -> None:
    """ccxt ``stream()`` by default; with :func:`pack_streams` — the number from the packer."""
    original_stream = exchange.stream

    def stream(type, subscriptionHash, numSubscriptions=1):
        packer = getattr(exchange, _PACKER_ATTR, None)
        if packer is None:
            return original_stream(type, subscriptionHash, numSubscriptions)
        group = (type, str(subscriptionHash).startswith(_PUBLIC_STREAM_HASHES))
        return str(packer.allocate(group, str(subscriptionHash), numSubscriptions))

    exchange.stream = stream


def pack_streams(exchange: Any, per_connection: int) -> bool:
    """The instance's subscriptions are packed into connections, up to ``per_connection`` per connection.

    For an instance with public streams of many symbols only (the market hub): 120 tickers — 3 connections of 50,
    not 50 (ccxt ``streamLimits``). The cap is ccxt ``subscriptionLimitByStream`` (200). Only a ccxt.pro binance
    instance with the SDK patches; False — not such an instance (another exchange, REST).
    """
    if getattr(exchange, 'id', None) != 'binance' or not getattr(exchange, _APPLIED_ATTR, False) \
            or not hasattr(exchange, 'parse_ws_order'):
        return False
    limits = (exchange.options or {}).get('subscriptionLimitByStream') or {}
    cap = min([int(per_connection), *(int(v) for v in limits.values() if v)])
    setattr(exchange, _PACKER_ATTR, _StreamPacker(cap))
    return True


def _ticker_stream_hash(exchange: Any, symbol: str) -> str:
    """ccxt streamHash of the ``watch_ticker(symbol)`` subscription (watch_multi_ticker_helper)."""
    channel, _ = exchange.handle_option_and_params({}, 'watchTicker', 'name', 'ticker')
    return f'{channel}::{exchange.symbol(symbol)}'


def release_ticker_stream(exchange: Any, symbol: str) -> None:
    """The place of the ``symbol`` ticker in a packed connection is free (after unsubscribe). No packing — no-op."""
    packer = getattr(exchange, _PACKER_ATTR, None)
    if packer is None:
        return
    try:
        packer.release(_ticker_stream_hash(exchange, symbol))
    except Exception:  # noqa: BLE001 — no market for the symbol: it held no place
        pass


# ─── un_watch_tickers: an unsubscribe after which watch_ticker subscribes again ──

def _patch_un_watch_tickers(exchange: Any) -> None:
    """ccxt 4.4.65 ``un_watch_tickers`` (and ``un_watch_ticker`` through it) waits for the reply on the data hash
    ``ticker:ticker@<symbol>`` (the unsubscribe is registered under it too): the exchange reply rejects it with
    UnsubscribeError, and the subscription key ``<id>@ticker`` stays in ``client.subscriptions`` — a repeated
    ``watch_ticker`` of the same symbol treats the subscription as live, sends no SUBSCRIBE and stays silent.
    Here — the same unsubscribe with the hash ``unsubscribe:ticker:<symbol>`` (as ``un_watch_order_book`` does);
    after the reply the subscription keys are removed. Unsubscribing all tickers (no symbols) and bookTicker —
    the ccxt path."""
    original = exchange.un_watch_tickers

    async def un_watch_tickers(symbols=None, params={}):  # noqa: B006 — ccxt signature
        channel, rest = exchange.handle_option_and_params(params, 'watchTickers', 'name', 'ticker')
        if not symbols or channel == 'bookTicker':
            return await original(symbols, params)
        await exchange.load_markets()
        symbols = exchange.market_symbols(symbols, None, True, False, True)
        first = exchange.market(symbols[0])
        market_type, rest = exchange.handle_market_type_and_params('watchTickers', first, rest)
        sub_type, rest = exchange.handle_sub_type_and_params('watchTickers', first, rest)
        if exchange.isLinear(market_type, sub_type):
            raw_type = 'future'
        elif exchange.isInverse(market_type, sub_type):
            raw_type = 'delivery'
        elif market_type == 'spot':
            raw_type = 'spot'
        else:
            return await original(symbols, params)
        markets = [exchange.market(s) for s in symbols]
        args = [m['lowercaseId'] + '@' + channel for m in markets]
        data_hashes = [exchange.get_message_hash(channel, m['symbol'], False) for m in markets]
        unsub_hashes = ['unsubscribe:ticker:' + m['symbol'] for m in markets]
        stream_hash = channel + '::' + ','.join(symbols)
        url = exchange.urls['api']['ws'][raw_type] + '/' + exchange.stream(raw_type, stream_hash)
        request_id = exchange.request_id(url)
        request = {'method': 'UNSUBSCRIBE', 'params': args, 'id': request_id}
        subscription = {'unsubscribe': True, 'id': str(request_id), 'subMessageHashes': data_hashes,
                        'messageHashes': unsub_hashes, 'symbols': symbols, 'topic': 'ticker'}
        result = await exchange.watch_multiple(url, unsub_hashes, exchange.extend(request, rest), unsub_hashes,
                                               subscription)
        client = (exchange.clients or {}).get(_route_future_url(url))
        if client is not None:  # the next watch_ticker sends SUBSCRIBE again
            for arg in args:
                client.subscriptions.pop(arg, None)
        return result

    exchange.un_watch_tickers = un_watch_tickers


def apply(exchange: Any) -> None:
    """ccxt binance instance patches (idempotent)."""
    if getattr(exchange, _APPLIED_ATTR, False):
        return
    if inspect.iscoroutinefunction(exchange.create_order):
        _patch_conditional_orders(exchange)
        _patch_futures_ticker_bid_ask(exchange)
    if hasattr(exchange, 'parse_ws_order'):  # ccxt.pro
        _fix_spot_testnet_ws_urls(exchange)
        demo.patch_spot_stream_detection(exchange)
        _patch_stream_alloc(exchange)   # before the routes: their stream() wrapper calls this one
        _patch_ws_routes(exchange)
        _patch_un_watch_tickers(exchange)
        _patch_ws_order_timestamp(exchange)
        _patch_algo_update(exchange)
        _patch_spot_user_data(exchange)
        _patch_client_routing(exchange)
        _use_agg_trades_on_futures(exchange)
    setattr(exchange, _APPLIED_ATTR, True)


_BID_ASK_KEYS = ('bid', 'bidVolume', 'ask', 'askVolume')


def _patch_futures_ticker_bid_ask(ex: Any) -> None:
    """USDⓈ-M ticker: bid/ask from bookTicker.

    Docs: «24hr Ticker Price Change Statistics» (/fapi/v1/ticker/24hr) has no
    bid/ask — they are in «Symbol Order Book Ticker» (/fapi/v1/ticker/bookTicker). ccxt
    returns the futures ticker with bid=ask=None (live check). We fetch bookTicker
    (weight 2 per symbol) and fill in the missing fields; spot and COIN-M are left alone.
    """
    original = ex.fetch_ticker

    async def fetch_ticker(symbol, params={}):  # noqa: B006 — ccxt signature
        ticker = await original(symbol, params)
        market = ex.market(ticker.get('symbol') or symbol)
        if market.get('linear') and ticker.get('bid') is None and ticker.get('ask') is None:
            # single symbol — weight 2 (ccxt fetch_bids_asks pulls the whole market, weight 5)
            raw = await ex.fapiPublicGetTickerBookTicker({'symbol': market['id']})
            book = ex.parse_ticker(raw, market)
            for key in _BID_ASK_KEYS:
                if ticker.get(key) is None and book.get(key) is not None:
                    ticker[key] = book[key]
        return ticker

    ex.fetch_ticker = fetch_ticker


_SPOT_TESTNET_STREAMS = 'wss://stream.testnet.binance.vision/ws'
_SPOT_TESTNET_WS_API = 'wss://ws-api.testnet.binance.vision/ws-api/v3'


def _fix_spot_testnet_ws_urls(ex: Any) -> None:
    """Spot testnet: docs — streams «wss://stream.testnet.binance.vision/ws», WS API —
    «wss://ws-api.testnet.binance.vision/ws-api/v3». ccxt 4.4.65 uses testnet.binance.vision/ws
    (404) — spot WS on demo didn't work (live check)."""
    ws = (ex.urls.get('api') or {}).get('ws') or {}
    for key in ('spot', 'margin'):
        if str(ws.get(key, '')).startswith('wss://testnet.binance.vision/ws'):
            ws[key] = _SPOT_TESTNET_STREAMS
    ws_api = ws.get('ws-api') if isinstance(ws.get('ws-api'), dict) else {}
    if str(ws_api.get('spot', '')).startswith('wss://testnet.binance.vision/ws-api'):
        ws_api['spot'] = _SPOT_TESTNET_WS_API


def _use_agg_trades_on_futures(ex: Any) -> None:
    """USDⓈ-M trade feed — the aggTrade stream (/market), not trade.

    USDⓈ-M Market Streams docs: the only trade stream is «Aggregate Trade»; ccxt
    subscribes by default to the undocumented «trade», which is silent on /market
    (live check: ws.trades with no data). On spot trade is documented — left alone.
    """
    if str((ex.options or {}).get('defaultType', '')).lower() in ('swap', 'future', 'linear'):
        # ccxt picks the option by the calling method name (callerMethodName) — set both
        for method in ('watchTrades', 'watchTradesForSymbols'):
            ex.options[method] = {**(ex.options.get(method) or {}), 'name': 'aggTrade'}
