"""ccxt 4.4.65 fixes for bingx (see tests/ground_truth).

Reference — BingX docs-v3 (https://bingx-api.github.io/docs-v3/), section USDT-M
Perp Futures (Swap). Everything below applies to linear swap only; spot, coin-M (cswap) and
standard contracts go to the original ccxt methods unchanged.
"""
from __future__ import annotations

from typing import Any

import ccxt
from ccxt.base.precise import Precise

from bytx.exchanges.bingx.patches import bingx_trade_id

_APPLIED = '_bytx_bingx_fixes'


def _is_linear_swap(market: Any) -> bool:
    return bool(market) and bool(market.get('swap')) and not market.get('inverse')


def _fix_order_book(exchange: Any) -> None:
    """Swap/Market Data/Order Book: amount in coin — bidsCoin/asksCoin.

    bids/asks — «quantity» (1000x smaller in the SHIB-USDT example), bidsCoin/asksCoin —
    «quantity(coin)». ccxt contractSize = 1, so the book amount must be in coin.
    """
    original = exchange.fetch_order_book

    async def fetch_order_book(symbol, limit=None, params={}):
        await exchange.load_markets()
        market = exchange.market(symbol)
        market_type, query = exchange.handle_market_type_and_params('fetchOrderBook', market, params)
        if market_type == 'spot' or not _is_linear_swap(market):
            return await original(symbol, limit, params)
        request = {'symbol': market['id']}
        if limit is not None:
            request['limit'] = limit
        response = await exchange.swapV2PublicGetQuoteDepth(exchange.extend(request, query))
        book = exchange.safe_dict(response, 'data', {})
        timestamp = exchange.safe_integer_2(book, 'T', 'ts')
        has_coin = 'bidsCoin' in book or 'asksCoin' in book
        bids_key, asks_key = ('bidsCoin', 'asksCoin') if has_coin else ('bids', 'asks')
        return exchange.parse_order_book(book, market['symbol'], timestamp, bids_key, asks_key, 0, 1)

    exchange.fetch_order_book = fetch_order_book


def _fix_funding_rate(exchange: Any) -> None:
    """Swap/Market Data/Mark Price and Funding Rate: with symbol, data is an object, not a list.

    ccxt reads data via safe_list → parse_funding_rate gets [] and all fields are None.
    """
    original = exchange.fetch_funding_rate

    async def fetch_funding_rate(symbol, params={}):
        await exchange.load_markets()
        market = exchange.market(symbol)
        if not _is_linear_swap(market):
            return await original(symbol, params)
        request = {'symbol': market['id']}
        response = await exchange.swapV2PublicGetQuotePremiumIndex(exchange.extend(request, params))
        data = exchange.safe_value(response, 'data', {})
        if isinstance(data, list):  # in case of a list — take our symbol's record
            data = next((d for d in data if exchange.safe_string(d, 'symbol') == market['id']),
                        exchange.safe_dict(data, 0, {}))
        return exchange.parse_funding_rate(data, market)

    exchange.fetch_funding_rate = fetch_funding_rate


def _fix_balance(exchange: Any) -> None:
    """Swap/Account Endpoints/Query account data: USDT-M balance — GET /openApi/swap/v3/user/balance.

    ccxt calls /v2/user/balance (absent from current docs). The v3 response is a list of assets,
    parsed by the stock parse_balance. The request goes through exchange.request → the instance's sign,
    so the X-SOURCE-KEY broker header is set as usual.
    """
    original = exchange.fetch_balance

    async def fetch_balance(params={}):
        await exchange.load_markets()
        standard, rest = exchange.handle_option_and_params(params, 'fetchBalance', 'standard', False)
        sub_type, rest = exchange.handle_sub_type_and_params('fetchBalance', None, rest)
        market_type, query = exchange.handle_market_type_and_params('fetchBalance', None, rest)
        if standard or market_type == 'spot' or sub_type == 'inverse':
            return await original(params)
        response = await exchange.request('user/balance', ['swap', 'v3', 'private'], 'GET', query,
                                          None, None, {'cost': 2})
        return exchange.parse_balance(response)

    exchange.fetch_balance = fetch_balance


def _fix_position(exchange: Any) -> None:
    """Swap/Account Endpoints/Query position data: liquidationPrice (float64) is in the response.

    ccxt hardcodes liquidationPrice=None. 0 is treated as «no liquidation price».
    """
    original = exchange.parse_position

    def parse_position(position, market=None):
        result = original(position, market)
        if result.get('liquidationPrice') is None:
            liq = exchange.safe_number(position, 'liquidationPrice')
            if liq:
                result['liquidationPrice'] = liq
        return result

    exchange.parse_position = parse_position


def _fix_trade_id(exchange: Any) -> None:
    """Swap/Websocket Account Data/Order update push: Trade ID — field td (ccxt reads only id/t)."""
    original = exchange.parse_trade

    def parse_trade(trade, market=None):
        result = original(trade, market)
        if result.get('id') is None and isinstance(trade, dict) and trade.get('td') not in (None, ''):
            result['id'] = str(trade['td'])
        return result

    exchange.parse_trade = parse_trade


# Query historical transaction details (fillHistory): window — last 7 days, up to 1000 rows per page
_FILL_HISTORY_WINDOW_MS = 7 * 24 * 3600 * 1000 - 60_000
_FILL_HISTORY_PAGE = 1000
_FILL_HISTORY_MAX_PAGES = 20


async def _fill_history(exchange: Any, market: dict, since: Any, limit: Any, now: int, until: Any = None) -> list:
    """Swap/Trades Endpoints/Query historical transaction details (GET /openApi/swap/v2/trade/fillHistory).

    Unlike allFillOrders, the row carries tradeId — the same id as «td» in the WS order event
    (demo VST 2026-10-29: 587875982 in both). With the synthetic allFillOrders id, one trade from
    REST reconciliation and from the stream was counted twice (demo: engine position 2.19 vs 0.73 on the exchange).
    Docs: startTs < filledTime <= endTs, both required, window — 7 days from the current date, sorted
    by filledTime ascending; currency defaults to USDT (passed for USDC pairs). ``until`` — endTs; if the pages do
    not cover the window with ``until`` — ExchangeError (an incomplete window is never returned as complete).
    """
    start = max(int(since) - 1 if since is not None else 0, now - _FILL_HISTORY_WINDOW_MS)
    request = {'symbol': market['id'], 'startTs': start, 'endTs': now if until is None else min(int(until), now),
               'pageSize': _FILL_HISTORY_PAGE}
    settle = market.get('settle')
    if settle and settle.upper() == 'USDC':
        request['currency'] = 'USDC'
    rows: list = []
    for page in range(1, _FILL_HISTORY_MAX_PAGES + 1):
        response = await exchange.swapV2PrivateGetTradeFillHistory(exchange.extend(request, {'pageIndex': page}))
        data = exchange.safe_dict(response, 'data', {})
        batch = exchange.safe_list(data, 'fill_history_orders', []) or []
        rows.extend(batch)
        total = exchange.safe_integer(data, 'total')
        if len(batch) < _FILL_HISTORY_PAGE or (total is not None and len(rows) >= total):
            break
    else:
        if until is not None:
            raise ccxt.ExchangeError(f'bingx fillHistory: {_FILL_HISTORY_MAX_PAGES} pages — window not read completely')
    trades = [_parse_fill_history_row(exchange, row, market) for row in rows]
    trades = [t for t in trades if since is None or (t['timestamp'] or 0) >= since]
    return trades[-limit:] if limit else trades


def _parse_fill_history_row(exchange: Any, row: dict, market: dict) -> dict:
    timestamp = exchange.parse8601(exchange.safe_string(row, 'filledTime'))
    commission = exchange.safe_string(row, 'commission')
    trade = exchange.safe_trade({
        'info': row,
        'id': exchange.safe_string(row, 'tradeId'),
        'order': exchange.safe_string(row, 'orderId'),
        'timestamp': timestamp,
        'datetime': exchange.iso8601(timestamp),
        'symbol': market['symbol'],
        'type': None,
        'side': exchange.safe_string_lower(row, 'side'),
        'takerOrMaker': exchange.safe_string_lower(row, 'role'),
        'price': exchange.safe_string(row, 'price'),
        'amount': exchange.safe_string(row, 'qty'),          # in coin (Transaction quantity)
        'cost': exchange.safe_string(row, 'quoteQty'),
        # commission < 0 — charged (like fillFee/«n» in WS), > 0 — rebate: ccxt cost is the fee paid
        'fee': {'cost': Precise.string_neg(commission) if commission is not None else None,
                'currency': exchange.safe_currency_code(exchange.safe_string(row, 'commissionAsset'))},
    }, market)
    trade['clientOrderId'] = exchange.safe_string(row, 'clientOrderID') or None
    return trade


def _fix_my_trades(exchange: Any) -> None:
    """Swap/Trades Endpoints/Query historical transaction orders: tradingUnit=COIN → volume in coin.

    ccxt sends tradingUnit=CONT by default and always multiplies volume by tradeMinQuantity —
    with COIN (as in the docs example) the size is inflated. We request COIN by default and
    take volume as is; an explicitly passed CONT is left to ccxt logic.
    The row has no trade id — ccxt returns None (deduping trades by id would drop them):
    :func:`bytx.exchanges.bingx.patches.bingx_trade_id`.
    """
    original = exchange.fetch_my_trades

    async def fetch_my_trades(symbol=None, since=None, limit=None, params={}):
        if symbol is None:
            return await original(symbol, since, limit, params)
        await exchange.load_markets()
        market = exchange.market(symbol)
        if not _is_linear_swap(market):
            return await original(symbol, since, limit, params)
        unit = exchange.safe_string_upper(params, 'tradingUnit', 'COIN')
        trades = await original(symbol, since, limit, exchange.extend(params, {'tradingUnit': unit}))
        # an allFillOrders row has no trade id (docs example) — stable id from the row fields
        seen: dict = {}
        for t in trades:
            if not t.get('id'):
                info = t.get('info') if isinstance(t.get('info'), dict) else {}
                t['id'] = str(info.get('tradeId') or '') or bingx_trade_id(info, seen)
        if unit != 'COIN':
            return trades
        for t in trades:
            volume = exchange.safe_string(t.get('info'), 'volume')
            if volume is None:
                continue
            t['amount'] = exchange.parse_number(volume)
            price = exchange.safe_string(t.get('info'), 'price')
            if price is not None:
                t['cost'] = exchange.parse_number(Precise.string_mul(price, volume))
        return trades

    exchange.fetch_my_trades = fetch_my_trades

    async def fetch_fill_history(symbol, since=None, limit=None, until=None):
        """Linear swap trades with tradeId (fillHistory); None — window older than 7 days or not swap:
        the caller falls back to fetch_my_trades (allFillOrders)."""
        await exchange.load_markets()
        market = exchange.market(symbol)
        now = exchange.milliseconds()
        if not _is_linear_swap(market) or (since is not None and since <= now - _FILL_HISTORY_WINDOW_MS):
            return None
        return await _fill_history(exchange, market, since, limit, now, until)

    exchange.bytx_fetch_fill_history = fetch_fill_history


_NO_TRADE_ID = (None, 0, '0', '')
_FINAL_STATUSES = ('FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED')


def _fix_ws_trades(exchange: Any) -> None:
    """Swap WS fills (Order update push): one fill — one trade with the fill amount.

    BingX sends two events per fill with identical fields (VST 2026-10-29, raw capture):
    ORDER_TRADE_UPDATE with ``td=0`` followed by TRADE_UPDATE with the trade id in ``td`` (= fillHistory
    tradeId). ccxt 4.4.65 makes a trade from both (demo: one fill counted twice) and
    only on ``X=FILLED`` (partial fills were lost until REST reconciliation), and takes the amount from ``q`` —
    the order amount. Here: a trade is only the event with ``td``; on every ``x=TRADE`` (partial too);
    amount — increment of the order's cumulative ``z``, price — ``tv`` / amount (``tv`` — trade value).
    """
    original_message = exchange.handle_message
    original_trades = exchange.handle_my_trades
    cum: dict = {}                       # order id → cumulative z over trades already emitted

    def handle_my_trades(client, message):
        o = message.get('o') if isinstance(message, dict) else None
        if not isinstance(o, dict):
            return original_trades(client, message)           # spot — stock ccxt
        if o.get('td') in _NO_TRADE_ID:
            return None                                       # copy without trade id: the trade comes in TRADE_UPDATE
        order_id = str(o.get('i'))
        total = exchange.safe_string(o, 'z')
        prev = cum.get(order_id, '0')
        if total is not None and Precise.string_gt(total, prev):
            qty = Precise.string_sub(total, prev)
            fixed = dict(o, q=qty)
            value = exchange.safe_string(o, 'tv')
            if value is not None and Precise.string_gt(value, '0'):
                fixed['p'] = Precise.string_div(value, qty)
            message = dict(message, o=fixed)
            cum[order_id] = total
        if str(o.get('X') or '').upper() in _FINAL_STATUSES:
            cum.pop(order_id, None)
        return original_trades(client, message)

    def handle_message(client, message):
        result = original_message(client, message)
        # ccxt emits a trade only on X=FILLED — a partial fill (x=TRADE) is a trade too
        if isinstance(message, dict) and message.get('e') in ('ORDER_TRADE_UPDATE', 'TRADE_UPDATE'):
            o = message.get('o') if isinstance(message.get('o'), dict) else {}
            if o.get('x') == 'TRADE' and str(o.get('X') or '').upper() != 'FILLED':
                handle_my_trades(client, message)
        return result

    exchange.handle_my_trades = handle_my_trades
    exchange.handle_message = handle_message


def _fix_ws(exchange: Any) -> None:
    """WS (ccxt.pro): order event name and book ts."""
    if not hasattr(exchange, 'handle_order_book'):
        return  # REST instance without ccxt.pro

    original_message = exchange.handle_message

    def handle_message(client, message):
        # Order update push: docs example — e='TRADE_UPDATE', text — ORDER_TRADE_UPDATE;
        # ccxt knows only the latter and silently drops the former.
        if isinstance(message, dict) and message.get('e') == 'TRADE_UPDATE':
            message = dict(message, e='ORDER_TRADE_UPDATE')
        return original_message(client, message)

    exchange.handle_message = handle_message
    _fix_ws_trades(exchange)

    original_status = exchange.parse_order_status

    def parse_order_status(status):
        # WS ORDER_TRADE_UPDATE: a resting limit order is X='WORKING' (VST 2026-10-29); ccxt's table
        # lacks it, the status passed through as is, and the tracker waited for 'open' until timeout
        if isinstance(status, str) and status.upper() == 'WORKING':
            return 'open'
        return original_status(status)

    exchange.parse_order_status = parse_order_status

    original_book = exchange.handle_order_book

    def handle_order_book(client, message):
        # Partial Order Book Depth: ts — server time the push was built; ccxt doesn't read it.
        # The handler is synchronous — set it before the subscriber gets the book.
        original_book(client, message)
        ts = exchange.safe_integer(message, 'ts')
        if ts is None:
            return
        data = exchange.safe_dict(message, 'data', {})
        first = (exchange.safe_string(message, 'dataType') or '').split('@')[0]
        market_type = 'swap' if 'swap' in (getattr(client, 'url', '') or '') else 'spot'
        market = exchange.safe_market(exchange.safe_string(data, 'symbol', first), None, None, market_type)
        book = exchange.orderbooks.get(market['symbol']) if market.get('symbol') else None
        if book is not None and book.get('timestamp') is None:
            book['timestamp'] = ts
            book['datetime'] = exchange.iso8601(ts)

    exchange.handle_order_book = handle_order_book


def _keep_ws_urls_on_demo(ex: Any) -> None:
    """VST (demo): ccxt set_sandbox_mode replaces urls['api'] with test URLs lacking 'ws' — any
    WS stream failed with KeyError('ws'). VST market data matches live, WS hosts are the same:
    take them from the live URLs ccxt saved (apiBackup)."""
    api = ex.urls.get('api') or {}
    backup = ex.urls.get('apiBackup') or {}
    if isinstance(api, dict) and 'ws' not in api and isinstance(backup, dict) and 'ws' in backup:
        api['ws'] = dict(backup['ws'])
        # Docs «Websocket Market Data → Access»: VST has its own host (live accepts a VST listenKey
        # too — checked on VST 2026-10-28, but the docs are the source of truth)
        api['ws']['linear'] = VST_WS


VST_WS = 'wss://vst-open-api-ws.bingx.com/swap-market'


#: VST: GETs of the `account` section that VST serves on the production paths (demo 2026-11-04, check_key):
#: «Query API KEY Permissions» (/openApi/v1/account/apiPermissions) and «Query Account UID» (/openApi/account/v1/uid)
VST_ACCOUNT_READS = frozenset({'account/apiPermissions', 'uid'})


def _vst_section(section: Any, path: str, method: str) -> bool:
    """A ccxt section that goes to the VST swap host on VST: `user` (listenKey) and the reads of :data:`VST_ACCOUNT_READS`."""
    if not section:
        return False
    if section[0] == 'user':
        return True
    return section[0] == 'account' and str(method).upper() == 'GET' and path in VST_ACCOUNT_READS


def _allow_listen_key_on_demo(ex: Any) -> None:
    """VST: listenKey for private streams (POST /openApi/user/auth/userDataStream) and key permissions / UID
    (check_key) — on VST REST.

    ccxt 4.4.65 `sign` in sandbox rejects everything except `swap` («does not have a testnet/sandbox URL
    for user endpoints») — private WS streams never started on demo, and check_key didn't know the key
    permissions (can_withdraw=None). VST issues a listenKey on the same path (checked 2026-10-28), key permissions
    and UID too (2026-11-04). Only the `user` section and the reads of :data:`VST_ACCOUNT_READS`, and only on the
    VST swap host.
    """
    original = ex.sign

    def sign(path, section='public', method='GET', params={}, headers=None, body=None):
        api = ex.urls.get('api') or {}
        if not (ex.safe_bool(ex.options, 'sandboxMode', False) and _vst_section(section, path, method)
                and isinstance(api, dict) and 'swap' in api):
            return original(path, section, method, params, headers, body)
        name = section[0]
        had, previous = name in api, api.get(name)
        api[name] = api['swap']
        ex.options['sandboxMode'] = False      # sign is synchronous: the swap doesn't overlap other requests
        try:
            return original(path, section, method, params, headers, body)
        finally:
            ex.options['sandboxMode'] = True
            if had:
                api[name] = previous
            else:
                api.pop(name, None)

    ex.sign = sign


def _fix_cancel_replace(ex: Any) -> None:
    """swap cancelReplace (ccxt edit_order): a partial outcome is an error, not «order with id 0».

    Response is code 0 even when replacement fails: data.cancelResult / data.replaceResult ("true"/"false"),
    replaceMsg — reason, newOrderResponse.orderId = '0'. ccxt parsed newOrderResponse as success:
    the original order was canceled, no new one exists, and the caller saw «modified» (VST 2026-10-28, 109400 in hedge).
    """
    original = ex.swapV1PrivatePostTradeCancelReplace

    async def cancel_replace(*args: Any, **kwargs: Any) -> Any:
        response = await original(*args, **kwargs)
        data = (response or {}).get('data') if isinstance(response, dict) else None
        if isinstance(data, dict):
            canceled = str(data.get('cancelResult')).lower() == 'true'
            replaced = str(data.get('replaceResult')).lower() == 'true'
            if not canceled or not replaced:
                state = ('original order CANCELED, new one not placed' if canceled
                         else 'original order not canceled, nothing changed')
                raise ccxt.InvalidOrder(f'bingx cancelReplace: {state}: '
                                        f'{data.get("replaceMsg") or data.get("cancelMsg") or "no reason given"}')
        return response

    ex.swapV1PrivatePostTradeCancelReplace = cancel_replace


def _parse_ws_positions(exchange: Any, message: dict) -> list:
    """Swap ACCOUNT_UPDATE ``a.P`` → ccxt positions (VST 2026-10-29: s, pa, ep, up, mt, iw, ps, cr).

    Close — ``pa`` = 0 (the position arrives with zero, side ``ps`` is kept).
    """
    a = exchange.safe_dict(message, 'a', {})
    timestamp = exchange.safe_integer_2(message, 'T', 'E')
    result = []
    for p in exchange.safe_list(a, 'P', []) or []:
        market = exchange.safe_market(exchange.safe_string(p, 's'), None, '-', 'swap')
        amount = exchange.safe_number(p, 'pa') or 0.0
        ps = (exchange.safe_string_upper(p, 'ps') or 'BOTH')
        side = ps.lower() if ps in ('LONG', 'SHORT') else ('long' if amount >= 0 else 'short')
        result.append(exchange.safe_position({
            'info': p,
            'id': None,
            'symbol': market['symbol'],
            'contracts': abs(amount),
            'contractSize': exchange.safe_number(market, 'contractSize'),
            'side': side,
            'hedged': ps in ('LONG', 'SHORT'),
            'entryPrice': exchange.safe_number(p, 'ep'),
            'unrealizedPnl': exchange.safe_number(p, 'up'),
            'marginMode': exchange.safe_string(p, 'mt'),
            'collateral': exchange.safe_number(p, 'iw'),
            'timestamp': timestamp,
            'datetime': exchange.iso8601(timestamp),
        }))
    return result


_POSITIONS_HASH = 'swap:positions'


def _add_watch_positions(exchange: Any) -> None:
    """watchPositions for linear swap: ccxt 4.4.65 lacks it, BingX positions arrive only inside
    ACCOUNT_UPDATE (no separate channel, swap docs «Account balance and position update push»).

    Subscription is the same as watch_balance (the account listenKey connection): watch_positions
    starts it with a background watch_balance and waits for the 'swap:positions' future woken by the
    handle_balance wrapper. A dropped connection rejects the future — the next call resubscribes.
    """
    if not hasattr(exchange, 'handle_balance'):
        return  # REST instance without ccxt.pro
    import asyncio

    original_balance = exchange.handle_balance

    def handle_balance(client, message):
        original_balance(client, message)
        if isinstance(message, dict) and 'P' in exchange.safe_dict(message, 'a', {}):
            positions = _parse_ws_positions(exchange, message)
            if positions:
                client.resolve(positions, _POSITIONS_HASH)

    exchange.handle_balance = handle_balance
    state: dict = {}

    async def watch_positions(symbols=None, since=None, limit=None, params={}):
        await exchange.load_markets()
        task = state.get('sub')
        if task is None or task.done():
            state['sub'] = asyncio.ensure_future(exchange.watch_balance())
            await asyncio.sleep(0)
        await exchange.authenticate()
        url = exchange.safe_string(exchange.urls['api']['ws'], 'linear') + '?listenKey=' + exchange.options['listenKey']
        positions = await exchange.client(url).future(_POSITIONS_HASH)
        if symbols:
            wanted = set(symbols)
            positions = [p for p in positions if p.get('symbol') in wanted]
        return positions

    exchange.watch_positions = watch_positions
    exchange.has = dict(exchange.has, watchPositions=True)


# Codes of the docs-v3 table (Error codes, Perpetual Futures; verified 2026-11-04, fixture
# tests/ground_truth/bingx/demo_error_codes.json) that ccxt 4.4.65 exceptions don't have:
# 101215 «Post Only order was cancelled because it would have been filled immediately» — post-only rejection
# (ccxt — a bare ExchangeError: chase did not recognize the «bounce», place_order went to outcome reconciliation);
# 109421 «The specified order does not exist — it may have been filled, cancelled, or the order ID is
# incorrect» — on Query Order too (VST: right after a fill, demo 2026-11-04);
# 100410 — request frequency limit («Request frequency too high, rate limit triggered…»; ccxt —
# OperationFailed, i.e. «outcome unknown», although the request was not accepted).
EXACT_ERRORS = {'101215': ccxt.OrderImmediatelyFillable, '109421': ccxt.OrderNotFound,
                '100410': ccxt.RateLimitExceeded}


def _fix_exceptions(exchange: Any) -> None:
    exceptions = dict(exchange.exceptions or {})
    exceptions['exact'] = {**(exceptions.get('exact') or {}), **EXACT_ERRORS}
    exchange.exceptions = exceptions


def apply(exchange: Any) -> None:
    """ccxt bingx instance patches (repeated call is a no-op)."""
    if getattr(exchange, _APPLIED, False):
        return
    setattr(exchange, _APPLIED, True)
    _fix_order_book(exchange)
    _fix_funding_rate(exchange)
    _fix_balance(exchange)
    _fix_position(exchange)
    _fix_trade_id(exchange)
    _fix_my_trades(exchange)
    _fix_ws(exchange)
    _add_watch_positions(exchange)
    _keep_ws_urls_on_demo(exchange)
    _allow_listen_key_on_demo(exchange)
    _fix_cancel_replace(exchange)
    _fix_exceptions(exchange)


__all__ = ['apply']
