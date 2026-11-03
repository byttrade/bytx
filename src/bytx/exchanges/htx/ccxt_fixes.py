"""ccxt 4.4.65 fixes for HTX (USDT-M linear swap, spot), based on official docs.

Reference — https://huobiapi.github.io/docs/usdt_swap/v1/en/ and https://huobiapi.github.io/docs/spot/v1/en/, fixtures
``tests/ground_truth/htx/``. Patches on the instance only.

- **Contract minimum amount**: ``volume`` — «Numbers of orders (volume)», long (contracts); ccxt puts the
  contract size (0.001 BTC) into ``limits.amount.min`` — the minimum is 1 contract.
- **reduce_only** — «in hedge mode it is invalid»: in hedge ccxt sends both ``offset=close`` and ``reduce_only=1`` —
  in hedge only offset defines a close.
- **TP/SL on entry**: Place An Order — ``tp_trigger_price`` / ``sl_trigger_price`` (+ ``*_order_price_type``,
  «default is market»), «Only open orders can support setting take profit and stop loss». ccxt doesn't know the
  ``takeProfit`` / ``stopLoss`` dicts (they'd go into the request as is), and ``stopLossPrice`` turns the order into
  a separate tpsl order.
- Contract **client_order_id** is an integer «[1, 9223372036854775807]» (ccxt uses ``safe_integer`` and silently
  drops a non-integer id) — the SDK validates the format before sending (``order_params.MARKET_CLIENT_ID_FORMATS``).

Contracts (instance ``defaultType`` swap), checked 2026-10-27 against the USDT-M v1 docs:

- **Cancel**: Cancel An Order responds ``status: ok`` even on failure — ``data.errors[{order_id, err_code,
  err_msg}]`` («successfully withdrew list» — ``successes``); ccxt always returns ``canceled``. A failure for
  our id → exception with ``err_code`` (1061 / 1063 / 1071 — ``error_codes.ORDER_NOT_OPEN``). Batch
  cancel (``parse_cancel_orders``) returns a failure as an element with status ``failed`` and an id — the SDK would take it as
  canceled: ``has['cancelOrders']`` is disabled, cancels go one by one.
- **Open orders**: Current unfilled order acquisition — ``page_size`` «default 20，no more than 50»,
  ``page_index``; ccxt fetches one page (20 orders). Without ``contract_code`` cross returns «all contract code in
  cross mode» (ccxt requires a symbol). Pages up to ``total_page``.
- **Margin mode**: each endpoint «only supports cross margin mode» / «isolated margin mode»; ccxt without
  ``marginMode`` goes to cross. The symbol's mode comes from the SDK (``bytx_margin``: default and per-symbol mode).
- **Order**: the order ``fee`` has the exchange's sign (charge < 0: in the orders_cross push ``fee`` = sum of
  ``trade[].trade_fee``), ccxt flips it for a trade (fee > 0) but not for an order; status 11 «Orders
  cancelling» ccxt returns as 'canceling' — the order is still in the book, it's 'open'. Empty ``data`` from Get Information of
  order (finished orders «will be deleted after the settlement of funding rate») — OrderNotFound, ccxt
  failed with TypeError.
- **Order batch**: a Place A Batch Of Orders element failure — ``{index, err_code, err_msg}`` without id: the code
  is put into ``info.code`` / ``info.msg`` (read by the SDK's batch parsing).
- **WS pong**: Order Push Heartbeat — «"ts" value in the return "pong" message is the "ts" value from "ping"
  push» (string); ccxt answered with a number.
"""
from __future__ import annotations

import json
from typing import Any, Optional

import ccxt

TP_SL_PRICE_TYPE = 'market'
PAGE_SIZE = 50        # «page size, default 20，no more than 50»
MAX_PAGES = 50


def _trigger(value: Any) -> Any:
    return value.get('triggerPrice') if isinstance(value, dict) else value


def _fix_contract_min_amount(exchange: Any) -> None:
    original = exchange.fetch_markets_by_type_and_sub_type

    async def fetch_markets_by_type_and_sub_type(type, subType, params={}):
        markets = await original(type, subType, params)
        for m in markets or []:
            if m.get('contract'):
                amount = m.setdefault('limits', {}).setdefault('amount', {})
                amount['min'] = 1.0       # volume — integer number of contracts
        return markets

    exchange.fetch_markets_by_type_and_sub_type = fetch_markets_by_type_and_sub_type


def _fix_contract_request(exchange: Any) -> None:
    original = exchange.create_contract_order_request

    def create_contract_order_request(symbol, type, side, amount, price=None, params={}):
        p = dict(params or {})
        attached = {key: _trigger(p.pop(key, None)) for key in ('takeProfit', 'stopLoss')}
        hedged = bool(p.get('hedged'))
        request = original(symbol, type, side, amount, price, p)
        if hedged:
            request.pop('reduce_only', None)          # «in hedge mode it is invalid» — close via offset
        for key, prefix in (('takeProfit', 'tp'), ('stopLoss', 'sl')):
            if attached[key] is not None:
                request[f'{prefix}_trigger_price'] = exchange.price_to_precision(symbol, attached[key])
                request[f'{prefix}_order_price_type'] = TP_SL_PRICE_TYPE
        return request

    exchange.create_contract_order_request = create_contract_order_request


# ─── contracts: margin mode, open orders, cancel, order parsing ─────────────

def margin_mode_of(exchange: Any, symbol: Optional[str]) -> Optional[str]:
    """Symbol margin mode from the SDK (``exchange.bytx_margin()`` → (default, {symbol: mode}))."""
    getter = getattr(exchange, 'bytx_margin', None)
    if getter is None:
        return None
    default, overrides = getter()
    return overrides.get(symbol, default) if symbol is not None else default


def _all_modes(exchange: Any) -> list:
    getter = getattr(exchange, 'bytx_margin', None)
    if getter is None:
        return [None]
    default, overrides = getter()
    modes = [m for m in dict.fromkeys([default, *overrides.values()]) if m is not None]
    return modes or [None]


def _with_mode(exchange: Any, symbol: Optional[str], params: Any) -> dict:
    p = dict(params or {})
    if 'marginMode' not in p:
        mode = margin_mode_of(exchange, symbol)
        if mode is not None:
            p['marginMode'] = mode
    return p


def _fix_margin_mode(exchange: Any) -> None:
    """Symbol requests go to the endpoint of its margin mode (ccxt without marginMode always uses cross)."""
    def by_id(name: str) -> None:            # (id, symbol, params)
        original = getattr(exchange, name)

        async def call(id, symbol=None, params={}):
            return await original(id, symbol, _with_mode(exchange, symbol, params))
        setattr(exchange, name, call)

    def by_symbol(name: str) -> None:        # (symbol, since, limit, params)
        original = getattr(exchange, name)

        async def call(symbol=None, since=None, limit=None, params={}):
            return await original(symbol, since, limit, _with_mode(exchange, symbol, params))
        setattr(exchange, name, call)

    for name in ('fetch_order', 'cancel_order'):
        by_id(name)
    for name in ('fetch_orders', 'fetch_closed_orders', 'fetch_my_trades'):
        by_symbol(name)

    cancel_all = exchange.cancel_all_orders

    async def cancel_all_orders(symbol=None, params={}):
        return await cancel_all(symbol, _with_mode(exchange, symbol, params))
    exchange.cancel_all_orders = cancel_all_orders

    positions = exchange.fetch_positions

    async def fetch_positions(symbols=None, params={}):
        if 'marginMode' in (params or {}):
            return await positions(symbols, params)
        groups: dict = {}
        if symbols:
            for s in symbols:
                groups.setdefault(margin_mode_of(exchange, exchange.symbol(s)), []).append(s)
        else:
            groups = {mode: None for mode in _all_modes(exchange)}
        out: list = []
        for mode, group in groups.items():
            p = dict(params or {}, marginMode=mode) if mode is not None else dict(params or {})
            out.extend(await positions(group, p))
        return out
    exchange.fetch_positions = fetch_positions

    for name, key in (('watch_orders', 'margin'), ('watch_my_trades', 'margin')):
        if hasattr(exchange, name):
            _wrap_watch(exchange, name, key)
    if hasattr(exchange, 'watch_positions'):
        watch_positions = exchange.watch_positions

        async def watch_positions_mode(symbols=None, since=None, limit=None, params={}):
            p = dict(params or {})
            mode = margin_mode_of(exchange, exchange.symbol(symbols[0]) if symbols else None)
            if mode is not None:
                p.setdefault('marginMode', mode)
            return await watch_positions(symbols, since, limit, p)
        exchange.watch_positions = watch_positions_mode


def _wrap_watch(exchange: Any, name: str, key: str) -> None:
    original = getattr(exchange, name)

    async def call(symbol=None, since=None, limit=None, params={}):
        p = dict(params or {})
        mode = margin_mode_of(exchange, symbol)
        if mode is not None:
            p.setdefault(key, mode)    # ccxt.pro: orders / matchOrders — params['margin'] (cross by default)
        return await original(symbol, since, limit, p)
    setattr(exchange, name, call)


def _fix_open_orders(exchange: Any) -> None:
    """All pages of open orders (``page_index`` up to ``total_page``); without a symbol — all of cross."""
    original = exchange.fetch_open_orders

    async def fetch_open_orders(symbol=None, since=None, limit=None, params={}):
        p = dict(params or {})
        if any(p.get(k) for k in ('stop', 'trigger', 'stopLossTakeProfit', 'trailing')):
            return await original(symbol, since, limit, params)
        await exchange.load_markets()
        market = exchange.market(symbol) if symbol is not None else None
        if market is not None and not market.get('linear'):
            return await original(symbol, since, limit, params)
        mode = p.pop('marginMode', None) or margin_mode_of(exchange, market['symbol'] if market else None) or 'cross'
        if mode == 'isolated':
            if market is None:
                return await original(symbol, since, limit, params)   # isolated: contract_code required
            fn = exchange.contractPrivatePostLinearSwapApiV1SwapOpenorders
        else:
            fn = exchange.contractPrivatePostLinearSwapApiV1SwapCrossOpenorders
        rows: list = []
        for page in range(1, MAX_PAGES + 1):
            request = dict(p, page_index=page, page_size=PAGE_SIZE)
            if market is not None:
                request['contract_code'] = market['id']
            response = await fn(request)
            data = response.get('data') if isinstance(response, dict) else None
            data = data if isinstance(data, dict) else {}
            rows.extend(r for r in data.get('orders') or [] if isinstance(r, dict))
            if page >= int(data.get('total_page') or 1):
                break
        return exchange.parse_orders(rows, market, since, limit)

    exchange.fetch_open_orders = fetch_open_orders


def _cancel_rejection(exchange: Any, response: Any, keys: set) -> Optional[dict]:
    """Cancel failure for our order from ``data.errors`` (``status: ok`` response), if the id is not in ``successes``."""
    data = response.get('data') if isinstance(response, dict) else None
    if not isinstance(data, dict):
        return None
    errors = [e for e in data.get('errors') or [] if isinstance(e, dict)]
    successes = {x.strip() for x in str(data.get('successes') or '').split(',') if x.strip()}
    if not errors or keys & successes:
        return None
    mine = [e for e in errors if str(e.get('order_id')) in keys]
    if mine:
        return mine[0]
    return errors[0] if not successes and len(errors) == 1 else None


def _raise_rejection(exchange: Any, error: dict) -> None:
    body = json.dumps({'status': 'error', 'err_code': error.get('err_code'), 'err_msg': error.get('err_msg')})
    feedback = f'{exchange.id} {body}'
    exchange.throw_exactly_matched_exception(exchange.exceptions['exact'], str(error.get('err_code')), feedback)
    raise ccxt.ExchangeError(feedback)


def _fix_cancel(exchange: Any) -> None:
    original = exchange.cancel_order

    async def cancel_order(id, symbol=None, params={}):
        p = dict(params or {})
        keys = {str(k) for k in (id, p.get('client_order_id'), p.get('clientOrderId')) if k not in (None, '')}
        result = await original(id, symbol, params)
        rejected = _cancel_rejection(exchange, (result or {}).get('info'), keys)
        if rejected is not None:
            _raise_rejection(exchange, rejected)
        return result

    exchange.cancel_order = cancel_order
    # batch cancel: a failure is an element with status 'failed' and an id; the SDK cancels one by one
    exchange.has = dict(exchange.has, cancelOrders=False)


def _fix_order_info_empty(exchange: Any) -> None:
    for name in ('contractPrivatePostLinearSwapApiV1SwapCrossOrderInfo',
                 'contractPrivatePostLinearSwapApiV1SwapOrderInfo'):
        original = getattr(exchange, name)

        def make(original=original):
            async def call(params={}):
                response = await original(params)
                if isinstance(response, dict) and response.get('data') in ([], None) \
                        and response.get('status') == 'ok':
                    raise ccxt.OrderNotFound(f'{exchange.id} {json.dumps(response)}')
                return response
            return call
        setattr(exchange, name, make())


def _contract_order(order: Any) -> bool:
    return isinstance(order, dict) and 'contract_code' in order and 'order_price_type' in order


def _fix_order_parse(exchange: Any) -> None:
    """Contract order fee — fee > 0 (as for a trade), status 11 «cancelling» — 'open'."""
    def wrap(name: str) -> None:
        original = getattr(exchange, name)

        def parse(order, market=None):
            result = original(order, market)
            if not _contract_order(order) or not isinstance(result, dict):
                return result
            if str(order.get('status')) == '11':
                result['status'] = 'open'
            fees = [result.get('fee'), *(result.get('fees') or [])]
            for fee in {id(f): f for f in fees if isinstance(f, dict)}.values():
                if fee.get('cost') is not None:
                    fee['cost'] = exchange.parse_number(ccxt.Precise.string_neg(str(fee['cost'])))
            return result
        setattr(exchange, name, parse)

    wrap('parse_order')
    if hasattr(exchange, 'parse_ws_order'):
        wrap('parse_ws_order')


def _fix_batch_rejections(exchange: Any) -> None:
    original = exchange.create_orders

    async def create_orders(orders, params={}):
        result = await original(orders, params)
        for row in result or []:
            info = row.get('info') if isinstance(row, dict) else None
            if isinstance(info, dict) and not row.get('id') and info.get('err_code') is not None:
                row['info'] = dict(info, code=info['err_code'], msg=info.get('err_msg'))
        return sorted(result or [], key=lambda r: (bool(r.get('id')) is False,
                                                   int((r.get('info') or {}).get('index') or 0)))

    exchange.create_orders = create_orders


def _fix_pong(exchange: Any) -> None:
    original = exchange.pong

    async def pong(client, message):
        if isinstance(message, dict) and message.get('op') == 'ping' and 'ts' in message:
            try:
                await client.send({'op': 'pong', 'ts': message['ts']})
            except Exception as e:
                client.reset(ccxt.NetworkError(f'{exchange.id} pong failed {e}'))
            return None
        return await original(client, message)

    exchange.pong = pong


def apply(exchange: Any) -> None:
    _fix_contract_min_amount(exchange)
    _fix_contract_request(exchange)
    if hasattr(exchange, 'pong'):
        _fix_pong(exchange)
    if (exchange.options or {}).get('defaultType', 'spot') == 'spot':
        return
    _fix_order_parse(exchange)
    _fix_order_info_empty(exchange)
    _fix_margin_mode(exchange)       # before _fix_open_orders / _fix_cancel: they wrap with the mode already set
    _fix_open_orders(exchange)
    _fix_cancel(exchange)
    _fix_batch_rejections(exchange)
