"""ccxt 4.4.65 fixes for Gate (APIv4), based on official docs.

Reference — https://www.gate.com/docs/developers/apiv4/en/ (and the model spec of the official SDK
https://github.com/gateio/gateapi-python/tree/master/docs), fixtures ``tests/ground_truth/gate/``.

- **TestNet** (Access URL): REST ``https://api-testnet.gateapi.io/api/v4`` for spot and futures;
  WS — ``wss://ws-testnet.gate.com/v4/ws/spot`` and ``…/v4/ws/futures/usdt``. ccxt uses the dead
  ``fx-api-testnet.gateio.ws`` (502), no spot (KeyError 'spot') and old WS hosts.
- **Contract fees** (Contract): ``maker_fee_rate`` / ``taker_fee_rate`` are fractions («negative values indicate
  rebates»); ccxt divides them by 100 (−0.0001 → −1e-06).
- **text** (Order / FuturesOrder): «1. Prefixed with `t-` 2. No longer than 28 bytes without `t-` prefix» — ccxt
  doesn't prefix an id that starts with «t» (``test1`` is sent as is — the exchange rejects it), and counts 28 including
  ``t-`` (rejects ``t-`` + 28 chars itself).
- **Order status** (FuturesOrder): ``status`` «open: Pending - finished: Completed», ``finish_as`` «filled: all
  filled - cancelled … - liquidated: cancelled because of liquidation - ioc … - auto_deleveraged: finished by ADL -
  reduce_only: cancelled because of increasing position while reduce-only set - position_closed … - reduce_out … -
  stp: cancelled because self trade prevention», ``left`` «Unfilled quantity». ccxt maps only part of
  ``finish_as`` (stp / reduce_only / position_closed / reduce_out pass through raw, liquidated — 'closed'), and for a
  price-triggered order (FuturesPriceTriggeredOrder: finish_as empty while active) the status is ''. Rule: filled —
  closed; finished with ``left`` 0 — closed, otherwise canceled (filled = size − left); price order — per
  :func:`bytx.exchanges.gate.tp_sl.state`.
- **Cancel by text** (cancel_futures_order / Spot cancel_order: ``order_id`` — «Order ID returned, or user custom
  ID (i.e., text field)»): ccxt ``cancel_order`` doesn't read the ``clientOrderId`` key — it sends DELETE …/orders/None
  with a stray ``clientOrderId`` in the query. Without an id — text goes into the path.
- **user id in WS** (Futures WebSocket, futures.orders / usertrades / positions: parameter «user id — User id»):
  ccxt requires ``uid`` (ArgumentsRequired), the SDK doesn't set it — taken from GET /account/detail (``user_id``).
"""
from __future__ import annotations

from typing import Any, Optional

TESTNET_REST = 'https://api-testnet.gateapi.io/api/v4'
TESTNET_WS = {'spot': 'wss://ws-testnet.gate.com/v4/ws/spot',
              'swap_usdt': 'wss://ws-testnet.gate.com/v4/ws/futures/usdt'}


def _fix_testnet(exchange: Any) -> None:
    if not getattr(exchange, 'isSandboxModeEnabled', False):
        return
    backup = exchange.urls.get('apiBackup')
    if not isinstance(backup, dict):
        return
    api = exchange.clone(backup)
    for scope in ('public', 'private'):
        if isinstance(api.get(scope), dict):
            api[scope] = {key: TESTNET_REST for key in api[scope]}
    if 'spot' in api:
        api['spot'] = TESTNET_WS['spot']
    swap = dict(api.get('swap') or {})
    swap['usdt'] = TESTNET_WS['swap_usdt']
    api['swap'] = swap
    test = exchange.urls.get('test') or {}
    for key in ('future', 'option'):        # delivery/options: no testnet WS in docs — left as ccxt has them
        if key in test:
            api[key] = exchange.clone(test[key])
    exchange.urls['api'] = api
    _testnet_markets(exchange)


def _testnet_markets(exchange: Any) -> None:
    """TestNet markets: spot and USDT-M. ccxt in sandbox doesn't load spot, and /futures/btc/contracts on TestNet is
    NOT_FOUND (live check 2026-10-26: load_markets failed entirely); the SDK doesn't need delivery and options."""
    import asyncio

    async def fetch_markets(params={}):
        spot, contracts = await asyncio.gather(exchange.fetch_spot_markets(params),
                                               exchange.publicFuturesGetSettleContracts({'settle': 'usdt'}))
        return list(spot) + [exchange.parse_contract_market(r, 'usdt') for r in contracts or []]

    exchange.fetch_markets = fetch_markets


def _fix_contract_fees(exchange: Any) -> None:
    original = exchange.parse_contract_market

    def parse_contract_market(market: dict, settle_id: str) -> dict:
        result = original(market, settle_id)
        for key, field in (('maker', 'maker_fee_rate'), ('taker', 'taker_fee_rate')):
            value = exchange.safe_number(market, field)
            if value is not None:
                result[key] = value
        return result

    exchange.parse_contract_market = parse_contract_market


TEXT_PREFIX = 't-'


def normalize_text(value: str) -> str:
    """Own id → text per docs: the ``t-`` prefix exactly once."""
    return value if value.startswith(TEXT_PREFIX) else TEXT_PREFIX + value


def _fix_text(exchange: Any) -> None:
    original = exchange.create_order_request

    def create_order_request(symbol, type, side, amount, price=None, params={}):
        cid = exchange.safe_string_2(params, 'text', 'clientOrderId')
        if cid is None:
            return original(symbol, type, side, amount, price, params)
        request = original(symbol, type, side, amount, price, exchange.omit(params, ['text', 'clientOrderId']))
        target = request.get('initial') if isinstance(request.get('initial'), dict) else request
        target['text'] = normalize_text(cid)
        return request

    exchange.create_order_request = create_order_request


def order_status(order: dict) -> Optional[str]:
    """SDK status from a raw Gate order (regular or price-triggered); None — keep the ccxt value."""
    if isinstance(order.get('trigger'), dict) and isinstance(order.get('initial'), dict):
        from bytx.exchanges.gate.tp_sl import state
        return {'open': 'open', 'triggered': 'closed', 'canceled': 'canceled'}.get(state(order) or '')
    finish_as = str(order.get('finish_as') or '').lower()
    status = str(order.get('status') or '').lower()
    if finish_as in ('open', '_new') or (not finish_as and status == 'open'):
        return 'open'
    if finish_as == 'filled':
        return 'closed'
    if not finish_as and status not in ('finished', 'closed', 'cancelled'):
        return None
    left = order.get('left')
    if left in (None, ''):
        return None         # not FuturesOrder / Order (spot price order — put/trigger): keep the ccxt value
    try:
        done = float(left) == 0
    except (TypeError, ValueError):
        return None
    return 'closed' if done else 'canceled'


def _fix_order_status(exchange: Any) -> None:
    original = exchange.parse_order

    def parse_order(order: dict, market: Any = None) -> dict:
        result = original(order, market)
        if isinstance(order, dict) and isinstance(result, dict) and result.get('status') != 'rejected':
            status = order_status(order)
            if status is not None:
                result['status'] = status
        return result

    exchange.parse_order = parse_order


def _fix_cancel_by_text(exchange: Any) -> None:
    original = exchange.cancel_order

    async def cancel_order(id, symbol=None, params={}):
        cid = exchange.safe_string_2(params, 'text', 'clientOrderId')
        if id is None and cid is not None:
            id = normalize_text(cid)
            params = exchange.omit(params, ['text', 'clientOrderId'])
        return await original(id, symbol, params)

    exchange.cancel_order = cancel_order


def _fix_ws_uid(exchange: Any) -> None:
    original = getattr(exchange, 'subscribe_private', None)
    if original is None:
        return

    async def subscribe_private(url, messageHash, payload, channel, params, requiresUid=False):
        if requiresUid and not exchange.uid:
            detail = await exchange.privateAccountGetDetail()
            uid = detail.get('user_id') if isinstance(detail, dict) else None
            if uid not in (None, ''):
                exchange.uid = str(uid)
        return await original(url, messageHash, payload, channel, params, requiresUid)

    exchange.subscribe_private = subscribe_private


def apply(exchange: Any) -> None:
    _fix_testnet(exchange)
    _fix_contract_fees(exchange)
    _fix_text(exchange)
    _fix_order_status(exchange)
    _fix_cancel_by_text(exchange)
    _fix_ws_uid(exchange)
