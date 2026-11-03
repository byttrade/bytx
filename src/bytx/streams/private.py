"""Private WS streams (orders, fills, positions, balance) and WS order routing.

A private stream always subscribes to ALL symbols (one ccxt subscription per account);
symbol filtering happens at the consumer — several strategies share one connection.

WS orders and broker tags. Our broker patches (:mod:`bytx.net._sig.headers`)
sit on the ccxt REST path (``sign`` / ``request`` / ``create_orders``). ccxt 4.4.65 WS methods
bypass them, so WS is used only where attribution is preserved:

- okx — ``create_order_ws`` builds the request via ``create_order_request``; we put our ``tag``
  (OKX_BROKER_CODE) into params ourselves (ccxt also sends ``tag`` over WS);
- bybit — WS has no ``Referer`` header: own BYBIT_BROKER_ID set → REST;
- binance — the own-ID cid prefix is added by the REST ``request`` wrapper: when active → REST;
- hyperliquid — builder fee is added by the REST ``create_orders`` wrapper: when set → REST;
- bingx, kucoin, bitget — ccxt has no WS orders → REST;
- gate — WS order ``req_header`` with X-Gate-Channel-Id (WS order_place doc example): the ``watch`` wrapper
  sets our GATE_CHANNEL_ID instead of 'ccxt' (or removes it) — attribution preserved, WS.
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

from bytx import convert
from bytx.errors import NotSupported
from bytx.streams.supervisor import Feed, FeedSpec
from bytx.types import Position

# ccxt capability for a WS order operation.
WS_CAPABILITY = {'create': 'createOrderWs', 'edit': 'editOrderWs', 'cancel': 'cancelOrderWs'}


def orders(hub: Any) -> FeedSpec:
    ex = hub.ccxt
    _require(ex, 'watchOrders', hub.name)
    exchange = hub.exchange

    if hub.name == 'okx':
        # OKX: conditional orders (TP/SL) — a separate orders-algo channel on the business connection,
        # ccxt has its own messageHash and cache for it (demo 2026-10-29: 0 TP/SL events in watch_orders)
        watch = _merged_watch(lambda: ex.watch_orders(), lambda: ex.watch_orders(params={'trigger': True}))
    else:
        def watch() -> Any:
            return ex.watch_orders()

    def conv(raw: Any, feed: Feed) -> list:
        return [exchange._order(o) for o in _logical_cids(ex, raw)]

    return FeedSpec(kind='orders', key=('orders',), symbol=None, watch=watch, convert=conv,
                    policy='queue', timestamp=_order_ts)


def _merged_watch(*factories: Any) -> Any:
    """One ``watch()`` over several ccxt subscriptions: returns the first batch that arrives.

    Unclaimed waits stay until the next call. Each is under ``asyncio.shield``:
    consumer cancellation (stream stop) does not cancel the shared ccxt future that
    other subscribers of the same messageHash may be waiting on. The error of an abandoned wait (ccxt closed on
    stop) is retrieved, otherwise asyncio logs "Future exception was never retrieved"
    (demo e2e OKX 2026-10-30).
    """
    import asyncio
    pending: dict[int, asyncio.Future] = {}

    def retrieve(fut: asyncio.Future) -> None:
        if not fut.cancelled():
            fut.exception()

    async def watch() -> Any:
        for i, make in enumerate(factories):
            if i not in pending:
                pending[i] = asyncio.ensure_future(make())
                pending[i].add_done_callback(retrieve)
        shields = [asyncio.shield(f) for f in pending.values()]
        for s in shields:
            s.add_done_callback(retrieve)
        done, _ = await asyncio.wait(shields, return_when=asyncio.FIRST_COMPLETED)
        for i, fut in list(pending.items()):
            if fut.done():
                del pending[i]
                return fut.result()      # exception — to the supervisor, as with a single subscription
        raise RuntimeError('merged watch: no completed subscription')  # unreachable

    return watch


def fills(hub: Any) -> FeedSpec:
    ex = hub.ccxt
    _require(ex, 'watchMyTrades', hub.name)
    exchange = hub.exchange

    def watch() -> Any:
        return ex.watch_my_trades()

    def conv(raw: Any, feed: Feed) -> list:
        return [convert.fill(t, exchange._market_or_none(t.get('symbol'))) for t in _logical_cids(ex, raw)]

    return FeedSpec(kind='fills', key=('fills',), symbol=None, watch=watch, convert=conv, policy='queue')


def positions(hub: Any, symbol: Optional[str] = None) -> FeedSpec:
    """Positions. A close arrives as a :class:`Position` with ``size == 0``.

    In one-way mode the exchange does not report the side of a closed position — then two
    closes arrive, ``long`` and ``short`` (see :func:`positions_or_flat`). In hedge mode
    such pairs (a zero record without a side, e.g. Binance ``ps=BOTH``) are not
    hedge positions: check :func:`flat_side` on ``raw`` (as PositionTracker does).

    ``watch_positions`` (all symbols) where available; otherwise ``watch_position(symbol)``
    (kucoinfutures) — then symbol is required.
    """
    ex = hub.ccxt
    exchange = hub.exchange

    def conv(raw: Any, feed: Feed) -> list:
        rows = raw if isinstance(raw, list) else [raw] if raw else []
        result = []
        for p in rows:
            result.extend(positions_or_flat(p, exchange._market_or_none(p.get('symbol'))))
        return result

    if ex.has.get('watchPositions'):
        def watch() -> Any:
            return ex.watch_positions()
        key: tuple = ('positions',)
    elif ex.has.get('watchPosition') and symbol:
        def watch() -> Any:
            return ex.watch_position(symbol)
        key = ('positions', symbol)
    else:
        raise NotSupported('watchPositions is not supported' +
                           (' (watch_position: pass symbol)' if ex.has.get('watchPosition') else ''),
                           exchange=hub.name)
    return FeedSpec(kind='positions', key=key, symbol=symbol if len(key) > 1 else None, watch=watch,
                    convert=conv, policy='queue')


def balance(hub: Any) -> FeedSpec:
    ex = hub.ccxt
    _require(ex, 'watchBalance', hub.name)

    if hub.name == 'bybit':
        # Bybit wallet sends only changed coins, and ccxt builds the balance from what was sent: coins
        # unchanged since subscribing are missing from the stream (demo 2026-10-30: REST USDC/BTC/ETH/USDT, stream —
        # USDT only; a bot on a USDC contract would not find its collateral). Base — REST snapshot at subscribe,
        # incoming coins are overlaid on top.
        base: dict = {}

        async def watch() -> Any:
            pending = asyncio.ensure_future(ex.watch_balance())   # subscribe before the snapshot
            if not base:
                try:
                    base.update(await ex.fetch_balance())
                except BaseException:
                    pending.cancel()
                    raise
            merged = _overlay_balance(base, await pending)
            base.clear()
            base.update(merged)
            return merged
    else:
        async def watch() -> Any:
            return await ex.watch_balance()

    def conv(raw: Any, feed: Feed) -> list:
        return [convert.balance(raw)] if raw else []

    return FeedSpec(kind='balance', key=('balance',), symbol=None, watch=watch, convert=conv,
                    policy='latest')


_BALANCE_MAPS = ('free', 'used', 'total')


def _overlay_balance(base: dict, update: dict) -> dict:
    """ccxt balance: coins from ``update`` over the ``base`` snapshot (other coins from the snapshot)."""
    merged = {k: (dict(v) if isinstance(v, dict) else v) for k, v in base.items()}
    for key in _BALANCE_MAPS:
        merged.setdefault(key, {})
    for code in (update.get('total') or {}):
        for key in _BALANCE_MAPS:
            merged[key][code] = (update.get(key) or {}).get(code)
        if isinstance(update.get(code), dict):
            merged[code] = dict(update[code])
    for key in ('info', 'timestamp', 'datetime'):
        if key in update:
            merged[key] = update[key]
    return merged


def _logical_cids(ccxt_ex: Any, raw: Any) -> list:
    """Binance: strip our broker prefix ``x-<ID>`` from the cid of WS order and trade events.

    On REST the ``request`` wrapper strips it; ccxt WS bypasses it, and without this the trackers and
    position journal would treat our orders as foreign. Prefixes — current and
    LEGACY of the instance config.
    """
    items = list(raw or [])
    if str(getattr(ccxt_ex, 'id', '')).lower() != 'binance':
        return items
    from bytx.exchanges.binance.broker_cid import exchange_binance_cid_config, strip_binance_ws_payload
    cfg = exchange_binance_cid_config(ccxt_ex)
    if cfg is not None:
        for item in items:
            strip_binance_ws_payload(item, cfg.known)
    return items


def flat_side(p: dict) -> Optional[str]:
    """Side of a zero position: ccxt ``side`` or the exchange hedge fields (:func:`convert.hedge_side`)."""
    side = p.get('side')
    if side in ('long', 'short'):
        return side
    return convert.hedge_side(p.get('info'))[0]


def _flat(p: dict, side: str) -> Position:
    return Position(symbol=p['symbol'], side=side, size=0.0, contracts=0.0,  # type: ignore[arg-type]
                    entry_price=convert._f(p.get('entryPrice')), mark_price=convert._f(p.get('markPrice')),
                    liquidation_price=None, leverage=convert._f(p.get('leverage')),
                    margin_mode=p.get('marginMode'), unrealized_pnl=convert._f(p.get('unrealizedPnl')),
                    realized_pnl=convert._f(p.get('realizedPnl')), notional=0.0, margin=0.0,
                    timestamp=p.get('timestamp'), raw=p)


def position_or_flat(p: dict, market: Any) -> Optional[Position]:
    """Like :func:`convert.position`, but a zero position with a known side → size=0.

    The zero position side is also taken from the exchange info (hedge). Unknown side
    (one-way) → None; for one-way closes see :func:`positions_or_flat`.
    """
    pos = convert.position(p, market)
    if pos is not None:
        return pos
    side = flat_side(p)
    if not p.get('symbol') or side is None:
        return None
    return _flat(p, side)


def positions_or_flat(p: dict, market: Any) -> list[Position]:
    """Position events from an exchange message: open — one, closed — with ``size == 0``.

    One-way: for a closed position the exchange does not report the side (Binance ``ps=BOTH``, OKX
    ``posSide=net``, Bybit ``side=""``, BingX ``positionSide=BOTH``) — then two
    closes, ``long`` and ``short``: the symbol is flat on either side.
    """
    pos = position_or_flat(p, market)
    if pos is not None:
        return [pos]
    if not p.get('symbol') or convert._f(p.get('contracts')) != 0:
        return []
    return [_flat(p, 'long'), _flat(p, 'short')]


def _order_ts(order: Any) -> Optional[int]:
    raw = getattr(order, 'raw', None) or {}
    return raw.get('lastUpdateTimestamp') or raw.get('lastTradeTimestamp') or getattr(order, 'timestamp', None)


def _require(ccxt_ex: Any, capability: str, name: str) -> None:
    if not ccxt_ex.has.get(capability):
        raise NotSupported(f'{capability} is not supported', exchange=name)


# ─── WS order routing ─────────────────────────────────────────────────────

def ws_order_route(ccxt_ex: Any, op: str) -> tuple[bool, str]:
    """(use WS, reason). ``op``: 'create' / 'edit' / 'cancel'."""
    capability = WS_CAPABILITY[op]
    if not ccxt_ex.has.get(capability):
        return False, f'{capability} is not in ccxt'
    ex_id = str(getattr(ccxt_ex, 'id', '')).lower()
    if ex_id == 'binance':
        from bytx.exchanges.binance.broker_cid import exchange_binance_cid_config
        cfg = exchange_binance_cid_config(ccxt_ex)
        if cfg is not None and cfg.active:
            return False, 'binance: the broker cid prefix is added only on REST'
    elif ex_id == 'bybit':
        if (ccxt_ex.options or {}).get('brokerId'):
            return False, 'bybit: the Referer header (broker id) exists only on REST'
    elif ex_id == 'hyperliquid':
        from bytx.exchanges.hyperliquid import builder as hl_builder
        if hl_builder.config_for_exchange(ccxt_ex) is not None:
            return False, 'hyperliquid: builder fee is added only on REST'
    return True, 'ws'


def ws_create_params(ccxt_ex: Any, params: dict) -> dict:
    """params for ``create_order_ws``: on OKX — our broker tag (as in the REST signature)."""
    from bytx.net._sig.headers import okx_broker_tag
    tag = okx_broker_tag(ccxt_ex)
    if tag:
        return {**params, 'tag': tag}
    return params


BUILDERS = {
    'orders': orders,
    'fills': fills,
    'positions': positions,
    'balance': balance,
}

__all__ = ['BUILDERS', 'orders', 'fills', 'positions', 'balance', 'position_or_flat', 'positions_or_flat', 'flat_side',
           'ws_order_route', 'ws_create_params', 'WS_CAPABILITY']
