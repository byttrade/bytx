"""Position protection: TP/SL, dead-man switch, client-side risk guard.

    orders = await ex.set_tp_sl('BTC/USDT', take_profit=70_000, stop_loss=60_000)
    await ex.cancel_tp_sl('BTC/USDT')

    async with ex.dead_man_switch(seconds=60):   # the exchange cancels orders if the process dies
        ...

    ex = create_exchange('okx', ..., risk=RiskGuard(max_position_notional=5_000,
                                                    max_daily_loss=200, max_orders_per_minute=60))

TP/SL here are separate reduce-only trigger orders on the CURRENT position (not
attached to the entry order). Per-exchange mechanics
(no in-process state):

- Binance USDⓈ-M: Algo Order API ``/fapi/v1/algoOrder`` (algoType=CONDITIONAL,
  ``clientAlgoId``) — since 2025-12-09 conditional orders are accepted only there;
  the transport adds the ``clientAlgoId`` broker prefix (``exchanges.binance.broker_cid``);
- Bybit: position TP/SL (``/v5/position/trading-stop``), one request per leg;
- OKX: algo ``conditional``, TP and SL in one request, ``tag`` with our broker code;
- BingX: ``TAKE_PROFIT_MARKET`` / ``STOP_MARKET`` via create_order with ``stopPrice``, without
  ``clientOrderID`` (Place order: «clientOrderId only supports LIMIT/MARKET order type»);
- Hyperliquid: two reduce-only trigger orders (tpsl), market execution;
- KuCoin Futures: stop orders (``takeProfitPrice`` / ``stopLossPrice``, reduce-only).
"""
from __future__ import annotations

import asyncio
import json
import math
import re
import time
from collections import deque
from dataclasses import dataclass, field, fields
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

import ccxt
from loguru import logger

from bytx import convert, errors as _errors, outcome
from bytx.errors import (
    BytxError,
    ConfigError,
    ExchangeError,
    InvalidOrder,
    NetworkError,
    NotSupported,
    OrderNotFound,
    OrderStatusUnknown,
    ProtectionNotPlaced,
    RateLimited,
    RequestTimeout,
    TimestampError,
)
from bytx.exchanges import tp_sl_fill_order_id, tp_sl_state, tp_sl_trigger_by
from bytx.markets import to_market
from bytx.order_params import check_client_id, client_id_params, generate_client_id, okx_tag_params
from bytx.types import Market, Order, OrderStatus, Position, PositionSide

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange

TriggerBy = Literal['mark', 'last']
TpSlKind = Literal['take_profit', 'stop_loss', 'tp_sl']

_TRIGGER_BY = ('mark', 'last')
_KINDS = ('take_profit', 'stop_loss')


@dataclass(slots=True)
class TpSlOrder(Order):
    """Position protection order (TP, SL or both at once — OKX algo).

    ``kind``: 'take_profit' / 'stop_loss' / 'tp_sl'; ``position_side`` — which
    position it closes ('long' / 'short'); ``take_profit`` / ``stop_loss`` —
    trigger prices. Amounts, as in :class:`Order`, are in base coin.

    ``has_client_id`` — False if the client_id is never sent to the exchange:
    Bybit places TP/SL on the position (``/v5/position/trading-stop``, the request has no cid field); BingX
    accepts ``clientOrderID`` only for LIMIT/MARKET (Place order, docs-v3), not for
    ``TAKE_PROFIT_MARKET`` / ``STOP_MARKET``. Such an order cannot be found by client_id — only by the id from
    the response, kind, side and trigger price.

    ``state`` — normalized state across all 6 exchanges: 'open' — waiting for
    the trigger, 'triggered' — fired, 'canceled' — removed without firing (canceled, expired,
    rejected); None — not reported by the exchange. ``status`` remains the SDK order status.

    ``fill_order_id`` — id of the order the exchange used to fill a triggered TP/SL, when it is a DIFFERENT order
    (Binance actualOrderId, OKX ordIdList, Gate trade_id, MEXC placeOrderId, HTX relation_order_id):
    closing trades go under it, not under ``id``. None — not triggered, filled under the same id (Bybit,
    KuCoin, Hyperliquid) or the link is kept by the child order (BingX triggerOrderId).
    """

    kind: str = 'take_profit'
    position_side: Optional[str] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    has_client_id: bool = True
    state: Optional[str] = None
    fill_order_id: Optional[str] = None


_ORDER_FIELDS = tuple(f.name for f in fields(Order))


def _wrap(order: Order, kind: str, position_side: Optional[str],
          take_profit: Optional[float] = None, stop_loss: Optional[float] = None) -> TpSlOrder:
    data = {name: getattr(order, name) for name in _ORDER_FIELDS}
    return TpSlOrder(**data, kind=kind, position_side=position_side,
                     take_profit=take_profit, stop_loss=stop_loss)


def _with_state(order: TpSlOrder, exchange_id: str, info: Any) -> TpSlOrder:
    order.state = tp_sl_state(exchange_id, info, order.status)
    order.fill_order_id = tp_sl_fill_order_id(exchange_id, info)
    return order


def _close_side(position_side: str) -> str:
    return 'sell' if position_side == 'long' else 'buy'


# ═══ TP/SL ═══════════════════════════════════════════════════════════════════

@dataclass(slots=True)
class _Ctx:
    """Everything an exchange implementation needs for a single TP/SL placement."""

    ex: 'Exchange'
    symbol: str
    market: Market
    position_side: str
    contracts: float
    partial: bool
    hedge: bool
    trigger_by: str
    margin_mode: str
    # already placed orders and all sent client_ids — for ProtectionNotPlaced
    placed: list = field(default_factory=list)
    client_ids: list = field(default_factory=list)
    # reference price of the main order (HL: trigger order slippage) — without another request
    ref_price: Optional[float] = None
    # caller's client_id by kind ('take_profit' / 'stop_loss')
    explicit: dict = field(default_factory=dict)

    @property
    def c(self) -> Any:
        return self.ex._ex

    @property
    def close_side(self) -> str:
        return _close_side(self.position_side)

    def price(self, value: float) -> str:
        return self.c.price_to_precision(self.symbol, value)

    def amount(self) -> str:
        return self.c.amount_to_precision(self.symbol, self.contracts)

    def client_id(self, kind: Optional[str] = None) -> str:
        """Leg client_id: the one given by the caller for ``kind`` (unchanged), otherwise
        a generated one — with the bot tag if the client has one."""
        cid = self.explicit.get(kind) if kind else None
        if not cid:
            if getattr(self.ex, 'journal', None) is not None:
                # with a journal — a next-generation client_id (role tp/sl + level from bind)
                cid = self.ex._new_client_id(role={'take_profit': 'tp', 'stop_loss': 'sl'}.get(kind or ''))
            else:
                cid = generate_client_id(self.c, self.ex.market_type, getattr(self.ex, 'client_id_prefix', None))
        self.client_ids.append(cid)
        return cid

    async def pre_submit(self, kind: str, cid: Optional[str], trigger: Optional[float], params: dict) -> None:
        """Exchange.pre_submit hook before the request for one TP/SL leg."""
        await self.ex._pre_submit({'symbol': self.symbol, 'type': 'market', 'side': self.close_side,
                                   'amount': self.contracts, 'price': None, 'client_id': cid,
                                   'reduce_only': True, 'params': params},
                                  kind, trigger_price=trigger, trigger_by=self.trigger_by)

    def cid_params(self, cid: str) -> dict:
        return client_id_params(self.c, cid, market_type=self.ex.market_type,
                                strict=getattr(self.ex, 'client_id_strict', False))


def _explicit_client_ids(ex: 'Exchange', take_profit: Optional[float], stop_loss: Optional[float],
                         take_profit_client_id: Optional[str],
                         stop_loss_client_id: Optional[str]) -> dict:
    """Validates the caller's TP/SL client_ids before any request.

    Exchange format (and with ``client_id_strict`` — length with the Binance broker prefix) —
    InvalidOrder; one id for both TP and SL — ConfigError (each leg is its own order, and exchanges
    reject a repeated id: HL cloid, KuCoin clientOid); an id without its price —
    ConfigError.
    """
    out = {}
    for kind, price, cid in (('take_profit', take_profit, take_profit_client_id),
                             ('stop_loss', stop_loss, stop_loss_client_id)):
        if cid is None:
            continue
        if price is None:
            raise ConfigError(f'{kind}_client_id without {kind}', exchange=ex.name, details={'client_id': cid})
        check_client_id(ex._ex, cid, ex.market_type, strict=getattr(ex, 'client_id_strict', False))
        out[kind] = cid
    if len(out) == 2 and out['take_profit'] == out['stop_loss']:
        raise ConfigError('take_profit_client_id and stop_loss_client_id must differ: '
                          'TP and SL are separate orders', exchange=ex.name,
                          details={'client_id': out['take_profit']})
    return out


async def _pick_position(ex: 'Exchange', s: str, side: Optional[str]) -> Position:
    if side is not None and side not in ('long', 'short'):
        raise ConfigError(f"side: 'long' or 'short' (position side), not {side!r} ({s})",
                          exchange=ex.name, details={'symbol': s, 'side': side})
    found = [p for p in await ex.positions([s]) if p.symbol == s and (side is None or p.side == side)]
    if not found:
        raise InvalidOrder(f'no open position {s}' + (f' {side}' if side else ''), exchange=ex.name)
    if len(found) > 1:
        raise ConfigError(f'{s} has both long and short open (hedge): specify side', exchange=ex.name)
    return found[0]


def _check_prices(side: str, take_profit: Optional[float], stop_loss: Optional[float],
                  exchange: str) -> None:
    for name, value in (('take_profit', take_profit), ('stop_loss', stop_loss)):
        if value is not None and not (value > 0 and math.isfinite(value)):
            raise ConfigError(f'{name} must be > 0', exchange=exchange)
    if take_profit is not None and stop_loss is not None:
        wrong = take_profit <= stop_loss if side == 'long' else take_profit >= stop_loss
        if wrong:
            rel = 'above' if side == 'long' else 'below'
            raise InvalidOrder(f'for a {side} position take_profit must be {rel} stop_loss',
                               exchange=exchange)


# margin mode goes into the TP/SL request: OKX tdMode, KuCoin marginMode
_MARGIN_IN_REQUEST = ('okx', 'kucoinfutures')


async def set_tp_sl(ex: 'Exchange', symbol: str, *, take_profit: Optional[float] = None,
                    stop_loss: Optional[float] = None, side: Optional[PositionSide] = None,
                    size: Optional[float] = None, trigger_by: Optional[TriggerBy] = None,
                    take_profit_client_id: Optional[str] = None,
                    stop_loss_client_id: Optional[str] = None,
                    contracts: Optional[float] = None, position_side: Optional[PositionSide] = None,
                    pair_client_id: Optional[str] = None) -> list[TpSlOrder]:
    """Places reduce-only TP and/or SL on the current position. See :meth:`Exchange.set_tp_sl`.

    ``pair_client_id`` — internal (modify_tp_sl): id of a TP+SL pair as one OKX algo order.
    """
    if position_side is not None:
        if position_side not in ('long', 'short'):
            raise ConfigError(f"position_side: 'long' or 'short', not {position_side!r} ({symbol})",
                              exchange=ex.name, details={'symbol': symbol, 'position_side': position_side})
        if side is not None and side != position_side:
            raise ConfigError(f'side={side!r} and position_side={position_side!r} differ ({symbol})',
                              exchange=ex.name, details={'symbol': symbol})
    if size is not None and contracts is not None:
        raise ConfigError(f'at most one of size / contracts is allowed ({symbol})', exchange=ex.name,
                          details={'symbol': symbol, 'size': size, 'contracts': contracts})
    if take_profit is None and stop_loss is None:
        raise ConfigError(f'take_profit and/or stop_loss is required ({symbol})', exchange=ex.name,
                          details={'symbol': symbol})
    explicit = _explicit_client_ids(ex, take_profit, stop_loss, take_profit_client_id, stop_loss_client_id)
    if pair_client_id is not None:
        check_client_id(ex._ex, pair_client_id, ex.market_type, strict=getattr(ex, 'client_id_strict', False))
        explicit['tp_sl'] = pair_client_id
    if trigger_by is None:  # exchange default
        trigger_by = tp_sl_trigger_by(ex._ex.id)  # type: ignore[assignment]
    if trigger_by not in _TRIGGER_BY:
        raise ConfigError(f'trigger_by: {_TRIGGER_BY}, not {trigger_by!r} ({symbol})', exchange=ex.name,
                          details={'symbol': symbol, 'trigger_by': trigger_by})
    await ex.connect()
    if ex.market_type == 'spot':
        raise NotSupported('position TP/SL is for derivatives only', exchange=ex.name)
    s = ex.symbol(symbol)
    m = to_market(ex._ex.market(s), ex._ex.id)
    if position_side is not None and (size is not None or contracts is not None):
        # side and size come from the caller — the position is not read (multi-level TP
        # without races or extra requests). Size is not checked against the position: staying within it is enforced
        # by the exchange's reduce-only / positionSide; Bybit is placed in Partial mode (own size)
        pos_side: str = position_side
        pos_contracts: Optional[float] = None
        pos_margin: Optional[str] = None
    else:
        pos = await _pick_position(ex, s, side or position_side)
        pos_side, pos_contracts, pos_margin = pos.side or 'long', pos.contracts, pos.margin_mode
    _check_prices(pos_side, take_profit, stop_loss, ex.name)
    if size is None and contracts is None:
        contracts, partial = pos_contracts, False
    else:
        # contracts= — in exchange units (OKX/KuCoin contractSize ≠ 1), size= — base
        what, value = ('size', size) if size is not None else ('contracts', contracts)
        if value is None or value <= 0:
            raise ConfigError(f'{what} must be > 0, not {value} ({s})', exchange=ex.name,
                              details={'symbol': s, what: value})
        contracts = (convert.contracts_from_size(ex._ex, m, value) if what == 'size'
                     else convert.contracts_to_precision(ex._ex, m, value))
        if contracts <= 0:
            raise InvalidOrder(f'{what} {value} is below the {s} amount step', exchange=ex.name)
        if pos_contracts is None:
            partial = True
        else:
            if contracts > pos_contracts * (1 + 1e-9):
                raise InvalidOrder(f'{what} {value} exceeds the position of {pos_contracts} contracts ({s})',
                                   exchange=ex.name)
            contracts = min(contracts, pos_contracts)  # snapping to the lot does not exceed the position
            partial = not math.isclose(contracts, pos_contracts, rel_tol=1e-9)
    margin = pos_margin or ex.margin_mode_of(s)
    if margin is None and pos_contracts is None and ex._ex.id in _MARGIN_IN_REQUEST:
        raise ex._no_margin_mode(s)  # without a position there is nowhere to take the mode from — no silent cross
    hedge = await ex.is_hedge_mode(s)
    assert contracts is not None
    ctx = _Ctx(ex, s, m, pos_side, contracts, partial, hedge, trigger_by,
               (margin or 'cross').lower(), explicit=explicit)
    impl = _SETTERS.get(ex._ex.id)
    if impl is None:
        raise NotSupported('TP/SL is not supported', exchange=ex.name)
    return await _place(ctx, impl, take_profit, stop_loss, strict=False)


async def place_tp_sl(ex: 'Exchange', symbol: str, kind: str, trigger: float, *,
                      position_side: PositionSide, size: Optional[float] = None,
                      contracts: Optional[float] = None, client_id: Optional[str] = None,
                      trigger_by: Optional[TriggerBy] = None) -> TpSlOrder:
    """One TP or SL leg → one :class:`TpSlOrder` or an exception. See :meth:`Exchange.place_tp_sl`."""
    if kind not in _KINDS:
        raise ConfigError(f'kind: {_KINDS}, not {kind!r} ({symbol})', exchange=ex.name,
                          details={'symbol': symbol, 'kind': kind})
    tp = kind == 'take_profit'
    orders = await set_tp_sl(ex, symbol, take_profit=trigger if tp else None, stop_loss=None if tp else trigger,
                             size=size, contracts=contracts, trigger_by=trigger_by, position_side=position_side,
                             take_profit_client_id=client_id if tp else None,
                             stop_loss_client_id=None if tp else client_id)
    return orders[0]


def _tp_first(orders: list[TpSlOrder]) -> list[TpSlOrder]:
    # place SL before TP, but return in the usual order: TP, then SL
    return sorted(orders, key=lambda o: o.kind == 'stop_loss')


async def _place(ctx: _Ctx, impl: Any, take_profit: Optional[float], stop_loss: Optional[float], *,
                 strict: bool) -> list[TpSlOrder]:
    """Calls the exchange implementation; a failure after some orders does not lose the placed ones.

    If anything was placed (or ``strict`` — the main order is already placed) —
    :class:`ProtectionNotPlaced` with ``placed`` and ``client_ids``. Otherwise the original error,
    whose ``details`` (if empty) get the ``client_ids`` of the sent requests.
    """
    try:
        return _tp_first(await impl(ctx, take_profit, stop_loss))
    except BytxError as e:
        if _ambiguous(e):
            found = await _reconcile_legs(ctx, take_profit, stop_loss)
            if found is not None:
                return _tp_first(found)
            unknown = OrderStatusUnknown(
                f'TP/SL outcome unknown ({ctx.symbol} {ctx.position_side}): no response received, reconciliation '
                f'of open TP/SL did not find the leg — do not blindly place it again: {e.message or e}',
                exchange=ctx.ex.name, code=e.code, symbol=ctx.symbol, side=ctx.close_side, amount=ctx.contracts,
                client_order_id=(ctx.client_ids or [None])[-1],
                details={'order': None, 'placed': _tp_first(list(ctx.placed)), 'client_ids': list(ctx.client_ids)})
            raise unknown from e
        placed = _tp_first(list(ctx.placed))
        details = {'order': None, 'placed': placed, 'client_ids': list(ctx.client_ids)}
        if placed or strict:
            raise ProtectionNotPlaced(f'TP/SL only partially placed ({len(placed)} placed): '
                                      f'{e.message or e}', exchange=ctx.ex.name, code=e.code,
                                      details=details) from e
        if e.details is None:
            e.details = details
        raise


async def attach_tp_sl(ex: 'Exchange', s: str, *, position_side: PositionSide, contracts: float,
                       take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
                       ref_price: Optional[float] = None, take_profit_client_id: Optional[str] = None,
                       stop_loss_client_id: Optional[str] = None) -> list[TpSlOrder]:
    """TP/SL for a just-placed opening order (place_order on exchanges without
    attached TP/SL): reduce-only conditional orders for the order size, no position needed."""
    _check_prices(position_side, take_profit, stop_loss, ex.name)
    explicit = _explicit_client_ids(ex, take_profit, stop_loss, take_profit_client_id, stop_loss_client_id)
    impl = _SETTERS.get(ex._ex.id)
    if impl is None:
        raise NotSupported('TP/SL is not supported', exchange=ex.name)
    m = to_market(ex._ex.market(s), ex._ex.id)
    ctx = _Ctx(ex, s, m, position_side, contracts, True, await ex.is_hedge_mode(s),
               tp_sl_trigger_by(ex._ex.id),
               (ex.margin_mode_of(s) or 'cross').lower(), ref_price=ref_price, explicit=explicit)
    return await _place(ctx, impl, take_profit, stop_loss, strict=True)


# ─── Binance: Algo Order API ────────────────────────────────────────────────

# binance algoStatus → SDK order status
_BINANCE_ALGO_STATUS = {
    'NEW': 'open', 'WORKING': 'open',
    'TRIGGERED': 'closed', 'FINISHED': 'closed', 'FILLED': 'closed',
    'CANCELED': 'canceled', 'CANCELLED': 'canceled', 'EXPIRED': 'expired', 'REJECTED': 'rejected',
}
_BINANCE_TP_SL_TYPES = {'TAKE_PROFIT_MARKET': 'take_profit', 'STOP_MARKET': 'stop_loss'}
_BINANCE_WORKING_TYPE = {'mark': 'MARK_PRICE', 'last': 'CONTRACT_PRICE'}
_BINANCE_ALGO_CID_RE = re.compile(r'^[.A-Z:/a-z0-9_-]{1,36}$')
# A reject ON THE MERITS of a reduceOnly order: a bare retry would open an opposite position.
_ALGO_REDUCE_ONLY_DENY_CODES = ('-2022',)
_ERROR_CODE_RE = re.compile(r'"code"\s*:\s*(-?\d+)')
_ERROR_PARAM_RE = re.compile(r"[Pp]arameter\s+'([^']+)'")
# Latch on the ccxt instance: the endpoint already rejected reduceOnly and the request went through without it.
_NO_REDUCE_ONLY_ATTR = '_bytx_algo_no_reduce_only'


def _rejection_may_concern_reduce_only(error: Exception) -> bool:
    """Could an algoOrder reject be about unsupported reduceOnly (see tp_sl.py)."""
    text = str(error)
    code = _ERROR_CODE_RE.search(text)
    if code and code.group(1) in _ALGO_REDUCE_ONLY_DENY_CODES:
        return False
    param = _ERROR_PARAM_RE.search(text)
    if param and param.group(1).lower() != 'reduceonly':
        return False
    return True


async def _binance_algo_create(c: Any, req: dict) -> dict:
    """POST /fapi/v1/algoOrder with a single retry without reduceOnly.

    Retry only on BadRequest (a deterministic parameter reject): on
    timeouts/unavailability the exchange may have accepted the order, and a retry would create a duplicate.
    The latch is set on the FACT of the retry succeeding, not by the error code.
    """
    if getattr(c, _NO_REDUCE_ONLY_ATTR, False):
        req = {k: v for k, v in req.items() if k != 'reduceOnly'}
    try:
        return await c.request('algoOrder', 'fapiPrivate', 'POST', req)
    except ccxt.BadRequest as e:
        if 'reduceOnly' not in req or not _rejection_may_concern_reduce_only(e):
            raise
        retry = {k: v for k, v in req.items() if k != 'reduceOnly'}
        try:
            result = await c.request('algoOrder', 'fapiPrivate', 'POST', retry)
        except Exception:
            raise e from None
        setattr(c, _NO_REDUCE_ONLY_ATTR, True)
        logger.warning(f'binance algoOrder rejected reduceOnly ({e}); accepted without it — '
                       f'parameter disabled for this client')
        return result


def _binance_algo_order(data: dict, symbol: str, market: Optional[Market]) -> Optional[TpSlOrder]:
    kind = _BINANCE_TP_SL_TYPES.get(str(data.get('orderType') or data.get('type') or '').upper())
    if kind is None:
        return None
    trigger = convert._f(data.get('triggerPrice'))
    qty = convert._f(data.get('quantity'))
    cs = market.contract_size if market is not None else 1.0
    side = str(data.get('side') or '').lower() or None
    reduce_only = data.get('reduceOnly')
    if isinstance(reduce_only, str):
        reduce_only = reduce_only.lower() == 'true'
    return _with_state(TpSlOrder(
        id=str(data.get('algoId') or ''), client_id=data.get('clientAlgoId') or None,
        symbol=symbol, side=side, type='take_profit_market' if kind == 'take_profit' else 'stop_market',
        status=_BINANCE_ALGO_STATUS.get(str(data.get('algoStatus') or '').upper(), 'open'),
        price=None, average=None, size=None if qty is None else qty * cs, filled=None, remaining=None,
        reduce_only=True if reduce_only is None else bool(reduce_only), post_only=None,
        time_in_force=None, trigger_price=trigger, fee=None, fee_currency=None,
        timestamp=data.get('createTime') or data.get('updateTime'), raw=data, contracts=qty,
        kind=kind, position_side=_position_side_of(data, side),
        take_profit=trigger if kind == 'take_profit' else None,
        stop_loss=trigger if kind == 'stop_loss' else None,
    ), 'binance', data)


async def _set_binance(ctx: _Ctx, take_profit: Optional[float],
                       stop_loss: Optional[float]) -> list[TpSlOrder]:
    if ctx.hedge:
        # hedge: positionSide scopes the position; Binance rejects reduceOnly (-1106)
        scope: dict = {'positionSide': 'LONG' if ctx.position_side == 'long' else 'SHORT'}
    else:
        # one-way: without reduceOnly excess size would open an opposite position
        scope = {'reduceOnly': True}
    result = ctx.placed
    for kind, trigger in (('STOP_MARKET', stop_loss), ('TAKE_PROFIT_MARKET', take_profit)):
        if trigger is None:
            continue
        cid = ctx.client_id(_BINANCE_TP_SL_TYPES[kind])
        req = {
            'algoType': 'CONDITIONAL',
            'symbol': ctx.market.id,
            'side': ctx.close_side.upper(),
            'type': kind,
            'triggerPrice': ctx.price(trigger),
            'quantity': ctx.amount(),
            'workingType': _BINANCE_WORKING_TYPE[ctx.trigger_by],
            **scope,
        }
        if _BINANCE_ALGO_CID_RE.fullmatch(cid):
            req['clientAlgoId'] = cid  # the transport adds the broker prefix
        await ctx.pre_submit(_BINANCE_TP_SL_TYPES[kind], req.get('clientAlgoId'), trigger, req)
        resp = await ctx.ex._call(_binance_algo_create, ctx.c, req, safe=False)
        merged = {**req, **(resp if isinstance(resp, dict) else {})}
        merged.setdefault('orderType', kind)
        order = _binance_algo_order(merged, ctx.symbol, ctx.market)
        if order is not None:
            order.position_side = ctx.position_side
            if order.id:  # ex.order / ex.cancel_order by this id go to the Algo API
                ctx.ex._binance_algo_ids.add(order.id)
            result.append(order)
    return result


# ─── Bybit: position trading-stop ───────────────────────────────────────────

_BYBIT_TRIGGER_BY = {'mark': 'MarkPrice', 'last': 'LastPrice'}


def _bybit_position_idx(ctx: _Ctx) -> int:
    if not ctx.hedge:
        return 0
    return 1 if ctx.position_side == 'long' else 2


async def _set_bybit(ctx: _Ctx, take_profit: Optional[float],
                     stop_loss: Optional[float]) -> list[TpSlOrder]:
    raw_market = ctx.market.raw
    base = {
        'category': 'inverse' if raw_market.get('inverse') else 'linear',
        'symbol': ctx.market.id,
        'positionIdx': _bybit_position_idx(ctx),
        'tpslMode': 'Partial' if ctx.partial else 'Full',
    }
    legs = []
    for kind, trigger, prefix, price_key in (('stop_loss', stop_loss, 'sl', 'stopLoss'),
                                             ('take_profit', take_profit, 'tp', 'takeProfit')):
        if trigger is None:
            continue
        req = dict(base)
        req[price_key] = ctx.price(trigger)
        req[f'{prefix}TriggerBy'] = _BYBIT_TRIGGER_BY[ctx.trigger_by]
        if ctx.partial:
            req[f'{prefix}Size'] = ctx.amount()
            req[f'{prefix}OrderType'] = 'Market'
        # one request per leg (Full mode replaces the leg entirely).
        # trading-stop does not accept a client_id (v5 docs: the request has no orderLinkId) —
        # a caller-given id is not sent, the order is marked has_client_id=False
        await ctx.pre_submit(kind, None, trigger, req)
        await ctx.ex._call(ctx.c.privatePostV5PositionTradingStop, req, safe=False)
        legs.append((kind, float(req[price_key])))
        ctx.placed.append(_minimal(ctx, kind, float(req[price_key]), has_client_id=False))
    # trading-stop returns no id: find the created conditional orders with one request
    try:
        listed = await _list_generic(ctx.ex, ctx.symbol, ctx.market)
    except BytxError as e:
        logger.warning(f'bybit: TP/SL placed, but the order list was not fetched: {e}')
        listed = []
    result = []
    for kind, trigger in legs:
        match = next((o for o in listed if o.kind == kind and o.position_side == ctx.position_side
                      and _same_price(_trigger_of(o, kind), trigger)), None)
        order = match or _minimal(ctx, kind, trigger)
        order.has_client_id = False
        result.append(order)
    if ctx.explicit:
        logger.warning(f'bybit: position TP/SL (trading-stop) has no client_id — the given '
                       f'{sorted(ctx.explicit)}_client_id were not sent (has_client_id=False)')
    return result


#: pause before reconciling TP/SL after an unclear outcome — lets the exchange show the leg in the list
RECONCILE_SETTLE = 1.5
RECONCILE_SCANS = 3


def _ambiguous(error: BytxError) -> bool:
    """The request may have reached the exchange and executed, but no response: network/timeout/5xx. 429 and a
    time error mean the exchange refused before execution."""
    return isinstance(error, NetworkError) and not isinstance(error, (RateLimited, TimestampError))


async def _reconcile_legs(ctx: _Ctx, take_profit: Optional[float],
                          stop_loss: Optional[float]) -> Optional[list[TpSlOrder]]:
    """After an unclear outcome: all requested legs are on the exchange (by client_id or trigger price) —
    returns them (together with the already placed ones); otherwise None. No placement retries — duplicate protection is worse."""
    wanted = [(k, p) for k, p in (('stop_loss', stop_loss), ('take_profit', take_profit)) if p is not None]
    for scan in range(RECONCILE_SCANS):
        await asyncio.sleep(RECONCILE_SETTLE if scan == 0 else RECONCILE_SETTLE / 2)
        try:
            listed = await tp_sl_orders(ctx.ex, ctx.symbol, ctx.position_side)  # type: ignore[arg-type]
        except BytxError:
            continue
        legs: list[TpSlOrder] = []
        for kind, trigger in wanted:
            leg = find_leg(ctx.ex, listed, kind, ctx.position_side, trigger, client_ids=ctx.client_ids)
            if leg is None:
                break
            if leg not in legs:
                legs.append(leg)
        else:
            return legs
    return None


def _same_price(a: Optional[float], b: Optional[float]) -> bool:
    return a is not None and b is not None and math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-12)


def _trigger_of(o: TpSlOrder, kind: str) -> Optional[float]:
    value = o.take_profit if kind == 'take_profit' else o.stop_loss
    return value if value is not None else o.trigger_price


def find_leg(ex: 'Exchange', orders: Any, kind: str, position_side: str, trigger: Optional[float], *,
             client_ids: Any = ()) -> Optional[TpSlOrder]:
    """Leg ``kind`` ('stop_loss' / 'take_profit') for ``position_side`` among open TP/SL
    ``orders`` (:meth:`Exchange.tp_sl_orders`) — whether protection is already in place (recovery, open_position).

    An OKX pair (kind 'tp_sl') covers both sides. Match: a client_id from ``client_ids`` (where it
    is sent to the exchange — the price may have been moved) or otherwise the trigger price within the exchange
    price step: both prices rounded with ``price_to_precision`` (as the SDK rounds on placement)
    are equal — or equal within float precision (``_same_price``). Leg side — the order's ``position_side``,
    without it — by the closing order side (sell closes long).
    """
    cids = {str(c).lower() for c in client_ids if c}
    for o in orders or ():
        if o.kind not in (kind, 'tp_sl') or getattr(o, 'state', None) not in (None, 'open'):
            continue
        side = o.position_side or (('long' if o.side == 'sell' else 'short') if o.side else None)
        if side != position_side:
            continue
        if o.client_id and o.client_id.lower() in cids:
            return o
        if trigger is not None and _near_price(ex, o.symbol, _trigger_of(o, kind), trigger):
            return o
    return None


def _near_price(ex: 'Exchange', symbol: str, a: Optional[float], b: float) -> bool:
    if a is None:
        return False
    if _same_price(a, b):
        return True
    try:
        c = ex._ex
        return float(c.price_to_precision(symbol, a)) == float(c.price_to_precision(symbol, b))
    except Exception:   # market not loaded / no precision — exact match only
        return False


def _minimal(ctx: _Ctx, kind: str, trigger: Optional[float], *, id: str = '',
             client_id: Optional[str] = None, take_profit: Optional[float] = None,
             stop_loss: Optional[float] = None, raw: Optional[dict] = None,
             has_client_id: bool = True) -> TpSlOrder:
    """Order from the known request data (the exchange returned only an id or nothing)."""
    return TpSlOrder(
        id=id, client_id=client_id, symbol=ctx.symbol, side=ctx.close_side, type='market',
        status='open', price=None, average=None, size=ctx.contracts * ctx.market.contract_size,
        filled=None, remaining=None, reduce_only=True, post_only=None, time_in_force=None,
        trigger_price=trigger, fee=None, fee_currency=None, timestamp=int(time.time() * 1000),
        raw=raw or {}, contracts=ctx.contracts, kind=kind, position_side=ctx.position_side,
        take_profit=take_profit if take_profit is not None else (trigger if kind == 'take_profit' else None),
        stop_loss=stop_loss if stop_loss is not None else (trigger if kind == 'stop_loss' else None),
        has_client_id=has_client_id, state='open',
    )


# ─── OKX: algo conditional ──────────────────────────────────────────────────

async def _set_okx(ctx: _Ctx, take_profit: Optional[float],
                   stop_loss: Optional[float]) -> list[TpSlOrder]:
    # TP and SL together — in ONE algo request (OKX keeps them as one pair). With caller
    # client_ids — as separate algo orders: a pair has one algoClOrdId, while the bot's TP and SL
    # live separately (each with its own id) and are canceled one by one
    both = take_profit is not None and stop_loss is not None
    if both and set(ctx.explicit) <= {'tp_sl'}:
        legs = [(take_profit, stop_loss)]
    else:
        legs = [leg for leg in ((None, stop_loss), (take_profit, None)) if leg != (None, None)]
    result = ctx.placed
    for tp, sl in legs:
        cid = ctx.client_id('take_profit' if sl is None else 'stop_loss' if tp is None else 'tp_sl')
        req: dict = {
            'instId': ctx.market.id,
            'tdMode': 'isolated' if ctx.margin_mode == 'isolated' else 'cross',
            'ordType': 'conditional',
            'side': ctx.close_side,
            'posSide': ctx.position_side if ctx.hedge else 'net',
            'sz': ctx.amount(),
            'reduceOnly': True,
            'cxlOnClosePos': True,
            'algoClOrdId': cid,
        }
        if tp is not None:
            req.update(tpTriggerPx=ctx.price(tp), tpOrdPx='-1', tpTriggerPxType=ctx.trigger_by)
        if sl is not None:
            req.update(slTriggerPx=ctx.price(sl), slOrdPx='-1', slTriggerPxType=ctx.trigger_by)
        req.update(okx_tag_params(ctx.c))  # broker tag with our code
        leg_kind = 'tp_sl' if tp is not None and sl is not None else 'take_profit' if tp is not None else 'stop_loss'
        await ctx.pre_submit(leg_kind, cid, tp if leg_kind == 'take_profit' else sl, req)
        resp = await ctx.ex._call(ctx.c.private_post_trade_order_algo, req, safe=False)
        data = (resp.get('data') or [{}])[0] if isinstance(resp, dict) else {}
        kind = 'tp_sl' if tp is not None and sl is not None else 'take_profit' if tp is not None else 'stop_loss'
        tp_f = float(req['tpTriggerPx']) if tp is not None else None
        sl_f = float(req['slTriggerPx']) if sl is not None else None
        result.append(_minimal(ctx, kind, tp_f if kind == 'take_profit' else sl_f if kind == 'stop_loss' else None,
                               id=str(data.get('algoId') or ''), client_id=data.get('algoClOrdId') or cid,
                               take_profit=tp_f, stop_loss=sl_f, raw=data))
    return result


# ─── BingX: TAKE_PROFIT_MARKET / STOP_MARKET ────────────────────────────────

async def _set_bingx(ctx: _Ctx, take_profit: Optional[float],
                     stop_loss: Optional[float]) -> list[TpSlOrder]:
    if ctx.hedge:
        scope: dict = {'positionSide': 'LONG' if ctx.position_side == 'long' else 'SHORT'}
    else:
        scope = {'reduceOnly': True}
    result = ctx.placed
    for kind, order_type, trigger in (('stop_loss', 'STOP_MARKET', stop_loss),
                                      ('take_profit', 'TAKE_PROFIT_MARKET', take_profit)):
        if trigger is None:
            continue
        # Place order (POST /openApi/swap/v2/trade/order, docs-v3): «clientOrderId only supports
        # LIMIT/MARKET order type" — conditional TP/SL have no cid. Not sent (like Bybit trading-stop):
        # the leg is found by the id from the response, without it — by kind, side and trigger (journal tp_sl_set, find_leg)
        params = {'stopPrice': float(ctx.price(trigger)), **scope,
                  'workingType': _BINANCE_WORKING_TYPE[ctx.trigger_by]}
        await ctx.pre_submit(kind, None, trigger, params)
        raw = await ctx.ex._call(ctx.c.create_order, ctx.symbol, order_type, ctx.close_side,
                                 ctx.contracts, None, params)
        order = _from_ccxt(ctx, raw, kind, trigger, order_type)
        order.has_client_id = False
        result.append(order)
    if ctx.explicit:
        logger.warning(f'bingx: clientOrderID only for LIMIT/MARKET — the given '
                       f'{sorted(ctx.explicit)}_client_id for TP/SL were not sent (has_client_id=False)')
    return result


def _from_ccxt(ctx: _Ctx, raw: dict, kind: str, trigger: float, order_type: str) -> TpSlOrder:
    fallback = {'symbol': ctx.symbol, 'side': ctx.close_side, 'type': order_type.lower(),
                'amount': ctx.contracts}
    order = convert.order(raw or {}, ctx.market, fallback)
    t = float(ctx.price(trigger))
    wrapped = _wrap(order, kind, ctx.position_side,
                    take_profit=t if kind == 'take_profit' else None,
                    stop_loss=t if kind == 'stop_loss' else None)
    if wrapped.trigger_price is None:
        wrapped.trigger_price = t
    if wrapped.status is None:
        wrapped.status = 'open'
    wrapped.reduce_only = True
    return _with_state(wrapped, ctx.ex._ex.id, (raw or {}).get('info'))


# ─── Hyperliquid: two reduce-only trigger orders ───────────────────────────

async def _set_hyperliquid(ctx: _Ctx, take_profit: Optional[float],
                           stop_loss: Optional[float]) -> list[TpSlOrder]:
    if ctx.trigger_by != 'mark':
        raise NotSupported('hyperliquid: TP/SL triggers work only on the mark price',
                           exchange=ctx.ex.name)
    # main order price, otherwise the WS cache or a single l2Book (not tickers of all markets)
    ref = ctx.ref_price
    if not ref:
        try:
            ref = await ctx.ex._reference_price(ctx.symbol, None)
        except InvalidOrder:
            raise InvalidOrder(f'no {ctx.symbol} price to compute slippage',
                               exchange=ctx.ex.name) from None
    result = ctx.placed
    for kind, key, trigger in (('stop_loss', 'stopLossPrice', stop_loss),
                               ('take_profit', 'takeProfitPrice', take_profit)):
        if trigger is None:
            continue
        # cloid is unique per order — TP and SL each have their own
        params = {key: trigger, 'reduceOnly': True, **ctx.cid_params(ctx.client_id(kind))}
        await ctx.pre_submit(kind, params.get('clientOrderId'), trigger, params)
        raw = await ctx.ex._call(ctx.c.create_order, ctx.symbol, 'market', ctx.close_side,
                                 ctx.contracts, ref, params)
        result.append(_from_ccxt(ctx, raw, kind, trigger, 'market'))
    return result


# ─── KuCoin Futures: stop orders ────────────────────────────────────────────

async def _set_kucoin(ctx: _Ctx, take_profit: Optional[float],
                      stop_loss: Optional[float]) -> list[TpSlOrder]:
    result = ctx.placed
    for kind, key, trigger in (('stop_loss', 'stopLossPrice', stop_loss),
                               ('take_profit', 'takeProfitPrice', take_profit)):
        if trigger is None:
            continue
        params = {key: trigger, 'reduceOnly': True, 'marginMode': ctx.margin_mode.upper(),
                  'triggerPriceType': ctx.trigger_by, **ctx.cid_params(ctx.client_id(kind))}
        await ctx.pre_submit(kind, params.get('clientOid'), trigger, params)
        raw = await ctx.ex._call(ctx.c.create_order, ctx.symbol, 'market', ctx.close_side,
                                 ctx.contracts, None, params)
        result.append(_from_ccxt(ctx, raw, kind, trigger, 'market'))
    return result


# ─── Bitget: place-tpsl-order (pos_profit / pos_loss) ───────────────────────

def _bitget_product(m: Market) -> str:
    settle = str((m.raw or {}).get('settle') or 'USDT').upper()
    return {'USDT': 'USDT-FUTURES', 'USDC': 'USDC-FUTURES'}.get(settle, 'COIN-FUTURES')


async def _set_bitget(ctx: _Ctx, take_profit: Optional[float],
                      stop_loss: Optional[float]) -> list[TpSlOrder]:
    """POST /api/v2/mix/order/place-tpsl-order (docs "Stop-profit and stop-loss plan orders"): whole position —
    pos_profit / pos_loss without size, partial — profit_plan / loss_plan with size (base); holdSide in one-way —
    buy / sell (position side), in hedge — long / short; triggerType mark_price | fill_price. ccxt 4.4.65
    sends holdSide long/short in one-way too, and takes the side from the order side (closing sell → short)."""
    from bytx.exchanges.bitget.tp_sl import TRIGGER_TYPES
    trigger_type = TRIGGER_TYPES.get(ctx.trigger_by)
    if trigger_type is None:
        raise NotSupported(f'bitget: TP/SL trigger — mark or last, not {ctx.trigger_by!r}', exchange=ctx.ex.name)
    long = ctx.position_side == 'long'
    hold = ('long' if long else 'short') if ctx.hedge else ('buy' if long else 'sell')
    result = ctx.placed
    for kind, trigger in (('stop_loss', stop_loss), ('take_profit', take_profit)):
        if trigger is None:
            continue
        loss = kind == 'stop_loss'
        req: dict = {'symbol': ctx.market.id, 'productType': _bitget_product(ctx.market),
                     'marginCoin': str((ctx.market.raw or {}).get('settle') or 'USDT').upper(),
                     'planType': ('loss_plan' if loss else 'profit_plan') if ctx.partial
                     else ('pos_loss' if loss else 'pos_profit'),
                     'triggerPrice': ctx.price(trigger), 'triggerType': trigger_type, 'holdSide': hold}
        if ctx.partial:
            req['size'] = ctx.amount()
        cid = ctx.client_id(kind)
        from bytx.order_params import check_client_id
        check_client_id(ctx.c, cid, ctx.ex.market_type)
        req['clientOid'] = cid
        await ctx.pre_submit(kind, cid, trigger, req)
        resp = await ctx.ex._call(ctx.c.privateMixPostV2MixOrderPlaceTpslOrder, req, safe=False)
        data = resp.get('data') if isinstance(resp, dict) else None
        data = data if isinstance(data, dict) else {}
        t = float(req['triggerPrice'])
        result.append(_minimal(ctx, kind, t, id=str(data.get('orderId') or ''),
                               client_id=data.get('clientOid') or cid, raw=data))
    return result


# ─── Gate: price_orders (close-*-position / plan-close-*-position) ──────────

async def _set_gate(ctx: _Ctx, take_profit: Optional[float],
                    stop_loss: Optional[float]) -> list[TpSlOrder]:
    """POST /futures/usdt/price_orders (FuturesPriceTriggeredOrder): whole position in single mode —
    close-<side>-position, initial.close=true, size 0; partial — plan-close-<side>-position, size signed as
    the close, reduce_only. Price 0 + tif ioc — market execution. rule 1 (≥) / 2 (≤) by side.
    price_orders have no user id — has_client_id=False (like Bybit trading-stop)."""
    from bytx.exchanges.gate.tp_sl import PRICE_TYPES
    if ctx.hedge:
        raise NotSupported('gate: dual mode is not supported in the SDK — TP/SL only in single mode', exchange=ctx.ex.name)
    long = ctx.position_side == 'long'
    side = 'long' if long else 'short'
    settle = str((ctx.market.raw or {}).get('settleId') or 'usdt').lower()
    result = ctx.placed
    for kind, trigger in (('stop_loss', stop_loss), ('take_profit', take_profit)):
        if trigger is None:
            continue
        rises = (kind == 'take_profit') == long          # TP long / SL short — the price rises to the trigger
        initial: dict = {'contract': ctx.market.id, 'price': '0', 'tif': 'ioc'}
        if ctx.partial:
            contracts = int(round(float(ctx.amount())))
            initial.update(size=-contracts if long else contracts, reduce_only=True)
            order_type = f'plan-close-{side}-position'
        else:
            initial.update(size=0, close=True)
            order_type = f'close-{side}-position'
        req = {'settle': settle, 'initial': initial, 'order_type': order_type,
               'trigger': {'strategy_type': 0, 'price_type': PRICE_TYPES[ctx.trigger_by],
                           'price': ctx.price(trigger), 'rule': 1 if rises else 2}}
        await ctx.pre_submit(kind, None, trigger, req)
        resp = await ctx.ex._call(ctx.c.privateFuturesPostSettlePriceOrders, req, safe=False)
        data = resp if isinstance(resp, dict) else {}
        t = float(req['trigger']['price'])
        result.append(_minimal(ctx, kind, t, id=str(data.get('id') or ''), client_id=None, raw=data,
                               has_client_id=False))
    return result


# ─── MEXC: stoporder/place (position TP/SL) ────────────────────────────────

async def _mexc_position_id(ctx: _Ctx) -> str:
    """positionId — "Position id", required: from GET position/open_positions (positionType 1 long, 2 short)."""
    resp = await ctx.ex._call(ctx.c.contractPrivateGetPositionOpenPositions, {'symbol': ctx.market.id})
    rows = resp.get('data') if isinstance(resp, dict) else None
    want = 1 if ctx.position_side == 'long' else 2
    for r in rows if isinstance(rows, list) else []:
        if isinstance(r, dict) and r.get('symbol') == ctx.market.id and int(r.get('positionType') or 0) == want:
            return str(r.get('positionId'))
    raise NotSupported(f'mexc: no {ctx.position_side} position on {ctx.symbol} — TP/SL is placed on a position',
                       exchange=ctx.ex.name)


async def _set_mexc(ctx: _Ctx, take_profit: Optional[float],
                    stop_loss: Optional[float]) -> list[TpSlOrder]:
    """POST /api/v1/private/stoporder/place: positionId, vol (contracts), volType 2 — whole position / 1 — partial,
    lossTrend / profitTrend "1 latest 2 fair 3 index", execution type 0 — market. Each leg is its own stoporder
    (one price), no user id — has_client_id=False."""
    from bytx.exchanges.mexc.tp_sl import TRENDS
    trend = TRENDS.get(ctx.trigger_by)
    if trend is None:
        raise NotSupported(f'mexc: TP/SL trigger — last, mark or index, not {ctx.trigger_by!r}', exchange=ctx.ex.name)
    position_id = await _mexc_position_id(ctx)
    result = ctx.placed
    for kind, trigger in (('stop_loss', stop_loss), ('take_profit', take_profit)):
        if trigger is None:
            continue
        loss = kind == 'stop_loss'
        # stopLossType "long | true | 0 - market SL", takeProfitType "0 - market TP" — both in every request
        req: dict = {'positionId': position_id, 'vol': float(ctx.amount()), 'volType': 1 if ctx.partial else 2,
                     'lossTrend': trend, 'profitTrend': trend, 'stopLossType': 0, 'takeProfitType': 0}
        if loss:
            req.update(stopLossPrice=float(ctx.price(trigger)))
        else:
            req.update(takeProfitPrice=float(ctx.price(trigger)))
        await ctx.pre_submit(kind, None, trigger, req)
        resp = await ctx.ex._call(ctx.c.request, 'stoporder/place', ['contract', 'private'], 'POST', req, safe=False)
        data = resp.get('data') if isinstance(resp, dict) else None
        oid = data.get('id') if isinstance(data, dict) else data
        result.append(_minimal(ctx, kind, float(ctx.price(trigger)), id=str(oid or ''), client_id=None,
                               raw=resp if isinstance(resp, dict) else {}, has_client_id=False))
    return result


# ─── HTX: swap_cross_tpsl_order / swap_tpsl_order ───────────────────────────

def _htx_tpsl(c: Any, margin_mode: Optional[str], cross: str, isolated: str) -> Any:
    return getattr(c, isolated if margin_mode == 'isolated' else cross)


async def _set_htx(ctx: _Ctx, take_profit: Optional[float],
                   stop_loss: Optional[float]) -> list[TpSlOrder]:
    """POST /linear-swap-api/v1/swap_cross_tpsl_order (isolated — swap_tpsl_order): direction — side
    of the closing order, volume — contracts, *_order_price_type market. The trigger price type cannot be set —
    last only. No user id — has_client_id=False."""
    if ctx.trigger_by != 'last':
        raise NotSupported(f'htx: TP/SL trigger only on the last price, not {ctx.trigger_by!r}', exchange=ctx.ex.name)
    place = _htx_tpsl(ctx.c, ctx.margin_mode, 'contract_private_post_linear_swap_api_v1_swap_cross_tpsl_order',
                      'contract_private_post_linear_swap_api_v1_swap_tpsl_order')
    result = ctx.placed
    for kind, trigger in (('stop_loss', stop_loss), ('take_profit', take_profit)):
        if trigger is None:
            continue
        prefix = 'sl' if kind == 'stop_loss' else 'tp'
        req = {'contract_code': ctx.market.id, 'direction': ctx.close_side, 'volume': int(float(ctx.amount())),
               f'{prefix}_trigger_price': ctx.price(trigger), f'{prefix}_order_price_type': 'market'}
        await ctx.pre_submit(kind, None, trigger, req)
        resp = await ctx.ex._call(place, req, safe=False)
        data = resp.get('data') if isinstance(resp, dict) else None
        leg = (data or {}).get(f'{prefix}_order') if isinstance(data, dict) else None
        oid = (leg or {}).get('order_id_str') or (leg or {}).get('order_id') if isinstance(leg, dict) else None
        result.append(_minimal(ctx, kind, float(ctx.price(trigger)), id=str(oid or ''), client_id=None,
                               raw=leg if isinstance(leg, dict) else {}, has_client_id=False))
    return result


_SETTERS = {
    'binance': _set_binance,
    'bybit': _set_bybit,
    'okx': _set_okx,
    'bingx': _set_bingx,
    'hyperliquid': _set_hyperliquid,
    'kucoinfutures': _set_kucoin,
    'bitget': _set_bitget,
    'gate': _set_gate,
    'mexc': _set_mexc,
    'htx': _set_htx,
}


# ─── active TP/SL list ──────────────────────────────────────────────────────

def _position_side_of(info: dict, order_side: Optional[str]) -> Optional[str]:
    """Side of the position being closed: explicit in the exchange response, otherwise from the order side."""
    for key in ('positionSide', 'posSide'):
        value = str(info.get(key) or '').lower()
        if value in ('long', 'short'):
            return value
    idx = str(info.get('positionIdx') or '')
    if idx in ('1', '2'):
        return 'long' if idx == '1' else 'short'
    if order_side in ('buy', 'sell'):
        return 'long' if order_side == 'sell' else 'short'
    return None


_BYBIT_STOP_TYPES = {'TakeProfit': 'take_profit', 'PartialTakeProfit': 'take_profit',
                     'StopLoss': 'stop_loss', 'PartialStopLoss': 'stop_loss'}
_HL_TYPES = (('take profit', 'take_profit'), ('stop', 'stop_loss'))


def _classify(exchange_id: str, o: dict) -> Optional[str]:
    """TP/SL kind of a ccxt order or None (a regular order)."""
    info = o.get('info') if isinstance(o.get('info'), dict) else {}
    tp = o.get('takeProfitPrice')
    sl = o.get('stopLossPrice')
    if exchange_id == 'bybit':
        kind = _BYBIT_STOP_TYPES.get(str(info.get('stopOrderType') or ''))
        if kind:
            return kind
        if o.get('reduceOnly') and o.get('triggerPrice'):
            return 'take_profit' if tp else 'stop_loss' if sl else None
        return None
    if exchange_id == 'bingx':
        # exchange type (info.type): ccxt 4.4.65 parse_order_type reduces TAKE_PROFIT_MARKET and
        # STOP_MARKET to 'market' — no TP/SL would be found by the unified type
        t = str(info.get('type') or o.get('type') or '').lower()
        return {'take_profit_market': 'take_profit', 'stop_market': 'stop_loss'}.get(t)
    if exchange_id == 'hyperliquid':
        if not (info.get('isTrigger') or convert._f(info.get('triggerPx'))):
            return None
        text = str(info.get('orderType') or o.get('type') or '').lower()
        for marker, kind in _HL_TYPES:
            if marker in text:
                return kind
        return 'take_profit' if tp else 'stop_loss' if sl else None
    if exchange_id == 'kucoinfutures':
        return _kucoin_kind(o)
    if tp and sl:
        return 'tp_sl'
    return 'take_profit' if tp else 'stop_loss' if sl else None


def _kucoin_kind(o: dict) -> Optional[str]:
    """KuCoin stop order: TP = sell+up | buy+down, SL = sell+down | buy+up (reduce-only)."""
    if o.get('takeProfitPrice'):
        return 'take_profit'
    if o.get('stopLossPrice'):
        return 'stop_loss'
    info = o.get('info') if isinstance(o.get('info'), dict) else {}
    stop = str(info.get('stop') or '').lower()
    side = str(o.get('side') or '').lower()
    reduce_only = o.get('reduceOnly')
    if reduce_only is None:
        reduce_only = info.get('reduceOnly') or info.get('closeOrder')
    if stop not in ('up', 'down') or side not in ('buy', 'sell') or not reduce_only:
        return None
    if not (o.get('triggerPrice') or info.get('stopPrice')):
        return None
    return 'take_profit' if (side == 'sell') == (stop == 'up') else 'stop_loss'


def _wrap_listed(exchange_id: str, raw: dict, market: Optional[Market]) -> Optional[TpSlOrder]:
    kind = _classify(exchange_id, raw)
    if kind is None:
        return None
    order = convert.order(raw, market)
    info = raw.get('info') if isinstance(raw.get('info'), dict) else {}
    trigger = order.trigger_price
    if trigger is None:
        trigger = convert._f(info.get('stopPrice') or info.get('triggerPx') or info.get('triggerPrice'))
        order.trigger_price = trigger
    tp = convert._f(raw.get('takeProfitPrice'))
    sl = convert._f(raw.get('stopLossPrice'))
    if kind == 'take_profit' and tp is None:
        tp = trigger
    if kind == 'stop_loss' and sl is None:
        sl = trigger
    wrapped = _wrap(order, kind, _position_side_of(info, order.side), take_profit=tp, stop_loss=sl)
    if exchange_id == 'bybit' and info.get('stopOrderType') in _BYBIT_STOP_TYPES and not order.client_id:
        wrapped.has_client_id = False   # position TP/SL (trading-stop): their orderLinkId is empty
    if exchange_id == 'bingx' and not order.client_id:
        wrapped.has_client_id = False   # clientOrderID — LIMIT/MARKET only (Place order)
    return _with_state(wrapped, exchange_id, info)


async def _list_generic(ex: 'Exchange', s: str, m: Market, params: Optional[dict] = None) -> list[TpSlOrder]:
    raw = await ex._call(ex._ex.fetch_open_orders, s, None, None, params or {})
    result = []
    for o in raw:
        wrapped = _wrap_listed(ex._ex.id, o, m)
        if wrapped is not None:
            result.append(wrapped)
    return result


async def _list_binance(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    # conditional TP/SL live in the Algo Order service (GET /fapi/v1/openAlgoOrders)
    raw = await ex._call(ex._ex.request, 'openAlgoOrders', 'fapiPrivate', 'GET', {})
    rows = raw if isinstance(raw, list) else (raw.get('orders') or []) if isinstance(raw, dict) else []
    result = []
    for d in rows:
        if d.get('symbol') != m.id:
            continue
        order = _binance_algo_order(d, s, m)
        if order is not None:
            result.append(order)
    return result


async def _list_okx(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    resp = await ex._call(ex._ex.private_get_trade_orders_algo_pending,
                          {'ordType': 'conditional', 'instId': m.id})
    result = []
    for d in (resp.get('data') or []) if isinstance(resp, dict) else []:
        parsed = ex._ex.parse_order(d, m.raw)
        if not parsed.get('clientOrderId') and d.get('algoClOrdId'):
            parsed['clientOrderId'] = d['algoClOrdId']  # algo order: client id in algoClOrdId
        wrapped = _wrap_listed('okx', parsed, m)
        if wrapped is not None:
            result.append(wrapped)
    return result


async def _list_kucoin(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    # KuCoin stop orders — a separate endpoint (params stop=True)
    return await _list_generic(ex, s, m, {'stop': True})


_BITGET_TPSL_KINDS = {'pos_loss': 'stop_loss', 'loss_plan': 'stop_loss',
                      'pos_profit': 'take_profit', 'profit_plan': 'take_profit'}


def _bitget_plan_order(d: dict, s: str, m: Market) -> Optional[TpSlOrder]:
    kind = _BITGET_TPSL_KINDS.get(str(d.get('planType') or ''))
    if kind is None:
        return None
    trigger = convert._f(d.get('triggerPrice'))
    pos = str(d.get('posSide') or '').lower()
    side = str(d.get('side') or '').lower()
    if pos not in ('long', 'short'):
        # one-way (posSide net): side — the CLOSING order side, tradeSide sell_single / buy_single
        # (demo 2026-10-27: holdSide=buy on placement → side=sell in Get Plan Orders); the position is the opposite
        pos = 'long' if side == 'sell' else 'short' if side == 'buy' else ''
    size = convert._f(d.get('size'))
    order = TpSlOrder(
        id=str(d.get('orderId') or ''), client_id=d.get('clientOid') or None, symbol=s,
        side=_close_side(pos) if pos else (side or None), type='market', status='open', price=None, average=None,
        size=size, filled=None, remaining=None, reduce_only=True, post_only=None, time_in_force=None,
        trigger_price=trigger, fee=None, fee_currency=None, timestamp=int(convert._f(d.get('cTime')) or 0) or None, raw=d,
        contracts=size, kind=kind, position_side=pos or None,
        take_profit=trigger if kind == 'take_profit' else None, stop_loss=trigger if kind == 'stop_loss' else None)
    return _with_state(order, 'bitget', d)


async def _list_bitget(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    # GET /api/v2/mix/order/orders-plan-pending, planType=profit_loss — «take profit and stop loss orders
    # (including the profit_plan, loss_plan, moving_plan, pos_profit and pos_loss)»
    resp = await ex._call(ex._ex.privateMixGetV2MixOrderOrdersPlanPending,
                          {'symbol': m.id, 'productType': _bitget_product(m), 'planType': 'profit_loss'})
    data = resp.get('data') if isinstance(resp, dict) else None
    rows = (data or {}).get('entrustedList') or [] if isinstance(data, dict) else []
    return [o for o in (_bitget_plan_order(d, s, m) for d in rows) if o is not None]


def _gate_price_order(d: dict, s: str, m: Market) -> Optional[TpSlOrder]:
    order_type = str(d.get('order_type') or '')
    trigger = d.get('trigger') if isinstance(d.get('trigger'), dict) else {}
    initial = d.get('initial') if isinstance(d.get('initial'), dict) else {}
    if 'long' in order_type:
        pos = 'long'
    elif 'short' in order_type:
        pos = 'short'
    else:
        return None
    rule = str(trigger.get('rule') or '')
    if rule not in ('1', '2'):
        return None
    rises = rule == '1'
    kind = 'take_profit' if rises == (pos == 'long') else 'stop_loss'
    price = convert._f(trigger.get('price'))
    size = abs(convert._f(initial.get('size')) or 0) or None
    t = convert._f(d.get('create_time'))
    from bytx.exchanges.gate.ccxt_fixes import order_status
    order = TpSlOrder(
        id=str(d.get('id') or ''), client_id=None, symbol=s, side=_close_side(pos), type='market',
        status=cast(Any, order_status(d) or 'open'),
        price=None, average=None, size=size * m.contract_size if size else None, filled=None, remaining=None,
        reduce_only=True, post_only=None, time_in_force=None, trigger_price=price, fee=None, fee_currency=None,
        timestamp=int(t * 1000) if t is not None else None, raw=d, contracts=size, kind=kind, position_side=pos,
        take_profit=price if kind == 'take_profit' else None, stop_loss=price if kind == 'stop_loss' else None,
        has_client_id=False)
    return _with_state(order, 'gate', d)


async def _get_gate(ex: 'Exchange', s: str, m: Market, oid: str) -> Optional[TpSlOrder]:
    """Price-triggered order by id in any status; None — missing, another contract, or not a position TP/SL."""
    if not oid.isdigit():
        return None
    settle = str((m.raw or {}).get('settleId') or 'usdt').lower()
    try:
        row = await ex._call(ex._ex.privateFuturesGetSettlePriceOrdersOrderId, {'settle': settle, 'order_id': oid})
    except OrderNotFound:
        return None
    initial = row.get('initial') if isinstance(row, dict) else None
    if not isinstance(initial, dict) or initial.get('contract') != m.id or str(row.get('id')) != oid:
        return None
    return _gate_price_order(row, s, m)


async def _list_gate(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    settle = str((m.raw or {}).get('settleId') or 'usdt').lower()
    rows = await ex._call(ex._ex.privateFuturesGetSettlePriceOrders, {'settle': settle, 'status': 'open',
                                                                      'contract': m.id})
    return [o for o in (_gate_price_order(d, s, m) for d in (rows if isinstance(rows, list) else [])) if o is not None]


_MEXC_STOP_STATUS: dict[str, OrderStatus] = {'open': 'open', 'triggered': 'closed', 'canceled': 'canceled'}


def _mexc_stop_orders(ex: 'Exchange', d: dict, s: str, m: Market) -> list[TpSlOrder]:
    """stoporder: stopLossPrice / takeProfitPrice in one record — one leg per set price.
    Order status — from the record's ``state`` (the history list returns both triggered and removed ones)."""
    pos = {1: 'long', 2: 'short'}.get(int(d.get('positionType') or 0))
    if pos is None:
        return []
    status = _MEXC_STOP_STATUS.get(str(tp_sl_state('mexc', d, 'open')), 'open')
    size = convert._f(d.get('vol'))
    out = []
    for kind, field_ in (('stop_loss', 'stopLossPrice'), ('take_profit', 'takeProfitPrice')):
        trigger = convert._f(d.get(field_))
        if not trigger:
            continue
        order = TpSlOrder(
            id=str(d.get('id') or ''), client_id=None, symbol=s, side=_close_side(pos), type='market', status=status,
            price=None, average=None, size=size * m.contract_size if size else None, filled=None, remaining=None,
            reduce_only=True, post_only=None, time_in_force=None, trigger_price=trigger, fee=None, fee_currency=None,
            timestamp=int(convert._f(d.get('createTime')) or 0) or None, raw=d, contracts=size, kind=kind,
            position_side=pos, take_profit=trigger if kind == 'take_profit' else None,
            stop_loss=trigger if kind == 'stop_loss' else None, has_client_id=False)
        out.append(_with_state(order, 'mexc', d))
    return out


async def _list_mexc(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    # GET /api/v1/private/stoporder/open_orders — «Get Current Take-Profit/Stop-Loss Order List»
    resp = await ex._call(ex._ex.request, 'stoporder/open_orders', ['contract', 'private'], 'GET', {'symbol': m.id})
    rows = resp.get('data') if isinstance(resp, dict) else None
    return [o for d in (rows if isinstance(rows, list) else []) if isinstance(d, dict) and d.get('symbol') == m.id
            for o in _mexc_stop_orders(ex, d, s, m)]


async def _find_mexc(ex: 'Exchange', s: str, m: Market, oid: str, max_pages: int = 10) -> Optional[TpSlOrder]:
    """TP/SL by id in any status — GET /api/v1/private/stoporder/list/orders ("Get Take-Profit/Stop-Loss Order
    List": state 1–5, triggerSide, placeOrderId — the fill order); page_size "max 100", newest first.
    A record with both prices — kind 'tp_sl'; triggered — the leg by triggerSide (1 TP, 2 SL)."""
    for page in range(1, max_pages + 1):
        resp = await ex._call(ex._ex.request, 'stoporder/list/orders', ['contract', 'private'], 'GET',
                              {'symbol': m.id, 'page_num': page, 'page_size': 100})
        rows = resp.get('data') if isinstance(resp, dict) else None
        rows = rows if isinstance(rows, list) else []
        for d in rows:
            if isinstance(d, dict) and str(d.get('id')) == oid:
                legs = _mexc_stop_orders(ex, d, s, m)
                fired = {1: 'take_profit', 2: 'stop_loss'}.get(int(d.get('triggerSide') or 0))
                if fired is not None:
                    legs = [o for o in legs if o.kind == fired] or legs
                if len(legs) == 2:
                    sl, tp = legs
                    tp.kind, tp.stop_loss = 'tp_sl', sl.stop_loss
                    return tp
                return legs[0] if legs else None
        if len(rows) < 100:
            break
    return None


def _htx_tpsl_order(d: dict, s: str, m: Market) -> Optional[TpSlOrder]:
    kind = {'tp': 'take_profit', 'sl': 'stop_loss'}.get(str(d.get('tpsl_order_type') or ''))
    direction = str(d.get('direction') or '')
    if kind is None or direction not in ('buy', 'sell'):
        return None
    pos = 'long' if direction == 'sell' else 'short'      # a closing sell — long position
    trigger = convert._f(d.get('trigger_price'))
    size = convert._f(d.get('volume'))
    order = TpSlOrder(
        id=str(d.get('order_id_str') or d.get('order_id') or ''), client_id=None, symbol=s, side=direction,
        type='market', status='open', price=None, average=None, size=size * m.contract_size if size else None,
        filled=None, remaining=None, reduce_only=True, post_only=None, time_in_force=None, trigger_price=trigger,
        fee=None, fee_currency=None, timestamp=int(convert._f(d.get('created_at')) or 0) or None, raw=d,
        contracts=size, kind=kind, position_side=pos, take_profit=trigger if kind == 'take_profit' else None,
        stop_loss=trigger if kind == 'stop_loss' else None, has_client_id=False)
    _with_state(order, 'htx', d)
    # history (tpsl_hisorders): order status follows the TP/SL state, not the "open" of the active list
    order.status = {'triggered': 'closed', 'canceled': 'canceled'}.get(order.state or '', 'open')  # type: ignore[assignment]
    return order


async def _find_htx(ex: 'Exchange', s: str, m: Market, oid: str) -> Optional[TpSlOrder]:
    """HTX: TP/SL by id — among open ones, then swap_cross_tpsl_hisorders (order_info does not know TP/SL)."""
    from bytx.exchanges.htx import tp_sl as htx_tp_sl
    found = next((o for o in await _list_htx(ex, s, m) if o.id == oid), None)
    if found is not None:
        return found
    history = _htx_tpsl(ex._ex, ex.margin_mode_of(s), 'contract_private_post_linear_swap_api_v1_swap_cross_tpsl_hisorders',
                        'contract_private_post_linear_swap_api_v1_swap_tpsl_hisorders')
    for page in range(1, htx_tp_sl.HISTORY_PAGES + 1):
        resp = await ex._call(history, {'contract_code': m.id, 'status': '0', 'create_date': htx_tp_sl.HISTORY_DAYS,
                                        'page_index': page, 'page_size': 50})
        data = resp.get('data') if isinstance(resp, dict) else None
        rows = (data or {}).get('orders') or [] if isinstance(data, dict) else []
        for d in rows:
            if isinstance(d, dict) and str(d.get('order_id_str') or d.get('order_id')) == oid:
                return _htx_tpsl_order(d, s, m)
        if not isinstance(data, dict) or page >= int(data.get('total_page') or 1):
            break
    return None


async def _list_htx(ex: 'Exchange', s: str, m: Market) -> list[TpSlOrder]:
    lister = _htx_tpsl(ex._ex, ex.margin_mode_of(s), 'contract_private_post_linear_swap_api_v1_swap_cross_tpsl_openorders',
                       'contract_private_post_linear_swap_api_v1_swap_tpsl_openorders')
    out: list[TpSlOrder] = []
    for page in range(1, 51):
        resp = await ex._call(lister, {'contract_code': m.id, 'page_index': page, 'page_size': 50})
        data = resp.get('data') if isinstance(resp, dict) else None
        rows = (data or {}).get('orders') or [] if isinstance(data, dict) else []
        out.extend(o for o in (_htx_tpsl_order(d, s, m) for d in rows if isinstance(d, dict)) if o is not None)
        if not isinstance(data, dict) or page >= int(data.get('total_page') or 1):
            break
    return out


_LISTERS = {
    'mexc': _list_mexc,
    'htx': _list_htx,
    'gate': _list_gate,
    'bitget': _list_bitget,
    'binance': _list_binance,
    'okx': _list_okx,
    'kucoinfutures': _list_kucoin,
}


async def conditional_symbols(ex: 'Exchange', symbol: Optional[str] = None) -> set[str]:
    """Symbols where TP/SL live outside the open orders list (Binance Algo, OKX algo
    conditional, KuCoin stop) and must be canceled separately. Other exchanges — empty.

    Without ``symbol`` — all symbols with such orders (one exchange request).
    """
    c = ex._ex
    if ex.market_type == 'spot' or c.id not in _LISTERS:
        return set()
    if symbol is not None:
        return {ex.symbol(symbol)}
    if c.id == 'kucoinfutures':
        raw = await ex._call(c.fetch_open_orders, None, None, None, {'stop': True})
        return {o['symbol'] for o in raw if o.get('symbol') in c.markets}
    if c.id == 'binance':
        raw = await ex._call(c.request, 'openAlgoOrders', 'fapiPrivate', 'GET', {})
        rows = raw if isinstance(raw, list) else (raw.get('orders') or []) if isinstance(raw, dict) else []
        ids = {d.get('symbol') for d in rows if isinstance(d, dict)}
    elif c.id == 'gate':
        rows = await ex._call(c.privateFuturesGetSettlePriceOrders, {'settle': 'usdt', 'status': 'open'})
        ids = {d.get('contract') for d in (rows if isinstance(rows, list) else []) if isinstance(d, dict)}
    elif c.id == 'mexc':
        resp = await ex._call(c.request, 'stoporder/open_orders', ['contract', 'private'], 'GET', {})
        rows = resp.get('data') if isinstance(resp, dict) else None
        ids = {d.get('symbol') for d in (rows if isinstance(rows, list) else []) if isinstance(d, dict)}
    elif c.id == 'htx':
        ids = set()
        for name in ('contract_private_post_linear_swap_api_v1_swap_cross_tpsl_openorders',
                     'contract_private_post_linear_swap_api_v1_swap_tpsl_openorders'):
            resp = await ex._call(getattr(c, name), {'page_size': 50})
            data = resp.get('data') if isinstance(resp, dict) else None
            rows = (data or {}).get('orders') or [] if isinstance(data, dict) else []
            ids |= {d.get('contract_code') for d in rows if isinstance(d, dict)}
    elif c.id == 'bitget':
        ids = set()
        for product in ('USDT-FUTURES', 'USDC-FUTURES'):
            resp = await ex._call(c.privateMixGetV2MixOrderOrdersPlanPending,
                                  {'productType': product, 'planType': 'profit_loss'})
            data = resp.get('data') if isinstance(resp, dict) else None
            rows = (data or {}).get('entrustedList') or [] if isinstance(data, dict) else []
            ids |= {str(d.get('symbol') or '').upper() for d in rows if isinstance(d, dict)}
    else:
        resp = await ex._call(c.private_get_trade_orders_algo_pending, {'ordType': 'conditional'})
        data = (resp.get('data') or []) if isinstance(resp, dict) else []
        ids = {d.get('instId') for d in data if isinstance(d, dict)}
    result = set()
    for mid in ids:
        if mid and mid in c.markets_by_id:
            result.add(c.safe_symbol(mid, None, None, 'swap'))
    return result


async def tp_sl_orders(ex: 'Exchange', symbol: str, side: Optional[PositionSide] = None) -> list[TpSlOrder]:
    """Active TP/SL for a symbol (side — position side, None — both)."""
    await ex.connect()
    if ex.market_type == 'spot':
        raise NotSupported('position TP/SL is for derivatives only', exchange=ex.name)
    if ex._ex.id not in _SETTERS:
        raise NotSupported('TP/SL is not supported', exchange=ex.name)
    s = ex.symbol(symbol)
    m = to_market(ex._ex.market(s), ex._ex.id)
    lister = _LISTERS.get(ex._ex.id, _list_generic)
    orders = await lister(ex, s, m)
    for o in orders:
        if o.state is None:  # active list: the exchange gave no status — so it is waiting for the trigger
            o.state = 'open'
    if side is not None:
        orders = [o for o in orders if o.position_side == side]
    return orders


async def tp_sl_order(ex: 'Exchange', symbol: str, order_id: str) -> TpSlOrder:
    """A single TP/SL by exchange id in any status — without process memory.

    Binance — ``GET /fapi/v1/algoOrder`` by algoId (conditional orders live in the Algo API); OKX —
    ``GET /api/v5/trade/order-algo`` by algoId; Gate — ``GET /futures/{settle}/price_orders/{order_id}``; others — a regular order lookup
    (:meth:`Exchange.order`: KuCoin returns stop orders by id too, Bybit — realtime → history).
    No such order or it is not a TP/SL — :class:`OrderNotFound`.
    """
    await ex.connect()
    if ex.market_type == 'spot':
        raise NotSupported('position TP/SL is for derivatives only', exchange=ex.name)
    s = ex.symbol(symbol)
    m = to_market(ex._ex.market(s), ex._ex.id)
    oid = str(order_id)
    found: Optional[TpSlOrder] = None
    if ex._ex.id == 'binance':
        algo = await ex._find_binance_algo(s, oid)
        if algo is not None:
            ex._binance_algo_ids.add(oid)
            found = _binance_algo_order(algo, s, m)
            if found is None:
                raise OrderNotFound(f'algo order {oid} ({s}) is not a TP/SL', exchange=ex.name)
    elif ex._ex.id == 'okx':
        resp = await ex._call(ex._ex.private_get_trade_order_algo, {'algoId': oid})
        rows = [d for d in ((resp.get('data') or []) if isinstance(resp, dict) else [])
                if isinstance(d, dict) and str(d.get('algoId')) == oid and d.get('instId') == m.id]
        if rows:
            parsed = ex._ex.parse_order(rows[0], m.raw)
            if not parsed.get('clientOrderId') and rows[0].get('algoClOrdId'):
                parsed['clientOrderId'] = rows[0]['algoClOrdId']
            found = _wrap_listed('okx', parsed, m)
            if found is None:
                raise OrderNotFound(f'algo order {oid} ({s}) is not a TP/SL', exchange=ex.name)
    elif ex._ex.id == 'gate':
        # price-triggered order: GET /futures/{settle}/price_orders/{order_id} («Query single auto order
        # details") — a regular GET orders/{order_id} uses a different id space
        found = await _get_gate(ex, s, m, oid)
        if found is None:
            raise OrderNotFound(f'TP/SL {oid} ({s}) not found among price-triggered orders', exchange=ex.name)
    elif ex._ex.id == 'mexc':
        # a stoporder lives separately from orders: order/get finds nothing by its id (an id of another entity)
        found = await _find_mexc(ex, s, m, oid)
        if found is None:
            raise OrderNotFound(f'TP/SL {oid} ({s}) not found in stoporder/list/orders', exchange=ex.name)
    elif ex._ex.id == 'htx':
        found = await _find_htx(ex, s, m, oid)
        if found is None:
            raise OrderNotFound(f'TP/SL {oid} ({s}) not found among open orders or in TP/SL history', exchange=ex.name)
    elif ex._ex.id == 'bitget':
        # position TP/SL (place-tpsl-order) are plan orders: a regular order detail does not know them (40109)
        found = next((o for o in await _list_bitget(ex, s, m) if str(o.id) == oid), None)
        if found is None:
            raise OrderNotFound(f'TP/SL {oid} ({s}) not found among open plan orders', exchange=ex.name)
    if found is None:
        order = await ex.order(s, oid)
        raw = order.raw if isinstance(order.raw, dict) else {}
        info = raw.get('info') if isinstance(raw.get('info'), dict) else {}
        if ex._ex.id == 'binance' and 'algoId' in info:
            # Algo API order (ccxt: type stop_market/take_profit_market, takeProfitPrice/
            # stopLossPrice = null) — the TP/SL kind from orderType in the Algo API response
            found = _binance_algo_order(info, s, m)
        else:
            found = _wrap_listed(ex._ex.id, raw, m)
        if found is None:
            raise OrderNotFound(f'order {oid} ({s}) is not a TP/SL', exchange=ex.name)
    return found


def _okx_body(error: BaseException) -> Optional[dict]:
    """OKX response body from the ccxt error text ("okx {...}"), if present."""
    text = str(error.__cause__ or error)
    start = text.find('{')
    if start < 0:
        return None
    try:
        body = json.loads(text[start:])
    except ValueError:
        return None
    return body if isinstance(body, dict) else None


def _okx_cancel_algos_items(ex: 'Exchange', chunk: list[str], response: Any,
                            error: Optional[BytxError]) -> list[Optional[BaseException]]:
    """Result of POST /trade/cancel-algos for each algoId from ``data[]`` (the element's sCode).

    Docs "Cancel algo order" / "Error Code": top-level ``code`` '0' — all canceled, '1' —
    "Operation failed", '2' — "Bulk operation partially succeeded" (ccxt 4.4.65 does not raise on '2',
    and on '1' raises by the first sCode — one error's class cannot be applied to the whole batch).
    ``sCode`` '0' — canceled; an "order no longer exists" code (ORDER_NOT_OPEN) — OrderNotFound; other —
    an error of that element. An element without a record in ``data[]`` is canceled only when ``code`` is '0'.
    """
    c = ex._ex
    data = response.get('data') if isinstance(response, dict) else None
    code = str(response.get('code')) if isinstance(response, dict) else None
    if not isinstance(data, list) or not data:
        if error is not None:
            return [error] * len(chunk)
        if code in (None, '0', 'None'):
            return [None] * len(chunk)
        return [ExchangeError(f'okx cancel-algos: code {code} without data[] — cancel outcome not confirmed',
                              exchange=ex.name, code=code)] * len(chunk)
    by_id = {str(d.get('algoId')): d for d in data if isinstance(d, dict)}
    out: list[Optional[BaseException]] = []
    for algo_id in chunk:
        item = by_id.get(algo_id)
        if item is None:
            out.append(None if code == '0' else ExchangeError(
                f'okx cancel-algos: no result for algoId {algo_id} (code {code}) — cancel not confirmed',
                exchange=ex.name, code=code))
            continue
        s_code = str(item.get('sCode', ''))
        if s_code == '0':
            out.append(None)
            continue
        feedback = 'okx ' + json.dumps({'code': '1', 'msg': '', 'data': [item]})
        try:
            c.throw_exactly_matched_exception(c.exceptions['exact'], s_code, feedback)
            c.throw_broadly_matched_exception(c.exceptions['broad'], str(item.get('sMsg') or ''), feedback)
            raise ccxt.ExchangeError(feedback)
        except ccxt.BaseError as raw:
            element = _errors.from_ccxt(raw, ex.name)
        if not isinstance(element, OrderNotFound) and outcome.order_not_open(c.id, element):
            element = OrderNotFound(element.message, exchange=element.exchange, code=element.code)
        out.append(element)
    return out


async def _htx_cancel_one(ex: 'Exchange', fn: Any, contract: str, order_id: str) -> Any:
    """HTX tpsl_cancel: a per-order reject goes to data.errors (1061 "This order doesnt exist") with status ok."""
    resp = await ex._call(fn, {'contract_code': contract, 'order_id': order_id})
    data = resp.get('data') if isinstance(resp, dict) else None
    for err in (data or {}).get('errors') or [] if isinstance(data, dict) else []:
        if str(err.get('order_id')) == str(order_id):
            raise OrderNotFound(f'htx {{"err_code":{err.get("err_code")},"err_msg":"{err.get("err_msg")}"}}',
                                exchange=ex.name, details={'order_id': order_id})
    return resp


async def cancel_tp_sl_each(ex: 'Exchange', s: str, orders: list[TpSlOrder]) -> list[Optional[BaseException]]:
    """Cancel a list of TP/SL on one symbol → per order: None (canceled) or an error.

    A TP+SL pair as one order (OKX) is canceled as a whole. OrderNotFound — the order is already gone.
    OKX: a batch of up to 10 algoIds in one request, results by each response element's sCode.
    """
    c = ex._ex
    m = to_market(c.market(s), c.id)
    if c.id == 'okx':
        out: list[Optional[BaseException]] = []
        ids = [o.id for o in orders]
        for i in range(0, len(ids), 10):  # OKX: up to 10 algo orders per request
            chunk = ids[i:i + 10]
            try:
                response = await ex._call(c.private_post_trade_cancel_algos,
                                          [{'algoId': a, 'instId': m.id} for a in chunk])
            except BytxError as e:
                out.extend(_okx_cancel_algos_items(ex, chunk, _okx_body(e), e))
                continue
            out.extend(_okx_cancel_algos_items(ex, chunk, response, None))
        return out
    if c.id == 'binance':
        def cancel(o: TpSlOrder):  # DELETE /fapi/v1/algoOrder: per the docs only algoId (no symbol)
            return ex._call(c.request, 'algoOrder', 'fapiPrivate', 'DELETE', {'algoId': o.id})
    elif c.id == 'kucoinfutures':
        def cancel(o: TpSlOrder):
            return ex._call(c.cancel_order, o.id, s, {'stop': True})
    elif c.id == 'gate':
        def cancel(o: TpSlOrder):  # DELETE /futures/{settle}/price_orders/{order_id}
            return ex._call(c.privateFuturesDeleteSettlePriceOrdersOrderId,
                            {'settle': str((m.raw or {}).get('settleId') or 'usdt').lower(), 'order_id': o.id})
    elif c.id == 'bitget':
        def cancel(o: TpSlOrder):  # cancel-plan-order: orderIdList «aligned with symbol/productType/planType»
            plan = str((o.raw or {}).get('planType') or ('pos_loss' if o.kind == 'stop_loss' else 'pos_profit'))
            return ex._call(c.privateMixPostV2MixOrderCancelPlanOrder, {
                'symbol': m.id, 'productType': _bitget_product(m), 'planType': plan,
                'marginCoin': str((m.raw or {}).get('settle') or 'USDT').upper(),
                'orderIdList': [{'orderId': o.id}]})
    elif c.id == 'mexc':
        def cancel(o: TpSlOrder):  # POST stoporder/cancel: [{"stopPlanOrderId": …}]
            return ex._call(c.contractPrivatePostStoporderCancel, [{'stopPlanOrderId': o.id}])
        orders = list({o.id: o for o in orders}.values())   # TP and SL of one record share one id
    elif c.id == 'htx':
        def cancel(o: TpSlOrder):  # swap_cross_tpsl_cancel: order_id (comma-separated, up to 10)
            fn = _htx_tpsl(c, ex.margin_mode_of(s), 'contract_private_post_linear_swap_api_v1_swap_cross_tpsl_cancel',
                           'contract_private_post_linear_swap_api_v1_swap_tpsl_cancel')
            return _htx_cancel_one(ex, fn, m.id, o.id)
    else:
        def cancel(o: TpSlOrder):
            return ex._call(c.cancel_order, o.id, s)
    results = await asyncio.gather(*(cancel(o) for o in orders), return_exceptions=True)
    if c.id == 'binance':
        for o, r in zip(orders, results):
            if not isinstance(r, BaseException) or isinstance(r, OrderNotFound):
                ex._binance_algo_ids.discard(o.id)
    return [r if isinstance(r, BaseException) else None for r in results]


@dataclass(slots=True)
class TpSlModifyResult:
    """Result of :meth:`Exchange.modify_tp_sl` — what happened to the old and the new order.

    ``old``: 'canceled' — the old trigger is gone (removed or replaced in place); 'alive' —
    the old order is still in place; 'triggered' — it fired in time (the new one was not placed); 'unknown' —
    the cancel is not confirmed and the state could not be read.
    ``new``: 'created' — the new one is in place (``order``); 'unknown' — the placement outcome is unknown
    (timeout, disconnect: look it up by ``client_ids``); 'rejected' — the exchange explicitly refused (the old one is already removed!);
    None — the new one was not placed. ``atomic`` — modified in a single exchange request (Bybit trading-stop).
    ``error`` — the exception of the step that failed.
    """

    old: str
    new: Optional[str]
    order: Optional[TpSlOrder] = None
    previous: Optional[TpSlOrder] = None
    atomic: bool = False
    error: Optional[BytxError] = None
    client_ids: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.new == 'created'


# Bybit: "full position" TP/SL (tpslMode=Full) are modified in place via the same /v5/position/trading-stop
# (docs: "Full position TP/SL orders: This API can be used to modify the parameters of existing
# TP/SL orders"). Partial — cancel and place anew
_BYBIT_FULL_STOP_TYPES = ('TakeProfit', 'StopLoss')


def _modify_prices(old: TpSlOrder, take_profit: Optional[float], stop_loss: Optional[float],
                   ex: 'Exchange') -> tuple[Optional[float], Optional[float]]:
    if take_profit is None and stop_loss is None:
        raise ConfigError('take_profit and/or stop_loss is required', exchange=ex.name, details={'order_id': old.id})
    if old.kind == 'tp_sl':  # OKX pair: a missing price keeps the previous one
        return (take_profit if take_profit is not None else old.take_profit,
                stop_loss if stop_loss is not None else old.stop_loss)
    wrong = stop_loss if old.kind == 'take_profit' else take_profit
    if wrong is not None:
        raise ConfigError(f'order {old.id} is {old.kind}: the other leg is not modified (place it separately)',
                          exchange=ex.name, details={'order_id': old.id, 'kind': old.kind})
    return take_profit, stop_loss


async def modify_tp_sl(ex: 'Exchange', symbol: str, order_id: str, *, take_profit: Optional[float] = None,
                       stop_loss: Optional[float] = None, size: Optional[float] = None,
                       contracts: Optional[float] = None, client_id: Optional[str] = None,
                       trigger_by: Optional[TriggerBy] = None) -> TpSlModifyResult:
    """Modifies TP/SL by exchange id. See :meth:`Exchange.modify_tp_sl`."""
    if size is not None and contracts is not None:
        raise ConfigError(f'at most one of size / contracts is allowed ({symbol})', exchange=ex.name,
                          details={'symbol': symbol, 'size': size, 'contracts': contracts})
    old = await tp_sl_order(ex, symbol, order_id)
    s = ex.symbol(symbol)
    tp, sl = _modify_prices(old, take_profit, stop_loss, ex)
    if old.state in ('triggered', 'canceled'):
        return TpSlModifyResult(old=old.state, new=None, previous=old)
    if old.position_side not in ('long', 'short'):
        raise ConfigError(f'order {old.id}: position side is undetermined', exchange=ex.name,
                          details={'order_id': old.id})
    position_side: str = old.position_side
    info = old.raw.get('info') if isinstance(old.raw.get('info'), dict) else old.raw
    if (ex._ex.id == 'bybit' and size is None and contracts is None
            and str((info or {}).get('stopOrderType') or '') in _BYBIT_FULL_STOP_TYPES):
        return await _modify_bybit_full(ex, s, old, tp, sl, trigger_by)
    # cancel the old one; the new one is placed only when the old one is definitely gone
    error = (await cancel_tp_sl_each(ex, s, [old]))[0]
    if error is not None:
        try:
            now = await tp_sl_order(ex, s, old.id)
        except BytxError:
            now = None
        state = now.state if now is not None else None
        if state == 'triggered':
            return TpSlModifyResult(old='triggered', new=None, previous=old)
        if state != 'canceled':
            err = error if isinstance(error, BytxError) else ExchangeError(str(error), exchange=ex.name)
            return TpSlModifyResult(old='alive' if state == 'open' else 'unknown', new=None,
                                    previous=old, error=err)
    if size is None and contracts is None:
        contracts = old.contracts
        if contracts is None and old.size is not None:
            contracts = convert.contracts_from_size(ex._ex, to_market(ex._ex.market(s), ex._ex.id), old.size)
    kwargs: dict = {}
    if old.kind == 'tp_sl':
        kwargs['pair_client_id'] = client_id
    elif old.kind == 'take_profit':
        kwargs['take_profit_client_id'] = client_id
    else:
        kwargs['stop_loss_client_id'] = client_id
    try:
        orders = await set_tp_sl(ex, s, take_profit=tp, stop_loss=sl, size=size, contracts=contracts,
                                 trigger_by=trigger_by, position_side=position_side, **kwargs)  # type: ignore[arg-type]
    except BytxError as e:
        details = e.details if isinstance(e.details, dict) else {}
        verdict = outcome.classify(ex._ex.id, e)
        return TpSlModifyResult(old='canceled', new='rejected' if verdict == 'rejected' else 'unknown',
                                previous=old, error=e, client_ids=list(details.get('client_ids') or []))
    return TpSlModifyResult(old='canceled', new='created', order=orders[0], previous=old,
                            client_ids=[o.client_id for o in orders if o.client_id])


async def _modify_bybit_full(ex: 'Exchange', s: str, old: TpSlOrder, tp: Optional[float],
                             sl: Optional[float], trigger_by: Optional[TriggerBy]) -> TpSlModifyResult:
    """Bybit Full: the new price goes via the same trading-stop — the old trigger is replaced in place."""
    by = trigger_by or tp_sl_trigger_by('bybit')
    if by not in _TRIGGER_BY:
        raise ConfigError(f'trigger_by: {_TRIGGER_BY}, not {by!r} ({s})', exchange=ex.name)
    m = to_market(ex._ex.market(s), ex._ex.id)
    ctx = _Ctx(ex, s, m, old.position_side or 'long', old.contracts or 0.0, False,
               await ex.is_hedge_mode(s), by, (ex.margin_mode_of(s) or 'cross').lower())
    _check_prices(ctx.position_side, tp, sl, ex.name)
    try:
        orders = await _place(ctx, _set_bybit, tp, sl, strict=False)
    except BytxError as e:
        if outcome.classify('bybit', e, has_client_id=False) == 'rejected':
            return TpSlModifyResult(old='alive', new=None, previous=old, atomic=True, error=e)
        return TpSlModifyResult(old='unknown', new='unknown', previous=old, atomic=True, error=e)
    return TpSlModifyResult(old='canceled', new='created', order=orders[0], previous=old, atomic=True)


async def cancel_tp_sl(ex: 'Exchange', symbol: str, *, side: Optional[PositionSide] = None,
                       kind: Optional[str] = None, order_ids: Optional[list[str]] = None) -> int:
    """Cancels TP/SL on a symbol. Returns the number canceled.

    kind: 'take_profit' / 'stop_loss' / None (all). A TP+SL pair as one order
    (OKX) is canceled as a whole for any kind. order_ids — only these orders: the list
    of active TP/SL is fetched from the exchange, so ids from a DB work after a restart too.
    """
    if kind is not None and kind not in _KINDS:
        raise ConfigError(f'kind: {_KINDS} or None, not {kind!r} ({symbol})', exchange=ex.name,
                          details={'symbol': symbol, 'kind': kind})
    orders = await tp_sl_orders(ex, symbol, side)
    if kind is not None:
        orders = [o for o in orders if o.kind in (kind, 'tp_sl')]
    if order_ids is not None:
        wanted = {str(i) for i in order_ids}
        orders = [o for o in orders if o.id in wanted]
    orders = [o for o in orders if o.id]
    if not orders:
        return 0
    results = await cancel_tp_sl_each(ex, ex.symbol(symbol), orders)
    errors = [r for r in results if r is not None and not isinstance(r, OrderNotFound)]
    if errors:
        raise errors[0]
    return sum(1 for r in results if r is None)


# ═══ Dead-man switch ════════════════════════════════════════════════════════

# Allowed windows (seconds) per exchange docs; 0 — remove the timer.
_DMS_RANGES = {'okx': (10, 120), 'bingx': (10, 120), 'bybit': (3, 300)}
_HL_MIN_SECONDS = 5


async def cancel_all_after(ex: 'Exchange', seconds: float, symbol: Optional[str] = None) -> Optional[int]:
    """Exchange dead-man switch: all orders are canceled after ``seconds`` unless the timer is extended.

    Returns the trigger time (ms, local clock) or None if the timer was removed.
    """
    if seconds is None or seconds < 0 or not math.isfinite(seconds):
        raise ConfigError('seconds must be >= 0 (0 — remove the timer)')
    await ex.connect()
    c = ex._ex
    eid = c.id
    ms = int(round(seconds * 1000))
    if eid == 'binance':
        if ex.market_type == 'spot':
            raise NotSupported('countdownCancelAll is for Binance futures only', exchange=ex.name)
        if not symbol:
            raise ConfigError('binance: cancel_all_after requires symbol (a per-symbol timer)',
                              exchange=ex.name)
        s = ex.symbol(symbol)
        # POST /fapi/v1/countdownCancelAll, countdownTime in ms (0 — remove)
        await ex._call(c.fapiPrivatePostCountdownCancelAll,
                       {'symbol': c.market_id(s), 'countdownTime': ms})
    elif eid == 'hyperliquid':
        if ms == 0:
            await ex._call(_hyperliquid_unschedule, c)
        else:
            if seconds < _HL_MIN_SECONDS:
                raise ConfigError(f'hyperliquid: seconds >= {_HL_MIN_SECONDS}', exchange=ex.name)
            await ex._call(c.cancel_all_orders_after, ms)
    elif eid in _DMS_RANGES:
        lo, hi = _DMS_RANGES[eid]
        if ms == 0 and eid == 'bybit':
            raise NotSupported('bybit: DCP can be removed only in the account settings', exchange=ex.name)
        whole = int(seconds)
        if ms != 0 and not (lo <= whole <= hi):
            raise ConfigError(f'{ex.name}: seconds must be in {lo}..{hi} (or 0)', exchange=ex.name)
        await ex._call(c.cancel_all_orders_after, whole * 1000)
    else:
        raise NotSupported('cancel_all_after is not supported by the exchange', exchange=ex.name)
    return None if ms == 0 else int(time.time() * 1000) + ms


async def _hyperliquid_unschedule(c: Any) -> Any:
    """Hyperliquid: scheduleCancel without time removes the timer (ccxt always sends time)."""
    c.check_required_credentials()
    nonce = c.milliseconds()
    action = {'type': 'scheduleCancel'}
    signature = c.sign_l1_action(action, nonce)
    return await c.privatePostExchange({'action': action, 'nonce': nonce, 'signature': signature})


class DeadManSwitch:
    """Keeps the exchange cancel timer armed while the process is alive.

        async with DeadManSwitch(ex, seconds=60, refresh_every=20):
            ...  # trading; if the process hangs/crashes the exchange cancels the orders

    The timer is re-armed every ``refresh_every`` seconds in the background. On a clean exit from
    the block the timer is removed; on exit by exception it is NOT removed (let the exchange
    cancel the orders). Re-arm errors do not crash the background task: they are counted in ``failures``,
    the last one goes to ``last_error``, the next attempt comes sooner.

    Bybit: DCP fires when the private WS connection drops and must be enabled
    in the account; the timer cannot be removed via the API. Binance: ``symbol`` is required.
    """

    def __init__(self, ex: 'Exchange', seconds: float, refresh_every: Optional[float] = None, *,
                 symbol: Optional[str] = None) -> None:
        if seconds <= 0:
            raise ConfigError('seconds must be > 0')
        refresh = seconds / 3 if refresh_every is None else refresh_every
        if not 0 < refresh < seconds:
            raise ConfigError('refresh_every must be > 0 and less than seconds')
        self.ex = ex
        self.seconds = seconds
        self.refresh_every = refresh
        self.symbol = symbol
        self.armed_until: Optional[int] = None
        self.failures = 0
        self.last_error: Optional[BaseException] = None
        self._task: Optional[asyncio.Task] = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def arm(self) -> Optional[int]:
        self.armed_until = await cancel_all_after(self.ex, self.seconds, self.symbol)
        return self.armed_until

    async def start(self) -> 'DeadManSwitch':
        """Arms the timer (an error raises immediately) and starts the background refresh."""
        if self.running:
            return self
        await self.arm()
        self._task = asyncio.create_task(self._loop(), name=f'bytx-dms-{self.ex.name}')
        return self

    async def stop(self, *, disarm: bool = True) -> None:
        """Stops the refresh; disarm=True — removes the timer on the exchange."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        if disarm:
            try:
                await cancel_all_after(self.ex, 0, self.symbol)
                self.armed_until = None
            except BytxError as e:
                logger.warning(f'dead-man switch {self.ex.name}: timer not removed: {e}')

    async def _loop(self) -> None:
        delay = self.refresh_every
        while True:
            await asyncio.sleep(delay)
            try:
                await self.arm()
                delay = self.refresh_every
            except asyncio.CancelledError:
                raise
            except Exception as e:  # the background task must not die on a network failure
                self.failures += 1
                self.last_error = e
                delay = min(self.refresh_every, 1.0)
                logger.warning(f'dead-man switch {self.ex.name}: re-arm failed: {e}')

    async def __aenter__(self) -> 'DeadManSwitch':
        return await self.start()

    async def __aexit__(self, exc_type: Any, *exc: Any) -> None:
        await self.stop(disarm=exc_type is None)


# ═══ Client-side risk guard ═════════════════════════════════════════════════

class RiskGuard:
    """Order checks BEFORE sending; a violation — :class:`InvalidOrder`.

    - ``max_position_notional`` — limit on the symbol's |position| in quote after the order;
    - ``max_daily_loss`` — daily (UTC) loss by the settlement currency balance: once
        reached, new positions are not opened;
    - ``max_orders_per_minute`` — a rolling 60 s window (protection against runaway loops).

    Reduce-only orders are never blocked (a position can always be closed).
    Position and balance are cached for ``state_ttl`` seconds; between requests the position
    accounts for our own orders (as if they had filled) — without REST on
    every order. If ``ex.position_tracker`` is running for the symbol and is current, the
    position is taken from it, without REST. A guard can be shared between accounts
    only with a shared limit.

    REST (position, balance, price) runs outside the shared locks: orders on other symbols and
    exchanges do not wait for it; position refreshes are coalesced by key (exchange, symbol).
    ``rest_timeout`` — the wait limit for such a request (seconds; None — no limit),
    after which :class:`~bytx.errors.RequestTimeout` is raised and the order is not sent.
    """

    def __init__(self, *, max_position_notional: Optional[float] = None,
                 max_daily_loss: Optional[float] = None, max_orders_per_minute: Optional[int] = None,
                 state_ttl: float = 2.0, rest_timeout: Optional[float] = 10.0) -> None:
        for name, value in (('max_position_notional', max_position_notional),
                            ('max_daily_loss', max_daily_loss),
                            ('max_orders_per_minute', max_orders_per_minute),
                            ('rest_timeout', rest_timeout)):
            if value is not None and value <= 0:
                raise ConfigError(f'{name} must be > 0')
        self.max_position_notional = max_position_notional
        self.max_daily_loss = max_daily_loss
        self.max_orders_per_minute = max_orders_per_minute
        self.state_ttl = state_ttl
        self.rest_timeout = rest_timeout
        self._orders: deque[float] = deque()
        self._exposure: dict[tuple, tuple[float, float]] = {}  # (exchange, symbol) → (signed notional, ts)
        self._equity: dict[tuple, tuple[float, float]] = {}    # (exchange, currency) → (balance, ts)
        self._day_start: dict[tuple, tuple[str, float]] = {}   # (exchange, currency) → (UTC date, balance)
        # locks only for per-key cache refresh: (exchange, symbol) / (exchange, currency)
        self._locks: dict[tuple, asyncio.Lock] = {}

    def reset(self) -> None:
        """Resets counters, the position cache and the daily loss baseline."""
        self._orders.clear()
        self._exposure.clear()
        self._equity.clear()
        self._day_start.clear()

    def daily_loss(self, exchange: str, currency: str) -> Optional[float]:
        """Current daily loss (a positive number) by the last known balance."""
        start = self._day_start.get((exchange, currency.upper()))
        eq = self._equity.get((exchange, currency.upper()))
        if start is None or eq is None:
            return None
        return max(0.0, start[1] - eq[0])

    async def check(self, ex: 'Exchange', symbol: str, side: str, size: float,
                    price: Optional[float], reduce_only: bool) -> None:
        """Called by the Exchange before sending an order (size in base coin)."""
        if reduce_only:
            return
        now = time.monotonic()
        slot = None
        if self.max_orders_per_minute is not None:
            # order window — synchronous (no await): the slot is reserved immediately and released on reject
            while self._orders and now - self._orders[0] >= 60.0:
                self._orders.popleft()
            if len(self._orders) >= self.max_orders_per_minute:
                raise InvalidOrder(f'risk guard: more than {self.max_orders_per_minute} orders per minute',
                                   exchange=ex.name)
            slot = now
            self._orders.append(slot)
        try:
            if self.max_daily_loss is not None:
                await self._check_daily_loss(ex, symbol, now)
            if self.max_position_notional is not None:
                if price is None:
                    price = await self._timed(ex, ex._reference_price(symbol, side))  # type: ignore[arg-type]
                await self._check_position(ex, symbol, side, size, price)  # type: ignore[arg-type]
        except BaseException:
            if slot is not None:
                try:
                    self._orders.remove(slot)  # keep only an order that passed the check
                except ValueError:
                    pass
            raise

    async def _check_position(self, ex: 'Exchange', symbol: str, side: str, size: float,
                              price: float) -> None:
        key = (ex.name, symbol)
        await self._position_notional(ex, symbol, key, time.monotonic())
        # read, check and write of exposure — no await in between (atomic within the loop)
        current = self._exposure[key][0]
        signed = current + size * price * (1 if side == 'buy' else -1)
        if abs(signed) > self.max_position_notional and abs(signed) > abs(current):  # type: ignore[operator]
            raise InvalidOrder(
                f'risk guard: position {symbol} {abs(signed):.2f} > {self.max_position_notional}',
                exchange=ex.name)
        self._exposure[key] = (signed, self._exposure[key][1])

    async def _timed(self, ex: 'Exchange', aw: Any) -> Any:
        try:
            return await asyncio.wait_for(aw, self.rest_timeout)
        except asyncio.TimeoutError:
            raise RequestTimeout(f'risk guard: account state not received within {self.rest_timeout} s',
                                 exchange=ex.name) from None

    def _lock(self, key: tuple) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = self._locks[key] = asyncio.Lock()
        return lock

    @staticmethod
    def _live_positions(ex: 'Exchange', symbol: str) -> Optional[list[Position]]:
        """Symbol positions from a current PositionTracker (None — no such tracker)."""
        for t in list(getattr(ex, '_trackers', ()) or ()):
            if getattr(t, 'kind', None) == 'positions' and t.symbol in (None, symbol) and t.live:
                return [p for p in t.positions if p.symbol == symbol]
        return None

    async def _position_notional(self, ex: 'Exchange', symbol: str, key: tuple, now: float) -> float:
        cached = self._exposure.get(key)
        if cached is not None and now - cached[1] < self.state_ttl:
            return cached[0]
        async with self._lock(key):  # concurrent orders on the symbol wait for one request
            cached = self._exposure.get(key)
            if cached is not None and time.monotonic() - cached[1] < self.state_ttl:
                return cached[0]
            positions = self._live_positions(ex, symbol)
            if positions is None:
                positions = await self._timed(ex, ex.positions([symbol]))
            value = 0.0
            for p in positions:
                if p.symbol != symbol:
                    continue
                px = p.mark_price or p.entry_price
                notional = abs(p.notional) if p.notional else p.size * (px or 0.0)
                value += notional if p.side == 'long' else -notional
            self._exposure[key] = (value, time.monotonic())
            return value

    async def _check_daily_loss(self, ex: 'Exchange', symbol: str, now: float) -> None:
        market = ex._ex.market(symbol)
        currency = str(market.get('settle') or market.get('quote') or 'USDT').upper()
        key = (ex.name, currency)
        cached = self._equity.get(key)
        if cached is None or now - cached[1] >= self.state_ttl:
            async with self._lock(key):
                cached = self._equity.get(key)
                if cached is None or now - cached[1] >= self.state_ttl:
                    balance = await self._timed(ex, ex.balance())
                    cached = (balance.total(currency), time.monotonic())
                    self._equity[key] = cached
        today = time.strftime('%Y-%m-%d', time.gmtime())
        start = self._day_start.get(key)
        if start is None or start[0] != today:
            start = (today, cached[0])
            self._day_start[key] = start
        loss = start[1] - cached[0]
        if loss >= self.max_daily_loss:  # type: ignore[operator]
            raise InvalidOrder(f'risk guard: daily loss {loss:.2f} {currency} '
                               f'>= {self.max_daily_loss}', exchange=ex.name)


__all__ = ['TpSlOrder', 'TriggerBy', 'TpSlKind', 'set_tp_sl', 'tp_sl_orders', 'tp_sl_order', 'cancel_tp_sl',
           'cancel_all_after', 'DeadManSwitch', 'RiskGuard']
