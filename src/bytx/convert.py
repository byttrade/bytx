"""Conversion of ccxt responses into :mod:`bytx.types` models and of symbols into ccxt format.

On derivatives ccxt returns amounts in contracts (OKX, KuCoin — not 1:1 with the base).
Here everything is converted to the base coin: ``size = contracts * contract_size``.
"""
from __future__ import annotations

import re
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any, Optional, cast

import ccxt
from ccxt.base.decimal_to_precision import (
    DECIMAL_PLACES,
    NO_PADDING,
    TICK_SIZE,
    TRUNCATE,
    decimal_to_precision,
)

from bytx.errors import OrderTooSmall
from bytx.sizing import snap_down
from bytx.types import (
    Balance,
    BalanceItem,
    Candle,
    Fill,
    FundingRate,
    Market,
    OpenInterest,
    Order,
    OrderBook,
    OrderStatus,
    Position,
    PositionSide,
    Ticker,
    Trade,
)

_SEP = re.compile(r'[-_/:\s]')
_USD_STABLES = ('USDT', 'USDC', 'USD', 'BUSD', 'FDUSD')
# Hyperliquid trades perps in USDC: BTC/USDT → BTC/USDC:USDC
_QUOTE_ALIASES = {'hyperliquid': {'USDT': 'USDC'}}


def _f(value: Any) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _s(value: Any) -> str:
    return '' if value is None else str(value)


def _cs(market: Optional[Market]) -> float:
    return market.contract_size if market is not None and market.is_contract else 1.0


def _mul(value: Any, factor: float) -> Optional[float]:
    v = _f(value)
    return None if v is None else v * factor


# ─── symbols ────────────────────────────────────────────────────────────────

def resolve_symbol(markets: dict, markets_by_id: dict, symbol: str, market_type: str,
                   exchange_id: str) -> Optional[str]:
    """Unified symbol for 'BTC/USDT', 'BTCUSDT', 'BTC-USDT-SWAP', 'btc_usdt' etc."""
    want_spot = market_type == 'spot'

    def fits(sym: str) -> bool:
        m = markets.get(sym)
        return m is not None and (m.get('type') == 'spot') == want_spot

    text = symbol.strip()
    if fits(text):
        return text
    aliases = _QUOTE_ALIASES.get(exchange_id, {})
    if '/' in text:
        base, _, rest = text.upper().partition('/')
        quote = rest.split(':')[0]
        quote = aliases.get(quote, quote)
        candidate = f'{base}/{quote}' if want_spot else f'{base}/{quote}:{quote}'
        if fits(candidate):
            return candidate
        return None
    # exchange id ('BTCUSDT', 'BTC-USDT-SWAP')
    for entry in (markets_by_id.get(text) or markets_by_id.get(text.upper()) or []):
        sym = entry.get('symbol') if isinstance(entry, dict) else None
        if sym and fits(sym):
            return sym
    # base+quote glued together without separators
    flat = _SEP.sub('', text.upper()).removesuffix('SWAP').removesuffix('PERP')
    # 'ETH-BTC' → exchange id 'ETHBTC' (quote not in the stablecoin list)
    for entry in markets_by_id.get(flat) or []:
        sym = entry.get('symbol') if isinstance(entry, dict) else None
        if sym and fits(sym):
            return sym
    for quote in _USD_STABLES:
        if flat.endswith(quote) and len(flat) > len(quote):
            base = flat[: -len(quote)]
            q = aliases.get(quote, quote)
            candidate = f'{base}/{q}' if want_spot else f'{base}/{q}:{q}'
            if fits(candidate):
                return candidate
    # Hyperliquid: native instrument id is the coin from meta ('BTC', 'ETH'), perps in USDC
    if exchange_id == 'hyperliquid' and not want_spot and flat:
        candidate = f'{flat}/USDC:USDC'
        if fits(candidate):
            return candidate
    return None


# ─── amounts ────────────────────────────────────────────────────────────────

def contracts_from_size(exchange: Any, market: Market, size: float) -> float:
    """Amount in base coin → exchange contracts, rounded down to the step."""
    if market.is_contract:
        # divide in Decimal: in float 0.29 / 0.01 = 28.999…, and truncation loses a whole step
        raw = float(Decimal(str(size)) / Decimal(str(market.contract_size)))
    else:
        raw = size
    return contracts_to_precision(exchange, market, raw, size=size)


def contracts_to_precision(exchange: Any, market: Market, contracts: float, *,
                           size: Optional[float] = None) -> float:
    """Contracts → to the exchange amount step, down (the amount never exceeds the request by more
    than float error 1e-9: snapped to a whole lot, :func:`bytx.sizing.snap_down`).

    Below one step — :class:`OrderTooSmall`. ``size`` — the original amount for the error text.
    """
    shown = contracts if size is None else size
    raw = contracts
    precision = (market.raw.get('precision') or {}).get('amount')
    if precision is not None and exchange.precisionMode == TICK_SIZE and raw > 0:
        # a value slightly below a whole
        # number of lots due to the caller's float error (notional / price, k·lot·price/lev·lev)
        # is whole lots; round down only if the whole lot is more than 1e-9 away relatively
        raw = float(snap_down(Decimal(str(raw)), Decimal(str(precision))))
    if precision is not None:
        # amount_to_precision on some exchanges (ccxt hyperliquid) rounds to nearest —
        # truncate down first so the amount does not exceed the request
        raw = float(decimal_to_precision(raw, TRUNCATE, precision, exchange.precisionMode, NO_PADDING))
        if raw <= 0:
            raise OrderTooSmall(f'amount {shown} is below the step {market.amount_step} ({market.symbol})',
                                exchange=exchange.id)
    try:
        return float(exchange.amount_to_precision(market.symbol, raw))
    except ccxt.InvalidOrder as e:
        raise OrderTooSmall(f'amount {shown} is below the step {market.amount_step} ({market.symbol})',
                            exchange=exchange.id) from e


def price_to_precision(exchange: Any, market: Market, price: float,
                       side: Optional[str] = None) -> float:
    """Price to the exchange price step.

    ``side`` rounds to the passive side — the price never crosses the user's limit:
    'buy' — down, 'sell' — up. Without ``side`` — to nearest.
    """
    if side not in ('buy', 'sell'):
        return float(exchange.price_to_precision(market.symbol, price))
    rounding = ROUND_FLOOR if side == 'buy' else ROUND_CEILING
    value = Decimal(str(price))
    if exchange.id == 'hyperliquid':
        # HL docs (tick-and-lot-size): up to 5 significant digits and at most MAX_DECIMALS - szDecimals
        # decimal places (6 perp, 8 spot); an integer price is always valid
        sig = max(5, len(str(int(value))))
        amount = (market.raw.get('precision') or {}).get('amount')
        sz_decimals = exchange.precision_from_string(exchange.number_to_string(amount)) if amount else 0
        max_decimals = (6 if market.is_contract else 8) - sz_decimals
        exp = max(value.adjusted() - sig + 1, -max_decimals)
        value = value.quantize(Decimal(1).scaleb(exp), rounding=rounding)
    else:
        step = (market.raw.get('precision') or {}).get('price')
        if step is None:
            return float(exchange.price_to_precision(market.symbol, price))
        if exchange.precisionMode == TICK_SIZE:
            tick = Decimal(str(step))
        elif exchange.precisionMode == DECIMAL_PLACES:
            tick = Decimal(1).scaleb(-int(step))
        else:  # significant digits: no directional rounding — to nearest
            return float(exchange.price_to_precision(market.symbol, price))
        value = (value / tick).to_integral_value(rounding=rounding) * tick
    # the value is already on the grid — ccxt only formats it
    return float(exchange.price_to_precision(market.symbol, float(value)))


# ─── models ─────────────────────────────────────────────────────────────────

def _info(raw: dict) -> dict:
    info = raw.get('info')
    return info if isinstance(info, dict) else {}


def ticker(t: dict, market: Optional[Market] = None) -> Ticker:
    """``market`` is needed for derivatives with contract_size != 1: best price sizes in contracts
    (OKX bidSz/askSz, KuCoin bestBidSize/bestAskSize) are converted to the base coin."""
    info = _info(t)
    # inverse (Bybit BTCUSD): contract in USD, base depends on price — sizes left as is
    cs = 1.0 if market is not None and market.raw.get('inverse') else _cs(market)
    # Hyperliquid: ccxt parse_ticker drops markPx / oraclePx from asset ctx
    mark = t.get('markPrice') if t.get('markPrice') is not None else info.get('markPx')
    index = t.get('indexPrice') if t.get('indexPrice') is not None else info.get('oraclePx')
    base_volume = t.get('baseVolume')
    if cs != 1.0 and info.get('volCcy24h') is not None:
        base_volume = info['volCcy24h']  # OKX SWAP: vol24h — contracts, volCcy24h — base
    return Ticker(
        symbol=_s(t.get('symbol')), bid=_f(t.get('bid')), ask=_f(t.get('ask')), last=_f(t.get('last')),
        mark=_f(mark), index=_f(index),
        bid_size=_mul(t.get('bidVolume'), cs), ask_size=_mul(t.get('askVolume'), cs),
        high=_f(t.get('high')), low=_f(t.get('low')),
        base_volume=_f(base_volume), quote_volume=_f(t.get('quoteVolume')),
        change_pct=_f(t.get('percentage')), timestamp=t.get('timestamp'), raw=t,
    )


# Exchanges where derivatives candle volume comes in contracts (ccxt does not convert):
# KuCoin Futures kline[5] — lots. For OKX ccxt takes the base (volCcy), Bybit/Binance — base.
_CANDLE_VOLUME_IN_CONTRACTS = frozenset({'kucoinfutures'})


def candle(c: list, exchange_id: str, market: Optional[Market]) -> Candle:
    factor = _cs(market) if exchange_id in _CANDLE_VOLUME_IN_CONTRACTS else 1.0
    return Candle(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]),
                  float(c[5] or 0) * factor)


def order_book(ob: dict, market: Optional[Market]) -> OrderBook:
    cs = _cs(market)
    return OrderBook(
        symbol=ob.get('symbol') or (market.symbol if market else ''),
        bids=[(float(p), float(a) * cs) for p, a, *_ in ob.get('bids') or []],
        asks=[(float(p), float(a) * cs) for p, a, *_ in ob.get('asks') or []],
        timestamp=ob.get('timestamp'), nonce=ob.get('nonce'),
    )


def trade(t: dict, market: Optional[Market]) -> Trade:
    return Trade(id=t.get('id'), symbol=_s(t.get('symbol')), side=t.get('side'),
                 price=float(t['price']), size=float(t.get('amount') or 0) * _cs(market),
                 timestamp=t.get('timestamp'), raw=t)


def balance(b: dict) -> Balance:
    items = {}
    free_all, used_all, total_all = b.get('free') or {}, b.get('used') or {}, b.get('total') or {}
    for cur in {**free_all, **used_all, **total_all}:
        free, used, total = _f(free_all.get(cur)), _f(used_all.get(cur)), _f(total_all.get(cur))
        if total is None:
            # ccxt does not compute total if used is unknown (WS BingX ACCOUNT_UPDATE: wb only)
            if free is None and used is None:
                continue
            total = (free or 0.0) + (used or 0.0)
        items[cur.upper()] = BalanceItem(cur.upper(), free or 0.0, used or 0.0, total)
    return Balance(items=items, timestamp=b.get('timestamp'), raw=b)


# Position side in the exchange info — known even for a zero position:
# Binance positionSide / ps (WS) — LONG/SHORT/BOTH (positionRisk), OKX posSide — long/short/net,
# BingX positionSide — LONG/SHORT/BOTH, KuCoin positionSide — LONG/SHORT/BOTH;
# Bybit V5 positionIdx: 0 — one-way, 1 — hedge buy-side (long), 2 — hedge sell-side (short).
_HEDGE_SIDE_KEYS = ('posSide', 'positionSide', 'ps')
_ONE_WAY_SIDES = ('both', 'net')
_BYBIT_POSITION_IDX = {1: 'long', 2: 'short'}


def hedge_side(info: Any) -> tuple[Optional[PositionSide], Optional[bool]]:
    """(hedge position side, hedged) from exchange fields in ``info``; (None, None) — no fields.

    One-way (BOTH / net / positionIdx 0) — (None, False): side from the amount sign."""
    if not isinstance(info, dict):
        return None, None
    if info.get('type') == 'oneWay':  # Hyperliquid clearinghouseState.assetPositions[]: one-way only
        return None, False
    for key in _HEDGE_SIDE_KEYS:
        value = str(info.get(key) or '').lower()
        if value in ('long', 'short'):
            return cast(PositionSide, value), True
        if value in _ONE_WAY_SIDES:
            return None, False
    try:
        idx = int(info['positionIdx'])
    except (KeyError, TypeError, ValueError):
        return None, None
    return cast(Optional[PositionSide], _BYBIT_POSITION_IDX.get(idx)), idx in _BYBIT_POSITION_IDX


def position(p: dict, market: Optional[Market], *, include_zero: bool = False) -> Optional[Position]:
    """ccxt position → :class:`Position`; zero — None (``include_zero`` — with ``size=0``).

    In hedge mode the side comes from exchange fields (Bybit ``positionIdx``, OKX ``posSide``,
    Binance/BingX/KuCoin ``positionSide``), not from ccxt ``side``; ``hedged`` — from the same
    fields (Hyperliquid — always one-way). One-way — by the amount sign; a zero one-way position
    has no side — ``side=None``.
    """
    contracts = _f(p.get('contracts')) or 0.0
    if contracts == 0 and not include_zero:
        return None
    cs = _f(p.get('contractSize')) or _cs(market)
    info_side, hedged = hedge_side(p.get('info'))
    side: Optional[PositionSide] = info_side or cast(PositionSide, p.get('side'))
    if contracts == 0 and hedged is False:
        side = None  # zero one-way: ccxt guesses 'long' (Binance BOTH, positionAmt 0)
    elif side not in ('long', 'short'):
        # one-way: side BOTH/net/empty, direction — the amount sign
        # (BingX positionAmt, Binance positionAmt, OKX pos — signed)
        side = None if contracts == 0 else 'short' if contracts < 0 else 'long'
    if hedged is None:
        raw_hedged = p.get('hedged')
        hedged = bool(raw_hedged) if isinstance(raw_hedged, bool) else None
    mode = p.get('marginMode')
    margin = _f(p.get('initialMargin')) or _f(p.get('collateral'))
    return Position(
        symbol=_s(p.get('symbol')), side=side, size=abs(contracts) * cs, contracts=abs(contracts),
        entry_price=_f(p.get('entryPrice')), mark_price=_f(p.get('markPrice')),
        liquidation_price=_f(p.get('liquidationPrice')), leverage=_f(p.get('leverage')),
        margin_mode=mode.lower() if isinstance(mode, str) else mode, unrealized_pnl=_f(p.get('unrealizedPnl')),
        realized_pnl=_f(p.get('realizedPnl')), notional=_f(p.get('notional')), margin=margin,
        timestamp=p.get('timestamp'), raw=p, hedged=hedged,
    )


# Raw statuses ccxt passes through as is (BingX REST: PARTIALLYFILLED without "_").
_RAW_ORDER_STATUS = {'PARTIALLYFILLED': 'open', 'PARTIALLY_FILLED': 'open'}
_ORDER_STATUSES = frozenset({'open', 'closed', 'canceled', 'expired', 'rejected'})


def _order_status(o: dict) -> Optional[OrderStatus]:
    """ccxt status → OrderStatus; exchange statuses ccxt did not map are mapped by meaning."""
    status = o.get('status')
    info = _info(o)
    if status is None:
        # Hyperliquid, order response: {"resting": {oid}} — in the book, {"filled": {…}} — filled
        if isinstance(info.get('resting'), dict):
            return 'open'
        if isinstance(info.get('filled'), dict):
            return 'closed'
    if not isinstance(status, str) or status in _ORDER_STATUSES:
        return cast(Optional[OrderStatus], status)
    mapped = _RAW_ORDER_STATUS.get(status.upper())
    if mapped is not None:
        return cast(OrderStatus, mapped)
    # Hyperliquid orderStatus: …Rejected (tickRejected, iocCancelRejected…),
    # …Canceled (reduceOnlyCanceled, selfTradeCanceled…), scheduledCancel
    low = status.lower()
    if low.endswith('rejected'):
        return 'rejected'
    if low.endswith(('canceled', 'cancelled')) or low == 'scheduledcancel':
        return 'canceled'
    return cast(OrderStatus, status)  # do not lose an unknown status


def order(o: dict, market: Optional[Market], fallback: Optional[dict] = None) -> Order:
    fb = fallback or {}
    cs = _cs(market)
    fee_cost, fee_currency = _order_fee(o)
    filled = o.get('filled')
    hl_filled = _info(o).get('filled')
    if filled is None and isinstance(hl_filled, dict):
        filled = hl_filled.get('totalSz')  # Hyperliquid: filled amount of the order response
    return Order(
        id=str(o.get('id')) if o.get('id') is not None else _s(fb.get('id')),
        client_id=o.get('clientOrderId') or fb.get('client_id'),
        symbol=_s(o.get('symbol') or fb.get('symbol')),
        # None if the exchange did not return a side (cancel response): an honest None beats 'buy'
        side=o.get('side') or fb.get('side'),
        type=_s(o.get('type') or fb.get('type')),
        status=_order_status(o),
        price=_f(o.get('price')) if o.get('price') is not None else fb.get('price'),
        average=_f(o.get('average')),
        size=_mul(o.get('amount') if o.get('amount') is not None else fb.get('amount'), cs),
        filled=_mul(filled, cs),
        remaining=_mul(o.get('remaining'), cs),
        reduce_only=o.get('reduceOnly'), post_only=o.get('postOnly'),
        time_in_force=o.get('timeInForce'), trigger_price=_f(o.get('triggerPrice')),
        fee=fee_cost, fee_currency=fee_currency,
        timestamp=o.get('timestamp') or fb.get('submitted_at'), raw=o,
        contracts=_f(o.get('amount') if o.get('amount') is not None else fb.get('amount')),
        filled_contracts=_f(filled), remaining_contracts=_f(o.get('remaining')),
    )


def _order_fee(o: dict) -> tuple[Optional[float], Optional[str]]:
    """Order fee. If ccxt did not aggregate ``fee`` (several trades, e.g. a Binance Spot FULL
    response with ``fills``), sums ``fees`` in a single currency."""
    fee = o.get('fee') or {}
    cost = _f(fee.get('cost'))
    if cost is not None:
        return cost, fee.get('currency')
    fees = [f for f in o.get('fees') or [] if isinstance(f, dict) and _f(f.get('cost')) is not None]
    currencies = {f.get('currency') for f in fees}
    if not fees or len(currencies) != 1:
        return None, fee.get('currency')
    return sum(_f(f['cost']) or 0.0 for f in fees), currencies.pop()


def fill(t: dict, market: Optional[Market], exchange: Any = None) -> Fill:
    """ccxt trade → :class:`Fill`. ``exchange`` — ccxt id / SDK name / instance: the hedge position side
    (``position_side``) follows the exchange's rules (:func:`bytx.journal.origin.position_side_of_fill`);
    without it — the common fields ``positionSide`` / ``ps`` / ``posSide`` / ``positionIdx``."""
    fee = t.get('fee') or {}
    info = _info(t)
    taker = t.get('takerOrMaker')
    maker = None if taker is None else taker == 'maker'
    if maker is None and isinstance(info.get('crossed'), bool):
        maker = not info['crossed']  # Hyperliquid: crossed — the order crossed the spread (taker)
    order_id = t.get('order') if t.get('order') is not None else info.get('oid')  # HL WS: oid
    return Fill(
        id=t.get('id'), order_id=None if order_id is None else str(order_id),
        # an empty string (Bybit orderLinkId "") means "no client_id"; OKX puts the trade's client id
        # in clOrdId (ccxt parse_trade does not carry it over)
        client_id=(t.get('clientOrderId') or info.get('clientOrderId') or info.get('orderLinkId')
                   or info.get('clOrdId') or _binance_ws_cid(info) or _bingx_ws_cid(info) or None),
        symbol=_s(t.get('symbol')), side=t.get('side'), price=float(t['price']),
        size=float(t.get('amount') or 0) * _cs(market), fee=_f(fee.get('cost')),
        fee_currency=fee.get('currency'), maker=maker,
        timestamp=t.get('timestamp'), raw=t, contracts=float(t.get('amount') or 0),
        position_side=_fill_position_side(info, exchange),
    )


def _fill_position_side(info: dict, exchange: Any) -> Optional[PositionSide]:
    """The trade's hedge position side; None — one-way or the exchange doesn't report it in the trade (Bybit execution)."""
    if exchange is not None:
        from bytx.errors import ConfigError
        from bytx.journal.origin import position_side_of_fill
        try:
            return cast(Optional[PositionSide], position_side_of_fill(exchange, info))
        except ConfigError:
            pass   # an exchange without origin rules (paper, test venues) — common fields
    return hedge_side(info)[0]


def _bingx_ws_cid(info: dict) -> Optional[str]:
    """BingX swap WS trade (Order update push): client id in "c"; BingX shape — i, X, td."""
    if isinstance(info, dict) and 'c' in info and 'i' in info and 'X' in info and 'td' in info:
        value = info.get('c')
        return value if isinstance(value, str) and value else None
    return None


def _binance_ws_cid(info: dict) -> Optional[str]:
    """Binance USDⓈ-M WS trade: info is the ORDER_TRADE_UPDATE order object, client id in "c"
    (ccxt parse_ws_trade does not carry it over — demo 2026-10-29: a fill without client_id
    did not match its level). Binance shape only: i — order id, X — status, t — trade id."""
    if isinstance(info, dict) and 'c' in info and 'i' in info and 'X' in info and 't' in info:
        value = info.get('c')
        return value if isinstance(value, str) and value else None
    return None


def funding_rate(r: dict) -> FundingRate:
    return FundingRate(
        symbol=_s(r.get('symbol')), rate=_f(r.get('fundingRate')),
        next_funding_time=r.get('fundingTimestamp') or r.get('nextFundingTimestamp'),
        mark_price=_f(r.get('markPrice')), index_price=_f(r.get('indexPrice')),
        timestamp=r.get('timestamp'), raw=r,
    )


def open_interest(r: dict, market: Optional[Market]) -> OpenInterest:
    return OpenInterest(symbol=_s(r.get('symbol')), size=_mul(r.get('openInterestAmount'), _cs(market)),
                        value=_f(r.get('openInterestValue')), timestamp=r.get('timestamp'), raw=r)
