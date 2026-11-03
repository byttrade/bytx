"""Size units: margin × leverage → contracts, snapping to a whole lot, minimum notional.

Rules (lot step and minimum, minimum notional) follow exchange docs,
in ``bytx.exchanges.<exchange>.limits``; ``contracts_for_margin`` matches
``bytx.utils.calculate_contract_size`` bit for bit.

Rounding rule (shared by ``contracts_for_margin`` and ``Exchange.place_order``):

1. contracts = amount / (price × contract_size) in ``Decimal`` (in float 0.29 / 0.01 = 28.999…);
2. **snap to a whole lot**: if the value is below the nearest whole number of lots by no
   more than 1e-9 relative, that is the caller's float error
   (``k·lot·price/leverage·leverage``), so the whole lot is taken;
3. round down by step — the amount never grows above the requested one.

If the result is below the minimum, ``contracts_for_margin`` takes one step up
if that is not below the minimum; otherwise ``None``.
"""
from __future__ import annotations

import importlib
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from typing import Any, Optional

# ccxt id → bytx.exchanges package
_PACKAGES = {
    'binance': 'binance', 'binanceusdm': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx',
    'kucoinfutures': 'kucoin', 'kucoin': 'kucoin', 'hyperliquid': 'hyperliquid',
    'bitget': 'bitget', 'gate': 'gate', 'mexc': 'mexc', 'htx': 'htx',
}

# snapping tolerance (measured: float error ≤ 3e-16 relative)
SNAP_TOLERANCE = Decimal('1e-9')


def _limits(exchange_id: str) -> Any:
    package = _PACKAGES.get(exchange_id)
    if package is None:
        return None
    return importlib.import_module(f'bytx.exchanges.{package}.limits')


def lot_rules(exchange_id: str, market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum) amount in order units (contracts) per exchange rules.

    Unknown exchange or no data — (None, None)."""
    module = _limits(exchange_id)
    if module is None:
        return None, None
    try:
        return module.lot_rules(market)
    except (KeyError, TypeError, ValueError):
        return None, None


def min_cost(exchange_id: Optional[str], market: dict) -> Optional[float]:
    """Minimum notional (quote):
    the maximum of ccxt ``limits.cost.min`` and the exchange rule (Bybit ``minNotionalValue``,
    Binance ``MIN_NOTIONAL.minNotional``, Hyperliquid $10). None — no limit."""
    values = []
    ccxt_min = ((market.get('limits') or {}).get('cost') or {}).get('min')
    if ccxt_min is not None:
        values.append(float(ccxt_min))
    module = _limits(exchange_id) if exchange_id else None
    if module is not None:
        extra = module.min_cost(market)
        if extra is not None:
            values.append(float(extra))
    best = max(values) if values else 0.0
    return best if best > 0 else None


def snap_to_lot(contracts: Decimal, step: Decimal) -> Decimal:
    """A value below a whole number of lots by float error is snapped to it."""
    if contracts > 0 and step > 0:
        snapped = contracts.quantize(step, rounding=ROUND_UP)
        if snapped - contracts <= contracts * SNAP_TOLERANCE:
            return snapped
    return contracts


def snap_down(contracts: Decimal, step: Decimal) -> Decimal:
    """Snap to a whole lot, then round down by step (``step`` need not be a power of 10)."""
    if step <= 0 or contracts <= 0:
        return contracts
    lots = contracts / step
    whole = lots.to_integral_value(rounding=ROUND_UP)
    if whole - lots > lots * SNAP_TOLERANCE:
        whole = lots.to_integral_value(rounding=ROUND_DOWN)
    return whole * step


def contracts_for_margin(exchange_id: str, market: dict, margin: float, leverage: float,
                         price: float) -> Optional[float]:
    """Margin (quote) × leverage → contracts at ``price``; bit for bit
    ``bytx.utils.calculate_contract_size(usdt_volume=margin * leverage, …)``.

    None — below the exchange minimum even after a step up, or no lot rules."""
    try:
        usdt_volume = float(margin * leverage)
        price = float(price)
        contract_size = float(market.get('contractSize', 1.0))
    except (TypeError, ValueError):
        return None
    lot_step, min_qty = lot_rules(exchange_id, market)
    if lot_step is None or min_qty is None:
        return None
    try:
        contracts = Decimal(str(usdt_volume)) / (Decimal(str(price)) * Decimal(str(contract_size)))
        step = Decimal(str(lot_step)).normalize()
        minimum = Decimal(str(min_qty))
        contracts = snap_to_lot(contracts, step)
        down = contracts.quantize(step, rounding=ROUND_DOWN)
        if down < minimum:
            up = contracts.quantize(step, rounding=ROUND_UP)
            return float(up) if up >= minimum else None
        return float(down)
    except (InvalidOperation, ValueError, ZeroDivisionError):
        return None


__all__ = ['lot_rules', 'min_cost', 'snap_to_lot', 'snap_down', 'contracts_for_margin', 'SNAP_TOLERANCE']
