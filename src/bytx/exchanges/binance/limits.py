"""Binance: amount step/minimum and min notional from exchangeInfo filters.

USDⓈ-M docs «Common Definition → Filters»
(https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition):
``LOT_SIZE`` — ``minQty`` / ``stepSize``; ``MIN_NOTIONAL`` — ``{"notional": "5.0"}``
(notional = price × quantity, for MARKET — at mark price). Spot: ``NOTIONAL`` / ``MIN_NOTIONAL``
with ``minNotional``.
"""
from __future__ import annotations

from typing import Any, Optional


def _filters(market: dict) -> list:
    info = market.get('info') or {}
    return [f for f in info.get('filters') or [] if isinstance(f, dict)]


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum) amount from ``LOT_SIZE``; no filter — (None, None)."""
    for f in _filters(market):
        if f.get('filterType') == 'LOT_SIZE':
            return float(f['stepSize']), float(f['minQty'])
    return None, None


def apply_market_lot(market: dict, amount: float) -> float:
    """MARKET order: ``MARKET_LOT_SIZE`` (filters.md: quantity ≥ minQty, ≤ maxQty, a multiple of stepSize; MARKET only).

    The size goes down to ``stepSize`` (0 — no step rule); below ``minQty`` — :class:`OrderTooSmall`, above
    ``maxQty`` — :class:`InvalidOrder`, before sending. No filter — the size as is.
    """
    from decimal import ROUND_DOWN, Decimal

    from bytx.errors import InvalidOrder, OrderTooSmall
    f = next((f for f in _filters(market) if f.get('filterType') == 'MARKET_LOT_SIZE'), None)
    if f is None:
        return amount
    step, low, high = (Decimal(str(f.get(k) or 0)) for k in ('stepSize', 'minQty', 'maxQty'))
    value = Decimal(str(amount))
    if step > 0:
        value = (value / step).to_integral_value(rounding=ROUND_DOWN) * step
    symbol = market.get('symbol')
    if value <= 0 or (low > 0 and value < low):
        raise OrderTooSmall(f'size {amount} is below MARKET_LOT_SIZE (minQty {low}, stepSize {step}) ({symbol})',
                            exchange='binance')
    if high > 0 and value > high:
        raise InvalidOrder(f'size {amount} is above MARKET_LOT_SIZE.maxQty {high} ({symbol})', exchange='binance')
    return float(value)


def min_cost(market: dict) -> Optional[float]:
    """``MIN_NOTIONAL.minNotional`` (only it; futures ``notional`` is
    already in ``limits.cost.min`` via ccxt)."""
    for f in _filters(market):
        if f.get('filterType') == 'MIN_NOTIONAL' and f.get('minNotional') is not None:
            return _num(f['minNotional'])
    return None


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
