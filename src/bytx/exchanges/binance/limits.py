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
