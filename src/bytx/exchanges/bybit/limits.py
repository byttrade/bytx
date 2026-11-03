"""Bybit: amount step/minimum and min notional from instruments-info.

Docs «Get Instruments Info» (https://bybit-exchange.github.io/docs/v5/market/instrument),
linear: ``lotSizeFilter.qtyStep`` / ``minOrderQty`` / ``minNotionalValue`` («minimum notional
value»). ccxt 4.4.65 doesn't parse ``minNotionalValue`` (``limits.cost.min`` = None) — it is
added by :func:`min_cost`.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum) from ``lotSizeFilter``: default 0.001."""
    lot = (market.get('info') or {}).get('lotSizeFilter', {})
    return float(lot.get('qtyStep', 0.001)), float(lot.get('minOrderQty', 0.001))


def min_cost(market: dict) -> Optional[float]:
    value = ((market.get('info') or {}).get('lotSizeFilter') or {}).get('minNotionalValue')
    try:
        return None if value in (None, '') else float(value)
    except (TypeError, ValueError):
        return None
