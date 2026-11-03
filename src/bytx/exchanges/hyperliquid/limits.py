"""Hyperliquid: amount step from szDecimals and a $10 minimum order value.

Docs «Tick and lot size» (https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size):
size step = 10^-szDecimals; an order below $10 gets «Order must have minimum value of $10.»
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

MIN_ORDER_VALUE = 10.0


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum): no step — 0.0001, no minimum — the step."""
    lot_step = (market.get('precision') or {}).get('amount', 0)
    min_qty = ((market.get('limits') or {}).get('amount') or {}).get('min', 0)
    step = Decimal(str(lot_step)) if lot_step and lot_step > 0 else Decimal('0.0001')
    minimum = Decimal(str(min_qty)) if min_qty else step
    return float(step), float(minimum)


def min_cost(market: dict) -> Optional[float]:
    return MIN_ORDER_VALUE
