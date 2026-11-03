"""OKX: amount step/minimum from /public/instruments.

Docs «Get instruments» (https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments):
``lotSz`` — step, ``minSz`` — minimum (in contracts for SWAP). OKX has no minimum
notional — ``min_cost`` is not set.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    """(step, minimum) amount: ``lotSz`` / ``minSz``."""
    info = market.get('info') or {}
    precision = market.get('precision') or {}
    limits = (market.get('limits') or {}).get('amount') or {}
    if info:
        return float(info.get('lotSz', precision.get('amount', 0.01))), float(info.get('minSz', limits.get('min', 0.01)))
    return float(precision.get('amount', 0.01)), float(limits.get('min', 0.01))


def min_cost(market: dict) -> Optional[float]:
    return None
