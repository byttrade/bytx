"""KuCoin Futures: amount in lots (contracts), ``multiplier`` — contract size.

Docs «Get All Contracts» (GET /api/v1/contracts/active): ``lotSize`` — step and minimum,
``multiplier`` — base per contract. ccxt: ``precision.amount`` / ``limits.amount.min``.
KuCoin futures have no minimum notional.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    return (float((market.get('precision') or {}).get('amount', 1)),
            float(((market.get('limits') or {}).get('amount') or {}).get('min', 1)))


def min_cost(market: dict) -> Optional[float]:
    return None
