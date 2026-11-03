"""MEXC futures: amount in contracts, ``contractSize`` — base per contract.

Get the contract information: ``volUnit`` — «volume unit», ``minVol`` — «minimum volume», ``contractSize`` —
«contract value» (ccxt: precision.amount = volUnit, limits.amount.min = minVol). Contracts have no minimum
notional; spot — ``quoteAmountPrecision`` (minimum quote amount, ccxt limits.cost.min).
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    return (float((market.get('precision') or {}).get('amount') or 1),
            float(((market.get('limits') or {}).get('amount') or {}).get('min') or 1))


def min_cost(market: dict) -> Optional[float]:
    if market.get('contract'):
        return None
    value = ((market.get('limits') or {}).get('cost') or {}).get('min')
    return float(value) if value else None
