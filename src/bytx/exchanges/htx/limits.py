"""HTX USDT-M: amount in contracts (integer), ``contract_size`` — base per contract.

[General] Query Swap Info: ``contract_size`` — «Contract Value (USDT of one contract)»; Place An Order: ``volume`` —
«Numbers of orders (volume)», long — step and minimum 1 contract (ccxt_fixes). Contracts have no minimum
notional; spot — ``min-order-value`` (ccxt limits.cost.min).
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
