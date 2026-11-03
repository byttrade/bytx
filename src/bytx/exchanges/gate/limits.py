"""Gate USDT-M: amount in contracts (integer), ``quanto_multiplier`` — base per contract.

Contract model: ``order_size_min`` / ``order_size_max`` — «Minimum/Maximum order size allowed by the contract»
(int), ``quanto_multiplier`` — «Multiplier used in converting from invoicing to settlement currency»;
FuturesOrder.size — int. ccxt: precision.amount = 1, limits.amount.min = order_size_min, contractSize =
quanto_multiplier. Contracts have no minimum notional.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    return (float((market.get('precision') or {}).get('amount') or 1),
            float(((market.get('limits') or {}).get('amount') or {}).get('min') or 1))


def min_cost(market: dict) -> Optional[float]:
    if market.get('contract'):
        return None
    value = ((market.get('limits') or {}).get('cost') or {}).get('min')   # spot: min_quote_amount
    return float(value) if value else None
