"""BingX: amount step/minimum — ccxt-parsed ``precision.amount`` / ``limits.amount.min``.

Docs «USDT-M Perp Futures symbols» (GET /openApi/swap/v2/quote/contracts): ``quantityPrecision``,
``tradeMinQuantity``, ``tradeMinUSDT``; ccxt puts ``tradeMinUSDT`` into ``limits.cost.min``.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    return (float((market.get('precision') or {}).get('amount', 0.001)),
            float(((market.get('limits') or {}).get('amount') or {}).get('min', 0.001)))


def min_cost(market: dict) -> Optional[float]:
    return None
