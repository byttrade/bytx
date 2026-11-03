"""Bitget USDT-M: amount in base coin (contractSize 1).

Docs «Get Contract Config» (GET /api/v2/mix/market/contracts): ``minTradeNum`` — «Minimum opening amount
(base currency)», ``sizeMultiplier`` — «the quantity of the order must be greater than minTradeNum and is a
multiple of sizeMulti», ``minTradeUSDT`` — «Minimum USDT transaction amount». Step — ``sizeMultiplier``
(ccxt 4.4.65 takes the step from minTradeNum — fixed in ``ccxt_fixes``), minimum — ``minTradeNum``.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    step = (market.get('precision') or {}).get('amount')
    low = ((market.get('limits') or {}).get('amount') or {}).get('min')
    return (float(step) if step else None), (float(low) if low else None)


def min_cost(market: dict) -> Optional[float]:
    value = ((market.get('limits') or {}).get('cost') or {}).get('min')
    return float(value) if value else None
