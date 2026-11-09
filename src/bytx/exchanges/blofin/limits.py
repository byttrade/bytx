"""BloFin USDT-M: size in contracts.

Docs «GET Instruments» (F#get-instruments): ``contractValue`` — base currency per contract (BTC-USDT 0.001),
``minSize`` — «Minimum order size» (contracts), ``lotSize`` — step (contracts). ccxt 4.4.65 puts them into
``precision.amount`` / ``limits.amount.min``. There is no minimum notional.
"""
from __future__ import annotations

from typing import Optional


def lot_rules(market: dict) -> tuple[Optional[float], Optional[float]]:
    step = (market.get('precision') or {}).get('amount')
    low = ((market.get('limits') or {}).get('amount') or {}).get('min')
    return (float(step) if step else None), (float(low) if low else None)


def min_cost(market: dict) -> Optional[float]:
    return None
