"""Bybit V5: leverage per position side.

Docs «Get Position Info» (GET /v5/position/list): with ``symbol`` records are returned
«regardless of having position or not»; ``positionIdx`` 0 — one-way, 1 — hedge Buy (long),
2 — hedge Sell (short); ``leverage`` — String. ccxt ``fetch_leverage`` takes one position
(the first one in hedge mode), so we read the per-side leverage ourselves.
"""
from __future__ import annotations

from typing import Any, Optional

_IDX_SIDE = {'1': 'long', '2': 'short'}


def leverage_by_side(response: Any) -> dict[str, float]:
    """``{'long': x, 'short': y}`` from the /v5/position/list response (one-way — both sides)."""
    result = response.get('result') if isinstance(response, dict) else None
    rows = result.get('list') if isinstance(result, dict) else None
    out: dict[str, float] = {}
    for row in rows if isinstance(rows, list) else []:
        value = _num(row.get('leverage')) if isinstance(row, dict) else None
        if value is None:
            continue
        side = _IDX_SIDE.get(str(row.get('positionIdx')))
        for s in (side,) if side else ('long', 'short'):
            out[s] = value
    return out


def position_mode(response: Any) -> Optional[bool]:
    """Position mode from the /v5/position/list response: True — hedge (positionIdx 1/2), False — one-way
    (positionIdx 0), None — no records (without symbol Bybit returns only positions with size > 0).

    Bybit V5 has no separate mode endpoint; the mode is set at symbol > coin > default level
    (Switch Position Mode docs), so it is read per symbol."""
    result = response.get('result') if isinstance(response, dict) else None
    rows = result.get('list') if isinstance(result, dict) else None
    idx = {str(row.get('positionIdx')) for row in rows or [] if isinstance(row, dict)}
    if idx & set(_IDX_SIDE):
        return True
    if '0' in idx:
        return False
    return None


def category(market: dict) -> str:
    """V5 category for a ccxt market: spot / linear / inverse."""
    if market.get('spot'):
        return 'spot'
    return 'inverse' if market.get('inverse') else 'linear'


def _num(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, '') else None
    except (TypeError, ValueError):
        return None
