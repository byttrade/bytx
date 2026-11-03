"""Bitget: applied leverage from the Change Leverage response.

POST /api/v2/mix/account/set-leverage — response ``data``: ``longLeverage``, ``shortLeverage``,
``crossMarginLeverage``, ``marginMode`` (crossed | isolated).
"""
from __future__ import annotations

from typing import Any, Optional


def applied_leverage(response: Any) -> Optional[float]:
    data = response.get('data') if isinstance(response, dict) else None
    if not isinstance(data, dict):
        return None
    key = 'crossMarginLeverage' if data.get('marginMode') == 'crossed' else 'longLeverage'
    for name in (key, 'longLeverage', 'crossMarginLeverage'):
        try:
            return float(data[name])
        except (KeyError, TypeError, ValueError):
            continue
    return None
