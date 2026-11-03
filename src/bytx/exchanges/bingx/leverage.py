"""BingX: applied leverage from the Set Leverage response.

Docs «Set Leverage» (POST /openApi/swap/v2/trade/leverage): response
``{"code": 0, "msg": "", "data": {"leverage": 8, "symbol": "ETH-USDT"}}``.
"""
from __future__ import annotations

from typing import Any, Optional


def applied_leverage(response: Any) -> Optional[float]:
    """Response ``data.leverage``; None — field missing."""
    if not isinstance(response, dict):
        return None
    data = response.get('data')
    if not isinstance(data, dict):
        return None
    try:
        return float(data['leverage'])
    except (KeyError, TypeError, ValueError):
        return None
