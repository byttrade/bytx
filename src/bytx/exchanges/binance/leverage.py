"""Binance USDⓈ-M: applied leverage from the change-leverage response.

Docs «Change Initial Leverage» (POST /fapi/v1/leverage): response
``{"leverage": 21, "maxNotionalValue": "1000000", "symbol": "BTCUSDT"}`` — the leverage the
exchange actually set (a cap by the symbol's tier shows up as a smaller value).
"""
from __future__ import annotations

from typing import Any, Optional


def applied_leverage(response: Any) -> Optional[float]:
    """``leverage`` from the POST /fapi/v1/leverage response; None — field missing."""
    if not isinstance(response, dict):
        return None
    info = response.get('info') if isinstance(response.get('info'), dict) else {}
    return _num(response.get('leverage', info.get('leverage')))


def _num(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, '') else None
    except (TypeError, ValueError):
        return None
