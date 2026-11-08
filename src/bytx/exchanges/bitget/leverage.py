"""Bitget: leverage — applied from the Change Leverage response and current from Get Single Account.

POST /api/v2/mix/account/set-leverage — response ``data``: ``longLeverage``, ``shortLeverage``,
``crossMarginLeverage``, ``marginMode`` (crossed | isolated).

GET /api/v2/mix/account/account (Get Single Account,
https://www.bitget.com/docs/catalog/classic-contract-account/classic-contract-account#get-single-account):
``crossedMarginLeverage`` — «Leverage in cross margin mode», ``isolatedLongLever`` / ``isolatedShortLever`` —
«Leverage of long/short positions in isolated margin mode», ``marginMode`` — the symbol's mode (isolated | crossed).
ccxt 4.4.65 ``parse_leverage`` always returns isolated* (marginMode 'isolated'): in cross the leverage came from the
other pocket (demo 2026-11-04: ETH cross 2, isolated 10 — ``leverage()`` returned 10).
"""
from __future__ import annotations

from typing import Any, Optional

_MODES = {'crossed': 'cross', 'cross': 'cross', 'isolated': 'isolated'}


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


def account_margin_mode(data: Any) -> Optional[str]:
    """The symbol's margin mode from Get Single Account ``data``: 'cross' / 'isolated'; no field — None."""
    return _MODES.get(str((data or {}).get('marginMode') or '').lower()) if isinstance(data, dict) else None


def leverage_pocket(data: Any, mode: Optional[str] = None) -> tuple[Optional[str], Any, Any]:
    """(mode, long leverage, short leverage) of the ``mode`` pocket ('cross' / 'isolated'); None — the symbol's mode
    from the response.

    cross — one leverage for both sides (``crossedMarginLeverage``), isolated — per side.
    """
    if not isinstance(data, dict):
        return mode, None, None
    mode = mode or account_margin_mode(data)
    if mode == 'cross':
        value = data.get('crossedMarginLeverage')
        return mode, value, value
    if mode == 'isolated':
        return mode, data.get('isolatedLongLever'), data.get('isolatedShortLever')
    return mode, None, None
