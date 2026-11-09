"""BloFin: leverage and position mode.

- Set Leverage (F#set-leverage): response ``data`` — ``leverage``, ``marginMode``, ``instId``, ``positionSide``.
- Get Position Mode (F#get-position-mode): ``positionMode`` — net_mode | long_short_mode, ``multiPosition`` —
  true | false (the docs example is the string "true", demo 2026-11-09 — boolean false). The mode is per account, for
  all contracts. Multi-Position (F#multi-position-mode): every opening order is its own position with ``positionId``;
  close / TP/SL / close-position require ``positionId`` — the SDK does not trade that way: ConfigError before trading.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import ConfigError, ExchangeError

DOC = 'https://docs.blofin.com/index.html#get-position-mode'


def applied_leverage(response: Any) -> Optional[float]:
    data = response.get('data') if isinstance(response, dict) else None
    try:
        return float(data['leverage']) if isinstance(data, dict) else None
    except (KeyError, TypeError, ValueError):
        return None


def hedge_of(response: Any, exchange: str = 'blofin') -> bool:
    """Get Position Mode response → hedge (long_short_mode); Multi-Position — ConfigError; no mode — ExchangeError."""
    data = response.get('data') if isinstance(response, dict) else None
    data = data if isinstance(data, dict) else {}
    if str(data.get('multiPosition')).lower() == 'true':
        raise ConfigError('BloFin: Multi-Position is enabled on the account (each entry is a separate position with '
                          'positionId) — the SDK does not support it; turn Multi-Position off in the BloFin settings',
                          exchange=exchange,
                          details={'position_mode': data})
    mode = data.get('positionMode')
    if mode not in ('net_mode', 'long_short_mode'):
        raise ExchangeError(f'position mode is unknown: positionMode={mode!r}', exchange=exchange)
    return mode == 'long_short_mode'


async def position_mode(ex: Any) -> bool:
    return hedge_of(await ex._call(ex.ccxt.request, 'account/position-mode', 'private', 'GET', {}), ex.name)
