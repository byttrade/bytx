"""KuCoin Futures: account position mode — GET /api/v2/position/getPositionMode («Get Position Mode»).

Docs https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-mode: ``positionMode``
(integer) — «Position mode setting applies to all futures trading pairs.» «0 -- One-Way Mode (one-way
position), 1 -- Hedge Mode (two-way position)». Hedge — since changelog 2025.09; in Add Order ``positionSide`` —
«optional in one-way mode, defaults to BOTH. Required in hedge mode.»

The SDK trades KuCoin in one-way mode only (doesn't send ``positionSide``): a hedge account gets :class:`ConfigError` before
the order is sent. The mode is NOT switched automatically (Switch Position Mode is an action on the user's
account). ccxt 4.4.65 doesn't know the endpoint — ``ccxt.request`` (instance signature, version v2).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import ConfigError, ExchangeError
from bytx.exchanges.kucoin.error_codes import POSITION_MODE_HINT

DOCS = 'https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-mode'
PATH = 'position/getPositionMode'
# «0 -- One-Way Mode (one-way position), 1 -- Hedge Mode (two-way position)»; True — hedge
MODES = {'0': False, '1': True}


def parse(response: Any) -> Optional[bool]:
    """True — hedge, False — one-way, None — no ``positionMode`` or a value outside the docs."""
    data = (response or {}).get('data') if isinstance(response, dict) else None
    mode = data.get('positionMode') if isinstance(data, dict) else None
    return MODES.get(str(mode)) if mode is not None else None


async def read(ex: Any) -> bool:
    """Account position mode (True — hedge). Not read — ExchangeError, not a silent one-way."""
    raw = await ex._call(ex.ccxt.request, PATH, 'futuresPrivate', 'GET', {'version': 'v2'})
    hedge = parse(raw)
    if hedge is None:
        raise ExchangeError(f'KuCoin position mode is undetermined: {PATH} → {raw!r}', exchange=ex.name,
                            details={'response': raw})
    return hedge


def hedge_account_error(ex: Any) -> ConfigError:
    return ConfigError(f'KuCoin Futures account is in hedge mode (positionMode=1): {POSITION_MODE_HINT}',
                       exchange=ex.name, details={'position_mode': 'hedge', 'docs': DOCS})
