"""Gate USDT-M: position leverage and margin mode.

Position model (https://github.com/gateio/gateapi-python/blob/master/docs/Position.md): ``leverage`` —
«Position leverage. 0 means cross margin; positive number means isolated margin», ``cross_leverage_limit`` —
«Cross margin leverage (valid only when leverage is 0)». Change — POST /futures/{settle}/positions/{contract}/leverage
(``leverage`` «New position leverage», ``cross_leverage_limit`` «valid only when leverage is 0»): cross — leverage 0 +
cross_leverage_limit, isolated — leverage N. ccxt ``set_leverage`` without ``marginMode`` sends leverage N — the position
switches to isolated; ``fetch_leverage`` for swap — NotSupported; ccxt gate has no ``set_margin_mode``.
"""
from __future__ import annotations

from typing import Any, Optional

DOC = 'https://github.com/gateio/gateapi-python/blob/master/docs/FuturesApi.md#update_position_leverage'


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def margin_mode(position: Any) -> Optional[str]:
    lev = _num(position.get('leverage')) if isinstance(position, dict) else None
    if lev is None:
        return None
    return 'cross' if lev == 0 else 'isolated'


def applied_leverage(response: Any) -> Optional[float]:
    """Position leverage from the Position response: isolated — ``leverage``, cross — ``cross_leverage_limit``."""
    if not isinstance(response, dict):
        return None
    mode = margin_mode(response)
    if mode is None:
        return None
    value = _num(response.get('cross_leverage_limit')) if mode == 'cross' else _num(response.get('leverage'))
    return value or None


def leverage_params(mode: str, leverage: Any) -> dict:
    """ccxt ``set_leverage`` params: cross — ``cross_leverage_limit`` (ccxt sends leverage 0), isolated — none.
    ``marginMode`` is not passed: ccxt 4.4.65 reads it but leaves it in the request as a stray parameter."""
    if mode == 'cross':
        value = float(leverage)
        return {'cross_leverage_limit': str(int(value)) if value == int(value) else str(value)}
    return {}


async def current(ex: Any, s: str) -> tuple[Optional[str], Optional[float]]:
    """(mode, leverage) of the contract position — GET /futures/{settle}/positions/{contract} («Get single position»)."""
    m = ex.ccxt.market(s)
    settle = str(m.get('settleId') or 'usdt').lower()
    row = await ex._call(ex.ccxt.privateFuturesGetSettlePositionsContract, {'settle': settle, 'contract': m['id']})
    return margin_mode(row), applied_leverage(row)
