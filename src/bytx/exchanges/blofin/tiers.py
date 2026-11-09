"""BloFin USDT-M: risk tiers — «GET Position Tiers», GET /api/v1/market/position-tiers (public;
F#get-position-tiers): ``instId`` and ``marginMode`` are required. Fields: ``minSize`` / ``maxSize`` — «position size»
(the docs do not name the unit; demo 2026-11-09 — contracts: DOGE-USDT tier 1 up to 1500 contracts of 1000 DOGE,
``tests/ground_truth/blofin/demo_position_tiers.json``) → base × ``contractValue``; ``maintenanceMarginRate``,
``maxLeverage``. No tier number — by order.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier


def parse(rows: list, contract_size: float) -> list[LeverageTier]:
    def base(v: Any) -> Optional[float]:
        n = num(v)
        return None if n is None else n * contract_size

    ordered = sorted(rows or [], key=lambda r: num(r.get('minSize')) or 0.0)
    return [LeverageTier(tier=i + 1, max_leverage=num(r.get('maxLeverage')),
                         maintenance_margin_rate=num(r.get('maintenanceMarginRate')),
                         min_size=base(r.get('minSize')), max_size=base(r.get('maxSize')), source='exchange', raw=r)
            for i, r in enumerate(ordered)]


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    request = {'instId': market['id'], 'marginMode': ex.margin_mode_of(s) or 'cross'}
    response = await ex._call(ex.ccxt.request, 'market/position-tiers', 'public', 'GET', request)
    return parse((response or {}).get('data') or [], float(market.get('contractSize') or 1.0))
