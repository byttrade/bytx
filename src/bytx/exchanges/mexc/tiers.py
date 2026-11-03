"""MEXC futures: risk tiers — from the contract record (GET /api/v1/contract/detail, public).

Fields: ``riskBaseVol`` — «initial volume», ``riskIncrVol`` — «risk increasing volume», ``riskIncrMmr`` /
``riskIncrImr`` — «maintain / initial increasing margin rate», ``riskLevelLimit`` — «risk level limit»,
``maintenanceMarginRate`` / ``initialMarginRate`` — first-level rates (volumes — in contracts). Level i
(0…limit−1): volume up to riskBaseVol + i·riskIncrVol, mmr = maintenanceMarginRate + i·riskIncrMmr, imr =
initialMarginRate + i·riskIncrImr, leverage = 1/imr (at most maxLeverage). Bounds — in base (× contractSize).
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier


def parse(info: dict) -> list[LeverageTier]:
    base, incr = num(info.get('riskBaseVol')), num(info.get('riskIncrVol')) or 0.0
    mmr0, imr0 = num(info.get('maintenanceMarginRate')), num(info.get('initialMarginRate'))
    d_mmr, d_imr = num(info.get('riskIncrMmr')) or 0.0, num(info.get('riskIncrImr')) or 0.0
    levels = int(num(info.get('riskLevelLimit')) or 1)
    size = num(info.get('contractSize')) or 1.0
    max_lev = num(info.get('maxLeverage'))
    if base is None or mmr0 is None or imr0 is None:
        return []
    out = []
    lower = 0.0
    for i in range(max(levels, 1)):
        upper = (base + i * incr) * size
        imr = imr0 + i * d_imr
        lev = 1 / imr if imr > 0 else None
        if lev is not None and max_lev:
            lev = min(lev, max_lev)
        out.append(LeverageTier(tier=i + 1, max_leverage=lev, maintenance_margin_rate=mmr0 + i * d_mmr,
                                min_size=lower, max_size=upper, source='exchange', raw=dict(info, level=i + 1)))
        lower = upper
    return out


async def leverage_tiers(ex: Any, s: str, market: dict) -> list:
    return parse(market.get('info') or {})
