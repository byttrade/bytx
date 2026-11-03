"""OKX: position tiers — «Get position tiers», GET /api/v5/public/position-tiers.

Docs (https://www.okx.com/docs-v5/en/#public-data-rest-api-get-position-tiers): ``instType`` and
``tdMode`` are required, ``instFamily`` is required for SWAP/FUTURES (ccxt 4.4.65 sends the deprecated
``uly`` — hence our own request). Response: ``tier``, ``minSz``/``maxSz`` — position in contracts,
``mmr`` — maintenance margin rate, ``imr``, ``maxLever``. Bounds → base: × ``ctVal``.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier


def parse(rows: list, ct_val: float) -> list[LeverageTier]:
    def base(v: Any) -> Optional[float]:
        n = num(v)
        return None if n is None else n * ct_val

    return sorted((LeverageTier(
        tier=int(r['tier']), max_leverage=num(r.get('maxLever')), maintenance_margin_rate=num(r.get('mmr')),
        min_size=base(r.get('minSz')), max_size=base(r.get('maxSz')), raw=r) for r in rows or []),
        key=lambda t: t.tier)


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    info = market.get('info') or {}
    family = info.get('instFamily') or info.get('uly')
    inst_type = 'FUTURES' if market.get('future') else 'SWAP'
    request = {'instType': inst_type, 'tdMode': ex.margin_mode_of(s) or 'cross', 'instFamily': family}
    response = await ex._call(ex.ccxt.publicGetPublicPositionTiers, request)
    return parse((response or {}).get('data') or [], float(market.get('contractSize') or 1.0))
