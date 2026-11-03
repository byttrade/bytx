"""Hyperliquid: margin tiers from ``meta`` (POST /info {"type": "meta"}).

Docs «Margin tiers» (https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margin-tiers):
margin tables — ``marginTables`` of the meta response, ``[id, {marginTiers: [{lowerBound, maxLeverage}]}]``;
«For IDs less than 50, there is a single tier with max leverage equal to the ID»;
``maintenance_margin_rate = (Initial Margin Rate at Maximum leverage) / 2`` = 1 / (2 · maxLeverage).
An asset references a table via the ``marginTableId`` field in ``universe`` (the docs example lacks the field —
then a single tier with the asset's ``maxLeverage``).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier


def _tier(n: int, lower: Optional[float], upper: Optional[float], lev: Optional[float], raw: dict) -> LeverageTier:
    return LeverageTier(tier=n, max_leverage=lev, maintenance_margin_rate=(1 / (2 * lev)) if lev else None,
                        min_notional=lower, max_notional=upper, raw=raw)


def parse(meta: dict, coin: str) -> list[LeverageTier]:
    asset = next((a for a in meta.get('universe') or [] if a.get('name') == coin), None)
    if asset is None:
        return []
    table_id = asset.get('marginTableId')
    tables = {int(t[0]): t[1] for t in meta.get('marginTables') or [] if isinstance(t, list) and len(t) == 2}
    if table_id is not None and int(table_id) in tables:
        tiers = tables[int(table_id)].get('marginTiers') or []
        bounds = [num(t.get('lowerBound')) for t in tiers]
        return [_tier(i + 1, bounds[i], bounds[i + 1] if i + 1 < len(bounds) else None,
                      num(t.get('maxLeverage')), t) for i, t in enumerate(tiers)]
    lev = num(table_id) if table_id is not None and int(table_id) < 50 else num(asset.get('maxLeverage'))
    return [_tier(1, 0.0, None, lev, asset)]


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    meta = await ex._call(ex.ccxt.publicPostInfo, {'type': 'meta'})
    return parse(meta or {}, str(market.get('baseName') or (market.get('info') or {}).get('name') or market['base']))
