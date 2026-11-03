"""Bitget USDT-M: risk tiers — «Get Position Tier», GET /api/v2/mix/market/query-position-lever
(public; docs https://www.bitget.com/docs/catalog/classic-contract-position/classic-contract-position#get-position-tier).

Fields: ``level``, ``startUnit`` / ``endUnit`` («Minimum value» / «Maximum value» — the docs don't name the
unit; the BTCUSDT example 0–50000 / 50000–200000 is USDT notional), ``leverage``, ``keepMarginRate``.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier

PRODUCT_TYPES = {'USDT': 'USDT-FUTURES', 'USDC': 'USDC-FUTURES'}


def parse(response: dict) -> list[LeverageTier]:
    out = []
    for i, r in enumerate((response or {}).get('data') or []):
        level = num(r.get('level'))
        out.append(LeverageTier(tier=int(level) if level is not None else i + 1, max_leverage=num(r.get('leverage')),
                                maintenance_margin_rate=num(r.get('keepMarginRate')),
                                min_notional=num(r.get('startUnit')), max_notional=num(r.get('endUnit')),
                                source='exchange', raw=r))
    return sorted(out, key=lambda t: t.tier)


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    request = {'symbol': market['id'], 'productType': PRODUCT_TYPES.get(str(market.get('settle')), 'COIN-FUTURES')}
    return parse(await ex._call(ex.ccxt.publicMixGetV2MixMarketQueryPositionLever, request))
