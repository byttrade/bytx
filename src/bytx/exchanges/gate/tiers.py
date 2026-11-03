"""Gate USDT-M: risk tiers — GET /futures/{settle}/risk_limit_tiers (public).

FuturesLimitRiskTiers model (https://github.com/gateio/gateapi-python/blob/master/docs/FuturesLimitRiskTiers.md): ``tier``, ``risk_limit`` — «Position risk limit»
(upper tier bound, in settle currency), ``initial_rate``, ``maintenance_rate``, ``leverage_max``.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._tiers_common import num
from bytx.types import LeverageTier


def parse(rows: list) -> list[LeverageTier]:
    out = []
    lower = 0.0
    for i, r in enumerate(sorted(rows or [], key=lambda x: num(x.get('tier')) or 0)):
        upper = num(r.get('risk_limit'))
        tier = num(r.get('tier'))
        out.append(LeverageTier(tier=int(tier) if tier is not None else i + 1, max_leverage=num(r.get('leverage_max')),
                                maintenance_margin_rate=num(r.get('maintenance_rate')), min_notional=lower,
                                max_notional=upper, source='exchange', raw=r))
        lower = upper if upper is not None else lower
    return out


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    settle = str(market.get('settleId') or market.get('settle') or 'usdt').lower()
    rows = await ex._call(ex.ccxt.publicFuturesGetSettleRiskLimitTiers, {'settle': settle, 'contract': market['id']})
    return parse(rows if isinstance(rows, list) else [])
