"""KuCoin Futures: risk tiers — «Get Isolated Margin Risk Limit»,
GET /api/v1/contracts/risk-limit/{symbol}, public (api-channel Public).

Docs (https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-isolated-margin-risk-limit):
``level``, ``minRiskLimit``/``maxRiskLimit`` (notional), ``maxLeverage``, ``maintainMargin`` (fraction:
0.004 for XBTUSDTM level 1). Parsed by ccxt ``fetch_market_leverage_tiers``.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._tiers_common import from_ccxt
from bytx.types import LeverageTier


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    return from_ccxt(await ex._call(ex.ccxt.fetch_market_leverage_tiers, s))
