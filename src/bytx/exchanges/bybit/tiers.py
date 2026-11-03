"""Bybit: risk tiers — «Get Risk Limit», GET /v5/market/risk-limit, public.

Docs (https://bybit-exchange.github.io/docs/v5/market/risk-limit): ``id`` — tier, ``riskLimitValue``
— «Position limit» (upper notional bound of the tier), ``maintenanceMargin`` — «Maintain margin rate»,
``maxLeverage``. Parsed by ccxt ``fetch_market_leverage_tiers`` (lower bound = previous tier's bound).
The docs response example (inverse BTCUSD: maintenanceMargin "0.5", initialMargin "1" with maxLeverage 100)
is in percent, while the live linear response is a fraction (0.005 for BTCUSDT, measured 2026-08-16);
the rate is returned as is.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._tiers_common import from_ccxt
from bytx.types import LeverageTier


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    return from_ccxt(await ex._call(ex.ccxt.fetch_market_leverage_tiers, s))
