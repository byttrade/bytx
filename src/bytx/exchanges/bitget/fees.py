"""Bitget: default fees for paper mode.

Futures: «Get Trade Rate» example (GET /api/v2/common/trade-rate, businessType=mix) — makerFeeRate 0.0002,
takerFeeRate 0.0006 («Fractional form»). Spot: the API docs have no tier table (Get Symbol
Info example — 0.002 for one symbol), standard 0.1% / 0.1% is used — ``verified=False``.
"""
from __future__ import annotations

_RATE = 'https://www.bitget.com/docs/catalog/classic-common-public/classic-common#get-trade-rate'
PAPER_FEES = {
    'swap': (0.0002, 0.0006, _RATE, False),
    'spot': (0.001, 0.001, _RATE, False),
}
