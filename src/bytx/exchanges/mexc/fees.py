"""MEXC: default fees for paper mode.

The API docs have no tier table; rates are contract fields ``takerFeeRate`` / ``makerFeeRate`` («taker rate» /
«maker rate», GET /api/v1/contract/detail) and spot pair ``makerCommission`` / ``takerCommission``
(GET /api/v3/exchangeInfo). Values — public BTC_USDT / BTCUSDT responses (live check 2026-10-26: contract
maker 0, taker 0.0002; spot maker 0, taker 0.0005) — ``verified=False``; futures API trading has a «separate fee
structure» (Broker FAQ).
"""
from __future__ import annotations

_CONTRACT = 'https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-contract-information'
_SPOT = 'https://mexcdevelop.github.io/apidocs/spot_v3_en/#exchange-information'
PAPER_FEES = {
    'swap': (0.0, 0.0002, _CONTRACT, False),
    'spot': (0.0, 0.0005, _SPOT, False),
}
