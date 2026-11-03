"""HTX: default fees for paper mode.

The API docs have no public rate table (personal — POST /linear-swap-api/v1/swap_fee, spot — GET
/v2/reference/transact-fee-rate). Values — ccxt 4.4.65 default rates (contracts maker 0.0002 / taker
0.0005, spot 0.002) — ``verified=False``.
"""
from __future__ import annotations

_SWAP = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#general-query-information-on-swap-trading-fee'
_SPOT = 'https://huobiapi.github.io/docs/spot/v1/en/#get-current-fee-rate-applied-to-the-user'
PAPER_FEES = {
    'swap': (0.0002, 0.0005, _SWAP, False),
    'spot': (0.002, 0.002, _SPOT, False),
}
