"""KuCoin: default fees for paper mode (LV0).

Futures: maker 0.02%, taker 0.06% (https://www.kucoin.com/vip/privilege; docs response example
«Get Actual Fee - Futures» — takerFeeRate 0.0006, makerFeeRate 0.0002); class A spot: 0.1% / 0.1%.
The tier table is script-rendered; during verification on 2026-10-26 there were no values in the text — ``verified=False``.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.0002, 0.0006, 'https://www.kucoin.com/vip/privilege', False),
    'spot': (0.001, 0.001, 'https://www.kucoin.com/vip/privilege', False),
}
