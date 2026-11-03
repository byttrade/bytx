"""Binance: default fees for paper mode (Regular / VIP 0 tier, no BNB discount).

USDⓈ-M: maker 0.02%, taker 0.05% — docs «Binance Futures Trading Fees Explained»
(https://www.binance.com/en/support/faq/binance-futures-trading-fees-explained-360033544231:
«a Regular User's maker fee is 0.02% and a Regular User's taker fee is 0.05%»), verified 2026-10-26.
Spot: 0.1% / 0.1% (https://www.binance.com/en/fee/trading) — the page is dynamic, not verified
offline: ``verified=False``. Your own tier — ``PaperConfig(fees=...)``.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.0002, 0.0005, 'https://www.binance.com/en/support/faq/binance-futures-trading-fees-explained-360033544231', True),
    'spot': (0.001, 0.001, 'https://www.binance.com/en/fee/trading', False),
}
