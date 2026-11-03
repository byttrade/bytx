"""Hyperliquid: default fees for paper mode (Tier 0, base rate).

Perpetuals: taker 0.045%, maker 0.015%; spot: taker 0.070%, maker 0.040% — docs «Fees»
(https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees), verified 2026-10-26.
Builder fee (if enabled) is not charged in paper.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.00015, 0.00045, 'https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees', True),
    'spot': (0.0004, 0.0007, 'https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees', True),
}
