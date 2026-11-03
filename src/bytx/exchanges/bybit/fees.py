"""Bybit: default fees for paper mode (Non-VIP).

USDT perpetuals: maker 0.02%, taker 0.055%; spot: 0.1% / 0.1% — «Trading Fee Structure»
(https://www.bybit.com/en/help-center/article/Trading-Fee-Structure). The page didn't load during
verification on 2026-10-26 (timeout) — ``verified=False``, to verify against docs. Your own tier —
``PaperConfig(fees=...)``.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.0002, 0.00055, 'https://www.bybit.com/en/help-center/article/Trading-Fee-Structure', False),
    'spot': (0.001, 0.001, 'https://www.bybit.com/en/help-center/article/Trading-Fee-Structure', False),
}
