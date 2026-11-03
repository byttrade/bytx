"""BingX: default fees for paper mode (default tier).

Perpetuals: maker 0.020%, taker 0.050% — https://bingx.com/en/support/articles/360027240173
(«Taker … 0.050%, Maker … 0.020%»), verified 2026-10-26. Spot: 0.1% / 0.1% — the page at
https://bingx.com/en/support/costs/ has no values in text: ``verified=False``.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.0002, 0.0005, 'https://bingx.com/en/support/articles/360027240173', True),
    'spot': (0.001, 0.001, 'https://bingx.com/en/support/costs/', False),
}
