"""OKX: default fees for paper mode (Regular user, Lv1).

USDT perpetuals: maker 0.02%, taker 0.05%; spot: maker 0.08%, taker 0.10% — page
https://www.okx.com/fees. The table is script-rendered; during verification on 2026-10-26 the page text had
no values — ``verified=False``, to verify against docs. Your own tier — ``PaperConfig(fees=...)``.
"""
from __future__ import annotations

PAPER_FEES = {
    'swap': (0.0002, 0.0005, 'https://www.okx.com/fees', False),
    'spot': (0.0008, 0.001, 'https://www.okx.com/fees', False),
}
