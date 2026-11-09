"""BloFin: default fees for paper mode.

The API docs have no fee rates (no trade-fee endpoint). Taker 0.06% verified on demo 2026-11-09: a fill's ``fee``
= 0.0006 × notional (``tests/ground_truth/blofin/demo_filled_fills_close.json``); maker 0.02% — the ccxt 4.4.65 value
(not verified on the BloFin site) — ``verified=False``.
"""
from __future__ import annotations

_DOC = 'https://docs.blofin.com/index.html#get-trade-history'
PAPER_FEES = {
    'swap': (0.0002, 0.0006, _DOC, False),
    'spot': (0.001, 0.001, _DOC, False),     # BloFin spot is not in the SDK yet
}
