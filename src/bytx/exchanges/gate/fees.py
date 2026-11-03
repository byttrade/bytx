"""Gate: default fees for paper mode.

The API docs have no tier table; rates are contract fields ``maker_fee_rate`` («negative values indicate
rebates») / ``taker_fee_rate`` and the pair's ``fee`` (CurrencyPair, «Trading fee rate» in percent). Values —
public BTC_USDT response (live check 2026-10-26: maker −0.0001, taker 0.00075; spot fee 0.2 %) —
``verified=False``.
"""
from __future__ import annotations

_URL = 'https://github.com/gateio/gateapi-python/blob/master/docs/Contract.md'
PAPER_FEES = {
    'swap': (-0.0001, 0.00075, _URL, False),
    'spot': (0.002, 0.002, 'https://github.com/gateio/gateapi-python/blob/master/docs/CurrencyPair.md', False),
}
