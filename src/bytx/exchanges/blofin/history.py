"""BloFin: market-history limits per docs (:mod:`bytx.data` loader).

Candles ``GET /api/v1/market/candles`` (F#get-candlesticks) — limit «The maximum is 1440. The default is 500»,
``after`` — «records earlier than the requested ts», ``before`` alone — the latest records. Funding
``GET /api/v1/market/funding-rate-history`` (F#get-funding-rate-history) — limit «maximum is 100», ``after`` /
``before`` by fundingTime (ccxt sends only ``before`` = since − 1 — per the docs that means the latest records). Trades
``GET /api/v1/market/trades`` (F#get-trades) — latest only, limit max 100, no time parameters.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory

_DOC = 'https://docs.blofin.com/index.html#'
CANDLES = {'swap': PageLimit(1440, _DOC + 'get-candlesticks', True, 'limit max 1440, after — earlier than ts')}
FUNDING = PageLimit(100, _DOC + 'get-funding-rate-history', True,
                    'limit max 100; before alone gives the latest records, so after is set explicitly')
TRADES = {'swap': TradesHistory(False, _DOC + 'get-trades', True, 'market/trades: latest only, limit max 100')}


def candle_params(_since: int, until: int) -> dict:
    """``after`` — strictly earlier than ts: the window bound is inclusive — until + 1."""
    return {'after': str(until + 1)}


funding_params = candle_params
