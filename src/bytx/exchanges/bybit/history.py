"""Bybit: market-history limits per docs (:mod:`bytx.data` loader).

Candles ``GET /v5/market/kline`` — limit «[1, 1000]. Default: 200», list «Sort in reverse by startTime»
(the loader always sets both window bounds). Funding ``GET /v5/market/funding/history`` — limit «[1, 200]»,
«Passing only startTime returns an error» — the window is set by startTime and endTime together. Public trades
``GET /v5/market/recent-trade`` — no time parameters (linear — last 1000, spot — 60); archive —
download from the Bybit website: no trade history via the API.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_DOC = 'https://bybit-exchange.github.io/docs/v5/market/'

CANDLES = {
    'swap': PageLimit(1000, _DOC + 'kline', True, 'limit [1, 1000], Default 200; Sort in reverse by startTime'),
    'spot': PageLimit(1000, _DOC + 'kline', True, 'same endpoint, category=spot'),
}
FUNDING = PageLimit(200, _DOC + 'history-fund-rate', True,
                    'limit [1, 200]; Passing only startTime returns an error — we send startTime+endTime')
TRADES = {
    'swap': TradesHistory(False, _DOC + 'recent-trade', True,
                          'recent-trade has no time parameters (last 1000); archive — '
                          'https://www.bybit.com/derivatives/en/history-data'),
    'spot': TradesHistory(False, _DOC + 'recent-trade', True, 'recent-trade spot: last 60'),
}
funding_params = until_param   # ccxt: since → startTime, until → endTime
candle_params = until_param    # ccxt: since → start, until → end
