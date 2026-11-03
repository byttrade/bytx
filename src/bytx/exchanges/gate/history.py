"""Gate: market-history limits per docs (:mod:`bytx.data` loader).

Futures candles ``GET /futures/{settle}/candlesticks`` — «Maximum of 2000 points are returned in one query»
(``from``/``to`` in seconds; limit conflicts with from/to); spot ``/spot/candlesticks`` — «Maximum of 1000
points can be returned in a query». Funding ``/futures/{settle}/funding_rate`` — limit «Maximum
number of records returned in a single list» (default 100; max 1000), ``from``/``to``. Trades
``/futures/{settle}/trades`` — from/to, limit max 1000.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_F = 'https://github.com/gateio/gateapi-python/blob/master/docs/FuturesApi.md'
_S = 'https://github.com/gateio/gateapi-python/blob/master/docs/SpotApi.md'
CANDLES = {
    'swap': PageLimit(1000, _F + '#list_futures_candlesticks', True,
                      '«Maximum of 2000 points … in one query»; page 1000 — with margin'),
    'spot': PageLimit(1000, _S + '#list_candlesticks', True, '«Maximum of 1000 points can be returned in a query»'),
}
FUNDING = PageLimit(100, _F + '#list_futures_funding_rate_history', False,
                    'limit defaults to 100, max not stated in the spec — 100')
TRADES = {
    'swap': TradesHistory(True, _F + '#list_futures_trades', False, 'from/to, limit — page 1000', page=1000),
    'spot': TradesHistory(True, _S + '#list_trades', False, 'from/to, limit — page 1000', page=1000),
}
candle_params = until_param
funding_params = until_param
