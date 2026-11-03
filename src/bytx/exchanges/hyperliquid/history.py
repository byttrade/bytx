"""Hyperliquid: market-history limits per docs (:mod:`bytx.data` loader).

Info endpoint: «Responses that take a time range will only return 500 elements or distinct blocks of
data» — the next page starts from the last received time. ``candleSnapshot`` — «Only the most recent
5000 candles are available». ``fundingHistory`` — startTime/endTime inclusive. There is no REST feed of public
trades (WS ``trades`` only).
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_INFO = 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint'

CANDLES = {
    'swap': PageLimit(500, _INFO + '#candle-snapshot', True,
                      'time range → at most 500 elements; only the most recent 5000 candles', depth_rows=5000),
    'spot': PageLimit(500, _INFO + '#candle-snapshot', True, 'same for spot', depth_rows=5000),
}
FUNDING = PageLimit(500, _INFO + '/perpetuals#retrieve-historical-funding-rates', True,
                    'time range → at most 500 elements')
TRADES = {
    'swap': TradesHistory(False, _INFO, True, 'no REST feed of public trades — WS trades only'),
    'spot': TradesHistory(False, _INFO, True, 'no REST feed of public trades — WS trades only'),
}
funding_params = until_param   # ccxt: until → endTime
candle_params = until_param    # ccxt: until → endTime
