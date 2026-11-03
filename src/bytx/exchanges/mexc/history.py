"""MEXC: market-history limits per docs (:mod:`bytx.data` loader).

Futures candles ``GET /api/v1/contract/kline/{symbol}`` — «The maximum data in a single request is 2000 pieces»
(start/end — seconds); spot ``GET /api/v3/klines`` — limit «Default 500; max 1000». Funding
``GET /api/v1/contract/funding_rate/history`` — page_size «default 20, maximum 1000», pages only. Trades:
futures ``/contract/deals/{symbol}`` — limit «maximum is 100», no time parameters (latest only);
spot ``/api/v3/aggTrades`` — startTime/endTime, limit max 1000.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_C = 'https://mexcdevelop.github.io/apidocs/contract_v1_en/'
_S = 'https://mexcdevelop.github.io/apidocs/spot_v3_en/'
CANDLES = {
    'swap': PageLimit(2000, _C + '#k-line-data', True, '«The maximum data in a single request is 2000 pieces»'),
    'spot': PageLimit(1000, _S + '#kline-candlestick-data', True, 'limit «Default 500; max 1000»'),
}
FUNDING = PageLimit(1000, _C + '#get-contract-funding-rate-history', True,
                    'page_size «default 20, maximum 1000», no time parameters')
TRADES = {
    'swap': TradesHistory(False, _C + '#get-contract-transaction-data', True,
                          'deals: latest only, limit «maximum is 100», no time parameters'),
    'spot': TradesHistory(True, _S + '#compressed-aggregate-trades-list', False,
                          'aggTrades: startTime/endTime, limit max 1000; depth not stated in docs', page=1000),
}
candle_params = until_param
funding_params = until_param
