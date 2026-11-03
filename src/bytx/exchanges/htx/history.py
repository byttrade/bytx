"""HTX: market-history limits per docs (:mod:`bytx.data` loader).

USDT-M candles ``GET /linear-swap-ex/market/history/kline`` — size «[1,2000]», ``from``/``to`` — seconds («Either size
field or from and to fields need to be filled»); spot ``GET /market/history/kline`` — size [1,2000] with no time
parameters (latest only). Funding ``GET /linear-swap-api/v1/swap_historical_funding_rate`` — page_size «20 by
default. 50 at most», pages only. Trades ``/linear-swap-ex/market/history/trade`` and spot
``/market/history/trade`` — size [1, 2000], latest only.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_F = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/'
_S = 'https://huobiapi.github.io/docs/spot/v1/en/'
CANDLES = {
    'swap': PageLimit(2000, _F + '#general-get-kline-data', True, 'size «[1,2000]», from/to — seconds'),
    'spot': PageLimit(2000, _S + '#get-klines-candles', True, 'size [1,2000], no time parameters',
                      depth_rows=2000),
}
FUNDING = PageLimit(50, _F + '#general-query-historical-funding-rate', True,
                    'page_size «20 by default. 50 at most», no time parameters')
TRADES = {
    'swap': TradesHistory(False, _F + '#general-query-a-batch-of-trade-records-of-a-contract', True,
                          'history/trade: latest only, size [1, 2000]'),
    'spot': TradesHistory(False, _S + '#get-the-most-recent-trades', True,
                          'history/trade: latest only, size [1, 2000]'),
}
candle_params = until_param
funding_params = until_param
