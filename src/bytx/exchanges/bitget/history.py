"""Bitget: market-history limits per docs (:mod:`bytx.data` loader).

Candles: ``GET /api/v2/mix/market/candles`` — limit «Default: 100, maximum: 1000», depth depends on the
interval («1m, 3m, and 5m can be checked for up to one month»); ``/history-candles`` — «return a maximum
of 200 pieces of data» (limit max 200); spot — candles max 1000, history-candles max 200. Page — 200:
with ``since`` ccxt switches to history-candles itself. Funding ``GET /api/v2/mix/market/history-fund-rate`` —
pageSize «Default: 20, maximum: 100», pageNo only, no time parameters. Trades
``/fills-history`` — «last 90 days», window «should not exceed 7 days», limit max 1000.
"""
from __future__ import annotations

from bytx.exchanges._history_common import DAY_MS, PageLimit, TradesHistory, until_param

_MKT = 'https://www.bitget.com/docs/catalog/classic-contract-market/classic-contract-market'
_SPOT = 'https://www.bitget.com/docs/catalog/classic-spot-market/classic-spot-market'
CANDLES = {
    'swap': PageLimit(200, _MKT + '#get-historical-candlestick', True,
                      'history-candles: limit max 200 (candles — max 1000, but 1m/3m/5m depth is one month)'),
    'spot': PageLimit(200, _SPOT + '#get-history-candlestick-data', True, 'history-candles: limit max 200'),
}
FUNDING = PageLimit(100, _MKT + '#get-historical-funding-rates', True,
                    'pageSize max 100, pageNo pages — no time parameters')
TRADES = {
    'swap': TradesHistory(True, _MKT + '#get-history-transactions', True,
                          'fills-history: 90 days, window ≤ 7 days, limit max 1000',
                          page=1000, window_ms=7 * DAY_MS, depth_ms=90 * DAY_MS),
    'spot': TradesHistory(True, _SPOT + '#get-market-trades', True,
                          'fills-history: 90 days, window ≤ 7 days, limit max 1000',
                          page=1000, window_ms=7 * DAY_MS, depth_ms=90 * DAY_MS),
}
candle_params = until_param
funding_params = until_param
