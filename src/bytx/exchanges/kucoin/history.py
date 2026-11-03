"""KuCoin: market-history limits per docs (:mod:`bytx.data` loader).

Futures ``GET /api/v1/kline/query`` — «the system will return at most 500 pieces of data. To obtain
more data, please page the data by time»; spot ``GET /api/v1/market/candles`` — «at most 1500».
Funding ``GET /api/v1/contract/funding-rates`` — from and to are required, the max record count is **not
stated** in the docs (conservative window). Trades ``GET /api/v1/trade/history`` — «the last 100 transaction
records», no time parameters: no trade history via the API.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

CANDLES = {
    'swap': PageLimit(500, 'https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines', True,
                      'at most 500 pieces of data; page the data by time'),
    'spot': PageLimit(1500, 'https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines', True,
                      'at most 1500 pieces of data'),
}
FUNDING = PageLimit(100, 'https://www.kucoin.com/docs-new/rest/futures-trading/funding-fees/get-public-funding-history',
                    False, 'max record count not stated in docs — window of 100 records')
TRADES = {
    'swap': TradesHistory(False, 'https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-trade-history',
                          True, 'last 100 transaction records, no time parameters'),
    'spot': TradesHistory(False, 'https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-trade-history',
                          False, 'recent trades only'),
}
funding_params = until_param   # ccxt: until → to
candle_params = until_param    # ccxt computes to = since + limit × interval itself
