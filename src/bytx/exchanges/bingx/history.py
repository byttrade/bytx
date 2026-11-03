"""BingX: market-history limits per docs (:mod:`bytx.data` loader).

Candles ``GET /openApi/swap/v3/quote/klines`` — limit «default: 500 maximum: 1440». Funding
``GET /openApi/swap/v2/quote/fundingRate`` — startTime, endTime, limit «default: 100 maximum: 1000».
ccxt 4.4.65 maps the unified ``until`` to **startTime** (a bug) — the window bound is sent as
``endTime`` directly. Trades ``GET /openApi/swap/v2/quote/trades`` — only limit «maximum 1000», no
time parameters: no trade history via the API.
"""
from __future__ import annotations

from bytx.exchanges._history_common import PageLimit, TradesHistory, until_param

_DOC = 'https://bingx-api.github.io/docs-v3/#/en/Swap/Market%20Data/'

CANDLES = {
    'swap': PageLimit(1440, _DOC + 'Kline%2FCandlestick%20Data', True, 'limit default 500, maximum 1440'),
    'spot': PageLimit(500, 'https://bingx-api.github.io/docs-v3/#/en/Spot/Market%20Data/', False,
                      'spot: max not verified in docs — conservatively 500'),
}
FUNDING = PageLimit(1000, _DOC + 'Get%20Funding%20Rate%20History', True, 'limit default 100, maximum 1000')
TRADES = {
    'swap': TradesHistory(False, _DOC + 'Recent%20Trades%20List', True,
                          'quote/trades: limit only (max 1000), no time parameters'),
    'spot': TradesHistory(False, 'https://bingx-api.github.io/docs-v3/#/en/Spot/Market%20Data/', False,
                          'recent trades only'),
}
candle_params = until_param    # ccxt: until → endTime


def funding_params(_since: int, until: int) -> dict:
    """ccxt: ``until`` → startTime (a bug) — endTime per docs, sent directly."""
    return {'endTime': until}
