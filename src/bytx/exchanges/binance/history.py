"""Binance: market-history limits per docs (:mod:`bytx.data` loader).

Candles: USDⓈ-M ``GET /fapi/v1/klines`` — limit «Default 500; max 1500»; Spot ``GET /api/v3/klines`` —
«Default: 500», max 1000. Funding ``GET /fapi/v1/fundingRate`` — «Default 100; max 1000», «In ascending
order», startTime/endTime inclusive. Trades ``GET /fapi/v1/aggTrades`` — limit max 1000, «only trade
histories within the past 48 hours», startTime–endTime window «must be less than 1 hour»; startTime/endTime
together with fromId «might cause response timeout», so after the first full page — fromId only.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._history_common import DAY_MS, HOUR_MS, PageLimit, TradesHistory, until_param

_FUT = 'https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/'
_SPOT = 'https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints'

CANDLES = {
    'swap': PageLimit(1500, _FUT + 'Kline-Candlestick-Data', True, 'limit Default 500; max 1500'),
    'spot': PageLimit(1000, _SPOT, True, 'limit Default: 500, max 1000'),
}
FUNDING = PageLimit(1000, _FUT + 'Get-Funding-Rate-History', True, 'limit Default 100; max 1000; ascending')
TRADES = {
    'swap': TradesHistory(True, _FUT + 'Compressed-Aggregate-Trades-List', True,
                          'aggTrades: max 1000, window < 1 hour, last 48 hours only',
                          page=1000, window_ms=HOUR_MS - 1, depth_ms=2 * DAY_MS),
    'spot': TradesHistory(True, _SPOT, True, 'aggTrades: max 1000 (window rule assumed same as futures)',
                          page=1000, window_ms=HOUR_MS - 1),
}
funding_params = until_param
candle_params = until_param


async def fetch_trades(c: Any, market: dict, since: int, until: int, page: int, window_ms: int) -> list[dict]:
    """aggTrades over ``[since, until]`` (ms, inclusive): by time in windows < 1 hour, after a full
    page — by ``fromId`` (no time, as the docs require). Returns ccxt trades in ascending order."""
    method = c.publicGetAggTrades if market.get('spot') else c.fapiPublicGetAggTrades
    out: list[dict] = []
    start, from_id = since, None
    while True:
        if from_id is None:
            end = min(until, start + window_ms)
            raw = await method({'symbol': market['id'], 'startTime': start, 'endTime': end, 'limit': page})
        else:
            raw = await method({'symbol': market['id'], 'fromId': from_id, 'limit': page})
        raw = list(raw or [])
        trades = c.parse_trades(raw, market)
        out.extend(t for t in trades if since <= t['timestamp'] <= until)
        if from_id is not None and (len(raw) < page or any(t['timestamp'] > until for t in trades)):
            return out
        if len(raw) >= page:
            from_id = max(int(r['a']) for r in raw) + 1
            continue
        if end >= until:
            return out
        start = end + 1
