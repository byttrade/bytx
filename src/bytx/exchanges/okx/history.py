"""OKX: market-history limits per docs (:mod:`bytx.data` loader).

Candles ``GET /api/v5/market/candles`` — «can retrieve the latest 1,440 data entries», limit «The maximum
is 300»; deeper — ``/market/history-candles`` («from recent years», limit max 300; ccxt switches by itself
on ``since``). Funding ``GET /api/v5/public/funding-rate-history`` — limit «maximum is 400»; ``before``
alone — «latest data» (ccxt sends only before = since − 1), so the upper window bound is set via ``after``
explicitly. Trades ``GET /api/v5/market/history-trades`` — «from the last 3 months with pagination», type
1 — tradeId, 2 — timestamp, ``after`` — «earlier than the requested tradeId or ts», limit max 100.
"""
from __future__ import annotations

from typing import Any

from bytx.exchanges._history_common import DAY_MS, PageLimit, TradesHistory, until_param

_DOC = 'https://www.okx.com/docs-v5/en/'

CANDLES = {
    'swap': PageLimit(300, _DOC + '#order-book-trading-market-data-get-candlesticks', True,
                      'limit max 300 (candles and history-candles)'),
    'spot': PageLimit(300, _DOC + '#order-book-trading-market-data-get-candlesticks', True, 'same endpoint'),
}
FUNDING = PageLimit(400, _DOC + '#public-data-rest-api-get-funding-rate-history', True,
                    'limit max 400; before alone gives latest records, so after is set explicitly')
_TRADES = TradesHistory(True, _DOC + '#order-book-trading-market-data-get-trades-history', True,
                        'history-trades: last 3 months, limit max 100, after — earlier than ts/tradeId',
                        page=100, depth_ms=90 * DAY_MS)
TRADES = {'swap': _TRADES, 'spot': _TRADES}
candle_params = until_param    # ccxt: until → after


def funding_params(_since: int, until: int) -> dict:
    """ccxt sends only ``before`` (= since − 1) — per docs that means «latest records»: the upper bound is explicit
    (``after`` — earlier than fundingTime, hence until + 1)."""
    return {'after': str(until + 1)}


async def fetch_trades(c: Any, market: dict, since: int, until: int, page: int, window_ms: Any = None) -> list[dict]:
    """history-trades over ``[since, until]``: the first page by time (type=2, after = until + 1),
    then by tradeId (type=1): an exact bound with no duplicates or losses within one millisecond."""
    req = {'instId': market['id'], 'type': '2', 'after': str(until + 1), 'limit': str(page)}
    out: list[dict] = []
    while True:
        resp = await c.publicGetMarketHistoryTrades(req)
        data = list((resp or {}).get('data') or [])
        if not data:
            return out
        trades = c.parse_trades(data, market)
        out.extend(t for t in trades if since <= t['timestamp'] <= until)
        if len(data) < page or min(t['timestamp'] for t in trades) < since:
            return out
        oldest = min(int(d['tradeId']) for d in data)
        req = {'instId': market['id'], 'type': '1', 'after': str(oldest), 'limit': str(page)}
