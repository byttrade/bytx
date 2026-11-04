"""Bybit V5: order lookup along the realtime → history chain; open orders — all pages.

Docs:
- «Get Open & Closed Orders» (GET /v5/order/realtime): ``openOnly`` «will be ignored when query
  by orderId or orderLinkId» — lookup by id returns both open and closed orders, but only
  «a maximum of recent 500 … under each account each category»;
- «Get Order History» (GET /v5/order/history, «2 years»): closed orders older than the realtime window.
  Canceled/rejected — for the last 24 h, other closed — 7 days, beyond that — only
  orders with fills.

ccxt 4.4.65 ``fetch_order`` (with ``acknowledged``) queries realtime only: an order pushed out
of the last 500 (a bot runs for weeks) yields OrderNotFound, and a filled level gets recreated.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges.bybit.leverage import category

# lookup order: open and last 500 closed → history
ENDPOINTS = ('private_get_v5_order_realtime', 'private_get_v5_order_history')


def order_request(market: dict, order_id: Optional[str] = None, client_id: Optional[str] = None) -> dict:
    """Order request params by ``orderId`` or ``orderLinkId``."""
    req: dict = {'category': category(market), 'symbol': market['id']}
    if order_id:
        req['orderId'] = str(order_id)
    else:
        req['orderLinkId'] = str(client_id)
    return req


def find_row(response: Any, order_id: Optional[str] = None, client_id: Optional[str] = None) -> Optional[dict]:
    """The ``result.list`` row with the given ``orderId`` / ``orderLinkId``, or None."""
    result = response.get('result') if isinstance(response, dict) else None
    rows = result.get('list') if isinstance(result, dict) else None
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if order_id and str(row.get('orderId')) == str(order_id):
            return row
        if not order_id and client_id and row.get('orderLinkId') == client_id:
            return row
    return None


# Open orders: «Get Open & Closed Orders» (GET /v5/order/realtime) — «limit: [1, 50]. Default: 20», further pages via
# ``cursor`` from ``result.nextPageCursor``. ccxt 4.4.65 ``fetch_open_orders`` returns one page (20) and no cursor: a
# grid with 24–60 levels has orders beyond the first page that are not visible.
OPEN_PAGE_LIMIT = 50
OPEN_MAX_PAGES = 40                      # 2000 orders per symbol — plenty; a hard stop instead of an endless loop


def open_request(market: dict, cursor: Optional[str] = None) -> dict:
    """Request params for one page of the symbol's open orders."""
    req: dict = {'category': category(market), 'symbol': market['id'], 'limit': OPEN_PAGE_LIMIT}
    if cursor:
        req['cursor'] = cursor
    return req


def open_page(response: Any) -> tuple[list[dict], Optional[str]]:
    """(``result.list`` rows, next page cursor or None)."""
    result = response.get('result') if isinstance(response, dict) else None
    if not isinstance(result, dict):
        return [], None
    rows = [r for r in result.get('list') or [] if isinstance(r, dict)]
    return rows, result.get('nextPageCursor') or None


# Own trades: «Get Trade History» (GET /v5/execution/list) — «limit: [1, 100]. Default: 50», cursor
# ``nextPageCursor``; «startTime and endTime … the time range is 7 days» (without endTime — 7 days from startTime).
# ccxt 4.4.65 ``fetch_my_trades`` returns one page (50) of one window: trades beyond it were lost. The ccxt request is
# repeated (execType=Trade, category, symbol), pages follow the cursor, windows are 7 days each.
TRADES_PAGE_LIMIT = 100
TRADES_MAX_PAGES = 50                    # per 7-day window — 5000 trades
TRADES_WINDOW_MS = 7 * 86_400_000 - 1


def trades_request(market: dict, start: Optional[int], end: Optional[int], cursor: Optional[str] = None,
                   page_limit: int = TRADES_PAGE_LIMIT) -> dict:
    req: dict = {'category': category(market), 'symbol': market['id'], 'execType': 'Trade', 'limit': page_limit}
    if start is not None:
        req['startTime'] = int(start)
    if end is not None:
        req['endTime'] = int(end)
    if cursor:
        req['cursor'] = cursor
    return req


TRADES_HISTORY_MS = 730 * 86_400_000    # Bybit keeps execution history for 2 years: an earlier since is clipped


def trade_windows(since: Optional[int], now_ms: int) -> list[tuple[Optional[int], Optional[int]]]:
    """7-day request windows from ``since`` to ``now_ms`` — newest first (exchange pages start from the newest trades,
    so ``limit`` means the latest trades, as in ccxt); the newest window has no endTime — the exchange takes 7 days from
    startTime itself, so trades "newer" than the local clock (host clock behind) are not cut off; ``since`` is clipped to
    the 2-year history and to ``now_ms``; without ``since`` — one window, the exchange default."""
    if since is None:
        return [(None, None)]
    start = max(int(since), now_ms - TRADES_HISTORY_MS)
    if start >= now_ms:
        return [(start, None)]
    out = []
    while start <= now_ms:
        end = min(start + TRADES_WINDOW_MS, now_ms)
        out.append((start, end))
        start = end + 1
    out[-1] = (out[-1][0], None)
    return list(reversed(out))
