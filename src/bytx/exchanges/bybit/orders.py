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

from typing import Any, Awaitable, Callable, Optional

from loguru import logger

from bytx.errors import ExchangeError
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


def open_request(market: dict, cursor: Optional[str] = None, params: Optional[dict] = None) -> dict:
    """Request params for one page of the symbol's open orders; ``params`` — docs filters (``orderFilter``,
    ``openOnly``)."""
    req: dict = {**(params or {}), 'category': category(market), 'symbol': market['id'], 'limit': OPEN_PAGE_LIMIT}
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


async def open_rows(fetch_page: Callable[[dict], Awaitable[Any]], market: dict,
                    params: Optional[dict] = None) -> list[dict]:
    """All rows of the symbol's open orders: ``open_request`` pages along the ``nextPageCursor`` cursor.

    ``fetch_page(request)`` — one GET /v5/order/realtime (through the client's limiter). Shared walk for
    ``Exchange.open_orders`` and ``protection.tp_sl_orders``. Rows with an already seen ``orderId`` are dropped.
    The end — an empty cursor, a page without new rows or an incomplete page (fewer than ``limit`` — «Limit for data
    size per page»): Bybit returns a cursor on the last page too (the docs example — one row and a non-empty
    ``nextPageCursor``); without this condition every list would cost an extra request. ``OPEN_MAX_PAGES`` — a cut-off:
    ExchangeError (an incomplete list is not passed off as complete), not an endless loop.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    cursor: Optional[str] = None
    for _ in range(OPEN_MAX_PAGES):
        page, cursor = open_page(await fetch_page(open_request(market, cursor, params)))
        new = [r for r in page if str(r.get('orderId')) not in seen]
        seen.update(str(r.get('orderId')) for r in new)
        rows += new
        if not cursor or not new or len(page) < OPEN_PAGE_LIMIT:
            return rows
    raise _truncated(f'order/realtime {market.get("id")}', OPEN_MAX_PAGES, OPEN_PAGE_LIMIT)


def _truncated(what: str, pages: int, limit: int) -> ExchangeError:
    """The page walk was cut off at ``pages``: an incomplete list is not passed off as complete — an exception."""
    msg = f'bybit {what}: the cursor did not end within {pages} pages of {limit} — the list is incomplete'
    logger.error(msg)
    return ExchangeError(msg, exchange='bybit', details={'pages': pages, 'limit': limit})


# Account lists without a symbol — pages via the ccxt request (ccxt builds ``category`` / ``settleCoin`` and parses the
# rows itself), the docs limit and cursor — in ``params``. ccxt puts the response cursor into ``info`` of the first row
# (``add_pagination_cursor_to_result``) — it is taken from there.
# «Get Position Info» (GET /v5/position/list): «limit [1, 200]. Default: 20», ``cursor`` ← ``nextPageCursor``.
POSITIONS_PAGE_LIMIT = 200


def position_key(p: dict) -> tuple:
    """A position in the account list: symbol, positionIdx (hedge — two rows per symbol), side."""
    return (p.get('symbol'), (p.get('info') or {}).get('positionIdx'), p.get('side'))


def ccxt_cursor(items: Any) -> Optional[str]:
    """Next page cursor from a list parsed by ccxt (``info.nextPageCursor``) or None."""
    for item in items if isinstance(items, list) else []:
        info = item.get('info') if isinstance(item, dict) else None
        if isinstance(info, dict) and info.get('nextPageCursor'):
            return str(info['nextPageCursor'])
    return None


async def ccxt_pages(fetch_page: Callable[[dict], Awaitable[Any]], page_limit: int, key: Callable[[dict], Any],
                     what: str, max_pages: Optional[int] = None) -> list[dict]:
    """All pages of a ccxt list: ``fetch_page({'limit': page_limit, 'cursor': …})`` — one request (through the
    caller's limiter). The end — as in :func:`open_rows`: an empty cursor, a page without new rows (``key``) or an
    incomplete page; ``max_pages`` — cut-off: ExchangeError."""
    max_pages = max_pages or OPEN_MAX_PAGES
    out: list[dict] = []
    seen: set = set()
    cursor: Optional[str] = None
    for _ in range(max_pages):
        params: dict = {'limit': page_limit}
        if cursor:
            params['cursor'] = cursor
        page = [r for r in (await fetch_page(params) or []) if isinstance(r, dict)]
        cursor = ccxt_cursor(page)
        new = [r for r in page if key(r) not in seen]
        seen.update(key(r) for r in new)
        out += new
        if not cursor or not new or len(page) < page_limit:
            logger.debug(f'bybit: {what} — {len(out)} rows')
            return out
    raise _truncated(what, max_pages, page_limit)


def account_scopes(markets: Any, *, inverse: bool = True) -> list[dict]:
    """Scopes of an account list without a symbol: per the docs linear without symbol requires ``settleCoin``: one per
    settle currency of the linear swap markets (USDT, USDC); ``inverse`` — ``subType=inverse`` (category=inverse in
    ccxt), if such markets are loaded. No markets — one ccxt default scope."""
    rows = list((markets or {}).values()) if isinstance(markets, dict) else []
    coins = sorted({str(m.get('settleId') or m.get('settle')) for m in rows
                    if m.get('linear') and m.get('swap') and (m.get('settleId') or m.get('settle'))})
    scopes: list[dict] = [{'settleCoin': c} for c in coins]
    if inverse and any(m.get('inverse') and m.get('swap') for m in rows):
        scopes.append({'subType': 'inverse'})
    return scopes or [{}]


async def account_pages(fetch_page: Callable[[dict], Awaitable[Any]], markets: Any, page_limit: int,
                        key: Callable[[dict], Any], what: str, *, inverse: bool = True) -> list[dict]:
    """:func:`ccxt_pages` for every scope of :func:`account_scopes`; duplicates (``key``) are dropped."""
    out: list[dict] = []
    seen: set = set()
    for scope in account_scopes(markets, inverse=inverse):
        rows = await ccxt_pages(lambda p, scope=scope: fetch_page({**scope, **p}), page_limit, key,
                                f'{what} {scope or ""}'.strip())
        for r in rows:
            k = key(r)
            if k not in seen:
                seen.add(k)
                out.append(r)
    return out


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
