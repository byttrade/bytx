"""OKX: open orders — all pages of «Get order List» (GET /api/v5/trade/orders-pending).

Docs: ``limit`` — «The maximum is 100; The default is 100», rows — newest first, ``after`` — «Pagination of data to
return records earlier than the requested ordId». ccxt 4.4.65 ``fetch_open_orders`` reads one page: with more than 100
orders the older ones are not visible, ``cancel_all(symbol, client_id_prefix=…)`` canceled 100, the account order
tracker kept an incomplete book.

A page is a ccxt request (the same ``instId``, ccxt parsing) with ``limit`` and ``after`` in ``params``: the caller's
``fetch_page(params)`` goes through its limiter (Exchange — ``_call`` per page).
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

from loguru import logger

from bytx.errors import ExchangeError

PAGE_LIMIT = 100
MAX_PAGES = 40                           # 4000 orders — with margin; beyond that — cut off with an error


def _oldest_id(page: list[dict]) -> Optional[str]:
    """The earliest ordId of the page — ``after`` of the next one. OKX ordId grows with time (a numeric string); ccxt
    sorts parsed orders by time, so without numeric ids the earliest by time is taken."""
    ids = [str(o.get('id')) for o in page if o.get('id')]
    if not ids:
        return None
    if all(i.isdigit() for i in ids):
        return min(ids, key=int)
    oldest = min(page, key=lambda o: o.get('timestamp') or 0)
    return str(oldest.get('id')) if oldest.get('id') else None


async def open_pages(fetch_page: Callable[[dict], Awaitable[Any]], what: str = '',
                     endpoint: str = 'orders-pending') -> list[dict]:
    """All pages: ``fetch_page({'limit': 100[, 'after': ordId]})`` → orders parsed by ccxt. The end — an incomplete
    page or a page without new ids; ``MAX_PAGES`` — ExchangeError (an incomplete book is not passed off as complete)."""
    out: list[dict] = []
    seen: set[str] = set()
    after: Optional[str] = None
    for _ in range(MAX_PAGES):
        params: dict = {'limit': PAGE_LIMIT}
        if after:
            params['after'] = after
        page = [o for o in (await fetch_page(params) or []) if isinstance(o, dict)]
        new = [o for o in page if str(o.get('id')) not in seen]
        seen.update(str(o.get('id')) for o in new)
        out += new
        if len(page) < PAGE_LIMIT or not new:
            return out
        after = _oldest_id(page)
        if after is None:
            return out
    msg = (f'okx {endpoint} {what or "account"}: the list did not end within {MAX_PAGES} pages of {PAGE_LIMIT} — '
           f'the book is incomplete')
    logger.error(msg)
    raise ExchangeError(msg, exchange='okx', details={'pages': MAX_PAGES, 'limit': PAGE_LIMIT})


# Spot: an order by clOrdId in the history (tests/ground_truth/okx/spot_orders_history.json). Docs: orders-history —
# «completed orders … placed in the last 7 days» (canceled unfilled — 2 hours), orders-history-archive — «last 3 months»;
# without ``state`` — filled, canceled, mmp_canceled; ``after`` — «records earlier than the requested ordId», limit ≤ 100.
HISTORY_METHODS = (('privateGetTradeOrdersHistory', 'orders-history'),
                   ('privateGetTradeOrdersHistoryArchive', 'orders-history-archive'))


async def spot_history_by_client_id(call: Callable[..., Awaitable[Any]], ccxt_ex: Any, market: dict,
                                    client_id: str) -> Optional[dict]:
    """The parsed ccxt order with ``clOrdId == client_id`` from orders-history, then the archive; none — None.

    ``call(fn, params)`` — a page request through the limiter. A page failure is the call's exception (not «no
    order»); a pagination cut-off is ExchangeError (:func:`open_pages`)."""
    for method, endpoint in HISTORY_METHODS:
        fn = getattr(ccxt_ex, method)

        async def page(params: dict, fn: Any = fn) -> list[dict]:
            resp = await call(fn, {'instType': 'SPOT', 'instId': market['id'], **params})
            rows = resp.get('data') if isinstance(resp, dict) else None
            return ccxt_ex.parse_orders([r for r in rows or [] if isinstance(r, dict)], market)

        for o in await open_pages(page, market['symbol'], endpoint):
            if str((o.get('info') or {}).get('clOrdId') or '') == client_id:
                return o
    return None
