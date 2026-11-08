"""KuCoin Futures: open orders — all pages of «Get Order List» (GET /api/v1/orders, ``status=active``).

Docs: ``currentPage`` — «The default currentPage is 1», ``pageSize`` — «The default pageSize is 50, The maximum cannot
exceed 1000»; the response has ``data.currentPage`` / ``pageSize`` / ``totalNum`` / ``totalPage``, rows —
``data.items``. ccxt 4.4.65 ``fetch_open_orders`` reads one page of 50 (and drops ``totalPage``): orders past it are
not visible.

The walk ends at ``currentPage >= totalPage`` from the response (not "a page shorter than pageSize": if the exchange
trims a page, the walk does not stop early).
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional

from loguru import logger

from bytx.errors import ExchangeError

PAGE_SIZE = 1000
MAX_PAGES = 20


def _int(value: Any) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def open_rows(fetch_page: Callable[[dict], Awaitable[Any]], request: dict) -> list[dict]:
    """``data.items`` rows of all pages; ``fetch_page(request)`` — one GET /api/v1/orders through the limiter."""
    rows: list[dict] = []
    seen: set[str] = set()
    for page_no in range(1, MAX_PAGES + 1):
        resp = await fetch_page({**request, 'currentPage': page_no, 'pageSize': PAGE_SIZE})
        data = resp.get('data') if isinstance(resp, dict) else None
        items = [r for r in (data.get('items') or []) if isinstance(r, dict)] if isinstance(data, dict) else []
        new = [r for r in items if str(r.get('id')) not in seen]
        seen.update(str(r.get('id')) for r in new)
        rows += new
        total = _int(data.get('totalPage')) if isinstance(data, dict) else None
        if not new or (total is not None and page_no >= total) or (total is None and len(items) < PAGE_SIZE):
            return rows
    msg = f'kucoin /api/v1/orders: the list did not end within {MAX_PAGES} pages of {PAGE_SIZE} — the book is incomplete'
    logger.error(msg)
    raise ExchangeError(msg, exchange='kucoinfutures', details={'pages': MAX_PAGES, 'limit': PAGE_SIZE})


# Spot (tests/ground_truth/kucoin/spot_orders.json, spec kucoin-universal-sdk openapi-spot.json).
# HF «Get Closed Orders» GET /api/v1/hf/orders/done: symbol is required, limit ≤ 100, ``lastId`` — «The id of the last set
# of data from the previous batch … can be used as a query parameter to look up new data from the next page».
DONE_LIMIT = 100
DONE_MAX_PAGES = 50


async def spot_hf_done_by_client_id(fetch_page: Callable[[dict], Awaitable[Any]], market_id: str,
                                    client_id: str) -> Optional[dict]:
    """The ``data.items`` row with ``clientOid == client_id`` over the hf/orders/done pages; none — None.

    The end is an empty page, a page shorter than the limit or a response without ``lastId``; ``DONE_MAX_PAGES`` is
    ExchangeError (not «no order»). A page failure is the exception of ``fetch_page``."""
    last_id: Any = None
    for _ in range(DONE_MAX_PAGES):
        params: dict = {'symbol': market_id, 'limit': DONE_LIMIT}
        if last_id is not None:
            params['lastId'] = last_id
        resp = await fetch_page(params)
        data = resp.get('data') if isinstance(resp, dict) else None
        items = [r for r in (data.get('items') or []) if isinstance(r, dict)] if isinstance(data, dict) else []
        for row in items:
            if str(row.get('clientOid') or '') == client_id:
                return row
        next_id = data.get('lastId') if isinstance(data, dict) else None
        if len(items) < DONE_LIMIT or next_id in (None, '', last_id):
            return None
        last_id = next_id
    msg = f'kucoin hf/orders/done {market_id}: the list did not end within {DONE_MAX_PAGES} pages of {DONE_LIMIT}'
    logger.error(msg)
    raise ExchangeError(msg, exchange='kucoin', details={'pages': DONE_MAX_PAGES, 'limit': DONE_LIMIT})
