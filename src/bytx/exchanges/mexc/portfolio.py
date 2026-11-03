"""MEXC futures: equity and funds journal for the portfolio.

- Equity — GET /api/v1/private/account/assets: ``equity`` — «total equity» per currency.
- Funding — GET /api/v1/private/position/funding_records: ``funding`` («funding»), ``settleTime``; page_size
  «default 20, maximum 100», pages only (newest first).
- Realized PnL and fees — GET /api/v1/private/order/list/order_deals: ``profit``, ``fee`` (``feeCurrency``),
  ``timestamp``; ``symbol`` — «true» (required), «start and end time span is 90 days», page_size max 100.
  Without a symbol list this part isn't requested — the result is marked incomplete.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import DAY_MS, Pages, add, check_depth, num
from bytx.portfolio import IncomeResult

LIMIT = 100
WINDOW_MS = 90 * DAY_MS


def parse_equity(response: Any) -> dict[str, float]:
    rows = response.get('data') if isinstance(response, dict) else None
    out: dict[str, float] = {}
    for r in rows if isinstance(rows, list) else []:
        value = num(r.get('equity'))
        if value:
            out[str(r.get('currency') or '').upper()] = value
    return out


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.contractPrivateGetAccountAssets))


def parse_funding(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        add(result, seen, ('funding', r.get('id')), ts=r.get('settleTime'), kind='funding', amount=r.get('funding'),
            currency='USDT', since=since, until=until, symbol=r.get('symbol'))


def parse_deals(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        key = r.get('id')
        profit = num(r.get('profit'))
        if profit:
            add(result, seen, ('pnl', key), ts=r.get('timestamp'), kind='realized', amount=profit, currency='USDT',
                since=since, until=until, symbol=r.get('symbol'))
        fee = num(r.get('fee'))
        if fee:
            # fee — fee paid (positive); in the journal — an expense with a minus sign
            add(result, seen, ('fee', key), ts=r.get('timestamp'), kind='fee', amount=-fee,
                currency=r.get('feeCurrency') or 'USDT', since=since, until=until, symbol=r.get('symbol'))


def _page(response: Any) -> tuple[list, int]:
    data = response.get('data') if isinstance(response, dict) else None
    if isinstance(data, dict):
        return list(data.get('resultList') or []), int(data.get('totalPage') or 1)
    return (list(data) if isinstance(data, list) else []), 0


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    pages = Pages('mexc funding_records')
    page = 1
    while pages.next(result):
        rows, total = _page(await ex._call(ex.ccxt.contractPrivateGetPositionFundingRecords,
                                           {'page_num': page, 'page_size': LIMIT}))
        parse_funding(result, seen, rows, since, until)
        oldest = min((num(r.get('settleTime')) or 0 for r in rows), default=0)
        if not rows or page >= total or oldest < since:
            break
        page += 1
    if not symbols:
        result.incomplete('mexc order_deals: symbol is required — PnL and fees not requested without a symbol list')
        return result
    check_depth(result, since, 90, 'mexc order_deals')
    for symbol in symbols:
        market_id = ex.ccxt.market(ex.symbol(symbol))['id']
        start = since
        while start <= until:
            end = min(until, start + WINDOW_MS - 1)
            deal_pages = Pages(f'mexc order_deals {market_id}')
            page = 1
            while deal_pages.next(result):
                rows, total = _page(await ex._call(ex.ccxt.contractPrivateGetOrderListOrderDeals, {
                    'symbol': market_id, 'start_time': start, 'end_time': end, 'page_num': page,
                    'page_size': LIMIT}))
                parse_deals(result, seen, rows, since, until)
                if len(rows) < LIMIT or (total and page >= total):
                    break
                page += 1
            start = end + 1
    return result
