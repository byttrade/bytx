"""BloFin USDT-M: equity and the funds journal for the portfolio.

- Equity — GET /api/v1/account/balance (F#get-futures-account-balance, productType USDT-FUTURES): ``equity`` by
  ``currency``.
- Funding — GET /api/v1/account/funding-fees (F#get-funding-fee-history): ``begin`` and ``end`` are required, «interval
  … within 7 days», «can retrieve data from the last 7 days» — no deeper; limit ≤ 1000, cursor ``after`` =
  ``billId``; ``fundingFee`` signed, ``fundingKind`` 0 — funding (1 — stock dividend).
- PnL and fees — GET /api/v1/trade/fills-history (F#get-trade-history): ``fillPnl``, ``fee`` — paid,
  positive, no currency — settlement currency USDT (demo 2026-11-09); limit ≤ 100, cursor ``after`` = ``tradeId``,
  ``begin`` / ``end``. The API has no journal of all account movements (``asset/bills`` — transfers only).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import DAY_MS, Pages, add, check_depth, num
from bytx.portfolio import IncomeResult

FUNDING_DAYS = 7
FUNDING_LIMIT = 1000
FILLS_LIMIT = 100


def parse_equity(response: Any) -> dict[str, float]:
    data = response.get('data') if isinstance(response, dict) else None
    out: dict[str, float] = {}
    for r in (data or {}).get('details') or [] if isinstance(data, dict) else []:
        value = num(r.get('equity'))
        coin = str(r.get('currency') or '').upper()
        if value is not None and coin:
            out[coin] = out.get(coin, 0.0) + value
    return out


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.request, 'account/balance', 'private', 'GET',
                                       {'productType': 'USDT-FUTURES'}))


def _symbol(ex: Any, inst: Any) -> Optional[str]:
    m = (getattr(ex.ccxt, 'markets_by_id', None) or {}).get(inst)
    return m[0]['symbol'] if isinstance(m, list) and m else inst


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    check_depth(result, since, FUNDING_DAYS, 'blofin funding-fees')
    end = until
    start_all = max(since, until - FUNDING_DAYS * DAY_MS)
    while end >= start_all:
        start = max(start_all, end - FUNDING_DAYS * DAY_MS + 1)
        pages = Pages('blofin funding-fees')
        cursor: Optional[str] = None
        while pages.next(result):
            params: dict = {'begin': str(start), 'end': str(end), 'limit': str(FUNDING_LIMIT)}
            if cursor:
                params['after'] = cursor
            rows = (await ex._call(ex.ccxt.request, 'account/funding-fees', 'private', 'GET', params) or {}).get(
                'data') or []
            for r in rows:
                if str(r.get('fundingKind', '0')) == '0':
                    add(result, seen, ('funding', r.get('billId')), ts=r.get('ts') or r.get('fundingTime'),
                        kind='funding', amount=r.get('fundingFee'), currency=r.get('currency') or 'USDT', since=since,
                        until=until, symbol=_symbol(ex, r.get('instId')))
            if len(rows) < FUNDING_LIMIT or not rows or str(rows[-1].get('billId')) == cursor:
                break
            cursor = str(rows[-1].get('billId'))
        end = start - 1
    pages = Pages('blofin fills-history')
    cursor = None
    while pages.next(result):
        params = {'begin': str(since), 'end': str(until), 'limit': str(FILLS_LIMIT)}
        if cursor:
            params['after'] = cursor
        rows = (await ex._call(ex.ccxt.request, 'trade/fills-history', 'private', 'GET', params) or {}).get(
            'data') or []
        for r in rows:
            common = dict(ts=r.get('ts'), currency='USDT', since=since, until=until, symbol=_symbol(ex, r.get('instId')))
            add(result, seen, ('realized', r.get('tradeId')), kind='realized', amount=r.get('fillPnl'), **common)
            fee = num(r.get('fee'))
            if fee:
                add(result, seen, ('fee', r.get('tradeId')), kind='fee', amount=-fee, **common)
        if len(rows) < FILLS_LIMIT or str(rows[-1].get('tradeId')) == cursor:
            break
        cursor = str(rows[-1].get('tradeId'))
    return result


__all__ = ['equity', 'income']
