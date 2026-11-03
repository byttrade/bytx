"""Binance USDⓈ-M: equity and funds journal for the portfolio.

- Equity — GET /fapi/v3/account (Account Information V3): ``totalMarginBalance`` (wallet +
  unrealized PnL), currency — USDT (single-asset; in multi-assets — USD equivalent).
- Journal — GET /fapi/v1/income (Get Income History), docs:
  https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account#get-income-history
  «If incomeType is not sent, all kinds of flow will be returned»; ``income``: «正数代表流入，负数代表
  流出» (+ inflow); «trandId is unique in the same incomeType for a user»; limit ≤ 1000; «Income
  history only contains data for the last three months». Types: REALIZED_PNL — realized PnL,
  COMMISSION / FEE_RETURN — fee / its rebate, FUNDING_FEE — funding; transfers and others are not PnL.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import Pages, add, check_depth, integer, num
from bytx.portfolio import IncomeKind, IncomeResult

KINDS: dict[str, IncomeKind] = {
    'REALIZED_PNL': 'realized',
    'COMMISSION': 'fee',
    'FEE_RETURN': 'fee',
    'FUNDING_FEE': 'funding',
}
LIMIT = 1000
HISTORY_DAYS = 90


def parse_equity(account: dict) -> dict[str, float]:
    value = num((account or {}).get('totalMarginBalance'))
    return {} if value is None else {'USDT': value}


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.fapiPrivateV3GetAccount))


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        kind = KINDS.get(str(r.get('incomeType')))
        if kind is None:
            continue
        add(result, seen, (r.get('incomeType'), r.get('tranId')), ts=r.get('time'), kind=kind,
            amount=r.get('income'), currency=r.get('asset'), since=since, until=until, symbol=r.get('symbol'))


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    pages = Pages('binance income')
    start = since
    while pages.next(result):
        rows = await ex._call(ex.ccxt.fapiPrivateGetIncome,
                              {'startTime': start, 'endTime': until, 'limit': LIMIT}) or []
        parse_rows(result, seen, rows, since, until)
        if len(rows) < LIMIT:
            break
        last = max((integer(r.get('time')) or start for r in rows), default=start)
        if last <= start:
            result.incomplete('binance income: full page without time progress')
            break
        start = last   # same-ms records at the page boundary are refetched, duplicates dropped by tranId
    check_depth(result, since, HISTORY_DAYS, 'binance income')
    return result
