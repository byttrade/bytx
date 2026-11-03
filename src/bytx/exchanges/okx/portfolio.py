"""OKX: equity and funds journal for the portfolio.

- Equity — GET /api/v5/account/balance: ``totalEq`` — «Total account assets denominated in USD».
- Journal — GET /api/v5/account/bills («last 7 days») and /api/v5/account/bills-archive («last 3
  months»), docs: https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-bills-details-last-7-days
  «sorted with the most recent first»; ``after`` — «records earlier than the requested bill ID»;
  ``begin``/``end`` — ts in ms; limit ≤ 100. Fields: ``pnl`` — «Profit and loss»; ``fee`` — «Negative
  number represents the user transaction fee charged by the platform. Positive number represents
  rebate»; ``balChg`` — «Signed change in account balance». Subtypes (``subType``, ``px`` description):
  1–6 and 204–209 — trades, 100–107 — liquidation, 112/113 — delivery, 173 — «Funding fee expense»,
  174 — «Funding fee income».
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import DAY_MS, Pages, add, check_depth, integer, now_ms, num
from bytx.portfolio import IncomeResult

TRADE_SUBTYPES = frozenset({'1', '2', '3', '4', '5', '6', '204', '205', '206', '207', '208', '209'})
LIQUIDATION_SUBTYPES = frozenset({'100', '101', '102', '103', '104', '105', '106', '107'})
DELIVERY_SUBTYPES = frozenset({'112', '113'})
FUNDING_SUBTYPES = frozenset({'173', '174'})
LIMIT = 100
RECENT_DAYS = 7
ARCHIVE_DAYS = 90


def parse_equity(response: dict) -> dict[str, float]:
    rows = (response or {}).get('data') or []
    value = num(rows[0].get('totalEq')) if rows else None
    return {} if value is None else {'USD': value}


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.privateGetAccountBalance))


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        sub = str(r.get('subType'))
        bill = r.get('billId')
        common = dict(ts=r.get('ts'), currency=r.get('ccy'), since=since, until=until, symbol=r.get('instId'))
        if sub in FUNDING_SUBTYPES:
            add(result, seen, (bill, 'funding'), kind='funding', amount=r.get('balChg'), **common)
            continue
        if sub in TRADE_SUBTYPES or sub in LIQUIDATION_SUBTYPES or sub in DELIVERY_SUBTYPES:
            add(result, seen, (bill, 'realized'), kind='realized', amount=r.get('pnl'), **common)
            add(result, seen, (bill, 'fee'), kind='fee', amount=r.get('fee'), **common)


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    recent = since >= now_ms() - RECENT_DAYS * DAY_MS
    method = ex.ccxt.privateGetAccountBills if recent else ex.ccxt.privateGetAccountBillsArchive
    pages = Pages('okx bills')
    after = None
    while pages.next(result):
        params: dict = {'begin': str(since), 'end': str(until), 'limit': str(LIMIT)}
        if after:
            params['after'] = after
        rows = ((await ex._call(method, params)) or {}).get('data') or []
        parse_rows(result, seen, rows, since, until)
        if len(rows) < LIMIT:
            break
        oldest = rows[-1]
        if (integer(oldest.get('ts')) or 0) < since:
            break
        after = oldest.get('billId')
        if not after:
            result.incomplete('okx bills: no billId for pagination')
            break
    if not recent:
        check_depth(result, since, ARCHIVE_DAYS, 'okx bills-archive')
    return result
