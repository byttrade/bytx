"""BingX perpetual: equity and funds journal for the portfolio.

- Equity — GET /openApi/swap/v3/user/balance («Query account data»): list of assets, ``equity`` —
  «net asset value» for each (USDT, USDC). ccxt 4.4.65 calls v2 — we send our own request, as in
  ``ccxt_fixes._fix_balance``.
- Journal — GET /openApi/swap/v2/user/income («Get Fund Flow»), docs:
  https://bingx-api.github.io/docs-v3/#/en/Swap/Account%20Endpoints/Get%20Fund%20Flow
  «If currency is omitted, the default is USDT. To query USDC fund flows, provide currency=USDC»;
  without symbol — «fund flows for all symbols under the specified currency»; ``income``: «positive
  numbers represent inflows, negative numbers represent outflows»; limit ≤ 1000. Types: REALIZED_PNL,
  FUNDING_FEE, TRADING_FEE (others — INSURANCE_CLEAR, TRIAL_FUND, ADL, SYSTEM_DEDUCTION — are not
  counted). History depth isn't stated in the docs — 3 months, as in ``fetch_funding_paid``.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import Pages, add, check_depth, integer, num
from bytx.portfolio import IncomeKind, IncomeResult

KINDS: dict[str, IncomeKind] = {'REALIZED_PNL': 'realized', 'TRADING_FEE': 'fee', 'FUNDING_FEE': 'funding'}
CURRENCIES = ('USDT', 'USDC')
LIMIT = 1000
HISTORY_DAYS = 90


def parse_equity(response: dict) -> dict[str, float]:
    data = (response or {}).get('data') or []
    if isinstance(data, dict):            # v2: {'balance': {...}}
        data = [data.get('balance') or {}]
    out: dict[str, float] = {}
    for row in data:
        value = num(row.get('equity'))
        if value is not None and row.get('asset'):
            out[str(row['asset']).upper()] = out.get(str(row['asset']).upper(), 0.0) + value
    return out


async def equity(ex: Any) -> dict[str, float]:
    response = await ex._call(ex.ccxt.request, 'user/balance', ['swap', 'v3', 'private'], 'GET', {},
                              None, None, {'cost': 2})
    return parse_equity(response)


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        kind = KINDS.get(str(r.get('incomeType')))
        if kind is None:
            continue
        add(result, seen, (r.get('incomeType'), r.get('tranId'), r.get('time')), ts=r.get('time'), kind=kind,
            amount=r.get('income'), currency=r.get('asset'), since=since, until=until, symbol=r.get('symbol'))


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    for currency in CURRENCIES:
        pages = Pages(f'bingx income {currency}')
        start = since
        while pages.next(result):
            response = await ex._call(ex.ccxt.swapV2PrivateGetUserIncome, {
                'currency': currency, 'startTime': start, 'endTime': until, 'limit': LIMIT})
            rows = (response or {}).get('data') or []
            parse_rows(result, seen, rows, since, until)
            if len(rows) < LIMIT:
                break
            last = max((integer(r.get('time')) or start for r in rows), default=start)
            if last <= start:
                result.incomplete(f'bingx income {currency}: full page without time progress')
                break
            start = last
    check_depth(result, since, HISTORY_DAYS, 'bingx income')
    return result
