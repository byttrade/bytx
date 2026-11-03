"""HTX USDT-M: equity and funds journal for the portfolio.

- Equity — POST /linear-swap-api/v1/swap_cross_account_info: ``margin_balance`` (USDT cross account equity).
- Journal — POST /linear-swap-api/v3/swap_financial_record: ``type`` «3:close long; 4:close short; 5–8: fees …;
  12/13: close … for liquidation; 30:Funding fee-income; 31:Funding fee-expenditure …», ``amount`` — signed,
  ``ts`` — ms; ``start_time``/``end_time`` window — «maximum query window size is 48 hours, query window shift should
  be within past 90 days»; ``direct=next`` + ``from_id`` — max query_id of the previous page.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import Pages, add, check_depth, num
from bytx.portfolio import IncomeKind, IncomeResult

KINDS: dict[int, IncomeKind] = {3: 'realized', 4: 'realized', 12: 'realized', 13: 'realized',
                                5: 'fee', 6: 'fee', 7: 'fee', 8: 'fee', 30: 'funding', 31: 'funding'}
WINDOW_MS = 48 * 3_600_000
MARGIN_ACCOUNT = 'USDT'


def parse_equity(response: Any) -> dict[str, float]:
    rows = response.get('data') if isinstance(response, dict) else None
    for r in rows if isinstance(rows, list) else []:
        value = num(r.get('margin_balance'))
        if value is not None:
            return {str(r.get('margin_asset') or 'USDT').upper(): value}
    return {}


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.contract_private_post_linear_swap_api_v1_swap_cross_account_info,
                                       {'margin_account': MARGIN_ACCOUNT}))


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        kind = KINDS.get(int(num(r.get('type')) or 0))
        if kind is None:
            continue
        add(result, seen, ('fin', r.get('id') or r.get('query_id')), ts=r.get('ts'), kind=kind, amount=r.get('amount'),
            currency=r.get('asset') or 'USDT', since=since, until=until, symbol=r.get('contract_code'))


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    check_depth(result, since, 90, 'htx swap_financial_record')
    start = since
    while start <= until:
        end = min(until, start + WINDOW_MS - 1)
        pages = Pages('htx swap_financial_record')
        from_id: Optional[int] = None
        while pages.next(result):
            req: dict = {'mar_acct': MARGIN_ACCOUNT, 'type': ','.join(str(t) for t in sorted(KINDS)),
                         'start_time': start, 'end_time': end, 'direct': 'next'}
            if from_id is not None:
                req['from_id'] = from_id
            resp = await ex._call(ex.ccxt.contract_private_post_linear_swap_api_v3_swap_financial_record, req)
            rows = resp.get('data') if isinstance(resp, dict) else None
            rows = rows if isinstance(rows, list) else []
            parse_rows(result, seen, rows, since, until)
            ids = [int(num(r.get('query_id')) or 0) for r in rows]
            if not rows or (from_id is not None and max(ids) <= from_id):
                break
            from_id = max(ids)
        start = end + 1
    return result
