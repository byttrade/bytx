"""Gate USDT-M: equity and funds journal for the portfolio.

- Equity — GET /futures/usdt/accounts (FuturesAccount, https://github.com/gateio/gateapi-python/blob/master/docs/FuturesAccount.md): ``total`` — «balance after the
  user's accumulated deposit, withdraw, profit and loss … excluding unrealized profit and loss» + ``unrealised_pnl``.
- Journal — GET /futures/usdt/account_book (FuturesAccountBook): ``type`` — «pnl: Profit and loss from position
  reduction - fee: Trading fees - fund: Funding fees …», ``change`` — signed amount, ``time`` — seconds;
  ``from``/``to`` — seconds; ``limit`` (default 100) and ``offset``. Depth is not stated in the spec.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import Pages, add, num
from bytx.portfolio import IncomeKind, IncomeResult

KINDS: dict[str, IncomeKind] = {'pnl': 'realized', 'fee': 'fee', 'fund': 'funding'}
LIMIT = 100
SETTLE = 'usdt'


def parse_equity(response: Any) -> dict[str, float]:
    r = response if isinstance(response, dict) else {}
    total = num(r.get('total'))
    if total is None:
        return {}
    return {str(r.get('currency') or 'USDT').upper(): total + (num(r.get('unrealised_pnl')) or 0.0)}


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.privateFuturesGetSettleAccounts, {'settle': SETTLE}))


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        kind = KINDS.get(str(r.get('type') or ''))
        if kind is None:
            continue
        t = num(r.get('time'))
        ts = int(round(t * 1000)) if t is not None else None
        add(result, seen, ('book', r.get('id') or (r.get('type'), r.get('time'), r.get('change'))), ts=ts, kind=kind,
            amount=r.get('change'), currency='USDT', since=since, until=until, symbol=r.get('contract'))


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    pages = Pages('gate account_book')
    offset = 0
    while pages.next(result):
        rows = await ex._call(ex.ccxt.privateFuturesGetSettleAccountBook, {
            'settle': SETTLE, 'from': since // 1000, 'to': -(-until // 1000), 'limit': LIMIT, 'offset': offset})
        rows = rows if isinstance(rows, list) else []
        parse_rows(result, seen, rows, since, until)
        if len(rows) < LIMIT:
            break
        offset += len(rows)
    return result
