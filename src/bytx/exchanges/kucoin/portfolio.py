"""KuCoin Futures: equity and funds journal for the portfolio.

- Equity — GET /api/v1/account-overview?currency=USDT (and USDC): ``accountEquity``.
- Realized PnL — GET /api/v1/transaction-history («Get Account Ledgers - Futures»), docs:
  https://www.kucoin.com/docs-new/api-3470124 — ``type``: «RealizedPNL-Realized profit and loss,
  Deposit, Withdrawal, Transferin, TransferOut» (the response has ``RealisedPNL``); ``offset`` — «only the
  attributes of the last returned result of the previous request are used»; ``hasMore`` — «true …
  you need to move to the next page»; ``maxCount`` — page size (default 50).
  The docs don't say whether RealisedPNL includes fees and funding.
- Funding — per symbol: ``fetch_funding_paid`` (GET /api/v1/funding-history, symbol
  required). Symbols — the passed ``symbols`` or open positions; without ``symbols`` positions closed during
  the period aren't visible — the report is marked incomplete.
- The docs offer no single account-wide fee request — ``fees`` are unavailable (None).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import Pages, add, check_depth, integer, num
from bytx.portfolio import IncomeItem, IncomeResult

CURRENCIES = ('USDT', 'USDC')
LIMIT = 200
FUNDING_HISTORY_DAYS = 180   # fetch_funding_paid: «history — 6 months»


def parse_equity(response: dict, currency: str) -> dict[str, float]:
    data = (response or {}).get('data') or {}
    value = num(data.get('accountEquity'))
    return {} if value is None else {str(data.get('currency') or currency).upper(): value}


async def equity(ex: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    for currency in CURRENCIES:
        out.update(parse_equity(await ex._call(ex.ccxt.futuresPrivateGetAccountOverview, {'currency': currency}),
                                currency))
    return out


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        if r.get('type') != 'RealisedPNL':
            continue
        add(result, seen, ('ledger', r.get('offset')), ts=r.get('time'), kind='realized', amount=r.get('amount'),
            currency=r.get('currency'), since=since, until=until, symbol=r.get('remark'))


class _FundingAdapter:
    """Adapter of :class:`~bytx.exchanges._funding_history.FundingHistory` to the Exchange's ccxt instance."""

    def __init__(self, ccxt: Any) -> None:
        self.exchange = ccxt
        self.symbol = None
        self.bot_id = 'portfolio'

    async def _load_markets_cached(self) -> None:   # markets are already loaded by Exchange.connect()
        return None


def _funding_cls() -> type:
    from bytx.exchanges._funding_history import FundingHistory
    return type('PortfolioFunding', (_FundingAdapter, FundingHistory), {})


FEES_UNAVAILABLE = 'kucoin: the docs offer no single account-wide fee request (fees=None)'


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult(unavailable={'fee'})
    result.incomplete(FEES_UNAVAILABLE)
    seen: set = set()
    pages = Pages('kucoin transaction-history')
    offset = None
    while pages.next(result):
        params: dict = {'type': 'RealisedPNL', 'startAt': since, 'endAt': until, 'maxCount': LIMIT, 'forward': True}
        if offset is not None:
            params['offset'] = offset
        data = ((await ex._call(ex.ccxt.futuresPrivateGetTransactionHistory, params)) or {}).get('data') or {}
        rows = data.get('dataList') or []
        parse_rows(result, seen, rows, since, until)
        if not data.get('hasMore') or not rows:
            break
        nxt = integer(rows[-1].get('offset'))
        if nxt is None or nxt == offset:
            result.incomplete('kucoin transaction-history: hasMore without offset progress')
            break
        offset = nxt

    targets = list(symbols or [])
    if not symbols:
        targets = sorted({p.symbol for p in positions or []})
        result.incomplete('kucoin funding: open positions only (pass symbols= for closed ones)')
    funding = _funding_cls()(ex.ccxt)
    for s in targets:
        got = await funding.fetch_funding_paid(s, since, until)
        if not got.get('complete'):
            result.incomplete(f'kucoin funding {s}: history incomplete')
        for i, item in enumerate(got.get('items') or []):
            key = ('funding', s, item.get('ts'), i)
            if key in seen:
                continue
            seen.add(key)
            result.items.append(IncomeItem(ts=int(item['ts']), kind='funding', amount=float(item['amount']),
                                           currency=str(item.get('currency') or '').upper(), symbol=s))
    check_depth(result, since, FUNDING_HISTORY_DAYS, 'kucoin funding-history')
    return result
