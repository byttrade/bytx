"""Bybit v5 (UTA): equity and funds journal for the portfolio.

- Equity — GET /v5/account/wallet-balance (accountType=UNIFIED): ``totalEquity`` — in USD.
- Journal — GET /v5/account/transaction-log, docs:
  https://bybit-exchange.github.io/docs/v5/account/transaction-log
  ``funding``: «Positive fee value means receive funding; negative fee value means pay funding»;
  ``fee``: «Positive fee value means expense; Negative fee value means rebates»; ``cashFlow``: «close
  the position, and unRPL converts to RPL, 8-hour session settlement for USDC Perp and Futures,
  transfer in or transfer out. This does not include trading fee, funding fee»; «Change = cashFlow +
  funding - fee». Window: endTime − startTime ≤ 7 days, «supports up to 2 years worth of data»; limit ≤ 50,
  ``cursor`` = ``nextPageCursor``. ``cashFlow`` is realized PnL only for trade types (TRADE,
  SETTLEMENT, DELIVERY, LIQUIDATION, ADL); for transfers (TRANSFER_IN/OUT …) it is not PnL.
  ``id`` in the docs example repeats across SETTLEMENT and TRADE — duplicate key = id + type + time + tradeId.
  Without ``category`` the log returns all markets («Product type spot, linear, option, inverse…»), and the ids of
  spot and the USDT perpetual are the same ('BTCUSDT') — the record's market type comes from ``category``
  (:data:`CATEGORY_MARKET`).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._funding_history import bybit_settlement_side
from bytx.exchanges._income_common import DAY_MS, Pages, add, check_depth, num
from bytx.portfolio import IncomeResult

REALIZED_TYPES = frozenset({'TRADE', 'SETTLEMENT', 'DELIVERY', 'LIQUIDATION', 'ADL'})
#: record category → ccxt market type (inverse — swap perpetuals; inverse futures are resolved by id, theirs differs)
CATEGORY_MARKET = {'spot': 'spot', 'linear': 'swap', 'inverse': 'swap', 'option': 'option'}
WINDOW_MS = 7 * DAY_MS
LIMIT = 50
HISTORY_DAYS = 730


def parse_equity(response: dict) -> dict[str, float]:
    rows = ((response or {}).get('result') or {}).get('list') or []
    value = num(rows[0].get('totalEquity')) if rows else None
    return {} if value is None else {'USD': value}


async def equity(ex: Any) -> dict[str, float]:
    return parse_equity(await ex._call(ex.ccxt.privateGetV5AccountWalletBalance, {'accountType': 'UNIFIED'}))


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        base = (r.get('id'), r.get('type'), r.get('transactionTime'), r.get('tradeId'))
        common = dict(ts=r.get('transactionTime'), currency=r.get('currency'), since=since, until=until,
                      symbol=r.get('symbol'), market=CATEGORY_MARKET.get(str(r.get('category') or '')))
        fee = num(r.get('fee'))
        if fee:
            add(result, seen, base + ('fee',), kind='fee', amount=-fee, **common)
        if num(r.get('funding')):
            # SETTLEMENT side is the position side (hedge: a row per side, ids may repeat — the side is in the key)
            side = bybit_settlement_side(r)
            add(result, seen, base + (side, 'funding'), kind='funding', amount=r.get('funding'), side=side,
                **common)
        if r.get('type') in REALIZED_TYPES and num(r.get('cashFlow')):
            add(result, seen, base + ('realized',), kind='realized', amount=r.get('cashFlow'), **common)


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    start = since
    while start <= until:
        end = min(until, start + WINDOW_MS - 1)
        pages = Pages('bybit transaction-log (7-day window)')
        cursor = None
        while pages.next(result):
            params: dict = {'accountType': 'UNIFIED', 'startTime': start, 'endTime': end, 'limit': LIMIT}
            if cursor:
                params['cursor'] = cursor
            response = await ex._call(ex.ccxt.privateGetV5AccountTransactionLog, params)
            data = (response or {}).get('result') or {}
            parse_rows(result, seen, data.get('list') or [], since, until)
            next_cursor = data.get('nextPageCursor')
            if not next_cursor or not data.get('list'):
                break
            if next_cursor == cursor:
                result.incomplete('bybit transaction-log: nextPageCursor does not advance')
                break
            cursor = next_cursor
        start = end + 1
    check_depth(result, since, HISTORY_DAYS, 'bybit transaction-log')
    return result
