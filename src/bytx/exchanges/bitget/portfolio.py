"""Bitget USDT-M: equity and funds journal for the portfolio.

- Equity — GET /api/v2/mix/account/accounts (productType USDT-FUTURES, USDC-FUTURES): ``accountEquity`` per
  ``marginCoin`` (docs https://www.bitget.com/docs/catalog/classic-contract-account/classic-contract-account#get-account-list).
- Journal — GET /api/v2/mix/account/bill («Get Account Bills»): «It only supports to get the data within
  90days»; startTime..endTime «<= 30 days»; limit «max 100»; pages — ``idLessThan`` = ``endId``.
  ``businessType`` (docs): contract_settle_fee — «funding fee»; open_/close_/force_close_/burst_/buy/sell/
  adl_/delivery_ — trades: ``amount`` — realized PnL (0 on open), ``fee`` — fee (signed).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._income_common import DAY_MS, Pages, add, check_depth, num
from bytx.portfolio import IncomeResult

PRODUCT_TYPES = ('USDT-FUTURES', 'USDC-FUTURES')
LIMIT = 100
WINDOW_MS = 30 * DAY_MS
HISTORY_DAYS = 90
FUNDING_TYPES = frozenset({'contract_settle_fee'})
TRADE_TYPES = frozenset({
    'open_long', 'open_short', 'close_long', 'close_short', 'force_close_long', 'force_close_short',
    'burst_long_loss_query', 'burst_short_loss_query', 'buy', 'sell', 'force_buy', 'force_sell', 'burst_buy',
    'burst_sell', 'delivery_long', 'delivery_short', 'adl_close_long', 'adl_close_short',
    'adl_buy_in_single_side_mode', 'adl_sell_in_single_side_mode'})


def parse_equity(response: dict) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in (response or {}).get('data') or []:
        value = num(row.get('accountEquity'))
        coin = str(row.get('marginCoin') or '').upper()
        if value is not None and coin:
            out[coin] = out.get(coin, 0.0) + value
    return out


async def equity(ex: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    for product in PRODUCT_TYPES:
        got = parse_equity(await ex._call(ex.ccxt.privateMixGetV2MixAccountAccounts, {'productType': product}))
        for coin, value in got.items():
            out[coin] = out.get(coin, 0.0) + value
    return out


def parse_rows(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> None:
    for r in rows or []:
        kind = str(r.get('businessType') or '')
        bill = r.get('billId')
        common = dict(ts=r.get('cTime'), currency=r.get('coin'), since=since, until=until, symbol=r.get('symbol'))
        if kind in FUNDING_TYPES:
            add(result, seen, ('funding', bill), kind='funding', amount=r.get('amount'), **common)
        elif kind in TRADE_TYPES:
            add(result, seen, ('realized', bill), kind='realized', amount=r.get('amount'), **common)
            add(result, seen, ('fee', bill), kind='fee', amount=r.get('fee'), **common)


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    for product in PRODUCT_TYPES:
        end = until
        while end >= since:
            start = max(since, end - WINDOW_MS + 1)
            pages = Pages(f'bitget bill {product}')
            cursor: Optional[str] = None
            while pages.next(result):
                params: dict = {'productType': product, 'startTime': str(start), 'endTime': str(end),
                                'limit': str(LIMIT)}
                if cursor:
                    params['idLessThan'] = cursor
                data = ((await ex._call(ex.ccxt.privateMixGetV2MixAccountBill, params)) or {}).get('data') or {}
                rows = data.get('bills') or []
                parse_rows(result, seen, rows, since, until)
                nxt = data.get('endId')
                if len(rows) < LIMIT or not nxt:
                    break
                if nxt == cursor:
                    result.incomplete(f'bitget bill {product}: endId does not advance')
                    break
                cursor = str(nxt)
            end = start - 1
    check_depth(result, since, HISTORY_DAYS, 'bitget bill')
    return result


__all__ = ['equity', 'income']
