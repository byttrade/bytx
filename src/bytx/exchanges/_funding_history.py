"""
Funding over a position window: fetch_funding_paid.

Sources, per exchange docs (ccxt is only the transport, implicit methods):
- binance:  GET /fapi/v1/income, incomeType=FUNDING_FEE; income: + received / − paid;
            limit ≤ 1000, history — 3 months.
- bybit:    GET /v5/account/transaction-log (UTA), type=SETTLEMENT; field funding:
            «Positive fee value means receive funding; negative ... pay funding»; the row's side is
            the position side (Buy — long, Sell — short: «For side=Buy, feeRate=market fee rate;
            For side=Sell, feeRate= - market fee rate»), in hedge mode — a row per side;
            endTime − startTime ≤ 7 days, limit ≤ 50, cursor. The request has NO symbol parameter
            (only accountType/category/currency/baseCoin/type/...) — the request is narrowed by
            baseCoin, rows are filtered by response field symbol == market id; the window is split
            into 7-day chunks, the _MAX_PAGES page limit applies per window.
- okx:      GET /api/v5/account/bills (7 days) / bills-archive (3 months), type=8
            (subType 173 — expense, 174 — income); balChg — signed balance change;
            limit ≤ 100, pagination after=billId (newest to oldest).
- bingx:    GET /openApi/swap/v2/user/income, incomeType=FUNDING_FEE; income: «正數代表流入，
            負數代表流出» (+ inflow); limit ≤ 1000, history — 3 months.
- kucoin:   GET /api/v1/funding-history; funding: + received (docs example: long, rate > 0
            → −positionCost×fundingRate); window ≤ 3 months, history — 6 months, hasMore.
- hyperliquid: POST /info {type: userFunding}; delta.usdc — balance change (long,
            rate > 0 → negative); response ≤ 500 records, next page —
            startTime = time of the last one + 1.

Symbol: binance/bingx (symbol), okx (instId), kucoin (symbol) filter by symbol on the
exchange side; rows with another symbol in the response are dropped as well
(_funding_other_symbol). hyperliquid — filtered by delta.coin.

Position side of a record (``side`` 'long' / 'short') — only where the exchange reports it (Bybit SETTLEMENT).
Binance / BingX income (symbol, incomeType, income, asset, info, time, tranId, tradeId), OKX bills,
KuCoin funding-history, Hyperliquid userFunding don't report it — the record has no ``side`` key.

No error propagates: on failure/unsupported exchange complete=False.
"""
import time
from abc import ABC

from loguru import logger

from bytx.utils.text import to_exchange_symbol

_DAY_MS = 86_400_000
_MAX_PAGES = 200           # pagination guard per call (Bybit — per 7-day window)


def _num(value):
    """Number from a string/number; '', None, bool, garbage — None."""
    if value is None or value == '' or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value):
    v = _num(value)
    return int(v) if v is not None else None


_BYBIT_SETTLEMENT_SIDE = {'buy': 'long', 'sell': 'short'}


def bybit_settlement_side(row: dict):
    """Position side of a /v5/account/transaction-log SETTLEMENT row: Buy — long, Sell — short, otherwise None."""
    return _BYBIT_SETTLEMENT_SIDE.get(str(row.get('side') or '').lower())


class _Incomplete(Exception):
    """Window data is incomplete (page limit, history depth)."""


class FundingHistory(ABC):
    """Client mixin: funding history for a symbol over a time window."""

    async def fetch_funding_paid(self, symbol: str | None = None, since_ms: int | None = None,
                                 until_ms: int | None = None) -> dict:
        """Funding for a symbol over [since_ms, until_ms] (ms, inclusive).

        Returns:
            {'total': float — sum in the market's settlement currency (+ received / − paid),
             'items': [{'ts', 'amount', 'currency', 'rate'?, 'side'?}] sorted by ts ascending
             ('side' — position side 'long' / 'short' where the exchange reports it: Bybit),
             'complete': bool — False on error, unsupported exchange, window deeper than
             exchange history, or broken pagination; total is then the sum of what was fetched}.
        """
        result = {'total': 0.0, 'items': [], 'complete': False}
        name = str(self.exchange).lower()
        try:
            if since_ms is None:
                raise ValueError('since_ms is required')
            until = int(until_ms) if until_ms is not None else int(time.time() * 1000)
            since = int(since_ms)
            if until < since:
                raise ValueError(f'until_ms {until} < since_ms {since}')
            await self._load_markets_cached()
            market = self.exchange.market(to_exchange_symbol(name, symbol or self.symbol))
            settle = market.get('settle') or market.get('quote')

            fetchers = {
                'binance': self._funding_binance,
                'bybit': self._funding_bybit,
                'okx': self._funding_okx,
                'bingx': self._funding_bingx,
                'kucoin futures': self._funding_kucoin,
                'kucoinfutures': self._funding_kucoin,
                'hyperliquid': self._funding_hyperliquid,
            }
            fetcher = fetchers.get(name)
            if fetcher is None:
                raise NotImplementedError(f'funding history is not supported for {name}')

            complete = True
            pages = [0]
            try:
                raw = await fetcher(market, since, until, pages)
            except _Incomplete as e:
                complete = False
                raw = e.args[1] if len(e.args) > 1 else []
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\t⚠️ Funding history incomplete ({name} {market.get('symbol')}): {e.args[0]}")

            items, seen = [], set()
            for key, ts, amount, currency, rate, *extra in raw:
                if ts is None or amount is None or not (since <= ts <= until):
                    continue
                if key is not None:
                    if key in seen:
                        continue
                    seen.add(key)
                item = {'ts': ts, 'amount': amount, 'currency': currency or settle}
                if rate is not None:
                    item['rate'] = rate
                side = extra[0] if extra else None
                if side is not None:
                    item['side'] = side
                items.append(item)
            items.sort(key=lambda i: i['ts'])

            total = 0.0
            for item in items:
                if item['currency'] and settle and item['currency'] != settle:
                    complete = False
                    logger.bind(bot_id=self.bot_id).warning(
                        f"[{self.bot_id}]\t⚠️ Funding item in {item['currency']} != settle {settle}, "
                        f"excluded from total")
                    continue
                total += item['amount']
            result.update(total=total, items=items, complete=complete)
        except Exception as e:
            logger.bind(bot_id=self.bot_id).warning(
                f"[{self.bot_id}]\t⚠️ fetch_funding_paid failed ({name}): {type(e).__name__}: {e}")
        return result

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _funding_page(pages: list, acc: list):
        pages[0] += 1
        if pages[0] > _MAX_PAGES:
            raise _Incomplete(f'more than {_MAX_PAGES} pages', acc)

    @staticmethod
    def _funding_other_symbol(row: dict, field: str, market_id) -> bool:
        """The response row clearly belongs to another symbol (field present and does not match)."""
        value = row.get(field)
        return value not in (None, '') and str(value) != str(market_id)

    @staticmethod
    def _funding_check_depth(since: int, days: int, acc: list):
        if since < int(time.time() * 1000) - days * _DAY_MS:
            raise _Incomplete(f'window starts deeper than {days} days of exchange history', acc)

    # ------------------------------------------------------------------ exchanges

    async def _funding_binance(self, market, since, until, pages):
        acc = []
        start = since
        while True:
            self._funding_page(pages, acc)
            rows = await self.exchange.fapiPrivateGetIncome({
                'symbol': market['id'], 'incomeType': 'FUNDING_FEE',
                'startTime': start, 'endTime': until, 'limit': 1000})
            for r in rows or []:
                if r.get('incomeType') != 'FUNDING_FEE' or \
                        self._funding_other_symbol(r, 'symbol', market['id']):
                    continue
                acc.append((r.get('tranId'), _int(r.get('time')), _num(r.get('income')),
                            r.get('asset'), None))
            if len(rows or []) < 1000:
                break
            start = max(_int(r.get('time')) or start for r in rows) + 1
        self._funding_check_depth(since, 90, acc)
        return acc

    async def _funding_bingx(self, market, since, until, pages):
        acc = []
        start = since
        while True:
            self._funding_page(pages, acc)
            resp = await self.exchange.swapV2PrivateGetUserIncome({
                'symbol': market['id'], 'incomeType': 'FUNDING_FEE',
                'startTime': start, 'endTime': until, 'limit': 1000})
            rows = (resp or {}).get('data') or []
            for r in rows:
                if r.get('incomeType') != 'FUNDING_FEE' or \
                        self._funding_other_symbol(r, 'symbol', market['id']):
                    continue
                acc.append((r.get('tranId'), _int(r.get('time')), _num(r.get('income')),
                            r.get('asset'), None))
            if len(rows) < 1000:
                break
            start = max(_int(r.get('time')) or start for r in rows) + 1
        self._funding_check_depth(since, 90, acc)
        return acc

    async def _funding_bybit(self, market, since, until, pages):
        acc = []
        # baseCoin (docs: «Base coin, uppercase only») narrows results to symbols of the same
        # base coin, so the account's other symbols do not eat the page limit.
        base_coin = str(market.get('baseId') or market.get('base') or '').upper()
        start = since
        while start <= until:
            end = min(until, start + 7 * _DAY_MS - 1)     # endTime − startTime ≤ 7 days
            window_pages = [0]                             # _MAX_PAGES limit is per window
            cursor = None
            while True:
                self._funding_page(window_pages, acc)
                pages[0] += 1
                # no symbol in the request per docs — filter by the row's symbol field
                params = {'accountType': 'UNIFIED', 'category': 'linear',
                          'type': 'SETTLEMENT', 'startTime': start, 'endTime': end, 'limit': 50}
                if base_coin:
                    params['baseCoin'] = base_coin
                if cursor:
                    params['cursor'] = cursor
                resp = await self.exchange.privateGetV5AccountTransactionLog(params)
                result = (resp or {}).get('result') or {}
                for r in result.get('list') or []:
                    amount = _num(r.get('funding'))
                    if r.get('type') != 'SETTLEMENT' or not amount:
                        continue      # USDC session settlement — no funding
                    if r.get('symbol') != market['id']:
                        continue      # funding for another account symbol
                    side = bybit_settlement_side(r)
                    # hedge: a row per side; the key carries the side (the rows of both sides may share an id)
                    key = r.get('id') if side is None or r.get('id') is None else f"{r.get('id')}:{side}"
                    acc.append((key, _int(r.get('transactionTime')), amount,
                                r.get('currency'), _num(r.get('feeRate')), side))
                next_cursor = result.get('nextPageCursor')
                if not next_cursor or not result.get('list'):
                    break
                if next_cursor == cursor:
                    raise _Incomplete('bybit nextPageCursor without progress', acc)
                cursor = next_cursor
            start = end + 1
        return acc

    async def _funding_okx(self, market, since, until, pages):
        acc = []
        recent = since >= int(time.time() * 1000) - 7 * _DAY_MS
        method = self.exchange.privateGetAccountBills if recent \
            else self.exchange.privateGetAccountBillsArchive
        after = None
        while True:
            self._funding_page(pages, acc)
            params = {'instType': 'SWAP', 'instId': market['id'], 'type': '8',
                      'begin': str(since), 'end': str(until), 'limit': '100'}
            if after:
                params['after'] = after
            resp = await method(params)
            rows = (resp or {}).get('data') or []
            for r in rows:
                if str(r.get('type')) != '8' or \
                        self._funding_other_symbol(r, 'instId', market['id']):
                    continue
                amount = _num(r.get('balChg'))
                if amount is None:
                    amount = _num(r.get('pnl'))
                acc.append((r.get('billId'), _int(r.get('ts')), amount, r.get('ccy'), None))
            if len(rows) < 100:
                break
            after = rows[-1].get('billId')
            if not after:
                raise _Incomplete('okx bills without billId for pagination', acc)
        if not recent:
            self._funding_check_depth(since, 90, acc)
        return acc

    async def _funding_kucoin(self, market, since, until, pages):
        acc = []
        start = since
        while start <= until:
            end = min(until, start + 7 * _DAY_MS - 1)
            chunk_start = start
            while True:
                self._funding_page(pages, acc)
                resp = await self.exchange.futuresPrivateGetFundingHistory({
                    'symbol': market['id'], 'startAt': chunk_start, 'endAt': end,
                    'forward': True, 'maxCount': 200})
                data = (resp or {}).get('data') or {}
                rows = data.get('dataList') or []
                for r in rows:
                    if self._funding_other_symbol(r, 'symbol', market['id']):
                        continue
                    acc.append((r.get('id'), _int(r.get('timePoint')), _num(r.get('funding')),
                                r.get('settleCurrency'), _num(r.get('fundingRate'))))
                if not data.get('hasMore'):
                    break
                last = max((_int(r.get('timePoint')) or 0 for r in rows), default=0)
                if last < chunk_start:
                    raise _Incomplete('kucoin hasMore without progress', acc)
                chunk_start = last + 1
            start = end + 1
        self._funding_check_depth(since, 180, acc)
        return acc

    async def _funding_hyperliquid(self, market, since, until, pages):
        acc = []
        user = getattr(self.exchange, 'walletAddress', None)
        if not user:
            raise ValueError('hyperliquid walletAddress is not set')
        coin = (market.get('info') or {}).get('name') or market.get('base')
        # info endpoint docs, Pagination: «use the last returned timestamp as the next startTime».
        # The next page starts at the same last (not last+1): rows of one settlement across all
        # account coins share one time and may straddle a page boundary. Duplicates
        # are dropped by key hash:ts:coin; a page with no new rows — _Incomplete.
        start = since
        seen = set()
        while True:
            self._funding_page(pages, acc)
            rows = await self.exchange.publicPostInfo({
                'type': 'userFunding', 'user': user, 'startTime': start, 'endTime': until})
            rows = rows or []
            fresh = 0
            for r in rows:
                delta = r.get('delta') or {}
                ts = _int(r.get('time'))
                key = f"{r.get('hash')}:{ts}:{delta.get('coin')}"
                if key in seen:
                    continue
                seen.add(key)
                fresh += 1
                if delta.get('type') != 'funding' or delta.get('coin') != coin:
                    continue
                acc.append((key, ts, _num(delta.get('usdc')), 'USDC',
                            _num(delta.get('fundingRate'))))
            if len(rows) < 500:
                break
            last = max((_int(r.get('time')) or 0 for r in rows), default=0)
            if last < start or not fresh:
                raise _Incomplete('hyperliquid page without progress', acc)
            start = last
        return acc
