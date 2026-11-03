"""Hyperliquid: equity and funds journal for the portfolio.

- Equity — POST /info {type: clearinghouseState}: ``marginSummary.accountValue`` (USDC). In the
  unifiedAccount / portfolioMargin modes (userAbstraction) the docs say to take the trading balance from spot —
  equity = USDC ``total`` of spotClearinghouseState + Σ perp ``unrealizedPnl`` (perp accountValue there is
  only position margin; testnet 2026-10-28: 0 with no positions and 999 USDC in the account).
- Trades — POST /info {type: userFillsByTime}, docs:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#retrieve-a-users-fills-by-time
  ``closedPnl`` — realized PnL, ``fee`` — «the total fee, inclusive of builderFee» (+ paid),
  ``feeToken``; «Returns at most 2000 fills per response and only the 10000 most recent fills are
  available»; pagination — «use the last returned timestamp as the next startTime».
- Funding — POST /info {type: userFunding} (perpetuals): ``delta.usdc`` — balance change
  (sign in the docs example: long with rate > 0 → negative); ≤ 500 records per response, same pagination.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import ConfigError
from bytx.exchanges._income_common import Pages, add, integer, num
from bytx.portfolio import IncomeResult

FILLS_PAGE = 2000
FILLS_AVAILABLE = 10000
FUNDING_PAGE = 500


def _user(ex: Any) -> str:
    user = getattr(ex.ccxt, 'walletAddress', None)
    if not user:
        raise ConfigError('hyperliquid: wallet_address is required', exchange=ex.name)
    return str(user)


def parse_equity(state: dict) -> dict[str, float]:
    value = num(((state or {}).get('marginSummary') or {}).get('accountValue'))
    return {} if value is None else {'USDC': value}


def parse_unified_equity(spot: dict, perp: dict) -> dict[str, float]:
    usdc = next((num(b.get('total')) for b in (spot or {}).get('balances') or [] if b.get('coin') == 'USDC'), None)
    if usdc is None:
        return {}
    upnl = sum(num((a.get('position') or {}).get('unrealizedPnl')) or 0.0 for a in (perp or {}).get('assetPositions') or [])
    return {'USDC': usdc + upnl}


async def equity(ex: Any) -> dict[str, float]:
    from bytx.exchanges.hyperliquid.ccxt_fixes import spot_collateral
    user = _user(ex)
    perp = await ex._call(ex.ccxt.publicPostInfo, {'type': 'clearinghouseState', 'user': user})
    if await spot_collateral(ex.ccxt):
        spot = await ex._call(ex.ccxt.publicPostInfo, {'type': 'spotClearinghouseState', 'user': user})
        return parse_unified_equity(spot, perp)
    return parse_equity(perp)


def parse_fills(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> int:
    fresh = 0
    for r in rows or []:
        key = ('fill', r.get('tid'), r.get('hash'), r.get('time'))
        common = dict(ts=r.get('time'), currency=str(r.get('feeToken') or 'USDC').strip(), since=since,
                      until=until, symbol=r.get('coin'))
        if key in seen:
            continue
        fresh += 1
        fee = num(r.get('fee'))
        add(result, seen, key + ('realized',), kind='realized', amount=r.get('closedPnl'), **common)
        if fee:
            add(result, seen, key + ('fee',), kind='fee', amount=-fee, **common)
        seen.add(key)
    return fresh


def parse_funding(result: IncomeResult, seen: set, rows: list, since: int, until: int) -> int:
    fresh = 0
    for r in rows or []:
        delta = r.get('delta') or {}
        key = ('funding', r.get('hash'), r.get('time'), delta.get('coin'))
        if key in seen:
            continue
        fresh += 1
        if delta.get('type') == 'funding':
            add(result, seen, key + ('v',), ts=r.get('time'), kind='funding', amount=delta.get('usdc'),
                currency='USDC', since=since, until=until, symbol=delta.get('coin'))
        seen.add(key)
    return fresh


async def _paged(ex: Any, result: IncomeResult, seen: set, kind: str, page: int, parse: Any,
                 since: int, until: int) -> int:
    pages = Pages(f'hyperliquid {kind}')
    start, total = since, 0
    while pages.next(result):
        rows = await ex._call(ex.ccxt.publicPostInfo, {'type': kind, 'user': _user(ex), 'startTime': start,
                                                       'endTime': until}) or []
        fresh = parse(result, seen, rows, since, until)
        total += fresh
        if len(rows) < page:
            break
        last = max((integer(r.get('time')) or 0 for r in rows), default=0)
        if last < start or not fresh:
            result.incomplete(f'hyperliquid {kind}: page without progress')
            break
        start = last
    return total


async def income(ex: Any, since: int, until: int, *, symbols: Optional[list] = None,
                 positions: Any = None) -> IncomeResult:
    result = IncomeResult()
    seen: set = set()
    fills = await _paged(ex, result, seen, 'userFillsByTime', FILLS_PAGE, parse_fills, since, until)
    if fills >= FILLS_AVAILABLE:
        result.incomplete(f'hyperliquid: only the last {FILLS_AVAILABLE} trades are available (docs)')
    await _paged(ex, result, seen, 'userFunding', FUNDING_PAGE, parse_funding, since, until)
    return result
