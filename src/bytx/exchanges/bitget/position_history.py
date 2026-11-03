"""Bitget USDT-M: closed position history — GET /api/v2/mix/position/history-position.

Docs (https://www.bitget.com/docs/catalog/classic-contract-position/classic-contract-position#get-historical-position): «Only check the
data within 3 months»; limit «Default 20 Max 100»; pages — ``idLessThan`` = ``endId``. Record fields:
``holdSide``, ``openAvgPrice`` / ``closeAvgPrice``, ``openTotalPos`` / ``closeTotalPos`` (base),
``pnl``, ``netProfit``, ``totalFunding``, ``openFee`` / ``closeFee``, ``ctime`` / ``utime``.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._closed_common import ClosedPosition, fnum

DOC = 'https://www.bitget.com/docs/catalog/classic-contract-position/classic-contract-position#get-historical-position'
LIMIT = 100
HISTORY_DAYS = 90


def parse(row: dict, symbol: Optional[str] = None) -> ClosedPosition:
    fees = [v for v in (fnum(row.get('openFee')), fnum(row.get('closeFee'))) if v is not None]
    return ClosedPosition(
        exchange='bitget', symbol=symbol or str(row.get('symbol') or ''), side=str(row.get('holdSide') or ''),
        size=fnum(row.get('closeTotalPos')), entry_price=fnum(row.get('openAvgPrice')),
        exit_price=fnum(row.get('closeAvgPrice')), realized_pnl=fnum(row.get('pnl')),
        net_pnl=fnum(row.get('netProfit')), fee=sum(fees) if fees else None, funding=fnum(row.get('totalFunding')),
        open_ts=_int(row.get('ctime')), close_ts=_int(row.get('utime')), id=str(row.get('positionId') or '') or None,
        raw=row)


def _int(value: Any) -> Optional[int]:
    v = fnum(value)
    return int(v) if v is not None else None


async def closed_positions(ex: Any, symbol: Optional[str] = None, *, since: Optional[int] = None,
                           until: Optional[int] = None, max_pages: int = 50) -> list[ClosedPosition]:
    """Closed positions (newest first); ``symbol`` — SDK symbol or None (all USDT-M)."""
    await ex.connect()
    params: dict = {'productType': 'USDT-FUTURES', 'limit': str(LIMIT)}
    market_symbol = None
    if symbol:
        market_symbol = ex.symbol(symbol)
        params['symbol'] = ex.ccxt.market(market_symbol)['id']
    if since is not None:
        params['startTime'] = str(since)
    if until is not None:
        params['endTime'] = str(until)
    out: list[ClosedPosition] = []
    by_id = {m['id']: m['symbol'] for m in ex.ccxt.markets.values() if m.get('swap')} if ex.ccxt.markets else {}
    for _ in range(max_pages):
        data = ((await ex._call(ex.ccxt.privateMixGetV2MixPositionHistoryPosition, dict(params))) or {}).get('data') or {}
        rows = data.get('list') or []
        out.extend(parse(r, market_symbol or by_id.get(r.get('symbol'))) for r in rows)
        if len(rows) < LIMIT or not data.get('endId'):
            break
        params['idLessThan'] = str(data['endId'])
    return out
