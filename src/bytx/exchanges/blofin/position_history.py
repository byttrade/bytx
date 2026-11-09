"""BloFin USDT-M: closed position history — GET /api/v1/account/positions-history (F#get-positions-history; not in
ccxt). Fields: ``positionSide`` (net — side from the opening ``side``), ``closePositions`` (contracts),
``openAveragePrice`` / ``closeAveragePrice``, ``realizedPnl``, ``fee`` (paid, positive), ``state``
partially_closed | closed | liquidated | partially_liquidated | adl; cursor ``after`` = ``historyId``, limit ≤ 100.
Retention depth is not stated in the docs.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._closed_common import ClosedPosition, fnum

DOC = 'https://docs.blofin.com/index.html#get-positions-history'
LIMIT = 100


def parse(row: dict, symbol: str, contract_size: float) -> ClosedPosition:
    side = str(row.get('positionSide') or '').lower()
    if side not in ('long', 'short'):
        side = 'long' if str(row.get('side') or '').lower() == 'buy' else 'short'
    closed = fnum(row.get('closePositions'))
    fee = fnum(row.get('fee'))
    return ClosedPosition(
        exchange='blofin', symbol=symbol, side=side, size=abs(closed) * contract_size if closed is not None else None,
        entry_price=fnum(row.get('openAveragePrice')), exit_price=fnum(row.get('closeAveragePrice')),
        realized_pnl=fnum(row.get('realizedPnl')), fee=-fee if fee is not None else None,
        open_ts=_int(row.get('createTime')), close_ts=_int(row.get('updateTime')),
        id=str(row.get('positionId') or '') or None, raw=row)


def _int(value: Any) -> Optional[int]:
    v = fnum(value)
    return int(v) if v is not None else None


async def closed_positions(ex: Any, symbol: Optional[str] = None, *, since: Optional[int] = None,
                           until: Optional[int] = None, max_pages: int = 50) -> list[ClosedPosition]:
    """Closed positions (newest first); ``symbol`` — SDK symbol or None (all USDT-M)."""
    await ex.connect()
    params: dict = {'limit': str(LIMIT)}
    if symbol:
        params['instId'] = ex.ccxt.market(ex.symbol(symbol))['id']
    if since is not None:
        params['begin'] = str(since)
    if until is not None:
        params['end'] = str(until)
    out: list[ClosedPosition] = []
    for _ in range(max_pages):
        rows = ((await ex._call(ex.ccxt.request, 'account/positions-history', 'private', 'GET', dict(params)))
                or {}).get('data') or []
        for r in rows:
            m = (ex.ccxt.markets_by_id or {}).get(r.get('instId'))
            market = m[0] if isinstance(m, list) and m else None
            if market is not None:
                out.append(parse(r, market['symbol'], float(market.get('contractSize') or 1.0)))
        if len(rows) < LIMIT or not rows[-1].get('historyId'):
            break
        params['after'] = str(rows[-1]['historyId'])
    return out
