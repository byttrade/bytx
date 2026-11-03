"""MEXC futures: closed position history — GET /api/v1/private/position/list/history_positions.

Fields (https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-user-s-history-position-information): ``positionType`` — «1 long 2 short»,
``state`` — «1holding 2 system auto-holding 3closed», ``openAvgPrice`` / ``closeAvgPrice``, ``closeVol`` (contracts),
``realised`` — «realized profit and loss», ``holdFee`` — «holding fee, positive means get it, negative means lost
it» (funding), ``createTime`` / ``updateTime``. Params: symbol, type, page_num, page_size (max 100);
no time parameters — the window is filtered client-side.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._closed_common import ClosedPosition, fnum

DOC = 'https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-user-s-history-position-information'
LIMIT = 100


def parse(row: dict, symbol: str, contract_size: float = 1.0) -> ClosedPosition:
    side = {1: 'long', 2: 'short'}.get(int(fnum(row.get('positionType')) or 0), '')
    vol = fnum(row.get('closeVol'))
    if not vol:
        vol = fnum(row.get('holdVol'))
    created, updated = fnum(row.get('createTime')), fnum(row.get('updateTime'))
    return ClosedPosition(
        exchange='mexc', symbol=symbol, side=side, size=abs(vol) * contract_size if vol else None,
        entry_price=fnum(row.get('openAvgPrice')), exit_price=fnum(row.get('closeAvgPrice')),
        realized_pnl=fnum(row.get('realised')), funding=fnum(row.get('holdFee')),
        open_ts=int(created) if created else None, close_ts=int(updated) if updated else None,
        id=str(row.get('positionId')) if row.get('positionId') is not None else None, raw=row)


async def closed_positions(ex: Any, symbol: Optional[str] = None, *, since: Optional[int] = None,
                           until: Optional[int] = None, max_pages: int = 50) -> list[ClosedPosition]:
    """Closed positions (state 3), as the exchange returns them; size — in base (× contractSize)."""
    await ex.connect()
    params: dict = {'page_num': 1, 'page_size': LIMIT}
    if symbol:
        params['symbol'] = ex.ccxt.market(ex.symbol(symbol))['id']
    markets = {m['id']: m for m in (ex.ccxt.markets or {}).values() if m.get('swap')}
    out: list[ClosedPosition] = []
    for _ in range(max_pages):
        resp = await ex._call(ex.ccxt.contractPrivateGetPositionListHistoryPositions, dict(params))
        rows = resp.get('data') if isinstance(resp, dict) else None
        rows = rows if isinstance(rows, list) else []
        for r in rows:
            if int(fnum(r.get('state')) or 0) != 3:
                continue
            m = markets.get(r.get('symbol')) or {}
            p = parse(r, m.get('symbol') or str(r.get('symbol') or ''), float(m.get('contractSize') or 1))
            if since is not None and p.close_ts is not None and p.close_ts < since:
                continue
            if until is not None and p.close_ts is not None and p.close_ts > until:
                continue
            out.append(p)
        if len(rows) < LIMIT:
            break
        params['page_num'] += 1
    return out
