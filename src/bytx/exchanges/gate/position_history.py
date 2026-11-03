"""Gate USDT-M: closed position history — GET /futures/{settle}/position_close («Query position close history»).

PositionClose model (https://github.com/gateio/gateapi-python/blob/master/docs/PositionClose.md): ``time`` — «Position close time» (seconds), ``side`` long/short,
``pnl`` (= ``pnl_pnl`` + ``pnl_fund`` + ``pnl_fee``), ``long_price`` / ``short_price`` — «When side is 'long', it
indicates the opening average price …», ``accum_size`` — «Cumulative closed position volume» (contracts),
``first_open_time``, ``text`` — source of the closing order. Params: contract, limit, offset, from, to.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges._closed_common import ClosedPosition, fnum

DOC = 'https://github.com/gateio/gateapi-python/blob/master/docs/FuturesApi.md#list_position_close'
LIMIT = 100


def parse(row: dict, symbol: str, contract_size: float = 1.0) -> ClosedPosition:
    side = str(row.get('side') or '')
    long_price, short_price = fnum(row.get('long_price')), fnum(row.get('short_price'))
    entry, exit_ = (long_price, short_price) if side == 'long' else (short_price, long_price)
    size = fnum(row.get('accum_size'))
    t = fnum(row.get('time'))
    first = fnum(row.get('first_open_time'))
    return ClosedPosition(
        exchange='gate', symbol=symbol, side=side, size=abs(size) * contract_size if size is not None else None,
        entry_price=entry, exit_price=exit_, realized_pnl=fnum(row.get('pnl_pnl')), net_pnl=fnum(row.get('pnl')),
        fee=fnum(row.get('pnl_fee')), funding=fnum(row.get('pnl_fund')),
        open_ts=int(first * 1000) if first is not None else None, close_ts=int(t * 1000) if t is not None else None,
        raw=row)


async def closed_positions(ex: Any, symbol: Optional[str] = None, *, since: Optional[int] = None,
                           until: Optional[int] = None, max_pages: int = 50) -> list[ClosedPosition]:
    """Closed USDT-M positions (as the exchange returns them — newest first); size — in base (× quanto_multiplier)."""
    await ex.connect()
    params: dict = {'settle': 'usdt', 'limit': LIMIT, 'offset': 0}
    if symbol:
        params['contract'] = ex.ccxt.market(ex.symbol(symbol))['id']
    if since is not None:
        params['from'] = since // 1000
    if until is not None:
        params['to'] = -(-until // 1000)
    markets = {m['id']: m for m in (ex.ccxt.markets or {}).values() if m.get('swap')}
    out: list[ClosedPosition] = []
    for _ in range(max_pages):
        rows = await ex._call(ex.ccxt.privateFuturesGetSettlePositionClose, dict(params))
        rows = rows if isinstance(rows, list) else []
        for r in rows:
            m = markets.get(r.get('contract')) or {}
            out.append(parse(r, m.get('symbol') or str(r.get('contract') or ''), float(m.get('contractSize') or 1)))
        if len(rows) < LIMIT:
            break
        params['offset'] += len(rows)
    return out
