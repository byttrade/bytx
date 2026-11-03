"""OKX: system status for health() — GET /api/v5/system/status («Get event status»).

Docs: https://www.okx.com/docs-v5/en/#status-get-status — without ``state`` «the data with status
scheduled, ongoing and pre_open will be returned by default»; ``pre_open`` — «Canceling orders,
placing Post Only orders, and transferring funds to trading accounts are back after preOpenBegin»;
``serviceType`` — «0 : WebSocket 5 : Trading service 6 : Block trading 7 : Trading bot 8 : Trading
service (in batches of accounts) 9 : Trading service (in batches of products) 10 : Spread trading
11 : Copy trading 99 : Others»; ``env`` — «1 : Production Trading 2 : Demo Trading».
Ongoing work: 5 → ``maintenance``; 8/9 (some accounts/products), 0, pre_open and others → ``degraded``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import MaintenanceEvent, Status, StatusResult, worst

DOC = 'https://www.okx.com/docs-v5/en/#status-get-status'
FULL_TRADING = frozenset({'5'})
PARTIAL_TRADING = frozenset({'8', '9'})
NOT_OUR_SERVICES = frozenset({'6', '7', '10', '11'})   # block trading, bots, spreads, copy trading


def _int(v: Any) -> Any:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse(response: dict, *, demo: bool) -> StatusResult:
    rows = (response or {}).get('data') or []
    env = '2' if demo else '1'
    status: Status = 'ok'
    events: list[MaintenanceEvent] = []
    for r in rows:
        state = str(r.get('state') or '')
        if state not in ('scheduled', 'ongoing', 'pre_open'):
            continue
        if str(r.get('env') or env) != env:
            continue
        service = str(r.get('serviceType') or '')
        if service in NOT_OUR_SERVICES:
            continue
        partial = service in PARTIAL_TRADING
        trading = service in FULL_TRADING or partial
        events.append(MaintenanceEvent(state, str(r.get('title') or ''), _int(r.get('begin')), _int(r.get('end')),
                                       affects_trading=trading, partial=partial, raw=r))
        if state == 'ongoing':
            status = worst(status, 'maintenance' if service in FULL_TRADING else 'degraded')
        elif state == 'pre_open':
            status = worst(status, 'degraded')
    return StatusResult(status, DOC, events)


async def system_status(ex: Any) -> StatusResult:
    return parse(await ex._call(ex.ccxt.publicGetSystemStatus), demo=ex.demo)
