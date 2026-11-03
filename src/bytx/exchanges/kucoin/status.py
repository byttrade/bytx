"""KuCoin Futures: service status for health() — GET /api/v1/status («Get Service Status»).

Docs: https://www.kucoin.com/docs-new/api-3470230 — ``status``: «open: normal transaction; close:
Stop Trading/Maintenance; cancelonly: can only cancel the order but not place order»; ``msg``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import MaintenanceEvent, Status, StatusResult

DOC = 'https://www.kucoin.com/docs-new/api-3470230'
STATUSES: dict[str, Status] = {'open': 'ok', 'close': 'maintenance', 'cancelonly': 'cancel_only'}


def parse(response: dict) -> StatusResult:
    data = (response or {}).get('data') or {}
    raw = str(data.get('status') or '')
    status = STATUSES.get(raw, 'unknown')
    events = [] if status in ('ok', 'unknown') else [MaintenanceEvent('ongoing', str(data.get('msg') or raw), raw=data)]
    return StatusResult(status, DOC, events, message=data.get('msg') or None)


async def system_status(ex: Any) -> StatusResult:
    return parse(await ex._call(ex.ccxt.futuresPublicGetStatus))
