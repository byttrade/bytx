"""Binance: system status for health() — GET /sapi/v1/system/status («System Status (System)»).

Docs: https://developers.binance.com/docs/wallet/others/system-status — ``status``: «0: normal,
1: system maintenance»; ``msg``: «normal» / «system_maintenance»; public, IP weight 1. The endpoint
is exchange-wide (wallet); the docs have no separate USDⓈ-M status.
"""
from __future__ import annotations

from typing import Any

from bytx.health import MaintenanceEvent, StatusResult

DOC = 'https://developers.binance.com/docs/wallet/others/system-status'


def parse(response: dict) -> StatusResult:
    r = response or {}
    code = r.get('status')
    msg = r.get('msg')
    if code is None:
        return StatusResult('unknown', DOC, message=msg)
    if int(code) == 1:
        return StatusResult('maintenance', DOC, [MaintenanceEvent('ongoing', str(msg or 'system_maintenance'),
                                                                  raw=r)], message=msg)
    return StatusResult('ok', DOC, message=msg)


async def system_status(ex: Any) -> StatusResult:
    if 'sapi' not in ex.ccxt.urls.get('api', {}):
        # Demo Mode / testnet: no /sapi (Demo Mode docs — only /api, /fapi, /dapi)
        return StatusResult('unknown', DOC, message=f'{ex.demo_env or "demo"}: /sapi/v1/system/status '
                                                    'unavailable (no /sapi in Demo Mode and testnet)', probed=False)
    return parse(await ex._call(ex.ccxt.sapiGetSystemStatus))
