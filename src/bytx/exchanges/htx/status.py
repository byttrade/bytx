"""HTX: status for health() — «Query whether the system is available» (https://api.hbdm.com/heartbeat/).

``linear_swap_heartbeat`` — «USDT margined Contracts 1: avaiable 0: not available(maintenance with service
suspended)», ``linear_swap_estimated_recovery_time`` — «null: normal. … estimated recovery time millionseconds».
"""
from __future__ import annotations

from typing import Any

from bytx.health import MaintenanceEvent, StatusResult

DOC = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#query-whether-the-system-is-available'


def parse(response: Any) -> StatusResult:
    data = response.get('data') if isinstance(response, dict) else None
    data = data if isinstance(data, dict) else {}
    beat = data.get('linear_swap_heartbeat')
    if beat == 1:
        return StatusResult('ok', DOC)
    if beat == 0:
        end = data.get('linear_swap_estimated_recovery_time')
        return StatusResult('maintenance', DOC, [MaintenanceEvent('ongoing', 'linear_swap_heartbeat=0',
                                                                  end=int(end) if end else None)])
    return StatusResult('unknown', DOC, message='no linear_swap_heartbeat in the response')


async def system_status(ex: Any) -> StatusResult:
    return parse(await ex._call(ex.ccxt.contractPublicGetHeartbeat))
