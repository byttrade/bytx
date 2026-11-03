"""Gate: status for health().

APIv4 has no service status endpoint (maintenance via announcements; contracts have ``status`` «prelaunch, trading,
delisting, delisted, circuit_breaker»). Availability and latency probe — GET /spot/time («Get server current
time»); status ``unknown``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = 'https://github.com/gateio/gateapi-python/blob/master/docs/SpotApi.md#get_system_time'


async def system_status(ex: Any) -> StatusResult:
    await ex._call(ex.ccxt.publicSpotGetTime)
    return StatusResult('unknown', DOC, message='no service status in the docs')
