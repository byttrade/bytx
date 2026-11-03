"""Bitget: status for health().

The classic docs have no service status endpoint: maintenance is published as text in
«Query Announcements» (annType=maintenance_system_updates — no machine-readable windows). Availability
and latency probe — GET /api/v2/public/time («Get Server Time»); status ``unknown``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = 'https://www.bitget.com/docs/catalog/classic-common-public/classic-common#get-server-time'


async def system_status(ex: Any) -> StatusResult:
    await ex._call(ex.ccxt.publicCommonGetV2PublicTime)
    return StatusResult('unknown', DOC, message='no service status in the docs (maintenance via announcements)')
