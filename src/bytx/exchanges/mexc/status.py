"""MEXC: status for health().

The Contract API has no service status endpoint; availability and latency probe — GET /api/v1/contract/ping
(«Get the server time»); status ``unknown``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = 'https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-the-server-time'


async def system_status(ex: Any) -> StatusResult:
    await ex._call(ex.ccxt.contractPublicGetPing)
    return StatusResult('unknown', DOC, message='no service status in the docs')
