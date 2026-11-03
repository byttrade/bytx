"""Hyperliquid: status for health().

The «Rate limits and user limits» docs mention the ``exchangeStatus`` info request (weight 2), but the
response format isn't described — we don't interpret the response: status ``unknown``, the request serves as a probe
of REST availability and latency. HL doesn't report server time (clock skew — None).
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits'


async def system_status(ex: Any) -> StatusResult:
    await ex._call(ex.ccxt.publicPostInfo, {'type': 'exchangeStatus'})
    return StatusResult('unknown', DOC, message='exchangeStatus format is not described in the docs')
