"""BloFin: status for health().

The docs have no status / maintenance or server-time endpoints (``public/time`` — 401, demo 2026-11-09).
Availability and latency probe — GET /api/v1/market/tickers (F#get-tickers); status ``unknown``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import StatusResult

DOC = 'https://docs.blofin.com/index.html#get-tickers'


async def system_status(ex: Any) -> StatusResult:
    await ex._call(ex.ccxt.request, 'market/tickers', 'public', 'GET', {'instId': 'BTC-USDT'})
    return StatusResult('unknown', DOC, message='the docs have no service status')
