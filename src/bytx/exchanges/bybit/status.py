"""Bybit v5: system status for health() — GET /v5/system/status («Get System Status»).

Docs: https://bybit-exchange.github.io/docs/v5/system-status. Record fields: ``state`` — enum
«scheduled, ongoing, completed, canceled»; ``serviceTypes`` — «1 Trading service, 2 … via http request,
3 … via websocket, 4 Private websocket stream, 5 Market data service»; ``product`` — «1 Futures, 2 Spot,
3 Option, 4 Spread»; ``uidSuffix`` — «Affected UID tail number»; ``env`` — «1 Production, 2 Production
Demo service»; ``begin`` / ``end`` — ms as string. ccxt 4.4.65 lacks the endpoint — ``ccxt.request`` (public).
Ongoing work affecting our product (swap → Futures, spot → Spot) and environment: trading services (1–3) →
``maintenance``, only WS/data or part of UIDs → ``degraded``.
"""
from __future__ import annotations

from typing import Any

from bytx.health import MaintenanceEvent, Status, StatusResult, worst

DOC = 'https://bybit-exchange.github.io/docs/v5/system-status'
TRADING_SERVICES = frozenset({1, 2, 3})
PRODUCTS = {'swap': 1, 'spot': 2}


def _int(v: Any) -> Any:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def parse(response: dict, *, demo: bool, market_type: str) -> StatusResult:
    rows = ((response or {}).get('result') or {}).get('list') or []
    env = 2 if demo else 1
    product = PRODUCTS.get(market_type, 1)
    status: Status = 'ok'
    events: list[MaintenanceEvent] = []
    for r in rows:
        state = str(r.get('state') or '')
        if state not in ('scheduled', 'ongoing'):
            continue
        if _int(r.get('env')) not in (None, env):
            continue
        products = {_int(p) for p in r.get('product') or []}
        if products and product not in products:
            continue
        services = {_int(s) for s in r.get('serviceTypes') or []}
        trading = not services or bool(services & TRADING_SERVICES)
        partial = bool(r.get('uidSuffix'))
        events.append(MaintenanceEvent(state, str(r.get('title') or ''), _int(r.get('begin')), _int(r.get('end')),
                                       affects_trading=trading, partial=partial, raw=r))
        if state == 'ongoing':
            status = worst(status, 'maintenance' if trading and not partial else 'degraded')
    return StatusResult(status, DOC, events)


async def system_status(ex: Any) -> StatusResult:
    response = await ex._call(ex.ccxt.request, 'v5/system/status', 'public', 'GET', {})
    return parse(response, demo=ex.demo, market_type=ex.market_type)
