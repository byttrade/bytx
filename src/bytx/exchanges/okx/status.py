"""OKX: system status for health() — GET /api/v5/system/status («Get event status»).

Docs: https://www.okx.com/docs-v5/en/#status-get-status — without ``state`` «the data with status
scheduled, ongoing and pre_open will be returned by default»; ``pre_open`` — «Canceling orders,
placing Post Only orders, and transferring funds to trading accounts are back after preOpenBegin»;
``serviceType`` — «0 : WebSocket 5 : Trading service 6 : Block trading 7 : Trading bot 8 : Trading
service (in batches of accounts) 9 : Trading service (in batches of products) 10 : Spread trading
11 : Copy trading 99 : Others»; ``env`` — «1 : Production Trading 2 : Demo Trading».
Ongoing work: 5 → ``maintenance``; 8/9 (some accounts/products), 0, pre_open and others → ``degraded``.

Rate: OKX has a limit per endpoint («The rate limit is different for each endpoint. You can find the limit for each
endpoint from the endpoint details», Rate Limits docs). ccxt 4.4.65 charges the status with cost 50 (×103 ms = 1
request / 5 s) in the SHARED IP limiter — after health() any next request from this IP (orders of all accounts)
waited ~5 s (demo 2026-11-04). Here: cost 1 in the shared limiter, and the endpoint's own rate — the result is
cached for :data:`MIN_INTERVAL` s per IP key and environment (concurrent calls — one request; a repeat from the cache
has ``probed=False``: it does not prove the exchange is reachable).
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
import weakref
from typing import Any, Callable

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


#: not more often than once per this many seconds per IP key and environment (ccxt: cost 50 × rateLimit 103 ms)
MIN_INTERVAL = 5.0
#: monotonic clock of the cache (tests replace it)
clock: Callable[[], float] = time.monotonic
# loop → (limiter key, demo) → [lock, (time, StatusResult) | None]; the lock is bound to the loop
_cache: 'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict]' = weakref.WeakKeyDictionary()


def reset_cache() -> None:
    _cache.clear()


async def system_status(ex: Any) -> StatusResult:
    by_key = _cache.setdefault(asyncio.get_running_loop(), {})
    slot = by_key.setdefault((ex.rate_limit_key, bool(ex.demo)), [asyncio.Lock(), None])
    async with slot[0]:
        hit = slot[1]
        if hit is not None and clock() - hit[0] < MIN_INTERVAL:
            age = clock() - hit[0]
            return dataclasses.replace(hit[1], probed=False,
                                       message=f'from cache {age:.1f} s (the status endpoint — at most once per '
                                               f'{MIN_INTERVAL:g} s)')
        response = await ex._call(ex.ccxt.request, 'system/status', 'public', 'GET', {}, None, None, {'cost': 1})
        result = parse(response, demo=ex.demo)
        slot[1] = (clock(), result)
        return result
