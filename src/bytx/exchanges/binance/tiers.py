"""Binance USDⓈ-M: leverage and maintenance margin tiers (brackets).

1. With keys — the official «Notional and Leverage Brackets» (GET /fapi/v1/leverageBracket,
   USER_DATA, signed; https://developers.binance.com/docs/derivatives/usds-margined-futures/account/rest-api/Notional-and-Leverage-Brackets):
   ``bracket``, ``initialLeverage``, ``notionalFloor``/``notionalCap``, ``maintMarginRatio``.
2. Without keys (or a key without permissions) — a **fallback public source**: the exchange's web endpoint
   ``/bapi/futures/v1/friendly/future/common/brackets`` (``riskBrackets``: ``bracketSeq``,
   ``bracketNotionalFloor``/``Cap``, ``bracketMaintenanceMarginRate``, ``maxOpenPosLeverage``).
   **It is not in the official documentation** — the format is pinned by a response snapshot from 2026-10-25
   (``tests/ground_truth/binance/bapi_brackets.json``).
   The table is ~1.5 MB for all symbols — one request per process, successes cached for ``BRACKETS_TTL_SEC``.
"""
from __future__ import annotations

import asyncio
import time
import weakref
from typing import Any

from bytx.exchanges._tiers_common import from_ccxt, num
from bytx.types import LeverageTier

PUBLIC_BRACKETS_URL = 'https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets'
BRACKETS_TTL_SEC = 3600.0

_cache: dict[str, Any] = {'rows': None, 'at': 0.0}
_locks: 'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]' = weakref.WeakKeyDictionary()


def clear_cache() -> None:
    _cache.update(rows=None, at=0.0)


def parse_public(payload: dict) -> dict[str, list[LeverageTier]]:
    """bapi brackets response → {symbol id: tiers}."""
    out: dict[str, list[LeverageTier]] = {}
    for item in ((payload or {}).get('data') or {}).get('brackets') or []:
        tiers = [LeverageTier(
            tier=int(b['bracketSeq']), max_leverage=num(b.get('maxOpenPosLeverage')),
            maintenance_margin_rate=num(b.get('bracketMaintenanceMarginRate')),
            min_notional=num(b.get('bracketNotionalFloor')), max_notional=num(b.get('bracketNotionalCap')),
            source='binance_public', raw=b) for b in item.get('riskBrackets') or []]
        if tiers:
            out[str(item.get('symbol'))] = sorted(tiers, key=lambda t: t.tier)
    return out


async def _public(ex: Any) -> dict[str, list[LeverageTier]]:
    rows = _cache['rows']
    if rows is not None and time.monotonic() - _cache['at'] < BRACKETS_TTL_SEC:
        return rows
    lock = _locks.setdefault(asyncio.get_running_loop(), asyncio.Lock())  # one request per process
    async with lock:
        rows = _cache['rows']
        if rows is not None and time.monotonic() - _cache['at'] < BRACKETS_TTL_SEC:
            return rows
        # ccxt transport: the instance's proxy, session and timeout; no keys needed
        payload = await ex._call(ex.ccxt.fetch, PUBLIC_BRACKETS_URL, 'GET')
        rows = parse_public(payload)
        if not rows:
            from bytx.errors import ExchangeError
            raise ExchangeError('binance public brackets: no tiers in the response', exchange=ex.name)
        _cache.update(rows=rows, at=time.monotonic())
        return rows


async def leverage_tiers(ex: Any, s: str, market: dict) -> list[LeverageTier]:
    from bytx.errors import AuthError, NotSupported
    if ex.ccxt.apiKey:
        try:
            return from_ccxt(await ex._call(ex.ccxt.fetch_market_leverage_tiers, s))
        except AuthError:
            pass  # key without USER_DATA permission — public source
    rows = (await _public(ex)).get(str(market.get('id')))
    if not rows:
        raise NotSupported(f'no brackets for {s} in the binance public table', exchange=ex.name)
    return rows
