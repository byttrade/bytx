"""Process-wide market cache and precision.

ccxt keeps markets in every instance and loads them with a separate request. With a hundred
accounts on one server that is a hundred identical loads and in-memory copies. Here
markets are loaded once per "exchange + network + market type" key and shared with instances
by reference: the first instance's ready structures (markets, markets_by_id, symbols, ids,
currencies, …) are assigned to the others without ``set_markets`` (it rebuilds every market
via deep_extend — tens of ms of CPU and megabytes per account). ccxt no longer loads markets itself.

Markets are shared and read-only: neither the SDK nor its ccxt patches modify the market dict on
the instance (verified: patch_hyperliquid_markets and ccxt_fixes only patch request responses).

A reload (TTL expired on a new ``connect`` or ``reload_markets``) hands the new copy to all live instances that held
the previous one (weak references) — one copy of a key's markets per process. Before: every hour a new connect added
a new copy while long-lived instances kept the old one — N copies of ~40 MiB (Binance with spot) and memory growth.
"""
from __future__ import annotations

import asyncio
import time
import weakref
from dataclasses import dataclass, field
from typing import Any, Optional

from loguru import logger

from bytx import sizing
from bytx.types import Market

DEFAULT_TTL_SEC = 3600.0


# ccxt attributes filled by set_markets: shared with instances by reference.
_SHARED_ATTRS = ('markets', 'markets_by_id', 'symbols', 'ids', 'currencies', 'currencies_by_id',
                 'codes', 'baseCurrencies', 'quoteCurrencies')


@dataclass(slots=True)
class _Entry:
    attrs: dict
    loaded_at: float
    #: instances these markets were handed to (weak references: a closed and forgotten instance is not kept)
    holders: 'weakref.WeakSet[Any]' = field(default_factory=weakref.WeakSet)


def _snapshot(exchange: Any) -> dict:
    return {name: getattr(exchange, name, None) for name in _SHARED_ATTRS}


def share_markets(exchange: Any, entry: _Entry) -> None:
    """Markets from the cache — by reference, without rebuilding (read-only); the instance is recorded as a holder of
    the copy."""
    for name, value in entry.attrs.items():
        setattr(exchange, name, value)
    # ccxt keeps the result of the previous load in markets_loading (a finished task → the previous markets dict):
    # without a reset the old copy would live on in every instance that loaded markets itself. A load in progress
    # is left alone
    loading = getattr(exchange, 'markets_loading', None)
    if loading is not None and callable(getattr(loading, 'done', None)) and loading.done():
        exchange.markets_loading = None
    entry.holders.add(exchange)


def publish_markets(store: dict, key: Any, exchange: Any) -> _Entry:
    """Markets just loaded by ``exchange`` — into ``store[key]`` and to all live holders of the previous copy (unless
    their markets are their own): after a reload there is one copy per process. ``store`` — the cache."""
    previous = store.get(key)
    entry = _Entry(_snapshot(exchange), time.monotonic())
    entry.holders.add(exchange)
    if isinstance(previous, _Entry):
        old = previous.attrs.get('markets')
        for holder in list(previous.holders):
            if holder is exchange:
                continue
            # markets the instance loaded itself (not from the cache) are left alone
            if old is not None and getattr(holder, 'markets', None) is old:
                share_markets(holder, entry)
    store[key] = entry
    if previous is not None:
        logger.info('bytx: markets reloaded key={} holders={}', key, len(entry.holders))
    return entry


def fresh_markets(store: dict, key: Any, ttl: float) -> Optional[_Entry]:
    """The cache entry ``store[key]`` if it is younger than ``ttl`` seconds; otherwise None."""
    entry = store.get(key)
    if isinstance(entry, _Entry) and (time.monotonic() - entry.loaded_at) < ttl:
        return entry
    return None


_cache: dict[tuple, _Entry] = {}
_locks: dict[tuple, asyncio.Lock] = {}


def cache_key(exchange: Any) -> tuple:
    options = exchange.options if isinstance(getattr(exchange, 'options', None), dict) else {}
    testnet = bool(options.get('sandboxMode')) or bool(getattr(exchange, 'isSandboxModeEnabled', False))
    demo = bool(options.get('enableDemoTrading'))
    # Binance: Demo Mode and testnet are both sandboxMode, but with different markets and filters
    env = options.get('bytxDemoEnv')
    # the set of loaded market types (a swap client loads only its own): different sets — different copies
    types = options.get('fetchMarkets')
    types = tuple(types) if isinstance(types, (list, tuple)) else (None if types is None else repr(types))
    return (exchange.id, testnet, demo, options.get('defaultType'), env, types)


async def ensure_markets(exchange: Any, *, reload: bool = False, ttl: float = DEFAULT_TTL_SEC) -> None:
    """Ensures the instance has markets (from the shared cache or a single load)."""
    key = cache_key(exchange)
    entry = fresh_markets(_cache, key, ttl)
    if entry is not None and not reload:
        if not exchange.markets:
            share_markets(exchange, entry)
        return
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        entry = fresh_markets(_cache, key, ttl)
        if not reload and entry is not None:
            share_markets(exchange, entry)
            return
        await exchange.load_markets(reload=True)
        publish_markets(_cache, key, exchange)


def stats() -> dict:
    """Observability of the shared market cache: key → holders, the number of distinct market copies among them
    (normally 1) and the load time (``time.monotonic``)."""
    out = {}
    for key, entry in list(_cache.items()):
        holders = list(entry.holders)
        copies = {id(getattr(h, 'markets', None)) for h in holders if getattr(h, 'markets', None)}
        out[key] = {'holders': len(holders), 'copies': len(copies), 'loaded_at': entry.loaded_at}
    return out


def clear_cache() -> None:
    _cache.clear()
    _locks.clear()


def _limit(market: dict, group: str, bound: str) -> Optional[float]:
    value = ((market.get('limits') or {}).get(group) or {}).get(bound)
    return None if value is None else float(value)


def to_market(m: dict, exchange_id: Optional[str] = None) -> Market:
    """market-dict ccxt → :class:`Market`.

    ``exchange_id`` (ccxt id) enables exchange rules for ``min_cost``: Bybit ``minNotionalValue``, Binance ``MIN_NOTIONAL``,
    Hyperliquid $10 (:func:`bytx.sizing.min_cost`). Without it — only the market's ``limits.cost.min``.
    No limit — None.
    """
    precision = m.get('precision') or {}
    contract_size = m.get('contractSize') if m.get('contract') else None
    return Market(
        symbol=m['symbol'],
        id=str(m.get('id')),
        base=m.get('base') or '',
        quote=m.get('quote') or '',
        settle=m.get('settle'),
        type=m.get('type') or ('swap' if m.get('swap') else 'spot'),
        contract_size=float(contract_size) if contract_size else 1.0,
        amount_step=None if precision.get('amount') is None else float(precision['amount']),
        price_step=None if precision.get('price') is None else float(precision['price']),
        # Bybit spot: minOrderQty «Deprecated, please use minOrderAmt» (Instruments Info) — the minimum size is the step
        min_amount=None if exchange_id == 'bybit' and m.get('spot') else _limit(m, 'amount', 'min'),
        min_cost=sizing.min_cost(exchange_id, m),
        max_leverage=_limit(m, 'leverage', 'max'),
        maker_fee=m.get('maker'),
        taker_fee=m.get('taker'),
        active=m.get('active') is not False,
        raw=m,
    )


__all__ = ['ensure_markets', 'clear_cache', 'cache_key', 'fresh_markets', 'publish_markets', 'share_markets', 'stats',
           'to_market', 'DEFAULT_TTL_SEC']
