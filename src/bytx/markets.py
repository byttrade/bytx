"""Process-wide market cache and precision.

ccxt keeps markets in every instance and loads them with a separate request. With a hundred
accounts on one server that is a hundred identical loads and in-memory copies. Here
markets are loaded once per "exchange + network + market type" key and shared with instances
by reference: the first instance's ready structures (markets, markets_by_id, symbols, ids,
currencies, …) are assigned to the others without ``set_markets`` (it rebuilds every market
via deep_extend — tens of ms of CPU and megabytes per account). ccxt no longer loads markets itself.

Markets are shared and read-only: neither the SDK nor its ccxt patches modify the market dict on
the instance (verified: patch_hyperliquid_markets and ccxt_fixes only patch request responses).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional

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


def _snapshot(exchange: Any) -> dict:
    return {name: getattr(exchange, name, None) for name in _SHARED_ATTRS}


def _share(exchange: Any, entry: _Entry) -> None:
    """Markets from the cache — by reference, without rebuilding (read-only)."""
    for name, value in entry.attrs.items():
        setattr(exchange, name, value)


_cache: dict[tuple, _Entry] = {}
_locks: dict[tuple, asyncio.Lock] = {}


def cache_key(exchange: Any) -> tuple:
    options = exchange.options if isinstance(getattr(exchange, 'options', None), dict) else {}
    testnet = bool(options.get('sandboxMode')) or bool(getattr(exchange, 'isSandboxModeEnabled', False))
    demo = bool(options.get('enableDemoTrading'))
    # Binance: Demo Mode and testnet are both sandboxMode, but with different markets and filters
    env = options.get('bytxDemoEnv')
    return (exchange.id, testnet, demo, options.get('defaultType'), env)


async def ensure_markets(exchange: Any, *, reload: bool = False, ttl: float = DEFAULT_TTL_SEC) -> None:
    """Ensures the instance has markets (from the shared cache or a single load)."""
    key = cache_key(exchange)
    entry = _cache.get(key)
    if entry is not None and not reload and (time.monotonic() - entry.loaded_at) < ttl:
        if not exchange.markets:
            _share(exchange, entry)
        return
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        entry = _cache.get(key)
        if not reload and entry is not None and (time.monotonic() - entry.loaded_at) < ttl:
            _share(exchange, entry)
            return
        await exchange.load_markets(reload=True)
        _cache[key] = _Entry(_snapshot(exchange), time.monotonic())


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
        min_amount=_limit(m, 'amount', 'min'),
        min_cost=sizing.min_cost(exchange_id, m),
        max_leverage=_limit(m, 'leverage', 'max'),
        maker_fee=m.get('maker'),
        taker_fee=m.get('taker'),
        active=m.get('active') is not False,
        raw=m,
    )


__all__ = ['ensure_markets', 'clear_cache', 'cache_key', 'to_market', 'DEFAULT_TTL_SEC']
