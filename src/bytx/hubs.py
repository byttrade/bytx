"""Process-wide WS connections shared by many bots.

- :func:`account` — one :class:`AccountHub` per (exchange, demo, account): one private
    connection, order and position trackers for ALL account symbols, and a balance stream. Bots on the same
    key share it; filtering by symbol is up to the consumer.
- :func:`market` — one :class:`MarketHub` per (exchange, demo): public tickers for the symbols of all bots.

Hubs are reference-counted: :func:`release` by the last consumer closes the connection.
Each hub is its own :class:`~bytx.Exchange` (its own ccxt.pro instance), not the bot's REST instance:
the caller's order placement locks live on it, so a shared client would
deadlock.

Reads return data only while the stream is current (tracker ``live``, ticker fresh), otherwise
``None`` — the caller takes the usual REST path. A hub never places or cancels anything.
"""
from __future__ import annotations

import asyncio
import hashlib
from typing import Any, Callable, Optional

from loguru import logger

from bytx.types import Balance, Order, Position

# Credential fields that distinguish accounts (no secrets in the key — only a hash)
_ACCOUNT_FIELDS = ('api_key', 'apiKey', 'wallet_address', 'walletAddress', 'uid')


def account_fingerprint(name: str, demo: bool, creds: dict) -> str:
    ident = next((str(creds[f]) for f in _ACCOUNT_FIELDS if creds.get(f)), '')
    raw = f'{name.lower()}|{int(bool(demo))}|{ident}'
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class _Hub:
    def __init__(self, key: tuple, ex: Any) -> None:
        self.key = key
        self.exchange = ex
        self.refs = 0
        self._closing = False

    async def _close(self) -> None:
        self._closing = True
        try:
            await self.exchange.close()
        except Exception as e:  # noqa: BLE001 — closing must not crash the consumer
            logger.warning(f'hub {self.key[0]}: close failed: {type(e).__name__}: {e}')


class AccountHub(_Hub):
    """Private account state from WS: orders, positions, balance."""

    def __init__(self, key: tuple, ex: Any) -> None:
        super().__init__(key, ex)
        self.orders = None
        self.positions = None
        self._balance_sub = None
        self._balance: Optional[Balance] = None
        self._balance_task: Optional[asyncio.Task] = None

    async def _start(self) -> None:
        self.orders = await self.exchange.order_tracker().start()
        try:
            self.positions = await self.exchange.position_tracker().start()
        except Exception as e:  # noqa: BLE001 — no WS positions (NotSupported): REST only
            logger.info(f'hub {self.key[0]}: positions stream unavailable ({type(e).__name__}), REST')
            self.positions = None
        try:
            if self.key[1] == 'binance':
                # Binance sends ACCOUNT_UPDATE only on change: REST snapshot on subscribe (ccxt option)
                opts = self.exchange.ccxt.options.setdefault('watchBalance', {})
                opts.update(fetchBalanceSnapshot=True, awaitBalanceSnapshot=False)
            self._balance_sub = await self.exchange.stream.balance().open()
            self._balance_task = asyncio.ensure_future(self._consume_balance())
        except Exception as e:  # noqa: BLE001 — no WS balance (Hyperliquid): REST only
            logger.info(f'hub {self.key[0]}: balance stream unavailable ({type(e).__name__}), REST')
            self._balance_sub = None

    async def _consume_balance(self) -> None:
        try:
            async for b in self._balance_sub:
                self._balance = b
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.warning(f'hub {self.key[0]}: balance stream stopped: {type(e).__name__}: {e}')
        self._balance = None

    # ─── reads (None — stream not current, go to REST) ────────────────────

    @property
    def orders_live(self) -> bool:
        return self.orders is not None and self.orders.live

    @property
    def positions_live(self) -> bool:
        return self.positions is not None and self.positions.live

    def open_orders(self, symbol: Optional[str] = None) -> Optional[list[Order]]:
        if not self.orders_live:
            return None
        rows = self.orders.open_orders
        return [o for o in rows if symbol is None or o.symbol == symbol]

    def order(self, order_id: str) -> Optional[Order]:
        return self.orders.get(order_id) if self.orders_live else None

    def position(self, symbol: str, side: Optional[str] = None) -> Optional[Position]:
        """Position; ``None`` — stream not current. Empty position — Position with size 0 or no record."""
        return self.positions.get(symbol, side) if self.positions_live else None

    def balance(self) -> Optional[Balance]:
        return self._balance

    async def _close(self) -> None:
        if self._balance_task is not None:
            self._balance_task.cancel()
        for part in (self.orders, self.positions):
            if part is not None:
                try:
                    await part.stop()
                except Exception:  # noqa: BLE001
                    pass
        if self._balance_sub is not None:
            try:
                await self._balance_sub.aclose()
            except Exception:  # noqa: BLE001
                pass
        await super()._close()


class MarketHub(_Hub):
    """Public tickers: one subscription per symbol per process."""

    def __init__(self, key: tuple, ex: Any) -> None:
        super().__init__(key, ex)
        self._subs: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def watch(self, symbol: str) -> None:
        """Subscribe to the symbol's ticker (idempotent)."""
        if symbol in self._subs:
            return
        async with self._lock:
            if symbol not in self._subs:
                self._subs[symbol] = await self.exchange.stream.ticker(symbol).open()

    def ticker(self, symbol: str):
        """Latest fresh ticker or None (no subscription / reconnecting / data stale)."""
        if symbol not in self._subs:
            return None
        return self.exchange.stream.last_ticker(symbol)

    def price(self, symbol: str) -> Optional[float]:
        t = self.ticker(symbol)
        return getattr(t, 'last', None) if t is not None else None

    async def _close(self) -> None:
        for sub in self._subs.values():
            try:
                await sub.aclose()
            except Exception:  # noqa: BLE001
                pass
        self._subs.clear()
        await super()._close()


# ─── per-process registry ─────────────────────────────────────────────────

_HUBS: dict[tuple, _Hub] = {}
_LOCKS: dict[tuple, asyncio.Lock] = {}


def _factory() -> Callable[..., Any]:
    from bytx import create_exchange
    return create_exchange


async def _acquire(key: tuple, cls: type, make: Callable[[], Any]) -> _Hub:
    lock = _LOCKS.setdefault(key, asyncio.Lock())
    async with lock:
        hub = _HUBS.get(key)
        if hub is None or hub._closing:
            hub = cls(key, make())
            if isinstance(hub, AccountHub):
                try:
                    await hub._start()
                except BaseException:
                    await hub._close()
                    raise
            _HUBS[key] = hub
        hub.refs += 1
        return hub


async def account(name: str, *, demo: bool, market: str = 'swap', **creds: Any) -> AccountHub:
    """Account hub (created on the first consumer). Return it with :func:`release`."""
    key = ('account', name.lower(), bool(demo), market, account_fingerprint(name, demo, creds))
    return await _acquire(key, AccountHub, lambda: _factory()(name, market=market, demo=demo, **creds))


async def market(name: str, *, demo: bool, market_type: str = 'swap') -> MarketHub:
    """Public exchange data hub (no keys). Return it with :func:`release`."""
    key = ('market', name.lower(), bool(demo), market_type)
    return await _acquire(key, MarketHub, lambda: _factory()(name, market=market_type, demo=demo))


async def release(hub: _Hub) -> None:
    """A consumer left; the last one closes the hub connection."""
    lock = _LOCKS.setdefault(hub.key, asyncio.Lock())
    async with lock:
        hub.refs -= 1
        if hub.refs > 0:
            return
        if _HUBS.get(hub.key) is hub:
            del _HUBS[hub.key]
    await hub._close()


async def close_all() -> None:
    """Close all process hubs (service shutdown), regardless of reference counts."""
    hubs = list(_HUBS.values())
    _HUBS.clear()
    for hub in hubs:
        hub.refs = 0
        await hub._close()


def stats() -> list[dict]:
    """Process hubs: kind, exchange, demo, consumers, freshness (for metrics)."""
    rows = []
    for key, hub in _HUBS.items():
        row = {'kind': key[0], 'exchange': key[1], 'demo': key[2], 'refs': hub.refs}
        if isinstance(hub, AccountHub):
            row.update(orders_live=hub.orders_live, positions_live=hub.positions_live,
                       balance=hub.balance() is not None)
        else:
            row.update(symbols=sorted(hub._subs))
        rows.append(row)
    return rows


def _reset_for_tests() -> None:
    _HUBS.clear()
    _LOCKS.clear()


__all__ = ['AccountHub', 'MarketHub', 'account', 'account_fingerprint', 'close_all', 'market', 'release',
           'stats']
