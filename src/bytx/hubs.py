"""Process-wide WS connections shared by many bots.

- :func:`account` — one :class:`AccountHub` per (exchange, demo, account): one private
    connection, order and position trackers for ALL account symbols, and a balance stream. Bots on the same
    key share it; filtering by symbol is up to the consumer. Own fills — :meth:`AccountHub.fills` (one
  ``watch_my_trades`` per hub, a queue per consumer).
- :func:`market` — one :class:`MarketHub` per (exchange, demo): public tickers for the symbols of all bots;
  :meth:`MarketHub.watch` / :meth:`MarketHub.unwatch` — a consumer count per symbol (the last one
  unsubscribes). Binance — tickers packed into connections (:data:`BINANCE_TICKERS_PER_CONNECTION`).

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
from typing import TYPE_CHECKING, Any, Callable, Optional

from loguru import logger

from bytx.errors import ConfigError
from bytx.types import Balance, Order, Position

if TYPE_CHECKING:
    from bytx.streams import Subscription

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
        self._balance_epoch = 0          # balance feed reconnects at the time the value was received
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
                self._balance_epoch = self._balance_feed_epoch()
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

    def _balance_feed_epoch(self) -> int:
        feed = getattr(self._balance_sub, 'feed', None)
        return int(getattr(getattr(feed, 'stats', None), 'reconnects', 0) or 0)

    def balance(self) -> Optional[Balance]:
        """Balance from the stream or None — the stream is not current: disconnected / reconnecting / stale / stopped,
        or the value was received before the last reconnect (coins may have changed during the gap)."""
        if self._balance is None:
            return None
        feed = getattr(self._balance_sub, 'feed', None)
        state = getattr(getattr(feed, 'stats', None), 'state', None)
        if state in ('disconnected', 'reconnecting', 'stale', 'stopped', 'idle'):
            return None
        if self._balance_epoch != self._balance_feed_epoch():
            return None
        return self._balance

    def fills(self, symbol: Optional[str] = None, *, maxlen: Optional[int] = None) -> 'Subscription':
        """Own account fills (:class:`~bytx.types.Fill`) — a subscription of one consumer.

        One ccxt ``watch_my_trades`` subscription per hub: the hub's :class:`~bytx.streams.Streams` shares it
        between consumers, each with its own queue filtered by ``symbol`` (None — all symbols). Same semantics as
        ``ex.stream.fills``: ``await sub.ready()`` (the subscription is active on the exchange), ``async for``,
        ``sub.pending``, ``await sub.aclose()`` — close your queue (the last consumer stops the stream).
        ``fill.id`` is the exchange trade id, the same as in REST ``my_trades``: deduplicate stream and REST by it.

        The consumer closes the subscription before :func:`release`; closing the hub (the last :func:`release`,
        :func:`close_all`) ends the iteration of all queues.
        """
        if self._closing:
            raise ConfigError('account hub is closed (release)', exchange=self.key[1])
        return self.exchange.stream.fills(symbol, maxlen=maxlen)

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


#: Binance: tickers per WS connection of the market hub. ccxt spreads subscriptions round-robin over
#: ``streamLimits`` (50) connections — 50 symbols = 50 WS; the hub fills connections one by one
#: (:func:`bytx.exchanges.binance.ccxt_fixes.pack_streams`). The ccxt cap is 200 per connection.
BINANCE_TICKERS_PER_CONNECTION = 50
#: how long to wait for the exchange to confirm a topic unsubscribe (in the background, not in unwatch), s
UNWATCH_TIMEOUT = 5.0


class MarketHub(_Hub):
    """Public tickers: one subscription per symbol per process, with a consumer count per symbol.

    :meth:`watch` — +1 consumer of the symbol (the first opens the subscription), :meth:`unwatch` — −1 (the last
    closes the subscription and unsubscribes the topic on the exchange where ccxt can: Binance, Bybit, OKX;
    BingX and Hyperliquid — the subscription stays on the connection until the hub closes). ``watch`` without
    ``unwatch`` — as before: releasing the hub (:func:`release`) closes the subscriptions.
    """

    def __init__(self, key: tuple, ex: Any) -> None:
        super().__init__(key, ex)
        self._subs: dict[str, Any] = {}
        self._refs: dict[str, int] = {}
        self._unwatching: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def watch(self, symbol: str) -> None:
        """Subscribe to the symbol's ticker: +1 consumer (paired with :meth:`unwatch`)."""
        async with self._lock:
            if self._closing:
                raise ConfigError('market hub is closed (release)', exchange=self.key[1])
            if symbol not in self._subs:
                pending = self._unwatching.get(symbol)
                if pending is not None:  # the same symbol is still being unsubscribed on the exchange — wait
                    await asyncio.wait({pending})
                self._subs[symbol] = await self.exchange.stream.ticker(symbol).open()
            self._refs[symbol] = self._refs.get(symbol, 0) + 1

    async def unwatch(self, symbol: str) -> None:
        """A consumer of the symbol left (−1). The last one closes the ticker subscription; the topic unsubscribe
        on the exchange and closing a connection without subscriptions run in the background. An extra
        ``unwatch`` does nothing."""
        async with self._lock:
            refs = self._refs.get(symbol, 0)
            if refs > 1:
                self._refs[symbol] = refs - 1
                return
            self._refs.pop(symbol, None)
            sub = self._subs.pop(symbol, None)
            if sub is None:
                return
            feed = sub.feed
            await sub.aclose()
            if feed is None or feed.mailboxes or self._closing:
                return  # someone else on this Streams still reads the symbol's stream
            task = asyncio.ensure_future(self._unsubscribe(symbol, feed))
            self._unwatching[symbol] = task

    async def _unsubscribe(self, symbol: str, feed: Any) -> None:
        """Topic unsubscribe ``un_watch_*`` (if its connection is alive), freeing the place in the Binance
        packing, closing connections without subscriptions. Best effort: on failure the subscription stays on
        the connection (data flows without a consumer) until the hub closes."""
        from bytx.streams.supervisor import _client_connected, matching_clients
        ccxt_ex = getattr(self.exchange, 'ccxt', None)
        try:
            live = [c for _, c in matching_clients(ccxt_ex, feed.spec.tokens) if _client_connected(c)]
            if feed.spec.unwatch is not None and live:
                await asyncio.wait_for(feed.spec.unwatch(), UNWATCH_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.debug(f'hub {self.key[1]}: unwatch {symbol} failed: {type(e).__name__}: {e}')
        finally:
            if self._unwatching.get(symbol) is asyncio.current_task():
                del self._unwatching[symbol]
        _release_ticker_slot(ccxt_ex, feed.spec.symbol or symbol)
        _close_idle_connections(ccxt_ex, self.key[1])

    def ticker(self, symbol: str):
        """Latest fresh ticker or None (no subscription / reconnecting / data stale)."""
        if symbol not in self._subs:
            return None
        return self.exchange.stream.last_ticker(symbol)

    def price(self, symbol: str) -> Optional[float]:
        t = self.ticker(symbol)
        return getattr(t, 'last', None) if t is not None else None

    def watchers(self, symbol: Optional[str] = None) -> Any:
        """Consumers of the symbol (0 — not subscribed); without a symbol — {symbol: consumers}."""
        return self._refs.get(symbol, 0) if symbol is not None else dict(self._refs)

    async def _close(self) -> None:
        self._closing = True
        for task in list(self._unwatching.values()):
            task.cancel()
        self._unwatching.clear()
        for sub in self._subs.values():
            try:
                await sub.aclose()
            except Exception:  # noqa: BLE001
                pass
        self._subs.clear()
        self._refs.clear()
        await super()._close()


def _pack_ticker_streams(ex: Any) -> None:
    """Binance market hub: tickers packed into connections (other exchanges keep all market tickers on one
    connection — unchanged)."""
    ccxt_ex = getattr(ex, 'ccxt', None)
    if getattr(ccxt_ex, 'id', None) != 'binance':
        return
    from bytx.exchanges.binance.ccxt_fixes import pack_streams
    pack_streams(ccxt_ex, BINANCE_TICKERS_PER_CONNECTION)


def _release_ticker_slot(ccxt_ex: Any, symbol: str) -> None:
    if getattr(ccxt_ex, 'id', None) == 'binance':
        from bytx.exchanges.binance.ccxt_fixes import release_ticker_stream
        release_ticker_stream(ccxt_ex, symbol)


def _close_idle_connections(ccxt_ex: Any, name: str) -> None:
    """Closes market hub WS connections without subscriptions and pending waits (after the last symbol left)."""
    import ccxt as ccxt_errors
    clients = getattr(ccxt_ex, 'clients', None) or {}
    for url, client in list(clients.items()):
        subs = getattr(client, 'subscriptions', None)
        futures = getattr(client, 'futures', None) or {}
        if subs is None or subs or any(not f.done() for f in list(futures.values())):
            continue
        try:  # the ccxt path: the client leaves exchange.clients, the socket is closed
            client.on_error(ccxt_errors.NetworkError(f'bytx: hub {name}: connection without subscriptions closed'))
        except Exception as e:  # noqa: BLE001
            logger.debug(f'hub {name}: close idle {url}: {e!r}')


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

    def make() -> Any:
        ex = _factory()(name, market=market_type, demo=demo)
        _pack_ticker_streams(ex)
        return ex
    return await _acquire(key, MarketHub, make)


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


def _ws_connections(hub: _Hub) -> int:
    """Open WS connections of the hub's ccxt instance."""
    from bytx.streams.supervisor import _client_connected
    clients = getattr(getattr(hub.exchange, 'ccxt', None), 'clients', None) or {}
    return sum(1 for c in list(clients.values()) if _client_connected(c))


def _fills_subscribers(hub: AccountHub) -> int:
    stream_stats = getattr(hub.exchange.stream, 'stats', None)
    if stream_stats is None:
        return 0
    return int(stream_stats()['streams'].get('fills', {}).get('subscribers', 0))


def stats() -> list[dict]:
    """Process hubs: kind, exchange, demo, consumers, open WS connections, freshness (for metrics)."""
    rows = []
    for key, hub in _HUBS.items():
        row = {'kind': key[0], 'exchange': key[1], 'demo': key[2], 'refs': hub.refs,
               'connections': _ws_connections(hub)}
        if isinstance(hub, AccountHub):
            row.update(orders_live=hub.orders_live, positions_live=hub.positions_live,
                       balance=hub.balance() is not None, fills=_fills_subscribers(hub))
        else:
            row.update(symbols=sorted(hub._subs), watchers=hub.watchers())
        rows.append(row)
    return rows


def _reset_for_tests() -> None:
    _HUBS.clear()
    _LOCKS.clear()


__all__ = ['AccountHub', 'MarketHub', 'account', 'account_fingerprint', 'close_all', 'market', 'release',
           'stats']
