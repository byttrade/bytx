"""WebSocket streams of :class:`bytx.Exchange` (``ex.stream``) with a reconnect supervisor.

Iterator::

    async for book in ex.stream.order_book('BTC/USDT', depth=20):
        print(book.best_bid, book.best_ask, book.nonce)

Callback::

    handle = ex.stream.subscribe('trades', 'BTC/USDT', on_trade)
    ...
    handle.stop()

State and metrics::

    ex.stream.on_state(lambda ev: print(ev.state, ev.stream, ev.attempt, ev.delay))
    ex.stream.stats()            # counters, "exchange → us" latency p50/p99, WS order latency
    ex.stream.last_book('BTC/USDT')     # latest order book without waiting (None — none yet)

Several consumers of one stream share one ccxt subscription. Order books, tickers and
balance keep only the latest value (a slow consumer skips intermediate ones);
trades, candles, orders, fills and positions are queued (on overflow the oldest
are dropped, counter ``dropped``). Auth errors stop the stream and
are raised to the consumer; network errors trigger a reconnect with backoff.
"""
from __future__ import annotations

import asyncio
import inspect
import time
from collections import OrderedDict, deque
from typing import Any, Callable, Optional

from loguru import logger

from bytx.errors import ConfigError
from bytx.streams import orderbook as _orderbook
from bytx.streams import private as _private
from bytx.streams import public as _public
from bytx.streams.orderbook import TopOfBookCache
from bytx.streams.supervisor import (
    DEFAULT_STALE_AFTER,
    Feed,
    FeedSpec,
    FeedStats,
    Mailbox,
    StateEvent,
    StreamConfig,
    install_activity_hook,
    percentile,
)
from bytx.types import Order, OrderBook, Ticker

KINDS = ('order_book', 'ticker', 'best_bid_ask', 'trades', 'candles',
         'orders', 'fills', 'positions', 'balance')
_PRIVATE = frozenset(_private.BUILDERS)
_TICKER_KINDS = frozenset({'ticker', 'best_bid_ask'})
_FINAL_STATUSES = frozenset({'closed', 'canceled', 'expired', 'rejected'})
#: how many final orders of the ``orders`` stream to remember by id (completing a market order without REST)
FINAL_ORDERS_KEEP = 500


# Default WS order book depth (levels per side). Converting the whole local ccxt book
# (up to 1000 levels on binance) on every message costs ~175 µs vs ~8 µs for 20 levels.
DEFAULT_BOOK_DEPTH = 50
_UNSET: Any = object()


class Subscription:
    """Async iterator of one stream consumer.

    The subscription opens on the first ``__anext__`` (or ``await sub.open()``); close it with
    ``sub.close()`` / ``async with`` / leaving ``async for`` and dropping the reference.
    """

    def __init__(self, hub: 'Streams', kind: str, symbol: Optional[str], *, depth: Any = _UNSET,
                 maxlen: Optional[int] = None, **opts: Any) -> None:
        if kind not in KINDS:
            raise ConfigError(f'stream kind {kind!r}; available: {", ".join(KINDS)}')
        if kind not in _PRIVATE and not symbol:
            raise ConfigError(f'stream {kind} requires symbol')
        if depth is _UNSET:  # order book defaults to DEFAULT_BOOK_DEPTH levels, full book — depth=None
            depth = DEFAULT_BOOK_DEPTH if kind == 'order_book' else None
        if depth is not None and depth <= 0:
            raise ConfigError('depth must be > 0')
        self.hub = hub
        self.kind = kind
        self.requested_symbol = symbol
        self.symbol: Optional[str] = None
        self.depth = depth
        self._maxlen = maxlen
        self._opts = opts
        self._feed: Optional[Feed] = None
        self._mailbox: Optional[Mailbox] = None
        self._closed = False

    @property
    def feed(self) -> Optional[Feed]:
        return self._feed

    @property
    def pending(self) -> int:
        """How many values are waiting for the consumer."""
        return self._mailbox.pending() if self._mailbox is not None else 0

    async def open(self) -> 'Subscription':
        if self._closed:
            raise ConfigError('subscription is closed')
        if self._mailbox is None:
            await self.hub._attach(self)
        return self

    async def ready(self, timeout: Optional[float] = None) -> bool:
        """Waits until the subscription is active on the exchange (opens it if not open).

        :meth:`open` returns immediately, and the exchange sends topic events only after
        subscribing (Bybit/Bitget — after the server's subscribe response, others — after
        the connection opens). An order placed earlier may produce no WS event.
        True — subscription confirmed or data arrived; False — not within ``timeout``
        (default ``StreamConfig.ready_timeout``) or the stream stopped.
        """
        await self.open()
        if self._feed is None:
            return False
        wait = self.hub.config.ready_timeout if timeout is None else timeout
        return await self._feed.wait_ready(wait)

    def __aiter__(self) -> 'Subscription':
        return self

    async def __anext__(self) -> Any:
        if self._mailbox is None:
            if self._closed:
                raise StopAsyncIteration
            await self.open()
            if self._mailbox is None:   # closed while opening
                raise StopAsyncIteration
        return await self._mailbox.get()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._feed is not None and self._mailbox is not None:
            self.hub._detach(self)
        elif self._mailbox is not None:
            self._mailbox.close()

    async def aclose(self) -> None:
        self.close()

    async def __aenter__(self) -> 'Subscription':
        return await self.open()

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:  # consumer left async for and abandoned the iterator
        try:
            if not self._closed:
                self.close()
        except Exception:
            pass

    def __repr__(self) -> str:
        return f'<Subscription {self.kind} {self.symbol or self.requested_symbol or "*"}>'


class Handle:
    """Callback-style subscription (:meth:`Streams.subscribe`)."""

    def __init__(self, hub: 'Streams', sub: Subscription, callback: Callable[[Any], Any],
                 on_error: Optional[Callable[[BaseException], Any]] = None) -> None:
        self.hub = hub
        self.subscription = sub
        self.error: Optional[BaseException] = None
        self._callback = callback
        self._on_error = on_error
        self._task = asyncio.get_running_loop().create_task(self._run(), name=f'bytx:cb:{sub.kind}')

    async def _run(self) -> None:
        try:
            async for item in self.subscription:
                try:
                    result = self._callback(item)
                    if inspect.isawaitable(result):
                        await result
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(f'{self.hub.name} {self.subscription!r}: error in callback')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.error = e
            if self._on_error is not None:
                try:
                    result = self._on_error(e)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception('error in on_error')
            else:
                logger.warning(f'{self.hub.name} {self.subscription!r}: stream stopped: {e}')
        finally:
            self.subscription.close()
            self.hub._handles.discard(self)

    @property
    def active(self) -> bool:
        return not self._task.done()

    def add_done_callback(self, callback: Callable[['Handle'], Any]) -> None:
        """Callback on subscription end (stop(), stream stop, ``Exchange.close``)."""
        self._task.add_done_callback(lambda _task: callback(self))

    def stop(self) -> None:
        self.subscription.close()
        if not self._task.done():
            self._task.cancel()

    async def wait(self) -> None:
        """Waits for completion (stop() or stream stop)."""
        try:
            await self._task
        except asyncio.CancelledError:
            pass


class LatencyStats:
    """Request latency (WS orders): count, errors, p50/p99, ms."""

    __slots__ = ('count', 'errors', 'max_ms', '_window')

    def __init__(self, window: int = 1000) -> None:
        self.count = 0
        self.errors = 0
        self.max_ms: Optional[float] = None
        self._window: deque = deque(maxlen=window)

    def record(self, ms: float, ok: bool) -> None:
        self.count += 1
        if not ok:
            self.errors += 1
        self._window.append(ms)
        if self.max_ms is None or ms > self.max_ms:
            self.max_ms = ms

    def as_dict(self) -> dict:
        lat = sorted(self._window)
        return {'count': self.count, 'errors': self.errors,
                'avg': sum(lat) / len(lat) if lat else None,
                'p50': percentile(lat, 50), 'p99': percentile(lat, 99), 'max': self.max_ms}


class Streams:
    """WS streams of one :class:`Exchange` (``ex.stream``)."""

    def __init__(self, exchange: Any, config: Optional[StreamConfig] = None) -> None:
        self.exchange = exchange
        self.config = config or StreamConfig()
        self._feeds: dict[tuple, Feed] = {}
        self._listeners: list[Callable[[StateEvent], Any]] = []
        self._handles: set[Handle] = set()
        self._cache = TopOfBookCache()
        self._orders: dict[str, LatencyStats] = {}
        self._final_orders: OrderedDict[str, Order] = OrderedDict()
        self._order_waiters: dict[str, asyncio.Future] = {}
        self._background: set[asyncio.Task] = set()
        self._closed = False
        # stream recording (bytx.paper.Recorder) — tap(spec, items) on every message
        self._tap: Optional[Callable[[FeedSpec, list], None]] = None
        try:  # time of the ccxt client's last message — for the silence watchdog
            install_activity_hook(exchange.ccxt)
        except Exception as e:
            logger.debug(f'{getattr(exchange, "name", "?")}: WS activity marker not installed: {e!r}')

    # ─── properties for Feed ──────────────────────────────────────────────────

    @property
    def ccxt(self) -> Any:
        return self.exchange.ccxt

    @property
    def name(self) -> str:
        return self.exchange.name

    def exchange_now_ms(self) -> float:
        """Local time adjusted to the exchange clock: the clock offset by the time request midpoint
        (``bytxClockOffset``, :class:`~bytx.net.timesync.TimeSync`); without it — ccxt timeDifference
        (local after the response − server: overstated by the response path, the WS latency by it is
        understated by ~half the RTT)."""
        options = getattr(self.ccxt, 'options', None)
        diff = None
        if isinstance(options, dict):
            diff = options.get('bytxClockOffset')
            if diff is None:
                diff = options.get('timeDifference')
        return time.time() * 1000 - (diff or 0)

    # ─── public streams ───────────────────────────────────────────────────

    def order_book(self, symbol: str, depth: Optional[int] = DEFAULT_BOOK_DEPTH, *, limit: Optional[int] = None,
                   params: Optional[dict] = None) -> Subscription:
        """Order book. ``depth`` — how many levels per side to deliver (sliced locally before conversion
        to SDK objects); default :data:`DEFAULT_BOOK_DEPTH` (50), the whole local book —
        only with an explicit ``depth=None`` (expensive: the whole book is converted on every message,
        for all subscribers of the symbol). ``limit`` — exchange subscription depth (ccxt
        ``watch_order_book(limit)``, values differ per exchange). The same default applies to
        ``subscribe('order_book', …)`` without ``depth``."""
        return Subscription(self, 'order_book', symbol, depth=depth, limit=limit, params=params)

    def ticker(self, symbol: str, *, params: Optional[dict] = None) -> Subscription:
        return Subscription(self, 'ticker', symbol, params=params)

    def best_bid_ask(self, symbol: str, *, params: Optional[dict] = None) -> Subscription:
        """Best bid/ask (:class:`Ticker` with bid/ask/bid_size/ask_size): bookTicker/bbo or the ticker."""
        return Subscription(self, 'best_bid_ask', symbol, params=params)

    def trades(self, symbol: str, *, maxlen: Optional[int] = None, params: Optional[dict] = None) -> Subscription:
        return Subscription(self, 'trades', symbol, maxlen=maxlen, params=params)

    def candles(self, symbol: str, timeframe: str = '1m', *, maxlen: Optional[int] = None,
                params: Optional[dict] = None) -> Subscription:
        return Subscription(self, 'candles', symbol, maxlen=maxlen, timeframe=timeframe, params=params)

    # ─── private streams ───────────────────────────────────────────────────

    def orders(self, symbol: Optional[str] = None, *, maxlen: Optional[int] = None) -> Subscription:
        """Own order updates (:class:`Order`), all symbols or one."""
        return Subscription(self, 'orders', symbol, maxlen=maxlen)

    def fills(self, symbol: Optional[str] = None, *, maxlen: Optional[int] = None) -> Subscription:
        """Own fills (:class:`Fill`), ccxt ``watch_my_trades``."""
        return Subscription(self, 'fills', symbol, maxlen=maxlen)

    def positions(self, symbol: Optional[str] = None, *, maxlen: Optional[int] = None) -> Subscription:
        """Positions (:class:`Position`); a closed position arrives with ``size == 0``."""
        return Subscription(self, 'positions', symbol, maxlen=maxlen)

    def balance(self) -> Subscription:
        return Subscription(self, 'balance', None)

    # ─── callbacks, state, cache ────────────────────────────────────────────

    def subscribe(self, kind: str, symbol: Optional[str] = None,
                  callback: Optional[Callable[[Any], Any]] = None, *,
                  on_error: Optional[Callable[[BaseException], Any]] = None, **opts: Any) -> Handle:
        """Callback subscription (sync or async): ``subscribe('order_book', 'BTC/USDT', cb, depth=5)``.

        Callback errors are logged and do not stop the stream. A stream stop (e.g.
        an auth error) → ``on_error(exc)`` and ``handle.error``.
        """
        if callback is None:
            raise ConfigError('callback is required')
        sub = Subscription(self, kind, symbol, **opts)
        handle = Handle(self, sub, callback, on_error)
        self._handles.add(handle)
        return handle

    def on_state(self, callback: Callable[[StateEvent], Any]) -> Callable[[], None]:
        """Listener for state events (connected, disconnected, reconnecting, resynced,
        stale, crossed, nonce, stopped). Returns an unsubscribe function."""
        self._listeners.append(callback)

        def remove() -> None:
            if callback in self._listeners:
                self._listeners.remove(callback)

        return remove

    def last_book(self, symbol: str) -> Optional[OrderBook]:
        """Latest order book from WS without waiting.

        None — no stream, no data yet, the stream is stopped or reconnecting,
        or the book is older than the silence watchdog threshold (``StreamConfig.stale_after``).
        """
        s = self._resolve(symbol)
        return self._cache.books.get(s) if self._fresh(('order_book',), s) else None

    def last_ticker(self, symbol: str) -> Optional[Ticker]:
        """Latest ticker / best_bid_ask from WS without waiting (None — as in :meth:`last_book`)."""
        s = self._resolve(symbol)
        return self._cache.tickers.get(s) if self._fresh(tuple(_TICKER_KINDS), s) else None

    async def final_order(self, order_id: str, timeout: float) -> Optional[Order]:
        """An order in a final status (filled, canceled, expired, rejected) from the ``orders`` stream without REST.

        Already arrived — at once (the WS event may come before the placement response); the ``orders`` stream is
        subscribed and not reconnecting — wait up to ``timeout`` s (a private stream is 'idle' until its first event
        but ``ready``: the subscription is confirmed); no stream or nothing arrived — None (the caller reads REST)."""
        hit = self._final_orders.get(order_id)
        if hit is not None or not any(f.spec.kind == 'orders' and f.running and f.ready
                                      and f.stats.state in ('idle', 'connected') for f in self._feeds.values()):
            return hit
        fut = self._order_waiters.get(order_id)
        if fut is None:
            fut = self._order_waiters[order_id] = asyncio.get_running_loop().create_future()
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            if not fut.done() and self._order_waiters.get(order_id) is fut:
                del self._order_waiters[order_id]
                fut.cancel()

    def _order_seen(self, order: Order) -> None:
        if not order.id or order.status not in _FINAL_STATUSES:
            return
        self._final_orders[order.id] = order
        self._final_orders.move_to_end(order.id)
        while len(self._final_orders) > FINAL_ORDERS_KEEP:
            self._final_orders.popitem(last=False)
        fut = self._order_waiters.pop(order.id, None)
        if fut is not None and not fut.done():
            fut.set_result(order)

    def _fresh(self, kinds: tuple[str, ...], symbol: str) -> bool:
        """There is a live ``kinds`` stream for the symbol with data no older than the silence threshold."""
        now = time.time()
        for feed in self._feeds.values():
            if feed.spec.kind not in kinds or feed.spec.symbol != symbol:
                continue
            st = feed.stats
            if not feed.running or st.state != 'connected' or st.last_message_at is None:
                continue
            limit = self.config.stale_for(feed.spec.kind)
            if limit is None or now - st.last_message_at <= limit:
                return True
        return False

    def stats(self) -> dict:
        streams = {feed.name: feed.stats.as_dict() for feed in self._feeds.values()}
        totals = {k: sum(s[k] for s in streams.values())
                  for k in ('messages', 'reconnects', 'errors', 'stale', 'crossed', 'resyncs',
                            'dropped', 'conflated', 'subscribers')}
        return {'streams': streams, 'totals': totals,
                'orders': {k: v.as_dict() for k, v in self._orders.items()}}

    async def close(self) -> None:
        """Stops all streams (called from ``Exchange.close``)."""
        self._closed = True
        for handle in list(self._handles):
            handle.stop()
        feeds = list(self._feeds.values())
        for feed in feeds:
            for mb in list(feed.mailboxes):
                mb.close()
            feed.mailboxes.clear()
            feed.stop()
        await asyncio.gather(*(f.wait_stopped() for f in feeds), return_exceptions=True)
        for task in list(self._background):
            task.cancel()
        self._feeds.clear()
        self._cache.clear()

    @property
    def closed(self) -> bool:
        return self._closed

    # ─── internal ─────────────────────────────────────────────────────────

    def _resolve(self, symbol: str) -> str:
        try:
            return self.exchange.symbol(symbol)
        except Exception:
            return symbol

    def _spec(self, kind: str, symbol: Optional[str], market: Any, opts: dict) -> FeedSpec:
        if kind in _PRIVATE:
            if kind == 'positions':
                return _private.positions(self, symbol)
            return _private.BUILDERS[kind](self)
        assert symbol is not None  # public streams require a symbol (Subscription)
        if kind == 'order_book':
            return _public.order_book(self, symbol, market, limit=opts.get('limit'), depth=opts.get('depth'),
                                     params=opts.get('params'))
        if kind == 'candles':
            return _public.candles(self, symbol, market, timeframe=opts.get('timeframe') or '1m',
                                   params=opts.get('params'))
        return _public.BUILDERS[kind](self, symbol, market, params=opts.get('params'))

    async def _attach(self, sub: Subscription) -> None:
        if self._closed:
            raise ConfigError('streams are closed (exchange.close())', exchange=self.name)
        await self.exchange.connect()
        if self._closed:   # close() completed while connect was running: don't open a stream after close
            raise ConfigError('streams are closed (exchange.close())', exchange=self.name)
        symbol = self.exchange.symbol(sub.requested_symbol) if sub.requested_symbol else None
        market = self.exchange._market_or_none(symbol) if symbol else None
        spec = self._spec(sub.kind, symbol, market, {**sub._opts, 'depth': sub.depth})
        if sub._closed:
            return
        feed = self._feeds.get(spec.key)
        if feed is None or feed.error is not None:
            feed = Feed(self, spec)
            self._feeds[spec.key] = feed
        accept = None
        if symbol and spec.symbol is None:     # private stream on all symbols → filter
            accept = (lambda item, s=symbol: getattr(item, 'symbol', None) == s)
        transform = None
        if sub.kind == 'order_book' and sub.depth is not None:
            transform = (lambda book, d=sub.depth: _orderbook.trim(book, d))
        mailbox = Mailbox(spec.policy, feed.stats, maxlen=sub._maxlen or self.config.queue_size,
                          accept=accept, transform=transform)
        sub.symbol = symbol
        sub._feed = feed
        sub._mailbox = mailbox
        if sub.kind == 'order_book':
            feed.depths[id(mailbox)] = sub.depth
            self._update_depth(feed)
        feed.attach(mailbox)

    def _detach(self, sub: Subscription) -> None:
        feed, mailbox = sub._feed, sub._mailbox
        if feed is None or mailbox is None:
            return
        feed.depths.pop(id(mailbox), None)
        self._update_depth(feed)
        feed.detach(mailbox)

    @staticmethod
    def _update_depth(feed: Feed) -> None:
        values = list(feed.depths.values())
        feed.depth = None if not values or None in values else max(values)  # type: ignore[type-var]

    def _feed_stopped(self, feed: Feed) -> None:
        # a stream stopped by an error stays in stats() (state='stopped') until a new subscription
        if self._feeds.get(feed.spec.key) is feed and feed.error is None and not feed.mailboxes:
            del self._feeds[feed.spec.key]
        self._drop_cache(feed)

    def _drop_cache(self, feed: Feed) -> None:
        """Removes the stopped stream's symbol from the cache if no other live streams use it."""
        kind, symbol = feed.spec.kind, feed.spec.symbol
        if symbol is None:
            return
        kinds = ('order_book',) if kind == 'order_book' else tuple(_TICKER_KINDS) if kind in _TICKER_KINDS else ()
        if not kinds:
            return
        if any(f is not feed and f.running and f.spec.kind in kinds and f.spec.symbol == symbol
               for f in self._feeds.values()):
            return
        (self._cache.books if kind == 'order_book' else self._cache.tickers).pop(symbol, None)

    def _on_items(self, feed: Feed, items: list) -> None:
        if self._tap is not None:
            self._tap(feed.spec, items)
        kind = feed.spec.kind
        if kind == 'order_book':
            self._cache.put_book(items[-1])
        elif kind in _TICKER_KINDS:
            self._cache.put_ticker(items[-1])
        elif kind == 'orders':
            for order in items:
                self._order_seen(order)

    def _emit(self, state: str, feed: Feed, **kwargs: Any) -> None:
        if not self._listeners:
            return
        event = StateEvent(state, feed.name, feed.spec.kind, feed.spec.symbol, **kwargs)  # type: ignore[arg-type]
        for cb in list(self._listeners):
            try:
                result = cb(event)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    self._background.add(task)
                    task.add_done_callback(self._background.discard)
            except Exception:
                logger.exception('error in on_state listener')

    def _record_order(self, op: str, route: str, seconds: float, ok: bool) -> None:
        key = f'{op}:{route}'
        stats = self._orders.get(key)
        if stats is None:
            stats = self._orders[key] = LatencyStats(self.config.latency_window)
        stats.record(seconds * 1000, ok)

    def __repr__(self) -> str:
        return f'<Streams {self.name} feeds={len(self._feeds)}>'


__all__ = [
    'Streams', 'Subscription', 'Handle', 'StreamConfig', 'StateEvent', 'FeedStats',
    'LatencyStats', 'DEFAULT_BOOK_DEPTH', 'DEFAULT_STALE_AFTER', 'KINDS',
]
