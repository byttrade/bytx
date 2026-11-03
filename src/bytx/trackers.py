"""Local order and position state on top of private WS.

``OrderTracker`` and ``PositionTracker`` keep an up-to-date picture without REST polling:
a REST snapshot at start, then ``ex.stream.orders`` / ``ex.stream.positions`` updates.
After a WS disconnect (disconnected/reconnecting/stale → connected), REST reconciliation runs again:
events may have been lost while the connection was down.

::

    async with ex.order_tracker('BTC/USDT') as orders, ex.position_tracker() as positions:
        o = await ex.buy('BTC/USDT', size=0.01, price=60_000, post_only=True)
        done = await orders.wait(o.id, timeout=30)       # closed / canceled / …
        print(done.status, positions.get('BTC/USDT'))
"""
from __future__ import annotations

import asyncio
import inspect
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from loguru import logger

from bytx import convert
from bytx.errors import BytxError, ConfigError, OrderNotFound
from bytx.streams.private import flat_side
from bytx.types import Order, Position

if TYPE_CHECKING:
    from bytx.exchange import Exchange
    from bytx.streams import Handle, StateEvent

TERMINAL = frozenset({'closed', 'canceled', 'expired', 'rejected'})
# stream states after which events may have been lost
_GAP_STATES = frozenset({'disconnected', 'reconnecting', 'stale'})


def _emit(listeners: list, item: Any) -> None:
    for cb in list(listeners):
        try:
            result = cb(item)
            if inspect.isawaitable(result):
                asyncio.ensure_future(result)
        except Exception:
            logger.exception('error in tracker callback')


class _Tracker:
    """Shared base: stream subscription, disconnect tracking, background REST reconciliation."""

    kind = ''

    def __init__(self, ex: 'Exchange', symbol: Optional[str] = None) -> None:
        self.ex = ex
        self.symbol = symbol
        self._listeners: list[Callable[[Any], Any]] = []
        self._handle: Optional['Handle'] = None
        self._remove_state: Optional[Callable[[], None]] = None
        self._gap = False
        self._resync_task: Optional[asyncio.Task] = None
        self._changed = asyncio.Event()
        self._stopped = False
        self.resyncs = 0

    # ─── lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> Any:
        if self._handle is not None:
            return self
        await self.ex.connect()
        s = self.ex.symbol(self.symbol) if self.symbol else None
        self.symbol = s
        self._stopped = False
        self._remove_state = self.ex.stream.on_state(self._on_state)
        # subscribe before the snapshot: updates between the snapshot and the subscription are not lost
        self._handle = self.ex.stream.subscribe(self.kind, s, self._apply)
        # subscription ended (stop, error, Exchange.close): wake up wait()/wait_flat()
        self._handle.add_done_callback(lambda _h: self._notify_waiters())
        # REST snapshot once the subscription is active (Bybit/Bitget: server reply to
        # subscribe); otherwise an order between snapshot and subscription would land in neither
        await self._handle.subscription.open()
        timeout = self.ex.stream.config.ready_timeout
        if timeout and not await self._handle.subscription.ready(timeout):
            if self._stopped_reason() is None:
                logger.warning(f'{self.ex.name}: {self.kind} subscription not confirmed within {timeout} s; '
                               f'taking REST snapshot before confirmation')
        await self.resync()
        registry = getattr(self.ex, '_trackers', None)
        if registry is not None:  # RiskGuard etc. take live state from here without REST
            registry.add(self)
        return self

    async def stop(self) -> None:
        self._stopped = True
        registry = getattr(self.ex, '_trackers', None)
        if registry is not None:
            registry.discard(self)
        self._notify_waiters()
        if self._remove_state is not None:
            self._remove_state()
            self._remove_state = None
        if self._handle is not None:
            self._handle.stop()
            await self._handle.wait()
            self._handle = None
        if self._resync_task is not None and not self._resync_task.done():
            self._resync_task.cancel()

    async def __aenter__(self) -> Any:
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    @property
    def error(self) -> Optional[BaseException]:
        """Error that stopped the stream (e.g. authentication); None means the stream is alive."""
        return self._handle.error if self._handle is not None else None

    @property
    def closed(self) -> bool:
        """True if the tracker is stopped (``stop()``, ``Exchange.close``, stream stop):
        no more updates will arrive."""
        if self._stopped:
            return True
        return self._handle is not None and not self._handle.active

    @property
    def live(self) -> bool:
        """State is up to date: tracker running, stream connected, REST reconciliation done."""
        resync = self._resync_task
        return (not self.closed and not self._gap and self.resyncs > 0
                and (resync is None or resync.done()))

    def _stopped_reason(self) -> Optional[str]:
        """Why waiting is pointless (None means the tracker is alive)."""
        if self.error is not None:
            return f'{self.kind} stream stopped: {self.error}'
        if self.closed:
            return f'{self.kind} tracker stopped (stop() or exchange.close())'
        return None

    def on_update(self, callback: Callable[[Any], Any]) -> Callable[[], None]:
        """Callback on every change (sync or async). Returns an unsubscribe function."""
        self._listeners.append(callback)

        def remove() -> None:
            if callback in self._listeners:
                self._listeners.remove(callback)

        return remove

    # ─── disconnects and reconciliation ─────────────────────────────────────

    def _on_state(self, ev: 'StateEvent') -> None:
        if ev.kind != self.kind:
            return
        if ev.state == 'stopped':
            # wake up wait(): the stream error becomes visible via self.error
            asyncio.get_running_loop().call_soon(self._notify_waiters)
        elif ev.state in _GAP_STATES:
            self._gap = True
        elif ev.state == 'connected' and self._gap:
            self._gap = False
            if self._resync_task is None or self._resync_task.done():
                self._resync_task = asyncio.get_running_loop().create_task(
                    self._safe_resync(), name=f'bytx:resync:{self.kind}')

    async def _safe_resync(self) -> None:
        try:
            await self.resync()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # next reconciliation happens on the next reconnect
            logger.warning(f'{self.ex.name}: {self.kind} REST reconciliation failed: {e}')
            self._gap = True

    async def resync(self) -> None:  # pragma: no cover — overridden
        raise NotImplementedError

    def _apply(self, item: Any) -> None:  # pragma: no cover — overridden
        raise NotImplementedError

    def _notify_waiters(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    def _notify(self, item: Any) -> None:
        self._notify_waiters()
        journal = getattr(self.ex, 'journal', None)
        if journal is not None:  # position journal (opt-in), written in the background in order
            self._journal(journal, item)
        intents = getattr(self.ex, 'intents', None)
        if intents is not None and self.kind == 'orders':  # intent outcome via WS
            intents._on_update(self.ex, item)
        _emit(self._listeners, item)

    def _journal(self, journal: Any, item: Any) -> None:  # pragma: no cover — overridden
        pass


class OrderTracker(_Tracker):
    """Own orders: open and recently finished (the last ``keep_closed``).

    Updates are applied monotonically: a finished order is not "revived" by
    a late WS message, and filled size never decreases.
    """

    kind = 'orders'

    def __init__(self, ex: 'Exchange', symbol: Optional[str] = None, *, keep_closed: int = 1000) -> None:
        super().__init__(ex, symbol)
        self.keep_closed = keep_closed
        self._open: dict[str, Order] = {}
        self._closed: OrderedDict[str, Order] = OrderedDict()
        self._by_cid: dict[str, str] = {}  # client_id → order id (O(1) for by_client_id / wait)

    @property
    def open_orders(self) -> list[Order]:
        return list(self._open.values())

    def get(self, order_id: str) -> Optional[Order]:
        return self._open.get(str(order_id)) or self._closed.get(str(order_id))

    def by_client_id(self, client_id: str) -> Optional[Order]:
        order_id = self._by_cid.get(client_id)
        return self.get(order_id) if order_id is not None else None

    def _find(self, order_id: Optional[str], client_id: Optional[str]) -> Optional[Order]:
        return self.get(order_id) if order_id is not None else self.by_client_id(client_id)  # type: ignore[arg-type]

    async def wait(self, order_id: Optional[str] = None, *, client_id: Optional[str] = None,
                   until: Iterable[str] = TERMINAL, timeout: Optional[float] = None) -> Order:
        """Waits until the order reaches one of the ``until`` statuses (default: finished).

        ``asyncio.TimeoutError`` on timeout; :class:`OrderNotFound` if the stream is stopped
        or the tracker is closed (``stop()``, ``Exchange.close``), including while waiting.
        """
        if (order_id is None) == (client_id is None):
            raise ConfigError('exactly one of order_id / client_id is required')
        wanted = frozenset(until)

        async def loop() -> Order:
            while True:
                o = self._find(order_id, client_id)
                if o is not None and o.status in wanted:
                    return o
                reason = self._stopped_reason()
                if reason is not None:
                    raise OrderNotFound(reason, exchange=self.ex.name) from self.error
                await self._changed.wait()

        return await asyncio.wait_for(loop(), timeout)

    def _apply(self, order: Order) -> None:
        if self.symbol and order.symbol != self.symbol:
            return
        key = order.id
        if not key:
            return
        old = self._open.get(key) or self._closed.get(key)
        if old is not None:
            if old.status in TERMINAL and order.status not in TERMINAL:
                return  # late "open" after completion
            if (order.status not in TERMINAL and old.filled is not None and order.filled is not None
                    and order.filled < old.filled):
                return  # stale message: less filled than already known
            if order.client_id is None and old.client_id is not None:
                order.client_id = old.client_id
        self._store(order)
        self._notify(order)

    def _journal(self, journal: Any, order: Order) -> None:
        journal._on_order_update(self.ex, order)

    def _store(self, order: Order) -> None:
        if order.client_id:
            self._by_cid[order.client_id] = order.id
        if order.status in TERMINAL:
            self._open.pop(order.id, None)
            self._closed[order.id] = order
            self._closed.move_to_end(order.id)
            while len(self._closed) > self.keep_closed:
                _, old = self._closed.popitem(last=False)
                if old.client_id and self._by_cid.get(old.client_id) == old.id:
                    del self._by_cid[old.client_id]
        else:
            self._closed.pop(order.id, None)
            self._open[order.id] = order

    async def resync(self) -> None:
        """REST: open orders; for those gone from the list, fetch the final status."""
        fresh = await self.ex.open_orders(self.symbol)
        fresh_ids = {o.id for o in fresh}
        for o in fresh:
            self._apply(o)
        missing = [o for o in self._open.values() if o.id not in fresh_ids]
        for o in missing:
            try:
                final = await self.ex.order(o.symbol, o.id)
            except OrderNotFound:
                logger.warning(f'{self.ex.name}: order {o.id} gone from open orders and not found')
                continue
            except BytxError as e:
                logger.warning(f'{self.ex.name}: failed to get status of order {o.id}: {e}')
                continue
            if final.status not in TERMINAL:
                continue  # exchange still considers it open: wait for WS
            self._apply(final)
        self.resyncs += 1


class PositionTracker(_Tracker):
    """Open positions by (symbol, side). A closed position is removed.

    In one-way mode a single position is kept per symbol: a long → short flip
    replaces the entry, even if the exchange did not send a close for the old side.
    """

    kind = 'positions'

    def __init__(self, ex: 'Exchange', symbol: Optional[str] = None) -> None:
        super().__init__(ex, symbol)
        self._positions: dict[tuple[str, Optional[str]], Position] = {}
        self._hedge = False
        # WS update versions: resync does not touch what WS updated during the REST request
        self._ws_seq = 0
        self._ws_ver: dict[tuple[str, Optional[str]], int] = {}

    async def start(self) -> 'PositionTracker':
        if self.ex.market_type == 'spot':
            raise ConfigError('positions are for derivatives only', exchange=self.ex.name)
        await self.ex.connect()
        self._hedge = await self.ex.is_hedge_mode(self.ex.symbol(self.symbol) if self.symbol else None)
        return await super().start()  # type: ignore[no-any-return]

    @property
    def positions(self) -> list[Position]:
        return list(self._positions.values())

    def get(self, symbol: str, side: Optional[str] = None) -> Optional[Position]:
        """Position for a symbol (in hedge mode, pass side)."""
        s = self.ex.symbol(symbol)
        for (sym, pos_side), p in self._positions.items():
            if sym == s and (side is None or pos_side == side):
                return p
        return None

    def net_size(self, symbol: str) -> float:
        """Net size in the base coin: long positive, short negative."""
        s = self.ex.symbol(symbol)
        return sum(p.size if p.side == 'long' else -p.size
                   for (sym, _), p in self._positions.items() if sym == s)

    async def wait_flat(self, symbol: str, *, timeout: Optional[float] = None) -> None:
        """Waits until no positions remain for the symbol.

        ``asyncio.TimeoutError`` on timeout; :class:`ConfigError` if the stream is stopped
        or the tracker is closed (``stop()``, ``Exchange.close``), including while waiting.
        """
        async def loop() -> None:
            while self.get(symbol) is not None:
                reason = self._stopped_reason()
                if reason is not None:
                    raise ConfigError(reason, exchange=self.ex.name) from self.error
                await self._changed.wait()

        await asyncio.wait_for(loop(), timeout)

    def _journal(self, journal: Any, pos: Position) -> None:
        journal._on_position_update(self.ex, pos)

    def _ver_key(self, symbol: str, side: Optional[str], hedge: bool) -> tuple[str, Optional[str]]:
        # one-way: one position per symbol, version shared by both sides
        return (symbol, side) if hedge else (symbol, '')

    def _row_hedge(self, pos: Position) -> bool:
        """Whether this position is hedge: from exchange fields in the row (Bybit ``positionIdx``, OKX ``posSide``,
        ``positionSide``), otherwise the tracker's mode. Bybit sets the mode per symbol: the account tracker
        (symbol None) read the mode from the BTCUSDT reference while ETHUSDT was hedge, so an empty short from a push
        wiped the long (demo 2026-10-30).
        A row can only turn hedge on, never off: Binance BOTH next to LONG/SHORT in a hedge account."""
        info = pos.raw.get('info') if isinstance(pos.raw, dict) else None
        return self._hedge or convert.hedge_side(info)[1] is True

    def _apply(self, pos: Position) -> None:
        """WS update: marks the key version and applies it."""
        if self.symbol and pos.symbol != self.symbol:
            return
        hedge = self._row_hedge(pos)
        if hedge and pos.size == 0 and pos.raw and flat_side(pos.raw) is None:
            # hedge: a zero row without side (Binance ps=BOTH next to LONG/SHORT in
            # ACCOUNT_UPDATE) is not a hedge position; treating it as long+short would close both sides
            return
        self._ws_seq += 1
        self._ws_ver[self._ver_key(pos.symbol, pos.side, hedge)] = self._ws_seq
        self._set(pos)

    def _set(self, pos: Position) -> None:
        if self.symbol and pos.symbol != self.symbol:
            return
        key = (pos.symbol, pos.side)
        hedge = self._row_hedge(pos)
        if pos.size == 0:
            # one-way: a close removes the whole symbol (the exchange may not know the closing side)
            gone = [key] if hedge else [k for k in self._positions if k[0] == pos.symbol]
            if not [self._positions.pop(k) for k in gone if k in self._positions]:
                return
        else:
            if not hedge:
                for other in [k for k in self._positions if k[0] == pos.symbol and k != key]:
                    del self._positions[other]
            self._positions[key] = pos
        self._notify(pos)

    async def resync(self) -> None:
        """REST: positions; those missing from the response are closed.

        Keys updated via WS during the request are left alone: WS is newer than the snapshot.
        """
        start = self._ws_seq
        fresh = await self.ex.positions([self.symbol] if self.symbol else None)

        def stale(symbol: str, side: Optional[str], hedge: bool) -> bool:
            return self._ws_ver.get(self._ver_key(symbol, side, hedge), 0) > start

        fresh_keys = {(p.symbol, p.side) for p in fresh}
        fresh_symbols = {p.symbol for p in fresh}
        for key in [k for k in self._positions if k not in fresh_keys]:
            # one-way: on a flip the entry replaces the snapshot, no false close
            if key not in self._positions:
                continue
            gone = self._positions[key]
            hedge = self._row_hedge(gone)
            if stale(*key, hedge) or (not hedge and key[0] in fresh_symbols):
                continue
            self._set(Position(symbol=gone.symbol, side=gone.side, size=0.0, contracts=0.0,
                               entry_price=gone.entry_price, mark_price=gone.mark_price,
                               liquidation_price=None, leverage=gone.leverage,
                               margin_mode=gone.margin_mode, unrealized_pnl=None,
                               realized_pnl=gone.realized_pnl, notional=0.0, margin=0.0,
                               timestamp=None, raw={'info': (gone.raw or {}).get('info')} if hedge else {}))
        for p in fresh:
            if not stale(p.symbol, p.side, self._row_hedge(p)):
                self._set(p)
        self.resyncs += 1


__all__ = ['OrderTracker', 'PositionTracker', 'TERMINAL']
