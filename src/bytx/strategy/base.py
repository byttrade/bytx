"""Strategy framework: lifecycle, subscriptions, timers, recovery.

::

    class Grid(Strategy):
        symbols = ('BTC/USDT:USDT',)
        feeds = ('ticker',)

        async def on_start(self, ctx):
            ctx.every(30, self.rebalance)                 # timer on the strategy clock
            for order in ctx.recovery.live if ctx.recovery else ():
                ...                                       # orders picked up after a restart

        async def on_tick(self, ctx, tick):               # tick.kind / tick.symbol / tick.data
            ...

        async def on_fill(self, ctx, fill): ...
        async def on_position(self, ctx, position): ...
        async def on_stop(self, ctx): await ctx.algos.cancel_all('stop')

        async def rebalance(self, ctx):
            with ctx.bind(position_id='grid-1', role='dca'):
                await ctx.ex.place_order('BTC/USDT:USDT', 'buy', size=0.01, price=...)

    result = await run_strategy(Grid(), ex)                    # live, demo, paper
    result = await ex.replay.run(lambda ex: run_strategy(Grid(), ex))   # replay of a recording

Same behavior on live, demo, paper and replay: the strategy sees only the public :class:`Exchange`
(``ctx.ex``), the clock (``ctx.clock``: for paper/replay — the engine clock, virtual in replay and
tests), the position journal, execution algos (``ctx.algos``), risk sizing
(``ctx.risk_size`` / ``ctx.open_position``) and the intent store.

Startup order: recovery (if the Exchange has ``intents``: :meth:`Exchange.recover`,
result — ``ctx.recovery``), order and position trackers (REST snapshot), ``symbols × feeds`` subscriptions,
``on_start``, then events. Events are delivered **one at a time** (strategy callbacks never
overlap): ticks, orders (``on_order``), fills (``on_fill``), positions
(``on_position``), timers. Book / ticker / bid-ask ticks are coalesced — the strategy gets
the latest (the queue does not grow on a slow callback), trades and candles — all in order. A timer
whose previous firing has not been processed yet is skipped (``timer.missed``).

A callback error goes to the log and ``result.errors``; ``max_errors`` — stop after that many errors
(``reason='errors'``). ``ctx.stop(reason)`` — a graceful stop; ``on_stop`` is always called
(also when the task is canceled). The runner neither closes the Exchange nor cancels orders — the strategy decides
that in ``on_stop``.
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterator, Optional, Sequence, Union

from loguru import logger

from bytx.errors import ConfigError

if TYPE_CHECKING:
    from bytx.exchange import Exchange
    from bytx.recovery import RecoveryReport
    from bytx.trackers import OrderTracker, PositionTracker
    from bytx.types import Fill, Order, Position

FEEDS = ('ticker', 'order_book', 'best_bid_ask', 'trades', 'candles')
_LATEST = frozenset({'ticker', 'order_book', 'best_bid_ask'})
TimerCallback = Callable[['StrategyContext'], Union[Awaitable[Any], Any]]


@dataclass(frozen=True, slots=True)
class Tick:
    """Subscription market event: ``kind`` from :data:`FEEDS`, ``data`` — an SDK model
    (:class:`Ticker` / :class:`OrderBook` / :class:`Trade` / :class:`Candle`), ``ts`` — the strategy
    clock time at delivery (ms)."""

    kind: str
    symbol: str
    data: Any
    ts: int


class Timer:
    """Strategy timer (:meth:`StrategyContext.every` / :meth:`StrategyContext.after`)."""

    def __init__(self, name: str, interval: float, repeat: bool, callback: Optional[TimerCallback]) -> None:
        self.name = name
        self.interval = float(interval)
        self.repeat = repeat
        self.callback = callback
        self.fired = 0       # delivered to the strategy
        self.missed = 0      # skipped: the previous firing is still queued
        self._pending = False
        self._task: Optional[asyncio.Task] = None

    @property
    def active(self) -> bool:
        return self._task is not None and not self._task.done()

    def cancel(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()

    def __repr__(self) -> str:
        return f'<Timer {self.name} {self.interval}s fired={self.fired}>'


@dataclass(slots=True)
class StrategyResult:
    """Run result: stop reason, time (ms of the strategy clock), callback errors, recovery."""

    reason: str
    started_ms: int
    stopped_ms: int
    events: int = 0
    errors: list[tuple[str, BaseException]] = field(default_factory=list)
    recovery: Optional['RecoveryReport'] = None


class Strategy:
    """Strategy base: override the ``on_*`` you need (all async, no-ops by default).

    ``symbols`` × ``feeds`` — subscriptions at start (``feeds`` from :data:`FEEDS`); others —
    :meth:`StrategyContext.subscribe`. ``timeframe`` — for ``'candles'``.
    """

    symbols: Sequence[str] = ()
    feeds: Sequence[str] = ('ticker',)
    timeframe: str = '1m'

    async def on_start(self, ctx: 'StrategyContext') -> None:
        """After recovery and subscriptions, before the first event."""

    async def on_tick(self, ctx: 'StrategyContext', tick: Tick) -> None:
        """Subscription market event."""

    async def on_order(self, ctx: 'StrategyContext', order: 'Order') -> None:
        """Own order change (order tracker: WS + REST reconciliation after a disconnect)."""

    async def on_fill(self, ctx: 'StrategyContext', fill: 'Fill') -> None:
        """Own fill (the ``fills`` stream)."""

    async def on_position(self, ctx: 'StrategyContext', position: 'Position') -> None:
        """Position change (position tracker; close — size 0)."""

    async def on_timer(self, ctx: 'StrategyContext', timer: Timer) -> None:
        """Timer without its own callback."""

    async def on_stop(self, ctx: 'StrategyContext') -> None:
        """Stop (always, including on task cancellation); ``ctx.stop_reason`` — the reason."""


class StrategyContext:
    """Everything available to the strategy: Exchange, clock, timers, subscriptions, journal, algos, risk."""

    def __init__(self, runner: 'StrategyRunner') -> None:
        self._runner = runner
        self.ex: 'Exchange' = runner.ex
        self.clock: Any = runner.clock
        self.state: dict = {}                          # free-form strategy storage
        self.recovery: Optional['RecoveryReport'] = None
        self.orders: Optional['OrderTracker'] = None
        self.positions: Optional['PositionTracker'] = None
        self.stop_reason: Optional[str] = None

    # ─── time and timers ────────────────────────────────────────────────────

    def now_ms(self) -> int:
        return int(self.clock.now_ms())

    async def sleep(self, seconds: float) -> None:
        """Sleep on the strategy clock (virtual in replay and tests)."""
        await self.clock.sleep(seconds)

    def every(self, seconds: float, callback: Optional[TimerCallback] = None, *, name: Optional[str] = None,
              first: Optional[float] = None) -> Timer:
        """Periodic timer: ``callback(ctx)`` (or ``strategy.on_timer``) every ``seconds``;
        ``first`` — delay of the first firing (default ``seconds``)."""
        return self._runner._timer(seconds, callback, name=name, repeat=True, first=first)

    @property
    def timers(self) -> list[Timer]:
        """Created timers (``fired`` / ``missed`` / ``active``)."""
        return list(self._runner.timers)

    def after(self, seconds: float, callback: Optional[TimerCallback] = None, *,
              name: Optional[str] = None) -> Timer:
        """One-shot timer after ``seconds``."""
        return self._runner._timer(seconds, callback, name=name, repeat=False, first=seconds)

    # ─── subscriptions ──────────────────────────────────────────────────────

    def subscribe(self, kind: str, symbol: str, **opts: Any) -> Any:
        """Subscribe: events arrive in ``on_tick`` (``kind`` from :data:`FEEDS`). Returns the stream
        handle (``handle.stop()``)."""
        return self._runner._subscribe(kind, symbol, **opts)

    # ─── access to subsystems ───────────────────────────────────────────────

    @property
    def journal(self) -> Any:
        """The Exchange's position journal (None — not attached)."""
        return self.ex.journal

    @property
    def intents(self) -> Any:
        """The Exchange's intent store (None — not attached)."""
        return self.ex.intents

    @property
    def algos(self) -> Any:
        """Execution algos (``ex.algos``). Their clock defaults to ``ex.clock`` (for paper/replay
        the same as the strategy's); with a custom strategy clock pass ``clock=ctx.clock``."""
        return self.ex.algos

    @contextlib.contextmanager
    def bind(self, *, position_id: Optional[str] = None, role: Optional[str] = None,
             level_id: Optional[int] = None) -> Iterator[None]:
        """``journal.bind(...)`` if a journal is attached, otherwise a no-op."""
        journal = self.ex.journal
        if journal is None:
            yield
            return
        with journal.bind(position_id=position_id, role=role, level_id=level_id):
            yield

    async def risk_size(self, symbol: str, side: str, **kwargs: Any) -> Any:
        """:meth:`Exchange.risk_size` — risk-based size without orders."""
        return await self.ex.risk_size(symbol, side, **kwargs)

    async def open_position(self, symbol: str, side: str, **kwargs: Any) -> Any:
        """:meth:`Exchange.open_position` — risk-based entry with a stop."""
        return await self.ex.open_position(symbol, side, **kwargs)

    def stop(self, reason: str = 'stop') -> None:
        """Graceful stop after the current callback."""
        self._runner.stop(reason)


class StrategyRunner:
    """Runs a :class:`Strategy` on an Exchange (see the module).

    ``clock`` — the clock (default: the paper/replay Exchange clock, otherwise wall clock); ``recover`` —
    intent reconciliation at start if the Exchange has ``intents`` (``recover_kwargs`` —
    :meth:`Exchange.recover` arguments, e.g. ``{'resubmit': True}``); ``track_orders`` /
    ``track_positions`` / ``track_fills`` — trackers and the fill stream (positions — derivatives
    only); ``max_errors`` — stop after that many callback errors (None — never stop).
    """

    def __init__(self, strategy: Strategy, ex: 'Exchange', *, clock: Any = None, recover: bool = True,
                 recover_kwargs: Optional[dict] = None, track_orders: bool = True, track_positions: bool = True,
                 track_fills: bool = True, max_errors: Optional[int] = None) -> None:
        if not isinstance(strategy, Strategy):
            raise ConfigError('run_strategy: a Strategy instance is required')
        bad = [f for f in strategy.feeds if f not in FEEDS]
        if bad:
            raise ConfigError(f'Strategy.feeds: must be from {FEEDS}, not {bad}')
        self.strategy = strategy
        self.ex = ex
        if clock is None:
            clock = getattr(ex, 'clock', None)
        if clock is None:
            from bytx.paper.clock import WallClock
            clock = WallClock()
        self.clock = clock
        self.recover = recover
        self.recover_kwargs = dict(recover_kwargs or {})
        self.track_orders = track_orders
        self.track_positions = track_positions
        self.track_fills = track_fills
        self.max_errors = max_errors
        self.ctx = StrategyContext(self)
        self.timers: list[Timer] = []
        self._handles: list[Any] = []
        self._removers: list[Callable[[], None]] = []
        self._queue: deque = deque()
        self._latest: dict[tuple[str, str], Tick] = {}
        self._wake: Optional[asyncio.Event] = None
        self._stop: Optional[str] = None
        self._errors: list[tuple[str, BaseException]] = []
        self._events = 0
        self._started = False

    # ─── run ────────────────────────────────────────────────────────────────

    async def run(self) -> StrategyResult:
        """Runs until :meth:`stop` / ``ctx.stop()`` / ``max_errors``; returns the result."""
        if self._started:
            raise ConfigError('StrategyRunner.run: already running')
        self._started = True
        self._wake = asyncio.Event()
        started = self.ctx.now_ms()
        reason = 'stop'
        try:
            await self._startup()
            if not await self._call('on_start', self.strategy.on_start, self.ctx):
                self.stop('start_error')
            while self._stop is None:
                if not self._queue:
                    self._wake.clear()
                    await self._wake.wait()
                    continue
                await self._dispatch(self._queue.popleft())
            reason = self._stop
        except asyncio.CancelledError:
            reason = 'cancelled'
            raise
        finally:
            self.ctx.stop_reason = reason
            self._stop = self._stop or reason
            await self._shutdown()
        return StrategyResult(reason=reason, started_ms=started, stopped_ms=self.ctx.now_ms(), events=self._events,
                              errors=list(self._errors), recovery=self.ctx.recovery)

    def stop(self, reason: str = 'stop') -> None:
        if self._stop is None:
            self._stop = reason
        if self._wake is not None:
            self._wake.set()

    async def _startup(self) -> None:
        ex = self.ex
        await ex.connect()
        if self.recover and getattr(ex, 'intents', None) is not None:
            self.ctx.recovery = await ex.recover(**self.recover_kwargs)
        if self.track_orders:
            tracker = ex.order_tracker()
            await tracker.start()
            self.ctx.orders = tracker
            self._removers.append(tracker.on_update(lambda o: self._put(('order', o))))
        if self.track_positions and ex.market_type != 'spot':
            ptracker = ex.position_tracker()
            await ptracker.start()
            self.ctx.positions = ptracker
            self._removers.append(ptracker.on_update(lambda p: self._put(('position', p))))
        if self.track_fills:
            self._handles.append(ex.stream.subscribe('fills', None, lambda f: self._put(('fill', f))))
        for symbol in self.strategy.symbols:
            for kind in self.strategy.feeds:
                opts = {'timeframe': self.strategy.timeframe} if kind == 'candles' else {}
                self._subscribe(kind, symbol, **opts)

    async def _shutdown(self) -> None:
        for t in self.timers:
            t.cancel()
        for h in self._handles:
            h.stop()
        for remove in self._removers:
            remove()
        try:
            await self._call('on_stop', self.strategy.on_stop, self.ctx)
        finally:
            for tracker in (self.ctx.orders, self.ctx.positions):
                if tracker is not None:
                    with contextlib.suppress(Exception):
                        await tracker.stop()
            for h in self._handles:
                with contextlib.suppress(Exception):
                    await h.wait()

    # ─── event sources ──────────────────────────────────────────────────────

    def _subscribe(self, kind: str, symbol: str, **opts: Any) -> Any:
        if kind not in FEEDS:
            raise ConfigError(f'strategy subscription: kind must be from {FEEDS}, not {kind!r}')
        s = self.ex.symbol(symbol)

        def on_item(item: Any) -> None:
            self._tick(kind, s, item)

        handle = self.ex.stream.subscribe(kind, s, on_item, **opts)
        self._handles.append(handle)
        return handle

    def _tick(self, kind: str, symbol: str, item: Any) -> None:
        tick = Tick(kind, symbol, item, self.ctx.now_ms())
        if kind in _LATEST:
            key = (kind, symbol)
            pending = key in self._latest
            self._latest[key] = tick       # coalescing: the strategy gets the latest
            if pending:
                return
            self._put(('latest', key))
            return
        self._put(('tick', tick))

    def _timer(self, seconds: float, callback: Optional[TimerCallback], *, name: Optional[str], repeat: bool,
               first: Optional[float]) -> Timer:
        if seconds <= 0 or (first is not None and first < 0):
            raise ConfigError('timer: seconds > 0, first >= 0')
        timer = Timer(name or f'timer{len(self.timers) + 1}', seconds, repeat, callback)
        self.timers.append(timer)
        timer._task = asyncio.get_running_loop().create_task(self._timer_loop(timer, first),
                                                             name=f'bytx:strategy:{timer.name}')
        return timer

    async def _timer_loop(self, timer: Timer, first: Optional[float]) -> None:
        delay = timer.interval if first is None else first
        while self._stop is None:
            await self.clock.sleep(delay)
            if self._stop is not None:
                return
            if timer._pending:
                timer.missed += 1
            else:
                timer._pending = True
                self._put(('timer', timer))
            if not timer.repeat:
                return
            delay = timer.interval

    def _put(self, event: tuple) -> None:
        if self._stop is not None:
            return
        self._queue.append(event)
        if self._wake is not None:
            self._wake.set()

    # ─── delivery ───────────────────────────────────────────────────────────

    async def _dispatch(self, event: tuple) -> None:
        kind, payload = event
        s, ctx = self.strategy, self.ctx
        self._events += 1
        if kind == 'latest':
            await self._call('on_tick', s.on_tick, ctx, self._latest.pop(payload))
        elif kind == 'tick':
            await self._call('on_tick', s.on_tick, ctx, payload)
        elif kind == 'order':
            await self._call('on_order', s.on_order, ctx, payload)
        elif kind == 'fill':
            await self._call('on_fill', s.on_fill, ctx, payload)
        elif kind == 'position':
            await self._call('on_position', s.on_position, ctx, payload)
        elif kind == 'timer':
            payload._pending = False
            payload.fired += 1
            if payload.callback is not None:
                await self._call(f'timer:{payload.name}', payload.callback, ctx)
            else:
                await self._call(f'timer:{payload.name}', s.on_timer, ctx, payload)

    async def _call(self, where: str, fn: Callable[..., Any], *args: Any) -> bool:
        """Strategy callback (sync or async); an error goes to the log and the result, returns False."""
        try:
            result = fn(*args)
            if inspect.isawaitable(result):
                await result
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._errors.append((where, e))
            logger.exception(f'strategy {type(self.strategy).__name__}: error in {where}')
            if self.max_errors is not None and len(self._errors) >= self.max_errors:
                self.stop('errors')
            return False


async def run_strategy(strategy: Strategy, ex: 'Exchange', **kwargs: Any) -> StrategyResult:
    """``StrategyRunner(strategy, ex, **kwargs).run()`` — see :class:`StrategyRunner`."""
    return await StrategyRunner(strategy, ex, **kwargs).run()


__all__ = ['FEEDS', 'Strategy', 'StrategyContext', 'StrategyResult', 'StrategyRunner', 'Tick', 'Timer',
           'run_strategy']
