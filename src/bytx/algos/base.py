"""Shared parts of execution algorithms: lifecycle, child orders, fill accounting.

An algorithm is a background task on top of regular :class:`~bytx.Exchange` methods (``place_order``,
``cancel_order``, ``order``, ``set_tp_sl`` …): each child order takes the same path as a
manual one, with the ``client_id_prefix`` bot tag, the transport's broker tag, the ``pre_submit`` hook,
RiskGuard and a position journal entry (if attached; role via ``journal.bind(role=...)``).

Rules shared by all algorithms:

- an order with an unknown outcome (:class:`~bytx.errors.OrderStatusUnknown`) is **not re-placed**:
  the algorithm looks it up by client_id (:meth:`Exchange.find_order`) on subsequent steps; if found,
  it is counted; if absence is confirmed, it is treated as not placed; if it cannot be checked
  ``max_errors`` times in a row, the algorithm stops with status ``failed``;
- no new child order is placed while the previous one may still be alive: cancel with a network error →
  re-read the order; status unclear → the step is retried later;
- network read failures do not crash the algorithm: the step is retried; ``max_errors`` in a row → ``failed``;
- cancel/pause are cooperative (at a safe point of the step); cancelling the task externally (``task.cancel``)
  also cancels working orders (best-effort, under ``shield``).

Child order state: a live :class:`~bytx.OrderTracker` (``tracker=``), otherwise REST
``ex.order``. After a WS disconnect the tracker is not ``live``; the algorithm falls back to REST until the
tracker's REST reconciliation; order book: fresh WS cache ``ex.stream`` or REST ``order_book``. Time: the ``clock``
(on paper, ``ex.clock``: virtual time on replay and in tests).
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import itertools
import random
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Generator, Iterator, Literal, Optional

from loguru import logger

from bytx.errors import (
    ConfigError,
    NetworkError,
    OrderNotFound,
    OrderStatusUnknown,
    OrderTooSmall,
)
from bytx.paper.clock import Clock, WallClock
from bytx.types import Order, OrderBook, Side

if TYPE_CHECKING:  # pragma: no cover
    from bytx.exchange import Exchange
    from bytx.trackers import OrderTracker

AlgoStatus = Literal['running', 'paused', 'done', 'canceled', 'failed']
FINAL = frozenset({'done', 'canceled', 'failed'})
TERMINAL_ORDER = frozenset({'closed', 'canceled', 'expired', 'rejected'})
EPS = 1e-12
_ids = itertools.count(1)


class AlgoStopped(Exception):
    """Internal signal: algorithm cancellation requested (never escapes)."""


@dataclass(slots=True)
class AlgoResult:
    """Algorithm result.

    ``status``: 'done' (target reached or exhausted per the algorithm's rules), 'canceled'
    (``cancel()`` or task cancellation), 'failed' (``error``); ``reason``: why it ended
    ('filled', 'max_moves', 'deviation', 'take_profit', 'stop_loss', 'trailing', 'position_closed',
    'no_liquidity', 'expired', 'user', …). Sizes are in the base coin; ``average`` is the average fill
    price across all child orders, ``fee`` is the total fee (currency per order);
    ``children``: last known state of each child order in placement order;
    ``extra``: algorithm-specific data (re-placements, slices, slippage, …).
    """

    algo: str
    id: str
    symbol: str
    side: Optional[str]
    status: str
    reason: Optional[str]
    target: Optional[float]
    filled: float
    average: Optional[float]
    fee: float
    children: list[Order]
    started_ms: int
    finished_ms: Optional[int]
    error: Optional[BaseException] = None
    extra: dict = field(default_factory=dict)

    @property
    def remaining(self) -> Optional[float]:
        return None if self.target is None else max(self.target - self.filled, 0.0)


@dataclass(slots=True)
class _Child:
    order: Order
    role: Optional[str]
    level_id: Optional[int] = None
    tag: str = ''          # tag within the algorithm: 'slice', 'entry', 'tp', 'sl', …


async def current_book(ex: 'Exchange', s: str, depth: Optional[int] = None) -> OrderBook:
    """Order book without extra requests: paper uses the engine's book, otherwise the fresh WS cache ``ex.stream``
    (absent after a WS disconnect: :meth:`Streams.last_book` returns None), otherwise REST."""
    engine = getattr(ex, 'engine', None)
    if engine is not None:
        book = engine.book(s)
        if book is not None:
            return book
    streams = getattr(ex, '_streams', None)
    if streams is not None:
        book = streams.last_book(s)
        if book is not None and book.bids and book.asks:
            return book
    return await ex.order_book(s, depth)


class ExecAlgo:
    """Execution algorithm base (see module). Subclasses implement :meth:`_run`.

    The object is awaitable: ``result = await algo`` waits for completion and returns :class:`AlgoResult`
    (does not raise the algorithm's error: it is in ``result.error`` with status 'failed').

    ``role``: role of child orders in the position journal (``entry``, ``dca``, ``close`` …; see
    :data:`bytx.journal.models.ROLES`), ``position_id``: journal position (as in
    ``journal.bind``); ``poll``: polling step, s; ``tracker``: a running OrderTracker
    (order state without REST while it is ``live``); ``clock``: the clock (defaults to ``ex.clock``
    on paper, otherwise real time); ``max_errors``: consecutive failures tolerated before 'failed';
    ``on_update(algo)``: callback on every change (sync/async; errors are logged).
    """

    kind = 'algo'

    def __init__(self, ex: 'Exchange', symbol: str, side: Optional[Side], *, role: Optional[str] = None,
                 position_id: Optional[str] = None, poll: float = 1.0, clock: Optional[Clock] = None,
                 tracker: Optional['OrderTracker'] = None, max_errors: int = 5,
                 on_update: Optional[Callable[['ExecAlgo'], Any]] = None) -> None:
        if side is not None and side not in ('buy', 'sell'):
            raise ConfigError(f"side: 'buy' or 'sell', not {side!r}", exchange=ex.name)
        if poll <= 0:
            raise ConfigError(f'poll must be > 0, not {poll!r}', exchange=ex.name)
        if max_errors < 1:
            raise ConfigError(f'max_errors must be >= 1, not {max_errors!r}', exchange=ex.name)
        if role is not None:
            from bytx.journal.models import ROLES
            if role not in ROLES:
                raise ConfigError(f'role: one of {sorted(ROLES)}, not {role!r}', exchange=ex.name)
        self.ex = ex
        self.symbol = symbol
        self.side: Optional[Side] = side
        self.role = role
        self.position_id = position_id
        self.poll = float(poll)
        self.clock: Clock = clock or getattr(ex, 'clock', None) or WallClock()
        self.tracker = tracker
        self.max_errors = int(max_errors)
        self.on_update = on_update
        self.id = f'{self.kind}-{next(_ids)}'
        self.target: Optional[float] = None
        self.status: AlgoStatus = 'running'
        self.reason: Optional[str] = None
        self.error: Optional[BaseException] = None
        self.extra: dict = {}
        self._children: list[_Child] = []
        self._by_id: dict[str, _Child] = {}
        self._unknown: list[tuple[str, Optional[str], Optional[int], str]] = []   # (cid, role, level, tag)
        self._errors = 0
        self._stop_reason: Optional[str] = None
        self._resume = asyncio.Event()
        self._resume.set()
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self.started_ms = self.clock.now_ms()
        self.finished_ms: Optional[int] = None
        self.paused_ms = 0          # time the algorithm spent paused (by its clock)
        self._paused_at = 0

    # ─── public API ─────────────────────────────────────────────────────────

    def start(self) -> 'ExecAlgo':
        """Starts the algorithm as a background task (``ex.algos.*`` do this themselves). Repeated calls are a no-op."""
        if self._task is None:
            self._task = asyncio.get_running_loop().create_task(self._main(), name=f'bytx:{self.id}')
        return self

    def __await__(self) -> Generator[Any, None, AlgoResult]:
        return self.wait().__await__()

    async def wait(self, timeout: Optional[float] = None) -> AlgoResult:
        """Waits for the algorithm to finish (``timeout`` in real seconds, ``asyncio.TimeoutError``; the algorithm
        itself is not cancelled). The result is returned even after the algorithm's task is cancelled."""
        self.start()
        assert self._task is not None
        done, _ = await asyncio.wait({self._task}, timeout=timeout)
        if not done:
            raise asyncio.TimeoutError(f'{self.id}: did not finish within {timeout} s')
        return self.result()

    @property
    def done(self) -> bool:
        return self.status in FINAL

    @property
    def paused(self) -> bool:
        return self.status == 'paused'

    def pause(self) -> None:
        """Pause: no new child orders, no re-placements; resting orders stay
        (their fills are counted after :meth:`resume`). Algorithm timers stop while paused."""
        if self.status == 'running':
            self.status = 'paused'
            self._paused_at = self.clock.now_ms()
            self._resume.clear()
            self._notify()

    def resume(self) -> None:
        if self.status == 'paused':
            self.status = 'running'
            self.paused_ms += self.clock.now_ms() - self._paused_at
            self._resume.set()
            self._wake.set()
            self._notify()

    async def cancel(self, reason: str = 'user') -> AlgoResult:
        """Cancel: cancels the algorithm's working child orders and waits for completion. Fills remain
        (the position is not closed). If already finished, just returns the result."""
        if not self.done and self._stop_reason is None:
            self._stop_reason = reason
            self._resume.set()
            self._wake.set()
        return await self.wait()

    @property
    def children(self) -> list[Order]:
        """Child orders (last known state) in placement order."""
        return [c.order for c in self._children]

    @property
    def filled(self) -> float:
        return sum(_filled(c.order) for c in self._children if self._counts(c))

    @property
    def average(self) -> Optional[float]:
        qty = cost = 0.0
        for c in self._children:
            if not self._counts(c):
                continue
            f = _filled(c.order)
            px = c.order.average or c.order.price
            if f > EPS and px:
                qty += f
                cost += f * px
        return cost / qty if qty > EPS else None

    @property
    def fee(self) -> float:
        return sum(float(c.order.fee or 0.0) for c in self._children)

    def result(self) -> AlgoResult:
        return AlgoResult(algo=self.kind, id=self.id, symbol=self.symbol, side=self.side, status=self.status,
                          reason=self.reason, target=self.target, filled=self.filled, average=self.average,
                          fee=self.fee, children=self.children, started_ms=self.started_ms,
                          finished_ms=self.finished_ms, error=self.error, extra=dict(self.extra))

    def __repr__(self) -> str:
        return (f'<{type(self).__name__} {self.id} {self.symbol} {self.side or ""} {self.status} '
                f'filled={self.filled:g}/{self.target if self.target is not None else "-"}>')

    # ─── lifecycle ──────────────────────────────────────────────────────────

    async def _run(self) -> None:  # pragma: no cover — overridden
        raise NotImplementedError

    async def _cleanup(self) -> None:
        """Cancel working child orders (default: all open ones)."""
        await self._cancel_open()

    def _counts(self, child: _Child) -> bool:
        """Whether the order's fill counts toward the algorithm's ``filled`` (bracket TP/SL do not)."""
        return True

    async def _main(self) -> None:
        try:
            await self.ex.connect()
            self.symbol = self.ex.symbol(self.symbol)
            await self._run()
            if self._stop_reason is not None:
                raise AlgoStopped()
            self._finish('done', self.reason or 'filled')
        except AlgoStopped:
            await self._safe_cleanup()
            self._finish('canceled', self._stop_reason or 'user')
        except asyncio.CancelledError:
            with contextlib.suppress(BaseException):
                await asyncio.shield(self._safe_cleanup())
            self._finish('canceled', 'task_cancelled')
            raise
        except Exception as e:
            self.error = e
            logger.warning(f'{self.ex.name} {self.id}: stopped with error: {e!r}')
            await self._safe_cleanup()
            self._finish('failed', self.reason or 'error')

    async def _safe_cleanup(self) -> None:
        try:
            await self._cleanup()
        except Exception as e:  # cancelling children failed: record in result, orders may remain
            logger.warning(f'{self.ex.name} {self.id}: child orders not canceled: {e!r}')
            self.extra.setdefault('cleanup_errors', []).append(repr(e))

    def _finish(self, status: AlgoStatus, reason: str) -> None:
        if self.done:
            return
        self.status = status
        self.reason = reason
        self.finished_ms = self.clock.now_ms()
        self._notify()

    def _stop(self, reason: str) -> None:
        """Finish the algorithm normally ('done') with a reason (from ``_run``: return afterwards)."""
        self.reason = reason

    def _notify(self) -> None:
        cb = self.on_update
        if cb is None:
            return
        try:
            out = cb(self)
            if inspect.isawaitable(out):
                asyncio.ensure_future(out)
        except Exception:
            logger.exception(f'{self.id}: error in on_update')

    # ─── step, pause, time ──────────────────────────────────────────────────

    def _check_stop(self) -> None:
        if self._stop_reason is not None:
            raise AlgoStopped()

    async def _gate(self) -> None:
        """Safe point: cancel → AlgoStopped; pause → wait for resume (or cancel)."""
        self._check_stop()
        if not self._resume.is_set():
            await self._resume.wait()
            self._check_stop()

    async def _sleep(self, seconds: float) -> None:
        """Sleep by the algorithm's clock; interrupted by cancel/resume (``_wake``)."""
        self._check_stop()
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        self._wake.clear()
        sleeper = asyncio.ensure_future(self.clock.sleep(seconds))
        waker = asyncio.ensure_future(self._wake.wait())
        try:
            await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (sleeper, waker):
                if not t.done():
                    t.cancel()
        self._check_stop()

    def _error(self, e: BaseException, what: str) -> None:
        """Step failure: count consecutive ones; at ``max_errors`` the error propagates (algorithm 'failed')."""
        self._errors += 1
        logger.debug(f'{self.ex.name} {self.id}: {what}: {e!r} ({self._errors}/{self.max_errors})')
        if self._errors >= self.max_errors:
            raise e
        self.extra['errors'] = self.extra.get('errors', 0) + 1

    def _ok(self) -> None:
        self._errors = 0

    # ─── child orders ───────────────────────────────────────────────────────

    @contextlib.contextmanager
    def _bind(self, role: Optional[str], level_id: Optional[int] = None) -> Iterator[None]:
        journal = self.ex.journal
        if journal is None:
            yield
            return
        with journal.bind(position_id=self.position_id, role=role, level_id=level_id):
            yield

    async def _place(self, *, side: Side, size: float, price: Optional[float] = None, type: Optional[str] = None,
                     post_only: bool = False, time_in_force: Optional[str] = None, reduce_only: bool = False,
                     role: Optional[str] = None, level_id: Optional[int] = None, tag: str = '') -> Optional[Order]:
        """Child order (size in base). None means unknown outcome (order watched by cid)."""
        role = role if role is not None else self.role
        with self._bind(role, level_id):
            try:
                order = await self.ex.place_order(self.symbol, side, size=size, price=price, type=type,
                                                  post_only=post_only, time_in_force=time_in_force,
                                                  reduce_only=reduce_only)
            except OrderStatusUnknown as e:
                logger.warning(f'{self.ex.name} {self.id}: order {e.client_order_id} outcome unknown; '
                               'looking up by client_id, not re-placing')
                self._unknown.append((str(e.client_order_id), role, level_id, tag))
                self._notify()
                return None
        self._add(order, role, level_id, tag)
        return order

    def _add(self, order: Order, role: Optional[str], level_id: Optional[int], tag: str) -> _Child:
        child = _Child(order, role, level_id, tag)
        self._children.append(child)
        self._by_id[order.id] = child
        self._notify()
        return child

    def _update(self, order: Order) -> Order:
        child = self._by_id.get(order.id)
        if child is not None:
            old = child.order
            if old.status in TERMINAL_ORDER and order.status not in TERMINAL_ORDER:
                return old  # late "open" after completion
            if order.filled is not None and old.filled is not None and order.filled < old.filled - EPS:
                return old
            if order.client_id is None:
                order.client_id = old.client_id
            child.order = order
            self._notify()
        return order

    async def _resolve_unknown(self) -> bool:
        """Reconcile orders with unknown outcome. True means all resolved (new ones may be placed)."""
        while self._unknown:
            cid, role, level, tag = self._unknown[0]
            try:
                found = await self.ex.find_order(self.symbol, cid)
            except NetworkError as e:   # RequestTimeout: absence not confirmed
                self._error(e, f'reconcile {cid}')
                return False
            self._ok()
            self._unknown.pop(0)
            if found is not None:
                self._add(found, role, level, tag)
            else:
                logger.info(f'{self.ex.name} {self.id}: order {cid} absent (reconcile); treating as not placed')
        return True

    async def _refresh(self, order: Order) -> Order:
        """Fresh child order state: live tracker, otherwise REST. On network failure, the previous state."""
        tr = self.tracker
        if tr is not None and tr.live:
            got = tr.get(order.id)
            if got is not None:
                return self._update(got)
        try:
            fresh = await self.ex.order(self.symbol, order.id)
        except NetworkError as e:
            self._error(e, f'read order {order.id}')
            return order
        self._ok()
        return self._update(fresh)

    async def _cancel_child(self, order: Order) -> Optional[Order]:
        """Cancel a child order → final state; None means status unclear (network)."""
        try:
            with self._bind(self._by_id[order.id].role if order.id in self._by_id else None):
                done = await self.ex.cancel_order(self.symbol, order.id)
        except NetworkError as e:
            self._error(e, f'cancel {order.id}')
            fresh = await self._refresh(order)
            return fresh if fresh.status in TERMINAL_ORDER else None
        except OrderNotFound:
            fresh = await self._refresh(order)
            return fresh if fresh.status in TERMINAL_ORDER else None
        self._ok()
        if done.status not in TERMINAL_ORDER:
            # exchange accepted the cancel but returned the old status (async cancel): re-read
            done = await self._refresh(done)
            if done.status not in TERMINAL_ORDER:
                return None
        return self._update(done)

    async def _cancel_open(self) -> None:
        """Cancel all open child orders; unclear ones are retried (up to ``max_errors``)."""
        for _ in range(self.max_errors):
            pending = [c.order for c in self._children if c.order.status not in TERMINAL_ORDER]
            if not pending:
                break
            for o in pending:
                await self._cancel_child(o)
        if self._unknown:
            with contextlib.suppress(Exception):
                await self._resolve_unknown()
            for c in list(self._children):
                if c.order.status not in TERMINAL_ORDER:
                    await self._cancel_child(c.order)

    async def _book(self, depth: Optional[int] = None) -> OrderBook:
        return await current_book(self.ex, self.symbol, depth)

    def _round_size(self, size: float) -> float:
        """Round size down to the step; below one step gives 0.0 (not OrderTooSmall)."""
        if size <= EPS:
            return 0.0
        try:
            return self.ex.round_size(self.symbol, size)
        except OrderTooSmall:
            return 0.0

    def _round_price(self, price: float, side: Optional[Side] = None) -> float:
        return self.ex.round_price(self.symbol, price, side=side)

    def _min_size(self) -> float:
        """Minimum order size (base): the market's amount step and ``min_amount``."""
        from bytx.markets import to_market
        m = to_market(self.ex.ccxt.market(self.symbol), self.ex.ccxt.id)
        k = m.contract_size if m.is_contract else 1.0
        return max((m.amount_step or 0.0) * k, (m.min_amount or 0.0) * (k if m.is_contract else 1.0))


def _filled(order: Order) -> float:
    return float(order.filled or 0.0)


def bps(a: float, b: float) -> float:
    """Deviation of a from b in basis points (signed)."""
    return (a - b) / b * 1e4 if b else 0.0


def algo_rng(ex: 'Exchange', seed: Optional[int]) -> random.Random:
    """The algorithm's randomization generator: if ``seed`` is set, derived from it; if not, on paper with a
    deterministic client_id generator (always on replay, ``PaperConfig(seed=)``), derived from that (same inputs →
    same slices); otherwise seeded by the OS."""
    if seed is not None:
        return random.Random(seed)
    base = getattr(ex, '_cid_rng', None)
    if isinstance(base, random.Random):
        return random.Random(base.getrandbits(64))
    return random.Random()


def check_positive(name: str, value: Optional[float], ex: 'Exchange', *, allow_none: bool = False) -> None:
    if value is None:
        if not allow_none:
            raise ConfigError(f'{name}: value required', exchange=ex.name)
        return
    if not value > 0:
        raise ConfigError(f'{name} must be > 0, not {value!r}', exchange=ex.name)


__all__ = ['AlgoResult', 'AlgoStatus', 'ExecAlgo', 'bps', 'current_book']
