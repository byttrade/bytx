"""WS subscription supervisor: reconnects, silence watchdog, fan-out, statistics.

A ccxt.pro ``watch_*`` method raises on disconnect; the next call reopens the
connection and resubscribes (ccxt has ping/keepAlive, but no backoff,
no detection of a "silently hung" socket, and no fan-out of one stream to several consumers).

On top of that:

- :class:`Feed` — ONE ccxt subscription per key (stream kind, symbol, params) and a
  ``watch_*`` loop with exponential backoff + jitter (reset after a stable period),
  a silence watchdog (no messages for N seconds: the whole connection is silent — close that
  ccxt client; the connection is alive — resubscribe only this topic), stopping on
  auth/configuration errors;
- :class:`Mailbox` — one consumer's queue: order books/tickers keep only
  the latest value (conflation), trades/orders get a bounded queue
  (drop-oldest with a counter);
- :class:`FeedStats` — counters and "exchange → us" latency (p50/p99);
- :class:`StateEvent` — state events for ``ex.stream.on_state``.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal, Optional

import ccxt
from loguru import logger

from bytx.errors import (
    AuthError,
    BadSymbol,
    BytxError,
    ConfigError,
    NetworkError,
    NotSupported,
    from_ccxt,
)

Policy = Literal['latest', 'queue']
State = Literal['connected', 'disconnected', 'reconnecting', 'resynced', 'stale', 'crossed', 'nonce',
                'stopped']

# Silence (s) after which a stream is considered hung. None — no watchdog
# (private streams may legitimately stay silent for hours; trades and candles of an illiquid
# symbol — for minutes, and their silence says nothing about the connection).
DEFAULT_STALE_AFTER: dict[str, Optional[float]] = {
    'order_book': 10.0,
    'best_bid_ask': 10.0,
    'ticker': 30.0,
    'trades': None,
    'candles': None,
    'orders': None,
    'fills': None,
    'positions': None,
    'balance': None,
}

# Errors a retry won't fix: the stream stops and the error goes to the consumer.
_FATAL_CCXT: tuple[type[BaseException], ...] = (
    ccxt.AuthenticationError,   # including PermissionDenied, AccountSuspended
    ccxt.NotSupported,
    ccxt.BadSymbol,
    ccxt.ArgumentsRequired,
    ccxt.BadRequest,            # invalid subscription params (Bybit: invalid depth) — retry won't help
    ccxt.ExchangeClosedByUser,  # ccxt instance closed — do not reopen connections
)
_FATAL_SDK: tuple[type[BytxError], ...] = (AuthError, NotSupported, BadSymbol, ConfigError)

# Replaced in tests.
_sleep = asyncio.sleep
_HAS_TIMEOUT = hasattr(asyncio, 'timeout')  # Python 3.11+
_READY_POLL = 0.1   # s: connection readiness polling after reconnect
#: default tracker wait for subscription confirmation, s (:attr:`StreamConfig.ready_timeout`)
DEFAULT_READY_TIMEOUT = 10.0


def _own_cancel(stopping: bool = False) -> bool:
    """True — the current task is being cancelled (not the shared ccxt Future it was waiting on).

    ``stopping`` — the :meth:`Feed.stop` flag; ``Task.cancelling()`` exists only since Python 3.11.
    """
    if stopping:
        return True
    task = asyncio.current_task()
    if task is None:
        return True
    cancelling = getattr(task, 'cancelling', None)
    return cancelling is not None and cancelling() > 0


def is_fatal(error: BaseException) -> bool:
    """True — retrying is pointless (auth, unsupported method, invalid symbol)."""
    return isinstance(error, _FATAL_CCXT) or isinstance(error, _FATAL_SDK)


@dataclass
class StreamConfig:
    """Supervisor settings (shared by all streams of one :class:`Exchange`)."""

    backoff_base: float = 0.5        # first pause before reconnecting, s
    backoff_factor: float = 2.0
    backoff_max: float = 30.0        # pause ceiling
    jitter: float = 0.25             # ±fraction of random pause spread
    stable_after: float = 60.0       # this many seconds without errors → attempt counter resets
    max_attempts: Optional[int] = None   # None — reconnect forever
    # consecutive exchange rejections (not network) before the first message, after which the stream stops
    # with an error: an invalid subscription must not spin silently forever. None — no limit
    max_rejections: Optional[int] = 5
    stale_after: dict[str, Optional[float]] = field(default_factory=lambda: dict(DEFAULT_STALE_AFTER))
    # consecutive watchdog triggers with a live connection (only the topic is silent), after
    # which the connection is recreated anyway; until then — resubscribe the single topic
    stale_escalate: int = 3
    queue_size: int = 1000           # for queued streams (trades, orders…)
    latency_window: int = 1000       # window for p50/p99 latency
    check_nonce: bool = True         # order book nonce went backwards → resync
    # trackers (OrderTracker/PositionTracker.start) wait for subscription confirmation before the REST snapshot,
    # no longer than this many seconds; if not confirmed, the snapshot is taken anyway, with a log warning.
    # 0 — don't wait (as before the 2026-10-26 demo run)
    ready_timeout: float = field(default_factory=lambda: DEFAULT_READY_TIMEOUT)

    def backoff(self, attempt: int, rng: Callable[[], float] = random.random) -> float:
        """Pause before attempt ``attempt`` (1, 2, …): exponential with a ceiling and jitter."""
        raw = self.backoff_base * self.backoff_factor ** max(attempt - 1, 0)
        delay = min(self.backoff_max, raw)
        if self.jitter:
            delay *= 1 + self.jitter * (2 * rng() - 1)
        return max(0.0, min(self.backoff_max, delay))

    def stale_for(self, kind: str) -> Optional[float]:
        return self.stale_after.get(kind, DEFAULT_STALE_AFTER.get(kind))


@dataclass(slots=True)
class StateEvent:
    """Stream state event (for ``ex.stream.on_state``)."""

    state: State
    stream: str                      # 'order_book:BTC/USDT:USDT'
    kind: str
    symbol: Optional[str]
    attempt: int = 0
    delay: Optional[float] = None
    error: Optional[BaseException] = None
    reason: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass(slots=True)
class FeedStats:
    """Counters of one stream. ``latency_*`` — local time minus exchange time, ms."""

    state: str = 'idle'
    messages: int = 0
    items: int = 0
    reconnects: int = 0
    errors: int = 0
    stale: int = 0
    crossed: int = 0
    resyncs: int = 0
    dropped: int = 0
    conflated: int = 0
    subscribers: int = 0
    last_message_at: Optional[float] = None
    last_error: Optional[str] = None
    latency_last_ms: Optional[float] = None
    latency_max_ms: Optional[float] = None
    _latency: deque = field(default_factory=lambda: deque(maxlen=1000), repr=False)

    def record_latency(self, value_ms: float) -> None:
        self.latency_last_ms = value_ms
        if self.latency_max_ms is None or value_ms > self.latency_max_ms:
            self.latency_max_ms = value_ms
        self._latency.append(value_ms)

    def as_dict(self) -> dict:
        lat = sorted(self._latency)
        return {
            'state': self.state, 'messages': self.messages, 'items': self.items,
            'reconnects': self.reconnects, 'errors': self.errors, 'stale': self.stale,
            'crossed': self.crossed, 'resyncs': self.resyncs, 'dropped': self.dropped,
            'conflated': self.conflated, 'subscribers': self.subscribers,
            'last_message_age': (None if self.last_message_at is None
                                 else time.time() - self.last_message_at),
            'last_error': self.last_error,
            'latency_ms': {
                'last': self.latency_last_ms,
                'avg': sum(lat) / len(lat) if lat else None,
                'p50': percentile(lat, 50),
                'p99': percentile(lat, 99),
                'max': self.latency_max_ms,
                'samples': len(lat),
            },
        }


def percentile(sorted_values: list[float], pct: float) -> Optional[float]:
    """Percentile (nearest-rank) over an already sorted list."""
    if not sorted_values:
        return None
    rank = max(1, int(round(pct / 100 * len(sorted_values) + 0.4999)))
    return sorted_values[min(rank, len(sorted_values)) - 1]


# ─── consumer mailbox ──────────────────────────────────────────────

class Mailbox:
    """Queue of one stream consumer.

    policy='latest' — only the latest value is kept (a slow consumer
    skips intermediate books/tickers, ``stats.conflated`` grows);
    policy='queue' — a queue up to ``maxlen``; on overflow the oldest
    item is dropped (``stats.dropped`` grows).
    """

    __slots__ = ('policy', 'stats', 'accept', 'transform', '_items', '_latest', '_has_latest',
                 '_event', '_error', '_closed')

    def __init__(self, policy: Policy, stats: FeedStats, *, maxlen: int = 1000,
                 accept: Optional[Callable[[Any], bool]] = None,
                 transform: Optional[Callable[[Any], Any]] = None) -> None:
        self.policy = policy
        self.stats = stats
        self.accept = accept
        self.transform = transform
        self._items: deque = deque(maxlen=maxlen)
        self._latest: Any = None
        self._has_latest = False
        self._event = asyncio.Event()
        self._error: Optional[BaseException] = None
        self._closed = False

    def put(self, item: Any) -> None:
        if self._closed:
            return
        if self.accept is not None and not self.accept(item):
            return
        if self.transform is not None:
            item = self.transform(item)
        if self.policy == 'latest':
            if self._has_latest:
                self.stats.conflated += 1
            self._latest = item
            self._has_latest = True
        else:
            if len(self._items) == self._items.maxlen:
                self.stats.dropped += 1
            self._items.append(item)
        self._event.set()

    def fail(self, error: BaseException) -> None:
        self._error = error
        self._event.set()

    def close(self) -> None:
        self._closed = True
        self._event.set()

    def pending(self) -> int:
        return len(self._items) + (1 if self._has_latest else 0)

    def get_nowait(self) -> Any:
        """Next value without waiting; IndexError — empty."""
        if self._has_latest:
            self._has_latest = False
            item, self._latest = self._latest, None
            return item
        return self._items.popleft()

    async def get(self) -> Any:
        while True:
            if self._has_latest or self._items:
                return self.get_nowait()
            if self._error is not None:
                raise self._error
            if self._closed:
                raise StopAsyncIteration
            self._event.clear()
            await self._event.wait()


# ─── spec and feed ───────────────────────────────────────────────────

class Resync(Exception):
    """Data integrity broken (crossed book, nonce backwards) → reconnect."""

    def __init__(self, reason: str, kind: Literal['crossed', 'nonce'] = 'crossed') -> None:
        super().__init__(reason)
        self.reason = reason
        self.kind = kind


class _Stale(Exception):
    pass


@dataclass
class FeedSpec:
    """How to read and parse one ccxt stream."""

    kind: str
    key: tuple                                   # fan-out key: (kind, symbol, params…)
    symbol: Optional[str]
    watch: Callable[[], Awaitable[Any]]          # one ccxt watch_* call
    convert: Callable[[Any, 'Feed'], list]       # raw response → list of models
    policy: Policy = 'queue'
    timestamp: Callable[[Any], Optional[int]] = lambda item: getattr(item, 'timestamp', None)
    tokens: tuple[str, ...] = ()                 # substrings to find the ccxt client (url/hash)
    check: Optional[Callable[[list, 'Feed'], None]] = None   # integrity check → Resync
    on_reconnect: Optional[Callable[['Feed'], None]] = None  # reset local state
    unwatch: Optional[Callable[[], Awaitable[Any]]] = None   # topic unsubscribe (ccxt un_watch_*)
    close: Optional[Callable[[], Any]] = None    # Feed stop: release its own resources (OKX merged-watch pumps)

    @property
    def name(self) -> str:
        return ':'.join(str(p) for p in self.key if p is not None)


# ccxt client attribute: time.monotonic() of the last incoming message
_LAST_MESSAGE = '_bytx_last_message'


# ─── subscription confirmation (stream readiness) ──────────────────────────────
# Bybit V5 and Bitget V2 confirm a subscription with a server response: topic events do not arrive
# before it (Bybit demo run 2026-10-26: an order placed between sending the subscription and the
# "subscribe success" response produced no "New" WS event). ccxt.pro returns from watch_*
# only with the first data — the confirmation is visible only in incoming messages.
# Bybit (docs v5/ws/connect): request {"op":"subscribe","req_id":…}, response {"op":"subscribe",
# "success":true,"req_id":…}. Bitget (docs «Websocket API»): {"op":"subscribe","args":[…]},
# response per arg {"event":"subscribe","arg":{…}}, rejection {"event":"error","arg":…}.
# OKX (docs WebSocket → Subscribe) — same format: {"op":"subscribe","args":[…]} → {"event":"subscribe",
# "arg":{…},"connId":…}. Without tracking the response, "connected" was announced on sending the subscription: the tracker's
# REST reconciliation ran before the subscription started delivering events, and an order cancel in that window
# was lost (demo 2026-10-27: the tracker was stuck at "open" forever).
# KuCoin (docs Classic WebSocket → Introduction, «5. Subscribe»): {"id":…,"type":"subscribe","topic":…,
# "response":true} → {"id":…,"type":"ack"} — «If the response is set to true, the system will return ack
# messages after the subscription has succeeded»; ccxt kucoinfutures sends response: true.
# HTX USDT-M (docs «Illustration of Request(req and rep) Data»): request {"op":"sub","cid":…,"topic":…}, response —
# same op and cid («Server will returns the same value») with "err-code" (0 — success); market: {"sub":…,"id":…} →
# {"id":…,"subbed":…} or {"id":…,"status":"error"}.
SUB_ACK_EXCHANGES = frozenset({'bybit', 'bitget', 'okx', 'kucoinfutures', 'htx'})
_SUBS = '_bytx_subs'


class _SubState:
    """Sent but not yet confirmed subscriptions of one ccxt instance."""

    __slots__ = ('pending', 'last_sent', 'last_ack')

    def __init__(self) -> None:
        self.pending: set[str] = set()
        self.last_sent = 0.0     # time.monotonic() of the last sent subscription
        self.last_ack = 0.0


def _arg_key(arg: Any) -> str:
    if isinstance(arg, dict):
        arg = {k: str(v).lower() for k, v in arg.items() if k in ('instType', 'channel', 'instId', 'coin')}
    return 'arg:' + json.dumps(arg, sort_keys=True, default=str)


def _outgoing_subs(exchange_id: str, message: Any) -> list[str]:
    if exchange_id == 'kucoinfutures':
        if isinstance(message, dict) and message.get('type') == 'subscribe' and message.get('response') \
                and message.get('id') is not None:
            return [f'id:{message["id"]}']
        return []
    if exchange_id == 'htx':
        return _htx_subs(message, outgoing=True)
    if not isinstance(message, dict) or message.get('op') != 'subscribe':
        return []
    if exchange_id == 'bybit':
        return [f'req:{message.get("req_id")}'] if message.get('req_id') is not None else []
    return [_arg_key(a) for a in message.get('args') or []]


def _htx_subs(message: Any, *, outgoing: bool) -> list[str]:
    if not isinstance(message, dict):
        return []
    if message.get('op') == 'sub' and message.get('cid') is not None:
        if outgoing or 'err-code' in message:
            return [f'cid:{message["cid"]}']
        return []
    if message.get('id') is not None and (('sub' in message) if outgoing
                                           else ('subbed' in message or message.get('status') == 'error')):
        return [f'id:{message["id"]}']
    return []


def _acked_subs(exchange_id: str, message: Any) -> list[str]:
    if not isinstance(message, dict):
        return []
    if exchange_id == 'htx':
        return _htx_subs(message, outgoing=False)
    if exchange_id == 'bybit':
        if message.get('op') == 'subscribe' and message.get('req_id') is not None:
            return [f'req:{message.get("req_id")}']
        return []
    if exchange_id == 'kucoinfutures':
        return [f'id:{message["id"]}'] if message.get('type') == 'ack' and message.get('id') is not None else []
    if message.get('event') in ('subscribe', 'error') and message.get('arg') is not None:
        return [_arg_key(message['arg'])]
    return []


def sub_state(ccxt_exchange: Any) -> Optional[_SubState]:
    return getattr(ccxt_exchange, _SUBS, None)


def _wrap_send(ccxt_exchange: Any, client: Any, state: _SubState) -> None:
    send = getattr(client, 'send', None)
    if send is None or getattr(send, '_bytx_subs', False):
        return
    exchange_id = getattr(ccxt_exchange, 'id', '')

    async def tracked_send(message: Any) -> Any:
        keys = _outgoing_subs(exchange_id, message)
        if keys:
            state.pending.update(keys)
            state.last_sent = time.monotonic()
        return await send(message)

    tracked_send._bytx_subs = True  # type: ignore[attr-defined]
    client.send = tracked_send


def _install_sub_tracking(ccxt_exchange: Any) -> Optional[_SubState]:
    """Tracking of "sent → confirmed" subscriptions (exchanges in :data:`SUB_ACK_EXCHANGES`)."""
    if getattr(ccxt_exchange, 'id', None) not in SUB_ACK_EXCHANGES:
        return None
    state = sub_state(ccxt_exchange)
    if state is not None:
        return state
    state = _SubState()
    setattr(ccxt_exchange, _SUBS, state)
    original_client = ccxt_exchange.client

    def client(url: str) -> Any:
        c = original_client(url)
        _wrap_send(ccxt_exchange, c, state)
        return c

    ccxt_exchange.client = client
    for c in list((getattr(ccxt_exchange, 'clients', None) or {}).values()):
        _wrap_send(ccxt_exchange, c, state)
    return state


def install_activity_hook(ccxt_exchange: Any) -> None:
    """Marks the last incoming message time on every ccxt client.

    Wraps the instance's ``handle_message`` (ccxt creates clients with it) and clients
    created earlier. With this mark the watchdog tells a silent topic from a silent
    connection. A repeated call is a no-op. Bybit/Bitget — also tracks subscription confirmations
    (:meth:`Feed.wait_ready`).
    """
    original = getattr(ccxt_exchange, 'handle_message', None)
    if original is None or getattr(original, '_bytx_activity', False):
        return
    subs = _install_sub_tracking(ccxt_exchange)
    exchange_id = getattr(ccxt_exchange, 'id', '')

    def handle_message(client: Any, message: Any) -> Any:
        try:
            setattr(client, _LAST_MESSAGE, time.monotonic())
            if subs is not None:
                acked = _acked_subs(exchange_id, message)
                if acked:
                    subs.pending.difference_update(acked)
                    subs.last_ack = time.monotonic()
        except Exception:
            pass
        return original(client, message)

    handle_message._bytx_activity = True  # type: ignore[attr-defined]
    ccxt_exchange.handle_message = handle_message
    for client in list((getattr(ccxt_exchange, 'clients', None) or {}).values()):
        if getattr(client, 'on_message_callback', None) is not None:
            client.on_message_callback = handle_message


def _client_connected(client: Any) -> bool:
    fut = getattr(client, 'connected', None)
    try:
        return fut is not None and fut.done() and not fut.cancelled() and fut.exception() is None
    except Exception:
        return False


def client_active(client: Any, window: float) -> bool:
    """True — the client received messages within the last ``window`` seconds."""
    at = getattr(client, _LAST_MESSAGE, None)
    return isinstance(at, (int, float)) and time.monotonic() - at <= window


def matching_clients(ccxt_exchange: Any, tokens: tuple[str, ...]) -> list[tuple[Any, Any]]:
    """ccxt clients (url, client) whose url/subscriptions/futures contain ``tokens``."""
    if not tokens:
        return []
    clients = getattr(ccxt_exchange, 'clients', None) or {}
    found = []
    for url, client in list(clients.items()):
        keys = [str(url)]
        for attr in ('subscriptions', 'futures'):
            mapping = getattr(client, attr, None) or {}
            keys.extend(k for k in list(mapping.keys()) if isinstance(k, str))
        if any(token in k for k in keys for token in tokens):
            found.append((url, client))
    return found


def force_reconnect(ccxt_exchange: Any, tokens: tuple[str, ...], reason: str,
                    clients: Optional[list[tuple[Any, Any]]] = None) -> int:
    """Closes ccxt clients (WS connections) whose subscriptions contain ``tokens``.

    ``client.on_error`` is the standard ccxt path: it rejects pending futures, removes the client from
    ``exchange.clients`` and closes the socket; the next ``watch_*`` opens a new
    connection and resends subscriptions (for order books — with a fresh snapshot).
    ``clients`` — close only these (from :func:`matching_clients`).
    Returns the number of closed clients.
    """
    closed = 0
    for url, client in (matching_clients(ccxt_exchange, tokens) if clients is None else clients):
        try:
            client.on_error(ccxt.NetworkError(f'bytx: forced reconnect ({reason})'))
            closed += 1
        except Exception as e:  # client already closing — leave it
            logger.debug(f'force_reconnect {url}: {e!r}')
    return closed


class Feed:
    """One ccxt subscription and its consumers."""

    def __init__(self, hub: Any, spec: FeedSpec) -> None:
        self.hub = hub
        self.spec = spec
        self.name = spec.name
        self.stats = FeedStats(_latency=deque(maxlen=hub.config.latency_window))
        self.mailboxes: list[Mailbox] = []
        self.last: Any = None
        self.depth: Optional[int] = None      # max depth among order book subscribers
        self.depths: dict[int, Optional[int]] = {}   # id(mailbox) → subscriber depth
        self.state: dict = {}                 # local spec state (nonce, etc.)
        self._task: Optional[asyncio.Task] = None
        self.error: Optional[BaseException] = None
        self._stale_strikes = 0                # consecutive watchdog triggers without messages
        self._stopping = False                 # stop() cancelled the task (Python 3.10: no Task.cancelling)
        self.started_at = 0.0                  # time.monotonic() of loop start (attach)
        self._ready = False                    # subscription confirmed or data arrived

    # ─── subscribers ─────────────────────────────────────────────────────────

    def attach(self, mailbox: Mailbox) -> None:
        if self.error is not None:
            mailbox.fail(self.error)
            return
        self.mailboxes.append(mailbox)
        self.stats.subscribers = len(self.mailboxes)
        if self.last is not None and self.spec.policy == 'latest':
            mailbox.put(self.last)            # a new consumer gets the current value immediately
        if self._task is None or self._task.done():
            self._stopping = False
            self.started_at = time.monotonic()
            self._task = asyncio.get_running_loop().create_task(self._run(), name=f'bytx:{self.name}')

    @property
    def ready(self) -> bool:
        """The subscription is active: data arrived; Bybit/Bitget — the server confirmed a subscription
        sent after the stream started; others — all WS connections of the instance are open."""
        if self._ready or self.stats.messages:
            self._ready = True
            return True
        if not self.running or not self._connection_ready(self.started_at):
            return False
        self._ready = True
        return True

    def _connection_ready(self, since: float) -> bool:
        """The instance's WS connections are open and subscriptions sent after ``since`` are confirmed."""
        ccxt_ex = self.hub.ccxt
        clients = list((getattr(ccxt_ex, 'clients', None) or {}).values())
        if not clients or not all(_client_connected(c) for c in clients):
            return False
        subs = sub_state(ccxt_ex)
        if subs is not None:
            if subs.pending or subs.last_sent < since or subs.last_ack < subs.last_sent:
                return False
        return True

    async def wait_ready(self, timeout: float) -> bool:
        """Waits for :attr:`ready` up to ``timeout`` seconds; False — not ready in time (stream not stopped)."""
        deadline = time.monotonic() + timeout
        while not self.ready:
            if self.error is not None or (self._task is not None and self._task.done()):
                return False
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    def detach(self, mailbox: Mailbox) -> None:
        if mailbox in self.mailboxes:
            self.mailboxes.remove(mailbox)
        mailbox.close()
        self.stats.subscribers = len(self.mailboxes)
        if not self.mailboxes:
            self.stop()

    def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._stopping = True
            self._task.cancel()
        if self.spec.close is not None:
            try:
                self.spec.close()
            except Exception as e:  # noqa: BLE001 — release is best effort
                logger.debug(f'{self.hub.name} {self.name}: spec close: {e!r}')
        self.stats.state = 'idle' if self.error is None else 'stopped'
        self.hub._feed_stopped(self)

    async def wait_stopped(self) -> None:
        if self._task is not None:
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ─── loop ───────────────────────────────────────────────────────────────

    async def _next(self, stale: Optional[float]) -> Any:
        if stale is None:
            return await self.spec.watch()
        try:
            if _HAS_TIMEOUT:
                async with asyncio.timeout(stale):  # type: ignore[attr-defined]
                    return await self.spec.watch()
            return await asyncio.wait_for(self.spec.watch(), stale)  # Python 3.10
        except (TimeoutError, asyncio.TimeoutError):
            raise _Stale() from None

    async def _next_announcing(self, stale: Optional[float], since: float, announce: Callable[[], None]) -> Any:
        """Like :meth:`_next`, but while waiting for a message — ``announce()`` as soon as the connection is ready."""
        task = asyncio.ensure_future(self._next(stale))
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=_READY_POLL)
                if done:
                    return task.result()
                if self._connection_ready(since):
                    announce()
                    return await task
        finally:
            if not task.done():
                task.cancel()

    def _publish(self, raw: Any) -> None:
        items = self.spec.convert(raw, self)
        st = self.stats
        st.messages += 1
        st.last_message_at = time.time()
        self._stale_strikes = 0
        if not items:
            return
        if self.spec.check is not None:
            self.spec.check(items, self)
        st.items += len(items)
        ts = self.spec.timestamp(items[-1])
        if ts:
            st.record_latency(self.hub.exchange_now_ms() - ts)
        self.last = items[-1]
        self.hub._on_items(self, items)
        for mb in list(self.mailboxes):
            for item in items:
                mb.put(item)

    async def _run(self) -> None:
        cfg = self.hub.config
        stale = cfg.stale_for(self.spec.kind)
        attempt = 0
        rejections = 0           # consecutive exchange rejections without a single received message
        reconnecting = False
        while True:
            connected_at: Optional[float] = None
            error: BaseException
            reason: Optional[str] = None
            cycle_at = time.monotonic()
            announced = False

            def announce() -> None:
                # "connected" — the first message or (after a drop) an open connection with a
                # confirmed subscription: a private stream is silent until there are events, and without
                # "connected" trackers don't REST-reconcile what was missed during the drop (demo 2026-10-27)
                nonlocal announced, connected_at, rejections, reconnecting
                announced = True
                rejections = 0
                connected_at = time.monotonic()
                self.stats.state = 'connected'
                self.hub._emit('connected', self, attempt=attempt)
                if reconnecting:
                    self.stats.resyncs += 1
                    self.hub._emit('resynced', self, attempt=attempt)
                    reconnecting = False

            try:
                while True:
                    if reconnecting and not announced:
                        raw = await self._next_announcing(stale, cycle_at, announce)
                    else:
                        raw = await self._next(stale)
                    now = time.monotonic()
                    if not announced:
                        announce()
                    elif attempt and now - connected_at >= cfg.stable_after:  # type: ignore[operator]
                        attempt = 0
                    self._publish(raw)
            except asyncio.CancelledError:
                if _own_cancel(self._stopping):
                    raise  # this stream was cancelled (stop/close)
                # the shared ccxt Future was cancelled (one per message_hash across several streams:
                # a sibling stream stopped or its timeout fired) — not our cancellation,
                # so the stream doesn't die silently but resubscribes
                error = NetworkError(f'stream {self.name}: ccxt wait cancelled externally',
                                     exchange=self.hub.name)
            except _Stale:
                self.stats.stale += 1
                self._stale_strikes += 1
                reason = f'no messages for {stale} s'
                error = NetworkError(f'stream {self.name} hung: {reason}', exchange=self.hub.name)
                self.hub._emit('stale', self, attempt=attempt, reason=reason)
                await self._recover_stale(stale)  # type: ignore[arg-type]
            except Resync as e:
                if e.kind == 'crossed':
                    self.stats.crossed += 1
                reason = e.reason
                error = NetworkError(f'stream {self.name}: {e.reason}', exchange=self.hub.name)
                self.hub._emit(e.kind, self, attempt=attempt, reason=reason)
                force_reconnect(self.hub.ccxt, self.spec.tokens, e.reason)
            except Exception as e:
                error = from_ccxt(e, self.hub.name)
                if is_fatal(e) or is_fatal(error):
                    self._fail(error)
                    return
                if connected_at is None and not isinstance(e, ccxt.NetworkError):
                    # the exchange rejects the subscription (not network): endless retries only hide
                    # the cause — after max_rejections in a row the stream stops with an error
                    rejections += 1
                    if cfg.max_rejections is not None and rejections >= cfg.max_rejections:
                        self._fail(error)
                        return
                if self.spec.on_reconnect is not None:
                    # order book: recreate the connection so ccxt takes a fresh snapshot
                    force_reconnect(self.hub.ccxt, self.spec.tokens, 'error')
            if self.spec.on_reconnect is not None:
                self.spec.on_reconnect(self)

            if connected_at is not None and time.monotonic() - connected_at >= cfg.stable_after:
                attempt = 0
            attempt += 1
            reconnecting = True
            st = self.stats
            st.errors += 1
            st.reconnects += 1
            st.last_error = str(error)
            st.state = 'disconnected'
            self.hub._emit('disconnected', self, attempt=attempt, error=error, reason=reason)
            if cfg.max_attempts is not None and attempt > cfg.max_attempts:
                self._fail(error)
                return
            delay = cfg.backoff(attempt)
            st.state = 'reconnecting'
            self.hub._emit('reconnecting', self, attempt=attempt, delay=delay, error=error)
            logger.debug(f'{self.hub.name} {self.name}: reconnect #{attempt} in {delay:.2f} s ({error})')
            await _sleep(delay)

    async def _recover_stale(self, window: float) -> None:
        """Topic silence: drop only silent connections; for a live one — resubscribe the topic.

        A connection is alive if its ccxt client received any messages within ``window``
        (other topics, exchange pongs). Then it must not be closed — all streams on it
        would drop; instead resubscribe this topic (ccxt ``un_watch_*``; the next
        ``watch_*`` subscribes again). If the topic stays silent ``stale_escalate`` times in a row
        with a live connection, the connection is recreated anyway.
        """
        ccxt_ex = self.hub.ccxt
        clients = matching_clients(ccxt_ex, self.spec.tokens)
        silent = [(url, c) for url, c in clients if not client_active(c, window)]
        if silent:
            force_reconnect(ccxt_ex, self.spec.tokens, 'stale', clients=silent)
            return
        if not clients:
            return
        if self._stale_strikes >= self.hub.config.stale_escalate:
            self._stale_strikes = 0
            force_reconnect(ccxt_ex, self.spec.tokens, 'stale', clients=clients)
            return
        if self.spec.unwatch is None:
            return  # no unsubscribe — keep waiting without dropping the connection
        try:
            await asyncio.wait_for(self.spec.unwatch(), window)
        except asyncio.CancelledError:
            if _own_cancel(self._stopping):
                raise
        except Exception as e:  # resubscribe is best effort; the watchdog fires again later
            logger.debug(f'{self.hub.name} {self.name}: topic unsubscribe failed: {e!r}')

    def _fail(self, error: BaseException) -> None:
        self.error = error
        self.stats.state = 'stopped'
        self.stats.last_error = str(error)
        self.hub._emit('stopped', self, error=error)
        logger.warning(f'{self.hub.name} {self.name}: stream stopped: {error}')
        for mb in list(self.mailboxes):
            mb.fail(error)
        self.mailboxes.clear()
        self.stats.subscribers = 0
        self.hub._feed_stopped(self)


__all__ = [
    'StreamConfig', 'StateEvent', 'FeedStats', 'Mailbox', 'Feed', 'FeedSpec', 'Resync',
    'DEFAULT_STALE_AFTER', 'force_reconnect', 'matching_clients', 'client_active',
    'install_activity_hook', 'is_fatal', 'percentile',
]
