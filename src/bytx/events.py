"""Exchange events for integrations: notifications, metrics, webhooks.

``ex.subscribe(callback, kinds=None)``: an event listener for one :class:`~bytx.Exchange`:

======================  ===================================================================
``order.placed``        order placed (REST, WS, batch; ``order``)
``order.rejected``      explicit exchange rejection (``error``; ``data['request']`` is the request without params)
``order.unknown``       outcome unclear (:class:`~bytx.errors.OrderStatusUnknown`)
``order.canceled``      canceled via SDK (``order``; ``data['ours']``: we canceled it, not the exchange earlier)
``tp_sl.placed``        TP/SL legs placed (``data['orders']``)
``protection.failed``   TP/SL not placed after entry (:class:`~bytx.errors.ProtectionNotPlaced`)
``stream.state``        WS stream state (``state``: :class:`~bytx.streams.StateEvent`)
``error``               integration error (webhook etc.), see :meth:`Exchange.emit`
======================  ===================================================================

Hot path: with no listeners, a single empty-list check at the event point. A listener
(sync or async) cannot break trading: exceptions are logged, coroutines go to a
background task. Events carry no secrets: the request is without ``params`` (which may hold signatures and
broker fields), errors are as the SDK raised them.
"""
from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from loguru import logger

#: all event kinds (``kinds`` in :meth:`Exchange.subscribe` is a subset or 'order.'-style prefixes)
KINDS = ('order.placed', 'order.rejected', 'order.unknown', 'order.canceled', 'tp_sl.placed',
         'protection.failed', 'stream.state', 'error')

#: request fields included in the event (no ``params``)
REQUEST_FIELDS = ('symbol', 'type', 'side', 'amount', 'price', 'client_id', 'reduce_only')


@dataclass(frozen=True, slots=True)
class ExchangeEvent:
    """A :class:`~bytx.Exchange` event for ``ex.subscribe`` listeners."""

    kind: str
    exchange: str                     # SDK name: binance, bybit, …
    account: Optional[str]            # account name in AccountPool (if any)
    demo: bool
    paper: bool
    symbol: Optional[str] = None
    order: Any = None                 # :class:`~bytx.types.Order`
    error: Optional[BaseException] = None
    state: Any = None                 # :class:`~bytx.streams.StateEvent`
    data: dict = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    @property
    def mode(self) -> str:
        """'paper' / 'demo' / 'live'."""
        return 'paper' if self.paper else ('demo' if self.demo else 'live')


Listener = Callable[[ExchangeEvent], Any]


def _matches(kind: str, kinds: Optional[frozenset[str]]) -> bool:
    if kinds is None:
        return True
    if kind in kinds:
        return True
    group = kind.split('.', 1)[0]
    return group in kinds or f'{group}.*' in kinds


def check_kinds(kinds: Optional[Iterable[str]]) -> Optional[frozenset[str]]:
    """Normalizes the kinds filter; an unknown kind raises ConfigError (a typo must not silence notifications)."""
    if kinds is None:
        return None
    from bytx.errors import ConfigError
    out = frozenset(kinds.split(',') if isinstance(kinds, str) else kinds)
    groups = {k.split('.', 1)[0] for k in KINDS}
    for k in out:
        if k not in KINDS and k not in groups and k.rstrip('.*') not in groups:
            raise ConfigError(f'unknown event kind {k!r}; available: {", ".join(KINDS)}')
    return out


class Observers:
    """Listener list of one Exchange (empty means no events are created)."""

    __slots__ = ('_items', '_background')

    def __init__(self) -> None:
        self._items: list[tuple[Listener, Optional[frozenset[str]]]] = []
        self._background: set[asyncio.Task] = set()

    def __bool__(self) -> bool:
        return bool(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def add(self, callback: Listener, kinds: Optional[frozenset[str]]) -> Callable[[], None]:
        entry = (callback, kinds)
        self._items.append(entry)

        def remove() -> None:
            if entry in self._items:
                self._items.remove(entry)

        return remove

    def wants(self, kind: str) -> bool:
        return any(_matches(kind, kinds) for _cb, kinds in self._items)

    def dispatch(self, event: ExchangeEvent) -> None:
        for cb, kinds in list(self._items):
            if not _matches(event.kind, kinds):
                continue
            try:
                result = cb(event)
            except Exception as e:
                logger.warning(f'event listener {event.kind} failed: {type(e).__name__}: {e}')
                continue
            if inspect.isawaitable(result):
                try:
                    task = asyncio.ensure_future(result)
                except RuntimeError:          # no loop: close the coroutine to avoid a warning
                    if inspect.iscoroutine(result):
                        result.close()
                    continue
                self._background.add(task)
                task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task) -> None:
        self._background.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.warning(f'event listener failed: {type(task.exception()).__name__}: {task.exception()}')


def request_view(request: Optional[dict]) -> dict:
    """Order request for an event, without ``params`` (signatures, broker fields)."""
    if not request:
        return {}
    return {k: request.get(k) for k in REQUEST_FIELDS}


__all__ = ['ExchangeEvent', 'KINDS', 'Listener', 'Observers', 'check_kinds', 'request_view']
