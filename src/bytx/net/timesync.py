"""Clock synchronization with the exchange.

Binance/Bybit/OKX/KuCoin/BingX signatures use ``milliseconds() - options['timeDifference']``;
ccxt fills the offset in ``load_time_difference()`` (a ``fetch_time`` request). Here:
one-off sync, coalescing of concurrent resyncs (a burst of TimestampError → one
request) and an optional background resync every ``interval`` seconds.
Hyperliquid does not return time (``has['fetchTime'] = False``) — sync is skipped.

Clock offset is a property of the host (and channel), not the account: instances with the same key
(exchange + network + market type + limiter key) share one offset. A neighbor's fresh result
is copied into ``options['timeDifference']`` without a request: on connect() if it is not older than
:data:`SHARED_MAX_AGE_SEC`, in the background if not older than ~interval (one request per key per interval).
"""
from __future__ import annotations

import asyncio
import time
import weakref
from typing import Any, Optional

from loguru import logger

from bytx.errors import ConfigError

MIN_GAP_SEC = 1.0  # no need to resync more than once a second: coalesce error bursts
SHARED_MAX_AGE_SEC = 60.0  # connect(): a neighbor's offset this old is still good


class _State:
    """Clock offset per key (shared by instances) or per single instance."""

    __slots__ = ('offset_ms', 'last_sync', 'last_mono', 'lock')

    def __init__(self) -> None:
        self.offset_ms: Optional[int] = None
        self.last_sync: Optional[float] = None
        self.last_mono: float = float('-inf')
        self.lock = asyncio.Lock()


# loop → key → shared state (the lock is bound to the loop)
_shared: 'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[Any, _State]]' = \
    weakref.WeakKeyDictionary()


def reset_shared() -> None:
    """Resets shared offsets (for tests)."""
    _shared.clear()


class TimeSync:
    """Clock offset of one ccxt instance."""

    def __init__(self, exchange: Any, interval: Optional[float] = None, *, key: Any = None) -> None:
        if interval is not None and interval <= 0:
            raise ConfigError('time_sync_interval: seconds > 0 or None')
        self._ex = exchange
        self.interval = interval
        self.key = key  # None — offset of this instance only
        self.offset_ms: Optional[int] = None
        self.last_sync: Optional[float] = None  # unix time of the last successful sync
        self._own: Optional[_State] = None
        self._task: Optional[asyncio.Task] = None

    def _state(self) -> _State:
        if self.key is None:
            if self._own is None:
                self._own = _State()
            return self._own
        by_key = _shared.get(asyncio.get_running_loop())
        if by_key is None:
            by_key = _shared[asyncio.get_running_loop()] = {}
        st = by_key.get(self.key)
        if st is None:
            st = by_key[self.key] = _State()
        return st

    def _adopt(self, st: _State) -> Optional[int]:
        """Takes the offset from shared state (including into ccxt ``options['timeDifference']``)."""
        if st.offset_ms is not None:
            self._ex.options['timeDifference'] = st.offset_ms
        self.offset_ms = st.offset_ms
        self.last_sync = st.last_sync
        return self.offset_ms

    @property
    def supported(self) -> bool:
        return bool(self._ex.has.get('fetchTime')) and self._ex.id != 'hyperliquid'

    async def sync(self, *, force: bool = False, max_age: Optional[float] = None) -> Optional[int]:
        """Synchronizes the clock; returns the offset in ms (local − exchange) or None.

        Without ``force``, a result obtained (by this or a sibling instance of the key) no earlier
        than ``max_age`` seconds before the call (default :data:`MIN_GAP_SEC`) is used without a request.
        """
        if not self.supported:
            return None
        st = self._state()
        started = time.monotonic()
        window = MIN_GAP_SEC if max_age is None else max_age
        async with st.lock:
            # while we waited for the lock, another call already synced — no need to repeat
            if not force and st.last_mono >= started - window:
                return self._adopt(st)
            offset = await self._ex.load_time_difference()
            # integer: ccxt subtracts the offset from milliseconds(), and a fraction yields timestamp '…722.0'
            # (Binance -1100, Bybit 10001, Bitget 40008 on demo 2026-10-26)
            st.offset_ms = int(round(offset)) if offset is not None else None
            st.last_sync = time.time()
            st.last_mono = time.monotonic()
            return self._adopt(st)

    def start(self) -> None:
        """Starts the background resync (if interval is set and the exchange supports it)."""
        if self.interval is None or not self.supported:
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.get_running_loop().create_task(self._run(), name='bytx-time-sync')

    async def _run(self) -> None:
        assert self.interval is not None
        while True:
            await asyncio.sleep(self.interval)
            try:
                # key siblings with the same interval: one makes the request, the others copy
                await self.sync(max_age=self.interval * 0.9)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # the background task never dies: next attempt after interval
                logger.debug('bytx: background time sync {} failed: {}', self._ex.id, e)

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


__all__ = ['SHARED_MAX_AGE_SEC', 'TimeSync', 'reset_shared']
