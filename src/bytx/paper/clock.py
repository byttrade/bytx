"""Paper-mode clocks: wall clock (live paper) and virtual (replay, tests).

All times are milliseconds (int). The paper engine takes time only from the clock: on a virtual
clock fills do not depend on machine speed or real time.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from typing import Protocol

from bytx.errors import ConfigError


class Clock(Protocol):
    """Paper-mode time source."""

    def now_ms(self) -> int: ...

    async def sleep(self, seconds: float) -> None: ...


class WallClock:
    """Real time (live paper against the exchange order book)."""

    virtual = False

    def now_ms(self) -> int:
        return int(time.time() * 1000)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class VirtualClock:
    """Virtual time: moves only explicitly (:meth:`advance` / :meth:`advance_to`) or via the
    replay driver. ``await clock.sleep(s)`` waits until time reaches ``now + s`` (the replay driver
    wakes sleepers in deadline order; without a driver — until someone moves the clock). Time never
    goes backwards: ``advance_to`` into the past is a no-op."""

    virtual = True

    def __init__(self, start_ms: int = 0) -> None:
        self._now = int(start_ms)
        self._waiters: list[tuple[int, int, asyncio.Future]] = []
        self._seq = itertools.count()

    def now_ms(self) -> int:
        return self._now

    def advance(self, ms: int) -> int:
        if ms < 0:
            raise ConfigError('VirtualClock.advance: ms >= 0')
        return self.advance_to(self._now + int(ms))

    def advance_to(self, ms: int) -> int:
        """Moves time forward and wakes ``sleep`` calls due at ≤ ``ms``. Returns the current time."""
        if ms > self._now:
            self._now = int(ms)
        self._wake(self._now)
        return self._now

    def next_wakeup(self) -> int | None:
        """Nearest ``sleep`` deadline (ms) or None."""
        while self._waiters and self._waiters[0][2].done():
            heapq.heappop(self._waiters)
        return self._waiters[0][0] if self._waiters else None

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (self._now + int(round(seconds * 1000)), next(self._seq), fut))
        await fut

    def _wake(self, until: int) -> None:
        while self._waiters and self._waiters[0][0] <= until:
            _, _, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)


__all__ = ['Clock', 'WallClock', 'VirtualClock']
