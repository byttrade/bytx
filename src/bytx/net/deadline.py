"""Shared call deadline: one time budget for the whole method.

``with ex.deadline(5.0): await ex.place_order(...)`` — requests, retries, pauses between them and
order reconciliation fit into 5 s. Stored in a contextvar: applies to the task and its
child tasks (``asyncio.gather``); a nested deadline cannot extend the outer one.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from contextvars import ContextVar
from typing import Iterator, Optional

_DEADLINE: ContextVar[Optional[float]] = ContextVar('bytx_deadline', default=None)


@contextlib.contextmanager
def deadline(seconds: float) -> Iterator[None]:
    """Deadline for everything executed inside the block (``time.monotonic`` + seconds)."""
    if seconds is None or seconds <= 0:
        from bytx.errors import ConfigError
        raise ConfigError(f'deadline: seconds > 0, not {seconds!r}')
    until = time.monotonic() + float(seconds)
    outer = _DEADLINE.get()
    token = _DEADLINE.set(until if outer is None else min(outer, until))
    try:
        yield
    finally:
        _DEADLINE.reset(token)


def deadline_left() -> Optional[float]:
    """Seconds left until the deadline (may be ≤ 0); None — no deadline."""
    until = _DEADLINE.get()
    return None if until is None else until - time.monotonic()


async def within_deadline(awaitable):  # type: ignore[no-untyped-def]
    """Awaits ``awaitable`` no longer than the remaining deadline (no deadline — as is)."""
    left = deadline_left()
    if left is None:
        return await awaitable
    if left <= 0:
        close = getattr(awaitable, 'close', None)
        if close is not None:
            close()  # coroutine was never started — avoids "never awaited"
        raise asyncio.TimeoutError('call deadline exceeded')
    return await asyncio.wait_for(awaitable, left)


__all__ = ['deadline', 'deadline_left', 'within_deadline']
