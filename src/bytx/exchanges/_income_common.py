"""Shared by exchange funds journals (``exchanges/<exchange>/portfolio.py``).

Number parsing, pagination guard and history depth are the same as in
``fetch_funding_paid`` (:mod:`bytx.exchanges._funding_history`): one rule set for the SDK.
"""
from __future__ import annotations

import time
from typing import Any, Callable, Optional

from bytx.exchanges._funding_history import _DAY_MS, _MAX_PAGES, _int, _num
from bytx.portfolio import IncomeItem, IncomeKind, IncomeResult

DAY_MS = _DAY_MS
MAX_PAGES = _MAX_PAGES   # per call (for windowed exchanges — per window)

num: Callable[[Any], Optional[float]] = _num
integer: Callable[[Any], Optional[int]] = _int


#: journal clock (tests swap in a virtual one)
clock: Callable[[], float] = time.time


def now_ms() -> int:
    return int(clock() * 1000)


def check_depth(result: IncomeResult, since: int, days: int, what: str) -> None:
    """The window starts deeper than the exchange keeps (per docs) — data is incomplete."""
    if since < now_ms() - days * DAY_MS:
        result.incomplete(f'{what}: window deeper than exchange history ({days} days per docs)')


class Pages:
    """Page counter: more than :data:`MAX_PAGES` — history is truncated."""

    __slots__ = ('count', 'limit', 'what')

    def __init__(self, what: str, limit: int = MAX_PAGES) -> None:
        self.count = 0
        self.limit = limit
        self.what = what

    def next(self, result: IncomeResult) -> bool:
        """True — another page may be requested; False — limit reached, result marked incomplete."""
        self.count += 1
        if self.count > self.limit:
            result.incomplete(f'{self.what}: more than {self.limit} pages — history truncated')
            return False
        return True


def add(result: IncomeResult, seen: set, key: Any, *, ts: Any, kind: IncomeKind, amount: Any,
        currency: Any, since: int, until: int, symbol: Any = None) -> bool:
    """Adds a record (duplicates by ``key`` and records outside the window are dropped); True — new record."""
    t = integer(ts)
    value = num(amount)
    if t is None or value is None or not (since <= t <= until):
        return False
    if key is not None:
        if key in seen:
            return False
        seen.add(key)
    if value == 0:
        return True
    result.items.append(IncomeItem(ts=t, kind=kind, amount=value, currency=str(currency or '').upper(),
                                   symbol=str(symbol) if symbol not in (None, '') else None,
                                   id=str(key) if key is not None else None))
    return True
