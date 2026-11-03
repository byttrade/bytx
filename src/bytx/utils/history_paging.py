"""
Shared primitives for paged loading of position/trade history (Bybit, Hyperliquid).

- HistoryBudget — one per top-level fetch_position_history / fetch_position_history_v2 call.
  Checked between pages via time.monotonic: asyncio.wait_for is not used,
  there are no internal retries. An error on a page = stop with complete=False.
- get_history_semaphore — process-wide semaphore per exchange. Each bot has its own
  ccxt client (and its own rate limiter), while the Hyperliquid weight limit is per IP, so
  history request concurrency is limited here. Semaphore wait time
  counts toward the budget deadline (take() is called after acquiring).
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from loguru import logger

DAY_MS = 86_400_000

# Bybit v5 position/closed-pnl and execution/list: endTime - startTime <= 7 days, limit <= 100,
# data available no deeper than 2 years.
_BYBIT_WINDOW_MS = 7 * DAY_MS - 1
_BYBIT_LIMIT = 100
_BYBIT_MAX_AGE_MS = 730 * DAY_MS

# Hyperliquid userFillsByTime: at most 2000 fills per response (across all account coins),
# only the latest 10000 fills are available.
_HL_PAGE = 2000
_HL_HORIZON = 10000

# How far back from the close to look for the position open when reconstructing Bybit.
_RECON_MAX_LOOKBACK_MS = 90 * DAY_MS

# Default budget: (max_calls, deadline_s).
_DEFAULT_BUDGETS = {
    'bybit': (40, 25.0),
    'hyperliquid': (6, 20.0),
}
_FALLBACK_BUDGET = (40, 25.0)

# History request concurrency per process.
_DEFAULT_CONCURRENCY = {
    'bybit': 2,
    'hyperliquid': 1,
}

# exchange_name -> (event loop, Semaphore). The semaphore is bound to the loop, so on a
# loop change (tests, restart) it is recreated.
_SEMAPHORES: dict[str, tuple[Any, asyncio.Semaphore]] = {}


@dataclass
class HistoryBudget:
    """Budget of one top-level history load call."""

    max_calls: int
    deadline_s: float
    started: float = field(default_factory=lambda: time.monotonic())
    calls: int = 0
    incomplete: bool = False
    reason: str | None = None

    def take(self) -> bool:
        """Allow one more request. False — budget exhausted (reason: max_calls/deadline)."""
        if self.calls >= self.max_calls:
            self.mark_incomplete('max_calls')
            return False
        if time.monotonic() - self.started >= self.deadline_s:
            self.mark_incomplete('deadline')
            return False
        self.calls += 1
        return True

    def mark_incomplete(self, reason: str) -> None:
        """Mark the result incomplete; the first reason is kept."""
        self.incomplete = True
        if self.reason is None:
            self.reason = reason

    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self.started) * 1000)


def new_budget(exchange_name: str, max_calls: int | None = None,
               deadline_s: float | None = None) -> HistoryBudget:
    """Default budget for an exchange, with optional override."""
    d_calls, d_deadline = _DEFAULT_BUDGETS.get(str(exchange_name).lower(), _FALLBACK_BUDGET)
    return HistoryBudget(
        max_calls=d_calls if max_calls is None else int(max_calls),
        deadline_s=d_deadline if deadline_s is None else float(deadline_s),
    )


def get_history_semaphore(exchange_name: str, n: int | None = None) -> asyncio.Semaphore:
    """Process-wide history request semaphore for an exchange."""
    name = str(exchange_name).lower()
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    cached = _SEMAPHORES.get(name)
    if cached is not None and cached[0] is loop:
        return cached[1]
    size = n if n is not None else _DEFAULT_CONCURRENCY.get(name, 2)
    sem = asyncio.Semaphore(max(1, int(size)))
    _SEMAPHORES[name] = (loop, sem)
    return sem


def dedup(items: Iterable, key_fn: Callable[[Any], Any]) -> list:
    """Remove duplicates by key, keeping the first occurrence and order."""
    seen = set()
    out = []
    for item in items:
        key = key_fn(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _iso(ts: int | None) -> str:
    if ts is None:
        return '-'
    try:
        return datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
    except Exception:
        return str(ts)


def log_fetch(bot_id, exchange: str, symbol: str, pages: int, items: int, elapsed_ms: int,
              complete: bool, reason: str | None, reached_ts: int | None) -> None:
    """Paged fetch summary: debug when complete, warning when incomplete."""
    msg = (
        f"[{bot_id}]\t{exchange} history {symbol}: pages={pages}, items={items}, "
        f"elapsed={elapsed_ms}ms"
    )
    if complete:
        logger.bind(bot_id=bot_id).debug(msg)
    else:
        logger.bind(bot_id=bot_id).warning(
            f"{msg} — ⚠️ history incomplete (reason={reason}), reached {_iso(reached_ts)}"
        )
