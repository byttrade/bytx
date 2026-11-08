"""Retry policy: exponential backoff + full jitter, only for safe calls.

Safe (idempotent) calls are market data, account reads, cancel by id,
settings (leverage, mode). They are retried on network errors (the
:class:`~bytx.errors.NetworkError` family): timeout, unavailability, 429 (honoring
``retry_after``; 418 — IP ban — is not retried). :class:`~bytx.errors.TimestampError` —
clock resync and one retry. The pause after 429/418 is also held by the shared limiter (bytx.net.limiter).

Order placement and amendment (``create_order``, ``create_orders``, ``edit_order``) and
transfers are **never** retried automatically: on timeout the order may have been accepted,
and a retry would create a duplicate. Such errors go to the caller (``Exchange.place_order`` reconciles
the order by ``client_id``).
"""
from __future__ import annotations

import asyncio
import random
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional, TypeVar

import ccxt
from loguru import logger

from bytx.errors import BytxError, ConfigError, RateLimited, TimestampError, ccxt_secrets, from_ccxt
from bytx.net.deadline import deadline_left, within_deadline

T = TypeVar('T')

# ccxt methods that are never retried, even if the call is marked safe=True.
UNSAFE_METHODS = frozenset({
    'create_order', 'create_orders', 'edit_order', 'edit_orders',
    'create_order_ws', 'create_orders_ws', 'edit_order_ws',
    'create_market_order', 'create_limit_order', 'create_market_buy_order',
    'create_market_sell_order', 'create_limit_buy_order', 'create_limit_sell_order',
    'create_market_order_with_cost', 'create_market_buy_order_with_cost',
    'create_market_sell_order_with_cost', 'create_stop_order', 'create_trigger_order',
    'create_reduce_only_order', 'create_post_only_order', 'create_order_with_take_profit_and_stop_loss',
    'close_position', 'close_all_positions', 'transfer', 'withdraw',
    # conditional order placement via the implicit API (bytx.protection)
    '_binance_algo_create', 'private_post_trade_order_algo', 'privatePostTradeOrderAlgo',
    'privatePostV5PositionTradingStop', 'private_post_v5_position_trading_stop',
})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times and how to retry safe calls.

    retries — number of retries (0 — none); base / max_delay — backoff seconds;
    max_retry_after — if the exchange asks to wait longer, the error is returned immediately.
    """

    retries: int = 3
    base: float = 0.2
    max_delay: float = 5.0
    max_retry_after: float = 30.0

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ConfigError('retries: >= 0')
        if self.base <= 0 or self.max_delay < self.base:
            raise ConfigError('backoff: (base > 0, max >= base) in seconds')

    @classmethod
    def build(cls, retries: int = 3, backoff: tuple[float, float] = (0.2, 5.0)) -> 'RetryPolicy':
        try:
            base, max_delay = backoff
        except (TypeError, ValueError):
            raise ConfigError('backoff: tuple (base, max) in seconds') from None
        return cls(int(retries), float(base), float(max_delay))

    def backoff(self, attempt: int) -> float:
        """Full jitter: random pause in [0, min(max, base·2^attempt)]."""
        cap = min(self.max_delay, self.base * (2 ** attempt))
        return random.uniform(0.0, cap)

    def delay(self, attempt: int, error: BytxError, hint: Optional[float] = None) -> Optional[float]:
        """Pause before a retry; None — do not retry (the exchange asks to wait too long)."""
        pause = self.backoff(attempt)
        if isinstance(error, RateLimited):
            retry_after = error.retry_after if error.retry_after is not None else hint
            if retry_after is not None:
                if retry_after > self.max_retry_after:
                    return None
                pause = max(pause, float(retry_after))
        return pause


_HTTP_418 = re.compile(r'(?<![\d.])418(?![\d.])')


def is_ip_ban(error: BaseException) -> bool:
    """HTTP 418 — IP ban (Binance: "auto-banned" after 429, from 2 minutes to 3 days): do not retry."""
    return isinstance(error, ccxt.DDoSProtection) and bool(_HTTP_418.search(str(error)))


def is_ban(error: BaseException) -> bool:
    """An exchange ban — retrying is pointless until it ends: HTTP 418 (IP ban), Bybit HTTP 403 «access too
    frequent» (IP ban), BingX 100410 («disabled period ... unblocked after»)."""
    if is_ip_ban(error):
        return True
    if not isinstance(error, ccxt.RateLimitExceeded):
        return False
    text = str(error)
    return 'access too frequent' in text.lower() or '100410' in text


def _ban_left(fn: Any) -> Optional[float]:
    """The rest of the ban the limiter set from a response (the instance's ``_bytx_ban_until``), s."""
    import time
    until = getattr(getattr(fn, '__self__', None), '_bytx_ban_until', None)
    if not isinstance(until, (int, float)):
        return None
    return max(0.0, until - time.monotonic())


def is_unsafe(fn: Any) -> bool:
    return getattr(fn, '__name__', '') in UNSAFE_METHODS


async def call_with_retry(fn: Callable[..., Awaitable[T]], args: tuple, kwargs: dict, *,
                          policy: RetryPolicy, safe: bool, exchange: Optional[str] = None,
                          resync: Optional[Callable[[], Awaitable[Any]]] = None,
                          metrics: Any = None) -> T:
    """Calls ``fn``, converting ccxt errors into SDK errors, with retries for safe calls.

    Errors already converted to :class:`BytxError` inside ``fn`` are not retried
    (the nested layer already retried them) — so retries do not multiply.
    """
    retryable_call = safe and not is_unsafe(fn)
    attempt = 0
    resynced = False
    while True:
        try:
            return await within_deadline(fn(*args, **kwargs))
        except BytxError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = from_ccxt(e, exchange, secrets=ccxt_secrets(getattr(fn, '__self__', None)))
            banned = is_ban(e)
            if isinstance(err, RateLimited) and err.retry_after is None and banned:
                err.retry_after = _ban_left(fn)       # rest of the ban (Bybit 403 — 600 s, BingX — until unblocked)
            if isinstance(err, RateLimited) and err.retry_after is None and metrics is not None:
                # Retry-After from the response headers — to the caller, not only to the retry pause
                err.retry_after = getattr(metrics, 'retry_after_hint', None)
            if isinstance(err, TimestampError):
                if resynced or resync is None:
                    raise err from e
                resynced = True
                try:
                    await resync()
                except Exception as sync_error:  # could not sync — raise the original
                    logger.debug('bytx: time resync {} failed: {}', exchange, sync_error)
                    raise err from e
                if not retryable_call:
                    raise err from e  # signature rejected, but orders are never retried
                if metrics is not None:
                    metrics.retries += 1
                continue  # retry immediately, outside the retries budget
            if not retryable_call or not err.retryable or attempt >= policy.retries or banned:
                raise err from e
            pause = policy.delay(attempt, err, getattr(metrics, 'retry_after_hint', None)
                                 if isinstance(err, RateLimited) else None)
            left = deadline_left()
            if pause is None or (left is not None and pause >= left):
                raise err from e  # a retry would not finish before the overall call deadline
            attempt += 1
            if metrics is not None:
                metrics.retries += 1
            logger.debug('bytx: {} {} — retry {}/{} in {:.3f}s', exchange,
                         type(err).__name__, attempt, policy.retries, pause)
            await asyncio.sleep(pause)


__all__ = ['RetryPolicy', 'UNSAFE_METHODS', 'call_with_retry', 'is_ban', 'is_ip_ban', 'is_unsafe']
