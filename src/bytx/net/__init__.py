"""SDK network layer: shared per-IP limiter, retries, latency metrics, time sync.

:class:`NetLayer` bundles everything for one ccxt instance; ``Exchange`` keeps it in ``_net``
and routes every ccxt call through it (``Exchange._call``).
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Optional, TypeVar

from bytx.markets import cache_key
from bytx.net.deadline import deadline, deadline_left, within_deadline
from bytx.net.limiter import RateCap, TokenBucket, install_limiter, limiter_key, reset_registry, shared_throttler
from bytx.net.metrics import EndpointMetrics, MeteredFetch, Metrics, MetricsSnapshot, instrument
from bytx.net.retry import UNSAFE_METHODS, RetryPolicy, call_with_retry, is_ip_ban, is_unsafe
from bytx.net.timesync import TimeSync

T = TypeVar('T')


class NetLayer:
    """Limiter, retry policy, metrics and time sync of one ccxt instance."""

    def __init__(self, exchange: Any, *, proxy: Any = None, rate_limit_key: Optional[str] = None,
                 rate_limit: Optional[float] = None, retries: int = 3,
                 backoff: tuple[float, float] = (0.2, 5.0),
                 time_sync_interval: Optional[float] = None) -> None:
        self._ex = exchange
        self._proxy = proxy
        self.metrics: Metrics = instrument(exchange)
        self.policy = RetryPolicy.build(retries, backoff)
        self.timesync = TimeSync(exchange, time_sync_interval)
        self.limiter_key = ''
        self.rate_limit: Optional[float] = None
        self.set_limiter(rate_limit_key, rate_limit)

    def set_limiter(self, rate_limit_key: Optional[str] = None, rate_limit: Optional[float] = None) -> None:
        """Shared limiter key (exchange + proxy by default) and the account's requests/sec ceiling."""
        self.limiter_key = rate_limit_key or limiter_key(self._ex.id, self._proxy)
        self.rate_limit = rate_limit
        install_limiter(self._ex, self.limiter_key, rate_limit)
        # clock offset is shared per (exchange, network, market type, channel)
        self.timesync.key = (cache_key(self._ex), self.limiter_key)

    def configure(self, *, rate_limit_key: Optional[str] = None, rate_limit: Optional[float] = None,
                  retries: int = 3, backoff: tuple[float, float] = (0.2, 5.0),
                  time_sync_interval: Optional[float] = None) -> 'NetLayer':
        self.policy = RetryPolicy.build(retries, backoff)
        self.timesync = TimeSync(self._ex, time_sync_interval)
        self.set_limiter(rate_limit_key, rate_limit)
        return self

    async def call(self, fn: Callable[..., Awaitable[T]], args: tuple, kwargs: dict, *,
                   safe: bool = True, exchange: Optional[str] = None) -> T:
        return await call_with_retry(fn, args, kwargs, policy=self.policy, safe=safe,
                                     exchange=exchange, resync=self.resync_time, metrics=self.metrics)

    async def resync_time(self) -> Optional[float]:
        return await self.timesync.sync()

    def snapshot(self) -> MetricsSnapshot:
        return self.metrics.snapshot(time_offset_ms=self.timesync.offset_ms,
                                     last_time_sync=self.timesync.last_sync)

    async def close(self) -> None:
        await self.timesync.stop()


__all__ = [
    'deadline', 'deadline_left', 'within_deadline',
    'EndpointMetrics', 'MeteredFetch', 'Metrics', 'MetricsSnapshot', 'NetLayer', 'RateCap',
    'RetryPolicy', 'TimeSync', 'UNSAFE_METHODS', 'call_with_retry', 'install_limiter', 'instrument',
    'is_ip_ban', 'is_unsafe', 'limiter_key', 'reset_registry', 'shared_throttler', 'TokenBucket',
]
