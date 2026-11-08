"""REST transport metrics: per-endpoint latency and exchange rate-limit headers.

The ccxt instance is instrumented at three levels:

- ``fetch2`` — stores the endpoint template (``GET fapiPublic:ticker/price``) in a contextvar,
  so order ids in the path don't multiply endpoints;
- ``fetch`` — measures network time only (limiter wait excluded);
- ``on_rest_response`` — used rate-limit headers (Binance
  ``X-MBX-USED-WEIGHT-1m``, Bybit ``X-Bapi-Limit-Status``, KuCoin ``gw-ratelimit-*``).

Per call: a few arithmetic operations and a write into a fixed-size
ring buffer; percentiles are computed only in :meth:`Metrics.snapshot`.
"""
from __future__ import annotations

import asyncio
import contextvars
import math
import time
from dataclasses import dataclass, field
from typing import Any, Optional

WINDOW = 512  # last N samples per endpoint for percentiles

_ENDPOINT: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('bytx_endpoint', default=None)
#: request instrument (OKX ``instId``): OKX trading endpoints are limited per «User ID + Instrument ID»
_INSTRUMENT: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('bytx_instrument', default=None)

# Rate-limit header prefixes (lowercase).
_RATE_LIMIT_HEADERS = (
    'x-mbx-used-weight',   # binance: weight per interval
    'x-mbx-order-count',   # binance: order count per interval
    'x-sapi-used',         # binance sapi
    'x-bapi-limit',        # bybit: X-Bapi-Limit, -Status, -Reset-Timestamp
    'gw-ratelimit',        # kucoin
    'x-ratelimit',         # generic format
    'retry-after',
)


@dataclass(frozen=True, slots=True)
class EndpointMetrics:
    """Latency of one endpoint (over the last ``WINDOW`` samples)."""

    endpoint: str
    count: int
    errors: int
    p50_ms: float
    p90_ms: float
    p99_ms: float
    avg_ms: float
    max_ms: float


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """Transport metrics snapshot of one account."""

    requests: int
    errors: int
    retries: int
    reconciled: int
    last_latency_ms: Optional[float]
    endpoints: dict[str, EndpointMetrics] = field(default_factory=dict)
    rate_limit: dict[str, str] = field(default_factory=dict)
    time_offset_ms: Optional[float] = None
    last_time_sync: Optional[float] = None

    @property
    def used_weight(self) -> Optional[int]:
        """Binance: used weight per minute (``X-MBX-USED-WEIGHT-1m``), if the exchange sent it."""
        value = self.rate_limit.get('x-mbx-used-weight-1m')
        try:
            return int(value) if value is not None else None
        except ValueError:
            return None


class _Stat:
    __slots__ = ('count', 'errors', 'total_ms', 'max_ms', 'ring', 'pos')

    def __init__(self) -> None:
        self.count = 0
        self.errors = 0
        self.total_ms = 0.0
        self.max_ms = 0.0
        self.ring = [0.0] * WINDOW
        self.pos = 0

    def add(self, ms: float, ok: bool) -> None:
        self.ring[self.pos % WINDOW] = ms
        self.pos += 1
        self.count += 1
        self.total_ms += ms
        if ms > self.max_ms:
            self.max_ms = ms
        if not ok:
            self.errors += 1

    def snapshot(self, endpoint: str) -> EndpointMetrics:
        n = min(self.pos, WINDOW)
        window = sorted(self.ring[:n])

        def pct(q: float) -> float:
            if not window:
                return 0.0
            return window[min(n - 1, max(0, math.ceil(q * n) - 1))]  # nearest-rank

        avg = self.total_ms / self.count if self.count else 0.0
        return EndpointMetrics(endpoint, self.count, self.errors, pct(0.5), pct(0.9), pct(0.99),
                               avg, self.max_ms)


class Metrics:
    """Metrics accumulator (one per ccxt instance)."""

    __slots__ = ('_stats', 'rate_limit', 'last_latency_ms', 'requests', 'errors', 'retries',
                 'reconciled', 'retry_after_hint')

    def __init__(self) -> None:
        self._stats: dict[str, _Stat] = {}
        self.rate_limit: dict[str, str] = {}
        self.last_latency_ms: Optional[float] = None
        self.requests = 0
        self.errors = 0
        self.retries = 0
        self.reconciled = 0
        self.retry_after_hint: Optional[float] = None

    def record(self, endpoint: str, ms: float, ok: bool = True) -> None:
        stat = self._stats.get(endpoint)
        if stat is None:
            stat = self._stats[endpoint] = _Stat()
        stat.add(ms, ok)
        self.last_latency_ms = ms
        self.requests += 1
        if not ok:
            self.errors += 1

    def observe_headers(self, headers: Any) -> None:
        """Stores the latest rate-limit header values from the response."""
        self.retry_after_hint = None  # hint from the latest response only
        if not headers:
            return
        for name, value in headers.items():
            low = name.lower()
            if low.startswith(_RATE_LIMIT_HEADERS):
                self.rate_limit[low] = value
                if low == 'retry-after':
                    try:
                        self.retry_after_hint = float(value)
                    except (TypeError, ValueError):
                        pass

    def reset(self) -> None:
        self._stats.clear()
        self.rate_limit.clear()
        self.last_latency_ms = None
        self.requests = self.errors = self.retries = self.reconciled = 0
        self.retry_after_hint = None

    def snapshot(self, *, time_offset_ms: Optional[float] = None,
                 last_time_sync: Optional[float] = None) -> MetricsSnapshot:
        return MetricsSnapshot(
            requests=self.requests, errors=self.errors, retries=self.retries,
            reconciled=self.reconciled, last_latency_ms=self.last_latency_ms,
            endpoints={k: s.snapshot(k) for k, s in self._stats.items()},
            rate_limit=dict(self.rate_limit), time_offset_ms=time_offset_ms,
            last_time_sync=last_time_sync)


def endpoint_label(api: Any, method: str, path: str) -> str:
    """Endpoint label from the ccxt template: 'GET fapiPublic:ticker/price'."""
    if isinstance(api, (list, tuple)):
        api = '/'.join(str(a) for a in api)
    return f'{method} {api}:{path}'


def _label_from_url(method: str, url: str) -> str:
    """Fallback label: path without host and query (if the request bypassed fetch2)."""
    rest = url.split('://', 1)[-1]
    slash = rest.find('/')
    path = rest[slash:] if slash >= 0 else '/'
    q = path.find('?')
    if q >= 0:
        path = path[:q]
    return f'{method} {path}'


class MeteredFetch:
    """``exchange.fetch`` wrapper that measures latency. The original is in ``inner``."""

    __slots__ = ('inner', 'metrics')

    def __init__(self, inner: Any, metrics: Metrics) -> None:
        self.inner = inner
        self.metrics = metrics

    async def __call__(self, url: str, method: str = 'GET', headers: Any = None, body: Any = None) -> Any:
        started = time.perf_counter()
        try:
            result = await self.inner(url, method, headers, body)
        except asyncio.CancelledError:
            raise
        except BaseException:
            self.metrics.record(_ENDPOINT.get() or _label_from_url(method, url),
                                (time.perf_counter() - started) * 1000.0, False)
            raise
        self.metrics.record(_ENDPOINT.get() or _label_from_url(method, url),
                            (time.perf_counter() - started) * 1000.0, True)
        return result


def instrument(exchange: Any) -> Metrics:
    """Installs metric wrappers on the ccxt instance (idempotent) and returns the accumulator."""
    existing = getattr(exchange, '_bytx_metrics', None)
    if isinstance(existing, Metrics):
        return existing
    metrics = Metrics()
    exchange._bytx_metrics = metrics

    original_fetch2 = exchange.fetch2

    # The fetch2 signature depends on the ccxt version (4.5 dropped headers/body) — pass through as is.
    async def fetch2(path: Any, api: Any = 'public', method: str = 'GET', *args: Any, **kwargs: Any) -> Any:
        params = args[0] if args else kwargs.get('params')
        token = _ENDPOINT.set(endpoint_label(api, method, path))
        inst = _INSTRUMENT.set(params.get('instId') if isinstance(params, dict) else None)
        try:
            return await original_fetch2(path, api, method, *args, **kwargs)
        finally:
            _INSTRUMENT.reset(inst)
            _ENDPOINT.reset(token)

    exchange.fetch2 = fetch2
    exchange.fetch = MeteredFetch(exchange.fetch, metrics)

    original_on_rest_response = exchange.on_rest_response

    def on_rest_response(code: Any, reason: Any, url: Any, method: Any, response_headers: Any,
                         *rest: Any) -> Any:
        try:
            metrics.observe_headers(response_headers)
        except Exception:  # pragma: no cover — metrics never break a request
            pass
        return original_on_rest_response(code, reason, url, method, response_headers, *rest)

    exchange.on_rest_response = on_rest_response
    return metrics


__all__ = ['EndpointMetrics', 'MeteredFetch', 'Metrics', 'MetricsSnapshot', 'WINDOW',
           'endpoint_label', 'instrument']
