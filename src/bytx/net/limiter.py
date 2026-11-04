"""Shared request limiter for accounts behind one external IP.

Each async ccxt instance has its own ``Throttler`` (``exchange.throttler``,
ccxt/async_support/base/throttler.py): ``fetch2`` calls ``await self.throttle(cost)``
before a request. Exchange limits are per IP, so a hundred accounts behind
one proxy with their own limiters easily trigger 429/418.

Here the instance's ``throttle`` is replaced: all instances with the same key
(exchange + proxy or ``direct``) in one event loop share one :class:`TokenBucket`.
The key can be set manually (``rate_limit_key``), e.g. if different proxies
exit through one IP. Additionally — an optional requests-per-second ceiling
per account (``rate_limit``).

Two levels (per exchange docs, see :data:`ACCOUNT_ENDPOINTS`): endpoints whose limit the
exchange counts per account (Binance USDⓈ-M orders — «0 on IP rate limit», OKX private and
KuCoin — «by User ID», Bybit orders — «per UID») go through the account limiter (key IP +
account) and deduct only their own IP weight (often 0) from the shared IP limiter. Orders of different
accounts no longer share one queue.

429/418: the response blocks the limiter (IP or account — by endpoint) for ``Retry-After``
(no header — :data:`DEFAULT_429_PAUSE` / :data:`DEFAULT_418_PAUSE`): the pause applies
to all instances with this key, not only to the retrying coroutine.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
import time
import weakref
from typing import Any, Callable, Optional

from bytx.errors import ConfigError
from bytx.net.metrics import _ENDPOINT

DEFAULT_429_PAUSE = 1.0     # s: 429 without Retry-After
DEFAULT_418_PAUSE = 120.0   # s: IP ban (Binance: «from 2 minutes to 3 days»)

# Endpoints limited per account: ccxt id → [(regex of label 'METHOD api:path', rule)].
# Rule: (cost in the account limiter | None — ccxt cost, cost on IP | None — ccxt).
# Cost unit — ccxt rateLimit (binance 50 ms, bybit 20 ms, okx ~103 ms, kucoinfutures 75 ms).
_Rule = tuple[Optional[float], Optional[float]]
ACCOUNT_ENDPOINTS: dict[str, list[tuple[re.Pattern, _Rule]]] = {
    # Binance USDⓈ-M: New/Modify Order, New Algo Order — «1 on 10s order rate limit; 1 on 1min
    # order rate limit; 0 on IP». Order limit «counted against each account»: 1200/min = 20/s
    # → 1 unit (50 ms). Batch: 5 per 10 s (300/10 s) → 10/3 units, IP weight 5 (ccxt).
    'binance': [
        (re.compile(r'^(POST|PUT) fapiPrivate:(order|algoOrder)$'), (1.0, 0.0)),
        (re.compile(r'^(POST|PUT) fapiPrivate:batchOrders$'), (10 / 3, None)),
    ],
    # Bybit v5: order limits «per second and UID» — Rate Limit docs, Trade table (UTA Pro and Classic):
    # create / amend / cancel — 10/s for inverse and linear (20/s for cancel and create — spot only; the stricter
    # one is used here, the client's market doesn't matter: 10/s = 5 units of 20 ms). Cancel used to be 2.5 (20/s):
    # a mass cancel ran twice as fast as the limit and hit 10006.
    # Per IP — 600 requests per 5 s = 120/s → 50/120 units (20 ms) per request.
    'bybit': [
        (re.compile(r'^POST private:v5/order/(create|amend|cancel)$'), (5.0, 5 / 12)),
        (re.compile(r'^POST private:v5/position/trading-stop$'), (5.0, 5 / 12)),
    ],
    # OKX: «Private REST endpoints are limited per User ID, public — per IP»
    'okx': [(re.compile(r'^\w+ private:'), (None, 0.0))],
    # KuCoin (classic): «Private endpoints are limited per UID; public — per IP»
    'kucoinfutures': [(re.compile(r'^\w+ futuresPrivate:'), (None, 0.0))],
    'kucoin': [(re.compile(r'^\w+ private:'), (None, 0.0))],
}


def endpoint_rule(exchange_id: str, label: Optional[str]) -> Optional[_Rule]:
    """Two-level limiter rule for an endpoint label (None — shared IP limiter only)."""
    if not label:
        return None
    for pattern, rule in ACCOUNT_ENDPOINTS.get(exchange_id, ()):
        if pattern.match(label):
            return rule
    return None


async def _real_sleep(delay: float) -> None:
    """Pause via a loop timer (not ``asyncio.sleep``): token waiting is real time."""
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    handle = loop.call_later(delay, lambda: fut.done() or fut.set_result(None))
    try:
        await fut
    finally:
        handle.cancel()


class TokenBucket:
    """Token bucket with a ccxt ``Throttler`` config (refillRate tokens/ms, capacity, tokens).

    Unlike ccxt (polling every 1 ms), each waiter sleeps once — until its
    slot: ``(cost - tokens) / refillRate``. Order is FIFO by request time.
    :meth:`block` — pause for everyone (429/418); requests that waited before the block re-queue
    after it, with no burst when the block is lifted.
    """

    __slots__ = ('config', '_stamp', 'blocked_until', '_epoch', 'waiting')

    def __init__(self, config: Optional[dict] = None) -> None:
        self.config: dict = {'refillRate': 1.0, 'cost': 1.0, 'tokens': 0, 'maxCapacity': 2000,
                             'capacity': 1.0}
        self.config.update(config or {})
        self._stamp = time.monotonic() * 1000.0
        self.blocked_until = 0.0  # time.monotonic()
        self._epoch = 0
        self.waiting = 0

    def _refill(self, now_ms: float) -> float:
        tokens = float(self.config['tokens'])
        cap = float(self.config['capacity'])
        if tokens < cap:
            tokens = min(tokens + (now_ms - self._stamp) * float(self.config['refillRate']), cap)
        self._stamp = now_ms
        self.config['tokens'] = tokens
        return tokens

    def _reserve(self, cost: float) -> float:
        """Deducts ``cost`` (as debt) and returns the pause until its slot, s."""
        now = time.monotonic()
        if cost <= 0:  # zero weight (Binance order on IP) doesn't wait for others' debt — only for a block
            return self.blocked_until - now
        tokens = self._refill(now * 1000.0)
        wait = 0.0 if tokens >= 0 else -tokens / float(self.config['refillRate']) / 1000.0
        self.config['tokens'] = tokens - cost
        return max(wait, self.blocked_until - now)

    async def __call__(self, cost: Any = None) -> None:
        cost = float(self.config['cost'] if cost is None else cost)
        while True:
            wait = self._reserve(cost)
            if wait <= 0:
                return
            if self.waiting >= self.config['maxCapacity']:
                self.config['tokens'] += cost
                raise RuntimeError(f'throttle queue is over maxCapacity ({int(self.config["maxCapacity"])})')
            epoch = self._epoch
            blocked = self.blocked_until > time.monotonic()
            self.waiting += 1
            try:
                # block — via asyncio.sleep (visible to tests with virtual time)
                await (asyncio.sleep(wait) if blocked else _real_sleep(wait))
            except BaseException:
                self.config['tokens'] += cost  # slot not used
                raise
            finally:
                self.waiting -= 1
            if epoch == self._epoch or time.monotonic() >= self.blocked_until:
                return
            self.config['tokens'] += cost  # a block arrived while waiting — re-queue

    def block(self, seconds: float) -> None:
        """Pause everyone for ``seconds`` (429/418); new requests come after it, one per slot."""
        if seconds <= 0:
            return
        now = time.monotonic()
        until = now + seconds
        if until <= self.blocked_until:
            return
        self.blocked_until = until
        self._epoch += 1
        tokens = self._refill(now * 1000.0)
        # debt for the block duration: after it requests go at the normal interval
        self.config['tokens'] = min(tokens, 0.0) - seconds * 1000.0 * float(self.config['refillRate'])

    @property
    def blocked(self) -> float:
        """Seconds the block still lasts (0 — none)."""
        return max(0.0, self.blocked_until - time.monotonic())


# loop → key → shared TokenBucket. Weak refs to the loop: a closed loop does not hold limiters.
_registry: 'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, TokenBucket]]' = \
    weakref.WeakKeyDictionary()


def limiter_key(exchange_id: str, proxy: Any = None) -> str:
    """Shared limiter key: exchange ccxt id + proxy key (one proxy = one IP) or 'direct'."""
    proxy_key = getattr(proxy, 'key', None) if proxy is not None else None
    return f'{exchange_id}|{proxy_key or "direct"}'


def account_key(key: str, exchange: Any) -> str:
    """Account limiter key: IP key + hash of apiKey / wallet address (the key itself is not stored)."""
    ident = getattr(exchange, 'apiKey', None) or getattr(exchange, 'walletAddress', None) \
        or f'instance-{id(exchange)}'
    return f'{key}|acct:{hashlib.sha256(str(ident).encode()).hexdigest()[:16]}'


def shared_throttler(key: str, bucket: dict) -> TokenBucket:
    """Shared :class:`TokenBucket` for the key in the current event loop (created on the first request).

    ``bucket`` — the ccxt instance's ``tokenBucket``; copied on first creation,
    later instances with the same key use the already created limiter.
    """
    loop = asyncio.get_running_loop()
    by_key = _registry.get(loop)
    if by_key is None:
        by_key = _registry[loop] = {}
    throttler = by_key.get(key)
    if throttler is None:
        throttler = by_key[key] = TokenBucket(dict(bucket or {}))
    return throttler


class RateCap:
    """Ceiling "no more than N requests per second" (evenly, no queues or lists)."""

    __slots__ = ('interval', '_next')

    def __init__(self, requests_per_second: float) -> None:
        if not requests_per_second or requests_per_second <= 0:
            raise ConfigError('rate_limit: requests per second > 0')
        self.interval = 1.0 / float(requests_per_second)
        self._next = 0.0

    def reserve(self) -> float:
        """Reserves a slot; returns how many seconds to wait for it."""
        now = time.monotonic()
        start = self._next if self._next > now else now
        self._next = start + self.interval
        return start - now

    async def acquire(self) -> None:
        wait = self.reserve()
        if wait > 0:
            await asyncio.sleep(wait)


def _retry_after(headers: Any) -> Optional[float]:
    if not headers:
        return None
    for name, value in headers.items():
        if str(name).lower() == 'retry-after':
            try:
                return max(0.0, float(value))
            except (TypeError, ValueError):
                return None
    return None


def rate_limit_pause(status: Any, headers: Any) -> Optional[float]:
    """Limiter pause from an HTTP response: 429 / 418 → Retry-After or the default pause."""
    try:
        code = int(status)
    except (TypeError, ValueError):
        return None
    if code not in (418, 429):
        return None
    hint = _retry_after(headers)
    if code == 418:
        return hint if hint is not None else DEFAULT_418_PAUSE
    return hint if hint is not None else DEFAULT_429_PAUSE


def install_limiter(exchange: Any, key: str, rate_limit: Optional[float] = None) -> None:
    """Replaces ``exchange.throttle``: a shared limiter by ``key`` (+ account limiter for
    endpoints in :data:`ACCOUNT_ENDPOINTS`) and an optional ceiling; 429/418 in a response
    block the corresponding limiter.

    A repeated call reconfigures the instance (new key / ceiling).
    """
    cap = RateCap(rate_limit) if rate_limit is not None else None
    acct = account_key(key, exchange)
    ex_id = exchange.id

    async def throttle(cost: Any = None) -> Any:
        rule = endpoint_rule(ex_id, _ENDPOINT.get())
        if rule is None:
            await shared_throttler(key, exchange.tokenBucket)(cost)
        else:
            acct_cost, ip_cost = rule
            await shared_throttler(acct, exchange.tokenBucket)(cost if acct_cost is None else acct_cost)
            # an IP block also applies to account endpoints (a 418 ban covers the whole IP)
            await shared_throttler(key, exchange.tokenBucket)(cost if ip_cost is None else ip_cost)
        if cap is not None:
            await cap.acquire()

    original: Callable[..., Any] = getattr(exchange, '_bytx_on_rest_response', None) \
        or exchange.on_rest_response
    exchange._bytx_on_rest_response = original

    def on_rest_response(code: Any, reason: Any, url: Any, method: Any, headers: Any, *rest: Any) -> Any:
        try:
            pause = rate_limit_pause(code, headers)
            if pause is not None:
                rule = endpoint_rule(ex_id, _ENDPOINT.get())
                # 418 — IP ban; 429 on an account endpoint — account limit
                target = acct if (rule is not None and str(code) == '429') else key
                shared_throttler(target, exchange.tokenBucket).block(pause)
        except Exception:  # pragma: no cover — the limiter never breaks response parsing
            pass
        return original(code, reason, url, method, headers, *rest)

    exchange.on_rest_response = on_rest_response
    exchange.throttle = throttle
    exchange._bytx_limiter_key = key


def limiter_state(key: str, loop: Optional[asyncio.AbstractEventLoop] = None) -> Optional[dict]:
    """State of the key's shared limiter in the current event loop, without creating it (``health()``):
    ``tokens`` (negative — queue debt), ``capacity``, ``waiting``, ``blocked_s``; None — no requests yet.
    ``loop`` — explicit loop (Prometheus scrape from the HTTP server thread)."""
    try:
        by_key = _registry.get(loop if loop is not None else asyncio.get_running_loop()) or {}
    except RuntimeError:
        return None
    bucket = by_key.get(key)
    if bucket is None:
        return None
    return {'tokens': float(bucket.config.get('tokens', 0.0)), 'capacity': float(bucket.config.get('capacity', 0.0)),
            'waiting': bucket.waiting, 'blocked_s': bucket.blocked}


def reset_registry() -> None:
    """Resets shared limiters (for tests)."""
    _registry.clear()


__all__ = ['ACCOUNT_ENDPOINTS', 'DEFAULT_418_PAUSE', 'DEFAULT_429_PAUSE', 'RateCap', 'TokenBucket',
           'account_key', 'endpoint_rule', 'install_limiter', 'limiter_key', 'rate_limit_pause',
           'reset_registry', 'shared_throttler']
