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
to all instances with this key, not only to the retrying coroutine. Bybit answers the UID limit with HTTP 200 and
retCode 10006 («Too many visits») — it pauses the account limiter of that endpoint for the exchange window
(:data:`BYBIT_UID_LIMIT_PAUSE`), including when the limit was exceeded by another process on the same key.

The account limiter is split into parts by endpoint (``part`` in :data:`ACCOUNT_ENDPOINTS`): the exchange counts each
endpoint's limit separately — Bybit «your remaining requests for current endpoint» (create, amend, cancel — 10/s per
UID each), OKX «Rate limits for placing orders, amending orders, and cancelling orders are independent from each
other». A mass cancel does not wait in one queue with placing orders and closing a position. Part ``''`` — orders
(placing) and endpoints without their own part. Endpoints with their own per-UID limit in the docs (Bybit reads,
OKX private endpoints, BingX, Bitget, Gate, MEXC) get their own part (:data:`PER_ENDPOINT`); OKX trading endpoints are
also split by the request ``instId`` («User ID + Instrument ID»).

Exchange bans pause the limiter until the ban ends, without retries (:func:`ban_signal`): HTTP 418, Bybit HTTP 403
«access too frequent» (IP, 10 minutes), BingX 100410 (the endpoint, until the unblock time from the response).
A limiter blocked longer than the remaining call deadline raises :class:`~bytx.errors.RateLimited` right away.

Demo / testnet with their own REST host get their own IP limiter (:func:`host_env`): a ban of the demo host does not
hold live clients. Endpoint costs that ccxt gets wrong are fixed per the docs (:data:`COST_FIXES`).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
import weakref
from typing import Any, Callable, Optional

from bytx.errors import ConfigError, RateLimited, ThrottleQueueFull
from bytx.net.metrics import _ENDPOINT, _INSTRUMENT, endpoint_label

DEFAULT_429_PAUSE = 1.0     # s: 429 without Retry-After
DEFAULT_418_PAUSE = 120.0   # s: IP ban (Binance: «from 2 minutes to 3 days»)
#: queue limit of a shared limiter (IP and key): requests of all clients of the process wait in it (ccxt — 1000 per
#: instance)
SHARED_MAX_CAPACITY = 10000

#: account limiter part "the endpoint has its own UID limit" (in the rule table): expands to ``'|<label>'``
PER_ENDPOINT = '|*'

# Endpoints limited per account: ccxt id → [(regex of label 'METHOD api:path', rule, account limiter part)].
# Rule: (cost in the account limiter | None — ccxt cost, cost on IP | None — ccxt).
# Cost unit — ccxt rateLimit (binance 50 ms, bybit 20 ms, okx ~103 ms, kucoinfutures 75 ms).
# Part: '' — the account's shared order limiter; any other — the endpoint's own limiter (the exchange counts its limit
# separately).
_Rule = tuple[Optional[float], Optional[float]]
ACCOUNT_ENDPOINTS: dict[str, list[tuple[re.Pattern, _Rule, str]]] = {
    # Binance USDⓈ-M: New/Modify Order, New Algo Order — «1 on 10s order rate limit; 1 on 1min
    # order rate limit; 0 on IP». Order limit «counted against each account»: 1200/min = 20/s
    # → 1 unit (50 ms). Batch: 5 per 10 s (300/10 s) → 10/3 units, IP weight 5 (ccxt).
    # Cancel (DELETE order / batchOrders) — IP weight 1 only («Request Weight 1»): not part of the order limit.
    'binance': [
        (re.compile(r'^(POST|PUT) fapiPrivate:(order|algoOrder)$'), (1.0, 0.0), ''),
        (re.compile(r'^(POST|PUT) fapiPrivate:batchOrders$'), (10 / 3, None), ''),
    ],
    # Bybit v5: order limits «per second and UID» — Rate Limit docs, Trade table (UTA Pro and Classic):
    # create / amend / cancel — 10/s for inverse and linear (20/s for cancel and create — spot only; the stricter
    # one is used here, the client's market doesn't matter: 10/s = 5 units of 20 ms). Cancel used to be 2.5 (20/s):
    # a mass cancel ran twice as fast as the limit and hit 10006.
    # Each endpoint has its own limit (header X-Bapi-Limit-Status — «your remaining requests for current endpoint»;
    # demo 2026-11-04: after 5 creates in the same second cancel still had 9 left, cancel-batch — its own):
    # create, amend, cancel, cancel-all and trading-stop — separate limiters. cancel-all — 10/s (linear, inverse; not
    # upgradable), ccxt charges 50 units (1/s) on the shared IP limiter.
    # Per IP — 600 requests per 5 s = 120/s → 50/120 units (20 ms) per request.
    'bybit': [
        (re.compile(r'^POST private:v5/order/create$'), (5.0, 5 / 12), ''),
        (re.compile(r'^POST private:v5/order/amend$'), (5.0, 5 / 12), '|amend'),
        (re.compile(r'^POST private:v5/order/cancel$'), (5.0, 5 / 12), '|cancel'),
        (re.compile(r'^POST private:v5/order/cancel-all$'), (5.0, 5 / 12), '|cancel-all'),
        (re.compile(r'^POST private:v5/position/trading-stop$'), (5.0, 5 / 12), '|trading-stop'),
        # Reads (same table, «rolling time window per second and UID»): position/list, order/realtime,
        # order/history, execution/list, account/wallet-balance — each has its own UID limit: Classic — 10/s, UTA Pro —
        # inverse 10/s, linear 50/s. The strict one (as for orders) — 10/s = 5 units per account; per IP — 600 per
        # 5 s (5/12). ccxt 4.4.65 charges 5 units on the shared IP limiter: 30 accounts read positions 10 times per
        # second for the whole IP.
        (re.compile(r'^GET private:v5/(position/list|order/realtime|order/history|execution/list|'
                    r'account/wallet-balance)$'), (5.0, 5 / 12), PER_ENDPOINT),
    ],
    # OKX: «Private REST endpoints are limited per User ID, public — per IP». Trading-related APIs: «Rate limits for
    # placing orders, amending orders, and cancelling orders are independent from each other» — cancel-order and
    # amend-order (60/2 s, ccxt 1/3 unit) have their own limiters; placing — ''. Trading endpoints are also limited
    # «on the Instrument ID level (except Options)» (Place order: «60 requests per 2 seconds», «User ID + Instrument
    # ID») — the part gets the request instId appended (:func:`_instrument_part`). Other private endpoints — each its
    # own limit (Get positions «10 requests per 2 seconds», Get balance «10 requests per 2 seconds», …): the
    # endpoint's own part, ccxt cost (= docs). Before: all private endpoints in part '' — position and balance reads
    # of bots on one key delayed a closing order by ~2 s.
    'okx': [(re.compile(r'^POST private:trade/order$'), (None, 0.0), ''),
            (re.compile(r'^POST private:trade/cancel-order$'), (None, 0.0), '|cancel'),
            (re.compile(r'^POST private:trade/amend-order$'), (None, 0.0), '|amend'),
            (re.compile(r'^\w+ private:'), (None, 0.0), PER_ENDPOINT)],
    # KuCoin (classic): «Private endpoints are limited per UID; public — per IP»
    'kucoinfutures': [(re.compile(r'^\w+ futuresPrivate:'), (None, 0.0), '')],
    'kucoin': [(re.compile(r'^\w+ private:'), (None, 0.0), '')],
    # BingX (unit 100 ms = 10/s): each endpoint has its own «/ UID» limit (swap V2 docs, «UID Rate Limit» metadata):
    # POST/DELETE trade/order — 10/s, GET trade/order — 30/s, user/positions — 10/s, trade/openOrders, user/balance,
    # trade/batchOrders, trade/allOpenOrders — 5/s; others — ccxt cost (2 = 5/s). Per IP — «Account API Group [2]/[3]:
    # total 2000 requests per 10 seconds, individual 300 requests per 10 seconds for each interface» → 30/s per
    # endpoint ≥ 200/s in total: 1/3 unit per request. ccxt 4.4.65 charges 2 units on the IP: 5 requests/s for the
    # whole IP.
    'bingx': [
        (re.compile(r'^POST swap/v\d/private:trade/order$'), (1.0, 1 / 3), ''),
        (re.compile(r'^DELETE swap/v\d/private:trade/order$'), (1.0, 1 / 3), PER_ENDPOINT),
        (re.compile(r'^GET swap/v\d/private:trade/order$'), (1 / 3, 1 / 3), PER_ENDPOINT),
        (re.compile(r'^GET swap/v\d/private:user/positions$'), (1.0, 1 / 3), PER_ENDPOINT),
        (re.compile(r'^\w+ swap/v\d/private:(trade/openOrders|user/balance|trade/batchOrders|trade/allOpenOrders)$'),
         (2.0, 1 / 3), PER_ENDPOINT),
        (re.compile(r'^\w+ swap/v\d/private:'), (None, 1 / 3), PER_ENDPOINT),
    ],
    # Bitget (unit 50 ms = 20/s): each endpoint has its own limit, private ones — «(UID)» / «/sec/UID» (v2 mix docs:
    # place-order, cancel-order, orders-pending … 10/s, all-position, set-leverage 5/s); ccxt costs match the docs.
    # Per IP — «overall rate limit 6000/IP/Min» = 100/s → 0.2.
    'bitget': [
        (re.compile(r'^POST private/mix:v2/mix/order/place-order$'), (None, 0.2), ''),
        (re.compile(r'^\w+ private/mix:'), (None, 0.2), PER_ENDPOINT),
    ],
    # Gate (unit 50 ms = 20/s), APIv4 docs «Frequency limit rule», Perpetual futures (private, «Based on UID»):
    # placing and amending — 100r/s together, cancel (single and batch) — 200r/s, others — 200r/10s per endpoint
    # (ccxt — 1 = 20/s, matches). Private endpoints are per UID, no IP limit (0).
    'gate': [
        (re.compile(r'^(POST private/futures:\{settle\}/(orders|batch_orders)|PUT private/futures:\{settle\}/orders/'
                    r'\{order_id\})$'), (0.2, 0.0), ''),
        (re.compile(r'^(DELETE private/futures:\{settle\}/orders(/\{order_id\})?|POST private/futures:\{settle\}/'
                    r'batch_cancel_orders)$'), (0.1, 0.0), '|cancel'),
        (re.compile(r'^\w+ private/futures:'), (None, 0.0), PER_ENDPOINT),
    ],
    # MEXC contract (unit 50 ms): «Rate limit: 20 times/2 seconds» for each endpoint (order, cancel, positions,
    # assets…), batch order — «1/2 seconds» (ccxt — 2 and 40, matches). Per UID or per IP — the docs do not say:
    # the account limiter per endpoint and the ccxt cost on the IP (strict).
    'mexc': [
        (re.compile(r'^POST contract/private:order/submit$'), (None, None), ''),
        (re.compile(r'^\w+ contract/private:'), (None, None), PER_ENDPOINT),
    ],
    # BloFin (ccxt unit 100 ms), F#rest-api-limits: trading — «30 requests per 10 seconds per UserId» (1 per 333 ms =
    # 10/3 units, orders / cancels / amend / TP/SL share one limit), IP — 500 / min (≈ 8.3/s → 1.2 units) and
    # 1500 / 5 min; ccxt 4.4.65 charges 1 unit (10/s) per IP.
    'blofin': [
        (re.compile(r'^POST private:(spot/)?trade/'), (10 / 3, 1.2), ''),   # spot shares the limit (S#rest-api-limits)
        (re.compile(r'^\w+ (private|public):'), (None, 1.2), PER_ENDPOINT),
    ],
    # HTX USDT-swap (unit 100 ms): «private interface rate limit of API key is at most 144 times every 3 seconds for
    # each UID (Trade Interface: at most 72 times every 3 seconds. Read Interface: at most 72 times every 3 seconds)» —
    # 24/s = 5/12 unit; private endpoints have no IP limit (240/3 s and 800/s are public). Reads — information
    # requests (*_info, *openorders, *hisorders, *matchresults, *detail, *record …), the rest — trading (order,
    # cancel, leverage). ccxt 4.4.65 charges 1 unit (10/s) on the shared IP limiter.
    'htx': [
        (re.compile(r'^\w+ contract/private:[\w-]+/v\d/\w*(info|openorders|hisorders|matchresults|detail|record|'
                    r'available|valuation|limit|query|get)\w*$'), (5 / 12, 0.0), '|read'),
        (re.compile(r'^\w+ contract/private:'), (5 / 12, 0.0), ''),
    ],
}

#: account limiter pause after Bybit retCode 10006 (the UID limit window is a sliding second), s
BYBIT_UID_LIMIT_PAUSE = 1.0


def _lookup(exchange_id: str, label: Optional[str]) -> Optional[tuple[_Rule, str]]:
    if not label:
        return None
    for pattern, rule, part in ACCOUNT_ENDPOINTS.get(exchange_id, ()):
        if pattern.match(label):
            return rule, (f'|{label}' if part == PER_ENDPOINT else part)
    return None


#: trading endpoints limited per «User ID + Instrument ID» (OKX Place / Amend / Cancel order)
_INSTRUMENT_LABELS = {'okx': frozenset({'POST private:trade/order', 'POST private:trade/cancel-order',
                                        'POST private:trade/amend-order'})}


def _instrument_part(exchange_id: str, label: Optional[str], part: str) -> str:
    """Account limiter part with the request instrument (OKX trading endpoints are limited per UID and instId)."""
    inst = _INSTRUMENT.get()
    if inst and label in _INSTRUMENT_LABELS.get(exchange_id, ()):
        return f'{part}|{inst}'
    return part


def endpoint_rule(exchange_id: str, label: Optional[str]) -> Optional[_Rule]:
    """Two-level limiter rule for an endpoint label (None — shared IP limiter only)."""
    found = _lookup(exchange_id, label)
    return found[0] if found is not None else None


def endpoint_part(exchange_id: str, label: Optional[str]) -> str:
    """The endpoint's account limiter part: '' — the shared order limiter, otherwise its own."""
    found = _lookup(exchange_id, label)
    return found[1] if found is not None else ''


def _uid_limited(exchange_id: str, body: Any) -> bool:
    """Bybit: a response (whole or a batch item) with code 10006 — the UID limit is exceeded (HTTP 200)."""
    if exchange_id != 'bybit' or not isinstance(body, str) or '10006' not in body:
        return False
    try:
        data = json.loads(body)
    except ValueError:
        return False
    if not isinstance(data, dict):
        return False
    if str(data.get('retCode')) == '10006':
        return True
    items = ((data.get('retExtInfo') or {}).get('list') or []) if isinstance(data.get('retExtInfo'), dict) else []
    return any(isinstance(i, dict) and str(i.get('code')) == '10006' for i in items)


def _binance_open_algo_orders(params: Any) -> float:
    """Current All Algo Open Orders: «1 for a single symbol; 40 when the symbol parameter is omitted»."""
    return 1.0 if isinstance(params, dict) and params.get('symbol') else 40.0


#: Endpoint cost per the exchange docs where ccxt is wrong: ccxt id → {endpoint label: cost in ccxt units or a
#: function of params}. Binance USDⓈ-M (unit — weight 1): Account Information V3 / Futures Account Balance V3 —
#: «IP Weight 5», Position Information V3 — «Request Weight 5»; V2 of the same endpoints — 5 (ccxt 4.4.65 — 1: 30
#: accounts behind one IP polled positionRisk at 6000 weight/min with a 2400 limit); openAlgoOrders — 1 with symbol, 40
#: without (ccxt — 1). Installed by :func:`install_limiter` (:func:`apply_cost_fixes`).
COST_FIXES: dict[str, dict[str, Any]] = {
    'binance': {
        'GET fapiPrivate:openAlgoOrders': _binance_open_algo_orders,
        'GET fapiPrivateV3:account': 5.0,
        'GET fapiPrivateV3:balance': 5.0,
        'GET fapiPrivateV3:positionRisk': 5.0,
        'GET fapiPrivateV2:account': 5.0,
        'GET fapiPrivateV2:balance': 5.0,
        'GET fapiPrivateV2:positionRisk': 5.0,
    },
}


def apply_cost_fixes(exchange: Any) -> None:
    """Endpoint costs from :data:`COST_FIXES` on the instance (over ccxt ``calculate_rate_limiter_cost``).
    Idempotent; an exchange without fixes is left unchanged."""
    fixes = COST_FIXES.get(getattr(exchange, 'id', ''))
    if not fixes or getattr(exchange, '_bytx_cost_fixes', False):
        return
    inner = exchange.calculate_rate_limiter_cost

    def calculate_rate_limiter_cost(api: Any, method: Any, path: Any, params: Any, config: Any = {}) -> Any:  # noqa: B006
        fixed = fixes.get(endpoint_label(api, method, path))
        if fixed is not None:
            return fixed(params) if callable(fixed) else fixed
        return inner(api, method, path, params, config)

    exchange.calculate_rate_limiter_cost = calculate_rate_limiter_cost
    exchange._bytx_cost_fixes = True


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

    __slots__ = ('config', '_stamp', 'blocked_until', '_epoch', 'waiting', 'key', '_warned_epoch')

    def __init__(self, config: Optional[dict] = None, key: Optional[str] = None) -> None:
        self.config: dict = {'refillRate': 1.0, 'cost': 1.0, 'tokens': 0, 'maxCapacity': 2000,
                             'capacity': 1.0}
        self.config.update(config or {})
        self._stamp = time.monotonic() * 1000.0
        self.blocked_until = 0.0  # time.monotonic()
        self._epoch = 0
        self.waiting = 0
        #: limiter key (for the text and details of the queue overflow error)
        self.key = key
        self._warned_epoch = -1

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
                busy = self.busy_for()
                raise ThrottleQueueFull(
                    f'SDK limiter queue {self.key or ""} is full: {self.waiting} requests waiting (limit '
                    f'{int(self.config["maxCapacity"])}, queue for {busy:.1f} s); the request was not sent — retry '
                    f'later or lower the rate (the IP / account limit is shared by all clients of the process)',
                    retry_after=busy, details={'key': self.key, 'waiting': self.waiting})
            epoch = self._epoch
            blocked = self.blocked_until > time.monotonic()
            if blocked:
                try:
                    self._on_blocked_wait(wait)
                except RateLimited:
                    self.config['tokens'] += cost     # slot not used
                    raise
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

    def _on_blocked_wait(self, wait: float) -> None:
        """A request waits for a block (429/418/ban): longer than the remaining call deadline — RateLimited with
        ``retry_after`` right away (not a silent sleep until the timeout); otherwise a WARNING once per block."""
        from bytx.net.deadline import deadline_left
        left = deadline_left()
        if left is not None and wait > left:
            raise RateLimited(f'limiter {self.key or ""} is blocked for {wait:.0f} s more (429/418/exchange ban) — '
                              f'longer than the call deadline ({max(left, 0.0):.1f} s); the request was not sent',
                              retry_after=wait, details={'key': self.key})
        if self._warned_epoch != self._epoch:
            self._warned_epoch = self._epoch
            from loguru import logger
            logger.warning('bytx: limiter {} is blocked for {:.0f} s more (429/418/exchange ban) — requests wait',
                           self.key or '', wait)

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

    def busy_for(self) -> float:
        """Seconds until the debt is repaid and the block ends (0 — the next request goes right away)."""
        now = time.monotonic()
        tokens = self._refill(now * 1000.0)
        debt = -tokens / float(self.config['refillRate']) / 1000.0 if tokens < 0 else 0.0
        return max(0.0, debt, self.blocked_until - now)


# loop → key → shared TokenBucket. Weak refs to the loop: a closed loop does not hold limiters.
_registry: 'weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, dict[str, TokenBucket]]' = \
    weakref.WeakKeyDictionary()


def limiter_key(exchange_id: str, proxy: Any = None, env: str = 'live') -> str:
    """Shared limiter key: exchange ccxt id + proxy key (one proxy = one IP) or 'direct' + the environment if it has
    its own host (``env`` ≠ 'live', :func:`host_env`): IP limits and bans are per host — a 418 of the demo host does
    not hold live clients."""
    proxy_key = getattr(proxy, 'key', None) if proxy is not None else None
    base = f'{exchange_id}|{proxy_key or "direct"}'
    return base if not env or env == 'live' else f'{base}|{env}'


def _hosts(api: Any) -> set:
    out: set = set()
    if isinstance(api, str):
        if not api.startswith('http'):          # REST only: WS (OKX demo — wspap) does not touch the REST limiter
            return out
        host = api.split('://', 1)[-1].split('/', 1)[0]
        if host:
            out.add(host)
    elif isinstance(api, dict):
        for value in api.values():
            out |= _hosts(value)
    return out


def host_env(exchange: Any) -> str:
    """Environment of the instance's REST host: 'live' — the ccxt class production hosts (including OKX demo: the same
    www.okx.com with the x-simulated-trading header — the IP ban is shared), otherwise 'demo' or 'testnet' (own host:
    Binance Demo Mode and testnet, Bybit demo, BingX VST, Hyperliquid testnet, KuCoin sandbox)."""
    try:
        live = _hosts((type(exchange).describe(exchange).get('urls') or {}).get('api'))
        mine = _hosts((getattr(exchange, 'urls', None) or {}).get('api'))
    except Exception:  # noqa: BLE001 — stubs without urls: treated as live
        return 'live'
    if not mine or mine <= live:
        return 'live'
    options = getattr(exchange, 'options', None)
    options = options if isinstance(options, dict) else {}
    env = options.get('bytxDemoEnv')
    if env in ('demo', 'testnet'):
        return env
    joined = ' '.join(sorted(mine - live)).lower()
    if options.get('enableDemoTrading') or 'demo' in joined or 'vst' in joined:
        return 'demo'
    return 'testnet'


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
        config = dict(bucket or {})
        # requests of all clients of the process wait in a shared limiter: the ccxt queue limit (1000, per instance)
        # is too small
        config['maxCapacity'] = max(float(config.get('maxCapacity') or 0), SHARED_MAX_CAPACITY)
        throttler = by_key[key] = TokenBucket(config, key)
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


#: Bybit IP ban: HTTP 403 «access too frequent» (v5 Rate Limit docs: «wait for at least 10 minutes»), s
BYBIT_IP_BAN_PAUSE = 600.0
#: lower bound of a ban pause (clocks out of sync, the unblock moment already passed), s
MIN_BAN_PAUSE = 1.0
#: BingX 100410 without the unblock moment in the text, s
BINGX_BAN_DEFAULT = 60.0
BLOFIN_IP_BAN_PAUSE = 300.0
_UNBLOCK_RE = re.compile(r'unblocked after (\d{10,})')


def ban_signal(exchange_id: str, code: Any, body: Any, now_ms: Optional[float] = None) -> Optional[tuple[str, float]]:
    """A ban from an exchange response: (scope, seconds) — ``'ip'`` or ``'endpoint'``; None — not a ban.

    Bybit: HTTP 403 and «access too frequent» — an IP ban for :data:`BYBIT_IP_BAN_PAUSE` (other 403 — geo block etc.
    — are not a ban). BingX: code 100410 (HTTP 200) «...disabled period and will be unblocked after <ms>» — until the
    unblock moment by the exchange clock (the response ``timestamp``, otherwise ``now_ms`` — the local clock with the
    offset); the docs do not name the scope, the text says «endpoint ... limit rule»: the endpoint part. Not less than
    :data:`MIN_BAN_PAUSE`."""
    text = body if isinstance(body, str) else ''
    if exchange_id == 'bybit':
        if str(code) == '403' and 'access too frequent' in text.lower():
            return 'ip', BYBIT_IP_BAN_PAUSE
        return None
    if exchange_id == 'blofin':
        # F#network-firewall-restrictions: HTTP 403 — firewall, IP ban for ~5 min
        return ('ip', BLOFIN_IP_BAN_PAUSE) if str(code) == '403' else None
    if exchange_id == 'bingx' and '100410' in text:
        try:
            data = json.loads(text)
        except ValueError:
            return None
        if not isinstance(data, dict) or str(data.get('code')) != '100410':
            return None
        m = _UNBLOCK_RE.search(str(data.get('msg') or ''))
        if m is None:
            return 'endpoint', BINGX_BAN_DEFAULT
        stamp = data.get('timestamp')
        server_ms = float(stamp) if isinstance(stamp, (int, float)) else (now_ms if now_ms is not None
                                                                           else time.time() * 1000.0)
        return 'endpoint', max(MIN_BAN_PAUSE, (int(m.group(1)) - server_ms) / 1000.0)
    return None


def _exchange_now_ms(exchange: Any) -> float:
    """The exchange clock from the local one with the time sync offset (``options['timeDifference']``, ms)."""
    options = getattr(exchange, 'options', None)
    diff = options.get('timeDifference') if isinstance(options, dict) else None
    return time.time() * 1000.0 - float(diff or 0)


#: BingX endpoint ban windows per IP key: account → until (monotonic); a second account in the window — an IP ban
_endpoint_bans: dict[str, dict[str, float]] = {}


def _note_ban(exchange: Any, seconds: float, where: str) -> None:
    exchange._bytx_ban_until = time.monotonic() + seconds
    from loguru import logger
    logger.warning('bytx: {} ban {} for {:.0f} s', getattr(exchange, 'id', '?'), where, seconds)


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
        label = _ENDPOINT.get()
        found = _lookup(ex_id, label)
        if found is None:
            await shared_throttler(key, exchange.tokenBucket)(cost)
        else:
            (acct_cost, ip_cost), part = found
            part = _instrument_part(ex_id, label, part)
            # the endpoint's part of the account limiter (the exchange counts each endpoint's limit separately)
            await shared_throttler(acct + part, exchange.tokenBucket)(cost if acct_cost is None else acct_cost)
            # an IP block also applies to account endpoints (a 418 ban covers the whole IP)
            await shared_throttler(key, exchange.tokenBucket)(cost if ip_cost is None else ip_cost)
        if cap is not None:
            await cap.acquire()

    original: Callable[..., Any] = getattr(exchange, '_bytx_on_rest_response', None) \
        or exchange.on_rest_response
    exchange._bytx_on_rest_response = original

    def on_rest_response(code: Any, reason: Any, url: Any, method: Any, headers: Any, *rest: Any) -> Any:
        try:
            label = _ENDPOINT.get()
            found = _lookup(ex_id, label)
            part = _instrument_part(ex_id, label, found[1]) if found is not None else ''
            pause = rate_limit_pause(code, headers)
            ban = ban_signal(ex_id, code, rest[0] if rest else None, _exchange_now_ms(exchange))
            if ban is not None:
                _apply_ban(ban, part if found is not None else None)
                pause = None
            elif str(code) == '418' and pause is not None:
                _note_ban(exchange, pause, 'IP (418)')
            if pause is not None:
                # 418 — IP ban; 429 on an account endpoint — account limit (this endpoint's part)
                target = acct + part if (found is not None and str(code) == '429') else key
                shared_throttler(target, exchange.tokenBucket).block(pause)
            elif found is not None and rest and _uid_limited(ex_id, rest[0]):
                # Bybit answers the UID limit with HTTP 200 + 10006: the rest of the window — a pause for this
                # endpoint's account limiter (including the limit exceeded by another process on the same key)
                shared_throttler(acct + part, exchange.tokenBucket).block(BYBIT_UID_LIMIT_PAUSE)
        except Exception:  # pragma: no cover — the limiter never breaks response parsing
            pass
        return original(code, reason, url, method, headers, *rest)

    def _apply_ban(ban: tuple[str, float], part: Optional[str]) -> None:
        """An IP ban — the IP limiter; an endpoint ban — its account limiter part, and for a second account of the IP
        within the window — the IP."""
        scope, seconds = ban
        now = time.monotonic()
        if scope == 'endpoint' and part is not None:
            windows = _endpoint_bans.setdefault(key, {})
            for k in [k for k, until in windows.items() if until <= now]:
                del windows[k]
            others = [k for k in windows if k != acct]
            windows[acct] = now + seconds
            if not others:
                shared_throttler(acct + part, exchange.tokenBucket).block(seconds)
                _note_ban(exchange, seconds, f'endpoint {_ENDPOINT.get()}')
                return
        shared_throttler(key, exchange.tokenBucket).block(seconds)
        _note_ban(exchange, seconds, 'IP')

    exchange.on_rest_response = on_rest_response
    exchange.throttle = throttle
    exchange._bytx_limiter_key = key
    apply_cost_fixes(exchange)


def limiter_state(key: str, loop: Optional[asyncio.AbstractEventLoop] = None) -> Optional[dict]:
    """State of the key's shared limiter in the current event loop, without creating it (``health()``):
    ``tokens`` (negative — queue debt), ``capacity``, ``waiting``, ``blocked_s``, ``busy_s`` (how long the next
    request would wait); None — no requests yet. Tokens are as of now (refilled since the last reservation), not
    as of the reservation: otherwise the debt of a long-repaid request hung in health. The limiter is not
    changed. ``loop`` — explicit loop (Prometheus scrape from the HTTP server thread)."""
    try:
        by_key = _registry.get(loop if loop is not None else asyncio.get_running_loop()) or {}
    except RuntimeError:
        return None
    bucket = by_key.get(key)
    if bucket is None:
        return None
    now = time.monotonic()
    tokens = float(bucket.config.get('tokens', 0.0))
    capacity = float(bucket.config.get('capacity', 0.0))
    rate = float(bucket.config.get('refillRate') or 0.0)
    if tokens < capacity:
        tokens = min(tokens + (now * 1000.0 - bucket._stamp) * rate, capacity)
    debt = -tokens / rate / 1000.0 if tokens < 0 and rate > 0 else 0.0
    return {'tokens': tokens, 'capacity': capacity, 'waiting': bucket.waiting, 'blocked_s': bucket.blocked,
            'busy_s': max(0.0, debt, bucket.blocked_until - now)}


def reset_registry() -> None:
    """Resets shared limiters (for tests)."""
    _registry.clear()
    _endpoint_bans.clear()


__all__ = ['ACCOUNT_ENDPOINTS', 'BYBIT_IP_BAN_PAUSE', 'BYBIT_UID_LIMIT_PAUSE', 'COST_FIXES', 'DEFAULT_418_PAUSE',
           'DEFAULT_429_PAUSE', 'PER_ENDPOINT', 'RateCap', 'TokenBucket', 'account_key', 'apply_cost_fixes',
           'ban_signal', 'endpoint_part', 'endpoint_rule', 'host_env', 'install_limiter', 'limiter_key',
           'rate_limit_pause', 'reset_registry', 'shared_throttler']
