"""TradingView webhook: alert → open_position / close_position / order / algo.

::

    ex = create_exchange('bybit', demo=True, api_key=..., secret=...)       # or paper=True
    hook = TradingViewWebhook(ex, secret=os.environ['BYTX_TV_SECRET'])
    runner = await hook.serve(port=8080)        # aiohttp (core); behind nginx/Caddy on 443
    # FastAPI: app.include_router(hook.fastapi_router())               (extra ``fastapi``)

The TradingView alert message is JSON (the "Message" field)::

    {"secret": "<secret>", "id": "{{strategy.order.id}}-{{timenow}}", "time": "{{timenow}}",
     "action": "open", "symbol": "{{ticker}}", "side": "long", "risk_usdt": 50, "stop": 61000}

TradingView rules follow the docs (``tests/ground_truth/tradingview``): POST; a JSON message arrives
as ``application/json``, anything else as ``text/plain`` (such an alert is rejected: the secret cannot be checked);
ports 80 and 443 only; a request longer than three seconds is canceled — so by default the response
``202 accepted`` is sent right after the checks and execution runs in the background (``wait=True`` — wait for the result);
sender IP addresses — :data:`TRADINGVIEW_IPS` (``allowed_ips=TRADINGVIEW_IPS`` — only those);
the docs mention no delivery retries ("webhooks may occasionally fail"), idempotency is by ``id``.

Security:

- **secret is mandatory** (at least 16 characters; ``secret=`` or env ``BYTX_TV_SECRET``);
    constant-time comparison, the secret never appears in responses or logs;
- **demo/paper only by default**: a live account raises ConfigError without ``allow_live=True``;
- rate limit (``max_per_minute``, ``burst``) — two budgets: failed requests (no secret,
    not JSON, too large) spend their sender address's limit and are checked before parsing (secret
    brute force hits 429), alerts with the correct secret share a common budget; a flood without the secret does not block
    a real alert. Behind a reverse proxy — ``trust_forwarded=True`` (otherwise everyone has the proxy address) or
    ``allowed_ips=TRADINGVIEW_IPS``; a body larger than ``max_body`` — 413;
- ``allowed_symbols`` / ``allowed_actions`` — allowlists; algo parameters are allowlisted too;
- ``max_age`` — an alert whose ``time`` is older than N seconds is rejected (delivery/queue delay);
- an execution error — ``ex.emit('error', data={'source': 'tradingview', ...})`` (seen by
    :class:`~bytx.integrations.notify.Notifier`) and the log.

Actions (``action``):

========================  =================================================================
``open``                  ``risk_usdt`` + ``stop`` → :meth:`Exchange.open_position`; otherwise an order
                          ``size``/``notional``/``contracts`` on side ``side`` (+ ``take_profit``,
                          ``stop_loss``, ``price``, ``leverage``)
``close``                 :meth:`Exchange.close_position` (``side`` — for hedge)
``buy`` / ``sell``        order; ``{{strategy.order.action}}``; ``market_position: "flat"`` —
                          closes the position (``{{strategy.market_position}}``)
``market`` ``chase``      ``ex.algos.<action>(symbol, buy|sell, size=…, …)`` — parameters from
``twap`` ``iceberg``      the :data:`ALGO_PARAMS` allowlist
``bracket``
``cancel_all``            :meth:`Exchange.cancel_all` (own orders of the symbol: requires
                          ``create_exchange(client_id_prefix=…)``, otherwise an SDK ConfigError)
========================  =================================================================

Symbol: ``BTC/USDT``, ``BTCUSDT`` or the TradingView perpetual ``{{ticker}}``
``BTCUSDT.P`` (the ``.P`` suffix is stripped), normalized by :meth:`Exchange.symbol`.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import sqlite3
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Mapping, Optional, Union

from loguru import logger

from bytx.errors import ConfigError, ccxt_secrets, scrub

#: TradingView IP addresses for webhooks (docs "About webhooks")
TRADINGVIEW_IPS = ('52.89.214.238', '34.212.75.30', '54.218.53.128', '52.32.178.7')
#: TradingView cancels a request longer than 3 s — by default respond immediately (202), execute in the background
TRADINGVIEW_TIMEOUT_S = 3.0
SECRET_ENV = 'BYTX_TV_SECRET'
MIN_SECRET_LEN = 16

ACTIONS = ('open', 'close', 'buy', 'sell', 'market', 'chase', 'twap', 'iceberg', 'bracket', 'cancel_all')
ORDER_PARAMS = ('size', 'notional', 'contracts', 'margin', 'leverage', 'price', 'type', 'post_only',
                'time_in_force', 'reduce_only', 'client_id', 'take_profit', 'stop_loss')
OPEN_PARAMS = ('risk_usdt', 'stop', 'leverage', 'take_profit', 'price', 'slippage_bps', 'trigger_by',
               'client_id', 'position_id')
ALGO_PARAMS: dict[str, tuple[str, ...]] = {
    'market': ('size', 'max_slippage_bps', 'reference', 'retries', 'reduce_only'),
    'chase': ('size', 'max_moves', 'max_deviation_bps', 'min_interval', 'timeout', 'post_only', 'on_exhausted',
              'max_slippage_bps', 'reduce_only'),
    'twap': ('size', 'duration', 'slices', 'randomize', 'seed', 'slice_type', 'max_slippage_bps', 'reduce_only'),
    'iceberg': ('size', 'price', 'visible', 'visible_jitter', 'seed', 'post_only', 'reduce_only'),
    'bracket': ('size', 'take_profit', 'stop_loss', 'price', 'entry_timeout', 'oco', 'trigger_by',
                'on_protect_fail', 'close_on_cancel'),
}
_NUMERIC = {'size', 'notional', 'contracts', 'margin', 'leverage', 'price', 'take_profit', 'stop_loss', 'risk_usdt',
            'stop', 'slippage_bps', 'max_slippage_bps', 'max_deviation_bps', 'min_interval', 'timeout', 'duration',
            'randomize', 'visible', 'visible_jitter', 'entry_timeout'}
_INTEGER = {'retries', 'max_moves', 'slices', 'seed'}
_BOOL = {'post_only', 'reduce_only', 'close_on_cancel'}


@dataclass(frozen=True, slots=True)
class WebhookResponse:
    """Webhook HTTP response (no secrets)."""

    status: int
    body: dict


class AlertError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class Alert:
    """Parsed alert."""

    id: str
    action: str
    symbol: Optional[str]
    side: Optional[str]
    account: Optional[str]
    params: dict = field(default_factory=dict)
    market_position: Optional[str] = None
    prev_market_position: Optional[str] = None
    time: Optional[float] = None


# ─── alert id store ─────────────────────────────────────────────────────────

class AlertStore:
    """Idempotency by id: ``claim`` — True if the id is new (and now taken); the result goes to ``finish``."""

    def claim(self, alert_id: str, now: float) -> Optional[dict]:
        """None — the id is new and now taken; otherwise the saved record (state processing/done/failed, result)."""
        raise NotImplementedError

    def finish(self, alert_id: str, record: dict) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class MemoryAlertStore(AlertStore):
    """In process memory: ``ttl`` seconds, at most ``max_ids`` (oldest are evicted)."""

    def __init__(self, ttl: float = 86_400.0, max_ids: int = 100_000) -> None:
        self.ttl = float(ttl)
        self.max_ids = int(max_ids)
        self._items: OrderedDict[str, tuple[float, dict]] = OrderedDict()

    def claim(self, alert_id: str, now: float) -> Optional[dict]:
        while self._items:
            first_id, (ts, _rec) = next(iter(self._items.items()))
            if now - ts < self.ttl and len(self._items) < self.max_ids:
                break
            del self._items[first_id]
        got = self._items.get(alert_id)
        if got is not None:
            return got[1]
        self._items[alert_id] = (now, {'state': 'processing'})
        return None

    def finish(self, alert_id: str, record: dict) -> None:
        ts = self._items[alert_id][0] if alert_id in self._items else time.time()
        self._items[alert_id] = (ts, record)


class SqliteAlertStore(AlertStore):
    """sqlite (stdlib): alert ids survive a restart — an alert received before a crash is not executed twice."""

    def __init__(self, path: str, ttl: float = 7 * 86_400.0) -> None:
        self.ttl = float(ttl)
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute('CREATE TABLE IF NOT EXISTS tv_alerts (id TEXT PRIMARY KEY, ts REAL, record TEXT)')

    def claim(self, alert_id: str, now: float) -> Optional[dict]:
        self._db.execute('DELETE FROM tv_alerts WHERE ts < ?', (now - self.ttl,))
        cur = self._db.execute('INSERT OR IGNORE INTO tv_alerts (id, ts, record) VALUES (?, ?, ?)',
                               (alert_id, now, json.dumps({'state': 'processing'})))
        if cur.rowcount == 1:
            return None
        row = self._db.execute('SELECT record FROM tv_alerts WHERE id = ?', (alert_id,)).fetchone()
        return json.loads(row[0]) if row else {'state': 'processing'}

    def finish(self, alert_id: str, record: dict) -> None:
        self._db.execute('UPDATE tv_alerts SET record = ? WHERE id = ?', (json.dumps(record, default=str), alert_id))

    def close(self) -> None:
        self._db.close()


# ─── rate limit ─────────────────────────────────────────────────────────────

class _Bucket:
    def __init__(self, per_minute: float, burst: int, clock: Callable[[], float]) -> None:
        self.rate = per_minute / 60.0
        self.capacity = float(max(1, burst))
        self.tokens = self.capacity
        self.clock = clock
        self.at = clock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.at) * self.rate)
        self.at = now

    def available(self) -> bool:
        """Whether a token is available (does not consume it)."""
        self._refill()
        return self.tokens >= 1.0

    def take(self) -> bool:
        self._refill()
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


#: how many sender addresses to remember for the failed-request limit (oldest are evicted)
MAX_SENDERS = 10_000


# ─── parsing ────────────────────────────────────────────────────────────────

def _parse_time(value: Any) -> Optional[float]:
    """``{{timenow}}`` / ``{{time}}``: 2019-08-27T09:56:00Z (UTC); a number — unix seconds or ms."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) / (1000.0 if value > 1e11 else 1.0)
    try:
        text = str(value).strip()
        if text.replace('.', '', 1).isdigit():
            return _parse_time(float(text))
        return datetime.strptime(text, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        raise AlertError(400, f'time: expected yyyy-MM-ddTHH:mm:ssZ, not {value!r}') from None


def _convert(name: str, value: Any) -> Any:
    if value is None or value == '':
        return None
    try:
        if name in _NUMERIC:
            if isinstance(value, bool):
                raise ValueError
            return float(value)
        if name in _INTEGER:
            return int(float(value))
    except (TypeError, ValueError):
        raise AlertError(400, f'{name}: expected a number, not {value!r}') from None
    if name in _BOOL:
        if isinstance(value, bool):
            return value
        if str(value).lower() in ('true', '1', 'yes'):
            return True
        if str(value).lower() in ('false', '0', 'no'):
            return False
        raise AlertError(400, f'{name}: expected true/false, not {value!r}')
    return str(value)


def tv_symbol(symbol: str) -> str:
    """``BTCUSDT.P`` (TradingView perpetual) → ``BTCUSDT``; an exchange prefix ``BYBIT:`` is stripped."""
    s = symbol.strip()
    if ':' in s and '/' not in s:
        s = s.split(':', 1)[1]
    if s.upper().endswith('.P'):
        s = s[:-2]
    return s


# ─── webhook ────────────────────────────────────────────────────────────────

ExchangeSet = Union[Any, Mapping[str, Any]]


class TradingViewWebhook:
    """Receives TradingView alerts (see the module)."""

    def __init__(self, exchanges: ExchangeSet, *, secret: Optional[str] = None, allow_live: bool = False,
                 allowed_symbols: Optional[Iterable[str]] = None, allowed_actions: Iterable[str] = ACTIONS,
                 max_per_minute: float = 30.0, burst: int = 10, max_body: int = 16_384,
                 allowed_ips: Optional[Iterable[str]] = None, trust_forwarded: bool = False,
                 max_age: Optional[float] = 120.0, store: Optional[AlertStore] = None, wait: bool = False,
                 clock: Callable[[], float] = time.monotonic, wall: Callable[[], float] = time.time) -> None:
        secret = secret if secret is not None else os.environ.get(SECRET_ENV)
        if not secret or len(secret) < MIN_SECRET_LEN:
            raise ConfigError(f'TradingView: a secret is required (at least {MIN_SECRET_LEN} characters; '
                              f'secret= or {SECRET_ENV})')
        self._secret = secret.encode()
        self.exchanges: dict[str, Any] = (dict(exchanges) if isinstance(exchanges, Mapping)
                                          else {getattr(exchanges, 'account', None) or exchanges.name: exchanges})
        if not self.exchanges:
            raise ConfigError('TradingView: at least one Exchange is required')
        for name, ex in self.exchanges.items():
            live = not (getattr(ex, 'demo', False) or getattr(ex, 'is_paper', False))
            if live and not allow_live:
                raise ConfigError(f'TradingView: account {name!r} is live; the webhook is demo/paper only by default '
                                  f'(allow_live=True — deliberately)')
        unknown = set(allowed_actions) - set(ACTIONS)
        if unknown:
            raise ConfigError(f'TradingView: unknown actions {sorted(unknown)}; available: {", ".join(ACTIONS)}')
        self.allowed_actions = frozenset(allowed_actions)
        self.allowed_symbols = frozenset(tv_symbol(s).upper().replace('/', '').split(':')[0]
                                         for s in allowed_symbols) if allowed_symbols is not None else None
        self.max_body = int(max_body)
        self.allowed_ips = frozenset(allowed_ips) if allowed_ips is not None else None
        self.trust_forwarded = bool(trust_forwarded)
        self.max_age = max_age
        self.store = store or MemoryAlertStore()
        self.wait = bool(wait)
        self._clock = clock
        self._wall = wall
        self._per_minute = float(max_per_minute)
        self._burst = int(burst)
        self._bucket = _Bucket(max_per_minute, burst, clock)          # alerts with the correct secret
        self._failures: dict[str, _Bucket] = {}                        # failed requests — per address
        self._tasks: set[asyncio.Task] = set()
        self.stats = {'received': 0, 'accepted': 0, 'duplicate': 0, 'rejected': 0, 'unauthorized': 0,
                      'rate_limited': 0, 'done': 0, 'failed': 0}

    @classmethod
    def paper(cls, name: str, *, secret: Optional[str] = None, **kwargs: Any) -> 'TradingViewWebhook':
        """Webhook on the paper account of exchange ``name`` (live book, orders in memory)."""
        from bytx import create_exchange
        hook_kw = {k: kwargs.pop(k) for k in list(kwargs) if k in _HOOK_KWARGS}
        return cls(create_exchange(name, paper=True, **kwargs), secret=secret, **hook_kw)

    def __repr__(self) -> str:
        return f'<TradingViewWebhook accounts={list(self.exchanges)}>'

    # ─── intake ─────────────────────────────────────────────────────────────

    async def handle(self, body: Union[bytes, str], *, content_type: Optional[str] = None,
                     remote: Optional[str] = None, forwarded_for: Optional[str] = None) -> WebhookResponse:
        """Handles the request body; never raises — only returns a response."""
        self.stats['received'] += 1
        try:
            return await self._handle(body, content_type, remote, forwarded_for)
        except AlertError as e:
            key = {401: 'unauthorized', 429: 'rate_limited'}.get(e.status, 'rejected')
            self.stats[key] += 1
            if e.status != 401:
                logger.warning(f'TradingView: alert rejected ({e.status}): {e.message}')
            return WebhookResponse(e.status, {'status': 'error', 'error': e.message})

    async def _handle(self, body: Union[bytes, str], content_type: Optional[str], remote: Optional[str],
                      forwarded_for: Optional[str]) -> WebhookResponse:
        ip = (forwarded_for.split(',')[0].strip() if self.trust_forwarded and forwarded_for else remote)
        if self.allowed_ips is not None and ip not in self.allowed_ips:
            raise AlertError(403, 'sender address not in allowed_ips')
        # Unauthenticated requests spend only their own address's limit (secret brute force → 429),
        # not the shared budget: a flood without the secret does not block a real alert (close included).
        failures = self._failures_of(ip or '')
        if not failures.available():
            raise AlertError(429, 'too many failed requests from this address')
        try:
            data = self._authenticate(body, content_type)
        except AlertError:
            failures.take()
            raise
        if not self._bucket.take():
            raise AlertError(429, 'too many alerts (max_per_minute)')
        alert = self.parse(data)
        ex = self._exchange(alert)
        prev = self.store.claim(alert.id, self._wall())
        if prev is not None:
            self.stats['duplicate'] += 1
            return WebhookResponse(200, {**prev, 'status': 'duplicate', 'id': alert.id})
        self.stats['accepted'] += 1
        task = asyncio.ensure_future(self._execute(ex, alert))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        if not self.wait:
            return WebhookResponse(202, {'status': 'accepted', 'id': alert.id})
        record = await asyncio.shield(task)
        return WebhookResponse(200 if record['state'] == 'done' else 500, {'id': alert.id, **record})

    def _authenticate(self, body: Union[bytes, str], content_type: Optional[str]) -> dict:
        """Body → JSON object with the correct secret (otherwise AlertError 413 / 400 / 401)."""
        raw = body if isinstance(body, bytes) else body.encode()
        if len(raw) > self.max_body:
            raise AlertError(413, 'alert body too large')
        ctype = (content_type or '').split(';')[0].strip().lower()
        try:
            data = json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError, ValueError):
            hint = ' (TradingView sends text/plain if the message is not valid JSON)' if ctype == 'text/plain' else ''
            raise AlertError(400, f'alert must be a JSON object{hint}') from None
        if not isinstance(data, dict):
            raise AlertError(400, 'alert must be a JSON object')
        given = data.get('secret')
        if not isinstance(given, str) or not hmac.compare_digest(given.encode(), self._secret):
            raise AlertError(401, 'invalid secret')
        return data

    def _failures_of(self, ip: str) -> _Bucket:
        bucket = self._failures.get(ip)
        if bucket is None:
            if len(self._failures) >= MAX_SENDERS:
                self._failures.pop(next(iter(self._failures)))
            bucket = self._failures[ip] = _Bucket(self._per_minute, self._burst, self._clock)
        return bucket

    def parse(self, data: dict) -> Alert:
        """Alert JSON → :class:`Alert` (error — 400)."""
        alert_id = data.get('id')
        if not isinstance(alert_id, (str, int)) or not str(alert_id).strip() or len(str(alert_id)) > 200:
            raise AlertError(400, 'id: required (string up to 200), e.g. "{{strategy.order.id}}-{{timenow}}"')
        action = str(data.get('action') or '').strip().lower()
        if action not in ACTIONS:
            raise AlertError(400, f'action: one of {", ".join(ACTIONS)}')
        if action not in self.allowed_actions:
            raise AlertError(403, f'action {action!r} not allowed')
        ts = _parse_time(data.get('time'))
        if ts is not None and self.max_age is not None and self._wall() - ts > self.max_age:
            raise AlertError(400, f'alert is stale: time older than {self.max_age:g} s')
        symbol = data.get('symbol')
        if not isinstance(symbol, str) or not symbol.strip():
            raise AlertError(400, 'symbol: required')
        symbol = tv_symbol(symbol)
        side = data.get('side')
        side = str(side).strip().lower() if side not in (None, '') else None
        mp = str(data['market_position']).lower() if data.get('market_position') not in (None, '') else None
        prev_mp = (str(data['prev_market_position']).lower()
                   if data.get('prev_market_position') not in (None, '') else None)
        for name, v in (('market_position', mp), ('prev_market_position', prev_mp)):
            if v is not None and v not in ('long', 'flat', 'short'):
                raise AlertError(400, f'{name}: long / flat / short')
        allowed = self._allowed_params(action)
        extra = sorted(k for k in data if k not in allowed and k not in _ENVELOPE)
        if extra:
            raise AlertError(400, f'unknown fields for {action}: {", ".join(extra)}')
        if self.allowed_symbols is not None and symbol.upper().replace('/', '').split(':')[0] not in self.allowed_symbols:
            raise AlertError(403, f'symbol {symbol!r} not allowed')
        params = {k: _convert(k, data[k]) for k in allowed if k in data}
        params = {k: v for k, v in params.items() if v is not None}
        account = str(data['account']) if data.get('account') not in (None, '') else None
        alert = Alert(id=str(alert_id).strip(), action=action, symbol=symbol, side=side, account=account,
                      params=params, market_position=mp, prev_market_position=prev_mp, time=ts)
        self._check_side(alert)
        return alert

    @staticmethod
    def _allowed_params(action: str) -> tuple[str, ...]:
        if action == 'open':
            return tuple(dict.fromkeys((*OPEN_PARAMS, *ORDER_PARAMS)))
        if action in ('buy', 'sell'):
            return ORDER_PARAMS
        if action in ALGO_PARAMS:
            return ALGO_PARAMS[action]
        return ()

    @staticmethod
    def _check_side(alert: Alert) -> None:
        a, side = alert.action, alert.side
        if a == 'open':
            if side not in ('long', 'short'):
                raise AlertError(400, 'open: side must be long or short')
            if ('risk_usdt' in alert.params) != ('stop' in alert.params):
                raise AlertError(400, 'open: risk_usdt and stop must be given together')
            if 'risk_usdt' not in alert.params and not any(k in alert.params for k in ('size', 'notional',
                                                                                         'contracts', 'margin')):
                raise AlertError(400, 'open: risk_usdt + stop or size / notional / contracts / margin')
        elif a == 'close':
            if side not in (None, 'long', 'short'):
                raise AlertError(400, 'close: side must be long or short')
        elif a in ALGO_PARAMS:
            if side not in ('buy', 'sell', 'long', 'short'):
                raise AlertError(400, f'{a}: side buy / sell')
            if 'size' not in alert.params:
                raise AlertError(400, f'{a}: size is required')
        elif a in ('buy', 'sell'):
            if alert.market_position != 'flat' and not any(k in alert.params for k in ('size', 'notional',
                                                                                        'contracts', 'margin')):
                raise AlertError(400, f'{a}: size / notional / contracts / margin (or market_position "flat")')

    def _exchange(self, alert: Alert) -> Any:
        if alert.account is not None:
            ex = self.exchanges.get(alert.account)
            if ex is None:
                raise AlertError(400, f'account {alert.account!r} not found')
            return ex
        if len(self.exchanges) != 1:
            raise AlertError(400, 'account: required when there are several accounts')
        return next(iter(self.exchanges.values()))

    # ─── execution ──────────────────────────────────────────────────────────

    async def _execute(self, ex: Any, alert: Alert) -> dict:
        try:
            result = await self.execute(ex, alert)
        except asyncio.CancelledError:
            self.store.finish(alert.id, {'state': 'failed', 'error': 'cancelled'})
            raise
        except Exception as e:
            self.stats['failed'] += 1
            err = scrub(f'{type(e).__name__}: {getattr(e, "message", None) or e}',
                        ccxt_secrets(getattr(ex, 'ccxt', None)))   # response, log and AlertStore — without keys
            record: dict[str, Any] = {'state': 'failed', 'error': err}
            self.store.finish(alert.id, record)
            logger.warning(f'TradingView: alert {alert.id} ({alert.action} {alert.symbol}) not executed: {err}')
            ex.emit('error', symbol=alert.symbol, error=e,
                    data={'source': 'tradingview', 'alert_id': alert.id, 'action': alert.action})
            return record
        self.stats['done'] += 1
        record = {'state': 'done', 'result': result}
        self.store.finish(alert.id, record)
        logger.info(f'TradingView: alert {alert.id} executed: {alert.action} {alert.symbol} → {result}')
        return record

    async def execute(self, ex: Any, alert: Alert) -> dict:
        """Executes a parsed alert on ``ex``; the result is a short dict (order ids, status)."""
        a, p, s = alert.action, dict(alert.params), alert.symbol
        assert s is not None
        if a == 'close' or (a in ('buy', 'sell') and alert.market_position == 'flat'):
            side = alert.side if alert.side in ('long', 'short') else (
                alert.prev_market_position if alert.prev_market_position in ('long', 'short') else None)
            order = await ex.close_position(s, side)
            return {'action': 'close', 'order': _order(order), 'closed': order is not None}
        if a == 'cancel_all':
            report = await ex.cancel_all(s)
            return {'action': 'cancel_all', 'canceled': len(report.canceled), 'errors': len(report.errors),
                    'ok': report.ok}
        if a == 'open' and 'risk_usdt' in p:
            res = await ex.open_position(s, alert.side, **{k: v for k, v in p.items() if k in OPEN_PARAMS})
            return {'action': 'open_position', 'order': _order(getattr(res, 'entry', None)),
                    'size': getattr(getattr(res, 'plan', None), 'size', None)}
        if a in ('open', 'buy', 'sell'):
            side = a if a in ('buy', 'sell') else ('buy' if alert.side == 'long' else 'sell')
            kw = {k: v for k, v in p.items() if k in ORDER_PARAMS}
            order = await ex.place_order(s, side, **kw)
            return {'action': 'order', 'order': _order(order)}
        side = {'long': 'buy', 'short': 'sell'}.get(alert.side or '', alert.side)
        algo = getattr(ex.algos, a)(s, side, **p)
        res = await algo
        return {'action': a, 'algo_id': res.id, 'status': res.status, 'filled': res.filled, 'average': res.average,
                'reason': res.reason}

    async def wait_idle(self) -> None:
        """Waits for background executions (tests, service shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def aclose(self) -> None:
        await self.wait_idle()
        self.store.close()

    # ─── servers ────────────────────────────────────────────────────────────

    def aiohttp_app(self, path: str = '/tradingview') -> Any:
        """aiohttp ``web.Application`` with ``POST path`` (aiohttp is a core dependency)."""
        from aiohttp import web

        async def handler(request: web.Request) -> web.Response:
            if request.content_length is not None and request.content_length > self.max_body:
                return web.json_response({'status': 'error', 'error': 'alert body too large'}, status=413)
            body = await request.read()
            resp = await self.handle(body, content_type=request.headers.get('Content-Type'), remote=request.remote,
                                     forwarded_for=request.headers.get('X-Forwarded-For'))
            return web.json_response(resp.body, status=resp.status)

        app = web.Application(client_max_size=self.max_body + 1024)
        app.router.add_post(path, handler)
        return app

    async def serve(self, host: str = '127.0.0.1', port: int = 8080, path: str = '/tradingview') -> Any:
        """Starts an aiohttp server; returns an ``AppRunner`` (``await runner.cleanup()`` — stop).
        TradingView sends only to ports 80 and 443 — expose 80/443 externally (usually nginx/Caddy with TLS)."""
        from aiohttp import web
        if port not in (80, 443):
            logger.info(f'TradingView: server on {port}; TradingView accepts only ports 80 and 443 — '
                        f'a reverse proxy is required')
        runner = web.AppRunner(self.aiohttp_app(path))
        await runner.setup()
        await web.TCPSite(runner, host, port).start()
        return runner

    def fastapi_router(self, path: str = '/tradingview') -> Any:
        """FastAPI ``APIRouter`` with ``POST path`` (extra ``fastapi``)."""
        try:
            from fastapi import APIRouter, Request
            from fastapi.responses import JSONResponse
        except ImportError as e:  # pragma: no cover
            raise ImportError('fastapi_router: pip install "bytx[fastapi]"') from e
        router = APIRouter()

        async def tradingview_webhook(request: Request) -> JSONResponse:
            body = await request.body()
            client = request.client.host if request.client else None
            resp = await self.handle(body, content_type=request.headers.get('content-type'), remote=client,
                                     forwarded_for=request.headers.get('x-forwarded-for'))
            return JSONResponse(resp.body, status_code=resp.status)

        # annotations as objects: the module uses ``from __future__ import annotations``, the import is local
        tradingview_webhook.__annotations__ = {'request': Request, 'return': JSONResponse}
        router.add_api_route(path, tradingview_webhook, methods=['POST'], include_in_schema=False)
        return router


_ENVELOPE = frozenset({'secret', 'id', 'action', 'symbol', 'side', 'account', 'time', 'market_position',
                       'prev_market_position', 'comment', 'ticker', 'exchange', 'close'})
_HOOK_KWARGS = frozenset({'allow_live', 'allowed_symbols', 'allowed_actions', 'max_per_minute', 'burst', 'max_body',
                          'allowed_ips', 'trust_forwarded', 'max_age', 'store', 'wait', 'clock', 'wall'})


def _order(order: Any) -> Optional[dict]:
    if order is None:
        return None
    return {'id': order.id, 'client_id': order.client_id, 'status': order.status, 'side': order.side,
            'size': order.size, 'filled': order.filled, 'average': order.average}


__all__ = ['ACTIONS', 'ALGO_PARAMS', 'Alert', 'AlertStore', 'MemoryAlertStore', 'SqliteAlertStore',
           'TRADINGVIEW_IPS', 'TRADINGVIEW_TIMEOUT_S', 'TradingViewWebhook', 'WebhookResponse', 'tv_symbol']
