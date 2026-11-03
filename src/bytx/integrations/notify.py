"""Telegram and Discord notifications: Exchange and journal events, interventions, errors.

::

    notifier = Notifier(TelegramChannel.from_env(), DiscordChannel.from_env())
    notifier.watch(ex)                 # rejects, unknown outcome, TP/SL not placed, WS drop, errors
    notifier.watch(ex, kinds=['order', 'tp_sl', 'protection', 'stream', 'error'])   # + every order
    notifier.watch_journal(journal)    # fills, closes, interventions, discrepancies
    ...
    await notifier.aclose()            # flush the queue and summaries

Channel rules follow the docs (fixtures ``tests/ground_truth/telegram|discord``):

- Telegram ``sendMessage``: ``text`` 1–4096 characters; response ``{"ok": false, "error_code": 429,
    "parameters": {"retry_after": N}}`` — wait N seconds; FAQ: "avoid sending more than one message
    per second" in a single chat → ``min_interval=1.0``.
- Discord Execute Webhook: ``content`` up to 2000 characters, without ``wait`` — ``204 No Content``;
    ``allowed_mentions`` — so the text does not mention people; 429 — JSON ``retry_after`` (seconds,
    float) or the ``Retry-After`` header; ``X-RateLimit-Remaining: 0`` — wait ``X-RateLimit-Reset-After``.

Throttling: the same key (event kind, exchange, account, symbol) at most once per ``throttle`` seconds —
repeats are counted and sent as one summary ("N more: …") when the window ends. The channel queue
is bounded (``max_queue``; on overflow the oldest are dropped, ``dropped`` counter).

Secrets: text goes through :func:`redact` — key values of watched exchanges (apiKey, secret,
password, privateKey), channel tokens, custom ``redact=``, and patterns: Telegram bot token,
Discord webhook URL, ``signature=``/``apiKey=``/… in a query, private key 0x + 64 hex. Send
errors are logged through :func:`redact` too (the Telegram URL contains the token).

The listener only formats and enqueues text — the hot path never waits on the network.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Protocol, Sequence

from loguru import logger

from bytx.errors import SECRET_PARAM_RE, ConfigError
from bytx.events import ExchangeEvent, check_kinds

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class HttpReply:
    """HTTP response for a channel (body — JSON or None)."""

    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    body: Any = None


Post = Callable[[str, dict], Awaitable[HttpReply]]


class SendRetry(Exception):
    """The channel asks to wait ``retry_after`` seconds (429)."""

    def __init__(self, retry_after: float, message: str = '') -> None:
        super().__init__(message or f'retry after {retry_after}')
        self.retry_after = float(retry_after)


class SendFailed(Exception):
    """The channel refused (not 429): the error text is already free of secrets."""


# ─── secrets ────────────────────────────────────────────────────────────────

_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r'\b\d{6,12}:[A-Za-z0-9_-]{30,}\b'), '***'),                         # Telegram bot token
    (re.compile(r'(discord(?:app)?\.com/api/webhooks/\d+/)[A-Za-z0-9_.-]+', re.I), r'\1***'),
    (SECRET_PARAM_RE, r'\1\2***'),                                                 # AccessKeyId=, signature= …
    (re.compile(r'\b0x[0-9a-fA-F]{64}\b'), '0x***'),                                 # EVM private key
)

_MIN_SECRET = 6   # shorter ones are not masked (otherwise '***' replaces ordinary words and numbers)


def redact(text: str, secrets: Iterable[Optional[str]] = ()) -> str:
    """Removes secrets from text: exact ``secrets`` values and known patterns."""
    for s in sorted({str(x) for x in secrets if x and len(str(x)) >= _MIN_SECRET}, key=len, reverse=True):
        text = text.replace(s, '***')
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    return text


def exchange_secrets(ex: Any) -> list[str]:
    """Secrets of the Exchange's ccxt instance (the values themselves are never logged)."""
    raw = getattr(ex, 'ccxt', None)
    return [v for v in (getattr(raw, k, None) for k in ('apiKey', 'secret', 'password', 'privateKey', 'token'))
            if isinstance(v, str) and v]


# ─── channels ───────────────────────────────────────────────────────────────

class Channel(Protocol):
    name: str
    text_max: int
    min_interval: float

    def secrets(self) -> list[str]: ...

    async def send(self, text: str) -> Optional[float]:
        """Sends; returns the pause before the next send (None — default) or raises
        :class:`SendRetry` / :class:`SendFailed`."""
        ...

    async def aclose(self) -> None: ...


class _HttpChannel:
    """Shared: aiohttp session (lazy), proxy, ``post`` replaceable for tests."""

    name = 'http'
    text_max = 4096
    min_interval = 0.0

    def __init__(self, *, post: Optional[Post] = None, proxy: Optional[str] = None, timeout: float = 10.0) -> None:
        self._post = post
        self._proxy = proxy
        self._timeout = float(timeout)
        self._session: Any = None

    def secrets(self) -> list[str]:
        return []

    async def _request(self, url: str, payload: dict) -> HttpReply:
        if self._post is not None:
            return await self._post(url, payload)
        import aiohttp
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))
        async with self._session.post(url, json=payload, proxy=self._proxy) as resp:
            body: Any = None
            if resp.status != 204:
                try:
                    body = await resp.json(content_type=None)
                except Exception:
                    body = None
            return HttpReply(resp.status, dict(resp.headers), body)

    async def aclose(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


class TelegramChannel(_HttpChannel):
    """Telegram bot: ``sendMessage`` to one chat (token from @BotFather, in env, not in code)."""

    name = 'telegram'
    text_max = 4096          # sendMessage: text 1-4096 characters
    min_interval = 1.0       # FAQ: one message per second in a single chat

    def __init__(self, token: str, chat_id: Any, *, api_url: str = 'https://api.telegram.org',
                 parse_mode: Optional[str] = None, silent: bool = False, post: Optional[Post] = None,
                 proxy: Optional[str] = None, timeout: float = 10.0, min_interval: Optional[float] = None) -> None:
        if not token or not chat_id:
            raise ConfigError('Telegram: token and chat_id are required')
        super().__init__(post=post, proxy=proxy, timeout=timeout)
        self._token = str(token)
        self.chat_id = chat_id
        self._api_url = api_url.rstrip('/')
        self.parse_mode = parse_mode
        self.silent = bool(silent)
        if min_interval is not None:
            self.min_interval = float(min_interval)   # group: 20 per minute → 3.0

    @classmethod
    def from_env(cls, prefix: str = 'BYTX_TELEGRAM_', **kwargs: Any) -> 'TelegramChannel':
        """``BYTX_TELEGRAM_TOKEN`` and ``BYTX_TELEGRAM_CHAT_ID``."""
        token, chat = os.environ.get(prefix + 'TOKEN'), os.environ.get(prefix + 'CHAT_ID')
        if not token or not chat:
            raise ConfigError(f'Telegram: set {prefix}TOKEN and {prefix}CHAT_ID')
        return cls(token, chat, **kwargs)

    def __repr__(self) -> str:   # without the token
        return f'<TelegramChannel chat={self.chat_id}>'

    def secrets(self) -> list[str]:
        return [self._token]

    @property
    def url(self) -> str:
        return f'{self._api_url}/bot{self._token}/sendMessage'

    def payload(self, text: str) -> dict:
        data: dict = {'chat_id': self.chat_id, 'text': text[:self.text_max] or ' ',
                      'link_preview_options': {'is_disabled': True}}
        if self.parse_mode:
            data['parse_mode'] = self.parse_mode
        if self.silent:
            data['disable_notification'] = True
        return data

    async def send(self, text: str) -> Optional[float]:
        reply = await self._request(self.url, self.payload(text))
        body = reply.body if isinstance(reply.body, dict) else {}
        if reply.status == 200 and body.get('ok') is True:
            return None
        params = body.get('parameters') if isinstance(body.get('parameters'), dict) else {}
        if reply.status == 429 or body.get('error_code') == 429:
            retry = params.get('retry_after') if params else None
            raise SendRetry(float(retry) if retry is not None else 1.0, str(body.get('description') or '429'))
        raise SendFailed(f'telegram {reply.status}: {body.get("description") or body.get("error_code") or ""}')


class DiscordChannel(_HttpChannel):
    """Discord channel webhook (the webhook URL is a secret: in env, not in code)."""

    name = 'discord'
    text_max = 2000          # content: up to 2000 characters
    min_interval = 0.0       # limit — from the X-RateLimit-* headers

    _URL = re.compile(r'^https://(?:[\w-]+\.)?discord(?:app)?\.com/api/(?:v\d+/)?webhooks/\d+/[\w.-]+/?$')

    def __init__(self, webhook_url: str, *, username: Optional[str] = None, post: Optional[Post] = None,
                 proxy: Optional[str] = None, timeout: float = 10.0) -> None:
        if not webhook_url or not self._URL.match(webhook_url):
            raise ConfigError('Discord: a webhook URL https://discord.com/api/webhooks/<id>/<token> is required')
        super().__init__(post=post, proxy=proxy, timeout=timeout)
        self._url = webhook_url
        self.username = username

    @classmethod
    def from_env(cls, var: str = 'BYTX_DISCORD_WEBHOOK_URL', **kwargs: Any) -> 'DiscordChannel':
        url = os.environ.get(var)
        if not url:
            raise ConfigError(f'Discord: set {var}')
        return cls(url, **kwargs)

    def __repr__(self) -> str:
        return '<DiscordChannel>'

    def secrets(self) -> list[str]:
        return [self._url.rstrip('/').rsplit('/', 1)[-1]]

    def payload(self, text: str) -> dict:
        data: dict = {'content': text[:self.text_max] or ' ', 'allowed_mentions': {'parse': []}}
        if self.username:
            data['username'] = self.username
        return data

    async def send(self, text: str) -> Optional[float]:
        reply = await self._request(self._url, self.payload(text))
        headers = {k.lower(): v for k, v in (reply.headers or {}).items()}
        if 200 <= reply.status < 300:
            if headers.get('x-ratelimit-remaining') == '0':
                return _float(headers.get('x-ratelimit-reset-after'))
            return None
        body = reply.body if isinstance(reply.body, dict) else {}
        if reply.status == 429:
            retry = body.get('retry_after')
            if retry is None:
                retry = _float(headers.get('retry-after'))
            raise SendRetry(float(retry) if retry is not None else 1.0, str(body.get('message') or '429'))
        raise SendFailed(f'discord {reply.status}: {body.get("message") or body.get("code") or ""}')


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


# ─── throttling ─────────────────────────────────────────────────────────────

class Throttle:
    """At most one message per key per ``window`` seconds; skipped ones go as a summary after the window."""

    def __init__(self, window: float, clock: Clock) -> None:
        self.window = float(window)
        self._clock = clock
        self._last: dict[Any, float] = {}
        self._held: dict[Any, tuple[int, str]] = {}     # key → (skipped count, last text)
        self.suppressed = 0

    def admit(self, key: Any, text: str) -> bool:
        if self.window <= 0 or key is None:
            return True
        now = self._clock()
        last = self._last.get(key)
        if last is not None and now - last < self.window:
            count, _ = self._held.get(key, (0, ''))
            self._held[key] = (count + 1, text)
            self.suppressed += 1
            return False
        self._last[key] = now
        if len(self._last) > 10_000:                     # idle keys do not accumulate forever
            for k in [k for k, t in self._last.items() if now - t >= self.window and k not in self._held]:
                del self._last[k]
        return True

    def due(self, *, force: bool = False) -> list[str]:
        """Summaries for keys whose window has expired (``force`` — all)."""
        now = self._clock()
        out = []
        for key, (count, text) in list(self._held.items()):
            if force or now - self._last.get(key, now) >= self.window:
                del self._held[key]
                self._last[key] = now
                out.append(f'{count} more in {self.window:g} s, last: {text}' if count > 1 else text)
        return out


# ─── formatting ─────────────────────────────────────────────────────────────

def _num(x: Any) -> str:
    if x is None:
        return '—'
    if isinstance(x, float):
        return f'{x:.10g}'
    return str(x)


def _head(ev: ExchangeEvent) -> str:
    who = ' '.join(x for x in (ev.exchange, ev.mode, ev.account) if x)
    return f'[{who}]'


def _err(error: Optional[BaseException]) -> str:
    if error is None:
        return ''
    msg = getattr(error, 'message', None) or str(error)
    return f'{type(error).__name__}: {msg}'


def format_event(ev: ExchangeEvent) -> Optional[str]:
    """Notification text for an Exchange event (None — do not notify)."""
    head = _head(ev)
    o = ev.order
    if ev.kind == 'order.placed' and o is not None:
        px = f' @ {_num(o.price)}' if o.price else ''
        return (f'{head} order {o.symbol} {o.side} {o.type} {_num(o.size)}{px} — {o.status}'
                f' (id {o.id}{", cid " + o.client_id if o.client_id else ""})')
    if ev.kind == 'order.canceled' and o is not None:
        what = 'canceled' if ev.data.get('ours', True) else f'already {o.status}'
        return f'{head} order {o.symbol} {o.id} {what}'
    req = ev.data.get('request') or {}
    if ev.kind == 'order.rejected':
        return f'{head} rejected: {req.get("symbol")} {req.get("side")} {_num(req.get("amount"))} — {_err(ev.error)}'
    if ev.kind == 'order.unknown':
        return (f'{head} ORDER OUTCOME UNKNOWN: {req.get("symbol")} {req.get("side")} {_num(req.get("amount"))}'
                f', client_id {req.get("client_id")} — {_err(ev.error)}')
    if ev.kind == 'tp_sl.placed':
        legs = ', '.join(f'{getattr(x, "kind", "?")} {_num(getattr(x, "trigger_price", None))}'
                         for x in ev.data.get('orders') or [])
        return f'{head} TP/SL {ev.symbol}: {legs}'
    if ev.kind == 'protection.failed':
        return f'{head} WARNING: position {ev.symbol} has no TP/SL — {_err(ev.error)}'
    if ev.kind == 'stream.state' and ev.state is not None:
        st = ev.state
        extra = f', attempt {st.attempt}' if st.attempt else ''
        why = f' — {_err(st.error) or st.reason}' if (st.error or st.reason) else ''
        return f'{head} WS {st.stream}: {st.state}{extra}{why}'
    if ev.kind == 'error':
        src = ev.data.get('source')
        return f'{head} error{" " + str(src) if src else ""}: {_err(ev.error) or ev.data.get("message", "")}'
    return None


def format_journal(ev: Any) -> Optional[str]:
    """Notification text for a position journal event."""
    who = ' '.join(str(x) for x in (ev.exchange, ev.bot_id and f'bot {ev.bot_id}') if x)
    kind = f' {ev.kind}' if ev.kind else ''
    size = f' {_num(ev.filled if ev.filled is not None else ev.amount)}' if (ev.filled or ev.amount) else ''
    price = f' @ {_num(ev.price)}' if ev.price else ''
    note = f' — {ev.note}' if ev.note else ''
    order = f' order {ev.exchange_order_id}' if ev.exchange_order_id else ''
    return (f'[{who}] {ev.event}{kind}: {ev.symbol or ""} {ev.position_side or ""}{size}{price}{order}{note}'
            .replace('  ', ' '))


# ─── notifier ───────────────────────────────────────────────────────────────

#: what ``watch(ex)`` sends by default: problems, not every order
DEFAULT_KINDS = ('order.rejected', 'order.unknown', 'protection.failed', 'stream.state', 'error')
#: WS stream states worth reporting (reconnecting/connected are noise)
DEFAULT_STREAM_STATES = ('disconnected', 'stopped')
#: default journal events
DEFAULT_JOURNAL_EVENTS = ('filled', 'closed', 'liquidated', 'adl', 'intervention', 'drift')


@dataclass
class NotifierStats:
    queued: int = 0
    sent: int = 0
    suppressed: int = 0
    dropped: int = 0
    failed: int = 0
    retried: int = 0


class _Lane:
    """Queue and worker of a single channel."""

    def __init__(self, channel: Channel, notifier: 'Notifier') -> None:
        self.channel = channel
        self.n = notifier
        self.queue: deque[str] = deque()
        self.wake = asyncio.Event()
        self.idle = asyncio.Event()
        self.idle.set()
        self.task: Optional[asyncio.Task] = None
        self.next_at = 0.0

    def put(self, text: str) -> None:
        if len(self.queue) >= self.n.max_queue:
            self.queue.popleft()
            self.n.stats.dropped += 1
        self.queue.append(text[:self.channel.text_max])
        self.idle.clear()
        self.wake.set()
        if self.task is None or self.task.done():
            try:
                self.task = asyncio.get_running_loop().create_task(self._run())
            except RuntimeError:       # no loop: flush() / the first call inside a loop will send it
                self.task = None

    async def _run(self) -> None:
        while True:
            if not self.queue:
                self.idle.set()
                self.wake.clear()
                await self.wake.wait()
                continue
            text = self.queue[0]
            wait = self.next_at - self.n._clock()
            if wait > 0:
                await self.n._sleep(wait)
            ok = await self._send(text)
            if self.queue and self.queue[0] is text:
                self.queue.popleft()
            if ok:
                self.n.stats.sent += 1

    async def _send(self, text: str) -> bool:
        ch = self.channel
        for attempt in range(self.n.max_retries + 1):
            try:
                pause = await ch.send(text)
            except SendRetry as e:
                if attempt >= self.n.max_retries:
                    break
                self.n.stats.retried += 1
                await self.n._sleep(max(e.retry_after, ch.min_interval))
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:     # network, SendFailed: the message is lost, the reason goes to the log without secrets
                self.n.stats.failed += 1
                self.next_at = self.n._clock() + ch.min_interval
                logger.warning(self.n.clean(f'notification {ch.name} not sent: {type(e).__name__}: {e}'))
                return False
            self.next_at = self.n._clock() + max(ch.min_interval, pause or 0.0)
            return True
        self.n.stats.failed += 1
        self.next_at = self.n._clock() + ch.min_interval
        logger.warning(f'notification {ch.name} not sent: channel limit (429) after {self.n.max_retries} retries')
        return False


class Notifier:
    """Dispatches events to channels with throttling and without secrets (see the module)."""

    def __init__(self, *channels: Channel, throttle: float = 60.0, max_queue: int = 200, max_retries: int = 3,
                 redact: Iterable[str] = (), stream_states: Sequence[str] = DEFAULT_STREAM_STATES,
                 formatter: Optional[Callable[[ExchangeEvent], Optional[str]]] = None,
                 journal_formatter: Optional[Callable[[Any], Optional[str]]] = None,
                 summary_every: float = 5.0, clock: Clock = time.monotonic, sleep: Sleep = asyncio.sleep) -> None:
        if not channels:
            raise ConfigError('Notifier: at least one channel is required')
        self.channels = list(channels)
        self.max_queue = int(max_queue)
        self.max_retries = int(max_retries)
        self.stream_states = frozenset(stream_states)
        self.formatter = formatter or format_event
        self.journal_formatter = journal_formatter or format_journal
        self.summary_every = float(summary_every)
        self._clock = clock
        self._sleep = sleep
        self._secrets: set[str] = {s for s in redact if s}
        for ch in self.channels:
            self._secrets.update(ch.secrets())
        self.throttle = Throttle(throttle, clock)
        self.stats = NotifierStats()
        self._lanes = [_Lane(ch, self) for ch in self.channels]
        self._unsubs: list[Callable[[], None]] = []
        self._ticker: Optional[asyncio.Task] = None

    # ─── sources ────────────────────────────────────────────────────────────

    def watch(self, ex: Any, *, kinds: Optional[Iterable[str]] = DEFAULT_KINDS) -> Callable[[], None]:
        """Subscribes to Exchange events (``ex.subscribe``); exchange keys are added to the masked list."""
        self._secrets.update(exchange_secrets(ex))
        unsub = ex.subscribe(self._on_event, kinds=check_kinds(kinds))
        self._unsubs.append(unsub)
        return unsub

    def watch_journal(self, journal: Any, *, events: Optional[Iterable[str]] = DEFAULT_JOURNAL_EVENTS
                      ) -> Callable[[], None]:
        """Subscribes to new position journal events (interventions — ``intervention``)."""
        wanted = frozenset(events) if events is not None else None

        def on_journal(ev: Any) -> None:
            if wanted is not None and ev.event not in wanted:
                return
            text = self.journal_formatter(ev)
            if text:
                self.notify(text, key=('journal', ev.exchange, ev.symbol, ev.event, ev.kind, ev.exchange_order_id))

        unsub = journal.subscribe(on_journal)
        self._unsubs.append(unsub)
        return unsub

    def _on_event(self, ev: ExchangeEvent) -> None:
        if ev.kind == 'stream.state' and (ev.state is None or ev.state.state not in self.stream_states):
            return
        text = self.formatter(ev)
        if not text:
            return
        stream = ev.state.stream if ev.state is not None else None
        self.notify(text, key=(ev.kind, ev.exchange, ev.account, ev.symbol, stream))

    # ─── sending ────────────────────────────────────────────────────────────

    def clean(self, text: str) -> str:
        return redact(text, self._secrets)

    def notify(self, text: str, *, key: Any = None) -> bool:
        """Enqueues text to channel queues. ``key`` — throttling key (None — no throttling).
        False — the message is held back by throttling (it will go in a summary)."""
        text = self.clean(text)
        if not self.throttle.admit(key, text):
            self.stats.suppressed += 1
            self._ensure_ticker()
            return False
        self._put(text)
        return True

    def _put(self, text: str) -> None:
        self.stats.queued += 1
        for lane in self._lanes:
            lane.put(text)

    def summaries(self, *, force: bool = False) -> int:
        """Enqueues summaries for expired throttling windows; returns their count."""
        texts = self.throttle.due(force=force)
        for t in texts:
            self._put(t)
        return len(texts)

    def _ensure_ticker(self) -> None:
        if self._ticker is not None and not self._ticker.done():
            return
        try:
            self._ticker = asyncio.get_running_loop().create_task(self._tick())
        except RuntimeError:
            self._ticker = None

    async def _tick(self) -> None:
        while self.throttle._held:
            await self._sleep(self.summary_every)
            self.summaries()

    async def flush(self) -> None:
        """Waits until channel queues are empty."""
        for lane in self._lanes:
            if lane.queue and (lane.task is None or lane.task.done()):
                lane.task = asyncio.get_running_loop().create_task(lane._run())
        await asyncio.gather(*(lane.idle.wait() for lane in self._lanes))

    async def aclose(self, *, summaries: bool = True) -> None:
        """Unsubscribes, sends summaries of held messages, flushes queues, closes sessions."""
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()
        if summaries:
            self.summaries(force=True)
        await self.flush()
        for task in [self._ticker, *(lane.task for lane in self._lanes)]:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        for ch in self.channels:
            await ch.aclose()

    def __repr__(self) -> str:
        return f'<Notifier channels={[c.name for c in self.channels]} sent={self.stats.sent}>'


__all__ = ['DEFAULT_JOURNAL_EVENTS', 'DEFAULT_KINDS', 'DEFAULT_STREAM_STATES', 'DiscordChannel', 'HttpReply',
           'Notifier', 'NotifierStats', 'SendFailed', 'SendRetry', 'TelegramChannel', 'Throttle', 'exchange_secrets',
           'format_event', 'format_journal', 'redact']
