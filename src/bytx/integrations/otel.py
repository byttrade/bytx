"""OpenTelemetry tracing (extra ``otel``): spans for REST, WS and orders.

::

    from bytx.integrations.otel import instrument
    undo = instrument(ex)                 # global TracerProvider or tracer_provider=
    ...
    undo()                                # remove the wrappers

Off by default: without :func:`instrument` the SDK has no tracing checks at all — wrappers
are installed on the **instance** (``ex._submit_order``, ``ex._cancel``, ``ex._ws_timed``, ``ex.ccxt.fetch``)
and removed by ``undo()``.

Spans (attributes ``bytx.exchange``, ``bytx.account``, ``bytx.mode``):

- ``bytx.order.place`` — placement (REST, a batch element-wise without createOrders): symbol, side,
  type, amount, price, client_id, reduce_only → order.id, order.status;
- ``bytx.order.cancel`` — cancel: symbol, order.id / client_id → order.status, ours;
- ``bytx.ws.order`` — ws_place/amend/cancel: op, route (ws|rest);
- ``bytx.rest`` — every HTTP request to the exchange: ``http.request.method``, ``url.path`` (no query —
    it holds signatures and keys), ``bytx.endpoint`` (ccxt endpoint template);
- ``bytx.ws.reconnect`` — from ``disconnected``/``reconnecting``/``stale`` to ``connected``/
    ``resynced`` (``stopped`` — ERROR status), events — intermediate states, time — from StateEvent.

The REST span is nested in the order span (context via contextvars). On error: ERROR status, type in
``bytx.error.type``, text via :func:`bytx.integrations.notify.redact` (exchange keys,
signature=… never reach traces); ``record_exception`` is not used (no stacktrace with locals,
but a ccxt exception text may contain a signed URL).
"""
from __future__ import annotations

import functools
import time
from typing import Any, Callable, Optional

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, SpanKind, Status, StatusCode
except ImportError as e:  # pragma: no cover — extra not installed
    raise ImportError('bytx.integrations.otel: pip install "bytx[otel]"') from e

from bytx.__version__ import __version__
from bytx.integrations.notify import exchange_secrets, redact
from bytx.net.metrics import _ENDPOINT

_MARK = '_bytx_otel'
_END_STATES = {'connected', 'resynced'}
_START_STATES = {'disconnected', 'reconnecting', 'stale'}


def _mode(ex: Any) -> str:
    return 'paper' if getattr(ex, 'is_paper', False) else ('demo' if getattr(ex, 'demo', False) else 'live')


def _path(url: str) -> str:
    rest = url.split('://', 1)[-1]
    slash = rest.find('/')
    path = rest[slash:] if slash >= 0 else '/'
    return path.split('?', 1)[0]


def _host(url: str) -> str:
    return url.split('://', 1)[-1].split('/', 1)[0]


def _clean_attrs(attrs: dict) -> dict:
    return {k: v for k, v in attrs.items() if v is not None}


class _Instrumentation:
    def __init__(self, ex: Any, tracer: Any) -> None:
        self.ex = ex
        self.tracer = tracer
        self.base = _clean_attrs({'bytx.exchange': ex.name, 'bytx.account': getattr(ex, 'account', None),
                                  'bytx.mode': _mode(ex), 'bytx.market_type': getattr(ex, 'market_type', None)})
        self.secrets = exchange_secrets(ex)
        self._reconnects: dict[str, Span] = {}
        self._undo: list[Callable[[], None]] = []

    # ─── errors ─────────────────────────────────────────────────────────────

    def fail(self, span: Span, error: BaseException) -> None:
        msg = redact(getattr(error, 'message', None) or str(error), self.secrets)[:500]
        span.set_attribute('bytx.error.type', type(error).__name__)
        code = getattr(error, 'code', None)
        if code is not None:
            span.set_attribute('bytx.error.code', str(code))
        span.set_status(Status(StatusCode.ERROR, f'{type(error).__name__}: {msg}'))

    # ─── wrappers ───────────────────────────────────────────────────────────

    def install(self) -> None:
        ex = self.ex
        self._wrap_method('_submit_order', self._submit_order)
        self._wrap_method('_cancel', self._cancel)
        self._wrap_method('_ws_timed', self._ws_timed)
        raw = ex.ccxt
        inner = raw.fetch
        tracer, base, inst = self.tracer, self.base, self

        async def fetch(url: str, method: str = 'GET', headers: Any = None, body: Any = None) -> Any:
            attrs = {**base, 'http.request.method': method, 'url.path': _path(url), 'server.address': _host(url)}
            endpoint = _ENDPOINT.get()
            if endpoint:
                attrs['bytx.endpoint'] = endpoint
            with tracer.start_as_current_span('bytx.rest', kind=SpanKind.CLIENT, attributes=attrs,
                                              record_exception=False, set_status_on_exception=False) as span:
                try:
                    return await inner(url, method, headers, body)
                except BaseException as e:
                    inst.fail(span, e)
                    raise

        raw.fetch = fetch

        def restore_fetch() -> None:
            if raw.fetch is fetch:
                raw.fetch = inner

        self._undo.append(restore_fetch)
        remove = ex.stream.on_state(self._on_state)
        self._undo.append(remove)

    def _wrap_method(self, name: str, make: Callable[[Callable[..., Any]], Callable[..., Any]]) -> None:
        ex = self.ex
        had_own = name in vars(ex)
        original = getattr(ex, name)
        wrapped = make(original)
        setattr(ex, name, wrapped)

        def undo() -> None:
            if vars(ex).get(name) is wrapped:
                if had_own:
                    setattr(ex, name, original)
                else:
                    delattr(ex, name)

        self._undo.append(undo)

    def _submit_order(self, original: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(original)
        async def submit(request: dict, **kwargs: Any) -> Any:
            attrs = _clean_attrs({**self.base, 'bytx.symbol': request.get('symbol'),
                                  'bytx.order.side': request.get('side'), 'bytx.order.type': request.get('type'),
                                  'bytx.order.amount': request.get('amount'),
                                  'bytx.order.price': request.get('price'),
                                  'bytx.order.client_id': request.get('client_id'),
                                  'bytx.order.reduce_only': bool(request.get('reduce_only'))})
            with self.tracer.start_as_current_span('bytx.order.place', kind=SpanKind.INTERNAL, attributes=attrs,
                                                   record_exception=False, set_status_on_exception=False) as span:
                try:
                    order = await original(request, **kwargs)
                except BaseException as e:
                    self.fail(span, e)
                    raise
                if order.id:
                    span.set_attribute('bytx.order.id', str(order.id))
                if order.status:
                    span.set_attribute('bytx.order.status', str(order.status))
                return order
        return submit

    def _cancel(self, original: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(original)
        async def cancel(s: str, order_id: Optional[str], client_id: Optional[str], **kwargs: Any) -> Any:
            attrs = _clean_attrs({**self.base, 'bytx.symbol': s, 'bytx.order.id': order_id,
                                  'bytx.order.client_id': client_id})
            with self.tracer.start_as_current_span('bytx.order.cancel', attributes=attrs,
                                                   record_exception=False, set_status_on_exception=False) as span:
                try:
                    order, ours = await original(s, order_id, client_id, **kwargs)
                except BaseException as e:
                    self.fail(span, e)
                    raise
                if order.status:
                    span.set_attribute('bytx.order.status', str(order.status))
                span.set_attribute('bytx.order.canceled_by_us', bool(ours))
                return order, ours
        return cancel

    def _ws_timed(self, original: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(original)
        async def timed(op: str, use_ws: bool, fn: Callable[..., Any], *args: Any) -> Any:
            attrs = {**self.base, 'bytx.ws.op': op, 'bytx.ws.route': 'ws' if use_ws else 'rest'}
            with self.tracer.start_as_current_span('bytx.ws.order', attributes=attrs,
                                                   record_exception=False, set_status_on_exception=False) as span:
                try:
                    return await original(op, use_ws, fn, *args)
                except BaseException as e:
                    self.fail(span, e)
                    raise
        return timed

    # ─── WS: reconnects ─────────────────────────────────────────────────────

    def _on_state(self, ev: Any) -> None:
        ts = int((ev.timestamp or time.time()) * 1e9)
        span = self._reconnects.get(ev.stream)
        attrs = _clean_attrs({'attempt': ev.attempt or None, 'delay': ev.delay, 'reason': ev.reason,
                              'error.type': type(ev.error).__name__ if ev.error is not None else None})
        if ev.state in _START_STATES:
            if span is None:
                span = self.tracer.start_span('bytx.ws.reconnect', start_time=ts, attributes=_clean_attrs({
                    **self.base, 'bytx.ws.stream': ev.stream, 'bytx.ws.kind': ev.kind,
                    'bytx.symbol': ev.symbol}))
                self._reconnects[ev.stream] = span
            span.add_event(ev.state, attributes=attrs, timestamp=ts)
            return
        if span is None:
            return
        if ev.state in _END_STATES:
            span.set_attribute('bytx.ws.attempts', int(ev.attempt or 0))
            span.end(end_time=ts)
            del self._reconnects[ev.stream]
        elif ev.state == 'stopped':
            if ev.error is not None:
                self.fail(span, ev.error)
            else:
                span.set_status(Status(StatusCode.ERROR, 'stopped'))
            span.end(end_time=ts)
            del self._reconnects[ev.stream]
        else:
            span.add_event(ev.state, attributes=attrs, timestamp=ts)

    def uninstall(self) -> None:
        for undo in reversed(self._undo):
            undo()
        self._undo.clear()
        for span in self._reconnects.values():
            span.end()
        self._reconnects.clear()


def instrument(ex: Any, *, tracer_provider: Any = None) -> Callable[[], None]:
    """Installs spans on the Exchange (see the module); a repeated call reuses the wrappers. Returns ``undo``."""
    existing: Optional[_Instrumentation] = getattr(ex, _MARK, None)
    if existing is not None:
        return functools.partial(uninstrument, ex)
    provider = tracer_provider if tracer_provider is not None else trace.get_tracer_provider()
    inst = _Instrumentation(ex, provider.get_tracer('bytx', __version__))
    inst.install()
    setattr(ex, _MARK, inst)
    return functools.partial(uninstrument, ex)


def uninstrument(ex: Any) -> None:
    """Removes tracing wrappers from the Exchange."""
    inst: Optional[_Instrumentation] = getattr(ex, _MARK, None)
    if inst is None:
        return
    inst.uninstall()
    delattr(ex, _MARK)


__all__ = ['instrument', 'uninstrument']
