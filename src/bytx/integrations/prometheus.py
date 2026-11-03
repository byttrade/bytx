"""Prometheus metrics (extra ``prometheus``): limiters, latencies, errors, WS, orders.

::

    from prometheus_client import start_http_server
    from bytx.integrations.prometheus import PrometheusCollector

    PrometheusCollector(ex1, ex2, pool=pool)      # registers in the default REGISTRY
    start_http_server(9108)                         # or metrics_text() in your own handler

Off by default: without the collector the SDK counts nothing beyond its own accumulators.
The collector **reads at scrape time** what the SDK already accumulates (``ex.metrics()`` — REST per endpoint,
``limiter_state`` — shared limiter, ``ex.stream.stats()`` — WS streams and WS order latencies), so
nothing is added to the hot path. The only thing the collector counts itself is order
events (``ex.subscribe(kinds='order')``: placed / rejected / unknown / canceled) — only while
the collector is attached (``close()`` unsubscribes).

Metrics (labels ``exchange``, ``account``, ``mode`` = live/demo/paper):

- ``bytx_rest_requests_total`` / ``_errors_total`` / ``_retries_total`` — transport counters;
- ``bytx_rest_endpoint_requests_total`` / ``_errors_total`` {endpoint};
- ``bytx_rest_latency_ms`` {endpoint, quantile=0.5|0.9|0.99|max} — over the last 512 samples;
- ``bytx_rate_limit_header`` {header} — numeric exchange rate-limit headers (X-MBX-USED-WEIGHT-1m, …);
- ``bytx_time_offset_ms`` — clock skew;
- ``bytx_limiter_tokens`` / ``_capacity`` / ``_waiting`` / ``_blocked_seconds`` {limiter};
- ``bytx_ws_messages_total`` / ``_reconnects_total`` / ``_errors_total`` / ``_stale_total`` /
  ``_dropped_total`` {stream}, ``bytx_ws_connected`` {stream}, ``bytx_ws_latency_ms`` {stream, quantile};
- ``bytx_ws_order_latency_ms`` {op, route, quantile}, ``bytx_ws_orders_total`` {op, route};
- ``bytx_orders_total`` {event}.

Scraping from the ``start_http_server`` thread: the limiter is read via the loop the collector was created in;
a dict changing during the read is retried (up to 3 times), then the metric is skipped for that scrape.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Iterator, Optional

try:
    from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
    from prometheus_client.registry import REGISTRY, CollectorRegistry
except ImportError as e:  # pragma: no cover — extra not installed
    raise ImportError('bytx.integrations.prometheus: pip install "bytx[prometheus]"') from e

from bytx.net.limiter import limiter_state

_LABELS = ['exchange', 'account', 'mode']
_ORDER_EVENTS = {'order.placed': 'placed', 'order.rejected': 'rejected', 'order.unknown': 'unknown',
                 'order.canceled': 'canceled'}


def _mode(ex: Any) -> str:
    return 'paper' if getattr(ex, 'is_paper', False) else ('demo' if getattr(ex, 'demo', False) else 'live')


def _base(ex: Any) -> list[str]:
    return [ex.name, getattr(ex, 'account', None) or '', _mode(ex)]


def _retry(fn: Callable[[], Any]) -> Any:
    for _ in range(3):
        try:
            return fn()
        except RuntimeError:     # "dictionary changed size during iteration" when scraping from a thread
            continue
    return None


class PrometheusCollector:
    """prometheus_client collector for a set of Exchanges and/or an AccountPool (see the module)."""

    def __init__(self, *exchanges: Any, pool: Any = None, registry: Optional[CollectorRegistry] = REGISTRY,
                 namespace: str = 'bytx') -> None:
        self.ns = namespace
        self._exchanges: list[Any] = []
        self._pool = pool
        self._orders: dict[tuple[str, ...], int] = {}
        self._unsubs: dict[int, Callable[[], None]] = {}
        try:
            self._loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        for ex in exchanges:
            self.add(ex)
        self._registry = registry
        if registry is not None:
            registry.register(self)

    # ─── membership ─────────────────────────────────────────────────────────

    def add(self, ex: Any) -> None:
        if any(x is ex for x in self._exchanges):
            return
        self._exchanges.append(ex)
        self._watch(ex)

    def remove(self, ex: Any) -> None:
        self._exchanges = [x for x in self._exchanges if x is not ex]
        unsub = self._unsubs.pop(id(ex), None)
        if unsub is not None:
            unsub()

    def _watch(self, ex: Any) -> None:
        if id(ex) in self._unsubs:
            return
        base = tuple(_base(ex))

        def on_order(ev: Any) -> None:
            key = (*base, _ORDER_EVENTS[ev.kind])
            self._orders[key] = self._orders.get(key, 0) + 1

        self._unsubs[id(ex)] = ex.subscribe(on_order, kinds=list(_ORDER_EVENTS))

    def exchanges(self) -> list[Any]:
        """Watched Exchanges: explicitly given + clients the pool already created (the pool creates no new ones)."""
        out = list(self._exchanges)
        if self._pool is not None:
            for ex in list(getattr(self._pool, '_clients', {}).values()):
                if not any(x is ex for x in out):
                    self._watch(ex)
                    out.append(ex)
        return out

    def close(self) -> None:
        """Unsubscribes from events and unregisters."""
        for unsub in self._unsubs.values():
            unsub()
        self._unsubs.clear()
        if self._registry is not None:
            try:
                self._registry.unregister(self)
            except KeyError:
                pass
            self._registry = None

    # ─── scrape ─────────────────────────────────────────────────────────────

    def describe(self) -> list:
        return []      # metrics are dynamic — no duplicate check on registration

    def collect(self) -> Iterator[Any]:
        ns = self.ns
        req = CounterMetricFamily(f'{ns}_rest_requests', 'REST requests to the exchange', labels=_LABELS)
        err = CounterMetricFamily(f'{ns}_rest_errors', 'REST errors (network and exchange responses)', labels=_LABELS)
        rty = CounterMetricFamily(f'{ns}_rest_retries', 'Retries of safe requests', labels=_LABELS)
        ep_req = CounterMetricFamily(f'{ns}_rest_endpoint_requests', 'REST requests per endpoint',
                                     labels=[*_LABELS, 'endpoint'])
        ep_err = CounterMetricFamily(f'{ns}_rest_endpoint_errors', 'REST errors per endpoint',
                                     labels=[*_LABELS, 'endpoint'])
        lat = GaugeMetricFamily(f'{ns}_rest_latency_ms', 'REST latency, ms (last 512 samples)',
                                labels=[*_LABELS, 'endpoint', 'quantile'])
        hdr = GaugeMetricFamily(f'{ns}_rate_limit_header', 'Exchange used rate-limit headers',
                                labels=[*_LABELS, 'header'])
        off = GaugeMetricFamily(f'{ns}_time_offset_ms', 'Exchange clock offset relative to local, ms',
                                labels=_LABELS)
        lim = {k: GaugeMetricFamily(f'{ns}_limiter_{k}', d, labels=[*_LABELS, 'limiter']) for k, d in (
            ('tokens', 'Shared limiter tokens (negative — queue debt)'),
            ('capacity', 'Shared limiter capacity'),
            ('waiting', 'Requests waiting in the limiter queue'),
            ('blocked_seconds', 'How long the limiter stays closed after 429/418, s'))}
        ws_cnt = {k: CounterMetricFamily(f'{ns}_ws_{k}', d, labels=[*_LABELS, 'stream']) for k, d in (
            ('messages', 'WS messages'), ('reconnects', 'WS reconnects'), ('errors', 'WS errors'),
            ('stale', 'Silence watchdog triggers'), ('dropped', 'Dropped from overflowing queues'))}
        ws_up = GaugeMetricFamily(f'{ns}_ws_connected', 'Stream connected (1/0)', labels=[*_LABELS, 'stream'])
        ws_lat = GaugeMetricFamily(f'{ns}_ws_latency_ms', 'Exchange → us latency, ms',
                                   labels=[*_LABELS, 'stream', 'quantile'])
        wo_lat = GaugeMetricFamily(f'{ns}_ws_order_latency_ms', 'Latency of ws_* order operations, ms',
                                   labels=[*_LABELS, 'op', 'route', 'quantile'])
        wo_cnt = CounterMetricFamily(f'{ns}_ws_orders', 'ws_* order operations', labels=[*_LABELS, 'op', 'route'])
        orders = CounterMetricFamily(f'{ns}_orders', 'Order events via the SDK', labels=[*_LABELS, 'event'])

        for ex in self.exchanges():
            base = _base(ex)
            snap = _retry(ex.metrics)
            if snap is not None:
                req.add_metric(base, snap.requests)
                err.add_metric(base, snap.errors)
                rty.add_metric(base, snap.retries)
                for name, m in snap.endpoints.items():
                    ep_req.add_metric([*base, name], m.count)
                    ep_err.add_metric([*base, name], m.errors)
                    for q, v in (('0.5', m.p50_ms), ('0.9', m.p90_ms), ('0.99', m.p99_ms), ('max', m.max_ms)):
                        lat.add_metric([*base, name, q], v)
                for h, v in snap.rate_limit.items():
                    try:
                        hdr.add_metric([*base, h], float(v))
                    except (TypeError, ValueError):
                        pass   # Bybit X-Bapi-Limit-Reset-Timestamp etc. — not limit counts
                if snap.time_offset_ms is not None:
                    off.add_metric(base, snap.time_offset_ms)
            key: str = getattr(ex, 'rate_limit_key', None) or ''
            state = _retry(lambda: limiter_state(key, self._loop)) if key else None
            if state:
                lim['tokens'].add_metric([*base, key], state['tokens'])
                lim['capacity'].add_metric([*base, key], state['capacity'])
                lim['waiting'].add_metric([*base, key], state['waiting'])
                lim['blocked_seconds'].add_metric([*base, key], state['blocked_s'])
            streams = getattr(ex, '_streams', None)     # not ex.stream: a scrape must not create the WS hub
            stats = _retry(streams.stats) if streams is not None else None
            if stats:
                for name, s in stats['streams'].items():
                    for k, fam in ws_cnt.items():
                        fam.add_metric([*base, name], s.get(k, 0))
                    ws_up.add_metric([*base, name], 1.0 if s.get('state') == 'connected' else 0.0)
                    lms = s.get('latency_ms') or {}
                    for q, k in (('0.5', 'p50'), ('0.99', 'p99'), ('max', 'max')):
                        if lms.get(k) is not None:
                            ws_lat.add_metric([*base, name, q], lms[k])
                for opr, o in stats.get('orders', {}).items():
                    op, _, route = opr.partition(':')
                    wo_cnt.add_metric([*base, op, route], o.get('count', 0))
                    for q, k in (('0.5', 'p50'), ('0.99', 'p99'), ('max', 'max')):
                        if o.get(k) is not None:
                            wo_lat.add_metric([*base, op, route, q], o[k])
        for (exn, acc, mode, event), n in list(self._orders.items()):
            orders.add_metric([exn, acc, mode, event], n)
        yield from (req, err, rty, ep_req, ep_err, lat, hdr, off, *lim.values(), *ws_cnt.values(), ws_up, ws_lat,
                    wo_lat, wo_cnt, orders)


def metrics_text(registry: Optional[CollectorRegistry] = None) -> tuple[bytes, str]:
    """Body and Content-Type of the /metrics response for your own HTTP handler (aiohttp, FastAPI)."""
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
    return generate_latest(registry if registry is not None else REGISTRY), CONTENT_TYPE_LATEST


__all__ = ['PrometheusCollector', 'metrics_text']
