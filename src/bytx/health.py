"""Exchange health: status from the public status/maintenance endpoint in the exchange docs,
REST and WS latency, clock skew, limiter state; works with and without keys.

    report = await ex.health()            # public; with keys also a private probe (balance)
    if not report.healthy: print(report.status, report.issues)
    reports = await pool.health()

Status (``status``): ``ok``: the exchange reports it is operating; ``maintenance``: trading
maintenance in progress; ``degraded``: partial (some UIDs / WS only / pre-open); ``cancel_only``: order
cancellation only (KuCoin); ``unknown``: the exchange has no documented status endpoint
(BingX, Hyperliquid), so judge by ``reachable`` and latency; ``unreachable``: the probe failed.
Scheduled maintenance is in ``upcoming``. Sources: ``bytx.exchanges.<exchange>.status``.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional

from bytx.errors import BytxError, ConfigError

if TYPE_CHECKING:
    from bytx.exchange import Exchange

Status = Literal['ok', 'maintenance', 'degraded', 'cancel_only', 'unknown', 'unreachable']
_RANK = {'ok': 0, 'unknown': 1, 'degraded': 2, 'cancel_only': 3, 'maintenance': 4, 'unreachable': 5}

#: health clock (tests swap in virtual ones): unix time and a monotonic counter, s
clock: Callable[[], float] = time.time
perf: Callable[[], float] = time.perf_counter


@dataclass(slots=True)
class MaintenanceEvent:
    """Exchange maintenance (normalized status endpoint entry)."""

    state: str                     # as reported by the exchange: scheduled / ongoing / pre_open / completed …
    title: str = ''
    begin: Optional[int] = None    # ms
    end: Optional[int] = None
    affects_trading: bool = True
    partial: bool = False          # some accounts / products
    raw: dict = field(default_factory=dict, repr=False)


@dataclass(slots=True)
class StatusResult:
    status: Status
    source: Optional[str] = None
    events: list[MaintenanceEvent] = field(default_factory=list)
    message: Optional[str] = None
    #: False: the exchange module made no network call (no status endpoint); reachability comes from the time probe
    probed: bool = True


@dataclass(slots=True)
class HealthReport:
    exchange: str
    demo: bool
    status: Status
    status_source: Optional[str] = None
    status_message: Optional[str] = None
    ongoing: list[MaintenanceEvent] = field(default_factory=list)
    upcoming: list[MaintenanceEvent] = field(default_factory=list)
    reachable: bool = False
    #: public probe latency (server time / status), ms
    rest_latency_ms: Optional[float] = None
    #: local clock minus exchange clock, ms (at the request midpoint); None if the exchange does not report time
    clock_offset_ms: Optional[float] = None
    #: offset currently used to sign requests (SDK time sync)
    time_sync_offset_ms: Optional[float] = None
    #: process REST metrics: requests / errors / retries / p50 / p99 per endpoint, rate-limit headers
    rest: dict = field(default_factory=dict)
    #: WS: state and "exchange → us" latency per stream (if streams were started)
    ws: dict = field(default_factory=dict)
    #: limiters: IP limiter key, tokens/queue/block
    limiter: dict = field(default_factory=dict)
    #: with keys, the private probe: {'ok', 'latency_ms', 'error'}
    private: Optional[dict] = None
    issues: list[str] = field(default_factory=list)
    checked_at: float = 0.0

    @property
    def healthy(self) -> bool:
        """Trading is possible: exchange reachable, not in maintenance, private probe (if any) passed,
        limiter not blocked."""
        if not self.reachable or self.status in ('maintenance', 'cancel_only', 'unreachable'):
            return False
        if self.private is not None and not self.private.get('ok'):
            return False
        return not self.limiter.get('blocked_s')


def _has_keys(ex: 'Exchange') -> bool:
    c = ex.ccxt
    return bool(getattr(c, 'apiKey', None) or getattr(c, 'walletAddress', None))


def _rest_summary(ex: 'Exchange') -> dict:
    snap = ex.metrics()
    endpoints = {name: {'count': m.count, 'errors': m.errors, 'p50_ms': m.p50_ms, 'p99_ms': m.p99_ms}
                 for name, m in snap.endpoints.items()}
    return {'requests': snap.requests, 'errors': snap.errors, 'retries': snap.retries,
            'last_latency_ms': snap.last_latency_ms, 'endpoints': endpoints, 'rate_limit': dict(snap.rate_limit)}


def _ws_summary(ex: 'Exchange') -> dict:
    streams = getattr(ex, '_streams', None)
    if streams is None:
        return {}
    stats = streams.stats()
    return {'streams': {name: {'state': s['state'], 'reconnects': s['reconnects'], 'stale': s['stale'],
                               'last_message_age': s['last_message_age'], 'latency_ms': s['latency_ms']}
                        for name, s in stats['streams'].items()},
            'totals': stats['totals']}


def _limiter_summary(ex: 'Exchange') -> dict:
    from bytx.net.limiter import account_key, limiter_state
    key = ex.rate_limit_key
    out: dict[str, Any] = {'key': key, 'ip': limiter_state(key),
                           'account': limiter_state(account_key(key, ex.ccxt))}
    out['blocked_s'] = max((s or {}).get('blocked_s', 0.0) for s in (out['ip'], out['account']))
    return out


async def _probe_time(ex: 'Exchange', report: HealthReport) -> None:
    """Server time: latency and clock offset at the request midpoint (NTP-like, no asymmetry correction)."""
    t0, p0 = clock(), perf()
    server = await ex._call(ex.ccxt.fetch_time, safe=False)
    p1, t1 = perf(), clock()
    report.reachable = True
    report.rest_latency_ms = (p1 - p0) * 1000.0
    if server is not None:
        report.clock_offset_ms = (t0 + t1) / 2 * 1000.0 - float(server)


async def health(ex: 'Exchange', *, private: Optional[bool] = None) -> HealthReport:
    from bytx.portfolio import exchange_module
    if getattr(ex, 'is_paper', False):
        raise ConfigError('health: paper account; check the exchange via a regular client', exchange=ex.name)
    report = HealthReport(exchange=ex.name, demo=ex.demo, status='unknown', checked_at=clock())
    module = exchange_module(ex, 'status')
    try:
        await ex.connect()
    except BytxError as e:
        report.status = 'unreachable'
        report.issues.append(f'connect: {e.message or e}')
    if report.status != 'unreachable':
        if ex.ccxt.has.get('fetchTime') and ex.id != 'hyperliquid':
            try:
                await _probe_time(ex, report)
            except BytxError as e:
                report.issues.append(f'server time: {e.message or e}')
        try:
            p0 = perf()
            result: StatusResult = await module.system_status(ex)
            if result.probed:
                if report.rest_latency_ms is None:
                    report.rest_latency_ms = (perf() - p0) * 1000.0
                report.reachable = True
        except BytxError as e:
            result = StatusResult('unknown', source=getattr(module, 'DOC', None))
            report.issues.append(f'exchange status: {e.message or e}')
        report.status = result.status
        report.status_source = result.source
        report.status_message = result.message
        report.ongoing = [e for e in result.events if e.state != 'scheduled']
        report.upcoming = [e for e in result.events if e.state == 'scheduled']
        if not report.reachable:
            report.status = 'unreachable'
    if private is None:
        private = _has_keys(ex)
    if private and report.status != 'unreachable':
        p0 = perf()
        try:
            await ex._call(ex.ccxt.fetch_balance)
            report.private = {'ok': True, 'latency_ms': (perf() - p0) * 1000.0, 'error': None}
        except BytxError as e:
            report.private = {'ok': False, 'latency_ms': (perf() - p0) * 1000.0,
                              'error': f'{type(e).__name__}: {e.message or e}'}
            report.issues.append(f'private probe: {e.message or e}')
    report.time_sync_offset_ms = ex.metrics().time_offset_ms
    report.rest = _rest_summary(ex)
    report.ws = _ws_summary(ex)
    report.limiter = _limiter_summary(ex)
    if report.limiter.get('blocked_s'):
        report.issues.append(f'limiter blocked for another {report.limiter["blocked_s"]:.1f} s (429/418)')
    return report


def worst(*statuses: Status) -> Status:
    return max(statuses, key=lambda s: _RANK[s])


__all__ = ['HealthReport', 'MaintenanceEvent', 'Status', 'StatusResult', 'health', 'worst']
