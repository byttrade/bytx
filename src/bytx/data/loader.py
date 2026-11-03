"""Market history loader with an on-disk cache: candles, public trades, funding.

    loader = HistoryLoader(ex, cache_dir='~/.cache/bytx/history')
    h = await loader.candles('BTC/USDT:USDT', '1h', since=t0, until=t1)
    df = h.to_pandas()                       # extra 'pandas'
    h.gaps, h.unavailable, h.complete        # data gaps and intervals the exchange does not serve

Interval ``[since, until)`` in ms. Only what is missing from the cache is downloaded (gap filling);
requests are paged by limits from the exchange docs (``exchanges/<exchange>/history.py``), each window has
both bounds set (Bybit returns candles from the end, OKX ``before`` alone returns the latest records).
Only final data goes to the cache: closed candles, funding up to "now", trades older than
``trades_settle_ms``. Beyond the exchange's history depth (HL — 5000 candles, Binance aggTrades — 48 hours, OKX
trades — 3 months) the interval goes to ``unavailable`` with a reason, with no requests and no caching.
Incomplete data is not an exception: ``complete=False``.
"""
from __future__ import annotations

import asyncio
import dataclasses
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Optional, Union

from bytx import convert
from bytx.data.cache import CacheKey, Fmt, HistoryCache, Interval, default_cache_dir, merge, subtract
from bytx.errors import ConfigError, NotSupported
from bytx.exchanges import history_spec
from bytx.exchanges._history_common import DAY_MS, HOUR_MS
from bytx.types import Candle, FundingPayment, Trade

Kind = Literal['candles', 'trades', 'funding']
#: windows between intermediate cache writes (progress of a long load is not lost)
_SAVE_EVERY = 25
_TRADES_CHUNK_MS = DAY_MS


@dataclass
class History:
    """Series over ``[since, until)``: ``rows`` by time, ``gaps`` — data gaps (candles: missing
    timeframe grid points), ``unavailable`` — (start, end, reason) intervals the exchange does not serve.
    ``fetched`` / ``cached`` — rows downloaded / taken from cache, ``requests`` — requests to the exchange."""

    kind: str
    exchange: str
    symbol: str
    timeframe: Optional[str]
    since: int
    until: int
    rows: list = field(default_factory=list)
    gaps: list[Interval] = field(default_factory=list)
    unavailable: list[tuple[int, int, str]] = field(default_factory=list)
    fetched: int = 0
    cached: int = 0
    requests: int = 0

    @property
    def complete(self) -> bool:
        return not self.gaps and not self.unavailable

    @property
    def columns(self) -> list[str]:
        model = {'candles': Candle, 'trades': Trade, 'funding': FundingPayment}[self.kind]
        return [f.name for f in dataclasses.fields(model) if f.name != 'raw']

    def __len__(self) -> int:
        return len(self.rows)

    def __iter__(self) -> Iterator[Any]:
        return iter(self.rows)

    def __getitem__(self, i: Any) -> Any:
        return self.rows[i]

    def to_pandas(self) -> Any:
        from bytx.data.frames import to_pandas
        return to_pandas(self)

    def to_polars(self) -> Any:
        from bytx.data.frames import to_polars
        return to_polars(self)


class HistoryLoader:
    """History of one exchange (``Exchange``) with an on-disk cache.

    ``cache_dir`` — cache directory (default :func:`~bytx.data.cache.default_cache_dir`),
    ``False`` — no disk (this loader's memory only). ``fmt`` — 'auto' (parquet with pyarrow,
    otherwise csv), 'parquet', 'csv'. ``now_ms`` — clock (tests, replay: virtual time).
    """

    def __init__(self, exchange: Any, *, cache_dir: Union[str, os.PathLike, None, Literal[False]] = None,
                 fmt: Fmt = 'auto', now_ms: Optional[Callable[[], int]] = None,
                 trades_settle_ms: int = 60_000) -> None:
        self.exchange = exchange
        if cache_dir is False:
            self.cache: Optional[HistoryCache] = None
        else:
            root = Path(cache_dir).expanduser() if cache_dir is not None else default_cache_dir()
            self.cache = HistoryCache(root, fmt)
        self._memory: dict[CacheKey, tuple[list[dict], list[Interval]]] = {}
        self._now = now_ms or (lambda: int(time.time() * 1000))
        self.trades_settle_ms = int(trades_settle_ms)
        self._locks: dict[CacheKey, asyncio.Lock] = {}

    # ─── public ────────────────────────────────────────────────────────

    async def candles(self, symbol: str, timeframe: str = '1m', *, since: int,
                      until: Optional[int] = None, refresh: bool = False) -> History:
        """Candles over ``[since, until)`` (by open time). ``refresh=True`` — re-download the interval."""
        ex, s, spec = await self._prepare(symbol)
        lim = self._table(spec.CANDLES, 'candles')
        tf_ms = int(ex.ccxt.parse_timeframe(timeframe) * 1000)
        if tf_ms <= 0:
            raise ConfigError(f'timeframe {timeframe!r}', exchange=ex.name)
        now = self._now()
        until = now if until is None else int(until)
        horizon = (now // tf_ms) * tf_ms if tf_ms <= DAY_MS else now - tf_ms   # closed candles only — earlier
        depth = lim.depth_ms or (lim.depth_rows * tf_ms if lim.depth_rows else None)
        market = ex._market_or_none(s)
        page = lim.page

        async def fetch(a: int, b: int, h: History) -> list[dict]:
            raw = await ex._call(ex.ccxt.fetch_ohlcv, s, timeframe, a, page, spec.candle_params(a, b - 1))
            h.requests += 1
            out = []
            for c in raw or []:
                if a <= int(c[0]) < b:
                    k = convert.candle(c, ex.ccxt.id, market)
                    out.append({'timestamp': k.timestamp, 'open': k.open, 'high': k.high, 'low': k.low,
                                'close': k.close, 'volume': k.volume})
            return out

        key = self._key(ex, 'candles', s, timeframe)
        h = History('candles', ex.name, s, timeframe, int(since), until)
        rows = await self._load(key, h, fetch, window=page * tf_ms, horizon=horizon, depth=depth, now=now,
                                depth_reason=f'{lim.note} ({lim.url})', refresh=refresh)
        h.rows = [Candle(r['timestamp'], r['open'], r['high'], r['low'], r['close'], r['volume']) for r in rows]
        h.gaps = _grid_gaps([c.timestamp for c in h.rows], h.since, min(until, horizon), tf_ms) \
            if tf_ms <= DAY_MS else []      # week/month — the exchange grid is not epoch-aligned
        return h

    async def funding(self, symbol: str, *, since: int, until: Optional[int] = None,
                      refresh: bool = False) -> History:
        """Funding rates over ``[since, until)`` (settlement time)."""
        ex, s, spec = await self._prepare(symbol)
        if ex.market_type == 'spot':
            raise NotSupported('funding exists only for perpetual contracts', exchange=ex.name)
        lim = spec.FUNDING
        now = self._now()
        until = now if until is None else int(until)
        page = lim.page

        async def fetch(a: int, b: int, h: History) -> list[dict]:
            raw = await ex._call(ex.ccxt.fetch_funding_rate_history, s, a, page, spec.funding_params(a, b - 1))
            h.requests += 1
            raw = list(raw or [])
            if len(raw) >= page and b - a > HOUR_MS:
                # full page — part of the window may not fit: split the window in half (exchanges return
                # in different directions — Binance from the start, Bybit/OKX from the end — splitting works for both)
                mid = a + (b - a) // 2
                return await fetch(a, mid, h) + await fetch(mid, b, h)
            return [{'timestamp': int(r['timestamp']), 'rate': float(r['fundingRate'])}
                    for r in raw if r.get('timestamp') is not None and a <= int(r['timestamp']) < b
                    and r.get('fundingRate') is not None]

        key = self._key(ex, 'funding', s, None)
        h = History('funding', ex.name, s, None, int(since), until)
        # window of page records at the most frequent funding (1 hour); more frequent — split on a full page
        rows = await self._load(key, h, fetch, window=page * HOUR_MS, horizon=now, depth=lim.depth_ms, now=now,
                                depth_reason=lim.note, refresh=refresh)
        h.rows = [FundingPayment(s, r['rate'], r['timestamp']) for r in rows]
        return h

    async def trades(self, symbol: str, *, since: int, until: Optional[int] = None,
                     refresh: bool = False) -> History:
        """Public trades over ``[since, until)``. Where the docs give no history (Bybit, BingX, KuCoin, HL) —
        :class:`~bytx.errors.NotSupported` with the reason and a docs link."""
        ex, s, spec = await self._prepare(symbol)
        th = self._table(spec.TRADES, 'trades')
        if not th.supported:
            raise NotSupported(f'public trade history is unavailable per the docs: {th.note} ({th.url})',
                               exchange=ex.name)
        now = self._now()
        until = now if until is None else int(until)
        market = ex._market_or_none(s)
        raw_market = ex.ccxt.market(s)

        async def fetch(a: int, b: int, h: History) -> list[dict]:
            raw = await ex._call(spec.fetch_trades, ex.ccxt, raw_market, a, b - 1, th.page, th.window_ms)
            h.requests += 1
            out = []
            for t in raw or []:
                tr = convert.trade(t, market)
                out.append({'timestamp': int(tr.timestamp or 0), 'id': tr.id, 'side': tr.side,
                            'price': tr.price, 'size': tr.size})
            return out

        key = self._key(ex, 'trades', s, None)
        h = History('trades', ex.name, s, None, int(since), until)
        rows = await self._load(key, h, fetch, window=_TRADES_CHUNK_MS, horizon=now - self.trades_settle_ms,
                                depth=th.depth_ms, now=now, depth_reason=th.note, refresh=refresh)
        h.rows = [Trade(r['id'], s, r['side'], r['price'], r['size'], r['timestamp']) for r in rows]
        return h

    # ─── internal ───────────────────────────────────────────────────────

    async def _prepare(self, symbol: str) -> tuple[Any, str, Any]:
        ex = self.exchange
        await ex.connect()
        s = ex.symbol(symbol)
        spec = history_spec(ex.ccxt.id)
        if spec is None:
            raise NotSupported('history limits are not described for this exchange (exchanges/<exchange>/history.py)',
                               exchange=ex.name)
        return ex, s, spec

    def _table(self, table: dict, what: str) -> Any:
        mt = self.exchange.market_type
        if mt not in table:
            raise NotSupported(f'{what} history for market {mt!r} is not described', exchange=self.exchange.name)
        return table[mt]

    def _key(self, ex: Any, kind: str, s: str, tf: Optional[str]) -> CacheKey:
        return CacheKey(ex.ccxt.id, ex.market_type, bool(getattr(ex, 'demo', False)), kind, s, tf)

    def _read(self, key: CacheKey) -> tuple[list[dict], list[Interval]]:
        if self.cache is None:
            return self._memory.get(key, ([], []))
        return self.cache.load(key)

    def _write(self, key: CacheKey, rows: list[dict], covered: list[Interval]) -> None:
        if self.cache is None:
            self._memory[key] = (rows, covered)
        else:
            self.cache.save(key, rows, covered)

    async def _load(self, key: CacheKey, h: History, fetch: Callable[[int, int, History], Any], *,
                    window: int, horizon: int, depth: Optional[int], now: int, depth_reason: str,
                    refresh: bool) -> list[dict]:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            cached, covered = self._read(key)
            h.cached = 0 if refresh else sum(1 for r in cached if h.since <= r['timestamp'] < h.until)
            if refresh:
                covered = list(subtract_all(covered, h.since, h.until))
            missing = subtract(h.since, h.until, covered)
            earliest = now - depth if depth else None
            fetched: list[dict] = []
            new_cover: list[Interval] = []
            extra: list[dict] = []          # unclosed data (candle, fresh trades) — in the response, not in the cache
            pending = 0

            def flush() -> None:
                nonlocal cached, covered
                extra.extend(r for r in fetched if r['timestamp'] >= horizon)
                h.fetched += sum(1 for r in fetched if r['timestamp'] >= horizon)
                fetched[:] = [r for r in fetched if r['timestamp'] < horizon]
                if not fetched and not new_cover:
                    return
                cached = _merge_rows(key.kind, cached, fetched)
                covered = merge(list(covered) + new_cover)
                self._write(key, cached, covered)
                h.fetched += len(fetched)
                fetched.clear()
                new_cover.clear()

            try:
                for a, b in missing:
                    if earliest is not None and a < earliest:
                        cut = min(b, earliest)
                        h.unavailable.append((a, cut, f'beyond exchange history depth: {depth_reason}'))
                        a = cut
                    t = a
                    while t < b:
                        w_end = min(b, t + window)
                        fetched.extend(await fetch(t, w_end, h))
                        if t < horizon:
                            new_cover.append((t, min(w_end, horizon)))
                        t = w_end
                        pending += 1
                        if pending % _SAVE_EVERY == 0:
                            flush()
            finally:
                # progress is saved even on error: downloaded windows won't need downloading again
                flush()
            in_range = [r for r in cached if h.since <= r['timestamp'] < h.until]
            return _merge_rows(key.kind, in_range, [r for r in extra if h.since <= r['timestamp'] < h.until])


def subtract_all(covered: list[Interval], a: int, b: int) -> list[Interval]:
    """``covered`` minus ``[a, b)`` (``refresh``)."""
    out: list[Interval] = []
    for x, y in covered:
        if y <= a or x >= b:
            out.append((x, y))
            continue
        if x < a:
            out.append((x, a))
        if y > b:
            out.append((b, y))
    return out


def _row_key(kind: str, r: dict) -> Any:
    if kind == 'trades':
        return ('id', r['id']) if r.get('id') else ('t', r['timestamp'], r.get('price'), r.get('size'), r.get('side'))
    return r['timestamp']


def _merge_rows(kind: str, old: list[dict], new: list[dict]) -> list[dict]:
    """Merge without duplicates (new wins), ascending by time."""
    if not new:
        return list(old)
    by: dict[Any, dict] = {_row_key(kind, r): r for r in old}
    for r in new:
        by[_row_key(kind, r)] = r
    return sorted(by.values(), key=lambda r: (r['timestamp'], str(r.get('id') or '')))


def _grid_gaps(ts: list[int], since: int, end: int, tf_ms: int) -> list[Interval]:
    """Timeframe grid gaps in ``[since, end)``: intervals with no candle where one should be."""
    if end <= since:
        return []
    first = since + (-since) % tf_ms          # first grid point not earlier than since
    gaps: list[Interval] = []
    expect = first
    for t in ts:
        if t >= end:
            break
        if t < expect:
            continue
        if t > expect:
            gaps.append((expect, t))
        expect = t + tf_ms
    if expect < end:
        gaps.append((expect, end))
    return gaps


__all__ = ['History', 'HistoryLoader']
