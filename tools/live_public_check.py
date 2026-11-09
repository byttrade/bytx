"""Live check of the SDK's public (no-key) functions on all exchanges.

    .venv/bin/python tools/live_public_check.py [--exchanges binance,okx] [--markets swap,spot]
                                                [--demo] [--ws-seconds 8] [--out report.json]

For each exchange and market type it calls REST methods and WS streams, checks invariants
(bid < ask, positive sizes, fresh timestamps, base vs contracts, REST and WS
consistent) and measures latency. Places no orders, needs no keys.
Not a CI test (needs network) — run manually; the report is JSON + a console summary.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Callable, Optional

from bytx import create_exchange, errors

ALL_EXCHANGES = ('binance', 'bybit', 'okx', 'bingx', 'kucoin', 'hyperliquid', 'bitget', 'gate', 'mexc', 'htx',
                 'blofin')
SYMBOL = {'hyperliquid': 'BTC/USDC'}            # others — BTC/USDT
SPOT_SYMBOL = {'hyperliquid': 'HYPE/USDC'}       # liquid HL spot pair
FRESH_MS = 120_000                               # a "fresh" timestamp is at most 2 minutes old
# KuCoin stays silent ~13–15 s after subscribing, then sends a batch (observed live 2026-10-25)
MIN_WS_SECONDS = {'kucoin': 20.0}
# REST ticker without a timestamp per the docs: Gate — APIv4 FuturesTicker / Ticker models have no time field
TICKER_WITHOUT_TS = frozenset({'gate'})


@dataclass
class Check:
    exchange: str
    market: str
    name: str
    ok: bool
    ms: Optional[float] = None
    detail: str = ''
    problems: list = field(default_factory=list)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _finite(x: Any) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(x)


class Runner:
    def __init__(self, exchange: str, market: str, demo: bool, ws_seconds: float) -> None:
        self.name, self.market, self.demo = exchange, market, demo
        self.ws_seconds = max(ws_seconds, MIN_WS_SECONDS.get(exchange, 0.0))
        self.checks: list[Check] = []
        self.symbol = (SPOT_SYMBOL if market == 'spot' else SYMBOL).get(exchange, 'BTC/USDT')

    async def step(self, name: str, fn: Callable[[], Awaitable[Any]],
                   validate: Optional[Callable[[Any], list]] = None) -> Any:
        t0 = time.perf_counter()
        try:
            result = await asyncio.wait_for(fn(), 30)
        except errors.NotSupported as e:
            self.checks.append(Check(self.name, self.market, name, True, None, f'NotSupported: {e}'))
            return None
        except Exception as e:  # noqa: BLE001 — reporting, keep going
            self.checks.append(Check(self.name, self.market, name, False, (time.perf_counter() - t0) * 1000,
                                     f'{type(e).__name__}: {e}'[:400]))
            return None
        ms = (time.perf_counter() - t0) * 1000
        problems = []
        if validate is not None:
            try:
                problems = validate(result) or []
            except Exception as e:  # noqa: BLE001
                problems = [f'validator crashed: {type(e).__name__}: {e}']
        self.checks.append(Check(self.name, self.market, name, not problems, round(ms, 1),
                                 _summary(result), problems))
        return result

    async def run(self) -> list[Check]:
        # exchange proxy from env: <NAME>_PROXY (e.g. BINANCE_PROXY)
        proxy = os.environ.get(f'{self.name.upper()}_PROXY') or None
        ex = create_exchange(self.name, market=self.market, demo=self.demo, proxy=proxy)
        try:
            await self.step('connect', ex.connect)
            markets = await self.step('markets', lambda: ex.markets(), _v_markets)
            m = await self.step('market', lambda: ex.market(self.symbol), _v_market)
            await self.step('server_time', ex.server_time, lambda t: [] if abs(t - _now_ms()) < 30_000
                            else [f'exchange clock is off by {t - _now_ms()} ms'])
            t = await self.step('ticker', lambda: ex.ticker(self.symbol),
                                lambda x: _v_ticker(x, need_ts=self.name not in TICKER_WITHOUT_TS))
            await self.step('tickers', lambda: ex.tickers([self.symbol]), lambda d: [] if d else ['empty'])
            ob = await self.step('order_book', lambda: ex.order_book(self.symbol, 20), lambda b: _v_book(b))
            if t is not None and ob is not None:
                self._cross_check('ticker~order_book', t, ob)
            await self.step('trades', lambda: ex.trades(self.symbol, limit=20), _v_trades)
            await self.step('candles', lambda: ex.candles(self.symbol, '1m', limit=50), _v_candles)
            if self.market != 'spot':
                await self.step('funding_rate', lambda: ex.funding_rate(self.symbol), _v_funding)
                await self.step('funding_rates', lambda: ex.funding_rates([self.symbol]),
                                lambda d: [] if d else ['empty'])
                await self.step('funding_history', lambda: ex.funding_history(self.symbol, limit=10),
                                lambda xs: [] if xs else ['empty'])
                await self.step('open_interest', lambda: ex.open_interest(self.symbol), _v_oi)
            if m is not None and markets:
                await self.step('symbol_aliases', lambda: _aliases(ex, self.symbol, m), None)
            await self._ws(ex, ob)
        finally:
            await self.step('close', ex.close)
        return self.checks

    def _cross_check(self, name: str, t: Any, ob: Any) -> None:
        problems = []
        if ob.best_bid and t.bid and abs(ob.best_bid[0] - t.bid) / t.bid > 0.005:
            problems.append(f'ticker bid {t.bid} vs order book {ob.best_bid[0]}')
        # a unit error is systematic (×contract_size: 100–1000x); the top of the book is noisy
        if t.bid_size and ob.best_bid and t.bid == ob.best_bid[0] and \
                not (0.01 < t.bid_size / ob.best_bid[1] < 100):
            problems.append(f'best bid size: ticker {t.bid_size} vs order book {ob.best_bid[1]} (units?)')
        self.checks.append(Check(self.name, self.market, name, not problems, None, '', problems))

    async def _ws(self, ex: Any, rest_book: Any) -> None:
        streams = {
            'ws.order_book': lambda: ex.stream.order_book(self.symbol, depth=20),
            'ws.ticker': lambda: ex.stream.ticker(self.symbol),
            'ws.best_bid_ask': lambda: ex.stream.best_bid_ask(self.symbol),
            'ws.trades': lambda: ex.stream.trades(self.symbol),
            'ws.candles': lambda: ex.stream.candles(self.symbol, '1m'),
        }
        for name, make in streams.items():
            await self.step(name, lambda make=make, name=name: self._collect(make, name, rest_book),
                            lambda r: r['problems'])
        stats = ex.stream.stats()
        self.checks.append(Check(self.name, self.market, 'ws.stats', True, None,
                                 json.dumps(stats, default=str)[:600]))

    async def _collect(self, make: Callable[[], Any], name: str, rest_book: Any) -> dict:
        sub = make()
        items, first_ms = [], None
        t0 = time.perf_counter()
        deadline = t0 + self.ws_seconds
        try:
            while time.perf_counter() < deadline:
                try:
                    item = await asyncio.wait_for(sub.__anext__(), max(0.1, deadline - time.perf_counter()))
                except asyncio.TimeoutError:
                    break
                if first_ms is None:
                    first_ms = (time.perf_counter() - t0) * 1000
                items.append(item)
        finally:
            await sub.aclose()
        problems = [] if items else [f'no data for {self.ws_seconds} s']
        for it in items[-3:]:
            if name == 'ws.order_book':
                problems += _v_book(it)
                if rest_book is not None and it.best_bid and rest_book.best_bid and \
                        abs(it.best_bid[0] - rest_book.best_bid[0]) / rest_book.best_bid[0] > 0.01:
                    problems.append(f'WS bid {it.best_bid[0]} far from REST {rest_book.best_bid[0]}')
                if rest_book is not None and it.bids and rest_book.bids:
                    # units: sum of 20 levels; a unit error is a systematic contract_size
                    # multiplier (100–1000x), not noise at the thin top of the book
                    ws_v = sum(sz for _, sz in it.bids[:20])
                    rest_v = sum(sz for _, sz in rest_book.bids[:20])
                    if rest_v and not (0.01 < ws_v / rest_v < 100):
                        problems.append(f'WS ({ws_v:.4g}) and REST ({rest_v:.4g}) sizes in different units?')
            elif name in ('ws.ticker', 'ws.best_bid_ask'):
                if it.bid and it.ask and it.bid >= it.ask:
                    problems.append(f'bid {it.bid} >= ask {it.ask}')
            elif name == 'ws.trades':
                problems += _v_trades([it])
            elif name == 'ws.candles':
                problems += _v_candles([it])
        return {'count': len(items), 'first_ms': None if first_ms is None else round(first_ms),
                'problems': sorted(set(problems))}


async def _aliases(ex: Any, symbol: str, m: Any) -> dict:
    base, quote = m.base, m.quote
    variants = [f'{base}{quote}', f'{base}-{quote}', f'{base.lower()}_{quote.lower()}', m.id]
    got = {v: ex.symbol(v) for v in variants}
    bad = {v: s for v, s in got.items() if s != m.symbol}
    if bad:
        raise AssertionError(f'symbol not recognized: {bad}')
    return got


def _summary(x: Any) -> str:
    if x is None:
        return ''
    if isinstance(x, list):
        return f'{len(x)} items'
    if isinstance(x, dict):
        return f'{len(x)} keys'
    text = repr(x)
    return text[:240]


def _v_markets(ms: list) -> list:
    out = [] if ms else ['no markets']
    bad = [m.symbol for m in ms[:500] if not m.contract_size or m.contract_size <= 0]
    if bad:
        out.append(f'contract_size <= 0: {bad[:5]}')
    return out


def _v_market(m: Any) -> list:
    out = []
    if not m.amount_step or m.amount_step <= 0:
        out.append(f'amount_step={m.amount_step}')
    if not m.price_step or m.price_step <= 0:
        out.append(f'price_step={m.price_step}')
    if m.min_cost is None and m.min_amount is None:
        out.append('neither min_cost nor min_amount')
    if not m.active:
        out.append('market inactive')
    return out


def _v_ticker(t: Any, need_ts: bool = True) -> list:
    out = []
    if not (_finite(t.bid) and _finite(t.ask)):
        out.append(f'no bid/ask: {t.bid}/{t.ask}')
    elif t.bid >= t.ask:
        out.append(f'bid {t.bid} >= ask {t.ask}')
    if not _finite(t.last):
        out.append('no last')
    if t.timestamp is None:
        if need_ts:
            out.append('no timestamp')
    elif abs(_now_ms() - t.timestamp) > FRESH_MS:
        out.append(f'timestamp stale by {(_now_ms() - t.timestamp) / 1000:.0f} s')
    return out


def _v_book(b: Any) -> list:
    out = []
    if not b.bids or not b.asks:
        return ['empty order book side']
    if b.bids[0][0] >= b.asks[0][0]:
        out.append(f'crossed: {b.bids[0][0]} >= {b.asks[0][0]}')
    if any(b.bids[i][0] <= b.bids[i + 1][0] for i in range(len(b.bids) - 1)):
        out.append('bids not descending')
    if any(b.asks[i][0] >= b.asks[i + 1][0] for i in range(len(b.asks) - 1)):
        out.append('asks not ascending')
    if any(s <= 0 for _, s in b.bids + b.asks):
        out.append('zero/negative level size')
    if b.timestamp and abs(_now_ms() - b.timestamp) > FRESH_MS:
        out.append(f'timestamp stale by {(_now_ms() - b.timestamp) / 1000:.0f} s')
    return out


def _v_trades(ts: list) -> list:
    out = []
    for t in ts:
        if not (_finite(t.price) and t.price > 0 and _finite(t.size) and t.size > 0):
            out.append(f'trade price/size: {t.price}/{t.size}')
        if t.side not in ('buy', 'sell', None):
            out.append(f'side={t.side}')
        if t.timestamp and abs(_now_ms() - t.timestamp) > 24 * 3600_000:
            out.append('trade older than a day')
    return sorted(set(out))


def _v_candles(cs: list) -> list:
    out = []
    if not cs:
        return ['empty']
    for c in cs:
        if not (c.low <= min(c.open, c.close) <= max(c.open, c.close) <= c.high):
            out.append(f'OHLC violated: {c}')
        if c.volume < 0:
            out.append('negative volume')
    ts = [c.timestamp for c in cs]
    if ts != sorted(ts):
        out.append('candles out of order')
    if len(cs) > 1 and abs(_now_ms() - ts[-1]) > 5 * 60_000:
        out.append(f'last candle is stale by {(_now_ms() - ts[-1]) / 60000:.0f} min')
    return sorted(set(out))[:5]


def _v_funding(f: Any) -> list:
    out = []
    if not _finite(f.rate) or abs(f.rate) > 0.05:
        out.append(f'rate={f.rate}')
    if f.next_funding_time is not None and f.next_funding_time < _now_ms() - 60_000:
        out.append(f'next_funding_time in the past: {f.next_funding_time}')
    return out


def _v_oi(o: Any) -> list:
    if o.size is None and o.value is None:
        return ['neither size nor value']
    if o.size is not None and o.size <= 0:
        return [f'size={o.size}']
    return []


_QUIET = ('no data for', 'candle is stale')


def _soften_demo(results: list) -> None:
    """Demo/testnet: a quiet market (no trades/changes) and an honest "demo unavailable" are
    not SDK errors, just a warning/info."""
    for c in results:
        if not c.ok and c.name == 'runner' and 'NotSupported' in c.detail:
            c.ok, c.detail = True, f'info: {c.detail}'
        elif not c.ok and c.problems and all(any(q in p for q in _QUIET) for p in c.problems):
            c.ok, c.detail = True, f'warning (quiet test market): {c.problems}'
            c.problems = []


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--exchanges', default=','.join(ALL_EXCHANGES))
    ap.add_argument('--markets', default='swap,spot')
    ap.add_argument('--demo', action='store_true')
    ap.add_argument('--ws-seconds', type=float, default=8)
    ap.add_argument('--out', default='live_public_report.json')
    a = ap.parse_args()
    runners = [Runner(e, mk, a.demo, a.ws_seconds) for e in a.exchanges.split(',') for mk in a.markets.split(',')]

    async def safe(r: Runner) -> list[Check]:
        try:
            return await r.run()
        except Exception as e:  # noqa: BLE001
            return r.checks + [Check(r.name, r.market, 'runner', False, None,
                                     f'{type(e).__name__}: {e}\n{traceback.format_exc()[-800:]}')]

    results = [c for chunk in await asyncio.gather(*(safe(r) for r in runners)) for c in chunk]
    if a.demo:
        _soften_demo(results)
    with open(a.out, 'w') as f:
        json.dump([asdict(c) for c in results], f, ensure_ascii=False, indent=1, default=str)
    bad = [c for c in results if not c.ok]
    print(f'checks {len(results)}, problems {len(bad)} → {a.out}')
    for c in bad:
        print(f'✗ {c.exchange}/{c.market} {c.name}: {c.detail[:200]} {c.problems}')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(asyncio.run(main()))
