"""Paper account on a virtual clock with no network: the test drives the order book and trades.

    ex = paper_exchange('okx', bids=[(99, 10)], asks=[(100, 10)])
    await ex.buy('BTC/USDT:USDT', size=1)                 # filled against the book, taker fee
    set_book(ex, bids=[(101, 5)], asks=[(102, 5)])         # new book → engine and ex.stream streams
    await drive(ex, lambda: algo.done)                     # virtual time until the event

There is no real time: only :func:`drive` (or ``ex.clock.advance``) moves the clock, so the test is deterministic
and never waits. Engine, fees per the docs and liquidation model: :mod:`bytx.paper`.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Iterable, Optional, Sequence

from bytx.paper import PaperConfig, PaperFees, VirtualClock
from bytx.testing.venue import NAMES, SYMBOLS, ccxt_id, markets, offline
from bytx.types import OrderBook, Trade

#: default virtual time start (2023-11-14 22:13:20 UTC)
T0 = 1_700_000_000_000
DEFAULT_CONFIG = PaperConfig(fees=PaperFees(0.0002, 0.0005), balance={'USDT': 100_000.0, 'USDC': 100_000.0})
Level = Sequence[float]


def paper_exchange(name: str = 'binance', *, symbols: Optional[Iterable[str]] = None,
                   bids: Iterable[Level] = ((99.0, 10.0),), asks: Iterable[Level] = ((100.0, 10.0),),
                   clock: Optional[VirtualClock] = None, config: Optional[PaperConfig] = None,
                   **kwargs: Any) -> Any:
    """``PaperExchange`` for exchange ``name`` on a market snapshot, a virtual clock and the test's order book.

    ``symbols``: symbols the test "feeds" (default: the exchange's BTC); the initial book ``bids`` /
    ``asks`` is set on the first one. ``config``: :class:`~bytx.paper.PaperConfig` (default 100 000
    USDT and USDC, maker 0.02 %, taker 0.05 %). ``kwargs``: as in ``create_exchange`` (margin_mode,
    client_id_prefix, journal, intents…)."""
    from bytx.paper.exchange import build_paper
    cid = ccxt_id(name)
    sdk_name = NAMES[cid]
    kwargs.setdefault('margin_mode', 'cross')
    clock = clock or VirtualClock(T0)
    ex = build_paper(sdk_name, config=config or DEFAULT_CONFIG, clock=clock, **kwargs)
    syms = list(symbols or [SYMBOLS[sdk_name]])
    rows = [m for m in markets(cid) if m['symbol'] in syms]
    missing = set(syms) - {m['symbol'] for m in rows}
    if missing:
        raise ValueError(f'missing from the {cid} market snapshot: {sorted(missing)}')
    ex.ccxt.set_markets(rows)
    ex._connected = True
    offline(ex)
    for s in syms:
        ex._fed.add(s)                    # no exchange subscription: the test drives the market
    if bids or asks:
        set_book(ex, bids, asks, syms[0])
    return ex


def _symbol(ex: Any, symbol: Optional[str]) -> str:
    return symbol or next(iter(sorted(ex._fed)))


def set_book(ex: Any, bids: Iterable[Level] = (), asks: Iterable[Level] = (), symbol: Optional[str] = None) -> None:
    """New order book for a symbol (levels (price, size in base)) into the paper engine and ``ex.stream`` streams."""
    s = _symbol(ex, symbol)
    ex._on_market('order_book', [OrderBook(s, [(float(p), float(a)) for p, a, *_ in bids],
                                           [(float(p), float(a)) for p, a, *_ in asks],
                                           timestamp=ex.clock.now_ms())])


def add_trade(ex: Any, side: str, price: float, size: float, symbol: Optional[str] = None) -> None:
    """A public tape trade (fills resting limits through the price if ``fill_on_trades``)."""
    s = _symbol(ex, symbol)
    ex._on_market('trades', [Trade(None, s, side, float(price), float(size), ex.clock.now_ms())])  # type: ignore[arg-type]


async def settle(n: int = 500) -> None:
    """While the event loop has ready tasks (algorithm, streams, journal), yield to them."""
    ready = getattr(asyncio.get_running_loop(), '_ready', None)
    for _ in range(n):
        await asyncio.sleep(0)
        if ready is not None and not ready:
            await asyncio.sleep(0)
            if not ready:
                return


async def drive(ex: Any, done: Callable[[], bool], *, script: Iterable[tuple[int, Callable[[Any], Any]]] = (),
                limit_ms: int = 3_600_000) -> int:
    """Advances virtual time until ``done()`` becomes True.

    ``script``: [(ms from start, fn(ex))]: market events at given times. Returns elapsed ms.
    A deadlock (nobody sleeping and no events) raises AssertionError instead of hanging the test.
    """
    clock = ex.clock
    start = clock.now_ms()
    events = sorted(script, key=lambda e: e[0])
    i = 0
    await settle()
    while not done():
        wake = clock.next_wakeup()
        nxt = start + events[i][0] if i < len(events) else None
        if wake is None and nxt is None:
            raise AssertionError('drive: deadlock; nobody is sleeping and there are no events')
        t = min(x for x in (wake, nxt) if x is not None)
        if t - start > limit_ms:
            raise AssertionError(f'drive: time limit exceeded ({t - start} ms)')
        clock.advance_to(t)
        while i < len(events) and start + events[i][0] <= clock.now_ms():
            events[i][1](ex)
            i += 1
        ex.engine.tick()
        ex._dispatch()
        await settle()
    return int(clock.now_ms() - start)


__all__ = ['DEFAULT_CONFIG', 'T0', 'add_trade', 'drive', 'paper_exchange', 'set_book', 'settle']
