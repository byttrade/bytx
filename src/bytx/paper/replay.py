"""Replay of recorded WS streams on a paper account with a virtual clock.

    ex = replay_exchange('btc.jsonl.gz', paper_config=PaperConfig(balance={'USDT': 1000}))

    async def strategy(ex):
        async for book in ex.stream.order_book('BTC/USDT'):
            if book.spread_bps > 5:
                await ex.buy('BTC/USDT', size=0.01, price=book.best_bid[0], post_only=True)
            await ex.clock.sleep(1)          # virtual time, not real

    result = await ex.replay.run(strategy)
    print(result.summary, result.fingerprint())

Order for each recorded message: clock → message time (waking the strategy's ``clock.sleep`` calls
along the way in deadline order), engine (fills of resting orders, triggers, funding), then
``ex.stream`` streams — and the driver waits until the event loop is idle (the strategy has reacted).

Determinism: same recording, config and strategy → same fills (ids, prices,
amounts, timestamps), as long as the strategy does not depend on real time or external I/O
(``asyncio.sleep`` with a delay, network, ``time.time``) — use ``ex.clock.sleep`` for pauses. On uvloop (no
ready-task queue) the driver waits a fixed number of switches (``settle_ticks``).
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from bytx.errors import ConfigError
from bytx.paper.clock import VirtualClock
from bytx.paper.exchange import PaperConfig, PaperExchange, build_paper
from bytx.paper.record import PathLike, iter_records, read_header
from bytx.types import Balance, Fill, Order, Position

Strategy = Callable[[PaperExchange], Coroutine[Any, Any, Any]]


@dataclass(slots=True)
class ReplayResult:
    """Replay result: orders, fills, positions and balance at the end, PnL summary."""

    orders: list[Order]
    fills: list[Fill]
    positions: list[Position]
    balance: Balance
    summary: dict
    messages: int
    start_ms: Optional[int]
    end_ms: Optional[int]
    strategy_result: Any = None
    events: list = field(default_factory=list, repr=False)

    def fingerprint(self) -> list[tuple]:
        """Fills in comparable form: (id, order, client_id, side, price, amount, fee, maker, time)."""
        return [(f.id, f.order_id, f.client_id, f.side, f.price, f.size, f.fee, f.maker, f.timestamp)
                for f in self.fills]


class Replay:
    """Replay driver for one recording on a :class:`PaperExchange` (see module).

    ``settle_ticks`` — limit of event loop switches per message; ``until_ms`` in
    :meth:`run` — stop at this recording time.
    """

    def __init__(self, exchange: PaperExchange, path: PathLike, *, settle_ticks: int = 10_000,
                 fallback_ticks: int = 64) -> None:
        if not isinstance(exchange.clock, VirtualClock):
            raise ConfigError('replay: a virtual clock is required (replay_exchange)')
        self.exchange = exchange
        self.path = path
        self.clock: VirtualClock = exchange.clock
        self.settle_ticks = settle_ticks
        self.fallback_ticks = fallback_ticks
        self.messages = 0

    async def settle(self) -> None:
        """Waits until all ready event loop tasks have run (strategy, streams, journal)."""
        loop = asyncio.get_running_loop()
        ready = getattr(loop, '_ready', None)
        limit = self.settle_ticks if ready is not None else self.fallback_ticks
        for _ in range(limit):
            await asyncio.sleep(0)
            if ready is not None and not ready:
                return

    async def _advance(self, t: int) -> None:
        ex = self.exchange
        while True:
            wake = self.clock.next_wakeup()
            if wake is None or wake > t:
                break
            self.clock.advance_to(wake)
            ex.engine.tick()
            ex._dispatch()
            await self.settle()
        self.clock.advance_to(t)

    async def run(self, strategy: Optional[Strategy] = None, *, until_ms: Optional[int] = None) -> ReplayResult:
        """Plays the recording; ``strategy(ex)`` is started before the first message and cancelled at the end
        (unless it finished on its own). A strategy error is re-raised."""
        ex = self.exchange
        await ex.connect()
        task = asyncio.get_running_loop().create_task(strategy(ex)) if strategy is not None else None
        await self.settle()
        start: Optional[int] = None
        end: Optional[int] = None
        try:
            for rec in iter_records(self.path):
                if until_ms is not None and rec.t > until_ms:
                    break
                if task is not None and task.done():
                    task.result()   # strategy error — immediately
                await self._advance(rec.t)
                start = rec.t if start is None else start
                end = rec.t
                self.messages += 1
                ex._on_market(rec.kind, [rec.item])
                streams = getattr(ex, '_streams', None)
                if streams is not None and rec.kind != 'funding' and rec.symbol:
                    streams.push_public(rec.kind, rec.symbol, rec.item, rec.timeframe)
                await self.settle()
            result = None
            if task is not None:
                if task.done():
                    result = task.result()
                else:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        except BaseException:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            raise
        if ex.journal is not None:
            await ex.journal.flush()
        eng = ex.engine
        return ReplayResult(orders=eng.orders(), fills=eng.fills(), positions=eng.positions(), balance=eng.balance(),
                            summary=ex.summary(), messages=self.messages, start_ms=start, end_ms=end,
                            strategy_result=result)


def replay_exchange(path: PathLike, *, paper_config: Optional[PaperConfig] = None, settle_ticks: int = 10_000,
                    margin_mode: Optional[str] = None, hedge: Optional[bool] = None, **kwargs: Any) -> PaperExchange:
    """PaperExchange to replay recording ``path`` (exchange, market type and markets from the recording, no network).

    ``ex.replay.run(strategy)`` — play; ``ex.clock`` — virtual clock (starting at the recording
    header time). Other arguments — as in :func:`bytx.create_exchange`
    (``client_id_prefix``, ``journal``, ``pre_submit``, ``client_id_strict``).
    """
    header, markets = read_header(path)
    if not markets:
        raise ConfigError(f'recording {path}: no markets (empty recording?)')
    from bytx.exchange import _check_margin_mode
    mode = _check_margin_mode(margin_mode, header.exchange) if margin_mode is not None else None
    risk = kwargs.pop('risk', None)
    ex = build_paper(header.exchange, market=header.market_type, config=paper_config,
                     clock=VirtualClock(header.created_ms), replay=True, margin_mode=mode, hedge=hedge, **kwargs)
    ex.ccxt.set_markets(list(markets.values()))
    if risk is not None:
        ex.risk = risk
    ex.replay = Replay(ex, path, settle_ticks=settle_ticks)
    return ex


__all__ = ['Replay', 'ReplayResult', 'replay_exchange', 'Strategy']
