"""A strategy on the Strategy framework that survives restarts.

Mean reversion on the best price stream (``best_bid_ask``): the mid ``entry_bps`` below the EMA —
post-only buy at the bid with TP at the EMA and SL below the entry; the limit order lives at most ``entry_ttl``
seconds (timer). Every order is written to sqlite before sending (``IntentStore``); after a process crash,
startup picks up live orders (``ctx.recovery``) — no second entry is placed, a missing TP/SL is recreated;
a position opened before the crash is taken from the tracker snapshot (``ctx.positions``) — no new entry is
placed on top of it.

    python examples/10_strategy_restart.py BTC/USDT:USDT 0.001            # exchange demo
    python examples/10_strategy_restart.py BTC/USDT:USDT 0.001 --paper    # paper on the live order book

The same class runs on a replayed recording: ``ex.replay.run(lambda ex: run_strategy(MeanRevert(...), ex))``.
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Optional

from _common import exchange_from_env

from bytx import Exchange
from bytx.journal import PositionJournal
from bytx.recovery import IntentStore
from bytx.strategy import Strategy, StrategyContext, Tick, run_strategy


def ema_next(prev: Optional[float], price: float, alpha: float) -> float:
    """Exponential moving average: the first value is the price itself."""
    return price if prev is None else prev + alpha * (price - prev)


def entry_price(ema: Optional[float], bid: Optional[float], ask: Optional[float], entry_bps: float) -> Optional[float]:
    """Entry price (bid) if the mid is more than ``entry_bps`` below the EMA; otherwise None."""
    if ema is None or bid is None or ask is None:
        return None
    mid = (bid + ask) / 2
    return bid if mid < ema * (1 - entry_bps / 10_000) else None


class MeanRevert(Strategy):
    # best bid/ask (bookTicker / bbo): the Binance USDⓈ-M WS ticker (24hrTicker) has no bid/ask — only last
    feeds = ('best_bid_ask',)

    def __init__(self, symbol: str, size: float, *, entry_bps: float = 20.0, stop_pct: float = 1.0,
                 alpha: float = 0.1, entry_ttl: float = 60.0) -> None:
        self.symbols = (symbol,)
        self.symbol, self.size = symbol, size
        self.entry_bps, self.stop_pct, self.alpha, self.entry_ttl = entry_bps, stop_pct, alpha, entry_ttl

    async def on_start(self, ctx: StrategyContext) -> None:
        live = [o for o in (ctx.recovery.live if ctx.recovery else []) if o.symbol == ctx.ex.symbol(self.symbol)]
        entries = [o for o in live if o.side == 'buy' and not o.reduce_only]
        if entries:                                   # restart: entry already placed — no duplicate
            ctx.state['entry'] = entries[0]
        # restart: the entry filled before the crash (or while the process was down) — the position is already
        # open. on_position reports only changes after the start, the current position is in the tracker snapshot
        # (demo 2026-11-04: without this, an entry after kill -9 doubled the position)
        pos = ctx.positions.get(self.symbol, 'long') if ctx.positions is not None else None
        if pos is not None and pos.size:
            ctx.state['position'] = pos.size
        ctx.every(self.entry_ttl, self.expire_entry, name='entry_ttl')

    async def on_tick(self, ctx: StrategyContext, tick: Tick) -> None:
        t = tick.data
        mid = (t.bid + t.ask) / 2 if t.bid and t.ask else t.last
        if mid is None:
            return
        ctx.state['ema'] = ema = ema_next(ctx.state.get('ema'), mid, self.alpha)
        if ctx.state.get('entry') is not None or ctx.state.get('position'):
            return
        price = entry_price(ema, t.bid, t.ask, self.entry_bps)
        if price is None:
            return
        with ctx.bind(position_id=f'mr-{ctx.now_ms()}', role='entry'):
            ctx.state['entry'] = await ctx.ex.place_order(
                self.symbol, 'buy', size=self.size, price=price, post_only=True,
                take_profit=round(ema, 8), stop_loss=price * (1 - self.stop_pct / 100))
        ctx.state['entry_ms'] = ctx.now_ms()

    async def on_order(self, ctx: StrategyContext, order) -> None:
        entry = ctx.state.get('entry')
        if entry is not None and order.id == entry.id and order.status in ('closed', 'canceled', 'expired',
                                                                           'rejected'):
            ctx.state['entry'] = None

    async def on_position(self, ctx: StrategyContext, position) -> None:
        # the runner delivers only positions of the strategy's symbols; the short side of the same symbol (hedge,
        # another bot) is not ours
        if position.side not in (None, 'long'):
            return
        ctx.state['position'] = position.size or 0.0

    async def expire_entry(self, ctx: StrategyContext) -> None:
        entry = ctx.state.get('entry')
        if entry is not None and ctx.now_ms() - ctx.state.get('entry_ms', 0) >= self.entry_ttl * 1000:
            await ctx.ex.cancel_order(self.symbol, entry.id)

    async def on_stop(self, ctx: StrategyContext) -> None:
        entry = ctx.state.get('entry')
        if entry is not None:            # don't leave a live entry; keep the position's TP/SL
            try:
                await ctx.ex.cancel_order(self.symbol, entry.id)
            except Exception as e:  # noqa: BLE001 — stopping must not fail
                print(f'entry {entry.id} not canceled: {e}', file=sys.stderr)


def build(paper: bool) -> Exchange:
    """Exchange with a position journal and an intent store (bot tag = journal tag)."""
    bot = os.environ.get('BYTX_BOT_ID', 'mean-revert-1')
    journal = PositionJournal(bot_id=bot)                 # a service plugs in its own JournalStore
    extra = {'client_id_prefix': journal.tag, 'journal': journal,
             'intents': IntentStore(os.environ.get('BYTX_INTENTS', f'{bot}.sqlite'), account=bot)}
    if paper:
        return Exchange.paper(os.environ.get('BYTX_EXCHANGE', 'binance'), margin_mode='cross', **extra)
    return exchange_from_env(**extra)


async def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        return
    symbol, size = sys.argv[1], float(sys.argv[2])
    ex = build('--paper' in sys.argv)
    try:
        result = await run_strategy(MeanRevert(symbol, size), ex, recover_kwargs={'resubmit': False})
        print(result)
    finally:
        await ex.close()


if __name__ == '__main__':
    asyncio.run(main())
