"""Grid bot (DEMO by default): post-only limit orders on a grid, a counter order on each fill.

    BYTX_API_KEY=… BYTX_SECRET=… python examples/04_grid_bot.py BTC/USDT 58000 62000 10 0.001

Buys below the current price, sells above. Bought at level i — place a sell at i+1,
sold at i — a buy at i-1. Order state comes from OrderTracker (WS + reconciliation after
a disconnect); the exchange cancels orders if the bot hangs (dead-man switch), and on exit.
"""
from __future__ import annotations

import asyncio
import sys

from _common import exchange_from_env

from bytx import RiskGuard
from bytx.types import Order


def grid_levels(low: float, high: float, n: int) -> list[float]:
    """n levels from low to high inclusive, evenly spaced."""
    if n < 2 or low <= 0 or high <= low:
        raise ValueError('need n >= 2 and 0 < low < high')
    step = (high - low) / (n - 1)
    return [low + i * step for i in range(n)]


def initial_orders(levels: list[float], mid: float) -> list[tuple[str, int]]:
    """(side, level index): buy below mid, sell above; a level exactly at the price is skipped."""
    return [('buy' if price < mid else 'sell', i) for i, price in enumerate(levels) if price != mid]


def counter_order(side: str, index: int, n: int) -> tuple[str, int] | None:
    """Counter order after a fill: buy at i → sell at i+1, sell at i → buy at i-1."""
    j = index + 1 if side == 'buy' else index - 1
    return ('sell' if side == 'buy' else 'buy', j) if 0 <= j < n else None


async def main(symbol: str, low: float, high: float, n: int, size: float) -> None:
    levels = grid_levels(low, high, n)
    ex = exchange_from_env(risk=RiskGuard(max_orders_per_minute=60))
    async with ex, ex.order_tracker(symbol) as tracker, ex.dead_man_switch(60):
        level_of: dict[str, int] = {}

        async def place(side: str, i: int) -> None:
            price = ex.round_price(symbol, levels[i], side=side)  # type: ignore[arg-type]
            o = await ex.place_order(symbol, side, size=size, price=price, post_only=True)  # type: ignore[arg-type]
            level_of[o.id] = i
            print(f'  {side} {size} @ {price} (level {i}) → {o.id}')

        async def on_order(o: Order) -> None:
            i = level_of.pop(o.id, None) if o.status in ('closed', 'canceled') else None
            if i is None or o.status != 'closed' or o.side is None:
                return
            nxt = counter_order(o.side, i, n)
            print(f'filled {o.side} at level {i} @ {o.average or o.price}')
            if nxt is not None:
                await place(*nxt)

        tracker.on_update(on_order)
        mid = (await ex.ticker(symbol)).mid or levels[n // 2]
        for side, i in initial_orders(levels, mid):
            await place(side, i)
        print(f'grid of {n} levels {low}…{high}, Ctrl+C to stop')
        try:
            await asyncio.Event().wait()
        finally:
            # cancel only our own grid orders (by id), with a per-order report
            report = await ex.cancel_all(symbol, order_ids=list(level_of)) if level_of else None
            print('orders canceled:', len(report.canceled) if report else 0)


if __name__ == '__main__':
    a = sys.argv[1:]
    if len(a) != 5:
        sys.exit('usage: 04_grid_bot.py SYMBOL LOW HIGH LEVELS SIZE')
    asyncio.run(main(a[0], float(a[1]), float(a[2]), int(a[3]), float(a[4])))
