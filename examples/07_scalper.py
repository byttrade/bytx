"""Market maker / scalper skeleton (DEMO by default). Not a strategy — a set of techniques.

    python examples/07_scalper.py BTC/USDT 0.001

- post-only quotes near the best prices, skewed by microprice (order book tilt);
- amend instead of cancel+new (fewer requests, keeps queue position where the exchange supports it);
- orders over WS (lower latency, REST fallback inside the SDK);
- state from OrderTracker/PositionTracker (no REST polling);
- protection: RiskGuard (position and rate limits), dead-man switch (the exchange cancels everything
  if the process hangs), kill switch on exit.
"""
from __future__ import annotations

import asyncio
import sys

from _common import exchange_from_env

from bytx import RiskGuard
from bytx.types import OrderBook

REQUOTE_TICKS = 2       # re-quote if the target moved more than N price steps
MAX_POSITION = 2_000    # USDT


def quotes(book: OrderBook, tick: float, inventory: float, max_inventory: float) -> tuple[float, float] | None:
    """Bid/ask prices: near the best prices, skewed toward microprice and against accumulated inventory.

    inventory — position in the quote currency (USDT, long > 0), max_inventory — its limit; the
    larger the long, the lower both quotes (more eager to sell, less eager to buy).
    """
    if not book.bids or not book.asks or book.microprice is None:
        return None
    bid, ask = book.bids[0][0], book.asks[0][0]
    skew_ticks = round((book.microprice - book.mid) / tick)            # type: ignore[operator]
    skew_ticks -= round(2 * inventory / max_inventory) if max_inventory else 0
    q_bid, q_ask = bid + skew_ticks * tick, ask + skew_ticks * tick
    q_bid, q_ask = min(q_bid, ask - tick), max(q_ask, bid + tick)       # don't cross the spread
    return q_bid, q_ask


async def main(symbol: str, size: float) -> None:
    ex = exchange_from_env(risk=RiskGuard(max_position_notional=MAX_POSITION, max_orders_per_minute=120))
    async with ex:
        await ex.connect()
        tick = (await ex.market(symbol)).price_step or 0.1
        async with ex.order_tracker(symbol) as orders, ex.position_tracker(symbol) as positions, \
                ex.dead_man_switch(30):
            live: dict[str, str] = {}          # side → id of our quote
            try:
                async for book in ex.stream.order_book(symbol, depth=10):
                    pos = positions.get(symbol)
                    inventory = positions.net_size(symbol)
                    target = quotes(book, tick, inventory * (book.mid or 0), MAX_POSITION)
                    if target is None:
                        continue
                    for side, price in zip(('buy', 'sell'), target):
                        price = ex.round_price(symbol, price, side=side)       # type: ignore[arg-type]
                        current = orders.get(live[side]) if side in live else None
                        if current is None or not current.is_open:
                            o = await ex.ws_place_order(symbol, side, size=size, price=price,  # type: ignore[arg-type]
                                                        post_only=True)
                            live[side] = o.id
                        elif abs((current.price or 0) - price) >= REQUOTE_TICKS * tick:
                            o = await ex.ws_amend_order(symbol, current.id, price=price, side=side)  # type: ignore[arg-type]
                            live[side] = o.id or current.id
                    if pos is not None:
                        print(f'{pos.side} {pos.size} uPnL {pos.unrealized_pnl}', end='\r')
            finally:
                # emergency: the symbol's whole book (explicit all_orders) and its position
                print('\nemergency stop:', await ex.kill_switch(symbol, all_orders=True))


if __name__ == '__main__':
    a = sys.argv[1:]
    asyncio.run(main(a[0] if a else 'BTC/USDT', float(a[1]) if len(a) > 1 else 0.001))
