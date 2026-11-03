"""Quotes and order book: a REST snapshot, then a live order book over WebSocket.

Public data only — no keys needed, no orders.

    python examples/01_market_data.py BTC/USDT
"""
from __future__ import annotations

import asyncio
import sys

from bytx import create_exchange
from bytx.types import OrderBook


def describe(book: OrderBook) -> str:
    """Short order book summary for a scalper."""
    bid, ask = book.best_bid, book.best_ask
    if bid is None or ask is None:
        return f'{book.symbol}: empty order book'
    return (f'{book.symbol} bid {bid[0]} × {bid[1]:.4f} | ask {ask[0]} × {ask[1]:.4f} | '
            f'spread {book.spread_bps:.2f} bps | micro {book.microprice:.2f} | '
            f'imbalance(5) {book.imbalance(5):+.2f} | buy 10k USDT ≈ {book.fill_price("buy", notional=10_000)}')


async def main(symbol: str, exchange: str = 'binance', updates: int = 10) -> None:
    async with create_exchange(exchange) as ex:              # public data, no demo needed
        t = await ex.ticker(symbol)
        print(f'{t.symbol} last {t.last} mid {t.mid} spread {t.spread_bps:.2f} bps, 24h {t.change_pct}%')
        print(describe(await ex.order_book(symbol, 20)))
        for c in await ex.candles(symbol, '1m', limit=3):
            print(f'  candle {c.timestamp}: O {c.open} H {c.high} L {c.low} C {c.close} V {c.volume}')

        # live order book: reconnects and resync are handled by the SDK
        ex.stream.on_state(lambda ev: print(f'  [ws] {ev.state} {ev.stream} {ev.reason or ""}'))
        n = 0
        async for book in ex.stream.order_book(symbol, depth=20):
            print(describe(book))
            n += 1
            if n >= updates:
                break
        print('exchange → us latency:', ex.stream.stats()['streams'])


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else 'BTC/USDT'))
