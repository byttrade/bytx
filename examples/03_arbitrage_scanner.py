"""Cross-exchange spread scanner for perpetual futures (read-only, no keys).

    python examples/03_arbitrage_scanner.py BTC ETH SOL

For each coin: where to buy cheapest (ask) and sell highest (bid), and the spread after
taker fees. Places no orders.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

from bytx import create_exchange, errors
from bytx.types import Ticker

EXCHANGES = ('binance', 'bybit', 'okx', 'bingx', 'hyperliquid')
TAKER_FEE = 0.0005  # rough estimate; exact — ex.trading_fee(symbol)


@dataclass
class Spread:
    coin: str
    buy_on: str
    sell_on: str
    ask: float
    bid: float
    gross_bps: float
    net_bps: float


def best_spread(coin: str, tickers: dict[str, Ticker], fee: float = TAKER_FEE) -> Spread | None:
    """Best "buy on A at ask → sell on B at bid" pair (A ≠ B)."""
    quotes = {ex: t for ex, t in tickers.items() if t.bid and t.ask}
    best: Spread | None = None
    for buy_on, tb in quotes.items():
        for sell_on, ts in quotes.items():
            if buy_on == sell_on:
                continue
            gross = (ts.bid - tb.ask) / tb.ask * 10_000  # type: ignore[operator]
            spread = Spread(coin, buy_on, sell_on, tb.ask, ts.bid, gross,  # type: ignore[arg-type]
                            gross - 2 * fee * 10_000)
            if best is None or spread.gross_bps > best.gross_bps:
                best = spread
    return best


async def _ticker(ex, coin: str) -> Ticker | None:
    try:
        return await ex.ticker(f'{coin}/USDT')   # Hyperliquid: USDT → USDC automatically
    except errors.BytxError:
        return None                              # coin not listed or exchange unavailable


async def main(coins: list[str]) -> None:
    clients = {name: create_exchange(name) for name in EXCHANGES}
    try:
        for coin in coins:
            got = await asyncio.gather(*(_ticker(ex, coin) for ex in clients.values()))
            tickers = {name: t for name, t in zip(clients, got) if t is not None}
            s = best_spread(coin, tickers)
            if s is None:
                print(f'{coin}: not enough quotes ({list(tickers)})')
                continue
            print(f'{coin}: buy {s.buy_on} {s.ask} → sell {s.sell_on} {s.bid}: '
                  f'{s.gross_bps:+.1f} bps, after fees {s.net_bps:+.1f} bps')
    finally:
        await asyncio.gather(*(ex.close() for ex in clients.values()))


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:] or ['BTC', 'ETH', 'SOL']))
