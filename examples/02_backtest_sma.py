"""Simple backtest: a crossover of two SMAs on exchange candles (no orders).

    python examples/02_backtest_sma.py BTC/USDT 1h 20 50

Accounts for the taker fee on entry and exit. This is a teaching example, not a strategy.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

from bytx import create_exchange
from bytx.types import Candle


@dataclass
class Result:
    trades: int
    return_pct: float
    max_drawdown_pct: float
    win_rate: float


def sma(values: list[float], n: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    total = 0.0
    for i, v in enumerate(values):
        total += v
        if i >= n:
            total -= values[i - n]
        if i >= n - 1:
            out[i] = total / n
    return out


def backtest(candles: list[Candle], fast: int, slow: int, fee: float = 0.0005) -> Result:
    """Long-only: enter when the fast SMA crosses the slow one from below, exit when it crosses from above."""
    if fast >= slow:
        raise ValueError('fast must be less than slow')
    closes = [c.close for c in candles]
    f, s = sma(closes, fast), sma(closes, slow)
    equity, peak, max_dd = 1.0, 1.0, 0.0
    entry: float | None = None
    wins = trades = 0
    for i in range(1, len(closes)):
        if None in (f[i], s[i], f[i - 1], s[i - 1]):
            continue
        crossed_up = f[i - 1] <= s[i - 1] and f[i] > s[i]  # type: ignore[operator]
        crossed_down = f[i - 1] >= s[i - 1] and f[i] < s[i]  # type: ignore[operator]
        if entry is None and crossed_up:
            entry = closes[i]
        elif entry is not None and crossed_down:
            r = closes[i] / entry * (1 - fee) ** 2
            equity *= r
            trades += 1
            wins += r > 1
            entry = None
        mark = equity * (closes[i] / entry if entry else 1.0)
        peak = max(peak, mark)
        max_dd = max(max_dd, 1 - mark / peak)
    return Result(trades, (equity - 1) * 100, max_dd * 100, wins / trades if trades else 0.0)


async def main(symbol: str, timeframe: str, fast: int, slow: int, exchange: str = 'binance') -> None:
    async with create_exchange(exchange) as ex:              # public candles
        candles = await ex.candles(symbol, timeframe, limit=1000)
        fee = (await ex.market(symbol)).taker_fee or 0.0005
        r = backtest(candles, fast, slow, fee)
        print(f'{symbol} {timeframe} SMA{fast}/{slow}, candles {len(candles)}, fee {fee}')
        print(f'trades {r.trades}, return {r.return_pct:+.2f}%, drawdown {r.max_drawdown_pct:.2f}%, '
              f'winners {r.win_rate:.0%}')


if __name__ == '__main__':
    a = sys.argv[1:]
    asyncio.run(main(a[0] if a else 'BTC/USDT', a[1] if len(a) > 1 else '1h',
                     int(a[2]) if len(a) > 2 else 20, int(a[3]) if len(a) > 3 else 50))
