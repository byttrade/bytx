"""Telegram alerts on the live price (WS): level crossings and sharp moves.

    BYTX_TELEGRAM_TOKEN=… BYTX_TELEGRAM_CHAT_ID=… python examples/06_telegram_alerts.py BTC/USDT 60000 65000

Public data only. Without BYTX_TELEGRAM_TOKEN messages are printed to the console.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from collections import deque

import aiohttp

from bytx import create_exchange


class Alerts:
    """Level crossings (both directions) and a move > move_pct within window_s seconds."""

    def __init__(self, levels: list[float], move_pct: float = 1.0, window_s: float = 300) -> None:
        self.levels = sorted(levels)
        self.move_pct = move_pct
        self.window_s = window_s
        self._last: float | None = None
        self._window: deque[tuple[float, float]] = deque()   # (time, price)
        self._move_alerted_at = float('-inf')

    def check(self, price: float, now: float | None = None) -> list[str]:
        now = time.time() if now is None else now
        out = []
        if self._last is not None:
            for lvl in self.levels:
                if self._last < lvl <= price:
                    out.append(f'⬆️ breakout up {lvl} (price {price})')
                elif self._last > lvl >= price:
                    out.append(f'⬇️ breakout down {lvl} (price {price})')
        self._last = price
        self._window.append((now, price))
        while self._window and now - self._window[0][0] > self.window_s:
            self._window.popleft()
        first = self._window[0][1]
        change = (price - first) / first * 100
        if abs(change) >= self.move_pct and now - self._move_alerted_at > self.window_s:
            self._move_alerted_at = now
            out.append(f'⚡ {change:+.2f}% in {self.window_s:.0f} s (price {price})')
        return out


async def send(session: aiohttp.ClientSession, text: str) -> None:
    token, chat = os.environ.get('BYTX_TELEGRAM_TOKEN'), os.environ.get('BYTX_TELEGRAM_CHAT_ID')
    if not token or not chat:
        print(text)
        return
    async with session.post(f'https://api.telegram.org/bot{token}/sendMessage',
                            json={'chat_id': chat, 'text': text}) as r:
        if r.status != 200:
            print('telegram:', r.status, await r.text())


async def main(symbol: str, levels: list[float]) -> None:
    alerts = Alerts(levels)
    async with create_exchange('binance') as ex, aiohttp.ClientSession() as session:
        async for t in ex.stream.ticker(symbol):
            price = t.last or t.mid
            if price is None:
                continue
            for text in alerts.check(price):
                await send(session, f'{symbol}: {text}')


if __name__ == '__main__':
    a = sys.argv[1:]
    asyncio.run(main(a[0] if a else 'BTC/USDT', [float(x) for x in a[1:]] or [60_000, 65_000]))
