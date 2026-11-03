"""DCA (DEMO by default): scheduled buys for a fixed amount, spot.

    BYTX_MARKET=spot python examples/05_dca.py BTC/USDT 25 3600 10

The amount is in the quote currency (USDT): the SDK converts it to size and rounds to the exchange step.
If the price is ``dip_pct`` below the average entry price, the buy is doubled.
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass, field

from _common import exchange_from_env


@dataclass
class DcaState:
    spent: float = 0.0
    bought: float = 0.0
    buys: list[tuple[float, float]] = field(default_factory=list)  # (price, size)

    @property
    def avg_price(self) -> float | None:
        return self.spent / self.bought if self.bought else None

    def record(self, price: float, size: float) -> None:
        self.spent += price * size
        self.bought += size
        self.buys.append((price, size))


def next_amount(state: DcaState, price: float, base_amount: float, dip_pct: float = 5.0) -> float:
    """Next buy amount: doubled if the price is dip_pct% below the average."""
    avg = state.avg_price
    return base_amount * 2 if avg is not None and price <= avg * (1 - dip_pct / 100) else base_amount


async def main(symbol: str, amount: float, every_s: float, times: int) -> None:
    state = DcaState()
    async with exchange_from_env(market='spot') as ex:
        for n in range(times):
            price = (await ex.ticker(symbol)).ask
            if price is None:
                print('no price, skipping')
            else:
                notional = next_amount(state, price, amount)
                o = await ex.buy(symbol, notional=notional)          # market
                filled = o.filled or 0.0
                state.record(o.average or price, filled)
                print(f'#{n + 1}: {notional} USDT → {filled} at {o.average or price}; '
                      f'average {state.avg_price}')
            if n + 1 < times:
                await asyncio.sleep(every_s)
    print(f'total: spent {state.spent:.2f}, bought {state.bought}, average {state.avg_price}')


if __name__ == '__main__':
    a = sys.argv[1:]
    asyncio.run(main(a[0] if a else 'BTC/USDT', float(a[1]) if len(a) > 1 else 25.0,
                     float(a[2]) if len(a) > 2 else 3600, int(a[3]) if len(a) > 3 else 10))
