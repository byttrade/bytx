"""Trading basics (DEMO by default): authentication, spot, futures, limit orders, reducing a position.

    export BYTX_EXCHANGE=bybit BYTX_API_KEY=… BYTX_SECRET=…   # exchange demo keys
    python examples/00_trading_basics.py BTC/USDT 20

Steps:
1. authentication: client with keys, key permission check, balance; futures leverage;
2. spot: market buy for an amount in USDT and selling what was bought;
3. limit orders: place below the market (post-only), amend the price, cancel;
4. futures: long on margin with leverage, reduce the position by half, close;
   short and close it.

Amounts are in the quote currency (USDT): the SDK converts them to size and rounds to the exchange step.
Live account only with ``BYTX_LIVE=1``.
"""
from __future__ import annotations

import asyncio
import sys

from _common import exchange_from_env

from bytx import Exchange, errors

LEVERAGE = 5

async def login(ex: Exchange) -> None:
    """Authentication: the exchange accepts the key; key permissions and balance."""
    info = await ex.check_key(withdraw='warn')    # a key with withdrawal rights — warning only
    print(f'[{ex.name}] account {info.account_uid}: trade={info.can_trade}, withdraw={info.can_withdraw}')
    for warning in info.warnings:
        print('  ⚠️', warning)
    balance = await ex.balance()
    print(f'  USDT: free {balance.free("USDT")}, total {balance.total("USDT")}')


async def spot_buy_sell(spot: Exchange, symbol: str, usdt: float) -> None:
    """Spot: market-buy for ``usdt`` and sell what was bought."""
    bought = await spot.buy(symbol, notional=usdt)
    print(f'spot: bought {bought.filled} {symbol} at {bought.average}')
    if bought.filled:
        sold = await spot.sell(symbol, size=bought.filled)
        print(f'spot: sold {sold.filled} at {sold.average}')


async def limit_orders(ex: Exchange, symbol: str, usdt: float) -> None:
    """Limit order: place 5% below the market, raise the price to −3%, cancel."""
    bid = (await ex.order_book(symbol)).best_bid[0]
    order = await ex.buy(symbol, notional=usdt, price=bid * 0.95, post_only=True)
    print(f'limit order {order.id}: {order.size} at {order.price} ({order.status})')

    try:
        order = await ex.amend_order(symbol, order.id, price=bid * 0.97)
    except errors.NotSupported:          # KuCoin, MEXC, HTX: no amend — cancel and place again
        await ex.cancel_order(symbol, order.id)
        order = await ex.buy(symbol, notional=usdt, price=bid * 0.97, post_only=True)
    print(f'amended {order.id}: price {order.price}')

    order = await ex.cancel_order(symbol, order.id)
    print(f'canceled {order.id}: {order.status}')


async def futures(ex: Exchange, symbol: str, usdt: float, leverage: float = LEVERAGE) -> None:
    """Futures: long on ``usdt`` margin with leverage, reduce by half, close; short and close."""
    await ex.buy(symbol, margin=usdt, leverage=leverage)
    pos = await ex.position(symbol, 'long')
    print(f'long: {pos.size} at {pos.entry_price}, leverage {pos.leverage}')

    try:
        reduced = await ex.reduce_position(symbol, 'long', size=pos.size / 2)
        print(f'long reduced by {reduced.filled}')
    except errors.OrderTooSmall:
        print('half the position is below the minimum lot — skipping the reduce')
    await ex.close_position(symbol, 'long')
    print('long closed')

    await ex.sell(symbol, margin=usdt, leverage=leverage)
    pos = await ex.position(symbol, 'short')
    print(f'short: {pos.size} at {pos.entry_price}')
    await ex.close_position(symbol, 'short')
    print('short closed')


async def main(symbol: str, usdt: float) -> None:
    async with exchange_from_env(market='spot') as spot, exchange_from_env(market='swap') as swap:
        await login(swap)
        # set futures leverage before the first order (KuCoin takes it from each order)
        await swap.set_leverage(symbol, LEVERAGE)
        await spot_buy_sell(spot, symbol, usdt)
        await limit_orders(swap, symbol, usdt)
        await futures(swap, symbol, usdt)


if __name__ == '__main__':
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else 'BTC/USDT',
                     float(sys.argv[2]) if len(sys.argv) > 2 else 20.0))
