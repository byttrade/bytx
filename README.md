# bytx — by [BYTtrade](https://byttrade.com)

**English** | [Русский](https://github.com/byttrade/bytx/blob/main/docs/README.ru.md) | [中文](https://github.com/byttrade/bytx/blob/main/docs/README.zh-CN.md) | [Español](https://github.com/byttrade/bytx/blob/main/docs/README.es.md) | [Português](https://github.com/byttrade/bytx/blob/main/docs/README.pt-BR.md) | [日本語](https://github.com/byttrade/bytx/blob/main/docs/README.ja.md) | [한국어](https://github.com/byttrade/bytx/blob/main/docs/README.ko.md) | [Türkçe](https://github.com/byttrade/bytx/blob/main/docs/README.tr.md)

[![PyPI](https://img.shields.io/pypi/v/bytx)](https://pypi.org/project/bytx/)
[![Python](https://img.shields.io/pypi/pyversions/bytx)](https://pypi.org/project/bytx/)
[![License](https://img.shields.io/badge/license-source--available-blue)](https://github.com/byttrade/bytx/blob/main/LICENSE)

A typed **async** Python SDK for crypto exchanges. One interface for spot and perpetual futures on
Binance, Bybit, OKX, BingX, Hyperliquid, KuCoin, Bitget; in beta: Gate, MEXC, HTX, BloFin (USDT-M futures).

![One API for every exchange](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

```bash
pip install bytx            # + extras: [ws] [fast]
```

## Quick start

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, swap by default
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # size in BTC, not in contracts
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` uses the exchange's demo/testnet; no real money involved. Market data needs no keys at all:
`create_exchange('okx')`.

**Take-profit and stop-loss** — one call on every exchange:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**A live order book** that survives disconnects:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps, book.imbalance(5))
```

![Live order book](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/order_book.gif)

**Paper trading** on the live order book — no keys, no real money:

```python
from bytx.paper import PaperConfig

ex = create_exchange('binance', paper=True, paper_config=PaperConfig(balance={'USDT': 1000}))
order = await ex.buy('BTC/USDT', notional=100)     # walks the book, real taker fee
```

![Paper trading](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/paper.gif)

**Spot**, **execution algorithms**, **risk-based sizing**:

```python
spot = create_exchange('okx', market='spot', api_key=…, secret=…, password=…, demo=True)
await spot.buy('DOGE/USDT', notional=10)                              # market buy for 10 USDT, one request
await ex.algos.twap('BTC/USDT', 'buy', size=0.5, duration=600)       # also chase, iceberg, bracket, trailing_stop
await ex.open_position('BTC/USDT', 'long', risk_usdt=50, stop=58_000) # size from the stop distance
```

More in [`examples/`](https://github.com/byttrade/bytx/tree/main/examples): market data, backtest, arbitrage
scanner, grid, DCA, Telegram alerts, scalper, multi-account, strategy with restart recovery.

## Why bytx

bytx is built on [ccxt](https://github.com/ccxt/ccxt) and turns it into a trading SDK you don't have to fight.

- **One line instead of a page.** `buy('BTC/USDT', notional=100)` — no contract sizes, precision or
  minimums to compute by hand; size in the base coin on every exchange.
- **No double entries.** On a timeout bytx finds the order by `client_id` and tells you exactly what
  happened (`OrderStatusUnknown` only when the exchange really can't say).
- **Same call everywhere.** TP/SL, leverage, margin and position mode, dead-man switch, transfers — one
  method instead of each exchange's private endpoints.
- **WebSocket that stays up.** Reconnect with backoff, silence watchdog, resubscribe, order book gap
  checks and resync from a snapshot.
- **Checked against the exchange docs.** Every exchange rule is tested against response examples from the
  official documentation; ccxt bugs found this way are fixed in the SDK.
- **Typed models and clear errors.** `Order`, `Position`, `OrderBook` dataclasses; one `BytxError`
  hierarchy with a `.retryable` flag; keys never appear in error text or logs.
- **Ready for production.** A proxy per account, one market cache per process, paper trading and replay,
  a position journal, API-key permission checks and exchange health.

<details>
<summary>Raw ccxt vs bytx: buy BTC for 100 USDT on OKX perpetuals</summary>

```python
# raw ccxt
ex = ccxt.okx({'apiKey': key, 'secret': secret, 'password': passphrase})
await ex.load_markets()
m = ex.market('BTC/USDT:USDT')
price = (await ex.fetch_ticker(m['symbol']))['last']
contracts = 100 / price / m['contractSize']               # OKX: 1 contract = 0.01 BTC
amount = float(ex.amount_to_precision(m['symbol'], contracts))
if amount < m['limits']['amount']['min']:
    raise ValueError('order too small')
order = await ex.create_order(m['symbol'], 'market', 'buy', amount)   # dict, amount in contracts
```

```python
# bytx
async with bytx.create_exchange('okx', api_key=key, secret=secret, password=passphrase,
                                margin_mode='cross') as ex:
    order = await ex.buy('BTC/USDT', notional=100)        # Order model, size in BTC
```

</details>

> No-code bots, backtests and trade confirmations in Telegram — at [byttrade.com](https://byttrade.com).

## Documentation

The full guide — every method, exchange specifics, errors, recovery after a restart:
**[docs/GUIDE.md](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md)**
([на русском](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md)).

[Features](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#features) ·
[Spot](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#spot) ·
[WebSocket](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#websocket) ·
[Scalping](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#scalping) ·
[Paper trading](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#paper-trading-and-replay) ·
[Algorithms](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#execution-algorithms) ·
[Risk sizing](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#risk-based-position-sizing) ·
[Restart recovery](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#restart-recovery-and-strategies) ·
[Proxies](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#proxies-and-multiple-accounts) ·
[Integrations](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#integrations-tradingview-telegramdiscord-prometheus-opentelemetry) ·
[Errors](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md#errors)

## License

Free to use under the [BYTtrade Source-Available License](https://github.com/byttrade/bytx/blob/main/LICENSE),
including commercially; copying the code into other libraries, modified versions and hosted services are not
permitted. Uses ccxt (MIT), see [`NOTICE`](https://github.com/byttrade/bytx/blob/main/NOTICE).
Not financial advice; trading cryptocurrency carries a risk of loss.
