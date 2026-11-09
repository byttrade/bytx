# bytx — от [BYTtrade](https://byttrade.com)

[English](https://github.com/byttrade/bytx/blob/main/README.md) | **Русский** | [中文](https://github.com/byttrade/bytx/blob/main/docs/README.zh-CN.md) | [Español](https://github.com/byttrade/bytx/blob/main/docs/README.es.md) | [Português](https://github.com/byttrade/bytx/blob/main/docs/README.pt-BR.md) | [日本語](https://github.com/byttrade/bytx/blob/main/docs/README.ja.md) | [한국어](https://github.com/byttrade/bytx/blob/main/docs/README.ko.md) | [Türkçe](https://github.com/byttrade/bytx/blob/main/docs/README.tr.md)

[![PyPI](https://img.shields.io/pypi/v/bytx)](https://pypi.org/project/bytx/)
[![Python](https://img.shields.io/pypi/pyversions/bytx)](https://pypi.org/project/bytx/)
[![License](https://img.shields.io/badge/license-source--available-blue)](https://github.com/byttrade/bytx/blob/main/LICENSE)

Типизированный **асинхронный** Python SDK для криптобирж. Единый интерфейс для спота и бессрочных фьючерсов
на Binance, Bybit, OKX, BingX, Hyperliquid, KuCoin, Bitget; в бете: Gate, MEXC, HTX.

![Один API для всех бирж](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

```bash
pip install bytx            # + extras: [ws] [fast]
```

## Быстрый старт

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, по умолчанию своп
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # размер в BTC, а не в контрактах
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` использует демо/тестнет биржи; реальные деньги не задействуются. Рыночным данным ключи не нужны:
`create_exchange('okx')`.

**Тейк-профит и стоп-лосс** — один вызов на любой бирже:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**Живой стакан**, который переживает обрывы связи:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps, book.imbalance(5))
```

![Живой стакан](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/order_book.gif)

**Paper-трейдинг** на живом стакане — без ключей и без реальных денег:

```python
from bytx.paper import PaperConfig

ex = create_exchange('binance', paper=True, paper_config=PaperConfig(balance={'USDT': 1000}))
order = await ex.buy('BTC/USDT', notional=100)     # проходит по стакану, с реальной комиссией тейкера
```

![Paper-трейдинг](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/paper.gif)

**Спот**, **алгоритмы исполнения**, **размер позиции от риска**:

```python
spot = create_exchange('okx', market='spot', api_key=…, secret=…, password=…, demo=True)
await spot.buy('DOGE/USDT', notional=10)                              # рыночная покупка на 10 USDT, один запрос
await ex.algos.twap('BTC/USDT', 'buy', size=0.5, duration=600)       # ещё chase, iceberg, bracket, trailing_stop
await ex.open_position('BTC/USDT', 'long', risk_usdt=50, stop=58_000) # размер от расстояния до стопа
```

Больше в [`examples/`](https://github.com/byttrade/bytx/tree/main/examples): рыночные данные, бэктест, сканер
арбитража, сетка, DCA, алерты в Telegram, скальпер, несколько аккаунтов, стратегия с восстановлением после рестарта.

## Почему bytx

bytx построен поверх [ccxt](https://github.com/ccxt/ccxt) и превращает его в торговый SDK, с которым не нужно воевать.

- **Одна строка вместо страницы.** `buy('BTC/USDT', notional=100)` — не нужно вручную считать контракты,
  точность и минимумы; размер в базовой монете на всех биржах.
- **Без двойных входов.** При таймауте bytx находит ордер по `client_id` и точно говорит, что произошло
  (`OrderStatusUnknown` — только если биржа действительно не может ответить).
- **Один вызов везде.** TP/SL, плечо, режим маржи и позиции, dead-man switch, переводы — один метод вместо
  внутренних эндпоинтов каждой биржи.
- **WebSocket, который не падает.** Переподключение с backoff, сторож тишины, переподписка, проверка
  разрывов стакана и восстановление из снимка.
- **Сверено с документацией бирж.** Каждое правило биржи проверено на примерах ответов из официальной
  документации; найденные так баги ccxt исправлены в SDK.
- **Типизированные модели и понятные ошибки.** Dataclass-модели `Order`, `Position`, `OrderBook`; одна
  иерархия `BytxError` с флагом `.retryable`; ключи не попадают ни в текст ошибок, ни в логи.
- **Готов к продакшену.** Прокси на каждый аккаунт, один кэш рынков на процесс, paper-трейдинг и replay,
  журнал позиций, проверка прав API-ключа и здоровья биржи.

<details>
<summary>Голый ccxt и bytx: купить BTC на 100 USDT на бессрочных фьючерсах OKX</summary>

```python
# голый ccxt
ex = ccxt.okx({'apiKey': key, 'secret': secret, 'password': passphrase})
await ex.load_markets()
m = ex.market('BTC/USDT:USDT')
price = (await ex.fetch_ticker(m['symbol']))['last']
contracts = 100 / price / m['contractSize']               # OKX: 1 контракт = 0.01 BTC
amount = float(ex.amount_to_precision(m['symbol'], contracts))
if amount < m['limits']['amount']['min']:
    raise ValueError('order too small')
order = await ex.create_order(m['symbol'], 'market', 'buy', amount)   # dict, количество в контрактах
```

```python
# bytx
async with bytx.create_exchange('okx', api_key=key, secret=secret, password=passphrase,
                                margin_mode='cross') as ex:
    order = await ex.buy('BTC/USDT', notional=100)        # модель Order, размер в BTC
```

</details>

> Боты без кода, бэктесты и подтверждение сделок в Telegram — на [byttrade.com](https://byttrade.com).

## Документация

Полное руководство — все методы, особенности бирж, ошибки, восстановление после рестарта:
**[docs/GUIDE.ru.md](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md)**
([in English](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.md)).

[Возможности](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#возможности) ·
[Спот](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#спот) ·
[WebSocket](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#websocket) ·
[Скальпинг](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#скальпинг) ·
[Paper-трейдинг](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#paper-трейдинг-и-replay) ·
[Алгоритмы](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#алгоритмы-исполнения) ·
[Размер от риска](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#размер-позиции-от-риска) ·
[Восстановление после рестарта](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#восстановление-после-рестарта-и-стратегии) ·
[Прокси](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#прокси-и-несколько-аккаунтов) ·
[Интеграции](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#интеграции-tradingview-telegramdiscord-prometheus-opentelemetry) ·
[Ошибки](https://github.com/byttrade/bytx/blob/main/docs/GUIDE.ru.md#ошибки)

## Лицензия

Бесплатное использование по [BYTtrade Source-Available License](https://github.com/byttrade/bytx/blob/main/LICENSE),
в том числе коммерческое; копировать код в другие библиотеки, публиковать изменённые версии и предлагать как
хостируемый сервис нельзя. Использует ccxt (MIT), см. [`NOTICE`](https://github.com/byttrade/bytx/blob/main/NOTICE).
Не является финансовой рекомендацией; торговля криптовалютой связана с риском потерь.
