# bytx — полное руководство

[English](GUIDE.md) | **Русский** · [← README](README.ru.md)

Типизированный **асинхронный** Python SDK для криптобирж: Binance, Bybit, OKX, BingX, Hyperliquid, KuCoin, Bitget;
в бете: Gate, MEXC, HTX, BloFin (спот и фьючерсы USDT-M).
Единый интерфейс для спота и бессрочных фьючерсов, WebSocket с умными переподключениями,
прокси на каждый аккаунт и инструменты для скальпинга.

```bash
pip install bytx            # + extras: [ws] [fast]
```

`[fast]` ставит и `coincurve`: действия Hyperliquid подписываются за ~4 мс вместо ~50 мс чистого Python в event loop
(подпись побайтно та же, что у ccxt).

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

`demo=True` использует демо/тестнет биржи; реальные деньги не задействуются.

> Боты без кода, бэктесты и подтверждение сделок в Telegram — на
> [byttrade.com](https://byttrade.com).

## Почему bytx, а не голый ccxt

bytx построен поверх [ccxt](https://github.com/ccxt/ccxt) и превращает его в торговый SDK, с которым не нужно воевать: та же
задача решается **одной строкой вместо страницы кода**, а пограничные случаи, которые в продакшене стоят
денег, уже обработаны за вас.

**Купить BTC на 100 USDT в бессрочных фьючерсах OKX**

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

**Тейк-профит и стоп-лосс на позицию** — в ccxt на каждой бирже по-своему (Bybit
`privatePostV5PositionTradingStop`, Binance — два условных ордера с `closePosition`, OKX — algo-ордера…).
В bytx — один и тот же вызов везде:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**Живой стакан, который переживает обрывы связи** — с ccxt цикл переподключения, backoff, переподписку,
поиск пропусков и ресинхронизацию по снимку вы пишете сами. В bytx:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**Таймауты при выставлении ордера** — в ccxt после таймаута непонятно, живой ли ордер (повтор — это
двойной вход). bytx сверяет по `client_id` и точно говорит, что произошло:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**Что ещё вы получаете из коробки**

| Голый ccxt | bytx |
|---|---|
| `amount` в контрактах, у каждой биржи свой `contractSize` | `size=` в базовой монете, `notional=` в USDT или `contracts=`; пересчёт и округление (вниз) встроены |
| `load_markets` на каждом инстансе, память × аккаунты | один кэш рынков на процесс |
| биржа отклоняет ордер из-за шага размера или минимального нотионала | проверка до отправки: `OrderTooSmall`, `InvalidOrder` |
| половина возможностей — в implicit API (`fapiPrivatePost…`) | прямые методы: TP/SL, dead-man switch, режим позиций, переводы |
| ответы — `dict` | dataclass-модели (`Order`, `Position`, `OrderBook`…), сырой ответ в `.raw` |
| голые исключения ccxt | единая иерархия `BytxError` с флагом `.retryable` |
| WS падает с исключением, переподключение — ваша забота | супервизор: backoff + jitter, сторож тишины, переподписка, ресинхронизация стакана, события состояния |
| ответы расходятся с документацией биржи | ответы сверены с официальной документацией бирж ([ground truth](#качество)), баги ccxt исправлены в SDK |

Плюс: типизированные модели везде, единая единица объёма на всех биржах, прокси на каждый аккаунт,
paper-трейдинг на живых стаканах, журнал позиций, проверка прав API-ключа и здоровье биржи — и всё это
с одинаковым интерфейсом на каждой бирже.

## Возможности

**Рыночные данные:** `ticker`, `tickers`, `order_book`, `trades`, `candles`, `funding_rate(s)`, `funding_history`,
`open_interest`, `markets`, `market`, `server_time`, `leverage_tiers`, `maintenance_margin_rate`.
Всё это работает без ключей (`create_exchange('bybit')`); рынки хранятся в общем кэше процесса с TTL 1 ч.
`leverage_tiers(symbol)` возвращает уровни плеча и ставки поддерживающей маржи (`LeverageTier`): Bybit, OKX
(границы в базе: `min_size`/`max_size`), KuCoin, Hyperliquid (MMR = 1 / (2 · maxLeverage) по документации),
Binance — `/fapi/v1/leverageBracket` с ключами, без ключей — запасной публичный веб-эндпоинт биржи
(`source='binance_public'`, в официальной документации его нет). BingX уровни не публикует — `NotSupported`.
`maintenance_margin_rate(symbol, notional=|size=)` возвращает ставку первого уровня или ставку уровня позиции.

**Аккаунт:** `balance`, `free_collateral(symbol)`, `positions`, `position`, `open_orders`,
`closed_orders`, `order` (по id или `client_id`), `my_trades`, `trading_fee`, `leverage`,
`is_hedge_mode`, `transfer`. `free_collateral(symbol)` — свободное обеспечение в валюте расчётов символа
(`Market.settle`: на Hyperliquid — USDC); если валюты нет в ответе биржи —
`ExchangeError`, а не 0. `Position.side` равен `'long'`/`'short'` и в hedge-режиме на любой бирже (Bybit
`positionIdx`, OKX `posSide`, Binance/BingX/KuCoin `positionSide`), `Position.hedged` помечает позицию
в hedge-режиме, `margin_mode` — `'cross'`/`'isolated'`, `raw` — исходный ответ. `positions(include_zero=True)`
возвращает и нулевые записи биржи (`size=0`; у нулевой one-way позиции `side=None`). `Fill.position_side` —
сторона hedge-позиции сделки из данных биржи (REST и WS: Binance/BingX `positionSide`/`ps`, OKX `posSide`);
`None` — one-way или биржа не отдаёт сторону в сделке (у Bybit `positionIdx` есть только у ордера, не у исполнения).

`my_trades(symbol, since=, until=, order_id=, client_id_prefix=)`: `order_id` — только сделки этого ордера (на Binance,
OKX и KuCoin фильтрует биржа, на остальных — SDK; задайте `since` не позже ордера); `client_id_prefix` — сделки ордеров
вашего бота (если биржа не отдаёт client_id в сделке, он берётся из ордера). `until` (мс, включительно) возвращает окно
`[since, until]` целиком: страницы читаются до конца окна, а если окно нельзя прочитать полностью — ошибка, а не часть
списка (нужны `symbol` и `since`, без `limit`).

**Торговля:** `buy` / `sell` / `place_order` (market/limit, `post_only`, `time_in_force`, `reduce_only`,
`take_profit` / `stop_loss`, `client_id`), `place_orders`, `amend_order`, `cancel_order`, `cancel_orders`,
`cancel_all`, `close_position`, `reduce_position`, `close_all`, `close_everything`, `flatten`,
`set_leverage`, `set_margin_mode`, `set_position_mode`.

**Алгоритмы исполнения** (`ex.algos`): `chase`, `twap`, `iceberg`, `bracket` (OCO по документации биржи),
`trailing_stop`, `market` с ограничением проскальзывания — см. [Алгоритмы исполнения](#алгоритмы-исполнения).

**Размер от риска:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` считает размер по расстоянию до стопа,
плечу, шагам и минимумам биржи и комиссиям, ставит стоп атомарно или откатывает вход; `risk_size` делает
тот же расчёт без ордеров — см. [Размер позиции от риска](#размер-позиции-от-риска).

**Защита:** `set_tp_sl` / `tp_sl_orders` / `tp_sl_order` / `cancel_tp_sl`, `cancel_all_after` и
`dead_man_switch()` (биржа отменит ордера, если процесс завис), `kill_switch(symbol)` /
`kill_everything()`, `RiskGuard` (лимиты на позицию, дневной убыток и частоту ордеров — проверяются до
отправки).

**Надёжность:** ордер никогда не повторяется автоматически; при таймауте или неизвестном исходе
(`UnknownOutcome`) SDK ищет его по `client_id` и возвращает найденное либо `RequestTimeout` с
`client_id` в `details`. Пакетные операции (`place_orders`, `cancel_orders`, `cancel_all`,
`close_all`, `close_everything`, `kill_switch`, `kill_everything`) не останавливаются на первой ошибке: при частичном сбое — `BatchError`,
в `details` — что сделано, а что нет. TP/SL не поставлены после входа — `ProtectionNotPlaced`
(ордер в `details['order']`, поставленные ноги TP/SL в `details['placed']`).
`place_orders` использует один пакетный запрос, где он есть у биржи; Binance спот (в Spot API нет пакетного
размещения), фьючерсы Gate и MEXC (createOrders в ccxt 4.4.65 их не поддерживает) идут по одному параллельно,
с тем же `BatchError` и результатом по каждому элементу.
`cancel_orders` и `cancel_all` по метке снимают пакетами по докам бирж: Bybit (linear/spot) — по 10,
OKX — по 20, Binance USDⓈ-M — по 10, Hyperliquid — по 40 (одно подписанное действие `cancel`); исход — по каждому ордеру (код элемента: «ордера нет» —
`OrderNotFound`, лимит — `RateLimited`; отказ элемента по лимиту — один повтор одиночной отменой).
Лимитер аккаунта делится по эндпоинтам, как считает биржа: у Bybit create, amend, cancel, cancel-all и
trading-stop, у OKX размещение, изменение и отмена — свои лимиты; массовая отмена не задерживает размещение и
закрытие позиции. Bybit 10006 («Too many visits») приостанавливает лимитер этого эндпоинта ключа на окно биржи.
Лимиты аккаунта — по докам Rate Limit каждой биржи: чтения Bybit (позиции, открытые ордера, история, исполнения,
баланс) — на UID; у каждого прочего приватного эндпоинта OKX свой лимит, торговые эндпоинты OKX делятся ещё и по
инструменту («User ID + Instrument ID»); BingX, Bitget, Gate, MEXC и HTX — по эндпоинту и UID, на общем лимитере IP —
только вес IP по доке. Стоимость, в которой ошибается ccxt, исправлена по доке (Binance USDⓈ-M account / balance /
positionRisk — вес 5; `openAlgoOrders` — 1 с символом, 40 без). Баны бирж ставят лимитер на паузу до конца бана и не
повторяются: HTTP 418, Bybit HTTP 403 «access too frequent» (IP, 10 минут), BingX 100410 (эндпоинт, до момента
разблокировки из ответа); `RateLimited` несёт `retry_after`. Лимитер заблокирован дольше остатка дедлайна вызова
(`ex.deadline(...)`) — сразу `RateLimited`, без сна; переполнена очередь лимитера SDK — `ThrottleQueueFull` (это
`RateLimited`). У demo и testnet со своим хостом (Binance Demo / testnet, Bybit demo, BingX VST, Hyperliquid testnet,
KuCoin sandbox) — свой лимитер IP: бан demo-хоста не держит боевых клиентов.
Списки читаются целиком: `open_orders()` (курсор Bybit, `after` OKX, `currentPage` KuCoin Futures — с символом и без)
и `positions()` Bybit без символа (каждая валюта расчёта и inverse); список, оборванный пределом страниц, — `ExchangeError`,
а не неполный список.

**TP/SL:**
- `trigger_by` по умолчанию (`None`): Binance и OKX — `'last'` (Binance
  `workingType=CONTRACT_PRICE`), Bybit, BingX, KuCoin и Hyperliquid — `'mark'`. Явный `trigger_by=`
  имеет приоритет.
- `set_tp_sl(..., position_side='long', contracts=n)` не читает позицию: сторону и размер задаёт
  вызывающий (многоуровневый TP без гонки). Размер не сверяется с позицией; превысить её не даёт
  reduce-only на бирже. Bybit в этом режиме ставит ногу как Partial. OKX и KuCoin
  без режима маржи выбрасывают `ConfigError`.
- `place_tp_sl(symbol, 'take_profit' | 'stop_loss', trigger, position_side=, contracts=|size=,
  client_id=)` ставит одну ногу и возвращает один `TpSlOrder` или выбрасывает исключение, без списка.
- У `TpSlOrder` есть поле `state`: `'open'` / `'triggered'` / `'canceled'` на любой бирже, из
  поля биржи (Binance `algoStatus`, Bybit `orderStatus`, OKX `state`, BingX `status`, KuCoin
  `stopTriggered`, HL `status`). `status` остаётся статусом ордера.
- `modify_tp_sl(symbol, order_id, take_profit=|stop_loss=, size=|contracts=, client_id=)` возвращает
  `TpSlModifyResult`: `old` — `canceled` / `alive` / `triggered` / `unknown`, `new` — `created` /
  `unknown` / `rejected` / `None`, `order` — новый ордер, `strategy` — как переносилось. Перенос — **без
  окна без стопа**, где биржа это позволяет (по документации, таблица ниже; `bytx.exchanges.tp_sl_modify`).

  | Биржа | Как переносится | Окно без SL |
  |---|---|---|
  | OKX | `amend-algos` — тот же algoId, новый триггер/объём (атомарно). С новым `client_id` или `trigger_by` (правка их не меняет, 51528) — make-before-break: до 100 TP/SL на инструмент | нет |
  | Bybit | Full (вся позиция) — `trading-stop` на месте, orderId прежний; Partial / новый объём — make-before-break («Partial: can only add») | нет |
  | Binance | make-before-break: algo `STOP_MARKET` с `quantity` сосуществуют. Ордер `closePosition=true` без нового объёма — один на сторону (-4130): отмена, затем новый | нет; closePosition — ~0,3–0,6 с (demo) |
  | BingX | make-before-break: второй `STOP_MARKET` на всю позицию VST принимает. Modify Order — только LIMIT, cancelReplace — та же отмена | нет (VST) |
  | KuCoin, Hyperliquid | make-before-break: до 200 стоп-ордеров на контракт (KuCoin, документация), reduce-only триггеры (HL, testnet) | нет |
  | Bitget | `modify-tpsl-order` на месте: тот же orderId и clientOid (demo 2026-11-04). `pos_loss` / `pos_profit` (вся позиция) — один на сторону: вторая постановка заменяет прежний с прежним clientOid, поэтому тот же объём — всегда на месте, новый `client_id` не применяется (warning). `loss_plan` / `profit_plan` с объёмом — несколько на сторону: с новым `client_id` или объёмом — make-before-break (вся позиция → объём: новый `loss_plan`, затем снятие `pos_loss`) | нет |
  | Gate, MEXC, HTX | отмена, затем новый (два TP/SL на позицию по документации не сверены) | время одного запроса |
  | BloFin | `amend-tpsl` на месте: тот же tpslId и clientOrderId, новый триггер / тип цены / объём; с новым `client_id` — make-before-break | нет |

  Make-before-break: новый ставится первым; отказ нового — прежний не тронут (`old='alive'`,
  `new='rejected'`), исход неизвестен — тоже (`new='unknown'`, ищите по `client_ids`); прежний снять
  не удалось — новый не теряется: `old='alive'` и `both_alive=True` (на бирже оба, решает вызывающий).
  Отмена первой: новый ставится, только если старого точно нет; отказ нового — триггер прежнего
  ставится заново (`restored`). Paper — make-before-break.

**Отмена и закрытие только своего:**
- `cancel_all(symbol, client_id_prefix=...)` отменяет только ордера и TP/SL с тегом бота. По
  умолчанию берётся клиентский тег из `create_exchange(client_id_prefix=...)`.
  `cancel_all(symbol, order_ids=[...])` отменяет только эти id, включая algo-ордера Binance/OKX
  по id из вашей БД после рестарта. Слепая отмена всего стакана — только явно: `all_orders=True`.
  Без любого из этих параметров `cancel_all` выбрасывает `ConfigError`.
- `cancel_all` возвращает `CancelReport`: по каждому ордеру `canceled`, `already_closed` или
  `error`. При ошибках выбрасывается `BatchError` с отчётом в `details['report']`.
- `close_all(symbol, side=None)` закрывает позиции одного символа. Все позиции аккаунта — явно,
  `close_everything()`. То же для `kill_switch(symbol, ...)` и `kill_everything()`.
- `close_position` / `reduce_position` принимают сторону **позиции** `'long'`/`'short'`. Передача
  `'buy'`/`'sell'` выбрасывает `ConfigError`.
- `tp_sl_order(symbol, id)` и `cancel_tp_sl(symbol, order_ids=[...])` работают без памяти
  процесса: Binance — Algo API по algoId, OKX — algo-ордер по algoId.
- Сработавший TP/SL биржа часто исполняет **другим** ордером, и сделки закрытия идут под его id, а не под
  id TP/SL. `tp_sl_order(symbol, id).fill_order_ids` (первый — `fill_order_id`) — эти ордера: OKX
  `ordIdList`, Binance `actualOrderId`, BingX (ордер с `triggerOrderId`), Gate, MEXC, HTX. Bybit
  (trading-stop), KuCoin, Hyperliquid исполняют тем же id — пусто, сделки ищутся по id TP/SL.
- `flatten(symbol, side)` выходит из позиции по шагам: отменяет ваши лимитные ордера, затем ваши TP/SL,
  закрывает позицию reduce-only ордером и ждёт, пока она исчезнет. Возвращает `FlattenReport`
  с исходом каждого шага; `ok` означает, что позиция закрыта без ошибок.

**Отмена уже закрытого ордера:** `cancel_order` для ордера, которого больше нет в стакане (исполнен,
отменён ранее), возвращает ордер с его финальным статусом (`'closed'`, `'canceled'`, …) вместо
исключения. «Не открыт» определяется по кодам из документации биржи. Bybit ищет ордер (`order`,
сопоставление по `client_id`) в `/v5/order/realtime`, затем в `/v5/order/history`: realtime хранит только
последние 500 закрытых ордеров.

**Плечо, маржа, режим позиций:**
- У `margin_mode` нет значения по умолчанию: SDK никогда молча не предполагает `'cross'`. OKX и KuCoin отправляют режим
  с каждым ордером, OKX и Hyperliquid — при смене плеча; без `create_exchange(margin_mode=...)` или
  `set_margin_mode(symbol, mode)` они выбрасывают `ConfigError`. Закрытие и `reduce_position` используют режим
  открытой позиции. `margin_mode_of(symbol)` возвращает `None`, если режим не задан.
- KuCoin берёт плечо из каждого ордера: открывающий ордер без `set_leverage` на этом инстансе
  выбрасывает `ConfigError`, а не ставит 1x по умолчанию.
- KuCoin хранит режим маржи и в ордере, и у символа; ордер в режиме, отличном от режима символа, биржа
  отклоняет (330005). `set_margin_mode(symbol, mode)` читает режим символа
  (`GET /api/v2/position/getMarginMode`) и, если он другой, переключает его (`POST /api/v2/position/changeMarginMode`,
  ответ сверяется); отказ биржи выбрасывает исключение, а SDK сохраняет прежний режим символа.
- `set_leverage(symbol, x)` возвращает плечо, которое биржа сообщила как применённое (ниже
  запрошенного — ограничено уровнем), или `None`, если биржа его не сообщает (Bybit, Hyperliquid, KuCoin).
  При открытой позиции по символу выбрасывается `bytx.PositionOpenError`; `allow_open_position=True` отключает проверку.
- `set_leverage(symbol, x, side='long'|'short')` ставит плечо одной стороны hedge-позиции (вторая сторона не
  меняется, проверяется позиция только своей стороны): OKX isolated + long/short (`posSide`), BingX hedge (`side`),
  Bybit hedge (`buyLeverage`/`sellLeverage`; в cross биржа требует равных — отказ 10001).
  Binance (плечо одно на символ), OKX cross, one-way и остальные биржи — `NotSupported` до запросов.
- `leverage(symbol, side=, margin_mode=)` читает плечо для стороны и корзины маржи. `None` — биржа
  его не сообщает. Разное плечо для long и short без `side` выбрасывает `ConfigError`.
- `is_hedge_mode(refresh=True)` перечитывает режим с биржи. `set_position_mode` обновляет кэш
  и сбрасывает его при сбое.

**Единицы размера.** В моделях размер в базовой монете — `size`, `filled`,
`remaining`; в единицах биржи (контракты; на споте — база) — `contracts`, `filled_contracts`,
`remaining_contracts` (`Order`, `Fill`, `Position`). Поля `amount` без единиц нет. На OKX и KuCoin
контракт ≠ монета (`Market.contract_size`): `BTC-USDT-SWAP` — это 0.01 BTC, `XBTUSDTM` — 0.001 BTC.

- `place_order(..., margin=, leverage=)` считает размер от маржи с плечом: контракты = маржа × плечо /
  (цена × contract_size). Цена — цена исполнения: своя для limit, опорная для market.
  `leverage` по умолчанию берётся из `set_leverage` на этом инстансе; нет плеча или оно другое —
  `ConfigError`. `notional=` — полный нотионал **без** плеча. Объём меньше минимального лота округляется вверх до
  минимума: 5 USDT маржи × плечо на BTC могут стать 100 USDT. `max_round_up=` (по умолчанию None — без границы, но
  WARNING при превышении в 2 раза и более) — во сколько раз минимум может превысить запрошенное, иначе `OrderTooSmall`
  (`details`: `requested`, `min`).
- `contracts_for_margin(symbol, margin, leverage, price)` делает тот же расчёт без ордера. `None` —
  ниже минимального лота биржи даже после округления на один шаг вверх.
- Округление размера одинаково на всех путях: деление в `Decimal`, подтяжка вверх до целого лота, если
  отклонение в пределах 1e-9 относительно (погрешность float на стороне вызывающего: `k·lot·price/lev`), затем вниз до шага.
  `reduce_position` и `set_tp_sl` никогда не превышают размер позиции.
- `set_tp_sl(..., contracts=)` — частичный TP/SL в контрактах; `size=` — в базе.
- `Market.min_cost`: Bybit `minNotionalValue`, Binance
  `MIN_NOTIONAL`, Hyperliquid $10, иначе ccxt `limits.cost.min`; `None` — ограничения нет.

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (бета) | MEXC (бета) | HTX (бета) | BloFin (бета) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Спот | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ ⁶ | ✓ | ✓ ⁹ |
| Бессрочные фьючерсы | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| Демо / тестнет | ✓ | ✓ | ✓ | фьючерсы | ✓ | — ² | ✓ ³ | ✓ ⁴ | — ⁷ | — ⁷ | ✓ |
| Ордера через WS | ¹ | ¹ | ✓ | — | ¹ | — | — | ✓ | — | — | — |
| Hedge-режим | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — ⁵ | ✓ | ✓ | ✓ ⁹ |

¹ Методы `ws_*` сами выбирают транспорт (WS или REST) — вызов в любом случае одинаковый.
² KuCoin закрыл песочницу (хосты не отвечают) — демо нет.
³ Bitget Demo Trading: Demo API Key + заголовок `paptrading: 1` на продовом хосте с теми же символами
(а не ccxt-шными `SBTC/SUSDT`), WS — `wss://wspap.bitget.com`.
⁴ Gate TestNet: `https://api-testnet.gateapi.io/api/v4` (спот и USDT-M), WS — `wss://ws-testnet.gate.com`.
⁵ Dual-режим Gate (закрытие через `auto_size`) пока не поддерживается: только single (one-way).
⁶ MEXC спот: WS-потоки — NotSupported (биржа шлёт protobuf, ccxt 4.4.65 использует старые JSON-каналы); REST — ✓.
⁷ У MEXC в документации нет тестнета; у HTX указано «Testnet (Stopped)»: `demo=True` → `NotSupported`, тестируйте на paper.
⁸ В документации HTX нет механизма брокерского тега: недокументированный `channel_code` из ccxt удалён, своего ID нет.
⁹ BloFin, спот: нет amend, TP/SL и трейлинга (`NotSupported` до отправки); фьючерсы: режим Multi-Position не
поддерживается — `ConfigError` до торговли (см. «BloFin (бета)» ниже).

`demo=True` — собственный тестнет/демо биржи (Bybit — demo trading, остальные — sandbox биржи),
**кроме Binance** — см. ниже. Это не симулятор; симулятор в памяти —
режим paper (`paper=True`, см. «Paper-трейдинг и replay»).

### Binance: Demo Mode (по умолчанию) и тестнет

`create_exchange('binance', demo=True)` (или `demo='demo'`) — это **Demo Mode** (demo.binance.com), один
ключ для спота и фьючерсов. `demo='testnet'` — тестнет Binance.
Окружение — `ex.demo_env` (`'demo'` / `'testnet'` / `None`); `ex.demo` по-прежнему bool. На других биржах
`demo='testnet'` выбрасывает `ConfigError` (демо-окружение одно).

| | Demo Mode (`demo=True`) | тестнет (`demo='testnet'`) |
|---|---|---|
| Спот REST / WS API / потоки | `demo-api.binance.com/api` / `demo-ws-api.binance.com/ws-api/v3` / `demo-stream.binance.com/ws` | `testnet.binance.vision/api` / `ws-api.testnet.binance.vision` / `stream.testnet.binance.vision` |
| USDⓈ-M REST / потоки | `demo-fapi.binance.com` / `demo-fstream.binance.com` (`/public`, `/market`, `/private` — как в проде) | `testnet.binancefuture.com` / `fstream.binancefuture.com` |
| USDⓈ-M WS API (`ws_place_order`) | `testnet.binancefuture.com/ws-fapi/v1` ¹ | то же |
| COIN-M REST / потоки | `demo-dapi.binance.com` / `demo-dstream.binance.com` | `testnet.binancefuture.com/dapi` / `dstream.binancefuture.com` |
| Цены, стаканы | близки к бирже (не реальные сделки) | спот — независимы от биржи |
| Фильтры, лимиты IP, число ордеров | как на бирже | спот — «в основном» как на бирже |
| Баланс | сбрасывается в UI в любой момент | спот — сброс ежемесячно |
| `/sapi`, `/papi`, `/eapi`, `/futures/data` | нет → `NotSupported` с причиной, до любого сетевого запроса | нет |

¹ В документе USDⓈ-M «WebSocket API General Info» указан только этот хост; `demo-ws-fapi.binance.com` не
отвечает (202 → www.binance.com), а testnet.binancefuture.com — тот же бэкенд, что и demo-fapi (тот же
стакан и lastUpdateId; проверено без ключей 2026-10-26).
Документация: [Spot Demo Mode](https://developers.binance.com/docs/binance-spot-api-docs/demo-mode/general-info),
[USDⓈ-M General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info).
Без `/sapi`: `health().status` — `'unknown'` (без запроса), `check_key()` возвращает права ключа как `None`
с причиной в `unknown`; UID в Demo Mode читается из `/api/v3/account` (демо 2026-11-04). ccxt выводил тип рынка для ошибок и спотовые WS-потоки из хоста — исправлено
для демо-хостов (`bytx/exchanges/binance/demo.py`).

### Особенности демо-окружений

Особенности самих демо-окружений (не баги SDK; проверено 2026-10-26):

- **Спотовый тестнет Binance** (`demo='testnet'`, `testnet.binance.vision`) — отдельная система со своими
  ключами: ключ фьючерсного тестнета получает там -2015. В Demo Mode (`demo=True`) ключ общий.
- **Binance Demo Mode и фьючерсный тестнет** не имеют sapi (`apiRestrictions`): `check_key()` возвращает
  `can_trade`/`can_withdraw`/`ip_restricted` = None с причиной в `unknown` (UID — из `/api/v3/account`, в
  Demo Mode есть). Политика `withdraw='reject'` такой ключ с правом вывода не поймает — только предупреждение.
- **BingX VST** — права ключа (`/openApi/v1/account/apiPermissions`) и UID (`/openApi/account/v1/uid`)
  отвечают на VST-хосте по боевым путям (демо 2026-11-04): `check_key()` на VST знает право вывода. Баланс и
  Get Fund Flow на VST — в валюте `VST` (виртуальный USDT; в итогах портфеля — как USDT); журнал операций VST
  появляется с задержкой: сделки — 6–13 мин, фандинг — 20–65 мин после расчёта (2026-11-04); история ставок
  фандинга на VST то пустая, то годичной давности.
- **`health().status`** на Binance (Demo Mode и тестнет) и демо Bitget — `'unknown'`: у этих окружений
  нет страниц статуса; `reachable` и смещение часов измеряются как обычно.
- **Демо Bitget** — аккаунт пополняется вручную в веб-интерфейсе (Demo Trading: USDT на фьючерсах и
  споте). При пустом аккаунте торговые запросы получают `InsufficientFunds` (40762 / 43012).
  Get Trade Rate (`trading_fee`) на демо — 40404 «Request URL NOT FOUND» (ставки сделок демо — maker 0.02%,
  taker 0.06%, как в документации).
- **Демо Bybit** не имеет публичного демо-WS (рыночные данные идут из продовых публичных потоков, без ключей).

Поведение SDK, выявленное на демо:

- Режим позиций читается с биржи, а не предполагается one-way: **Bybit** — по символу из
  `GET /v5/position/list` (`positionIdx` 0 / 1–2; режим определяется как символ > монета > по умолчанию, кэшируется
  по символу), **Bitget** — `posMode` из Get Single Account. Не читается — `ExchangeError`.
  `set_position_mode` / `set_leverage` с «уже установлено» (Bybit 110025 / 110043) — это успех.
- Трекеры (`order_tracker`, `position_tracker`) берут REST-снимок, когда подписка уже активна:
  Bybit/Bitget — после ответа сервера на subscribe, остальные — после открытия соединения
  (`await sub.ready()`; лимит — `StreamConfig.ready_timeout`, 10 с). Ордер, поставленный сразу
  после `start()`, не теряется.
- Binance `amend_order(price=…)` без `size` отправляет полный размер ордера (`origQty`): Modify Order
  требует и количество, и цену.
- Там, где биржа изменяет ордер через отмену + новый ордер (Binance спот cancelReplace,
  BingX cancelReplace, Bitget modify-order для контрактов, Hyperliquid modify), `amend_order` даёт новому ордеру
  client_id, равный старому с новым случайным хвостом (тег бота, роль и уровень сохраняются; брокерский
  префикс Binance добавляется): ордер остаётся вашим для `owns` / `cancel_all`, журнал записывает
  `canceled` для старого и `replaced` (`replaces` = старый id), а хранилище намерений получает новое намерение с
  той же привязкой. Один и тот же client_id никогда не используется повторно (Binance: «unique id among open
  orders»; поиск по id был бы неоднозначным). KuCoin (alter не задаёт client_id нового ордера),
  MEXC и HTX (нет editOrder / эмуляции в ccxt) — `NotSupported`: отмените и поставьте заново.
- **Bitget** (демо 2026-11-04): `leverage()` — плечо кармана режима символа (cross — `crossedMarginLeverage`
  Get Single Account, isolated — `isolatedLong/ShortLever`; ccxt и в cross отдавал isolated); hedge — `side` ответа
  биржи = направление позиции (закрытие long — `buy` + `tradeSide close`), `Fill.side` / `Order.side` SDK —
  направление сделки (REST-сделки и WS-ордера закрытия ccxt отдавал покупкой; WS Fill Channel уже шлёт направление
  сделки); `modify_tp_sl` для TP/SL на всю позицию (`pos_loss` / `pos_profit`, size `"0"`) — на месте без окна
  (второй такой ордер на сторону биржа не создаёт, а заменяет прежний), а там, где перенос идёт отменой и новой
  постановкой, новый ставится тоже на всю позицию (раньше прежний снимался, а новый отклонялся до сети — позиция без
  стопа); объём ≤ 0 — `ConfigError` до любых действий; `tp_sl_order` находит сработавший / снятый TP/SL в истории
  plan-ордеров (Get History Trigger Order); отказ TP/SL по цене (40834, 40915, 40917 …) — `InvalidOrder`; отмена
  cancel-plan-order засчитывается только по `successList` (пустые списки — ордер перечитывается: его нет —
  `OrderNotFound`, жив — повтор с его `planType` или «отмена не подтверждена»).

Та же таблица в коде, без сети и ключей: `bytx.exchange_info('okx')` →
`ExchangeInfo(credentials=('api_key', 'secret', 'password'), markets=('spot', 'swap'), demo={...})`,
`bytx.exchanges_info()` — для всех бирж.

## Спот

`create_exchange(name, market='spot')` на Binance, Bybit, OKX и KuCoin работает по документации каждой биржи:

```python
ex = create_exchange('bybit', market='spot', api_key=..., secret=..., demo=True)
await ex.buy('DOGE/USDT', notional=10, client_id='b1')   # market buy на 10 USDT
await ex.order('DOGE/USDT', client_id='b1')               # в любом статусе, в пределах хранения биржи
await ex.find_order('DOGE/USDT', 'b1', timeout=8)         # None — нет; RequestTimeout — не смогли проверить
await ex.sell_all('DOGE/USDT', client_id='s1')            # весь свободный остаток базы; пыль → None + WARNING
```

| | Binance | Bybit | OKX | KuCoin |
|---|---|---|---|---|
| `buy(notional=…)` market | `quoteOrderQty` | `marketUnit=quoteCoin` | `tgtCcy=quote_ccy` | `funds` |
| шаг и минимумы | `LOT_SIZE`, `MARKET_LOT_SIZE` (market-ордера), `NOTIONAL` | `basePrecision`, `minOrderAmt` | `lotSz`, `minSz` | `baseIncrement`, `baseMinSize`, `minFunds`, `quoteMinSize` |
| ордер по `client_id` | `GET /api/v3/order` | `/v5/order/realtime` → `/v5/order/history` | `/trade/order` → `orders-history` (7 дней) → `orders-history-archive` (3 месяца) | HF: `hf/orders/client-order` → `hf/orders/done` (`lastId`); classic: только активные |
| demo | Demo Mode / testnet | demo | demo trading | — |

- `buy(notional=…)`: сумма quote уходит параметром биржи и округляется вниз по шагу quote; меньше мин. суммы (у OKX —
  меньше `minSz` по опорной цене) — `OrderTooSmall` до отправки. Продажа на сумму и другие биржи — пересчёт в базу
  по опорной цене.
- `sell_all(symbol, *, client_id=None)` — только спот (иначе `ConfigError`): свободный остаток базы строго вниз по
  шагу лота, затем market-продажа через `place_order`. Пыль (меньше шага, мин. объёма или мин. суммы) — `None`
  и WARNING.
- Сбой страницы истории никогда не считается «ордера нет»: `find_order` — `RequestTimeout`; обрыв пагинации —
  `ExchangeError`. Таймаут размещения на споте сверяется тем же поиском — второй ордер не отправляется.
- KuCoin: пустой ответ `data: null` считается «не найден» (ответ на несуществующий ордер в документации не описан).
- Один запрос на market-ордер: `buy(notional=…)` без цены, `size` проверяет минимум суммы по цене из кэша на 5 с.
  Ответ без исполнения (Bybit, OKX, KuCoin) дополняется из потока `ex.stream.orders()`, если он подписан, иначе одним
  `order(id)`; не вышло — у ордера `partial_info=True`.
- Явный отказ биржи — сразу `InvalidOrder` (ниже минимума у Bybit — `OrderTooSmall`), ошибка ccxt — в `__cause__`;
  сверка — только при таймаутах и кодах «исход неизвестен».
- `cancel_order(..., strict=True)` — если ордер уже не открыт, ошибка биржи (например, Binance -2011), а не финальный
  статус. `exchange_client_id(ex, market, client_id)` — cid в том виде, в каком он уйдёт на биржу.
- Binance: `demo=True` — Demo Mode, `demo='testnet'` — прежний testnet.

## WebSocket

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps, book.microprice, book.imbalance(5))

handle = ex.stream.subscribe('trades', 'BTC/USDT', on_trade)      # или с колбэком
ex.stream.on_state(lambda ev: print(ev.state, ev.stream, ev.attempt))
```

Потоки: `order_book`, `ticker`, `best_bid_ask`, `trades`, `candles`, приватные `orders`, `fills`,
`positions`, `balance`. Все подписчики делят один поток. Обрыв → переподключение с backoff;
стакан или тикер молчит дольше порога → этот топик переподписывается, а если затихло всё
соединение — оно переподключается (у сделок и свечей порога тишины по умолчанию нет);
стакан проверяется на разрывы и пересечение и пересобирается из REST-снимка.

**Общие соединения для многих ботов в одном процессе** — `bytx.hubs`:

```python
from bytx import hubs

acc = await hubs.account('bybit', demo=True, api_key='…', secret='…')   # один на аккаунт
fills = acc.fills('BTC/USDT:USDT')                # своя очередь, один watch_my_trades на весь хаб
async for f in fills: ...                         # f.id — id сделки биржи, как в REST my_trades
acc.open_orders('BTC/USDT:USDT'), acc.position('BTC/USDT:USDT'), acc.balance()  # None — поток не актуален, идти в REST

mkt = await hubs.market('binance', demo=False)    # один на биржу: публичные тикеры всех ботов
await mkt.watch('ETH/USDT:USDT')                  # +1 потребитель символа
mkt.price('ETH/USDT:USDT')                        # None — свежего тикера нет
await mkt.unwatch('ETH/USDT:USDT')                # −1; последний отписывает топик на бирже
await hubs.release(mkt); await hubs.release(acc)  # последний release закрывает соединение
hubs.stats()                                      # потребители, WS-соединения, символы — для метрик
```

Хаб аккаунта держит одно приватное соединение с трекерами ордеров и позиций по всем символам аккаунта; хаб рынка —
одну подписку тикера на символ. `unwatch` последнего потребителя отписывает топик там, где это умеет ccxt (Binance,
Bybit, OKX), и закрывает соединение, оставшееся без подписок. На Binance хаб рынка кладёт тикеры плотно, до 50 на
соединение, а не по кругу ccxt на 50 соединений. Хаб никогда не ставит и не снимает ордера.

## Скальпинг

```python
async with ex.order_tracker('BTC/USDT') as orders, ex.dead_man_switch(30):
    price = ex.round_price('BTC/USDT', book.best_bid[0], side='buy')   # в сторону пассивной стороны
    o = await ex.ws_place_order('BTC/USDT', 'buy', size=0.001, price=price, post_only=True)
    await ex.ws_amend_order('BTC/USDT', o.id, price=price - 1)
    done = await orders.wait(o.id, timeout=30)                           # closed / canceled
```

- ордера через WS с откатом на REST (`ws_place_order`, `ws_amend_order`, `ws_cancel_order`);
- `OrderTracker` / `PositionTracker` — состояние из приватного WS, сверка через REST после обрыва;
- `OrderBook`: `spread_bps`, `microprice`, `imbalance`, `depth`, `fill_price(notional=…)`;
- метрики: `ex.metrics()` (p50/p99 по методам), `ex.stream.stats()` (задержка биржа → мы).

## Paper-трейдинг и replay

```python
from bytx import create_exchange
from bytx.paper import PaperConfig, Recorder, replay_exchange

# живой стакан биржи, виртуальный аккаунт: ключи не нужны и никуда не отправляются
ex = create_exchange('binance', paper=True, paper_config=PaperConfig(balance={'USDT': 1000}))
o = await ex.buy('BTC/USDT', notional=50)          # проходит по стакану, тейкер-комиссия базового уровня
print(ex.summary())                                # реализованный / нереализованный PnL, комиссии, фандинг

with Recorder(live_ex, 'btc.jsonl.gz'):            # запись потоков ex.stream в файл
    ...

ex = replay_exchange('btc.jsonl.gz')               # те же данные, виртуальные часы, без сети
result = await ex.replay.run(strategy)             # strategy(ex) — обычный код на ex.stream / ex.buy
result.fingerprint()                               # те же входы → те же исполнения
```

`create_exchange(..., paper=True)` (или `Exchange.paper(name)`) возвращает `PaperExchange` — тот же
интерфейс `Exchange`: единицы, округление, минимумы, client_id, `pre_submit`, RiskGuard, журнал,
`stream.orders/fills/positions/balance`, трекеры, TP/SL, `close_position`, `kill_switch`, `flatten`.

- **market** проходит по уровням стакана (тейкер); пересекающий **limit** — тейкер до своей цены,
  остаток встаёт в стакан (IOC — expired, FOK — всё или ничего); стоящий limit исполняется как мейкер по своей цене,
  когда стакан или сделка его пересекают; частичные исполнения; собственные исполнения съедают уровни
  до следующего снимка. Пересекающий **post-only** и **market** без встречной ликвидности ведут себя как
  биржа по её документации (`bytx.exchanges.post_only_cross` / `market_no_liquidity`, таблицы в
  `exchanges/<exchange>/matching.py` с URL и `verified`): отклоняются с `InvalidOrder` (Binance -5022) или
  создаются и отменяются — `status='canceled'` без исполнения (post-only: Bybit, OKX, Hyperliquid; market: Bybit);
  стакана нет вовсе — `InvalidOrder`. **reduce-only**: нет
  позиции — `PositionAlreadyClosed`, исполнение ограничено позицией, при закрытии reduce-only ордера
  и TP/SL этой стороны отменяются. **TP/SL** срабатывают по mark (или last), исполняются по стакану.
- **Комиссии** — базовый уровень каждой биржи по её документации (`bytx.exchanges.<exchange>.fees`,
  каждая ставка с URL и `verified`); свои — `PaperConfig(fees=(maker, taker))`.
- **Позиции, баланс, PnL, фандинг** — в памяти: one-way и hedge, плечо, маржа, `InsufficientFunds`;
  фандинг по ставке в `next_funding_time` (живой paper опрашивает ставку).
- **Ликвидация упрощена** и помечена (`raw['paper']['liquidation_model'] == 'simplified'`):
  `entry × (1 ∓ 1/leverage ± mmr)` по каждой позиции, закрытие по mark.
- Запись — JSON Lines (`.gz`), версионированный формат `bytx.ws-record`; рынки хранятся в файле.
  Replay детерминирован, если стратегия не зависит от реального времени: спите через `ex.clock.sleep`;
  сгенерированные SDK client_id и рандомизация `twap` / `iceberg` без `seed` в replay берутся из
  генератора с зерном `PaperConfig(seed=)` (по умолчанию 0).
- Не эмулируются: `cancel_all_after`, `transfer` (`NotSupported`).

> ⚠️ Paper **не** заменяет демо/тестнет биржи: модели ликвидации и маржи упрощены.

## Алгоритмы исполнения

```python
algo = ex.algos.chase('BTC/USDT:USDT', 'buy', size=0.5, max_moves=20, max_deviation_bps=30)
algo.pause(); algo.resume()                         # на паузе: ни новых ордеров, ни перекотировок
res = await algo                                    # AlgoResult: status, reason, filled, average, children
await ex.algos.market('BTC/USDT:USDT', 'sell', size=1, max_slippage_bps=20)        # limit IOC с ограничением
await ex.algos.twap('ETH/USDT:USDT', 'buy', size=10, duration=600, slices=20, randomize=0.3, seed=1)
await ex.algos.iceberg('BTC/USDT:USDT', 'sell', size=2, price=72_000, visible=0.1)
await ex.algos.bracket('BTC/USDT:USDT', 'buy', size=0.1, take_profit=72_000, stop_loss=66_000)
trail = ex.algos.trailing_stop('BTC/USDT:USDT', 'long', distance_pct=1.5, exchange_stop=True)
await ex.algos.cancel_all()                         # отменить все незавершённые алгоритмы
```

`ex.algos.*` запускает алгоритм фоновой задачей и сразу возвращает его объект: `await algo` даёт результат
(`AlgoResult`: `status` 'done' / 'canceled' / 'failed', `reason`, `filled` / `average` / `fee`,
`children`, `extra`), плюс `algo.status`, `algo.filled`, `pause()` / `resume()` / `await cancel()`.
Неверные аргументы сразу выбрасывают `ConfigError`. Работает одинаково на бирже, в paper и replay (время —
`ex.clock` от paper; в тестах — `VirtualClock`). Модуль импортируется при первом обращении к `ex.algos`:
горячий путь `place_order` / WS не меняется.

- **chase** — post-only по лучшей цене своей стороны; когда цена уходит — отмена, подтверждённый
  финальный статус, новый ордер на остаток (не amend: на некоторых биржах он теряет post-only).
  Отскок post-only (отклонён при размещении — Binance -5022, BingX 101215 — или отменён биржей) повторяется.
  Лимиты: `max_moves`, `max_deviation_bps` от стартовой цены, `timeout`, `min_interval` между перекотировками;
  при исчерпании — `on_exhausted='stop'` или `'market'` (IOC с ограничением).
- **market** — limit IOC не хуже «лучшая встречная (или mid) ± `max_slippage_bps`», цена округляется внутрь
  ограничения, а где биржа ограничивает цену ордера коридором (OKX `price-limit`: за ним 51006, demo BTC — ±0.5%) —
  и внутрь коридора (`extra['price_band']`); без встречной ликвидности в пределах цены ордер не отправляется;
  `retries` используют ту же цену. Так же — IOC-части twap и остаток chase.
- **twap** — `slices` частей за `duration` секунд: части догоняют отставание от графика, `randomize`
  рандомизирует моменты и веса (`seed`), часть меньше минимального ордера накапливается, хвост меньше минимума —
  в последнюю часть; `slice_type='market'` (с `max_slippage_bps` — IOC) или `'limit'` (post-only, остаток
  переходит в следующую часть, в конце — market).
- **iceberg** — в стакане только `visible` (± `visible_jitter`) по `price`, но не меньше минимального ордера
  (`extra['visible_min']`); часть, отменённая кем-то другим, — остановка (`child_canceled`); объём, который не
  делится на части меньше себя, — `OrderTooSmall` (весь объём молча в стакан не выставляется).
- **bracket** — вход market/limit (`entry_timeout`) → `set_tp_sl` на исполненный размер → **OCO**:
  как биржа связывает TP и SL, следует её документации и demo (`bytx.exchanges.tp_sl_oco`): OKX — один
  algo-ордер `oco` (срабатывает одна нога; у `conditional` с TP и SL в one-way биржа исполняет только SL — дока
  Place algo order, demo 2026-11-04); Binance, Bybit (Partial-ноги), BingX, Hyperliquid (grouping `na`), KuCoin —
  ноги независимы, и алгоритм отменяет соседку сработавшей ноги (иначе reduce-only нога
  закрыла бы следующую позицию). Demo 2026-11-04: у Bybit Partial и BingX соседка жива, пока позиция открыта
  (OCO нет), а при закрытии позиции оставшиеся ноги биржа снимает сама; KuCoin — только по документации (связь
  не описана, demo нет), `verified=False`. Нога сработала между чтением ног и чтением позиции — причина всё равно
  `take_profit`/`stop_loss`. OKX: сработавшая algo-пара минуты не находится по `algoId` (51603) — тогда причина
  `position_closed`. Защита не поставлена — вход закрывается (`on_protect_fail='close'`). С хранилищем намерений план
  защиты (цены TP/SL, `trigger_by`, объём, client_id ног) пишется **до отправки входа** — процесс убит между входом и
  ногами: `ex.recover()` ставит недостающие ноги по плану (см. «Восстановление после рестарта и стратегии»); ноги
  поставлены — план закрыт, дальше их ведёт алгоритм.
- **trailing_stop** — уровень на стороне клиента, следующий за экстремумом (`distance` / `distance_pct`,
  `activation_price`), срабатывает через reduce-only market. `exchange_stop=True` — резервный SL на
  бирже, переносится по принципу make-before-break (Bybit Full — `modify_tp_sl` на месте).

Минимальный ордер части — шаг объёма, `min_amount` и **минимальный нотионал** `min_cost` по цене ордера
(с запасом 1%; у reduce-only нотионал не проверяется — как и в `place_order`). Весь `size` меньше минимума —
`status='failed'`, `OrderTooSmall`, без ордеров; остаток меньше минимума поставить нельзя — итог `filled`,
остаток в `extra['below_min']` (chase снимает частично исполненную лимитку).

Общие правила: каждый дочерний ордер — обычный ордер SDK (тег бота `client_id_prefix`, брокерский
тег, `pre_submit`, RiskGuard); с журналом позиций — роль (`role=`, `entry`/`tp`/`sl` для bracket,
`trailing` для trailing) и `position_id=`. Ордер с неизвестным исходом не перекотируется — он сверяется по
client_id; новый ордер не уходит, пока предыдущий может быть жив; сетевые сбои, «ордера не видно» у принятого
биржей ордера и неясный отказ отмены повторяются до `max_errors`. Ответ отмены дочернего ордера без объёма
исполнения (Bitget Cancel Order — только `orderId` / `clientOid`; OKX, Bybit, KuCoin, HL — только id) — ордер
перечитывается (сначала живой трекер, иначе REST): исполненное до отмены не теряется, chase не ставит остаток заново.
Отмена принята, а чтение видит ордер открытым (асинхронная отмена) — второй ордер не ставится; финала нет за
`max_errors` пауз `poll` — алгоритм `failed` с причиной `cancel_unconfirmed` (ордер в `extra['unconfirmed']`, уборка
снимает его ещё раз; с хранилищем намерений запись `unclean` — `recover()` после рестарта снимет живой ордер).
Состояние ордеров берётся из живого `OrderTracker` (`tracker=`), после обрыва WS — из REST, пока трекер не ресинхронизируется.
`cancel()` отменяет рабочие ордера (для bracket/trailing — и защиту); сбой алгоритма или отмена его задачи
**не** снимает защиту позиции. Алгоритмы живут в памяти процесса: после рестарта они **не возобновляются**.
`kill -9` (в отличие от отмены задачи) не снимает рабочие ордера: лимитка chase, видимая часть iceberg,
лимитная часть twap, лимитный вход bracket остались бы в стакане по последней цене без присмотра. С хранилищем
намерений (`intents=IntentStore(...)`) это делает `ex.recover()` — **безопасная политика**
(`cancel_orphaned_algos=True` по умолчанию): рабочие ордера незавершённых алгоритмов снимаются, защита позиции
(ноги bracket, SL трейлинга) остаётся, исполненное не трогается — `report.algos`, см. «Восстановление после
рестарта и стратегии». bracket, убитый между исполнением входа и ногами, получает ноги по плану защиты (записан до
входа) — это защита позиции, не продолжение. OCO-слежение bracket и подтяжка SL трейлинга после рестарта не
возобновляются.

> ⚠️ **Осиротевшая нога OCO.** Там, где биржа не связывает TP и SL (все, кроме OKX), соседку сработавшей ноги
> отменяет задача bracket. Если задачу отменили, алгоритм упал или процесс перезапустился, связь потеряна: после
> срабатывания одной ноги другая (reduce-only) остаётся на бирже и закроет **следующую** позицию на той же
> стороне. `ex.recover()` отменяет **ваши собственные** такие ноги, если позиции на их стороне точно нет
> (`report.orphans_canceled`; `cancel_orphaned=False` — только отчёт, в `report.orphaned`), см. ниже.
> Demo 2026-11-04: Binance оставляет ноги после закрытия позиции — `recover()` их нашёл и снял;
> Bybit, BingX и OKX (`cxlOnClosePos`) снимают ноги сами, когда позиция закрыта. Остаётся риск, когда
> позиция символа больше объёма bracket (другой бот, ручной вход): нога живёт на остатке позиции, это
> не «сирота», и у Bybit (trading-stop) и BingX (условные ордера) у ноги нет client_id — `recover()`
> не опознаёт её как вашу.

## Размер позиции от риска

```python
plan = await ex.risk_size('BTC/USDT:USDT', 'long', risk_usdt=50, stop=66_000)   # без ордеров
res = await ex.open_position('BTC/USDT:USDT', 'long', risk_usdt=50, stop=66_000, take_profit=72_000,
                             leverage=5, slippage_bps=5)
res.plan.contracts, res.plan.risk, res.risk          # размер, плановый риск и фактический риск после исполнения
res.stop_mode                                        # 'attached' | 'separate'
```

Размер: убыток на единицу базы = `|entry − stop exit| + fee_rate × (entry + exit)`; вход по рынку использует лучшую
встречную цену, `slippage_bps` ухудшает и вход, и выход по стопу; `fee_rate` по умолчанию — тейкер-комиссия
базового уровня биржи по её документации (`bytx.exchanges.<exchange>.fees`; paper — ставка аккаунта).
Контракты = `risk_usdt / (loss × contractSize)` с округлением вниз до шага биржи, поэтому фактический риск
(`RiskPlan.risk`, с комиссиями) никогда не превышает `risk_usdt`. Если минимальный размер (`min amount`, `min_cost`)
рискует больше — `errors.MinSizeExceedsRisk` (`details`: `min_contracts`, `min_risk`), ордер не отправляется.
Плечо не влияет на риск, но задаёт маржу (сверяется со свободным обеспечением, `check_margin`) и
оценку ликвидации: в isolated-режиме стоп за ценой ликвидации отклоняется.

Стоп. Для входа по рынку на биржах, чья документация позволяет прикрепить SL к ордеру входа
(`bytx.exchanges.attached_sl`: Bybit `stopLoss`, OKX `attachAlgoOrds`, BingX `stopLoss`, KuCoin
`st-orders`), SL и TP уходят **в том же запросе**. Binance (в New Order нет полей TP/SL),
Hyperliquid (SDK отправляет grouping `na`), вход лимиткой (`price=` — IOC), `stop_mode='separate'` или явный
`trigger_by` — reduce-only `set_tp_sl` на исполненный размер сразу после входа. Если прикреплённый вход исполнился частично
(OKX ставит algo только после полного исполнения входа), SL проверяется на бирже (`tp_sl_orders`; для OKX
документация говорит, что его ещё нет, — без запроса); если его нет, он ставится отдельно, как описано ниже (`stop_mode='separate'`, предупреждение
в `res.warnings`). Если SL не встал —
поставленный TP отменяется, позиция закрывается reduce-only ордером по рынку и выбрасывается `errors.ProtectionNotPlaced`
(`details['rolled_back']`, `close`, `close_error`); `on_stop_fail='keep'` — без закрытия. Если исход входа
неизвестен, но позиция есть, прикреплённый SL проверяется на бирже (OKX тоже: исполнение неизвестно):
если он есть, второй не ставится; если нет, стоп ставится отдельно (предупреждение в `res.warnings`). Есть ли стоп,
решает `bytx.protection.find_leg`: client_id там, где он отправляется на биржу, иначе тип + сторона +
цена срабатывания с точностью до шага цены биржи.

Прогон на demo 2026-11-04 (Bybit / Binance / OKX / BingX, BTC и ETH):

- убыток при срабатывании стопа (сделки входа и выхода с биржи, сверка с журналом средств биржи
  `account_report`) — в пределах `RiskPlan.risk` плюс проскальзывание сверх `slippage_bps`; объём сделок равен
  расчёту с contractSize (OKX — контракты 0.01 BTC / 0.1 ETH); комиссия сделок равна taker-ставке из документации
  (Bybit 0.055%, OKX и BingX 0.05%); Binance Demo Mode — ставка аккаунта 0.04% < 0.05% документации (расчёт с
  запасом; свой уровень — `fee_rate=`);
- прикреплённый SL срабатывает от триггера биржи по умолчанию: Bybit `slTriggerBy` и OKX `slTriggerPxType` — last,
  BingX — mark; отдельный (`set_tp_sl`) — по `trigger_by` / умолчанию SDK;
- вход с прикреплённым SL не по ту сторону цены биржа отклоняет **целиком** (Bybit 10001, OKX 51051, BingX
  101400 — `InvalidOrder`), позиции нет; отказ отдельного SL (Bybit 10001, OKX 51280, Binance -2021, BingX
  110411) — откат reduce-only, `ProtectionNotPlaced(rolled_back=True)`;
- BingX: ноги STOP_MARKET / TAKE_PROFIT_MARKET встают при исполнении входа на весь объём —
  `attached_sl('bingx')` сверен (`verified=True`); OKX: TP+SL из `attachAlgoOrds` биржа держит ордером
  `oco` (только SL — `conditional`) — SDK запрашивает список TP/SL как `conditional,oco`; KuCoin — только по
  спецификации документации (demo нет, `verified=False`). Частичное исполнение прикреплённого входа на demo не
  воспроизвести (market исполняется целиком).

С хранилищем намерений (`intents=`) план стопа — цены SL и TP, `trigger_by`, сторона, режим
`attached` / `separate`, client_id стопа (`stop_client_id`: OKX `attachAlgoClOrdId`, для separate —
сгенерирован заранее и отправляется как есть; Bybit trading-stop и пара TP+SL на OKX — `None`) —
записывается в намерение входа (`Intent.tp_sl`) **до отправки** в обоих режимах; вход помечается
защищённым, когда стоп поставлен (или позиции нет). Если процесс убит в любой момент, `recover()` доставит
защиту без дублей (ниже).

`open_position` открывает только **новую** позицию: если по символу она уже есть (в hedge-режиме — на этой
стороне), `errors.PositionOpenError`. С журналом позиций — роли `entry` / `sl` / `tp` / `close` и
`position_id=`. Работает на бирже и в paper; модуль `bytx.risk_sizing` импортируется при первом вызове.

## Восстановление после рестарта и стратегии

```python
from bytx.recovery import IntentStore
from bytx.strategy import Strategy, run_strategy

ex = create_exchange('okx', ..., client_id_prefix='bot42', intents=IntentStore('bot42.sqlite'))
report = await ex.recover(resubmit=False)        # при старте процесса, до любых новых ордеров
report.adopted, report.finished, report.abandoned, report.unverified, report.live
report.orphans_canceled, report.orphaned, report.orphan_reasons   # осиротевшие TP/SL: отменены / оставлены и почему
report.algos     # алгоритмы ex.algos, умершие с процессом: canceled / finished / kept + reasons / filled / closed
report.algos[0].protection, report.algos[0].protected   # bracket: итог защиты по плану по ногам, поставленные ноги
report.ok            # все намерения сверены — и только: позицию без стопа ok НЕ показывает
report.unprotected   # ⚠️ алгоритмы, чья позиция может остаться без стопа (решает вызывающий) — проверяйте вместе с ok
report.stale_plans   # входы, чей план TP/SL закрыт, а не применён ('superseded' / 'expired')
report.deferred      # открытые входы с планом trigger_by без позиции — защита после исполнения
```

> ⚠️ **Голую позицию показывает `report.unprotected`, а не `report.ok`.** `ok=True` значит только «намерения
> сверены»: bracket, у которого цена прошла SL (`price_passed`), `protect=False` или отказ биржи, оставляет позицию без
> стопа и при `ok=True`.

`IntentStore` (stdlib `sqlite3`, без зависимостей) записывает каждый ордер **до отправки** (`pending`, последним
перед сетью — после `pre_submit` и журнала), затем его исход: `placed` / `final` / `rejected` (явный отказ) /
`unknown`; исход пишется после журнала. Ноги TP/SL с client_id — тоже, со ссылкой на родительский вход (Bybit trading-stop без
client_id — нет). Отмены через SDK и `OrderTracker` закрывают намерения. `durable=True` (по умолчанию) —
`synchronous=FULL`; `durable=False` — быстрее, переживает убийство процесса, но не отключение питания. Один
файл обслуживает несколько аккаунтов: пространство имён = биржа, рынок, live/demo/paper, `account=` и отпечаток ключа
API (хеш, без секретов; paper без ключа — `paper`): `AccountPool(intents=store)` и общий файл разных ключей не
смешивают намерения — `recover()` аккаунта A не трогает B. Схема 5: строки схем 1–4 (без отпечатка) при заданном
`account=` переносятся автоматически при первом обращении; без `account=` `recover()` их не трогает — они в
`report.unverified` с `ConfigError` и WARNING, перенос своих — `store.claim_legacy(ex)`. После смены ключа API
пространство имён новое — прежние намерения переносит `store.claim_legacy(ex, namespace=<прежнее>)`. Один процесс на
файл хранилища: `IntentStore` держит файловую блокировку `<db>.lock` (POSIX), второй процесс получает `ConfigError`
при открытии (несколько `IntentStore` на одном файле в одном процессе — можно). Выкат — **stop-first**; параллельным
процессам — разные файлы; `recover()` — при старте, до новых ордеров.

`ex.recover()` сверяет открытые намерения с биржей по id / client_id / открытым ордерам:

| Ордер на бирже | Исход |
|---|---|
| найден, жив | `adopted` (журнал позиций получает роль и позицию из намерения) |
| найден, завершён | `finished` |
| подтверждённо отсутствует | `resubmit=True` или `callable(intent)` и не старше `max_age` — пересоздаётся **с тем же client_id** (`resubmitted`); иначе `abandoned` |
| проверить не удалось | `unverified`: ничего не отправляется, намерение остаётся открытым |

> ⚠️ Биржи отклоняют дубль client_id только среди **открытых** ордеров, а поиск по client_id находит
> завершённые ордера лишь ограниченное время (OKX — отменённые неисполненные ордера около 2 ч, Binance — 3 дня).
> Поэтому `max_age=None` (без ограничения) опасен: давно исполненный и исчезнувший ордер может быть пересоздан.
> Оставляйте `max_age` (по умолчанию 300 с). Тот же client_id с другим запросом, пока намерение открыто, —
> `ConfigError` до отправки; если намерение закрыто, оно перезаписывается новым запросом целиком — и тем же запросом
> тоже: план TP/SL, `protected`, `filled` прошлого входа не переносятся, его ноги новый вход не покрывают.

Свежие намерения ждут `settle` секунд (поиск на бирже может запаздывать). Вход с планом защиты, чьи TP/SL
так и не были записаны (падение между входом и защитой: `place_order` Binance/HL, `open_position` в обоих
режимах), получает недостающие ноги (`protect=True`, reduce-only) — но сначала `recover()` перечитывает
открытые TP/SL биржи по символу и стороне: если стоп уже есть (прикреплён биржей к attached-входу,
поставлен до падения), второй не ставится (`find_leg`: client_id, иначе тип + сторона + цена с точностью до шага).
Список TP/SL не читается — `unverified`, ничего не ставится (лишний стоп и позиция без стопа одинаково
плохи — повторите `recover()`); attached-вход ещё исполняется — тоже `unverified`; лимитный вход в книге с планом
`trigger_by` (ноги — на позицию), позиции ещё нет — `report.deferred` (на `ok` не влияет), вход не помечается
защищённым, ноги ставит `recover()` после исполнения. Вход помечается
защищённым, только когда поставлены все ноги. План входа закрывается (схема 5, `Intent.plan_retired`) и не
применяется: провал защиты сообщён вызывающему (`ProtectionNotPlaced` из `place_order` / `open_position` —
'protection_failed'); есть более новый вход той же стороны символа ('superseded'); план старше `protect_max_age`
(по умолчанию сутки) от последнего движения входа (исполнение / обновление ордера с биржи), позиция стороны есть, ног
нет ('expired'). Последние два — в `report.stale_plans` с причиной и WARNING; защиту позиции решает вызывающий. Собственный вход с защитой после него —
`with ex.intents.protection({'stop_loss': ..., 'take_profit': ...}):` вокруг `place_order`. Сбой внутри
`recover()` не размножает ноги: перед пересозданием ноги проверяется, нет ли уже ноги того же типа для того же
входа. Нога TP/SL хранит `trigger_by` (`Intent.trigger_by`) и пересоздаётся с тем же типом триггера.

**Осиротевшие TP/SL** — живые ноги без позиции на своей стороне. `recover(cancel_orphaned=True)` (по
умолчанию) отменяет их, только если они **ваши** (намерение в хранилище или тег `client_id_prefix`;
чужие, ручные и без client_id — никогда) и позиции по символу и стороне точно нет: позиции перечитываются
с биржи прямо перед отменой, ошибка чтения — не отменяется; вход на той же стороне ещё открыт (ноги Binance
ставятся до исполнения) — не отменяется. Отмена идёт через `cancel_tp_sl` (algo Binance/OKX,
стоп-ордера KuCoin); сбой одной ноги не ломает `recover()`. Отменённые — `report.orphans_canceled`
(намерение `final` с пометкой `orphan_canceled`, событие `canceled` в журнале позиций), оставленные —
`report.orphaned`, причина — `report.orphan_reasons[id]`. Кандидаты ищутся по символам всех намерений
в хранилище и `symbols=`.

**Алгоритмы исполнения после рестарта** (`ex.algos`: chase, twap, iceberg, bracket, trailing, market с
потолком) не продолжаются — прогресс жил в памяти процесса. С хранилищем запуск алгоритма записывается до
первого дочернего ордера (таблица `algos`: id, вид, символ, сторона, объём; `running`), конец — `finished`
(рабочие ордера сняты им самим) или `unclean` (отмена не удалась, исход дочернего неизвестен); намерение
каждого дочернего ордера и ноги TP/SL — с `algo_id` (метки в client_id нет: формат client_id не меняется, а у
ног Bybit/BingX client_id нет вовсе). `recover(cancel_orphaned_algos=True)` (по умолчанию) — для каждого
незавершённого алгоритма (`running` / `unclean`, не работающего в этом же процессе):

| Ордер алгоритма | Что делает `recover()` |
|---|---|
| рабочий (обычный ордер: лимитка chase, часть twap / iceberg, вход bracket, IOC), живой | снимает через `cancel_order`; исполненная часть остаётся позицией (`filled` ордера и `report.algos[i].filled`) — `canceled` |
| рабочий, исполнился / снят, пока процесс лежал (или исполнился между сверкой и отменой) | `finished` |
| рабочий, до биржи не дошёл | `abandoned`, **не пересоздаётся** и при `resubmit=True` (вести его некому) |
| нога TP/SL (ноги bracket, SL трейлинга) | **остаётся** — защита позиции (`kept`, причина в `reasons`); без позиции её снимает логика осиротевших |
| нога bracket по плану, которой нет (умер между входом и ногами) | **ставится** (`protect=True`), если позиция стороны плана есть — `protected`, итог по ноге в `protection` (ниже) |

Запись алгоритма закрывается (`recovered`) — повторный `recover()` его не трогает; рабочий ордер не проверен
(сеть) или не снялся — запись остаётся открытой, следующий `recover()` повторит. `cancel_orphaned_algos=False` —
только отчёт (рабочие ордера подхвачены как раньше, `kept` с причиной). bracket без плана защиты (запись схемы 3,
план снят отменой алгоритма) — пояснение в `notes` (TP/SL стороны на бирже есть — не тронуты; нет — «позиция без
защиты»); с планом — ноги по плану (ниже). Один процесс на пространство имён хранилища: `recover()` другого процесса
снял бы рабочие ордера живого алгоритма. Demo 2026-11-04 (`kill -9` дочернего процесса): Bybit SOL — chase и twap
(лимитная часть в стакане) → `recover()` снял ордер (биржа подтверждает `canceled`), повторный — без действий;
Binance SOL — bracket: ноги оставлены защитой, лимитный вход снят; Bybit XRP — то же с ногами без client_id.
Продолжение алгоритмов (resume) не реализовано.

**Защита позиции bracket по плану.** bracket пишет план защиты — сторона позиции, цены TP/SL, `trigger_by`, объём,
client_id ног восстановления — в свою запись (`algos.plan`) **до отправки входа** и закрывает план сам (`retired`),
как только ноги поставлены (`protected` — дальше ноги ведёт алгоритм), его отменили (`cancel()`, до снятия ног) или
защита не встала (`on_protect_fail`). Процесс убит (или задачу остановили: `task.cancel`, SIGTERM — запись тогда
`unclean`, не `finished`) между исполнением входа и ногами — алгоритм не продолжается, а `recover(protect=True)`
(по умолчанию) после сверки и снятия рабочего входа ставит недостающие ноги — только защиту позиции:

- план старше `protect_max_age` (по умолчанию сутки, `None` — без предела) от последнего движения входа (время
  исполнения / обновления ордера входа с биржи; лимитный вход мог стоять дольше суток) — не применяется (`expired`):
  позиция могла смениться; проверяется, только когда позиция стороны плана есть и ног не хватает;
- исполнение входа — по ордерам алгоритма; завершённые перечитываются с биржи (ответ отмены Bybit / OKX / KuCoin / HL
  / Bitget — только id), сбой чтения — `unverified`; вход ещё работает (`cancel_orphaned_algos=False`, отмена не
  удалась) — `unverified`, ноги на часть объёма не ставятся;
- нога уже есть (живое намерение ноги алгоритма; на бирже — по client_id плана / ног алгоритма; по виду + стороне +
  цене — только нога без client_id: Bybit trading-stop, BingX, созданная не раньше входа этого bracket, и одна такая
  нога — одному bracket за восстановление; пара OKX — обе) — вторая не ставится: повторный `recover()` и `kill -9`
  внутри него ног не дублируют; чужие ноги (другой бот, свой другой bracket с теми же ценами) не засчитываются и не
  усыновляются;
- позиции стороны плана нет — ничего (`no_position`); открыта только другая сторона — ничего, предупреждение
  (`opposite_side`; в hedge — отдельная позиция, не трогается);
- объём — исполненный входом (за вычетом закрытого алгоритмом), не больше позиции; меньше минимального ордера —
  `too_small`. TP/SL «на всю позицию» не используется: Bybit Full заменил бы чужой стоп позиции, Binance
  `closePosition` — один на сторону, и такой стоп защищал бы чужой объём;
- цена по `trigger_by` плана (mark — mark позиции / тикер, last — последняя сделка) уже за триггером — нога сразу
  сработала бы или была бы отвергнута: не ставится (`price_passed`, предупреждение в логе); **позицию SDK не
  закрывает** — решает вызывающий;
- нога алгоритма сработала — ничего (`triggered`); снята или пропала из истории биржи — не досоздаётся (`canceled`);
  исход постановки неизвестен — `unverified`, следующий `recover()` сверит.

```python
report = await ex.recover()
for a in report.unprotected:      # позиция может быть без стопа: SL плана не стоит / без плана — SL стороны нет
    ...   # a.symbol, a.side, a.protection, a.notes: закрыть позицию или поставить стоп — решает вызывающий
```

Итог по ноге — `a.protection['stop_loss' | 'take_profit']`: `placed` (в `a.protected` и `report.protected`),
`present`, `triggered`, `canceled`, `no_position`, `opposite_side`, `price_passed`, `too_small`, `expired`, `failed`
(ошибка — в `report.failed`), `unverified`, `disabled` (`protect=False` — только отчёт). При `failed` и `unverified`
запись алгоритма остаётся открытой — следующий `recover()` повторит (не дольше `protect_max_age`); при остальных
итогах запись закрывается. Ноги по плану независимы (OCO-слежения нет): где биржа не снимает оставшуюся ногу при
закрытии позиции (Binance), её снимет следующий `recover()` (логика осиротевших) или вызывающий.

> Ограничения. Ноги без client_id (Bybit trading-stop, BingX, Gate, MEXC, HTX) опознаются по виду + стороне + цене;
> нога, созданная раньше входа этого bracket, его ногой не считается. Если такие же ноги (ваш другой bracket с теми
> же ценами) поставлены позже входа или биржа не отдаёт время создания ноги, bracket, умерший до ног, примет их за
> свои (`present`).

Схема хранилища — версия 5 (2: колонка `trigger_by`; 3: колонка `algo_id` и таблица `algos`; 4: `algos.plan`; 5:
`intents.plan_retired` и отпечаток ключа в пространстве имён): база версий 1–4 мигрирует на месте при открытии
(`ALTER TABLE`, старые строки читаются; пространство имён строк не переименовывается — `claim_legacy`); база новее
библиотеки — `ConfigError` (старый SDK на базе новой схемы не стартует — так задумано).

**Откат схемы** (откат версии SDK) — одно из двух: (1) старый процесс стартует с новой пустой базой (`recover()` по
прежним намерениям не нужен); (2) остановить процесс (блокировка `<db>.lock`: пока он работает, инструмент откажет) и
выполнить `python tools/intents_downgrade.py bot.sqlite --to 3` (bytx 0.1.5 и раньше): отпечатки ключа снимаются с
пространства имён, `meta.schema` — целевая; конфликт client_id между ключами — отказ без изменений (`--force` —
оставить такие строки с отпечатком, старая версия их не увидит). Версия 3 не читает `algos.plan` (защиты по плану у
неё нет). При обновлении обратно база мигрирует заново.
Ордера с тегом бота, но без намерения — `untracked` (роль из журнала), только при точном совпадении тега. Закрытые
намерения старше `prune_after` удаляются. Тестовая матрица по точкам сбоя (до/после намерения, в полёте, до
исхода, до журнала, между входом и TP/SL; `open_position` на 6 биржах: до/после плана, вход в полёте,
частичное исполнение с прикреплённым SL и без, до стопа, внутри `recover()`) — покрыты тестами: ни
дублей, ни потерь, ни позиции без стопа. Demo 2026-11-04 с настоящим `kill -9` процесса (Bybit, Binance, OKX,
BingX — BTC и ETH): запрос не ушёл — `abandoned` или пересоздание тем же client_id (один ордер); ответ потерян —
adopted без дубля; исполнен или снят за время простоя — `finished`; убит между входом и стопом — SL и TP по плану;
`kill -9` внутри самого `recover()` — следующий доводит без дублей; ордер с тегом без намерения — `untracked`, без
тега — не тронут.

**Strategy** — жизненный цикл `on_start` / `on_tick` / `on_order` / `on_fill` / `on_position` / `on_timer` /
`on_stop`, подписки `symbols × feeds` (`ticker`, `order_book`, `best_bid_ask`, `trades`, `candles`) и
`ctx.subscribe`, таймеры `ctx.every` / `ctx.after` на часах стратегии (paper/replay — часы движка),
`ctx.ex`, `ctx.journal`, `ctx.algos`, `ctx.risk_size` / `ctx.open_position`, `ctx.bind(position_id=, role=)`.
Старт: `ex.recover()` (если задан `intents`; результат в `ctx.recovery`), трекеры, подписки, `on_start`.
События доставляются по одному (колбэки никогда не пересекаются); тики стакана/тикера схлопываются до последнего; ошибки
колбэков попадают в `result.errors` (`max_errors` — остановка); `on_stop` вызывается всегда.

После рестарта то, что осталось от прошлого процесса, читается в `on_start`: ордера намерений —
`ctx.recovery` (`live`, `finished` …), позиции и открытые ордера — снимки трекеров `ctx.positions` /
`ctx.orders`. `on_position` / `on_order` сообщают только об изменениях **после** старта: стратегия, которая
узнаёт о позиции лишь из `on_position`, после `kill -9` с открытой позицией войдёт второй раз (demo
2026-11-04 — так удваивал позицию пример 10 до исправления). `on_order` / `on_fill` / `on_position` — только
символов `Strategy.symbols` и подписок `ctx.subscribe` (трекеры — по всему аккаунту; стратегия без `symbols` получает
все; событие без символа доставляется — приписать его чужому нельзя; символ подписки остаётся своим и после
`handle.stop()`): закрытие позиции другого символа тем же аккаунтом (другой бот) раньше доходило до стратегии как её
закрытие — второй вход (demo Bitget 2026-11-04, BTC при параллельном ETH). В hedge сторону позиции проверяет сама
стратегия (пример 10 — только long). Лучшие bid/ask — поток `best_bid_ask`: в
WS-тикере Binance USDⓈ-M (`24hrTicker`) bid/ask нет, только `last`.

```python
class Mine(Strategy):
    symbols, feeds = ('BTC/USDT:USDT',), ('ticker',)

    async def on_start(self, ctx):
        ctx.every(30, self.check)
    async def on_tick(self, ctx, tick): ...

result = await run_strategy(Mine(), ex)                                 # live / demo / paper
result = await ex.replay.run(lambda ex: run_strategy(Mine(), ex))      # replay записи
```

Пример — [`examples/10_strategy_restart.py`](../examples/10_strategy_restart.py). Модули `bytx.recovery` и
`bytx.strategy` не импортируются, пока не используются: путь ордера без хранилища стоит одной проверки.

## Bitget и Gate (Gate — бета)

Обе биржи интегрированы так же, как шесть бирж v0.1: правила следуют официальной документации, у каждого
правила сверено с примерами ответов из документации, расхождения ccxt
4.4.65 исправлены в `exchanges/<exchange>/ccxt_fixes.py`, код биржи живёт в `exchanges/<exchange>/` (tp_sl, matching, fees,
limits, error_codes, journal_origin, history, status, api_key, portfolio, tiers, position_history).

```python
ex = create_exchange('bitget', api_key=…, secret=…, password=…, margin_mode='cross', demo=True)
ex = create_exchange('gate', api_key=…, secret=…, demo=True)            # USDT-M, single-режим

from bytx.exchanges.bitget.position_history import closed_positions   # /mix/position/history-position
rows = await closed_positions(ex, 'BTC/USDT:USDT', since=…)              # ClosedPosition: pnl, комиссии, фандинг
```

| | Bitget (classic API v2) | Gate (APIv4) |
|---|---|---|
| Размер | база; шаг — `sizeMultiplier` (ccxt брал минимум) | контракты × `quanto_multiplier` |
| Режим маржи | в каждом ордере (`marginMode`) — без него `ConfigError` | на позицию |
| TP/SL | `place-tpsl-order`: `pos_loss` / `pos_profit` (вся позиция), `loss_plan` / `profit_plan` с размером; `holdSide` buy/sell в one-way | `price_orders`: `close-<side>-position` / `plan-close-<side>-position`, по рынку (цена 0 + ioc) |
| SL на входе | `presetStopLossPrice` в ордере | отдельно сразу после входа (с откатом) |
| Происхождение ордера | `enterPointSource` WEB/IOS/ANDROID, `tradeSide` burst_* / ADL, `orderSource` | `text`: web/app, liq-*, auto_deleveraging; `is_liq` |
| Ошибки | коды (`40010` «Request timed out» — исход неизвестен) | метки `{"label": …}` (`SERVER_ERROR`, `TOO_BUSY` — неизвестен) |
| Проверка ключа | `authorities` (`wwow` — вывод), `ips` | `account/detail`: IP и UID; права ключа API не возвращает |
| Портфель | `mix/account/bill` (окна ≤ 30 дн., 90 дн.) | `futures/usdt/account_book` |

Исправлено относительно ccxt 4.4.65: Bitget — демо без `S`-символов и с `Content-Type`, шаг размера,
регистр `force` / `tradeSide`, `holdSide` у TP/SL, WS-стакан по `seq`/`pseq` (в документации нет checksum — ccxt
падал с ChecksumError), сделки на демо; Gate — хосты TestNet (ccxt получал 502), рынки TestNet, ставки комиссий
контрактов (ccxt делил на 100), префикс `t-` у `text`. Эмуляторы: `fake_exchange('bitget' |
'gate')`. Живая проверка публичных функций — `tools/live_public_check.py --exchanges bitget,gate`
(продовые и демо-окружения, 160 проверок — 0 проблем, 2026-10-26).

## MEXC и HTX (бета)

Интегрированы так же, как Bitget и Gate: правила следуют официальной документации,
расхождения ccxt 4.4.65 исправлены в `exchanges/{mexc,htx}/ccxt_fixes.py`, код биржи живёт в `exchanges/<exchange>/`.
Обе биржи отправляют плечо с каждым ордером: `set_leverage` / `set_margin_mode` запоминают его (без запроса к
бирже); если не задано, MEXC берёт его из `position/leverage`, HTX — из `swap_cross_account_info` (плечо ордера
HTX должно совпадать с плечом открытой позиции). Демо нет — тестируйте с `paper=True` или
`fake_exchange('mexc' | 'htx')`.

```python
ex = create_exchange('mexc', api_key=…, secret=…, margin_mode='cross')   # USDT-M; режим отправляется с каждым ордером
ex = create_exchange('htx', api_key=…, secret=…, margin_mode='isolated') # cross / isolated — разные эндпоинты

from bytx.exchanges.mexc.position_history import closed_positions    # history_positions (state 3)
```

| | MEXC (Contract API) | HTX (USDT-M linear swap) |
|---|---|---|
| Размер | контракты × `contractSize`, шаг `volUnit` | контракты × `contract_size`, минимум 1 (ccxt — `contract_size`) |
| Ордер | `order/create`: `side` 1–4, `type` 1–5, `openType`, `positionMode` явно | `swap_cross_order` / `swap_order`, `lever_rate` в каждом |
| TP/SL | `stoporder/place` по `positionId` (`lossTrend` 1/2/3 — last/mark/index) | `swap_cross_tpsl_order`, только last |
| SL на входе | `stopLossPrice` в ордере | `sl_trigger_price` в открывающем ордере |
| client_id | `externalOid` ≤ 32 (код 2030) | своп — **целое** 1…2⁶³−1, спот — ≤ 64 |
| Происхождение | `category` 2/3 — ликвидация, 4 — ADL | `order_source` (web/app — UI), `order_type` 3 / 22, `liquidation_type` |
| Проверка ключа | права ключа не возвращает (только чтение активов) | `/v2/user/api-key`: `permission`, `ipAddresses`, UID |
| Портфель | `account/assets`, `funding_records`, `order_deals` (нужен список символов) | `swap_financial_record` окнами по 48 ч, 90 дн. |
| Статус | — (проба `contract/ping`) | `heartbeat/` (`linear_swap_heartbeat`) |
| Уровни / закрытые позиции | из записи контракта / `history_positions` | `NotSupported` / `NotSupported` |

HTX: client_order_id контракта — целое число, поэтому на свопе `client_id_prefix` — от 1 до 8 цифр, а client_id
нового поколения (тег + роль) не помещается: с журналом SDK использует числовой id, а журнал хранит роль ордера
(событие `intent`). Тег кодируется в 18-значном id как `9`, длина тега и сам тег
(тег `7` даёт `917…`, тег `71` — `9271…`). Id без тега начинаются с 1–8. Поэтому `owns()` и
`cancel_all` бота с тегом `7` не считают своими ни ордера бота `71`, ни ордера инстанса без тега.

Исправлено относительно ccxt 4.4.65: MEXC — фьючерсные ордера по новой документации («2026-03-31 Futures Trading API
maintenance ended»): путь `order/create` вместо `order/submit`, id из `data.orderId`, market — `type 5`,
закрытие в hedge отправляет `side` 4/2 (ccxt открывал противоположную позицию), снят запрет ccxt на
BTC/ETH/LTC (`unavailableContracts`), контракт с `apiAllowed=false` — `NotSupported` до отправки, отмена
2040/2041 — `OrderNotFound`, WS `push.deal` с массивом сделок; HTX — удалены `channel_code` из ccxt и
префикс `AA03022abc`, `reduce_only` не отправляется в hedge-режиме, TP/SL на входе, реальное плечо вместо `lever_rate=1`,
нецелый client_id больше не отбрасывается молча. Живая проверка публичных функций —
`tools/live_public_check.py --exchanges mexc,htx` (80 проверок — 0 проблем, 2026-10-26).

Фьючерсы MEXC через API зависят от политики биржи: размещение ордеров было закрыто с 2022-07-25
по 2026-03-31 (futures API Update log). Отказы «торговля недоступна» (6001, 6002, 6005, 511, 1002) отображаются в
`NotSupported`, «у ключа нет прав» (401, 402, 406, 701–704) — в `PermissionDenied`; ни те, ни другие не повторяются.
На MEXC `check_key()` определяет право торговли пробой — отменой заведомо несуществующего ордера
(2040 — разрешено, 704 — нет); право вывода и список IP MEXC не возвращает.

## BloFin (бета)

Бессрочные фьючерсы USDT-M: `create_exchange('blofin', api_key=…, secret=…, password=…)`; `demo=True` — demo-хост
BloFin. Правила — по официальной документации, расхождения ccxt 4.4.65 исправлены в `exchanges/blofin/ccxt_fixes.py`,
код биржи — в `exchanges/blofin/`. Спот — `market='spot'` (Spot API, см. ниже).

```python
ex = create_exchange('blofin', api_key=…, secret=…, password=…, margin_mode='cross')  # режим уходит в каждом ордере
```

- Режим маржи уходит в каждом ордере (`margin_mode` обязателен); режим позиций (one-way / hedge) читается с аккаунта.
  Multi-Position (каждый вход — отдельная позиция с `positionId`) не поддерживается: `ConfigError` до торговли —
  выключите его в настройках BloFin.
- Объём — в контрактах (`contractValue`, шаг `lotSize`, минимум `minSize`) и проверяется до отправки: биржа отвечает
  кодом 152002 и на неверный объём, и на плечо выше `maxLeverage` инструмента.
- Ордера: post_only / IOC / FOK через `orderType`, изменение на месте (`amend-order`), ордер по id или client_id
  (`order-detail`), пакеты до 20 ордеров одного символа. TP/SL: TP и SL одной заявкой `order-tpsl` с явным типом цены,
  перенос на месте (`amend-tpsl`); список, деталь и история.
- WebSocket: стакан с проверкой `prevSeqId` (разрыв — переподписка), TP/SL через канал `orders-algo`, свои сделки из
  `fills-history` при росте исполнения ордера.
- Также: тиры плеча (`position-tiers`), закрытые позиции, фандинг и PnL для портфеля, проверка ключа
  (`query-apikey`), время сервера по заголовку `Date`.

Исправлено относительно ccxt 4.4.65: `settle` рынка из `settleCurrency` (рынки SDK — только USDT-M), терялись
post_only / IOC / FOK, уникальный `ACCESS-NONCE` (ccxt повторял timestamp — параллельные запросы отвергались), ошибки
в `data`-объекте при `code` 0, коды 150004 / 103003 (средства), 102068 / 102067 / 1000 (ордера нет), 429 (лимит),
плечо до `maxLeverage` инструмента, `batch-leverage-info` вместо устаревшего `leverage-info`.

**Спот** (`create_exchange('blofin', market='spot', …)`, в том числе demo): в ccxt 4.4.65 спота BloFin нет — SDK
шлёт запросы в Spot API (`/api/v1/spot/…`, `instType=SPOT`). `buy(notional=…)` — market-покупка на сумму в quote
(`targetCurrency=quote_currency`), прочие market-ордера — в базе; ограничения размера (`minSize` / `lotSize`, в базе)
проверяются до отправки; валюта комиссии ордера и сделки — база или quote рынка. Пакеты и отмена по id / client_id —
как на фьючерсах; amend, TP/SL (в ордере и отдельно), трейлинг, плечо и позиции — `NotSupported` до отправки.
WebSocket: стакан (проверка `prevSeqId`), ордера и баланс (`spot-account`).

## Прокси и несколько аккаунтов

```python
ex = create_exchange('binance', api_key=…, secret=…, proxy='socks5://user:pass@host:1080')
```

Один прокси и для REST, и для WS (`pip install bytx[ws]` для SOCKS через WS). Rate limit
общий на ключ «биржа + прокси», поэтому аккаунты за одним IP не получают бан.

```python
from bytx import AccountPool

async with AccountPool.from_file('accounts.yaml') as pool:   # ключи через ${ENV}
    print(pool.shared_ip_warnings())
    balances = await pool.gather(lambda ex: ex.balance())    # ошибка одного аккаунта не влияет на остальные
```

## Портфель, проверка ключа, здоровье биржи

```python
async with AccountPool.from_file('accounts.yaml') as pool:
    report = await pool.portfolio(since=week_ago_ms)          # until по умолчанию — сейчас
    print(report.total_equity, report.realized_pnl, report.fees, report.funding)
    report.to_csv('portfolio.csv')                            # одна строка на аккаунт и валюту
    if not report.complete:
        print(report.issues())                                # почему данные неполные

    keys = await pool.check_keys()       # {name: KeyInfo | UnsafeApiKey | error}
    health = await pool.health()         # {name: HealthReport | error}
```

- **Портфель** (`ex.account_report(since=, until=)`, `pool.portfolio(...)`): equity аккаунта, нереализованный
  PnL позиций, реализованный PnL, комиссии и фандинг за период — из журнала операций каждой биржи по её
  документации (Binance income, Bybit transaction-log, OKX bills, BingX income, KuCoin ledger + фандинг по символам,
  Hyperliquid fills + funding). Знаки одинаковы на всех биржах: `fees` — уплачено (+), `funding` —
  получено (+) / уплачено (−), `net_pnl = realized − fees + funding`. Если история обрезана (лимит
  страниц, окно глубже истории биржи), биржа не возвращает часть данных (KuCoin — комиссии аккаунта:
  `fees=None`) или аккаунт не удалось прочитать — `complete=False` с причиной в `issues`: частичная сумма никогда
  не выдаётся за полную. Итоги по аккаунту — в USDT/USDC/USD (1:1; VST демо BingX — тоже); другие валюты — в
  `totals`. Символ записи журнала (`keep_items=True` → `report.items`) — символ SDK (`'BTC/USDT:USDT'`), а не id
  рынка биржи; `account_report(symbols=['BTC/USDT:USDT'])` — отчёт по этим символам (журнал, позиции,
  нереализованный PnL), equity — всего аккаунта. Сверено на демо Bybit / Binance / OKX / BingX (2026-11-04):
  equity = ответ биржи, реализованный PnL и комиссии своих сделок = журнал операций, фандинг = журнал биржи
  и −размер × mark × ставка.
- **Фандинг одного символа** (`ex.funding_paid(symbol, since=, until=)`): записи фандинга за окно из того же журнала,
  `total` (+ получено / − уплачено), `complete` / `issues` — как в отчёте.
- **Проверка ключа** (`ex.check_key()`, `ex.account_uid()`): права на торговлю и вывод, переводы, привязка
  к IP, тип аккаунта (Bybit unified/classic, OKX acctLv, Binance multi-assets, HL abstraction), режим
  позиций, UID — из эндпоинта прав ключа биржи по её документации. Ключ с правом вывода по умолчанию отклоняется
  (`UnsafeApiKey`); `withdraw='warn'` — только предупреждение. Всё, что биржа не
  сообщила, — `None` с причиной в `info.unknown` («не проверено» ≠ «нет права»). Если право вывода
  не определено (`can_withdraw=None`: API MEXC не возвращает права ключа, sapi Binance недоступен), то даже
  с `withdraw='reject'` вы получите только предупреждение в `info.warnings`, а не `UnsafeApiKey`. Сервис,
  которому нужна строгая проверка, должен сам отклонять ключ при `info.can_withdraw is None`.
  Hyperliquid: ключ кошелька
  может выводить, API-кошелёк (agent) — нет.
- **Здоровье** (`ex.health()`): статус обслуживания из публичного эндпоинта биржи (Binance, Bybit, OKX,
  KuCoin; у BingX и Hyperliquid документированного статуса нет — `unknown`), задержка REST и расхождение
  часов по запросу серверного времени, метрики REST и WS-потоков, состояние rate limiter. Без ключей — только
  публичное; с ключами — ещё и приватная проба (баланс). `report.healthy` означает, что торговать можно. Задержка
  WS «биржа → мы» считается по смещению часов середины запроса времени (`TimeSync.clock_offset_ms`), а не по
  ccxt `timeDifference` (он — для подписи и завышен на путь ответа: задержка выходила ≈ 0 и меньше нуля).
  Состояние лимитера — на момент вызова (`tokens`, `busy_s` — сколько ждать следующему запросу). OKX: статус
  запрашивается не чаще раза в 5 с на IP (повтор — из кэша) и не оставляет долга в общем лимитере IP (раньше
  health() задерживал следующий запрос с этого IP на ~5 с). Смещение часов и задержка — по одной пробе: на
  холодном соединении (первый запрос нового клиента) оба завышены — смотрите `rest_latency_ms`.

## Данные, тестирование, синхронный клиент

```python
h = await ex.history.candles('BTC/USDT:USDT', '1h', since=t0, until=t1)   # [since, until), мс
df = h.to_pandas()                        # или h.to_polars(); любые модели — bytx.data.to_pandas(rows)
if not h.complete:
    print(h.gaps, h.unavailable)          # дыры в сетке таймфрейма; что биржа не отдаёт и почему
funding = await ex.history.funding('BTC/USDT:USDT', since=t0)
trades = await ex.history.trades('BTC/USDT:USDT', since=t1 - 3_600_000)   # Binance, OKX
```

- **Загрузчик истории** (`ex.history`, `bytx.data.HistoryLoader(ex, cache_dir=, fmt=)`): свечи,
  публичные сделки, фандинг с кэшем на диске (`~/.cache/bytx/history`, `$BYTX_CACHE_DIR`;
  parquet, если установлен pyarrow, иначе csv). Повторные запросы обслуживаются из кэша, скачиваются только
  дыры (`refresh=True` скачивает заново). Кэшируются только финальные данные: закрытые свечи,
  сделки старше минуты. Демо и live хранятся раздельно. Размер страницы и глубина истории берутся из документации
  каждой биржи (`exchanges/<exchange>/history.py`): Binance 1500 свечей (спот 1000), Bybit 1000, OKX 300,
  BingX 1440, KuCoin 500 (спот 1500), Hyperliquid 500 и только последние 5000 свечей. История сделок
  есть только на Binance (aggTrades, 48 часов) и OKX (3 месяца); на Bybit, BingX, KuCoin и Hyperliquid
  REST отдаёт только недавние сделки, отсюда `NotSupported` со ссылкой на документацию. Всё, что глубже
  истории биржи, попадает в `unavailable`, а не в молча обрезанный ряд.
- **pandas / polars** — extras: `pip install 'bytx[pandas]'`, `[polars]`, `[parquet]`, `[data]`
  (pandas + pyarrow). Ядро SDK работает без них.
- **`bytx.testing`** — эмуляторы для тестирования ваших стратегий, без сети и ключей:
  `fake_exchange('bybit', price=50_000)` возвращает обычный `Exchange` для любой из 11 бирж на снимках рынков из
  документации (шаг, минимумы и contractSize как на бирже, путь TP/SL SDK) плюс эмулятор `FakeVenue`.
  `paper_exchange('okx', bids=..., asks=...)` возвращает paper-аккаунт на виртуальных часах; рынок задаётся через
  `set_book` / `add_trade`, время двигается через `drive`. Плагин pytest включается через
  `pytest_plugins = ['bytx.testing.pytest_plugin']` (фикстуры `fake_exchange`, `paper_exchange`,
  `bytx_clock`). Неэмулируемый запрос выбрасывает `NetworkDisabled` вместо обращения к бирже.
- **`bytx.sync`** — блокирующий клиент для скриптов и Jupyter:
  `with bytx.sync.create_exchange('okx', demo=True, ...) as ex: ex.buy(...)`. Клиент крутит свой event loop
  в отдельном потоке, поэтому работает и внутри уже запущенного цикла. WS-поток читается как обычный
  итератор (`for book in ex.stream.order_book(s)`). Вызов из собственного колбэка клиента сразу выбрасывает
  `RuntimeError` вместо дедлока. `close()` / `with` закрывают Exchange и останавливают поток.
  `call_timeout` отменяет вызов; ордер при этом мог дойти до биржи, поэтому сверяйтесь по `client_id`.

## Интеграции: TradingView, Telegram/Discord, Prometheus, OpenTelemetry

Все интеграции по умолчанию выключены, и ядро их не импортирует. Уведомления и webhook-сервер на aiohttp
работают на aiohttp ядра. Всему остальному нужны extras: `[prometheus]`, `[otel]`, `[fastapi]` или все сразу
через `[integrations]`.

```python
ex = create_exchange('bybit', demo=True, api_key=..., secret=..., client_id_prefix='tv1')

# События биржи: основа всех интеграций
unsubscribe = ex.subscribe(print, kinds=['order', 'protection', 'stream'])

# Webhook TradingView: секрет обязателен; без allow_live=True принимает только demo/paper
hook = TradingViewWebhook(ex, secret=os.environ['BYTX_TV_SECRET'])
runner = await hook.serve(port=8080)       # снаружи 443 (nginx/Caddy); FastAPI: hook.fastapi_router()

# уведомления: отказы, неизвестные исходы, не поставленные TP/SL, обрывы WS, ошибки webhook, вмешательства
notifier = Notifier(TelegramChannel.from_env(), DiscordChannel.from_env())
notifier.watch(ex)
notifier.watch_journal(journal)

PrometheusCollector(ex)                    # + prometheus_client.start_http_server(9108)
instrument(ex)                             # спаны OpenTelemetry
```

- **События** (`ex.subscribe(callback, kinds=)` → функция отписки): `order.placed`, `order.rejected`,
  `order.unknown`, `order.canceled`, `tp_sl.placed`, `protection.failed`, `stream.state`, `error`.
  `kinds` принимает типы или группы (`'order'`, `'stream'`); опечатка в типе выбрасывает `ConfigError`. Колбэк
  может быть синхронным или асинхронным. Его исключение логируется и не ломает торговлю. Событие несёт запрос
  без `params`, то есть без подписей и брокерских полей. Без слушателей цена — одна проверка пустого
  списка. `journal.subscribe(cb)` доставляет каждое новое событие журнала позиций, включая
  вмешательства. `AccountPool` выставляет `ex.account` (имя аккаунта).
- **Webhook TradingView** (`bytx.integrations.tradingview`). Поместите JSON в поле Message алерта:
  `{"secret": "…", "id": "{{strategy.order.id}}-{{timenow}}", "time": "{{timenow}}", "action": "open",
  "symbol": "{{ticker}}", "side": "long", "risk_usdt": 50, "stop": 61000}`. Действия:
  - `open` открывает позицию через `open_position` (по `risk_usdt` и `stop`) или обычным ордером (по
    `size` / `notional`);
  - `close` закрывает позицию через `close_position`;
  - `buy` / `sell` соответствуют `{{strategy.order.action}}`; при `market_position: "flat"` позиция
    закрывается;
  - `market` / `chase` / `twap` / `iceberg` / `bracket` запускают алгоритмы `ex.algos`;
  - `cancel_all` отменяет только ваши собственные ордера, поэтому `client_id_prefix` обязателен.

  Неизвестные поля отклоняются. Правила следуют документации TradingView. Алерт, сообщение которого не является
  валидным JSON, приходит как `text/plain` и отклоняется. TradingView отменяет запросы дольше 3 с, поэтому webhook
  сразу отвечает `202` и исполняет в фоне (`wait=True` ждёт результата). Только порты 80 и 443.
  IP отправителей — в `TRADINGVIEW_IPS` (`allowed_ips=`). Защита:
  - секрет обязателен, не короче 16 символов (`BYTX_TV_SECRET`), сравнивается за константное время и никогда
    не появляется в ответах и логах;
  - live-аккаунт без `allow_live=True` выбрасывает `ConfigError`;
  - повторный `id` не исполняется дважды: дубль возвращает предыдущий результат. `SqliteAlertStore(path)`
    хранит id между рестартами;
  - rate limit (`max_per_minute`, `burst`) — два бюджета. Неудачные запросы (неверный секрет, не JSON,
    слишком большое тело) тратят бюджет адреса отправителя, проверяемый до секрета, поэтому
    перебор секрета упирается в 429. Алерты с правильным секретом считаются по общему бюджету, поэтому флуд без
    секрета с другого адреса не блокирует настоящий алерт (включая `close`). За reverse proxy нужен
    `trust_forwarded=True` (иначе у каждого отправителя адрес прокси) или `allowed_ips=TRADINGVIEW_IPS`;
  - поддерживаются `allowed_symbols` и `allowed_actions`. Алерт, чей `time` старше `max_age` (по умолчанию
    120 с), отклоняется.

  Ошибка исполнения приходит как событие `error` с `source='tradingview'`, и Notifier её доставляет.
- **Уведомления** (`bytx.integrations.notify`).
  - Каналы. `TelegramChannel`: `sendMessage`, до 4096 символов, при 429 ждёт `retry_after`, не больше
    1 сообщения в секунду на чат (по Telegram FAQ). `DiscordChannel`: webhook канала, до 2000 символов, без
    упоминаний, при 429 ждёт `retry_after` / `Retry-After`, учитывает `X-RateLimit-Remaining` /
    `Reset-After`.
  - Что отправляется. `watch(ex)` по умолчанию отправляет только проблемы. Каждый ордер отправляется с
    `kinds=['order', …]`. `watch_journal` отправляет исполнения, закрытия, ликвидации, вмешательства и
    расхождения.
  - Троттлинг: одинаковое событие (тип, биржа, аккаунт, символ) отправляется не чаще раза в `throttle` секунд
    (по умолчанию 60). Повторы приходят одной сводкой «ещё N».
  - Секреты. Текст очищается перед отправкой: ключи наблюдаемых бирж, токены каналов, ваши собственные
    значения `redact=` и шаблоны (токен бота, URL webhook, `signature=` / `apiKey=`, приватный
    ключ). Лог ошибок отправки очищается так же.
  - Слушатель только кладёт текст в очередь канала, поэтому торговля никогда не ждёт сеть.
- **Prometheus** (`PrometheusCollector(*exchanges, pool=)`): значения читаются в момент scrape из
  аккумуляторов SDK: REST-запросы, ошибки и повторы, задержка по эндпоинтам (p50/p90/p99/max), заголовки rate limit
  биржи, смещение часов, общий лимитер (токены, очередь, блокировка после 429/418), WS (сообщения,
  переподключения, ошибки, тишина, обрывы, подключён или нет, задержка), задержка ордеров `ws_*` и
  счётчик событий ордеров. Метки: `exchange`, `account`, `mode` (live/demo/paper). Scrape не создаёт
  WS-хаб и не трогает горячий путь. Свой `/metrics` отдаётся через `metrics_text()`.
- **OpenTelemetry** (`instrument(ex)` → `undo`): спаны `bytx.order.place`, `bytx.order.cancel`,
  `bytx.ws.order`, `bytx.rest` (вложен в спан ордера; `url.path` без query) и
  `bytx.ws.reconnect` (от обрыва до `connected`). Обёртки ставятся на инстанс, и без `instrument`
  в коде нет вообще никаких проверок трассировки. Текст ошибок проходит через `redact`.

## Конкурентность и доступ к голому ccxt

- Методы `Exchange` не изменяют `exchange.options`. Режимы (createOrder, тип рынка и т. п.) передаются как
  параметры запроса, поэтому параллельные вызовы на одном клиенте не мешают друг другу. Это
  закреплено тестами. Исключение — собственные ленивые кэши данных
  ccxt: Binance `options['leverageBrackets']`.
- Один инстанс ccxt на `Exchange` (`ex.ccxt`, с патчами SDK). В `AccountPool` аккаунты делят только рынки (по ссылке из
  кэша). Опции, сессии и ключи — свои у каждого аккаунта.
- Не сериализуйте вызовы `Exchange` собственной реентерабельной блокировкой: `place_orders`, `cancel_all` и
  `close_all` внутри используют `asyncio.gather`, дочерние задачи блокировкой не владеют — дедлок.
- `ex.name` — стабильное имя в SDK: binance, bybit, okx, bingx, hyperliquid, kucoin, bitget, gate,
  mexc, htx, blofin.
- `ex.market(symbol)` отдаёт `settle` и `raw` (словарь рынка ccxt); `ex.candles(...)` и
  `ex.leverage_tiers(symbol)` работают без ключей. `ex.close()` идемпотентен и останавливает
  WS-потоки и фоновую синхронизацию времени. Не вызывайте `ex.ccxt.close()` напрямую: фоновые задачи останутся
  висеть.

## Примеры

[`examples/`](../examples): основы торговли ([`00_trading_basics.py`](../examples/00_trading_basics.py) — аутентификация, спот и фьючерсы, лимитные ордера: размещение / изменение / отмена, сокращение позиции), рыночные данные и стакан, бэктест, сканер арбитража, сетка, DCA, алерты в Telegram,
каркас скальпера, мультиаккаунт, стратегия с восстановлением после рестарта. Торговые
примеры по умолчанию работают на демо. Не хотите писать код? Те же боты и бэктесты запускаются на [byttrade.com](https://byttrade.com).

## client_id

SDK никогда не подменяет client_id вызывающего: неверный для биржи формат выбрасывает `InvalidOrder` до отправки.
Без собственного id SDK генерирует его сам (с тегом `client_id_prefix`, если задан).

- `bytx.max_client_id_len(ex)` — максимальная длина id (Binance — 36 минус брокерский префикс
  `x-<ID>`, который добавляет транспорт). Держите id в пределах этой длины;
  `create_exchange(..., client_id_strict=True)` превращает более длинный id в `InvalidOrder`.
- `bytx.generate_client_id(ex, prefix='d0123…')` — id в формате и длине биржи.
- `bytx.owns(order, prefix, exchange)` — ваш ли ордер (Hyperliquid и BingX — без учёта регистра).
  `Order.client_id` — логический: брокерский префикс Binance отрезается.
- `bytx.numeric_client_id_prefix(tag)` — числовая метка (до 8 цифр) для бирж с целым client_id (HTX USDT-M),
  детерминированно из любой метки.
- `ex.client_id_lookup_exact` — `True`, если биржа ищет ордер по client_id в любом статусе (Binance, Bybit, OKX,
  KuCoin, Bitget, Hyperliquid, MEXC, BloFin swap): тогда «не найден» от `order` / `find_order` значит, что ордера нет.
  `False` (Gate, BingX, HTX, спот) — «не найден» не доказательство.
- `set_tp_sl(..., take_profit_client_id=, stop_loss_client_id=)` — отдельный id на каждую ногу. На Bybit
  TP/SL ставятся на позицию без client_id — у ордеров `has_client_id=False` (то же на Gate:
  у price_orders нет пользовательского id; на BingX: «clientOrderId only supports LIMIT/MARKET order
  type» — TAKE_PROFIT_MARKET / STOP_MARKET отправляются без `clientOrderID`, нога определяется по id из ответа,
  журнал — по `tp_sl_set`).
- Gate: client_id — это поле `text`, «prefixed with `t-`, no longer than 28 bytes without `t-`»: id SDK
  всегда `t-…` (`generate_client_id`, client_id нового поколения, `owns` игнорирует `t-`); id без
  префикса — `InvalidOrder`. Bitget: `clientOid` до 50 символов (коды 40304/40305). MEXC: `externalOid` до 32
  (код 2030). HTX своп: `client_order_id` — целое 1…9223372036854775807 (код 1065), спот — до 64 символов.
  BloFin: `clientOrderId` — `[A-Za-z0-9_]{1,32}` (`_` принимается — demo 2026-11-09).
- `create_exchange(..., pre_submit=hook)` — `hook(request)` (может быть async) выполняется перед каждым запросом
  на размещение с финальным `client_id`: сохраните намерение до отправки. Исключение в хуке отменяет запрос.

## Журнал позиций

Биржи не знают о «позициях бота»: ордера входа, добора, TP/SL и закрытия живут сами по себе.
`bytx.journal` связывает их с позицией и хранит историю событий — только добавление, без
перезаписи. Журнал **включается явно**: без него Exchange и трекеры работают как раньше.

```python
from bytx.journal import PositionJournal

journal = PositionJournal(bot_id=bot_id)            # по умолчанию хранилище в памяти
ex = create_exchange('okx', ..., margin_mode='cross', journal=journal)

with journal.bind(position_id=cycle_id, role='entry'):
    await ex.buy('BTC/USDT', margin=50, leverage=5)
with journal.bind(position_id=cycle_id, role='tp', level_id=1):
    await ex.place_tp_sl('BTC/USDT', 'take_profit', 72_000, position_side='long', size=0.001)

async with ex.order_tracker('BTC/USDT'), ex.position_tracker('BTC/USDT'):
    ...                                              # исполнения и изменения позиции тоже попадают в журнал

for e in await journal.read(cycle_id):               # по возрастанию seq
    print(e.seq, e.event, e.role, e.origin, e.exchange_order_id)
record = await journal.summary(cycle_id)             # ордера с ролями и статусами, комиссии, фандинг
```

**Что записывается.** `intent` — до отправки (после хука `pre_submit`): связь client_id → позиция →
роль переживает падение процесса. Затем `placed` / `filled` / `rejected` (при неизвестном исходе
остаётся `intent`); `replaced` — `amend_order`, `modify_tp_sl` (поле `replaces`); `canceled` —
`cancel_order`, `cancel_orders`, `cancel_all`; TP/SL — с ролью `tp`/`sl` и уровнем.
`OrderTracker` — `placed` / `partially_filled` / `filled` / `canceled` / `expired`;
`PositionTracker` — `position_opened` / `increased` / `reduced` / `closed`. Фандинг и ликвидация —
`journal.record_funding(...)`, `journal.record_liquidation(...)`.

**Событие** (`JournalEvent`, алиас `JournalEntry`): `_v`, `seq`, `ts`, `bot_id`, `position_id`,
`event`, `origin`, `role`, `level_id`, `exchange`, `symbol`, `position_side`, `exchange_order_id`,
`client_order_id`, `trade_id`, `replaces`, `side`, `price`, `amount`, `filled`, `contracts_after`,
`fee`, `fee_currency`, `confidence`, `note`, `kind`, `order_type`, `trigger_price`, `reduce_only`,
`key`, `raw`. Размеры — в базовой монете.
Сериализация — `to_dict()` / `from_dict()` (неизвестные ключи пропускаются).

**Позиция** ордера: `bind(position_id=...)` (uuid цикла), иначе позиция, открытая в журнале для
`(exchange, symbol, side)`, иначе новый ключ `account:exchange:symbol:side:ts`.

**Происхождение** (`origin`) — детерминированное, `classify_origin` / `journal.classify_order`:
`own` — тег бота нового поколения, записанный `intent` или известный id ордера; `bot_legacy` —
старый формат cid бота (тег + hex; algo TP/SL `t|s…` — с `confidence='heuristic'`); `platform` —
форматы cid вашего приложения из `platform_formats=` (регулярные выражения);
далее маркеры API биржи по её документации (`exchanges/<exchange>/journal_origin.py`): ликвидация, ADL,
расчёт (settlement), интерфейс биржи (`exchange_ui`: Bybit `createType` «web/app», Hyperliquid `FrontendMarket`),
боты биржи, дочерний ордер TP/SL (происхождение родителя); чужой cid — `foreign_api` (на Binance —
`unknown`: биржа генерирует cid сама); без cid — `manual` (`heuristic`). `confidence`: `exact` —
маркер биржи или собственный cid, `heuristic` — выведено без прямого маркера.

**Чем закрыта позиция** — `await ex.classify_close(order_or_fill)` (или `bytx.journal.classify_close(exchange_id,
info=, client_id=, tag=)`): `liquidation`, `adl`, `settlement`, `bot` (ваша метка), `conditional` (сработавший TP/SL,
чей — не видно), `manual` или `unknown` — по маркерам самих бирж на 11 биржах (BingX — через эндпоинт
принудительных ордеров).

**Вмешательство** — событие `intervention` (`kind`) и колбэк `on_intervention=`:
`foreign_order` (не наш ордер в позиции: ручной добор/сокращение, стороннее ПО),
`liquidation` / `adl` / `settlement`, `external_cancel` / `external_amend` (наш ордер отменён /
изменён вне SDK — видно через WS; отмены и изменения через Exchange помечаются до запроса),
`position_drift` — `await journal.reconcile(position_id, size_on_exchange)`.
TP/SL позиции на Bybit (`/v5/position/trading-stop`) не принимают client_id: журнал записывает `tp_sl_set`
(роль, сторона, триггер) до запроса и считает своим условный ордер без `orderLinkId` (stopOrderType
`TakeProfit` / `StopLoss` / `PartialTakeProfit` / `PartialStopLoss`) с тем же символом, стороной и
триггером с точностью до шага цены (`confidence='heuristic'`); TP/SL с другой ценой — `foreign_order`.

```python
journal = PositionJournal(bot_id=bot_id, on_intervention=lambda e: alert(e.kind, e.origin, e.note))
rec = await journal.summary(cycle_id)
rec.orders_by_outcome()      # {'executed': [...], 'canceled': [...], 'open': [...], 'rejected': [...]}
rec.intervention_events      # вмешательства; OrderSummary.canceled_by — кто отменил
```

**Восстановление из истории биржи** — `await journal.recover(ex, 'BTC/USDT', 'long', since=ms)`:
эпизод позиции «от нуля до нуля» из исполнений, ордеров (исполненных / отменённых / открытых), фандинга,
ликвидаций; всё не наше — `intervention`; неполная история — `incomplete`. Идемпотентно, не
дублирует живой журнал. Источник — `HistorySource` (по умолчанию `ExchangeHistorySource`).

**Идемпотентность.** Ключ события — `(bot_id, exchange_order_id, event, trade_id)`; без
`trade_id` при частичном исполнении — накопленный размер. Повторы WS, REST-сверка трекера и
рестарты не дублируют события.

**Хранилище** — протокол `JournalStore` (async `append`, `read`, `find_order`, `positions`):
`append` идемпотентен по `key` и возвращает `AppendResult(event, created)`; `seq` назначает
хранилище (строго возрастает в пределах `(bot_id, position_id)`). Встроенное — `InMemoryJournalStore`.
Пример адаптера Mongo (коллекция `position_journal`): документ = `event.to_dict()`, индексы —
уникальный `key` и уникальный `(bot_id, position_id, seq)`; `seq` = последний + 1, при
DuplicateKey по `seq` — повтор, по `key` — вернуть существующий документ (`created=False`).
`strict=True` — сбой хранилища при записи `intent` отменяет отправку ордера (по умолчанию —
WARNING в логе, ордер уходит).

**client_id нового поколения** — `{tag}{role}{tail}`, ≤ 24 символов (с брокерским префиксом Binance
`x-<ID>` — до 36). Роль — не-hex символ: `n` вход, `g` dca, `p` tp, `q` sl
(+ уровень, 2 символа base36), `r` tp_reduce, `x` закрытие. С журналом SDK сам генерирует такой id,
когда роль известна (`bind(role=...)`, закрытие, TP/SL с `level_id`); id вызывающего никогда не меняется.
Старые id распознаются: тег + hex — `legacy` (роль из журнала), `t|s{level}{hex}` —
`legacy_algo`. Hyperliquid: cloid — 128-битный hex, роль не помещается и живёт только в журнале.

```python
from bytx.journal import bot_tag, make_client_id, parse_client_id

cid = make_client_id(ex, bot_tag(bot_id), 'tp', level_id=2)
parse_client_id(cid)    # ClientIdInfo(generation='v2', bot_tag='d…', role='tp', level_id=2)
```

## Качество

- Ответы и запросы сверены с **официальной документацией бирж**: примеры ответов из документации —
  эталонные фикстуры, у каждой ссылка на источник. Найденные так баги
  ccxt исправлены в SDK (`bytx.exchanges.<exchange>.ccxt_fixes`).
- каждый релиз проходит ~2800 офлайн-тестов, ruff и mypy; gitleaks в CI.
- ccxt закреплён на проверенной версии (`4.4.65`).

## Ошибки

```python
from bytx import errors

try:
    await ex.buy('BTC/USDT', size=0.0000001)
except errors.OrderTooSmall:
    ...
except errors.BytxError as e:
    if e.retryable:          # сеть, rate limit, биржа недоступна
        ...
```

Все ошибки SDK наследуются от `errors.BytxError`. У каждой есть машиночитаемые атрибуты:

- `e.code` — код биржи строкой (`'-2019'`, `'110072'`, OKX — `sCode` элемента), если есть;
- `e.error_code` — унифицированный `errors.ErrorCode`;
- `e.is_business` — содержательный отказ (средства, параметры, символ, ключ, нет позиции), а не
  инфраструктурный сбой: circuit breaker не должен на них срабатывать.

Текст ошибок никогда не содержит ключей. ccxt включает URL запроса в текст сетевой ошибки, а на HTX строка запроса
несёт `AccessKeyId` и `Signature`. Поэтому `errors.from_ccxt` пропускает текст через `errors.scrub`:
он удаляет точные значения ключей инстанса, query-строки URL с подписью или ключом (путь остаётся) и
секретные параметры (`signature=`, `apiKey:` …). Исходное исключение в `__cause__` тоже очищается. В
результате ключ не появляется ни в `str(e)`, ни в логах ядра, ни в `health().issues`,
ни в ответах и логах webhook TradingView.

Ошибки SDK также наследуются от одноимённых классов ccxt
(`errors.InsufficientFunds` ⊂ `ccxt.InsufficientFunds`, `errors.RateLimited` ⊂
`ccxt.RateLimitExceeded`, …), поэтому существующие `except ccxt.…` продолжают работать.
Исключения: `OrderStatusUnknown`, `BatchError`, `ProtectionNotPlaced`, `ConfigError` —
ни ccxt, ни `NetworkError`, поэтому широкий `except` не примет их за повторяемый сбой.

### Неизвестный исход ордера

`errors.OrderStatusUnknown` означает: запрос на размещение отправлен, а ответа не было или он был
неоднозначным (таймаут, -1006/-1007, «server timeout», дубль client_id), и сверка по client_id
не нашла ордер или не смогла проверить. **Ордер мог быть размещён.** Поля: `client_order_id`,
`exchange_order_id` (если известен), `symbol`, `side`, `amount`; `retryable=False`.

Как обрабатывать:

1. **Не повторяйте размещение** — ни с тем же, ни с новым client_id: это и есть двойной вход.
2. Проверьте позицию: `await ex.position(e.symbol)`.
3. Найдите ордер по client_id: `await ex.find_order(e.symbol, e.client_order_id, timeout=8)`: ордер — учтите его; `None` — не найден
   в пределах окна (не доказательство: движок может принять его позже); `RequestTimeout` — проверить не удалось,
   повторите сверку позже.
4. Решайте о новом ордере только после сверки и с тем же client_id: биржа отклонит дубль.

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)   # None — не найден в пределах окна
```

Бюджет времени: `with ex.deadline(15): ...` ограничивает весь вызов (запросы, повторы чтения,
сверку). `create_exchange(retries=0)` — без повторов чтения (для сервисов со своим
circuit breaker). `errors.RateLimited.retry_after` — пауза из заголовка `Retry-After`.

## Разработка

```bash
git clone https://github.com/byttrade/bytx && cd bytx
python -m venv .venv && .venv/bin/pip install -e ".[dev,ws,yaml]"
.venv/bin/ruff check src examples && .venv/bin/mypy
```

Живая проверка публичных функций (без ключей, нужна сеть): `python tools/live_public_check.py --exchanges okx,bybit`.

## Лицензия

bytx можно бесплатно использовать по лицензии [BYTtrade Source-Available License](../LICENSE): устанавливать и подключать в свои
скрипты, боты и приложения, в том числе в коммерческих целях. Копировать код в другие библиотеки, публиковать
изменённые версии и предоставлять его как облачный сервис нельзя.
Использует ccxt (MIT), см. [`NOTICE`](../NOTICE).
Не является финансовой рекомендацией; торговля криптовалютой сопряжена с риском потерь.
