# bytx — por [BYTtrade](https://byttrade.com)

[English](../README.md) | [Русский](README.ru.md) | [中文](README.zh-CN.md) | **Español** | [Português](README.pt-BR.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | [Türkçe](README.tr.md)

SDK de Python **async** y tipado para exchanges de criptomonedas: Binance, Bybit, OKX, BingX, Hyperliquid,
KuCoin, Bitget; en beta: Gate, MEXC, HTX. Una sola interfaz para spot y futuros perpetuos, WebSocket con
reconexión inteligente, un proxy por cuenta y herramientas para scalping.

> Documentación completa: [docs/GUIDE.md](GUIDE.md) (inglés)


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## Instalación

```bash
pip install bytx            # + extras: [ws] [fast]
```

## Inicio rápido

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, swap por defecto
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # size en BTC, no en contratos
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` usa el entorno demo/testnet del exchange; no se usa dinero real.

> Bots sin código, backtests y confirmación de operaciones en Telegram, en
> [byttrade.com](https://byttrade.com).

## Por qué bytx y no ccxt a secas

bytx está construido sobre [ccxt](https://github.com/ccxt/ccxt) y lo convierte en un SDK de trading con el que no tienes que
pelearte: la misma tarea ocupa **una línea en lugar de una página**, y los casos límite que en producción
cuestan dinero ya están resueltos por ti.

**Comprar BTC por 100 USDT en perpetuos de OKX**

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

**Take-profit y stop-loss sobre una posición**: en ccxt cada exchange es distinto (Bybit
`privatePostV5PositionTradingStop`, Binance dos órdenes condicionales con `closePosition`, OKX órdenes algo…).
En bytx es la misma llamada en todas partes:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**Un order book en vivo que sobrevive a las desconexiones**: con ccxt escribes tú el bucle de reconexión,
el backoff, la resuscripción, la detección de huecos y la resincronización con snapshot. En bytx:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**Timeouts al enviar órdenes**: con ccxt, un timeout te deja adivinando si la orden está viva (reintentar
significa entrar dos veces). bytx concilia por `client_id` y te dice exactamente qué pasó:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**Qué más obtienes de serie**

| ccxt a secas | bytx |
|---|---|
| `amount` en contratos, cada exchange con su propio `contractSize` | `size=` en la moneda base, `notional=` en USDT o `contracts=`; conversión y redondeo (hacia abajo) incluidos |
| `load_markets` en cada instancia, memoria × cuentas | una sola caché de mercados por proceso |
| el exchange rechaza la orden por step size o notional mínimo | se valida antes de enviar: `OrderTooSmall`, `InvalidOrder` |
| la mitad de las funciones son API implícita (`fapiPrivatePost…`) | métodos directos: TP/SL, dead-man switch, modo de posición, transferencias |
| respuestas en `dict` | modelos dataclass (`Order`, `Position`, `OrderBook`…), respuesta original en `.raw` |
| excepciones de ccxt sin más | una única jerarquía `BytxError` con el flag `.retryable` |
| el WS muere con una excepción y reconectar es cosa tuya | supervisor: backoff + jitter, watchdog de silencio, resuscripción, resincronización del order book, eventos de estado |
| las respuestas no coinciden con la documentación del exchange | respuestas verificadas contra la documentación oficial ([ground truth](GUIDE.md#quality)); los bugs de ccxt se corrigen en el SDK |

Además: modelos tipados en todas partes, una sola unidad de volumen en todos los exchanges, un proxy por
cuenta, paper trading sobre order books en vivo, un diario de posiciones, comprobación de permisos de la API
key y estado del exchange; todo con la misma interfaz en cada exchange.

## Características principales

- **Datos de mercado:** `ticker`, `order_book`, `trades`, `candles`, funding, open interest, niveles de apalancamiento; sin claves API, con caché de mercados por proceso.
- **Cuenta:** `balance`, `free_collateral`, `positions`, `open_orders`, `my_trades`, `transfer`; `Position.side` es `'long'`/`'short'` también en modo hedge en todos los exchanges.
- **Trading:** `buy` / `sell` / `place_order` (market/limit, `post_only`, `reduce_only`, TP/SL, `client_id`), órdenes en lote, modificación, cancelación y cierre de posiciones.
- **Algoritmos de ejecución** (`ex.algos`): `chase`, `twap`, `iceberg`, `bracket`, `trailing_stop`, `market` con límite de slippage.
- **Tamaño por riesgo:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` calcula el tamaño según la distancia al stop, el apalancamiento, los steps y las comisiones; coloca el stop de forma atómica o revierte.
- **Protección:** `set_tp_sl`, `dead_man_switch()` (el exchange cancela las órdenes si el proceso se cuelga), `kill_switch`, `RiskGuard` (límites de posición, pérdida diaria y frecuencia de órdenes).
- **Fiabilidad:** una orden nunca se reintenta automáticamente; ante timeout o resultado desconocido se busca por `client_id`; las operaciones en lote devuelven `BatchError` con el detalle en caso de fallo parcial.
- **Cancela y cierra solo lo tuyo:** `cancel_all` filtra por la etiqueta del bot (`client_id_prefix`) o por ids; cancelar todo el libro requiere `all_orders=True` explícito.
- **WebSocket:** reconexión automática, resincronización del order book, eventos de estado; órdenes por WS donde el exchange lo permite.
- **Paper trading y replay:** `paper=True`, order book real + cuenta virtual, sin claves.
- **Recuperación tras reinicio y estrategias:** journal de posiciones, restauración del estado por `client_id`.
- **Multicuenta y proxies:** un proxy por cuenta, caché de mercados compartida.
- **Integraciones:** webhooks de TradingView, alertas en Telegram/Discord, Prometheus, OpenTelemetry.

## Exchanges soportados

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Spot | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Futuros perpetuos | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| Demo / testnet | ✓ | ✓ | ✓ | futuros | ✓ | — | ✓ | ✓ | — | — |
| Modo hedge | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ |

Los detalles y limitaciones de cada exchange (órdenes por WS, particularidades de los entornos demo, etc.)
están en [docs/GUIDE.md](GUIDE.md).

## Ejemplos

[`examples/`](../examples): fundamentos de trading ([`00_trading_basics.py`](../examples/00_trading_basics.py): autenticación,
spot y futuros, órdenes limit: colocar / modificar / cancelar, reducir una posición), datos de mercado y order book,
backtest, escáner de arbitraje, grid, DCA, alertas en Telegram, esqueleto de scalper, multicuenta y una estrategia con
recuperación tras reinicio. Los ejemplos de trading
se ejecutan en demo por defecto. ¿Prefieres no programar? Los mismos bots y backtests funcionan en
[byttrade.com](https://byttrade.com).

## Licencia

bytx es de uso gratuito bajo la [BYTtrade Source-Available License](../LICENSE): puedes instalarlo y usarlo en tus propios scripts,
bots y aplicaciones, también con fines comerciales. No está permitido copiar su código en otras librerías,
publicar versiones modificadas ni ofrecerlo como servicio alojado.
Usa ccxt (MIT), ver [`NOTICE`](../NOTICE).
No es asesoramiento financiero; operar con criptomonedas conlleva riesgo de pérdidas.
