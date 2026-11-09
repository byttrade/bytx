# bytx — por [BYTtrade](https://byttrade.com)

[English](../README.md) | [Русский](README.ru.md) | [中文](README.zh-CN.md) | [Español](README.es.md) | **Português** | [日本語](README.ja.md) | [한국어](README.ko.md) | [Türkçe](README.tr.md)

SDK Python **async** e tipado para exchanges de criptomoedas: Binance, Bybit, OKX, BingX, Hyperliquid,
KuCoin, Bitget; em beta: Gate, MEXC, HTX, BloFin (futuros USDT-M). Uma única interface para spot e futuros perpétuos, WebSocket com
reconexão inteligente, um proxy por conta e ferramentas para scalping.

> Documentação completa: [docs/GUIDE.md](GUIDE.md) (inglês)


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## Instalação

```bash
pip install bytx            # + extras: [ws] [fast]
```

## Início rápido

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, swap por padrão
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # size em BTC, não em contratos
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` usa o ambiente demo/testnet da exchange; nenhum dinheiro real é usado.

> Bots sem código, backtests e confirmação de trades no Telegram, em
> [byttrade.com](https://byttrade.com).

## Por que bytx e não ccxt puro

O bytx é construído sobre o [ccxt](https://github.com/ccxt/ccxt) e o transforma em um SDK de trading com o qual você não precisa
brigar: a mesma tarefa cabe em **uma linha em vez de uma página**, e os casos extremos que custam dinheiro
em produção já são tratados para você.

**Comprar BTC por 100 USDT em perpétuos da OKX**

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

**Take-profit e stop-loss em uma posição** — no ccxt cada exchange é diferente (Bybit
`privatePostV5PositionTradingStop`, Binance duas ordens condicionais com `closePosition`, OKX ordens algo…).
No bytx é a mesma chamada em todas:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**Um order book ao vivo que sobrevive a desconexões** — com o ccxt, você mesmo escreve o loop de
reconexão, o backoff, a reinscrição, a detecção de lacunas e a ressincronização por snapshot. No bytx:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**Timeouts no envio de ordens** — com o ccxt, um timeout deixa você sem saber se a ordem está ativa
(tentar de novo significa entrar em dobro). O bytx concilia pelo `client_id` e diz exatamente o que aconteceu:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**O que mais você ganha de fábrica**

| ccxt puro | bytx |
|---|---|
| `amount` em contratos, cada exchange com seu próprio `contractSize` | `size=` na moeda base, `notional=` em USDT ou `contracts=`; conversão e arredondamento (para baixo) embutidos |
| `load_markets` em cada instância, memória × contas | um único cache de mercados por processo |
| a exchange rejeita a ordem por step size ou notional mínimo | validação antes do envio: `OrderTooSmall`, `InvalidOrder` |
| metade das funções é API implícita (`fapiPrivatePost…`) | métodos diretos: TP/SL, dead-man switch, modo de posição, transferências |
| respostas em `dict` | modelos dataclass (`Order`, `Position`, `OrderBook`…), resposta original em `.raw` |
| exceções do ccxt sem tratamento | uma única hierarquia `BytxError` com a flag `.retryable` |
| o WS cai com uma exceção e reconectar é problema seu | supervisor: backoff + jitter, watchdog de silêncio, reinscrição, ressincronização do order book, eventos de estado |
| respostas divergem da documentação da exchange | respostas verificadas contra a documentação oficial ([ground truth](GUIDE.md#quality)); bugs do ccxt corrigidos no SDK |

E mais: modelos tipados em tudo, uma única unidade de volume em todas as exchanges, um proxy por conta,
paper trading em order books ao vivo, um diário de posições, verificação de permissões da chave de API e
saúde da exchange — tudo com a mesma interface em cada exchange.

## Principais recursos

- **Dados de mercado:** `ticker`, `order_book`, `trades`, `candles`, funding, open interest, faixas de alavancagem; sem chaves de API, com cache de mercados por processo.
- **Conta:** `balance`, `free_collateral`, `positions`, `open_orders`, `my_trades`, `transfer`; `Position.side` é `'long'`/`'short'` também em modo hedge, em todas as exchanges.
- **Trading:** `buy` / `sell` / `place_order` (market/limit, `post_only`, `reduce_only`, TP/SL, `client_id`), ordens em lote, edição, cancelamento e fechamento de posições.
- **Algoritmos de execução** (`ex.algos`): `chase`, `twap`, `iceberg`, `bracket`, `trailing_stop`, `market` com limite de slippage.
- **Tamanho por risco:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` calcula o tamanho pela distância do stop, alavancagem, steps e taxas; coloca o stop de forma atômica ou faz rollback.
- **Proteção:** `set_tp_sl`, `dead_man_switch()` (a exchange cancela as ordens se o processo travar), `kill_switch`, `RiskGuard` (limites de posição, perda diária e frequência de ordens).
- **Confiabilidade:** uma ordem nunca é reenviada automaticamente; em timeout ou resultado desconhecido, a busca é feita por `client_id`; operações em lote retornam `BatchError` com detalhes em caso de falha parcial.
- **Cancele e feche só o que é seu:** `cancel_all` filtra pela tag do bot (`client_id_prefix`) ou por ids; cancelar o livro inteiro exige `all_orders=True` explícito.
- **WebSocket:** reconexão automática, ressincronização do order book, eventos de estado; ordens via WS onde a exchange suporta.
- **Paper trading e replay:** `paper=True`, order book real + conta virtual, sem chaves.
- **Recuperação após reinício e estratégias:** journal de posições, restauração do estado por `client_id`.
- **Múltiplas contas e proxies:** um proxy por conta, cache de mercados compartilhado.
- **Integrações:** webhooks do TradingView, alertas no Telegram/Discord, Prometheus, OpenTelemetry.

## Exchanges suportadas

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) | BloFin (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Spot | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| Futuros perpétuos | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| Demo / testnet | ✓ | ✓ | ✓ | futuros | ✓ | — | ✓ | ✓ | — | — | ✓ |
| Modo hedge | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ | ✓ |

Detalhes e limitações de cada exchange (ordens via WS, particularidades dos ambientes demo etc.) estão no
[docs/GUIDE.md](GUIDE.md).

## Exemplos

[`examples/`](../examples): fundamentos de trading ([`00_trading_basics.py`](../examples/00_trading_basics.py): autenticação,
spot e futuros, ordens limit: criar / editar / cancelar, reduzir uma posição), dados de mercado e order book,
backtest, scanner de arbitragem, grid, DCA, alertas no Telegram, esqueleto de scalper, múltiplas contas e uma
estratégia com recuperação após reinício.
Os exemplos de trading rodam em demo por padrão. Prefere não programar? Os mesmos bots e backtests rodam em
[byttrade.com](https://byttrade.com).

## Licença

O bytx é gratuito para uso sob a [BYTtrade Source-Available License](../LICENSE): você pode instalá-lo e usá-lo nos seus próprios
scripts, bots e aplicações, inclusive para fins comerciais. Não é permitido copiar o código para outras
bibliotecas, publicar versões modificadas nem oferecê-lo como serviço hospedado.
Usa ccxt (MIT), veja [`NOTICE`](../NOTICE).
Não é recomendação financeira; operar criptomoedas envolve risco de perda.
