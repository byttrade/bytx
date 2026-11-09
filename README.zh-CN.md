# bytx — 由 [BYTtrade](https://byttrade.com) 出品

[English](README.md) | [Русский](README.ru.md) | **中文** | [Español](README.es.md) | [Português](README.pt-BR.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | [Türkçe](README.tr.md)

面向加密货币交易所的强类型 **async** Python SDK：支持 Binance、Bybit、OKX、BingX、Hyperliquid、KuCoin、Bitget；
Gate、MEXC、HTX 处于 beta 阶段。现货和永续合约使用同一套接口，WebSocket 具备智能重连，
每个账户可单独配置代理，并提供面向剥头皮（scalping）交易的工具。

> 完整文档：[docs/GUIDE.md](docs/GUIDE.md)（英文）


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## 安装

```bash
pip install bytx            # 可选 extras：[ws] [fast]
```

## 快速开始

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT，默认为永续合约（swap）
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # size 以 BTC 计，而不是合约张数
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` 使用交易所的模拟盘 / testnet，不涉及真实资金。

> 无代码机器人、回测以及 Telegram 交易确认 —— 尽在
> [byttrade.com](https://byttrade.com)。

## 为什么选择 bytx 而不是直接用 ccxt

bytx 构建于 [ccxt](https://github.com/ccxt/ccxt) 之上，把它变成一个不用和它较劲的交易 SDK：同样的任务**一行代码搞定，而不是一整页**，
那些在生产环境中会让你赔钱的边界情况也都已经替你处理好了。

**在 OKX 永续合约上用 100 USDT 买入 BTC**

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

**为持仓设置止盈止损** —— 在 ccxt 中每个交易所都不一样（Bybit 用 `privatePostV5PositionTradingStop`，
Binance 是两张带 `closePosition` 的条件单，OKX 是策略委托（algo）单……）。在 bytx 中，所有交易所都是同一个调用：

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**断线也不怕的实时订单簿** —— 用 ccxt，重连循环、退避、重新订阅、缺口检测和快照重新同步都得自己写。用 bytx：

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**下单超时** —— 用 ccxt，超时后你只能猜订单到底有没有挂上（重试就可能重复开仓）。bytx 通过 `client_id`
对账，明确告诉你到底发生了什么：

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**开箱即用的更多功能**

| 原生 ccxt | bytx |
|---|---|
| `amount` 以合约张数计，每个交易所的 `contractSize` 各不相同 | `size=` 以基础币计，`notional=` 以 USDT 计，或 `contracts=`；内置换算与（向下）取整 |
| 每个实例都要 `load_markets`，内存占用 × 账户数 | 每个进程共享一份市场缓存 |
| 数量精度或最小名义价值不符时被交易所拒单 | 发送前校验：`OrderTooSmall`、`InvalidOrder` |
| 一半功能依赖隐式 API（`fapiPrivatePost…`） | 直接提供方法：TP/SL、dead-man switch、持仓模式、划转 |
| 返回 `dict` | dataclass 模型（`Order`、`Position`、`OrderBook`…），原始响应在 `.raw` 中 |
| 裸露的 ccxt 异常 | 统一的 `BytxError` 异常体系，带 `.retryable` 标志 |
| WS 抛异常后断开，重连得自己处理 | 监督器：退避 + 抖动、静默看门狗、自动重新订阅、订单簿重新同步、状态事件 |
| 响应与交易所文档不一致 | 响应已对照交易所官方文档校验（[ground truth](docs/GUIDE.md#quality)），ccxt 的 bug 在 SDK 中修复 |

此外：处处都是类型化模型，所有交易所统一的数量单位，每个账户独立代理，基于实时订单簿的模拟交易（paper trading），
持仓日志，API key 权限检查和交易所健康状态 —— 而且在每个交易所上都是同一套接口。

## 主要特性

- **行情数据：** `ticker`、`order_book`、`trades`、`candles`、资金费率、持仓量、杠杆档位 —— 无需 API key；市场信息在进程级缓存中。
- **账户：** `balance`、`free_collateral`、`positions`、`open_orders`、`my_trades`、`transfer`；对冲模式下 `Position.side` 在所有交易所统一为 `'long'`/`'short'`。
- **交易：** `buy` / `sell` / `place_order`（市价/限价、`post_only`、`reduce_only`、TP/SL、`client_id`）、批量下单、改单、撤单、平仓。
- **执行算法**（`ex.algos`）：`chase`、`twap`、`iceberg`、`bracket`、`trailing_stop`、带滑点上限的 `market`。
- **按风险计算仓位：** `open_position(symbol, 'long', risk_usdt=50, stop=...)` 根据止损距离、杠杆、精度和手续费计算数量，原子化地挂止损，失败则回滚。
- **保护机制：** `set_tp_sl`、`dead_man_switch()`（进程卡死时由交易所撤单）、`kill_switch`、`RiskGuard`（仓位、日亏损和下单频率限制）。
- **可靠性：** 订单绝不自动重试；超时或结果未知时按 `client_id` 查询订单；批量操作在部分失败时抛出 `BatchError` 并附带详细结果。
- **只撤 / 只平自己的单：** `cancel_all` 按 bot 标签（`client_id_prefix`）或订单 id 过滤；撤掉整个订单簿必须显式指定 `all_orders=True`。
- **WebSocket：** 自动重连、订单簿重新同步、状态事件；部分交易所支持通过 WS 下单。
- **模拟交易与回放：** `paper=True` —— 真实订单簿 + 虚拟账户，无需 API key。
- **重启恢复与策略：** 持仓日志、按 `client_id` 恢复状态。
- **多账户与代理：** 每个账户独立代理，共享市场缓存。
- **集成：** TradingView webhook、Telegram/Discord 通知、Prometheus、OpenTelemetry。

## 支持的交易所

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| 现货 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 永续合约 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| 模拟盘 / testnet | ✓ | ✓ | ✓ | 合约 | ✓ | — | ✓ | ✓ | — | — |
| 对冲模式 | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ |

各交易所的细节和限制（WS 下单、模拟环境的特殊行为等）见 [docs/GUIDE.md](docs/GUIDE.md)。

## 示例

[`examples/`](examples)：交易基础（[`00_trading_basics.py`](examples/00_trading_basics.py) —— 认证、现货与合约、
限价单的下单 / 改单 / 撤单、减仓）、行情与订单簿、回测、套利扫描器、网格、定投（DCA）、Telegram 提醒、
剥头皮策略骨架、多账户、带重启恢复的策略。
交易类示例默认在模拟盘运行。不想写代码？同样的机器人和回测可以直接在 [byttrade.com](https://byttrade.com) 上运行。

## 支持本项目

BYTtrade SDK 免费使用。最好的感谢方式就是使用它：SDK 参与了交易所的合作伙伴计划，
交易所本来就会收取的手续费中，有一部分会返还给项目。对你来说没有任何变化 —— 手续费和成交都不受影响
（许多其他交易库也是这样做的）。感谢你帮助项目成长！

<details>
<summary>详细说明</summary>

SDK 不会发送 ccxt 内置的 broker 标签。如果你不想使用合作伙伴标签，可以将其关闭：
在 `set_broker_config(...)` / `create_exchange(..., broker=...)` 中传入 `BrokerConfig.disabled()`，
或在环境变量中设置 `BYTX_BROKER_MODE=off`。

- **Hyperliquid builder fee** —— 仅在显式配置（`BrokerConfig(hl_builder_address=…)`）且用户批准后才会启用。
- **HTX** —— 文档中没有 broker 标签机制：ccxt 未公开的 `channel_code` 已被移除。

</details>

## 许可证

bytx 依据 [BYTtrade Source-Available License](LICENSE) 免费使用：你可以安装它，并在自己的脚本、机器人和应用中使用，包括商业用途。
不允许将其代码复制到其他库、发布修改版本，或将其作为托管服务提供。
使用了 ccxt（MIT），见 [`NOTICE`](NOTICE)。
不构成投资建议；加密货币交易存在亏损风险。
