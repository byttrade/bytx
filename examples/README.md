# Cookbook

Examples of working with `bytx`. Trading examples run on **demo/testnet** by default
(no real money involved); a live account only with `BYTX_LIVE=1`.
Keys come from environment variables (see [`_common.py`](_common.py)).

| Example | What it shows | Keys |
|---|---|---|
| [`00_trading_basics.py`](00_trading_basics.py) | authentication and key check, balance; buy/sell on spot and futures; limit order: place, amend, cancel; reducing and closing a position | demo |
| [`01_market_data.py`](01_market_data.py) | ticker, order book, candles; live order book over WS; spread, microprice, imbalance, fill price | none |
| [`02_backtest_sma.py`](02_backtest_sma.py) | SMA crossover backtest on exchange candles with fees | none |
| [`03_arbitrage_scanner.py`](03_arbitrage_scanner.py) | cross-exchange spread across 5 exchanges in parallel | none |
| [`04_grid_bot.py`](04_grid_bot.py) | post-only grid, counter orders via OrderTracker, dead-man switch | demo |
| [`05_dca.py`](05_dca.py) | scheduled buys by amount (`notional=`), extra buys on drawdowns | demo |
| [`06_telegram_alerts.py`](06_telegram_alerts.py) | Telegram alerts: level breakouts, sharp moves (WS) | none |
| [`07_scalper.py`](07_scalper.py) | quotes near the best prices skewed by microprice and position, amend, orders over WS, RiskGuard, kill switch | demo |
| [`08_multi_account.py`](08_multi_account.py) | `AccountPool`: many accounts, a proxy per account, shared IP warning | demo |
| [`10_strategy_restart.py`](10_strategy_restart.py) | `Strategy` skeleton: ticks, timer, TP/SL, journal; intents in sqlite — after a process crash orders are picked up without duplicates; `--paper` | demo / none |

```bash
pip install bytx
python examples/01_market_data.py BTC/USDT

export BYTX_EXCHANGE=bybit BYTX_API_KEY=… BYTX_SECRET=…   # exchange demo keys
python examples/04_grid_bot.py BTC/USDT 58000 62000 10 0.001
```

Example logic lives in functions (`backtest`, `best_spread`, `grid_levels`, `quotes`…), so it is easy
to reuse in your own code.
