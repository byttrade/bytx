# bytx — full guide

**English** | [Русский](GUIDE.ru.md) · [← README](../README.md)

A typed **async** Python SDK for crypto exchanges: Binance, Bybit, OKX, BingX, Hyperliquid, KuCoin, Bitget;
in beta: Gate, MEXC, HTX.
One interface for spot and perpetual futures, WebSocket with smart reconnects,
a proxy per account, and tooling for scalping.

```bash
pip install bytx            # + extras: [ws] [fast]
```

`[fast]` also installs `coincurve`: Hyperliquid actions are signed in ~4 ms instead of ~50 ms of pure Python
in the event loop (byte-for-byte the same signature as ccxt).

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

`demo=True` uses the exchange's demo/testnet; no real money involved.

> No-code bots, backtests and trade confirmations in Telegram — at
> [byttrade.com](https://byttrade.com).

## Why bytx instead of raw ccxt

bytx is built on top of [ccxt](https://github.com/ccxt/ccxt) and turns it into a trading SDK you don't
have to fight: the same task takes **one line instead of a page**, and the edge cases that cost money
in production are handled for you.

**Buy BTC for 100 USDT on OKX perpetuals**

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

**Take-profit and stop-loss on a position** — in ccxt every exchange is different (Bybit
`privatePostV5PositionTradingStop`, Binance two conditional orders with `closePosition`, OKX algo orders…).
In bytx it's the same call everywhere:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**A live order book that survives disconnects** — with ccxt you write the reconnect loop, backoff,
resubscribe, gap detection and snapshot resync yourself. In bytx:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**Timeouts on order placement** — with ccxt a timeout leaves you guessing whether the order is live
(retrying means a double entry). bytx reconciles by `client_id` and tells you exactly what happened:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**What else you get out of the box**

| Raw ccxt | bytx |
|---|---|
| `amount` in contracts, every exchange has its own `contractSize` | `size=` in the base coin, `notional=` in USDT, or `contracts=`; conversion and rounding (down) built in |
| `load_markets` on every instance, memory × accounts | one market cache per process |
| the exchange rejects the order over step size or min notional | checked before sending: `OrderTooSmall`, `InvalidOrder` |
| half the features are implicit API (`fapiPrivatePost…`) | direct methods: TP/SL, dead-man switch, position mode, transfers |
| `dict` responses | dataclass models (`Order`, `Position`, `OrderBook`…), raw response in `.raw` |
| bare ccxt exceptions | one `BytxError` hierarchy with a `.retryable` flag |
| WS dies with an exception, reconnecting is your problem | supervisor: backoff + jitter, silence watchdog, resubscribe, order book resync, state events |
| responses diverge from exchange docs | responses verified against official exchange docs ([ground truth](#quality)), ccxt bugs fixed in the SDK |

Plus: typed models everywhere, one volume unit across all exchanges, a proxy per account, paper trading
on live order books, a position journal, API-key permission checks and exchange health — all with the
same interface on every exchange.

## Features

**Market data:** `ticker`, `tickers`, `order_book`, `trades`, `candles`, `funding_rate(s)`, `funding_history`,
`open_interest`, `markets`, `market`, `server_time`, `leverage_tiers`, `maintenance_margin_rate`.
All of these work without keys (`create_exchange('bybit')`); markets live in a process-wide cache with a 1 h TTL.
`leverage_tiers(symbol)` returns leverage tiers and maintenance margin rates (`LeverageTier`): Bybit, OKX
(bounds in base: `min_size`/`max_size`), KuCoin, Hyperliquid (MMR = 1 / (2 · maxLeverage) per the docs),
Binance — `/fapi/v1/leverageBracket` with keys, without keys a fallback public web endpoint of the exchange
(`source='binance_public'`, not in the official docs). BingX does not publish tiers — `NotSupported`.
`maintenance_margin_rate(symbol, notional=|size=)` returns the tier-1 rate or the rate of the position's tier.

**Account:** `balance`, `free_collateral(symbol)`, `positions`, `position`, `open_orders`,
`closed_orders`, `order` (by id or `client_id`), `my_trades`, `trading_fee`, `leverage`,
`is_hedge_mode`, `transfer`. `free_collateral(symbol)` is free collateral in the symbol's settle
currency (`Market.settle`: USDC on Hyperliquid); if the currency is missing from the exchange response —
`ExchangeError`, not 0. `Position.side` is `'long'`/`'short'` in hedge mode too on every exchange (Bybit
`positionIdx`, OKX `posSide`, Binance/BingX/KuCoin `positionSide`), `Position.hedged` marks a hedge-mode
position, `margin_mode` is `'cross'`/`'isolated'`, `raw` is the underlying response. `positions(include_zero=True)`
also returns the exchange's zero entries (`size=0`; a zero one-way position has `side=None`). `Fill.position_side`
is the hedge position side of a trade from the exchange's data (REST and WS: Binance/BingX `positionSide`/`ps`, OKX
`posSide`); `None` — one-way, or the exchange doesn't report the side in a trade (Bybit has `positionIdx` only on the
order, not on the execution).

**Trading:** `buy` / `sell` / `place_order` (market/limit, `post_only`, `time_in_force`, `reduce_only`,
`take_profit` / `stop_loss`, `client_id`), `place_orders`, `amend_order`, `cancel_order`, `cancel_orders`,
`cancel_all`, `close_position`, `reduce_position`, `close_all`, `close_everything`, `flatten`,
`set_leverage`, `set_margin_mode`, `set_position_mode`.

**Execution algorithms** (`ex.algos`): `chase`, `twap`, `iceberg`, `bracket` (OCO per exchange docs),
`trailing_stop`, `market` with a slippage cap — see [Execution algorithms](#execution-algorithms).

**Risk-based sizing:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` sizes by stop distance,
leverage, exchange steps and minimums and fees, places the stop atomically or rolls back; `risk_size` does
the same math without orders — see [Risk-based position sizing](#risk-based-position-sizing).

**Protection:** `set_tp_sl` / `tp_sl_orders` / `tp_sl_order` / `cancel_tp_sl`, `cancel_all_after` and
`dead_man_switch()` (the exchange cancels orders if the process hangs), `kill_switch(symbol)` /
`kill_everything()`, `RiskGuard` (limits on position, daily loss and order rate — checked before
sending).

**Reliability:** an order is never retried automatically; on a timeout or unknown outcome
(`UnknownOutcome`) the SDK looks it up by `client_id` and returns what it found, or `RequestTimeout` with
`client_id` in `details`. Batch operations (`place_orders`, `cancel_orders`, `cancel_all`,
`close_all`, `close_everything`, `kill_switch`, `kill_everything`) do not stop at the first error: on partial failure — `BatchError`,
with what was and wasn't done in `details`. TP/SL not placed after entry — `ProtectionNotPlaced`
(the order is in `details['order']`, placed TP/SL legs in `details['placed']`).
`place_orders` uses one batch request where the exchange has it; Binance spot (the Spot API has no batch
placement), Gate and MEXC futures (ccxt 4.4.65 createOrders doesn't support them) go one by one in parallel,
with the same `BatchError` and a per-item result.
`cancel_orders` and `cancel_all` by tag cancel in batches per the exchange docs: Bybit (linear/spot) — 10 per
request, OKX — 20, Binance USDⓈ-M — 10, Hyperliquid — 40 (one signed `cancel` action); the outcome is per order (item code: "no such order" — `OrderNotFound`,
rate limit — `RateLimited`; an item rejected by the rate limit is retried once by a single cancel).
The account limiter is split by endpoint, as the exchange counts it: Bybit create, amend, cancel, cancel-all and
trading-stop, OKX placing, amending and cancelling have their own limits; a mass cancel does not delay placing orders
and closing a position. Bybit 10006 («Too many visits») pauses that endpoint's limiter of the key for the exchange
window. Per-account limits follow each exchange's Rate Limit docs: Bybit reads (positions, open orders, history,
executions, balance) per UID; every other OKX private endpoint has its own limit, and OKX trading endpoints are also
split by instrument («User ID + Instrument ID»); BingX, Bitget, Gate, MEXC and HTX — per endpoint and per UID, with
only the documented IP weight on the shared IP limiter. Endpoint costs ccxt gets wrong are fixed per the docs
(Binance USDⓈ-M account / balance / positionRisk — weight 5; `openAlgoOrders` — 1 with a symbol, 40 without).
Exchange bans pause the limiter until the ban ends and are not retried: HTTP 418, Bybit HTTP 403 «access too
frequent» (the IP, 10 minutes), BingX 100410 (the endpoint, until the unblock time in the response); `RateLimited`
carries `retry_after`. A limiter blocked longer than the remaining call deadline (`ex.deadline(...)`) raises
`RateLimited` right away instead of sleeping; a full SDK limiter queue — `ThrottleQueueFull` (a `RateLimited`).
Demo and testnet hosts (Binance Demo / testnet, Bybit demo, BingX VST, Hyperliquid testnet, KuCoin sandbox) have their
own IP limiter: a ban of the demo host does not hold live clients.
List reads return every page: `open_orders()` (Bybit cursor, OKX `after`, KuCoin Futures `currentPage`, with and
without a symbol) and Bybit `positions()` without a symbol (every settle coin and inverse); a list cut off by the page
cap raises `ExchangeError` instead of returning an incomplete list.

**TP/SL:**
- Default `trigger_by` (`None`): Binance and OKX — `'last'` (Binance
  `workingType=CONTRACT_PRICE`), Bybit, BingX, KuCoin and Hyperliquid — `'mark'`. An explicit `trigger_by=`
  wins.
- `set_tp_sl(..., position_side='long', contracts=n)` does not read the position: the caller sets side
  and size (multi-level TP without a race). Size is not checked against the position; the exchange's
  reduce-only keeps it from exceeding it. Bybit places the leg as Partial in this mode. OKX and KuCoin
  without a margin mode raise `ConfigError`.
- `place_tp_sl(symbol, 'take_profit' | 'stop_loss', trigger, position_side=, contracts=|size=,
  client_id=)` places one leg and returns a single `TpSlOrder` or raises, no list.
- `TpSlOrder` has a `state` field: `'open'` / `'triggered'` / `'canceled'` on every exchange, from the
  exchange's field (Binance `algoStatus`, Bybit `orderStatus`, OKX `state`, BingX `status`, KuCoin
  `stopTriggered`, HL `status`). `status` remains the order status.
- `modify_tp_sl(symbol, order_id, take_profit=|stop_loss=, size=|contracts=, client_id=)` returns
  `TpSlModifyResult`: `old` — `canceled` / `alive` / `triggered` / `unknown`, `new` — `created` /
  `unknown` / `rejected` / `None`, `order` — the new order, `strategy` — how it was moved. The move is done
  **without a window without a stop** where the exchange allows it (per its docs, table below;
  `bytx.exchanges.tp_sl_modify`).

  | Exchange | How it is moved | Window without SL |
  |---|---|---|
  | OKX | `amend-algos` — the same algoId, new trigger/size (atomic). With a new `client_id` or `trigger_by` (the amend doesn't change them, 51528) — make-before-break: up to 100 TP/SL per instrument | none |
  | Bybit | Full (whole position) — `trading-stop` in place, the orderId stays; Partial / a new size — make-before-break («Partial: can only add») | none |
  | Binance | make-before-break: algo `STOP_MARKET` orders with `quantity` coexist. A `closePosition=true` order without a new size — one per side (-4130): cancel, then the new one | none; closePosition — ~0.3–0.6 s (demo) |
  | BingX | make-before-break: VST accepts a second `STOP_MARKET` on the whole position. Modify Order — LIMIT only, cancelReplace — the same cancel | none (VST) |
  | KuCoin, Hyperliquid | make-before-break: up to 200 stop orders per contract (KuCoin, docs), reduce-only triggers (HL, testnet) | none |
  | Bitget | `modify-tpsl-order` in place: the same orderId and clientOid (demo 2026-11-04). `pos_loss` / `pos_profit` (whole position) — one per side: a second placement replaces the previous one with the previous clientOid, so the same size is always moved in place, a new `client_id` is not applied (warning). `loss_plan` / `profit_plan` with a size — several per side: with a new `client_id` or size — make-before-break (whole position → size: a new `loss_plan`, then removing `pos_loss`) | none |
  | Gate, MEXC, HTX | cancel, then the new one (two TP/SL per position not verified against the docs) | the time of one request |

  Make-before-break: the new one is placed first; the new one rejected — the previous one is untouched
  (`old='alive'`, `new='rejected'`), outcome unknown — the same (`new='unknown'`, look it up by `client_ids`); the
  previous one could not be removed — the new one is not lost: `old='alive'` and `both_alive=True` (both are on the
  exchange, the caller decides). Cancel first: the new one is placed only if the old one is definitely gone; the new
  one rejected — the previous trigger is placed again (`restored`). Paper — make-before-break.

**Cancel and close only what's yours:**
- `cancel_all(symbol, client_id_prefix=...)` cancels only orders and TP/SL carrying the bot tag. By
  default the client tag from `create_exchange(client_id_prefix=...)` is used.
  `cancel_all(symbol, order_ids=[...])` cancels only those ids, including Binance/OKX algo orders
  by ids from your DB after a restart. Blindly cancelling the whole book is explicit only: `all_orders=True`.
  Without any of these, `cancel_all` raises `ConfigError`.
- `cancel_all` returns a `CancelReport`: per order `canceled`, `already_closed` or
  `error`. On errors it raises `BatchError` with the report in `details['report']`.
- `close_all(symbol, side=None)` closes positions of one symbol. All positions on the account — explicitly,
  `close_everything()`. Same for `kill_switch(symbol, ...)` and `kill_everything()`.
- `close_position` / `reduce_position` take the **position** side `'long'`/`'short'`. Passing
  `'buy'`/`'sell'` raises `ConfigError`.
- `tp_sl_order(symbol, id)` and `cancel_tp_sl(symbol, order_ids=[...])` work without in-process
  memory: Binance — Algo API by algoId, OKX — algo order by algoId.
- A triggered TP/SL is often filled by the exchange with a **different** order, and closing trades go under its id, not
  under the TP/SL id. `tp_sl_order(symbol, id).fill_order_ids` (the first one — `fill_order_id`) are those orders: OKX
  `ordIdList`, Binance `actualOrderId`, BingX (the order with `triggerOrderId`), Gate, MEXC, HTX. Bybit
  (trading-stop), KuCoin, Hyperliquid fill under the same id — empty, trades are looked up by the TP/SL id.
- `flatten(symbol, side)` exits a position step by step: cancels your limit orders, then your TP/SL,
  closes the position with a reduce-only order and waits until it is gone. Returns a `FlattenReport`
  with each step's outcome; `ok` means the position is closed with no errors.

**Cancelling an already closed order:** `cancel_order` for an order no longer on the book (filled,
cancelled earlier) returns the order with its final status (`'closed'`, `'canceled'`, …) instead of
raising. "Not open" is detected by the codes from the exchange docs. Bybit looks the order up (`order`,
matching by `client_id`) in `/v5/order/realtime`, then in `/v5/order/history`: realtime keeps only the
last 500 closed orders.

**Leverage, margin, position mode:**
- `margin_mode` has no default: the SDK never silently assumes `'cross'`. OKX and KuCoin send the mode
  with every order, OKX and Hyperliquid with leverage changes; without `create_exchange(margin_mode=...)` or
  `set_margin_mode(symbol, mode)` they raise `ConfigError`. Closing and `reduce_position` use the mode of the
  open position. `margin_mode_of(symbol)` returns `None` if no mode is set.
- KuCoin takes leverage from each order: an opening order without `set_leverage` on this instance
  raises `ConfigError` rather than defaulting to 1x.
- KuCoin keeps the margin mode both in the order and on the symbol; an order in a mode other than the
  symbol's is rejected (330005). `set_margin_mode(symbol, mode)` reads the symbol's mode
  (`GET /api/v2/position/getMarginMode`) and, if different, switches it (`POST /api/v2/position/changeMarginMode`,
  the response is checked); an exchange rejection raises and the SDK keeps the previous symbol mode.
- `set_leverage(symbol, x)` returns the leverage the exchange reported as applied (lower than
  requested — capped by tier), or `None` if the exchange doesn't report it (Bybit, Hyperliquid, KuCoin).
  An open position on the symbol raises `bytx.PositionOpenError`; `allow_open_position=True` skips the check.
- `set_leverage(symbol, x, side='long'|'short')` sets leverage of one hedge position side (the other side is not
  changed, only this side's position is checked): OKX isolated + long/short (`posSide`), BingX hedge (`side`),
  Bybit hedge (`buyLeverage`/`sellLeverage`; in cross the exchange requires them equal — rejection 10001).
  Binance (one leverage per symbol), OKX cross, one-way and other exchanges — `NotSupported` before any request.
- `leverage(symbol, side=, margin_mode=)` reads leverage for a side and margin bucket. `None` — the exchange
  doesn't report it. Different long and short leverage without `side` raises `ConfigError`.
- `is_hedge_mode(refresh=True)` re-reads the mode from the exchange. `set_position_mode` updates the cache
  and resets it on failure.

**Size units.** In models, size in the base coin is `size`, `filled`,
`remaining`; in exchange units (contracts; base on spot) — `contracts`, `filled_contracts`,
`remaining_contracts` (`Order`, `Fill`, `Position`). There is no unitless `amount` field. On OKX and KuCoin
a contract ≠ a coin (`Market.contract_size`): `BTC-USDT-SWAP` is 0.01 BTC, `XBTUSDTM` is 0.001 BTC.

- `place_order(..., margin=, leverage=)` sizes from margin with leverage: contracts = margin × leverage /
  (price × contract_size). Price is the execution price: your own for limit, the reference price for market.
  `leverage` defaults to the one from `set_leverage` on this instance; no leverage or a different one —
  `ConfigError`. `notional=` is the full notional **without** leverage. A size below the minimum lot is rounded up
  to the minimum: 5 USDT of margin × leverage on BTC may become 100 USDT. `max_round_up=` (default None — no cap, but
  a WARNING at 2× and more) caps how many times the minimum may exceed the requested size, otherwise `OrderTooSmall`
  (`details`: `requested`, `min`).
- `contracts_for_margin(symbol, margin, leverage, price)` does the same math without an order. `None` —
  below the exchange's minimum lot even after rounding up one step.
- Size rounding is the same on every path: division in `Decimal`, snapping up to a whole lot if it is
  within 1e-9 relative (caller-side float error: `k·lot·price/lev`), then down to the step.
  `reduce_position` and `set_tp_sl` never exceed the position size.
- `set_tp_sl(..., contracts=)` is a partial TP/SL in contracts; `size=` in base.
- `Market.min_cost`: Bybit `minNotionalValue`, Binance
  `MIN_NOTIONAL`, Hyperliquid $10, otherwise ccxt `limits.cost.min`; `None` means no limit.

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Spot | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ ⁶ | ✓ |
| Perpetual futures | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| Demo / testnet | ✓ | ✓ | ✓ | futures | ✓ | — ² | ✓ ³ | ✓ ⁴ | — ⁷ | — ⁷ |
| Orders over WS | ¹ | ¹ | ✓ | — | ¹ | — | — | ✓ | — | — |
| Hedge mode | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — ⁵ | ✓ | ✓ |

¹ `ws_*` methods pick the transport automatically (WS or REST) — the call is the same either way.
² KuCoin shut down its sandbox (hosts don't respond) — no demo.
³ Bitget Demo Trading: Demo API Key + `paptrading: 1` header on the production host with the same symbols
(not ccxt's `SBTC/SUSDT`), WS — `wss://wspap.bitget.com`.
⁴ Gate TestNet: `https://api-testnet.gateapi.io/api/v4` (spot and USDT-M), WS — `wss://ws-testnet.gate.com`.
⁵ Gate dual mode (closing via `auto_size`) is not supported yet: single (one-way) only.
⁶ MEXC spot: WS streams are NotSupported (the exchange sends protobuf, ccxt 4.4.65 uses the old JSON channels); REST — ✓.
⁷ MEXC has no testnet in its docs; HTX lists "Testnet (Stopped)": `demo=True` → `NotSupported`, test on paper.
⁸ HTX docs have no broker tag mechanism: ccxt's undocumented `channel_code` is removed, and there is no own ID.

`demo=True` is the exchange's own testnet/demo (Bybit — demo trading, others — the exchange sandbox),
**except Binance** — see below. It is not a simulator; the in-memory
simulator is paper mode (`paper=True`, see "Paper trading and replay").

### Binance: Demo Mode (default) and testnet

`create_exchange('binance', demo=True)` (or `demo='demo'`) is **Demo Mode** (demo.binance.com), one
key for spot and futures. `demo='testnet'` is the Binance testnet.
The environment is `ex.demo_env` (`'demo'` / `'testnet'` / `None`); `ex.demo` is still a bool. On other exchanges
`demo='testnet'` raises `ConfigError` (single demo environment).

| | Demo Mode (`demo=True`) | testnet (`demo='testnet'`) |
|---|---|---|
| Spot REST / WS API / streams | `demo-api.binance.com/api` / `demo-ws-api.binance.com/ws-api/v3` / `demo-stream.binance.com/ws` | `testnet.binance.vision/api` / `ws-api.testnet.binance.vision` / `stream.testnet.binance.vision` |
| USDⓈ-M REST / streams | `demo-fapi.binance.com` / `demo-fstream.binance.com` (`/public`, `/market`, `/private` — as in production) | `testnet.binancefuture.com` / `fstream.binancefuture.com` |
| USDⓈ-M WS API (`ws_place_order`) | `testnet.binancefuture.com/ws-fapi/v1` ¹ | same |
| COIN-M REST / streams | `demo-dapi.binance.com` / `demo-dstream.binance.com` | `testnet.binancefuture.com/dapi` / `dstream.binancefuture.com` |
| Prices, order books | close to the exchange (not real trades) | spot — independent of the exchange |
| Filters, IP limits, order count | as on the exchange | spot — "mostly" as on the exchange |
| Balance | reset in the UI any time | spot — reset monthly |
| `/sapi`, `/papi`, `/eapi`, `/futures/data` | absent → `NotSupported` with a reason, before any network call | absent |

¹ The USDⓈ-M "WebSocket API General Info" doc lists only this host; `demo-ws-fapi.binance.com` does not
respond (202 → www.binance.com), and testnet.binancefuture.com is the same backend as demo-fapi (same
order book and lastUpdateId; checked without keys 2026-10-26).
Docs: [Spot Demo Mode](https://developers.binance.com/docs/binance-spot-api-docs/demo-mode/general-info),
[USDⓈ-M General Info](https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info).
Without `/sapi`: `health().status` is `'unknown'` (no request), `check_key()` returns key permissions as `None`
with the reason in `unknown`; the UID in Demo Mode is read from `/api/v3/account` (demo 2026-11-04). ccxt inferred the error market type and WS spot streams from the host — fixed
for demo hosts (`bytx/exchanges/binance/demo.py`).

### Demo environment quirks

Quirks of the demo environments themselves (not SDK bugs; checked 2026-10-26):

- **Binance spot testnet** (`demo='testnet'`, `testnet.binance.vision`) is a separate system with its own
  keys: a futures testnet key gets -2015 there. In Demo Mode (`demo=True`) the key is shared.
- **Binance Demo Mode and futures testnet** have no sapi (`apiRestrictions`): `check_key()` returns
  `can_trade`/`can_withdraw`/`ip_restricted` = None with the reason in `unknown` (the UID comes from
  `/api/v3/account`, available in Demo Mode). A `withdraw='reject'` policy won't catch such a key with withdrawal
  rights — only a warning.
- **BingX VST** — key permissions (`/openApi/v1/account/apiPermissions`) and UID (`/openApi/account/v1/uid`)
  answer on the VST host on the production paths (demo 2026-11-04): `check_key()` on VST knows the withdrawal
  permission. Balance and Get Fund Flow on VST are in the `VST` currency (virtual USDT; counted as USDT in portfolio
  totals); the VST ledger lags: trades — 6–13 min, funding — 20–65 min after settlement (2026-11-04); the
  funding rate history on VST is sometimes empty, sometimes a year old.
- **`health().status`** on Binance (Demo Mode and testnet) and Bitget demo is `'unknown'`: these environments
  have no status pages; `reachable` and clock offset are measured as usual.
- **Bitget demo** — the account is funded manually in the web UI (Demo Trading: USDT on futures and
  spot). With an empty account, trading requests get `InsufficientFunds` (40762 / 43012).
  Get Trade Rate (`trading_fee`) on demo — 40404 «Request URL NOT FOUND» (demo trade rates — maker 0.02%,
  taker 0.06%, as in the docs).
- **Bybit demo** has no public demo WS (market data comes from production public streams, no keys).

SDK behavior discovered on demo:

- Position mode is read from the exchange rather than assumed one-way: **Bybit** — per symbol from
  `GET /v5/position/list` (`positionIdx` 0 / 1–2; the mode resolves symbol > coin > default, cached
  per symbol), **Bitget** — `posMode` from Get Single Account. Not readable — `ExchangeError`.
  `set_position_mode` / `set_leverage` with "already set" (Bybit 110025 / 110043) is success.
- Trackers (`order_tracker`, `position_tracker`) take the REST snapshot once the subscription is live:
  Bybit/Bitget — after the server's subscribe response, others — after the connection opens
  (`await sub.ready()`; limit — `StreamConfig.ready_timeout`, 10 s). An order placed right
  after `start()` is not lost.
- Binance `amend_order(price=…)` without `size` sends the order's full size (`origQty`): Modify Order
  requires both quantity and price.
- Where the exchange amends by cancel + new order (Binance spot cancelReplace,
  BingX cancelReplace, Bitget contracts modify-order, Hyperliquid modify), `amend_order` gives the new order
  a client_id equal to the old one with a new random tail (bot tag, role and level are kept; the Binance
  broker prefix is added): the order stays yours for `owns` / `cancel_all`, the journal records
  `canceled` for the old one and `replaced` (`replaces` = old id), and the intent store gets a new intent with
  the same binding. The same client_id is never reused (Binance: "unique id among open
  orders"; lookup by id would be ambiguous). KuCoin (alter doesn't set the new order's client_id),
  MEXC and HTX (no editOrder / ccxt emulation) — `NotSupported`: cancel and place again.
- **Bitget** (demo 2026-11-04): `leverage()` — the leverage of the symbol's margin-mode pocket (cross —
  `crossedMarginLeverage` of Get Single Account, isolated — `isolatedLong/ShortLever`; ccxt returned isolated in cross
  too); hedge — the exchange response `side` is the position direction (closing a long — `buy` + `tradeSide close`),
  SDK `Fill.side` / `Order.side` is the trade direction (ccxt reported REST trades and WS closing orders as buys; the
  WS Fill Channel already sends the trade direction); `modify_tp_sl` for a whole-position TP/SL (`pos_loss` /
  `pos_profit`, size `"0"`) — in place without a window (the exchange doesn't create a second such order per side but
  replaces the previous one), and where the move is a cancel and a new placement, the new one also covers the whole
  position (before, the previous one was removed and the new one rejected before the network — a position without a
  stop); a size ≤ 0 — `ConfigError` before any action; `tp_sl_order` finds a fired / removed TP/SL in the plan order
  history (Get History Trigger Order); a TP/SL rejected by price (40834, 40915, 40917 …) — `InvalidOrder`; a
  cancel-plan-order counts only `successList` (empty lists — the order is re-read: gone — `OrderNotFound`, alive —
  retried with its `planType` or «cancel not confirmed»).

The same table in code, without network or keys: `bytx.exchange_info('okx')` →
`ExchangeInfo(credentials=('api_key', 'secret', 'password'), markets=('spot', 'swap'), demo={...})`,
`bytx.exchanges_info()` for all exchanges.

## Spot

`create_exchange(name, market='spot')` on Binance, Bybit, OKX and KuCoin follows each exchange's own documentation:

```python
ex = create_exchange('bybit', market='spot', api_key=..., secret=..., demo=True)
await ex.buy('DOGE/USDT', notional=10, client_id='b1')   # market buy for 10 USDT
await ex.order('DOGE/USDT', client_id='b1')               # any status, within the exchange's retention
await ex.find_order('DOGE/USDT', 'b1', timeout=8)         # None — absent; RequestTimeout — could not check
await ex.sell_all('DOGE/USDT', client_id='s1')            # the whole free base balance; dust → None + WARNING
```

| | Binance | Bybit | OKX | KuCoin |
|---|---|---|---|---|
| `buy(notional=…)` market | `quoteOrderQty` | `marketUnit=quoteCoin` | `tgtCcy=quote_ccy` | `funds` |
| size / cost limits | `LOT_SIZE`, `MARKET_LOT_SIZE` (market orders), `NOTIONAL` | `basePrecision`, `minOrderAmt` | `lotSz`, `minSz` | `baseIncrement`, `baseMinSize`, `minFunds`, `quoteMinSize` |
| order by `client_id` | `GET /api/v3/order` | `/v5/order/realtime` → `/v5/order/history` | `/trade/order` → `orders-history` (7 days) → `orders-history-archive` (3 months) | HF: `hf/orders/client-order` → `hf/orders/done` (`lastId`); classic: active orders only |
| demo | Demo Mode / testnet | demo | demo trading | — |

- `buy(notional=…)`: the quote amount is sent with the exchange parameter and rounded down to the quote step; below
  the minimum cost (on OKX — below `minSz` at the reference price) it is `OrderTooSmall` before sending. Sells by
  notional and other exchanges convert to the base size at the reference price.
- `sell_all(symbol, *, client_id=None)` — spot only (`ConfigError` otherwise): the free base balance strictly rounded
  down to the lot step, then a market sell through `place_order`. Dust (below the step, the minimum size or the
  minimum cost) returns `None` with a WARNING.
- A failed history page is never read as «no order»: `find_order` raises `RequestTimeout`; a pagination cut-off is
  `ExchangeError`. A placement timeout on spot is reconciled through the same lookup, so no second order is sent.
- KuCoin: an empty `data: null` response is treated as «not found» (the documentation does not describe the
  not-found response).
- One request per market order: `buy(notional=…)` needs no price, `size` checks the minimum cost with a price cached
  for 5 s. A response without the fill (Bybit, OKX, KuCoin) is completed from the `ex.stream.orders()` stream if it is
  subscribed, otherwise with one `order(id)`; if that fails, the order has `partial_info=True`.
- An explicit rejection by the exchange is `InvalidOrder` at once (below the minimum on Bybit — `OrderTooSmall`), with
  the ccxt error in `__cause__`; only timeouts and «outcome unknown» codes are reconciled.
- `cancel_order(..., strict=True)` raises the exchange error (e.g. Binance -2011) if the order is no longer open instead
  of returning its final status. `exchange_client_id(ex, market, client_id)` returns the cid as it goes to the exchange.
- Binance: `demo=True` is Demo Mode, `demo='testnet'` is the older testnet.

## WebSocket

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps, book.microprice, book.imbalance(5))

handle = ex.stream.subscribe('trades', 'BTC/USDT', on_trade)      # or with a callback
ex.stream.on_state(lambda ev: print(ev.state, ev.stream, ev.attempt))
```

Streams: `order_book`, `ticker`, `best_bid_ask`, `trades`, `candles`, private `orders`, `fills`,
`positions`, `balance`. All subscribers share one stream. Disconnect → reconnect with backoff;
order book or ticker silent longer than the threshold → that topic is resubscribed, and if the whole
connection goes quiet — it is reconnected (trades and candles have no silence threshold by default); the
order book is checked for gaps and crossing and rebuilt from a REST snapshot.

## Scalping

```python
async with ex.order_tracker('BTC/USDT') as orders, ex.dead_man_switch(30):
    price = ex.round_price('BTC/USDT', book.best_bid[0], side='buy')   # toward the passive side
    o = await ex.ws_place_order('BTC/USDT', 'buy', size=0.001, price=price, post_only=True)
    await ex.ws_amend_order('BTC/USDT', o.id, price=price - 1)
    done = await orders.wait(o.id, timeout=30)                           # closed / canceled
```

- orders over WS with REST fallback (`ws_place_order`, `ws_amend_order`, `ws_cancel_order`);
- `OrderTracker` / `PositionTracker` — state from private WS, REST reconciliation after a disconnect;
- `OrderBook`: `spread_bps`, `microprice`, `imbalance`, `depth`, `fill_price(notional=…)`;
- metrics: `ex.metrics()` (p50/p99 per method), `ex.stream.stats()` (exchange → us latency).

## Paper trading and replay

```python
from bytx import create_exchange
from bytx.paper import PaperConfig, Recorder, replay_exchange

# live exchange order book, virtual account: no keys needed, none are sent anywhere
ex = create_exchange('binance', paper=True, paper_config=PaperConfig(balance={'USDT': 1000}))
o = await ex.buy('BTC/USDT', notional=50)          # walks the book, default-tier taker fee
print(ex.summary())                                # realized / unrealized PnL, fees, funding

with Recorder(live_ex, 'btc.jsonl.gz'):            # record ex.stream streams to a file
    ...

ex = replay_exchange('btc.jsonl.gz')               # same data, virtual clock, no network
result = await ex.replay.run(strategy)             # strategy(ex) — regular code on ex.stream / ex.buy
result.fingerprint()                               # same inputs → same fills
```

`create_exchange(..., paper=True)` (or `Exchange.paper(name)`) returns a `PaperExchange` — the same
`Exchange` interface: units, rounding, minimums, client_id, `pre_submit`, RiskGuard, journal,
`stream.orders/fills/positions/balance`, trackers, TP/SL, `close_position`, `kill_switch`, `flatten`.

- **market** walks the book levels (taker); a crossing **limit** is taker up to its price, the
  rest rests on the book (IOC — expired, FOK — all or nothing); a resting limit fills as maker at its price
  when the book or a trade crosses it; partial fills; your own fills consume levels
  until the next snapshot. A crossing **post-only** and a **market** with no opposite liquidity behave like
  the exchange per its docs (`bytx.exchanges.post_only_cross` / `market_no_liquidity`, tables in
  `exchanges/<exchange>/matching.py` with URL and `verified`): rejected with `InvalidOrder` (Binance -5022) or
  created and cancelled — `status='canceled'` with no fill (post-only: Bybit, OKX, Hyperliquid; market: Bybit);
  no order book at all — `InvalidOrder`. **reduce-only**: no
  position — `PositionAlreadyClosed`, fill capped at the position, on close the side's reduce-only
  orders and TP/SL are cancelled. **TP/SL** trigger on mark (or last), fill against the book.
- **Fees** — each exchange's default tier per its docs (`bytx.exchanges.<exchange>.fees`,
  every rate with URL and `verified`); custom — `PaperConfig(fees=(maker, taker))`.
- **Positions, balance, PnL, funding** — in memory: one-way and hedge, leverage, margin, `InsufficientFunds`;
  funding at the rate at `next_funding_time` (live paper polls the rate).
- **Liquidation is simplified** and flagged (`raw['paper']['liquidation_model'] == 'simplified'`):
  `entry × (1 ∓ 1/leverage ± mmr)` per position, closed at mark.
- Recording is JSON Lines (`.gz`), versioned `bytx.ws-record` format; markets are stored in the file.
  Replay is deterministic if the strategy doesn't depend on real time: sleep via `ex.clock.sleep`;
  SDK-generated client_ids and `twap` / `iceberg` randomization without `seed` come in replay from a
  generator seeded with `PaperConfig(seed=)` (default 0).
- Not emulated: `cancel_all_after`, `transfer` (`NotSupported`).

> ⚠️ Paper does **not** replace the exchange demo/testnet: liquidation and margin models are simplified.

## Execution algorithms

```python
algo = ex.algos.chase('BTC/USDT:USDT', 'buy', size=0.5, max_moves=20, max_deviation_bps=30)
algo.pause(); algo.resume()                         # paused: no new orders or re-quotes
res = await algo                                    # AlgoResult: status, reason, filled, average, children
await ex.algos.market('BTC/USDT:USDT', 'sell', size=1, max_slippage_bps=20)        # limit IOC with a cap
await ex.algos.twap('ETH/USDT:USDT', 'buy', size=10, duration=600, slices=20, randomize=0.3, seed=1)
await ex.algos.iceberg('BTC/USDT:USDT', 'sell', size=2, price=72_000, visible=0.1)
await ex.algos.bracket('BTC/USDT:USDT', 'buy', size=0.1, take_profit=72_000, stop_loss=66_000)
trail = ex.algos.trailing_stop('BTC/USDT:USDT', 'long', distance_pct=1.5, exchange_stop=True)
await ex.algos.cancel_all()                         # cancel all unfinished algorithms
```

`ex.algos.*` runs the algorithm as a background task and returns its object immediately: `await algo` gives the result
(`AlgoResult`: `status` 'done' / 'canceled' / 'failed', `reason`, `filled` / `average` / `fee`,
`children`, `extra`), plus `algo.status`, `algo.filled`, `pause()` / `resume()` / `await cancel()`.
Bad arguments raise `ConfigError` immediately. Works the same on the exchange, paper and replay (time —
paper's `ex.clock`; in tests — `VirtualClock`). The module is imported on first `ex.algos` access:
the `place_order` / WS hot path is unchanged.

- **chase** — post-only at the best price on its side; when the price moves — cancel, confirmed
  final status, new order for the remainder (not amend: on some exchanges it loses post-only).
  A post-only bounce (rejected on placement — Binance -5022, BingX 101215 — or cancelled by the exchange) is
  retried. Limits: `max_moves`, `max_deviation_bps` from the start price, `timeout`, `min_interval` between
  re-quotes; when exhausted — `on_exhausted='stop'` or `'market'` (IOC with a cap).
- **market** — limit IOC no worse than "best opposite (or mid) ± `max_slippage_bps`", price rounded inside
  the cap, and where the exchange limits the order price with a band (OKX `price-limit`: beyond it 51006, demo BTC —
  ±0.5%) — inside the band too (`extra['price_band']`); without opposite liquidity within the price no order is
  sent; `retries` reuse the same price. The same applies to twap IOC parts and the chase remainder.
- **twap** — `slices` parts over `duration` seconds: parts catch up on schedule lag, `randomize`
  jitters timing and weights (`seed`), a part below the minimum order accumulates, a tail below the minimum goes
  into the last part; `slice_type='market'` (with `max_slippage_bps` — IOC) or `'limit'` (post-only, remainder
  rolls into the next part, market at the end).
- **iceberg** — only `visible` (± `visible_jitter`) at `price` on the book, but not below the minimum order
  (`extra['visible_min']`); a part cancelled by someone else — stop (`child_canceled`); a size that can't be split
  into parts smaller than itself — `OrderTooSmall` (the whole size is never silently placed on the book).
- **bracket** — market/limit entry (`entry_timeout`) → `set_tp_sl` on the filled size → **OCO**:
  how the exchange links TP and SL follows its docs and demo runs (`bytx.exchanges.tp_sl_oco`): OKX — one `oco`
  algo order (one leg fires; for a `conditional` order with TP and SL in one-way the exchange executes only the SL —
  Place algo order docs, demo 2026-11-04); Binance, Bybit (Partial legs), BingX, Hyperliquid (grouping `na`), KuCoin —
  legs are independent, and the algorithm cancels the sibling of the fired leg (otherwise the reduce-only leg
  would close the next position). Demo 2026-11-04: on Bybit Partial and BingX the sibling stays alive while the
  position is open (no OCO), and when the position closes the exchange cancels the remaining legs itself; KuCoin —
  by the docs only (the link is not described, no demo), `verified=False`. A leg that fired between reading the legs
  and reading the position still gives the reason `take_profit`/`stop_loss`. OKX: a fired algo pair is not found by
  `algoId` for minutes (51603) — then the reason is `position_closed`. Protection not placed — the entry is closed
  (`on_protect_fail='close'`). With an intent store the protection plan (TP/SL prices, `trigger_by`, size, leg
  client_ids) is written **before the entry is sent** — if the process is killed between the entry and the legs,
  `ex.recover()` places the missing legs from the plan (see "Restart recovery and strategies"); once the legs are
  placed the plan is retired and the algorithm manages them.
- **trailing_stop** — client-side level trailing the extreme (`distance` / `distance_pct`,
  `activation_price`), triggered via reduce-only market. `exchange_stop=True` — a backup SL on the
  exchange, moved make-before-break (Bybit Full — `modify_tp_sl` in place).

The minimum order of a part is the amount step, `min_amount` and the **minimum notional** `min_cost` at the order
price (with a 1% margin; for reduce-only the notional is not checked — same as in `place_order`). The whole `size`
below the minimum — `status='failed'`, `OrderTooSmall`, no orders; a remainder below the minimum can't be placed —
result `filled`, the remainder in `extra['below_min']` (chase cancels the partially filled limit order).

Common rules: every child order is a regular SDK order (bot tag `client_id_prefix`, broker
tag, `pre_submit`, RiskGuard); with a position journal — a role (`role=`, `entry`/`tp`/`sl` for bracket,
`trailing` for trailing) and `position_id=`. An order with an unknown outcome is not re-quoted — it is reconciled by
client_id; no new order goes out while the previous one may be alive; network failures, «order not found» for an
order the exchange accepted and an unclear cancel rejection retry up to `max_errors`. A child cancel response without
the filled amount (Bitget Cancel Order — `orderId` / `clientOid` only; OKX, Bybit, KuCoin, HL — the id only) — the
order is re-read (a live tracker first, otherwise REST): what was filled before the cancel is not lost, chase doesn't
place the remainder anew. The cancel is accepted but a read sees the order open (an async cancel) — no second order
is placed; no final state within `max_errors` pauses of `poll` — the algorithm is `failed` with the reason
`cancel_unconfirmed` (the order is in `extra['unconfirmed']`, cleanup cancels it once more; with an intent store the
record is `unclean` — `recover()` after a restart cancels the live order).
Order state comes from a live `OrderTracker` (`tracker=`), after a WS disconnect — REST until the tracker resyncs.
`cancel()` cancels working orders (for bracket/trailing — protection too); an algorithm failure or cancelling its task
does **not** remove position protection. Algorithms live in process memory: they **don't resume** after a restart.
`kill -9` (unlike cancelling the task) doesn't cancel working orders: a chase limit order, the visible iceberg part,
a twap limit part, a bracket limit entry would stay on the book at the last price unattended. With an intent store
(`intents=IntentStore(...)`) `ex.recover()` takes care of it — the **safe policy** (`cancel_orphaned_algos=True` by
default): working orders of unfinished algorithms are cancelled, position protection (bracket legs, trailing SL)
stays, filled parts are not touched — `report.algos`, see "Restart recovery and strategies". A bracket killed
between the entry fill and its legs gets the legs from its protection plan (written before the entry) — position
protection, not resuming. Bracket OCO watching and trailing SL tightening do not resume after a restart.

> ⚠️ **Orphaned OCO leg.** Where the exchange doesn't link TP and SL (all but OKX), the sibling of the fired leg
> is cancelled by the bracket task. If the task is cancelled, the algorithm crashes or the process restarts, the link is gone: after
> one leg fires, the other (reduce-only) stays on the exchange and will close the **next** position on the same
> side. `ex.recover()` cancels **your own** such legs if there is definitely no position on their side
> (`report.orphans_canceled`; `cancel_orphaned=False` — report only, in `report.orphaned`), see below.
> Demo 2026-11-04: Binance leaves the legs after the position closes — `recover()` found and cancelled them;
> Bybit, BingX and OKX (`cxlOnClosePos`) cancel the legs themselves once the position is closed. A risk remains when
> the symbol's position is larger than the bracket size (another bot, a manual entry): the leg lives on the rest of
> the position, it is not an "orphan", and on Bybit (trading-stop) and BingX (conditional orders) the leg has no
> client_id — `recover()` doesn't recognize it as yours.

## Risk-based position sizing

```python
plan = await ex.risk_size('BTC/USDT:USDT', 'long', risk_usdt=50, stop=66_000)   # no orders
res = await ex.open_position('BTC/USDT:USDT', 'long', risk_usdt=50, stop=66_000, take_profit=72_000,
                             leverage=5, slippage_bps=5)
res.plan.contracts, res.plan.risk, res.risk          # size, planned risk and actual risk after fill
res.stop_mode                                        # 'attached' | 'separate'
```

Size: loss per base unit = `|entry − stop exit| + fee_rate × (entry + exit)`; a market entry uses the best
opposite price, `slippage_bps` worsens both entry and stop exit; `fee_rate` defaults to the exchange's
default-tier taker fee from its docs (`bytx.exchanges.<exchange>.fees`; paper — the account rate).
Contracts = `risk_usdt / (loss × contractSize)` rounded down to the exchange step, so the actual risk
(`RiskPlan.risk`, with fees) never exceeds `risk_usdt`. If the minimum size (`min amount`, `min_cost`)
risks more — `errors.MinSizeExceedsRisk` (`details`: `min_contracts`, `min_risk`), no order is sent.
Leverage doesn't affect risk but sets margin (checked against free collateral, `check_margin`) and the
liquidation estimate: in isolated mode a stop beyond liquidation is rejected.

Stop. For a market entry on exchanges whose docs allow attaching an SL to the entry order
(`bytx.exchanges.attached_sl`: Bybit `stopLoss`, OKX `attachAlgoOrds`, BingX `stopLoss`, KuCoin
`st-orders`), SL and TP go **in the same request**. Binance (New Order has no TP/SL fields),
Hyperliquid (the SDK sends grouping `na`), a limit entry (`price=` — IOC), `stop_mode='separate'` or an explicit
`trigger_by` — reduce-only `set_tp_sl` on the filled size right after entry. If an attached entry fills partially
(OKX places the algo only after the entry fills completely), the SL is checked on the exchange (`tp_sl_orders`; for OKX
the docs say it isn't there yet — no request); if missing, it is placed separately as below (`stop_mode='separate'`, a warning
in `res.warnings`). If the SL fails —
the placed TP is cancelled, the position is closed with a reduce-only market order and `errors.ProtectionNotPlaced` is raised
(`details['rolled_back']`, `close`, `close_error`); `on_stop_fail='keep'` — no close. If the entry outcome is
unknown but a position exists, the attached SL is checked on the exchange (OKX too: the fill is unknown):
if present, no second one is placed; if not, the stop is placed separately (warning in `res.warnings`). Whether a stop exists
is decided by `bytx.protection.find_leg`: client_id where it is sent to the exchange, otherwise kind + side +
trigger price at the exchange price step.

Demo run 2026-11-04 (Bybit / Binance / OKX / BingX, BTC and ETH):

- the loss when the stop fires (entry and exit trades from the exchange, reconciled with the exchange's funds journal
  `account_report`) stays within `RiskPlan.risk` plus slippage beyond `slippage_bps`; the traded size equals the
  plan with contractSize (OKX — contracts of 0.01 BTC / 0.1 ETH); the trade fee equals the taker fee from the docs
  (Bybit 0.055%, OKX and BingX 0.05%); Binance Demo Mode — the account rate 0.04% < 0.05% of the docs (the plan has
  a margin; your own tier — `fee_rate=`);
- an attached SL fires on the exchange's default trigger: Bybit `slTriggerBy` and OKX `slTriggerPxType` — last,
  BingX — mark; a separate one (`set_tp_sl`) — by `trigger_by` / the SDK default;
- an entry with an attached SL on the wrong side of the price is rejected by the exchange **entirely** (Bybit 10001,
  OKX 51051, BingX 101400 — `InvalidOrder`), no position; a rejected separate SL (Bybit 10001, OKX 51280, Binance
  -2021, BingX 110411) — reduce-only rollback, `ProtectionNotPlaced(rolled_back=True)`;
- BingX: STOP_MARKET / TAKE_PROFIT_MARKET legs are placed when the entry fills, on the whole size —
  `attached_sl('bingx')` is verified (`verified=True`); OKX: the exchange keeps a TP+SL from `attachAlgoOrds` as an
  `oco` order (SL only — `conditional`) — the SDK requests the TP/SL list as `conditional,oco`; KuCoin — per the docs
  spec only (no demo, `verified=False`). A partial fill of an attached entry can't be reproduced on demo (market fills
  entirely).

With an intent store (`intents=`), the stop plan — SL and TP prices, `trigger_by`, side, mode
`attached` / `separate`, the stop's client_id (`stop_client_id`: OKX `attachAlgoClOrdId`, for separate —
pre-generated and sent as is; Bybit trading-stop and an OKX TP+SL pair — `None`) — is
written into the entry intent (`Intent.tp_sl`) **before sending** in both modes; the entry is marked
protected once the stop is placed (or there is no position). If the process is killed at any point, `recover()` completes
the protection without duplicates (below).

`open_position` opens only a **new** position: if one already exists on the symbol (in hedge mode — on this
side), `errors.PositionOpenError`. With a position journal — roles `entry` / `sl` / `tp` / `close` and
`position_id=`. Works on the exchange and paper; the `bytx.risk_sizing` module is imported on first call.

## Restart recovery and strategies

```python
from bytx.recovery import IntentStore
from bytx.strategy import Strategy, run_strategy

ex = create_exchange('okx', ..., client_id_prefix='bot42', intents=IntentStore('bot42.sqlite'))
report = await ex.recover(resubmit=False)        # at process start, before any new orders
report.adopted, report.finished, report.abandoned, report.unverified, report.live
report.orphans_canceled, report.orphaned, report.orphan_reasons   # orphaned TP/SL: cancelled / kept and why
report.algos     # ex.algos algorithms that died with the process: canceled / finished / kept + reasons / filled / closed
report.algos[0].protection, report.algos[0].protected   # bracket: plan protection result per leg, placed legs
report.ok            # all intents reconciled — and only that: ok does NOT show a position without a stop
report.unprotected   # ⚠️ algorithms whose position may be left without a stop (the caller decides) — check with ok
report.stale_plans   # entries whose TP/SL plan was closed instead of applied ('superseded' / 'expired')
report.deferred      # open entries with a trigger_by plan and no position yet — protection after the fill
```

> ⚠️ **A naked position shows up in `report.unprotected`, not in `report.ok`.** `ok=True` only means "intents
> reconciled": a bracket whose price passed the SL (`price_passed`), `protect=False` or an exchange rejection leaves
> the position without a stop even with `ok=True`.

`IntentStore` (stdlib `sqlite3`, no dependencies) writes every order **before sending** (`pending`, last
before the network — after `pre_submit` and the journal), then its outcome: `placed` / `final` / `rejected` (explicit rejection) /
`unknown`; the outcome is written after the journal. TP/SL legs with a client_id too, linked to the parent entry (Bybit trading-stop without
client_id — not). Cancels via the SDK and `OrderTracker` close intents. `durable=True` (default) —
`synchronous=FULL`; `durable=False` — faster, survives a killed process but not a power loss. One
file serves several accounts: namespace = exchange, market, live/demo/paper, `account=` and the API key fingerprint
(a hash, no secrets; paper without a key — `paper`): `AccountPool(intents=store)` and a shared file of different keys
do not mix intents — `recover()` of account A does not touch B. Schema 5: rows of schemas 1–4 (no fingerprint) are
moved automatically on first access when `account=` is set; without `account=` `recover()` leaves them alone — they
are in `report.unverified` with `ConfigError` and a WARNING, move yours with `store.claim_legacy(ex)`. After an API key
change the namespace is new — `store.claim_legacy(ex, namespace=<previous>)` moves the previous intents. One process
per store file: `IntentStore` holds a file lock `<db>.lock` (POSIX), a second process gets `ConfigError` on open
(several `IntentStore` objects on one file within a process are fine). Deploy **stop-first**; parallel processes —
different files; `recover()` — at start, before new orders.

`ex.recover()` reconciles open intents with the exchange by id / client_id / open orders:

| Order on the exchange | Outcome |
|---|---|
| found, live | `adopted` (the position journal gets the intent's role and position) |
| found, finished | `finished` |
| confirmed absent | `resubmit=True` or `callable(intent)` and not older than `max_age` — recreated **with the same client_id** (`resubmitted`); otherwise `abandoned` |

> ⚠️ Exchanges reject a duplicate client_id only among **open** orders, and lookup by client_id finds
> finished orders only for a limited time (OKX — cancelled unfilled orders for about 2 h, Binance — 3 days).
> So `max_age=None` (no limit) is dangerous: a long-filled-and-gone order could be recreated.
> Keep `max_age` (default 300 s). The same client_id with a different request while the intent is open —
> `ConfigError` before sending; if the intent is closed, it is overwritten with the new request as a whole — with
> the same request too: the previous entry's TP/SL plan, `protected` and `filled` are not carried over, and its legs do
> not cover the new entry.
| could not verify | `unverified`: nothing is sent, the intent stays open |

Fresh intents wait `settle` seconds (exchange lookup may lag). An entry with a protection plan whose TP/SL
were never recorded (crash between entry and protection: `place_order` Binance/HL, `open_position` in both
modes) gets the missing legs (`protect=True`, reduce-only) — but first `recover()` re-reads the
exchange's open TP/SL for the symbol and side: if a stop is already there (attached by the exchange to an attached entry,
placed before the crash), no second one is placed (`find_leg`: client_id, otherwise kind + side + price at step).
TP/SL list not readable — `unverified`, nothing is placed (an extra stop and a position without a stop are equally
bad — retry `recover()`); an attached entry still filling — also `unverified`; a limit entry on the book with a
`trigger_by` plan (legs go on the position) and no position yet — `report.deferred` (does not affect `ok`), the entry
is not marked protected, `recover()` places the legs after the fill. The entry is marked
protected only when all legs are placed. An entry plan is closed (schema 5, `Intent.plan_retired`) and not applied
when the protection failure was reported to the caller (`ProtectionNotPlaced` from `place_order` / `open_position` —
'protection_failed'); when there is a newer entry of the same side of the symbol ('superseded'); when the plan is
older than `protect_max_age` (a day by default) since the last movement of the entry (fill / order update from the
exchange), the side's position exists and legs are missing ('expired'). The last two go to `report.stale_plans` with
the reason and a WARNING; the position's protection is the caller's decision. Your own entry with protection after it —
`with ex.intents.protection({'stop_loss': ..., 'take_profit': ...}):` around `place_order`. A failure inside
`recover()` doesn't multiply legs: before recreating a leg it checks whether a leg of the same kind already exists for the same
entry. A TP/SL leg stores `trigger_by` (`Intent.trigger_by`) and is recreated with the same trigger type.

**Orphaned TP/SL** — live legs with no position on their side. `recover(cancel_orphaned=True)` (the
default) cancels them only if they are **yours** (intent in the store or a `client_id_prefix` tag;
foreign, manual and client_id-less ones — never) and there is definitely no position for the symbol and side: positions are re-read
from the exchange right before cancelling, a read failure — not cancelled; an entry on the same side still open (Binance
legs are placed before the fill) — not cancelled. Cancelling goes through `cancel_tp_sl` (Binance/OKX algo,
KuCoin stop orders); one leg failing doesn't break `recover()`. Cancelled — `report.orphans_canceled`
(intent `final` flagged `orphan_canceled`, a `canceled` event in the position journal), kept —
`report.orphaned`, reason — `report.orphan_reasons[id]`. Candidates are searched across the symbols of all intents
in the store and `symbols=`.

**Execution algorithms after a restart** (`ex.algos`: chase, twap, iceberg, bracket, trailing, capped
market) don't resume — their progress lived in process memory. With a store, an algorithm's start is recorded before
its first child order (the `algos` table: id, kind, symbol, side, size; `running`), its end — `finished`
(the algorithm cancelled its working orders itself) or `unclean` (a cancel failed, a child's outcome is unknown); the
intent of every child order and TP/SL leg carries `algo_id` (there is no marker in the client_id: the client_id format
stays unchanged, and Bybit/BingX legs have no client_id at all). `recover(cancel_orphaned_algos=True)` (the default)
handles each unfinished algorithm (`running` / `unclean`, not running in this same process):

| Algorithm order | What `recover()` does |
|---|---|
| working (a regular order: chase limit order, twap / iceberg part, bracket entry, IOC), live | cancels it via `cancel_order`; the filled part stays as the position (the order's `filled` and `report.algos[i].filled`) — `canceled` |
| working, filled / cancelled while the process was down (or filled between reconciliation and the cancel) | `finished` |
| working, never reached the exchange | `abandoned`, **not recreated** even with `resubmit=True` (nobody would manage it) |
| TP/SL leg (bracket legs, trailing SL) | **stays** — position protection (`kept`, reason in `reasons`); without a position the orphan logic cancels it |
| a bracket plan leg that is missing (died between the entry and the legs) | **placed** (`protect=True`) if the plan side's position exists — `protected`, the result per leg in `protection` (below) |

The algorithm record is closed (`recovered`) — a repeated `recover()` doesn't touch it; a working order not verified
(network) or not cancelled — the record stays open, the next `recover()` retries. `cancel_orphaned_algos=False` —
report only (working orders are adopted as before, `kept` with a reason). A bracket without a protection plan (a
schema 3 record, a plan retired by cancelling the algorithm) — an explanation in `notes` (the side's TP/SL are on the
exchange — not touched; none — "the position is unprotected"); with a plan — legs from the plan (below). One process
per store namespace:
`recover()` of another process would cancel the working orders of a live algorithm. Demo 2026-11-04 (`kill -9` of a
child process): Bybit SOL — chase and twap (a limit part on the book) → `recover()` cancelled the order (the exchange
confirms `canceled`), a repeated one — no actions; Binance SOL — bracket: legs kept as protection, the limit entry
cancelled; Bybit XRP — the same with client_id-less legs. Resuming algorithms is not implemented.

**Bracket position protection from the plan.** A bracket writes its protection plan — position side, TP/SL prices,
`trigger_by`, size, client_ids of the recovery legs — to its record (`algos.plan`) **before sending the entry** and
retires the plan itself (`retired`) as soon as the legs are placed (`protected` — from then on the algorithm manages
them), it is cancelled (`cancel()`, before removing the legs) or protection failed (`on_protect_fail`). If the process
is killed (or the task is stopped: `task.cancel`, SIGTERM — the record is then `unclean`, not `finished`) between the
entry fill and the legs, the algorithm does not resume, but `recover(protect=True)` (the default) places the missing
legs after reconciling and cancelling the working entry — position protection only:

- a plan older than `protect_max_age` (a day by default, `None` — no limit) since the last movement of the entry
  (fill / update time of the entry order from the exchange; a limit entry may rest longer than a day) is not applied
  (`expired`): the position may have changed; checked only when the plan side's position exists and legs are missing;
- the entry fill comes from the algorithm's orders; finished ones are re-read from the exchange (a cancel response on
  Bybit / OKX / KuCoin / HL / Bitget has only the id), a read failure — `unverified`; the entry still works
  (`cancel_orphaned_algos=False`, cancel failed) — `unverified`, no legs for a partial size;
- a leg already exists (a live leg intent of the algorithm; on the exchange — by the plan / algorithm leg client_id;
  by kind + side + price — only a leg without a client_id: Bybit trading-stop, BingX, created no earlier than this
  bracket's entry, and one such leg for one bracket per recovery; an OKX pair — both) — a second one is not placed: a
  repeated `recover()` and a `kill -9` inside it do not duplicate legs; foreign legs (another bot, another own bracket
  with the same prices) are not counted and not adopted;
- no position on the plan's side — nothing (`no_position`); only the other side is open — nothing, a warning
  (`opposite_side`; in hedge — a separate position, not touched);
- size — filled by the entry (minus what the algorithm closed), no more than the position; below the minimum order —
  `too_small`. "Whole position" TP/SL is not used: Bybit Full would replace a foreign stop of the position, Binance
  `closePosition` is one per side, and such a stop would protect a foreign size;
- the price by the plan's `trigger_by` (mark — position mark / ticker, last — last trade) is already past the trigger
  — the leg would trigger at once or be rejected: not placed (`price_passed`, a warning in the log); **the SDK does not
  close the position** — the caller decides;
- the algorithm's leg triggered — nothing (`triggered`); cancelled or gone from the exchange history — not re-created
  (`canceled`); placement outcome unknown — `unverified`, the next `recover()` checks it.

```python
report = await ex.recover()
for a in report.unprotected:      # the position may have no stop: the plan SL is not placed / no plan — no side SL
    ...   # a.symbol, a.side, a.protection, a.notes: closing the position or placing a stop is the caller's decision
```

The result per leg — `a.protection['stop_loss' | 'take_profit']`: `placed` (in `a.protected` and `report.protected`),
`present`, `triggered`, `canceled`, `no_position`, `opposite_side`, `price_passed`, `too_small`, `expired`, `failed`
(the error — in `report.failed`), `unverified`, `disabled` (`protect=False` — report only). With `failed` and
`unverified` the algorithm record stays open — the next `recover()` retries (no longer than `protect_max_age`); with
the other results the record is closed. Plan legs are independent (no OCO watching): where the exchange does not
cancel the remaining leg when the position closes (Binance), the next `recover()` (orphan logic) or the caller cancels
it.

> Limitations. Legs without a client_id (Bybit trading-stop, BingX, Gate, MEXC, HTX) are recognized by kind + side +
> price; a leg created before this bracket's entry is not counted as its leg. If identical legs (your other bracket
> with the same prices) were placed after the entry, or the exchange gives no leg creation time, a bracket that died
> before its legs will take them as its own (`present`).

Store schema is version 5 (2: `trigger_by` column; 3: `algo_id` column and the `algos` table; 4: `algos.plan`; 5:
`intents.plan_retired` and the key fingerprint in the namespace): a version 1–4 database migrates in place on open
(`ALTER TABLE`, old rows are readable; row namespaces are not renamed — `claim_legacy`); a database newer than the
library — `ConfigError` (an older SDK does not start on a newer schema — by design).

**Schema rollback** (rolling back the SDK version) — one of two: (1) the old process starts with a new empty database
(`recover()` over the previous intents is not needed); (2) stop the process (the `<db>.lock` lock: while it runs the
tool refuses) and run `python tools/intents_downgrade.py bot.sqlite --to 3` (bytx 0.1.5 and earlier): key
fingerprints are stripped from namespaces, `meta.schema` — the target one; a client_id conflict between keys — refused
without changes (`--force` — keep such rows with the fingerprint, the older version will not see them). Version 3 does
not read `algos.plan` (no plan protection). On upgrading again the database migrates anew.
Orders with the bot tag but no intent are `untracked` (role from the journal), exact tag match only. Closed
intents older than `prune_after` are pruned. A test matrix over failure points (before/after the intent, in flight, before
the outcome, before the journal, between entry and TP/SL; `open_position` on 6 exchanges: before/after the plan, entry in flight,
partial fill with and without an attached SL, before the stop, inside `recover()`) is covered by tests: no
duplicates, no losses, no position without a stop. Demo 2026-11-04 with a real `kill -9` of the process (Bybit,
Binance, OKX, BingX — BTC and ETH): request not sent — `abandoned` or recreated with the same client_id (one order);
response lost — adopted without a duplicate; filled or cancelled while down — `finished`; killed between entry and
stop — SL and TP per the plan; `kill -9` inside `recover()` itself — the next one completes without duplicates; a
tagged order without an intent — `untracked`, an untagged one — untouched.

**Strategy** — lifecycle `on_start` / `on_tick` / `on_order` / `on_fill` / `on_position` / `on_timer` /
`on_stop`, subscriptions `symbols × feeds` (`ticker`, `order_book`, `best_bid_ask`, `trades`, `candles`) and
`ctx.subscribe`, timers `ctx.every` / `ctx.after` on the strategy clock (paper/replay — the engine clock),
`ctx.ex`, `ctx.journal`, `ctx.algos`, `ctx.risk_size` / `ctx.open_position`, `ctx.bind(position_id=, role=)`.
Start: `ex.recover()` (if `intents` is set; result in `ctx.recovery`), trackers, subscriptions, `on_start`.
Events are delivered one at a time (callbacks never overlap); order book/ticker ticks are coalesced to the latest; callback
errors go to `result.errors` (`max_errors` — stop); `on_stop` is always called.

After a restart, what's left from the previous process is read in `on_start`: intent orders — `ctx.recovery`
(`live`, `finished` …), positions and open orders — the tracker snapshots `ctx.positions` / `ctx.orders`.
`on_position` / `on_order` report only changes **after** the start: a strategy that learns about a position only from
`on_position` will enter a second time after `kill -9` with an open position (demo 2026-11-04 — that's how example 10
doubled the position before the fix). `on_order` / `on_fill` / `on_position` — only of `Strategy.symbols` and
`ctx.subscribe` subscriptions (the trackers cover the whole account; a strategy without `symbols` gets everything; an
event without a symbol is delivered — it can't be attributed to someone else; a subscription symbol stays the
strategy's own after `handle.stop()` too): a position of another symbol closed on the same account (another bot) used
to reach the strategy as its own close — a second entry (demo Bitget 2026-11-04, BTC with ETH in parallel). In hedge
mode the strategy checks the position side itself (example 10 — long only). Best bid/ask — the `best_bid_ask` stream:
the Binance USDⓈ-M WS ticker (`24hrTicker`) has no bid/ask, only `last`.

```python
class Mine(Strategy):
    symbols, feeds = ('BTC/USDT:USDT',), ('ticker',)

    async def on_start(self, ctx):
        ctx.every(30, self.check)
    async def on_tick(self, ctx, tick): ...

result = await run_strategy(Mine(), ex)                                 # live / demo / paper
result = await ex.replay.run(lambda ex: run_strategy(Mine(), ex))      # replay a recording
```

Example — [`examples/10_strategy_restart.py`](https://github.com/byttrade/bytx/blob/main/examples/10_strategy_restart.py). The `bytx.recovery` and
`bytx.strategy` modules aren't imported until used: the order path without a store costs one check.

## Bitget and Gate (Gate — beta)

Both exchanges are integrated the same way as the six v0.1 exchanges: rules follow the official docs, every
rule is checked against response examples from the docs, ccxt
4.4.65 discrepancies are fixed in `exchanges/<exchange>/ccxt_fixes.py`, exchange code lives in `exchanges/<exchange>/` (tp_sl, matching, fees,
limits, error_codes, journal_origin, history, status, api_key, portfolio, tiers, position_history).

```python
ex = create_exchange('bitget', api_key=…, secret=…, password=…, margin_mode='cross', demo=True)
ex = create_exchange('gate', api_key=…, secret=…, demo=True)            # USDT-M, single mode

from bytx.exchanges.bitget.position_history import closed_positions   # /mix/position/history-position
rows = await closed_positions(ex, 'BTC/USDT:USDT', since=…)              # ClosedPosition: pnl, fees, funding
```

| | Bitget (classic API v2) | Gate (APIv4) |
|---|---|---|
| Size | base; step — `sizeMultiplier` (ccxt used the minimum) | contracts × `quanto_multiplier` |
| Margin mode | in every order (`marginMode`) — without it `ConfigError` | per position |
| TP/SL | `place-tpsl-order`: `pos_loss` / `pos_profit` (whole position), `loss_plan` / `profit_plan` with size; `holdSide` buy/sell in one-way | `price_orders`: `close-<side>-position` / `plan-close-<side>-position`, at market (price 0 + ioc) |
| SL on entry | `presetStopLossPrice` in the order | separately right after entry (with rollback) |
| Order origin | `enterPointSource` WEB/IOS/ANDROID, `tradeSide` burst_* / ADL, `orderSource` | `text`: web/app, liq-*, auto_deleveraging; `is_liq` |
| Errors | codes (`40010` "Request timed out" — outcome unknown) | labels `{"label": …}` (`SERVER_ERROR`, `TOO_BUSY` — unknown) |
| Key check | `authorities` (`wwow` — withdraw), `ips` | `account/detail`: IP and UID; the API doesn't return key permissions |
| Portfolio | `mix/account/bill` (windows ≤ 30 d, 90 d) | `futures/usdt/account_book` |

Fixed relative to ccxt 4.4.65: Bitget — demo without `S`-symbols and with `Content-Type`, size step,
`force` / `tradeSide` case, TP/SL `holdSide`, WS order book by `seq`/`pseq` (the docs have no checksum — ccxt
failed with ChecksumError), demo trades; Gate — TestNet hosts (ccxt got 502), TestNet markets, contract fee
rates (ccxt divided by 100), the `t-` prefix on `text`. Emulators: `fake_exchange('bitget' |
'gate')`. Live check of public functions — `tools/live_public_check.py --exchanges bitget,gate`
(production and demo environments, 160 checks — 0 issues, 2026-10-26).

## MEXC and HTX (beta)

Integrated the same way as Bitget and Gate: rules follow the official docs,
ccxt 4.4.65 discrepancies are fixed in `exchanges/{mexc,htx}/ccxt_fixes.py`, exchange code lives in `exchanges/<exchange>/`.
Both send leverage with every order: `set_leverage` / `set_margin_mode` remember it (no request to the
exchange); if unset, MEXC takes it from `position/leverage`, HTX from `swap_cross_account_info` (the HTX
order leverage must match the open position's leverage). No demo — test with `paper=True` or
`fake_exchange('mexc' | 'htx')`.

```python
ex = create_exchange('mexc', api_key=…, secret=…, margin_mode='cross')   # USDT-M; mode sent with every order
ex = create_exchange('htx', api_key=…, secret=…, margin_mode='isolated') # cross / isolated use different endpoints

from bytx.exchanges.mexc.position_history import closed_positions    # history_positions (state 3)
```

| | MEXC (Contract API) | HTX (USDT-M linear swap) |
|---|---|---|
| Size | contracts × `contractSize`, step `volUnit` | contracts × `contract_size`, minimum 1 (ccxt — `contract_size`) |
| Order | `order/create`: `side` 1–4, `type` 1–5, `openType`, `positionMode` explicit | `swap_cross_order` / `swap_order`, `lever_rate` in each |
| TP/SL | `stoporder/place` by `positionId` (`lossTrend` 1/2/3 — last/mark/index) | `swap_cross_tpsl_order`, last only |
| SL on entry | `stopLossPrice` in the order | `sl_trigger_price` in the opening order |
| client_id | `externalOid` ≤ 32 (code 2030) | swap — **integer** 1…2⁶³−1, spot — ≤ 64 |
| Origin | `category` 2/3 — liquidation, 4 — ADL | `order_source` (web/app — UI), `order_type` 3 / 22, `liquidation_type` |
| Key check | doesn't return key permissions (asset read only) | `/v2/user/api-key`: `permission`, `ipAddresses`, UID |
| Portfolio | `account/assets`, `funding_records`, `order_deals` (needs a symbol list) | `swap_financial_record` in 48 h windows, 90 d |
| Status | — (probe `contract/ping`) | `heartbeat/` (`linear_swap_heartbeat`) |
| Tiers / closed positions | from the contract record / `history_positions` | `NotSupported` / `NotSupported` |

HTX: contract client_order_id is an integer, so on swap `client_id_prefix` is 1 to 8 digits, and the new-generation
client_id (tag + role) doesn't fit: with a journal the SDK uses a numeric id and the journal stores the order role
(the `intent` event). The tag is encoded in the 18-digit id as `9`, the tag length and the tag itself
(tag `7` gives `917…`, tag `71` — `9271…`). Untagged ids start with 1–8. So `owns()` and
`cancel_all` of a bot tagged `7` treat neither bot `71`'s orders nor an untagged instance's orders as its own.

Fixed relative to ccxt 4.4.65: MEXC — futures orders per the new docs ("2026-03-31 Futures Trading API
maintenance ended"): path `order/create` instead of `order/submit`, id from `data.orderId`, market — `type 5`,
hedge close sends `side` 4/2 (ccxt opened the opposite position), ccxt's ban on
BTC/ETH/LTC removed (`unavailableContracts`), a contract with `apiAllowed=false` — `NotSupported` before sending, cancel
2040/2041 — `OrderNotFound`, WS `push.deal` with an array of trades; HTX — ccxt's `channel_code` and
`AA03022abc` prefix removed, `reduce_only` not sent in hedge mode, TP/SL on entry, real leverage instead of `lever_rate=1`,
a non-integer client_id is no longer silently dropped. Live check of public functions —
`tools/live_public_check.py --exchanges mexc,htx` (80 checks — 0 issues, 2026-10-26).

MEXC futures over the API depend on exchange policy: order placement was closed from 2022-07-25
to 2026-03-31 (futures API Update log). "Trading unavailable" rejections (6001, 6002, 6005, 511, 1002) map to
`NotSupported`, "key lacks permission" (401, 402, 406, 701–704) to `PermissionDenied`; neither is retried.
On MEXC `check_key()` detects trading permission with a probe — cancelling a known non-existent order
(2040 — permitted, 704 — not); MEXC doesn't return withdrawal permission or the IP list.

## Proxies and multiple accounts

```python
ex = create_exchange('binance', api_key=…, secret=…, proxy='socks5://user:pass@host:1080')
```

One proxy for both REST and WS (`pip install bytx[ws]` for SOCKS over WS). The rate limit is
shared per "exchange + proxy" key, so accounts behind one IP don't get banned.

```python
from bytx import AccountPool

async with AccountPool.from_file('accounts.yaml') as pool:   # keys via ${ENV}
    print(pool.shared_ip_warnings())
    balances = await pool.gather(lambda ex: ex.balance())    # one account's error doesn't affect the rest
```

## Portfolio, key check, exchange health

```python
async with AccountPool.from_file('accounts.yaml') as pool:
    report = await pool.portfolio(since=week_ago_ms)          # until defaults to now
    print(report.total_equity, report.realized_pnl, report.fees, report.funding)
    report.to_csv('portfolio.csv')                            # one row per account and currency
    if not report.complete:
        print(report.issues())                                # why the data is incomplete

    keys = await pool.check_keys()       # {name: KeyInfo | UnsafeApiKey | error}
    health = await pool.health()         # {name: HealthReport | error}
```

- **Portfolio** (`ex.account_report(since=, until=)`, `pool.portfolio(...)`): account equity, unrealized
  position PnL, realized PnL, fees and funding for the period — from each exchange's ledger per its
  docs (Binance income, Bybit transaction-log, OKX bills, BingX income, KuCoin ledger + per-symbol funding,
  Hyperliquid fills + funding). Signs are the same on every exchange: `fees` — paid (+), `funding` —
  received (+) / paid (−), `net_pnl = realized − fees + funding`. If history was cut off (page
  limit, window deeper than the exchange's history), the exchange doesn't return part of the data (KuCoin — account fees:
  `fees=None`) or an account couldn't be read — `complete=False` with the reason in `issues`: a partial sum is never
  passed off as complete. Per-account totals are in USDT/USDC/USD (1:1; BingX demo VST too); other currencies are
  in `totals`. The symbol of a ledger record (`keep_items=True` → `report.items`) is the SDK symbol
  (`'BTC/USDT:USDT'`), not the exchange market id; `account_report(symbols=['BTC/USDT:USDT'])` — a report for these
  symbols (ledger, positions, unrealized PnL), equity — of the whole account. Checked on Bybit / Binance / OKX /
  BingX demo (2026-11-04): equity = the exchange response, realized PnL and fees of own trades = the ledger,
  funding = the exchange ledger and −size × mark × rate.
- **Key check** (`ex.check_key()`, `ex.account_uid()`): trading and withdrawal permissions, transfers, IP
  binding, account type (Bybit unified/classic, OKX acctLv, Binance multi-assets, HL abstraction), position
  mode, UID — from the exchange's key permissions endpoint per its docs. A key with withdrawal rights is rejected by default
  (`UnsafeApiKey`); `withdraw='warn'` — warning only. Whatever the exchange didn't
  report is `None` with the reason in `info.unknown` ("not checked" ≠ "no permission"). If withdrawal permission is
  undetermined (`can_withdraw=None`: MEXC's API doesn't return key permissions, Binance sapi is unavailable), then even
  with `withdraw='reject'` you only get a warning in `info.warnings`, not `UnsafeApiKey`. A service
  that needs a strict check should reject the key itself when `info.can_withdraw is None`.
  Hyperliquid: a wallet key
  can withdraw, an API wallet (agent) cannot.
- **Health** (`ex.health()`): maintenance status from the exchange's public endpoint (Binance, Bybit, OKX,
  KuCoin; BingX and Hyperliquid have no documented status — `unknown`), REST latency and clock
  skew from the server time request, REST and WS stream metrics, rate limiter state. Without keys — public
  only; with keys — also a private probe (balance). `report.healthy` means it's OK to trade. The WS latency
  «exchange → us» is measured by the clock offset at the midpoint of the time request (`TimeSync.clock_offset_ms`),
  not by ccxt `timeDifference` (that one is for signing and is overstated by the response path: the latency came
  out ≈ 0 or below zero). The limiter state is as of the call (`tokens`, `busy_s` — how long the next request would
  wait). OKX: the status is requested at most once per 5 s per IP (a repeat comes from the cache) and leaves no debt
  in the shared IP limiter (health() used to delay the next request from this IP by ~5 s). Clock offset and latency
  come from a single probe: on a cold connection (a new client's first request) both are overstated — look at
  `rest_latency_ms`.

## Data, testing, sync client

```python
h = await ex.history.candles('BTC/USDT:USDT', '1h', since=t0, until=t1)   # [since, until), ms
df = h.to_pandas()                        # or h.to_polars(); any models — bytx.data.to_pandas(rows)
if not h.complete:
    print(h.gaps, h.unavailable)          # gaps in the timeframe grid; what the exchange doesn't serve and why
funding = await ex.history.funding('BTC/USDT:USDT', since=t0)
trades = await ex.history.trades('BTC/USDT:USDT', since=t1 - 3_600_000)   # Binance, OKX
```

- **History loader** (`ex.history`, `bytx.data.HistoryLoader(ex, cache_dir=, fmt=)`): candles,
  public trades, funding with an on-disk cache (`~/.cache/bytx/history`, `$BYTX_CACHE_DIR`;
  parquet if pyarrow is installed, otherwise csv). Repeat requests are served from the cache, only gaps are
  downloaded (`refresh=True` re-downloads). Only final data is cached: closed candles,
  trades older than a minute. Demo and live are stored separately. Page size and history depth come from each
  exchange's docs (`exchanges/<exchange>/history.py`): Binance 1500 candles (spot 1000), Bybit 1000, OKX 300,
  BingX 1440, KuCoin 500 (spot 1500), Hyperliquid 500 and only the last 5000 candles. Trade history
  exists only on Binance (aggTrades, 48 hours) and OKX (3 months); on Bybit, BingX, KuCoin and Hyperliquid
  REST returns only recent trades, hence `NotSupported` with a docs link. Anything deeper than the exchange's
  history goes into `unavailable` rather than a silently truncated series.
- **pandas / polars** — extras: `pip install 'bytx[pandas]'`, `[polars]`, `[parquet]`, `[data]`
  (pandas + pyarrow). The SDK core works without them.
- **`bytx.testing`** — emulators for testing your strategies, no network or keys:
  `fake_exchange('bybit', price=50_000)` returns a regular `Exchange` for any of the 10 exchanges on market snapshots from
  the docs (step, minimums and contractSize as on the exchange, the SDK's TP/SL path) plus a `FakeVenue` emulator.
  `paper_exchange('okx', bids=..., asks=...)` returns a paper account on a virtual clock; set the market with
  `set_book` / `add_trade`, advance time with `drive`. The pytest plugin is enabled with
  `pytest_plugins = ['bytx.testing.pytest_plugin']` (fixtures `fake_exchange`, `paper_exchange`,
  `bytx_clock`). A non-emulated request raises `NetworkDisabled` instead of hitting the exchange.
- **`bytx.sync`** — a blocking client for scripts and Jupyter:
  `with bytx.sync.create_exchange('okx', demo=True, ...) as ex: ex.buy(...)`. The client runs its own event loop
  in a separate thread, so it works inside an already running loop too. A WS stream reads like a regular
  iterator (`for book in ex.stream.order_book(s)`). Calling from the client's own callback raises
  `RuntimeError` immediately instead of deadlocking. `close()` / `with` close the Exchange and stop the thread.
  `call_timeout` cancels the call; the order may still have reached the exchange, so reconcile by `client_id`.

## Integrations: TradingView, Telegram/Discord, Prometheus, OpenTelemetry

All integrations are off by default, and the core doesn't import them. Notifications and the aiohttp webhook server
run on the core's aiohttp. Everything else needs extras: `[prometheus]`, `[otel]`, `[fastapi]`, or all of them
via `[integrations]`.

```python
ex = create_exchange('bybit', demo=True, api_key=..., secret=..., client_id_prefix='tv1')

# Exchange events: the basis of all integrations
unsubscribe = ex.subscribe(print, kinds=['order', 'protection', 'stream'])

# TradingView webhook: secret required; without allow_live=True accepts demo/paper only
hook = TradingViewWebhook(ex, secret=os.environ['BYTX_TV_SECRET'])
runner = await hook.serve(port=8080)       # 443 outside (nginx/Caddy); FastAPI: hook.fastapi_router()

# notifications: rejections, unknown outcomes, TP/SL not placed, WS drops, webhook errors, interventions
notifier = Notifier(TelegramChannel.from_env(), DiscordChannel.from_env())
notifier.watch(ex)
notifier.watch_journal(journal)

PrometheusCollector(ex)                    # + prometheus_client.start_http_server(9108)
instrument(ex)                             # OpenTelemetry spans
```

- **Events** (`ex.subscribe(callback, kinds=)` → unsubscribe function): `order.placed`, `order.rejected`,
  `order.unknown`, `order.canceled`, `tp_sl.placed`, `protection.failed`, `stream.state`, `error`.
  `kinds` accepts kinds or groups (`'order'`, `'stream'`); a typo in a kind raises `ConfigError`. The callback
  can be sync or async. Its exception is logged and doesn't break trading. The event carries the request
  without `params`, i.e. without signatures and broker fields. With no listeners the cost is a single empty-list
  check. `journal.subscribe(cb)` delivers every new position journal event, including
  interventions. `AccountPool` sets `ex.account` (the account name).
- **TradingView webhook** (`bytx.integrations.tradingview`). Put JSON in the alert's Message field:
  `{"secret": "…", "id": "{{strategy.order.id}}-{{timenow}}", "time": "{{timenow}}", "action": "open",
  "symbol": "{{ticker}}", "side": "long", "risk_usdt": 50, "stop": 61000}`. Actions:
  - `open` opens a position via `open_position` (by `risk_usdt` and `stop`) or a regular order (by
    `size` / `notional`);
  - `close` closes the position via `close_position`;
  - `buy` / `sell` map to `{{strategy.order.action}}`; with `market_position: "flat"` the position
    is closed;
  - `market` / `chase` / `twap` / `iceberg` / `bracket` start `ex.algos` algorithms;
  - `cancel_all` cancels only your own orders, so `client_id_prefix` is required.

  Unknown fields are rejected. Rules follow the TradingView docs. An alert whose message isn't valid
  JSON arrives as `text/plain` and is rejected. TradingView cancels requests longer than 3 s, so the webhook
  answers `202` immediately and executes in the background (`wait=True` waits for the result). Ports 80 and 443 only.
  Sender IPs are in `TRADINGVIEW_IPS` (`allowed_ips=`). Protection:
  - the secret is required, at least 16 characters (`BYTX_TV_SECRET`), compared in constant time and never
    appears in responses or logs;
  - a live account without `allow_live=True` raises `ConfigError`;
  - a repeated `id` is not executed twice: the duplicate returns the previous result. `SqliteAlertStore(path)`
    keeps ids across restarts;
  - rate limit (`max_per_minute`, `burst`) — two budgets. Failed requests (wrong secret, not JSON,
    body too large) spend their sender address's budget, checked before the secret, so
    secret brute-forcing hits 429. Alerts with the correct secret count against the shared budget, so a flood without
    the secret from another address doesn't block a real alert (including `close`). Behind a reverse proxy you need
    `trust_forwarded=True` (otherwise every sender has the proxy's address) or `allowed_ips=TRADINGVIEW_IPS`;
  - `allowed_symbols` and `allowed_actions` are supported. An alert whose `time` is older than `max_age` (default
    120 s) is rejected.

  An execution error arrives as an `error` event with `source='tradingview'`, and Notifier delivers it.
- **Notifications** (`bytx.integrations.notify`).
  - Channels. `TelegramChannel`: `sendMessage`, up to 4096 characters, on 429 waits `retry_after`, at most
    1 message per second per chat (per the Telegram FAQ). `DiscordChannel`: channel webhook, up to 2000 characters, no
    mentions, on 429 waits `retry_after` / `Retry-After`, honors `X-RateLimit-Remaining` /
    `Reset-After`.
  - What is sent. `watch(ex)` sends only problems by default. Every order is sent with
    `kinds=['order', …]`. `watch_journal` sends fills, closes, liquidations, interventions and
    discrepancies.
  - Throttling: an identical event (kind, exchange, account, symbol) is sent at most once per `throttle` seconds
    (default 60). Repeats arrive as a single "N more" summary.
  - Secrets. Text is scrubbed before sending: keys of watched exchanges, channel tokens, your own
    `redact=` values, and patterns (bot token, webhook URL, `signature=` / `apiKey=`, private
    key). The send error log is scrubbed the same way.
  - The listener only enqueues text to the channel, so trading never waits on the network.
- **Prometheus** (`PrometheusCollector(*exchanges, pool=)`): values are read at scrape time from SDK
  accumulators: REST requests, errors and retries, per-endpoint latency (p50/p90/p99/max), exchange rate limit
  headers, clock offset, the shared limiter (tokens, queue, lockout after 429/418), WS (messages,
  reconnects, errors, silence, drops, connected or not, latency), `ws_*` order latency and
  the order event counter. Labels: `exchange`, `account`, `mode` (live/demo/paper). Scraping doesn't create
  a WS hub or touch the hot path. Serve your own `/metrics` via `metrics_text()`.
- **OpenTelemetry** (`instrument(ex)` → `undo`): spans `bytx.order.place`, `bytx.order.cancel`,
  `bytx.ws.order`, `bytx.rest` (nested in the order span; `url.path` without query) and
  `bytx.ws.reconnect` (from drop to `connected`). Wrappers are installed per instance, and without `instrument`
  the code has no tracing checks at all. Error text goes through `redact`.

## Concurrency and raw ccxt access

- `Exchange` methods don't mutate `exchange.options`. Modes (createOrder, market type, etc.) are sent as
  request parameters, so concurrent calls on one client don't interfere. This is
  pinned by tests. The exception is ccxt's own lazy data
  caches: Binance `options['leverageBrackets']`.
- One ccxt instance per `Exchange` (`ex.ccxt`, with SDK patches). In an `AccountPool` accounts share only markets (by reference from
  the cache). Options, sessions and keys are per account.
- Don't serialize `Exchange` calls with your own reentrant lock: `place_orders`, `cancel_all` and
  `close_all` use `asyncio.gather` internally, child tasks don't own the lock — deadlock.
- `ex.name` is the stable SDK name: binance, bybit, okx, bingx, hyperliquid, kucoin, bitget, gate,
  mexc, htx.
- `ex.market(symbol)` exposes `settle` and `raw` (the ccxt market dict); `ex.candles(...)` and
  `ex.leverage_tiers(symbol)` work without keys. `ex.close()` is idempotent and stops
  WS streams and background time sync. Don't call `ex.ccxt.close()` directly: background tasks would be left
  hanging.

## Examples

[`examples/`](https://github.com/byttrade/bytx/tree/main/examples): trading basics ([`00_trading_basics.py`](https://github.com/byttrade/bytx/blob/main/examples/00_trading_basics.py) — authentication, spot and futures, limit orders: place / amend / cancel, reducing a position), market data and order book, backtest, arbitrage scanner, grid, DCA, Telegram alerts,
scalper skeleton, multi-account, a strategy with restart recovery. Trading
examples run on demo by default. Prefer no code? The same bots and backtests run at [byttrade.com](https://byttrade.com).

## Support the project

BYTtrade SDK is free to use. The best thank-you is simply to use it: the SDK takes part in
exchange partner programs, and a share of the fee the exchange charges anyway goes to the project.
Nothing changes for you — neither fees nor execution (many other trading libraries
work the same way). Thank you for helping the project grow!

<details>
<summary>Details</summary>

The SDK does not send ccxt's built-in broker tags. If the partner tag doesn't suit you, you can
turn it off: `BrokerConfig.disabled()` in `set_broker_config(...)` / `create_exchange(..., broker=...)`
or `BYTX_BROKER_MODE=off` in the environment.

- **Hyperliquid builder fee** — only explicitly (`BrokerConfig(hl_builder_address=…)`) and only
  after the user approves it: `await ex.builder_fee_status()`, `await ex.check_builder_fee(require=True)`
  (`HyperliquidBuilderNotApprovedError` / `HyperliquidBuilderCheckUnavailableError`),
  `ex.on_builder_fallback = callback` (HL rejected due to the builder — retried without it),
  `ex.builder_approval_request()` — EIP-712 payload to sign with the main wallet.
- **HTX** — the docs have no broker tag: ccxt's undocumented `channel_code` in contract
  orders and the same spot client-order-id prefix are removed.

</details>

## client_id

The SDK never replaces the caller's client_id: an invalid exchange format raises `InvalidOrder` before sending.
Without your own id the SDK generates one (with the `client_id_prefix` tag, if set).

- `bytx.max_client_id_len(ex)` — the maximum id length (Binance — 36 minus the broker prefix
  `x-<ID>` the transport adds). Longer ids are sent without the prefix, i.e. without broker
  attribution (WARNING, `binance_unattributed_counts()`); `create_exchange(..., client_id_strict=True)`
  makes this an `InvalidOrder`.
- `bytx.generate_client_id(ex, prefix='d0123…')` — an id in the exchange's format and length.
- `bytx.owns(order, prefix, exchange)` — whether the order is yours (Hyperliquid and BingX — case-insensitive).
  `Order.client_id` is logical: the Binance broker prefix is stripped.
- `set_tp_sl(..., take_profit_client_id=, stop_loss_client_id=)` — a separate id per leg. On Bybit
  TP/SL are set on the position without a client_id — orders have `has_client_id=False` (same on Gate:
  price_orders have no user id; on BingX: "clientOrderId only supports LIMIT/MARKET order
  type" — TAKE_PROFIT_MARKET / STOP_MARKET are sent without `clientOrderID`, the leg is identified by the id from the response,
  the journal by `tp_sl_set`).
- Gate: client_id is the `text` field, "prefixed with `t-`, no longer than 28 bytes without `t-`": SDK ids
  are always `t-…` (`generate_client_id`, new-generation client_id, `owns` ignores `t-`); an id without the
  prefix — `InvalidOrder`. Bitget: `clientOid` up to 50 characters (codes 40304/40305). MEXC: `externalOid` up to 32
  (code 2030). HTX swap: `client_order_id` is an integer 1…9223372036854775807 (code 1065), spot — up to 64 characters.
- `create_exchange(..., pre_submit=hook)` — `hook(request)` (may be async) runs before each placement
  request with the final `client_id`: persist the intent before sending. An exception in the hook cancels the request.

## Position journal

Exchanges don't know about "bot positions": entry, add-on, TP/SL and close orders live on their own.
`bytx.journal` links them to a position and stores the event history — append-only, no
overwrites. The journal is **opt-in**: without it Exchange and trackers work as before.

```python
from bytx.journal import PositionJournal

journal = PositionJournal(bot_id=bot_id)            # in-memory store by default
ex = create_exchange('okx', ..., margin_mode='cross', journal=journal)

with journal.bind(position_id=cycle_id, role='entry'):
    await ex.buy('BTC/USDT', margin=50, leverage=5)
with journal.bind(position_id=cycle_id, role='tp', level_id=1):
    await ex.place_tp_sl('BTC/USDT', 'take_profit', 72_000, position_side='long', size=0.001)

async with ex.order_tracker('BTC/USDT'), ex.position_tracker('BTC/USDT'):
    ...                                              # fills and position changes go to the journal too

for e in await journal.read(cycle_id):               # ascending by seq
    print(e.seq, e.event, e.role, e.origin, e.exchange_order_id)
record = await journal.summary(cycle_id)             # orders with roles and statuses, fees, funding
```

**What is recorded.** `intent` — before sending (after the `pre_submit` hook): the client_id → position →
role link survives a process crash. Then `placed` / `filled` / `rejected` (on an unknown outcome
`intent` remains); `replaced` — `amend_order`, `modify_tp_sl` (`replaces` field); `canceled` —
`cancel_order`, `cancel_orders`, `cancel_all`; TP/SL — with role `tp`/`sl` and level.
`OrderTracker` — `placed` / `partially_filled` / `filled` / `canceled` / `expired`;
`PositionTracker` — `position_opened` / `increased` / `reduced` / `closed`. Funding and liquidation —
`journal.record_funding(...)`, `journal.record_liquidation(...)`.

**Event** (`JournalEvent`, alias `JournalEntry`): `_v`, `seq`, `ts`, `bot_id`, `position_id`,
`event`, `origin`, `role`, `level_id`, `exchange`, `symbol`, `position_side`, `exchange_order_id`,
`client_order_id`, `trade_id`, `replaces`, `side`, `price`, `amount`, `filled`, `contracts_after`,
`fee`, `fee_currency`, `confidence`, `note`, `kind`, `order_type`, `trigger_price`, `reduce_only`,
`key`, `raw`. Sizes are in the base coin.
Serialization — `to_dict()` / `from_dict()` (unknown keys are skipped).

**Position** of an order: `bind(position_id=...)` (cycle uuid), otherwise the position open in the journal for
`(exchange, symbol, side)`, otherwise a new key `account:exchange:symbol:side:ts`.

**Origin** (`origin`) — deterministic, `classify_origin` / `journal.classify_order`:
`own` — a new-generation bot tag, a recorded `intent` or a known order id; `bot_legacy` —
the old bot cid format (tag + hex; algo TP/SL `t|s…` — with `confidence='heuristic'`); `platform` —
your application's cid formats from `platform_formats=` (regular expressions);
then exchange API markers per its docs (`exchanges/<exchange>/journal_origin.py`): liquidation, ADL,
settlement, exchange UI (`exchange_ui`: Bybit `createType` "web/app", Hyperliquid `FrontendMarket`),
exchange bots, TP/SL child order (parent's origin); a foreign cid — `foreign_api` (on Binance —
`unknown`: the exchange generates the cid itself); no cid — `manual` (`heuristic`). `confidence`: `exact` —
an exchange marker or own cid, `heuristic` — inferred without a direct marker.

**Intervention** — an `intervention` event (`kind`) and the `on_intervention=` callback:
`foreign_order` (not our order in the position: manual add/reduce, third-party software),
`liquidation` / `adl` / `settlement`, `external_cancel` / `external_amend` (our order cancelled /
amended outside the SDK — seen via WS; cancels and amends through Exchange are marked before the request),
`position_drift` — `await journal.reconcile(position_id, size_on_exchange)`.
Bybit position TP/SL (`/v5/position/trading-stop`) don't accept a client_id: the journal records `tp_sl_set`
(role, side, trigger) before the request and treats as its own a conditional order without `orderLinkId` (stopOrderType
`TakeProfit` / `StopLoss` / `PartialTakeProfit` / `PartialStopLoss`) with the same symbol, side and
trigger at the price step (`confidence='heuristic'`); TP/SL with a different price — `foreign_order`.

```python
journal = PositionJournal(bot_id=bot_id, on_intervention=lambda e: alert(e.kind, e.origin, e.note))
rec = await journal.summary(cycle_id)
rec.orders_by_outcome()      # {'executed': [...], 'canceled': [...], 'open': [...], 'rejected': [...]}
rec.intervention_events      # interventions; OrderSummary.canceled_by — who cancelled
```

**Recovery from exchange history** — `await journal.recover(ex, 'BTC/USDT', 'long', since=ms)`:
a position episode "from zero to zero" from fills, orders (filled / cancelled / open), funding,
liquidations; anything not ours — `intervention`; incomplete history — `incomplete`. Idempotent, doesn't
duplicate the live journal. Source — `HistorySource` (default `ExchangeHistorySource`).

**Idempotency.** The event key is `(bot_id, exchange_order_id, event, trade_id)`; without
`trade_id` on a partial fill — the cumulative size. WS repeats, tracker REST reconciliation and
restarts don't duplicate events.

**Store** — the `JournalStore` protocol (async `append`, `read`, `find_order`, `positions`):
`append` is idempotent by `key` and returns `AppendResult(event, created)`; `seq` is assigned by the
store (strictly increasing within `(bot_id, position_id)`). Built-in — `InMemoryJournalStore`.
Example Mongo adapter (collection `position_journal`): document = `event.to_dict()`, indexes —
unique `key` and unique `(bot_id, position_id, seq)`; `seq` = last + 1, on
DuplicateKey on `seq` — retry, on `key` — return the existing document (`created=False`).
`strict=True` — a store failure while writing `intent` cancels sending the order (default —
WARNING in the log, the order goes out).

**New-generation client_id** — `{tag}{role}{tail}`, ≤ 24 characters (with the Binance broker prefix
`x-<ID>` — up to 36). The role is a non-hex character: `n` entry, `g` dca, `p` tp, `q` sl
(+ level, 2 base36 chars), `r` tp_reduce, `x` close. With a journal the SDK generates such an id itself
when the role is known (`bind(role=...)`, close, TP/SL with `level_id`); the caller's id is never changed.
Old ids are recognized: tag + hex — `legacy` (role from the journal), `t|s{level}{hex}` —
`legacy_algo`. Hyperliquid: the cloid is 128-bit hex, the role doesn't fit and lives only in the journal.

```python
from bytx.journal import bot_tag, make_client_id, parse_client_id

cid = make_client_id(ex, bot_tag(bot_id), 'tp', level_id=2)
parse_client_id(cid)    # ClientIdInfo(generation='v2', bot_tag='d…', role='tp', level_id=2)
```

## Quality

- Responses and requests are verified against **official exchange documentation**: response examples from the docs are
  reference fixtures, each with a source link. ccxt bugs
  found this way are fixed in the SDK (`bytx.exchanges.<exchange>.ccxt_fixes`).
- every release passes ~2800 offline tests, ruff and mypy; gitleaks in CI.
- ccxt is pinned to a verified version (`4.4.65`).

## Errors

```python
from bytx import errors

try:
    await ex.buy('BTC/USDT', size=0.0000001)
except errors.OrderTooSmall:
    ...
except errors.BytxError as e:
    if e.retryable:          # network, rate limit, exchange unavailable
        ...
```

All SDK errors subclass `errors.BytxError`. Each has machine-readable attributes:

- `e.code` — the exchange code as a string (`'-2019'`, `'110072'`, OKX — the item's `sCode`), if any;
- `e.error_code` — the unified `errors.ErrorCode`;
- `e.is_business` — a substantive rejection (funds, parameters, symbol, key, no position) rather than an
  infrastructure failure: a circuit breaker should not trip on these.

Error text never contains keys. ccxt includes the request URL in network error text, and on HTX the query string
carries `AccessKeyId` and `Signature`. So `errors.from_ccxt` runs the text through `errors.scrub`:
it removes the instance's exact key values, URL query strings with a signature or key (the path stays) and
secret parameters (`signature=`, `apiKey:` …). The original exception in `__cause__` is scrubbed too. As a
result the key appears neither in `str(e)`, nor in core logs, nor in `health().issues`, nor in TradingView webhook responses and logs.

SDK errors also subclass the ccxt classes of the same name
(`errors.InsufficientFunds` ⊂ `ccxt.InsufficientFunds`, `errors.RateLimited` ⊂
`ccxt.RateLimitExceeded`, …), so existing `except ccxt.…` clauses keep working.
Exceptions: `OrderStatusUnknown`, `BatchError`, `ProtectionNotPlaced`, `ConfigError` are
neither ccxt nor `NetworkError`, so a broad `except` won't mistake them for a retryable failure.

### Unknown order outcome

`errors.OrderStatusUnknown` means: the placement request was sent, and there was no response or it was
ambiguous (timeout, -1006/-1007, "server timeout", duplicate client_id), and reconciliation by client_id
didn't find the order or couldn't check. **The order may have been placed.** Fields: `client_order_id`,
`exchange_order_id` (if known), `symbol`, `side`, `amount`; `retryable=False`.

How to handle it:

1. **Do not retry the placement** — neither with the same nor a new client_id: that is exactly a double entry.
2. Check the position: `await ex.position(e.symbol)`.
3. Find the order by client_id: `await ex.find_order(e.symbol, e.client_order_id, timeout=8)`: an order — account for it; `None` — not found
   within the window (not proof: the engine may accept it later); `RequestTimeout` — couldn't check,
   retry reconciliation later.
4. Decide on a new order only after reconciliation, and with the same client_id: the exchange will reject a duplicate.

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)   # None — not found within the window
```

Time budget: `with ex.deadline(15): ...` bounds the whole call (requests, read retries,
reconciliation). `create_exchange(retries=0)` — no read retries (for services with their own
circuit breaker). `errors.RateLimited.retry_after` — the pause from the `Retry-After` header.

## Development

```bash
git clone https://github.com/byttrade/bytx && cd bytx
python -m venv .venv && .venv/bin/pip install -e ".[dev,ws,yaml]"
.venv/bin/ruff check src examples && .venv/bin/mypy
```

Live check of public functions (no keys, needs network): `python tools/live_public_check.py --exchanges okx,bybit`.

## License

bytx is free to use under the [BYTtrade Source-Available License](https://github.com/byttrade/bytx/blob/main/LICENSE): install it and use it in
your own scripts, bots and applications, including commercially. Copying its code into other libraries,
publishing modified versions and offering it as a hosted service are not permitted.
Uses ccxt (MIT), see [`NOTICE`](https://github.com/byttrade/bytx/blob/main/NOTICE).
Not financial advice; trading cryptocurrency carries a risk of loss.
