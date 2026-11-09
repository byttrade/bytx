# bytx — by [BYTtrade](https://byttrade.com)

[English](../README.md) | [Русский](README.ru.md) | [中文](README.zh-CN.md) | [Español](README.es.md) | [Português](README.pt-BR.md) | **日本語** | [한국어](README.ko.md) | [Türkçe](README.tr.md)

暗号資産取引所向けの、型付き **async** Python SDK です。対応取引所は Binance、Bybit、OKX、BingX、
Hyperliquid、KuCoin、Bitget、ベータ版として Gate、MEXC、HTX、BloFin（USDT-M 先物）。現物と無期限先物をひとつのインターフェースで扱え、
スマートな再接続を備えた WebSocket、アカウントごとのプロキシ、スキャルピング向けのツールを提供します。

> 完全なドキュメント: [docs/GUIDE.md](GUIDE.md)(英語)


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## インストール

```bash
pip install bytx            # + extras: [ws] [fast]
```

## クイックスタート

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT、デフォルトはスワップ
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # サイズは BTC 単位(コントラクト数ではない)
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` は取引所のデモ/テストネットを使用します。実際の資金は動きません。

> ノーコードのボット、バックテスト、Telegram での取引承認は
> [byttrade.com](https://byttrade.com) で。

## 素の ccxt ではなく bytx を使う理由

bytx は [ccxt](https://github.com/ccxt/ccxt) の上に構築され、ccxt を「格闘せずに済む」トレーディング SDK に変えます。
同じ処理が **1 ページではなく 1 行** で済み、本番環境でお金を失う原因になるエッジケースは SDK が処理します。

**OKX の無期限先物で BTC を 100 USDT 分買う**

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

**ポジションへの利確・損切り (TP/SL)** — ccxt では取引所ごとにまったく違います(Bybit は
`privatePostV5PositionTradingStop`、Binance は `closePosition` 付きの条件付き注文 2 本、OKX はアルゴ注文…)。
bytx ならどの取引所でも同じ呼び出しです:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**切断されても生き続けるリアルタイムのオーダーブック** — ccxt では再接続ループ、backoff、再購読、欠落検知、
スナップショットからの再同期をすべて自分で書くことになります。bytx なら:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**注文発注時のタイムアウト** — ccxt ではタイムアウトすると注文が生きているのか分からなくなります
(リトライすれば二重エントリー)。bytx は `client_id` で照合し、何が起きたかを正確に教えてくれます:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**その他、すぐに使える機能**

| 素の ccxt | bytx |
|---|---|
| `amount` はコントラクト単位で、取引所ごとに `contractSize` が異なる | `size=` はベース通貨、`notional=` は USDT、または `contracts=`。換算と丸め(切り捨て)を内蔵 |
| インスタンスごとに `load_markets`、メモリ使用量 × アカウント数 | プロセスごとにマーケットキャッシュはひとつ |
| ステップサイズや最小注文額で取引所に注文を拒否される | 送信前にチェック: `OrderTooSmall`、`InvalidOrder` |
| 機能の半分は implicit API(`fapiPrivatePost…`) | 直接メソッド: TP/SL、dead-man switch、ポジションモード、資金移動 |
| レスポンスは `dict` | dataclass モデル(`Order`、`Position`、`OrderBook`…)、生レスポンスは `.raw` に |
| ccxt の例外がそのまま | `.retryable` フラグ付きの統一 `BytxError` 階層 |
| WS は例外で落ち、再接続は自分で実装 | スーパーバイザー: backoff + jitter、無通信ウォッチドッグ、再購読、オーダーブックの再同期、状態イベント |
| レスポンスが取引所ドキュメントと食い違う | 取引所の公式ドキュメントと照合済み([ground truth](GUIDE.md#quality))、ccxt のバグは SDK 側で修正 |

さらに: どこでも型付きモデル、全取引所で統一された数量単位、アカウントごとのプロキシ、ライブのオーダーブック上での
ペーパートレード、ポジションジャーナル、API キーの権限チェック、取引所のヘルス情報 — すべてどの取引所でも同じ
インターフェースで使えます。

## 主な機能

- **マーケットデータ:** `ticker`、`order_book`、`trades`、`candles`、ファンディングレート、建玉、レバレッジ階層。すべて API キー不要。
- **アカウント:** `balance`、`positions`、`open_orders`、`my_trades`、手数料、レバレッジ、ヘッジモード、資金移動。
- **取引:** `buy` / `sell` / `place_order`(market/limit、`post_only`、`reduce_only`、TP/SL、`client_id`)、一括発注、注文変更、キャンセル、ポジションのクローズ。
- **執行アルゴリズム**(`ex.algos`): `chase`、`twap`、`iceberg`、`bracket`、`trailing_stop`、スリッページ上限付き `market`。
- **リスクベースのサイジング:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` がストップ幅・レバレッジ・取引所の刻みと最小値・手数料からサイズを計算。
- **保護機能:** TP/SL、`dead_man_switch()`、`kill_switch`、`RiskGuard`(ポジション・日次損失・注文レートの上限を送信前にチェック)。
- **信頼性:** 注文を自動リトライしない。タイムアウトや結果不明時は `client_id` で注文を照会。一括操作の部分失敗は `BatchError` で報告。
- **自分の注文だけを操作:** `client_id_prefix` タグでボットの注文だけをキャンセル/クローズ。
- **WebSocket:** オーダーブック、ティッカー、約定、ローソク足、プライベートストリーム。自動再接続とオーダーブックの再構築。
- **スキャルピング:** WS 経由の注文(REST フォールバック付き)、`OrderTracker` / `PositionTracker`、レイテンシ計測。
- **ペーパートレードとリプレイ:** 実際のオーダーブックで仮想アカウントを運用、キー不要。
- **再起動からの復旧**、マルチアカウントとプロキシ、ポートフォリオ、ポジションジャーナル。
- **連携:** TradingView、Telegram/Discord、Prometheus、OpenTelemetry。

## 対応取引所

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) | BloFin (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| 現物 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — |
| 無期限先物 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| デモ / テストネット | ✓ | ✓ | ✓ | 先物 | ✓ | — | ✓ | ✓ | — | — | ✓ |
| WS 経由の注文 | ✓ | ✓ | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| ヘッジモード | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ | ✓ |

注記と制限事項(Binance/Bybit/Hyperliquid の WS 注文の条件、MEXC 現物の WS など)は
[英語版 README](GUIDE.md) を参照してください。

## サンプル

[`examples/`](../examples): 取引の基本([`00_trading_basics.py`](../examples/00_trading_basics.py) — 認証、現物と先物、
指値注文の発注/変更/キャンセル、ポジションの縮小)、マーケットデータとオーダーブック、バックテスト、
アービトラージスキャナー、グリッド、DCA、Telegram アラート、スキャルパーの雛形、マルチアカウント、
再起動から復旧するストラテジー。
取引系のサンプルはデフォルトでデモ環境で動作します。コードを書きたくない場合は、同じボットやバックテストを
[byttrade.com](https://byttrade.com) で利用できます。

## ライセンス

bytx は [BYTtrade Source-Available License](../LICENSE) のもとで無料で利用できます。自分のスクリプト、ボット、アプリケーションに
インストールして使うことができ、商用利用も可能です。コードを他のライブラリに複製すること、改変版を公開すること、
ホスティングサービスとして提供することは禁止されています。
ccxt(MIT)を使用しています。[`NOTICE`](../NOTICE) を参照。
投資助言ではありません。暗号資産の取引には損失のリスクがあります。
