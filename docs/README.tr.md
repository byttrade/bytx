# bytx — [BYTtrade](https://byttrade.com) tarafından

[English](../README.md) | [Русский](README.ru.md) | [中文](README.zh-CN.md) | [Español](README.es.md) | [Português](README.pt-BR.md) | [日本語](README.ja.md) | [한국어](README.ko.md) | **Türkçe**

Kripto para borsaları için tip güvenli, **async** bir Python SDK'sı. Desteklenen borsalar: Binance, Bybit,
OKX, BingX, Hyperliquid, KuCoin, Bitget; beta aşamasında: Gate, MEXC, HTX. Spot ve vadeli (perpetual) işlemler
için tek bir arayüz, akıllı yeniden bağlanan WebSocket, hesap başına proxy ve scalping
için araçlar sunar.

> Tam dokümantasyon: [docs/GUIDE.md](GUIDE.md) (İngilizce)


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## Kurulum

```bash
pip install bytx            # + extras: [ws] [fast]
```

## Hızlı başlangıç

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, varsayılan olarak swap
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # miktar kontrat değil, BTC cinsinden
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True` borsanın demo/testnet ortamını kullanır; gerçek para kullanılmaz.

> Kodsuz botlar, backtest'ler ve Telegram üzerinden işlem onayları:
> [byttrade.com](https://byttrade.com).

## Neden düz ccxt yerine bytx

bytx, [ccxt](https://github.com/ccxt/ccxt) üzerine kuruludur ve onu boğuşmanız gerekmeyen bir alım-satım SDK'sına dönüştürür:
aynı iş **bir sayfa yerine tek satır** tutar, canlı ortamda para kaybettiren uç durumlar ise sizin yerinize
halledilir.

**OKX perpetual vadelide 100 USDT'lik BTC almak**

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

**Pozisyona take-profit ve stop-loss** — ccxt'de her borsa farklıdır (Bybit
`privatePostV5PositionTradingStop`, Binance `closePosition` ile iki koşullu emir, OKX algo emirleri…).
bytx'te her yerde aynı çağrı:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**Bağlantı kopmalarından sağ çıkan canlı emir defteri** — ccxt ile yeniden bağlanma döngüsünü, backoff'u,
yeniden aboneliği, boşluk tespitini ve snapshot ile yeniden senkronizasyonu kendiniz yazarsınız. bytx'te:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**Emir gönderirken zaman aşımları** — ccxt'de zaman aşımı, emrin aktif olup olmadığını tahmin etmeye bırakır
(yeniden denemek çift giriş demektir). bytx `client_id` ile mutabakat yapar ve tam olarak ne olduğunu söyler:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**Kutudan çıktığı gibi gelen diğer özellikler**

| Düz ccxt | bytx |
|---|---|
| `amount` kontrat cinsinden, her borsanın kendi `contractSize` değeri var | `size=` baz coin, `notional=` USDT veya `contracts=`; dönüşüm ve yuvarlama (aşağı) dahili |
| Her örnekte `load_markets`, bellek × hesap sayısı | süreç başına tek market önbelleği |
| Borsa, adım büyüklüğü veya minimum tutar yüzünden emri reddeder | gönderimden önce kontrol: `OrderTooSmall`, `InvalidOrder` |
| Özelliklerin yarısı implicit API (`fapiPrivatePost…`) | doğrudan metotlar: TP/SL, dead-man switch, pozisyon modu, transferler |
| `dict` yanıtlar | dataclass modelleri (`Order`, `Position`, `OrderBook`…), ham yanıt `.raw` içinde |
| Çıplak ccxt istisnaları | `.retryable` bayraklı tek bir `BytxError` hiyerarşisi |
| WS bir istisnayla ölür, yeniden bağlanmak sizin işiniz | supervisor: backoff + jitter, sessizlik watchdog'u, yeniden abonelik, emir defteri senkronizasyonu, durum olayları |
| Yanıtlar borsa dokümantasyonundan sapar | yanıtlar resmi borsa dokümantasyonuna göre doğrulanır ([ground truth](GUIDE.md#quality)), ccxt hataları SDK'da düzeltilir |

Ayrıca: her yerde tipli modeller, tüm borsalarda tek hacim birimi, hesap başına proxy, canlı emir defterleri
üzerinde paper trading, pozisyon günlüğü, API anahtarı yetki kontrolleri ve borsa sağlık durumu — hepsi her
borsada aynı arayüzle.

## Temel özellikler

- **Piyasa verisi:** `ticker`, `order_book`, `trades`, `candles`, fonlama oranları, açık pozisyon (open interest), kaldıraç kademeleri. Hepsi API anahtarı olmadan çalışır.
- **Hesap:** `balance`, `positions`, `open_orders`, `my_trades`, komisyonlar, kaldıraç, hedge modu, transferler.
- **İşlem:** `buy` / `sell` / `place_order` (market/limit, `post_only`, `reduce_only`, TP/SL, `client_id`), toplu emirler, emir değiştirme, iptal, pozisyon kapatma.
- **Yürütme algoritmaları** (`ex.algos`): `chase`, `twap`, `iceberg`, `bracket`, `trailing_stop`, slippage sınırlı `market`.
- **Riske dayalı pozisyon boyutlandırma:** `open_position(symbol, 'long', risk_usdt=50, stop=...)` miktarı stop mesafesi, kaldıraç, borsa adımları ve minimumları ile komisyonlara göre hesaplar.
- **Koruma:** TP/SL, `dead_man_switch()`, `kill_switch`, `RiskGuard` (pozisyon, günlük zarar ve emir hızı limitleri gönderimden önce kontrol edilir).
- **Güvenilirlik:** emirler asla otomatik olarak tekrar gönderilmez; zaman aşımı veya belirsiz sonuçta emir `client_id` ile aranır. Toplu işlemlerde kısmi hata `BatchError` ile raporlanır.
- **Yalnızca kendi emirleriniz:** `client_id_prefix` etiketiyle sadece botun emirleri iptal edilir/kapatılır.
- **WebSocket:** emir defteri, ticker, işlemler, mumlar, özel akışlar; otomatik yeniden bağlanma ve emir defterinin yeniden kurulması.
- **Scalping:** WS üzerinden emirler (REST yedeğiyle), `OrderTracker` / `PositionTracker`, gecikme ölçümleri.
- **Paper trading ve replay:** gerçek emir defteri üzerinde sanal hesap, anahtar gerekmez.
- **Yeniden başlatma sonrası kurtarma**, çoklu hesap ve proxy'ler, portföy, pozisyon günlüğü.
- **Entegrasyonlar:** TradingView, Telegram/Discord, Prometheus, OpenTelemetry.

## Desteklenen borsalar

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| Spot | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Perpetual vadeli | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| Demo / testnet | ✓ | ✓ | ✓ | vadeli | ✓ | — | ✓ | ✓ | — | — |
| WS üzerinden emir | ✓ | ✓ | ✓ | — | ✓ | — | — | ✓ | — | — |
| Hedge modu | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ |

Dipnotlar ve kısıtlamalar (Binance/Bybit/Hyperliquid'de WS emir koşulları, MEXC spot WS vb.) için
[İngilizce README](GUIDE.md)'ye bakın.

## Örnekler

[`examples/`](../examples): işlem temelleri ([`00_trading_basics.py`](../examples/00_trading_basics.py) — kimlik doğrulama,
spot ve vadeli, limit emirler: verme / değiştirme / iptal, pozisyon azaltma), piyasa verisi ve emir defteri,
backtest, arbitraj tarayıcı, grid, DCA, Telegram uyarıları, scalper iskeleti, çoklu hesap ve yeniden başlatmadan
kurtulabilen bir strateji. İşlem örnekleri
varsayılan olarak demo ortamında çalışır. Kod yazmak istemiyor musunuz? Aynı botlar ve backtest'ler
[byttrade.com](https://byttrade.com) üzerinde de çalışır.

## Lisans

bytx, [BYTtrade Source-Available License](../LICENSE) kapsamında ücretsiz kullanılabilir: kendi betiklerinize, botlarınıza ve
uygulamalarınıza, ticari amaçlar dahil, kurup kullanabilirsiniz. Kodunu başka kütüphanelere kopyalamak,
değiştirilmiş sürümlerini yayımlamak ve barındırılan bir hizmet olarak sunmak yasaktır.
ccxt (MIT) kullanır, bkz. [`NOTICE`](../NOTICE).
Yatırım tavsiyesi değildir; kripto para ticareti kayıp riski taşır.
