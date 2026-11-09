# bytx — by [BYTtrade](https://byttrade.com)

[English](../README.md) | [Русский](README.ru.md) | [中文](README.zh-CN.md) | [Español](README.es.md) | [Português](README.pt-BR.md) | [日本語](README.ja.md) | **한국어** | [Türkçe](README.tr.md)

암호화폐 거래소를 위한 타입이 지정된 **async** Python SDK입니다. 지원 거래소: Binance, Bybit, OKX, BingX,
Hyperliquid, KuCoin, Bitget, 베타: Gate, MEXC, HTX. 현물과 무기한 선물을 하나의 인터페이스로 다루고,
스마트 재연결 기능을 갖춘 WebSocket, 계정별 프록시, 스캘핑을 위한 도구를 제공합니다.

> 전체 문서: [docs/GUIDE.md](GUIDE.md) (영어)


![bytx](https://raw.githubusercontent.com/byttrade/bytx/main/docs/media/one_api.gif)

## 설치

```bash
pip install bytx            # + extras: [ws] [fast]
```

## 빠른 시작

```python
import asyncio
from bytx import create_exchange

async def main():
    async with create_exchange('bybit', api_key='…', secret='…', demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))                      # BTC/USDT:USDT, 기본값은 스왑
        order = await ex.buy('BTC/USDT', notional=100, price=60_000, post_only=True)
        print(order.status, order.size)                         # 수량은 계약 수가 아닌 BTC 단위
        await ex.cancel_order('BTC/USDT', order.id)

asyncio.run(main())
```

`demo=True`는 거래소의 데모/테스트넷을 사용합니다. 실제 자금은 사용되지 않습니다.

> 노코드 봇, 백테스트, 텔레그램 거래 승인은
> [byttrade.com](https://byttrade.com)에서 이용하세요.

## 순수 ccxt 대신 bytx를 쓰는 이유

bytx는 [ccxt](https://github.com/ccxt/ccxt) 위에 구축되어, ccxt를 씨름할 필요 없는 트레이딩 SDK로 바꿔 줍니다. 같은 작업이
**한 페이지가 아니라 한 줄**이면 끝나고, 실전에서 돈을 잃게 만드는 엣지 케이스는 SDK가 알아서 처리합니다.

**OKX 무기한 선물에서 100 USDT어치 BTC 매수**

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

**포지션에 익절·손절(TP/SL) 설정** — ccxt에서는 거래소마다 방식이 다릅니다(Bybit는
`privatePostV5PositionTradingStop`, Binance는 `closePosition`이 붙은 조건부 주문 두 개, OKX는 알고 주문…).
bytx에서는 어디서나 같은 호출입니다:

```python
await ex.set_tp_sl('BTC/USDT', take_profit=72_000, stop_loss=58_000)
```

**연결이 끊겨도 살아남는 실시간 오더북** — ccxt에서는 재연결 루프, backoff, 재구독, 누락 감지, 스냅샷
재동기화를 직접 작성해야 합니다. bytx에서는:

```python
async for book in ex.stream.order_book('BTC/USDT', depth=20):
    print(book.best_bid, book.best_ask, book.spread_bps)
```

**주문 전송 시 타임아웃** — ccxt에서는 타임아웃이 나면 주문이 살아 있는지 추측할 수밖에 없습니다(재시도하면
이중 진입). bytx는 `client_id`로 대조해 정확히 무슨 일이 있었는지 알려 줍니다:

```python
try:
    order = await ex.buy('BTC/USDT', size=0.01, client_id=cid)
except errors.OrderStatusUnknown as e:
    order = await ex.find_order(e.symbol, e.client_order_id)
```

**기본으로 제공되는 그 밖의 기능**

| 순수 ccxt | bytx |
|---|---|
| `amount`는 계약 단위이고 거래소마다 `contractSize`가 다름 | `size=`는 기초 코인, `notional=`은 USDT, 또는 `contracts=`; 환산과 반올림(내림) 내장 |
| 인스턴스마다 `load_markets`, 메모리 × 계정 수 | 프로세스당 하나의 마켓 캐시 |
| 스텝 크기나 최소 주문 금액 때문에 거래소가 주문을 거부 | 전송 전에 검사: `OrderTooSmall`, `InvalidOrder` |
| 기능의 절반이 implicit API(`fapiPrivatePost…`) | 직접 메서드: TP/SL, dead-man switch, 포지션 모드, 자금 이체 |
| `dict` 응답 | dataclass 모델(`Order`, `Position`, `OrderBook`…), 원본 응답은 `.raw` |
| ccxt 예외 그대로 | `.retryable` 플래그가 있는 단일 `BytxError` 계층 |
| WS가 예외로 죽고 재연결은 직접 구현 | 슈퍼바이저: backoff + jitter, 무응답 감시, 재구독, 오더북 재동기화, 상태 이벤트 |
| 응답이 거래소 문서와 다름 | 거래소 공식 문서와 대조 검증([ground truth](GUIDE.md#quality)), ccxt 버그는 SDK에서 수정 |

그 밖에도: 어디서나 타입이 지정된 모델, 모든 거래소에서 하나의 수량 단위, 계정별 프록시, 실시간 오더북 기반
페이퍼 트레이딩, 포지션 저널, API 키 권한 검사와 거래소 상태 확인 — 모두 모든 거래소에서 같은 인터페이스로
제공됩니다.

## 주요 기능

- **시장 데이터:** `ticker`, `order_book`, `trades`, `candles`, 펀딩비, 미결제약정, 레버리지 구간. 모두 API 키 없이 사용 가능.
- **계정:** `balance`, `positions`, `open_orders`, `my_trades`, 수수료, 레버리지, 헤지 모드, 자금 이체.
- **거래:** `buy` / `sell` / `place_order`(market/limit, `post_only`, `reduce_only`, TP/SL, `client_id`), 일괄 주문, 주문 정정, 취소, 포지션 청산.
- **실행 알고리즘**(`ex.algos`): `chase`, `twap`, `iceberg`, `bracket`, `trailing_stop`, 슬리피지 상한이 있는 `market`.
- **리스크 기반 사이징:** `open_position(symbol, 'long', risk_usdt=50, stop=...)`이 손절 거리, 레버리지, 거래소 단위와 최소값, 수수료로 수량을 계산.
- **보호 기능:** TP/SL, `dead_man_switch()`, `kill_switch`, `RiskGuard`(포지션·일일 손실·주문 빈도 한도를 전송 전에 검사).
- **신뢰성:** 주문을 자동으로 재시도하지 않음. 타임아웃이나 결과 불명 시 `client_id`로 주문을 조회. 일괄 작업의 부분 실패는 `BatchError`로 보고.
- **내 주문만 처리:** `client_id_prefix` 태그로 봇의 주문만 취소/청산.
- **WebSocket:** 오더북, 티커, 체결, 캔들, 프라이빗 스트림. 자동 재연결과 오더북 재구성.
- **스캘핑:** WS 주문(REST 폴백 포함), `OrderTracker` / `PositionTracker`, 지연 시간 측정.
- **페이퍼 트레이딩과 리플레이:** 실제 오더북 위에서 가상 계정 운용, 키 불필요.
- **재시작 복구**, 멀티 계정과 프록시, 포트폴리오, 포지션 저널.
- **연동:** TradingView, Telegram/Discord, Prometheus, OpenTelemetry.

## 지원 거래소

| | Binance | Bybit | OKX | BingX | Hyperliquid | KuCoin | Bitget | Gate (beta) | MEXC (beta) | HTX (beta) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| 현물 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| 무기한 선물 | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M | ✓ USDT-M |
| 데모 / 테스트넷 | ✓ | ✓ | ✓ | 선물 | ✓ | — | ✓ | ✓ | — | — |
| WS 주문 | ✓ | ✓ | ✓ | — | ✓ | — | — | ✓ | — | — |
| 헤지 모드 | ✓ | ✓ | ✓ | ✓ | — | — | ✓ | — | ✓ | ✓ |

각주와 제한 사항(Binance/Bybit/Hyperliquid의 WS 주문 조건, MEXC 현물 WS 등)은
[영문 README](GUIDE.md)를 참고하세요.

## 예제

[`examples/`](../examples): 거래 기초([`00_trading_basics.py`](../examples/00_trading_basics.py) — 인증, 현물과 선물,
지정가 주문 생성/정정/취소, 포지션 축소), 시장 데이터와 오더북, 백테스트, 차익거래 스캐너, 그리드, DCA,
텔레그램 알림, 스캘퍼 뼈대, 멀티 계정, 재시작 복구가 가능한 전략.
거래 예제는 기본적으로 데모 환경에서 실행됩니다.
코드를 작성하고 싶지 않다면 같은 봇과 백테스트를 [byttrade.com](https://byttrade.com)에서 실행할 수 있습니다.

## 라이선스

bytx는 [BYTtrade Source-Available License](../LICENSE)에 따라 무료로 사용할 수 있습니다. 자신의 스크립트, 봇, 애플리케이션에 설치해
사용할 수 있으며 상업적 용도도 허용됩니다. 코드를 다른 라이브러리에 복사하거나, 수정한 버전을 배포하거나,
호스팅 서비스로 제공하는 것은 허용되지 않습니다.
ccxt(MIT)를 사용합니다. [`NOTICE`](../NOTICE) 참고.
투자 조언이 아닙니다. 암호화폐 거래에는 손실 위험이 따릅니다.
