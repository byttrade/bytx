"""BingX USDT-M perpetual: ccxt 4.4.65 patches on the ccxt INSTANCE.

Source — https://bingx-api.github.io/docs-v3/ (Swap section; bundle
``static/js/app.a2976421d1f36b01e621.js``, verified 2026-10-27).

1. Trades (``fetch_my_trades`` → GET /openApi/swap/v2/trade/allFillOrders, «Query historical
   transaction orders»). The response row has no trade id — only filledTm, volume, price, amount,
   commission, currency, orderId, clientOrderID, symbol (docs example). ccxt returns ``id=None``,
   and deduping trades by id drops them: NOT A SINGLE trade was found on BingX.
   A trade without an id gets a stable id from the row fields (order, time, price, amount; a repeat
   of the same row in the response — suffix ``#n``).
   Amount: ccxt sends tradingUnit=CONT and multiplies volume by tradeMinQuantity («contract size»,
   the docs have no such mapping). Docs: «COIN represents assets such as BTC and ETH, while CONT
   represents the number of contracts» — we request COIN and take volume as is (amount in coin,
   like BingX positions and orders).
2. Order status (Query Order details / Query Order history / Current All Open Orders):
   «NEW/PENDING/FILLED/PARTIALLYFILLED/CANCELLED/FAILED». ccxt knows only PARTIALLY_FILLED —
   PARTIALLYFILLED leaked out as a raw string (neither open nor closed) → 'open'.

Linear swap only; spot and coin-M are untouched. Idempotent.
"""
from typing import Any

from ccxt.base.precise import Precise

_APPLIED = "_bytx_bingx_patched"

# REST statuses that ccxt 4.4.65 bingx.parse_order_status doesn't map.
_EXTRA_STATUSES = {'PARTIALLYFILLED': 'open'}


def _is_linear_swap(market: Any) -> bool:
    return isinstance(market, dict) and bool(market.get('swap')) and not market.get('inverse')


def bingx_trade_id(info: dict, seen: dict) -> str:
    """Stable allFillOrders trade id (the response has none): order, time, price, amount.

    ``seen`` — counter of identical rows within one response: two identical fills of one
    order in the same millisecond get different ids (``…#1``), and a repeat request gets the same ids.
    """
    base = ':'.join(str(info.get(k) or '') for k in ('orderId', 'filledTm', 'price', 'volume'))
    if not info.get('filledTm'):
        base = ':'.join(str(info.get(k) or '') for k in ('orderId', 'filledTime', 'price', 'volume'))
    n = seen.get(base, 0)
    seen[base] = n + 1
    return f'bingx:{base}' + (f'#{n}' if n else '')


def _fix_my_trades(exchange: Any) -> None:
    original = exchange.fetch_my_trades

    async def fetch_my_trades(symbol=None, since=None, limit=None, params={}):
        if symbol is None:
            return await original(symbol, since, limit, params)
        await exchange.load_markets()
        market = exchange.market(symbol)
        if not _is_linear_swap(market):
            return await original(symbol, since, limit, params)
        params = dict(params or {})
        unit = str(params.get('tradingUnit') or 'COIN').upper()
        params['tradingUnit'] = unit
        trades = await original(symbol, since, limit, params)
        seen: dict = {}
        for t in trades:
            info = t.get('info') if isinstance(t.get('info'), dict) else {}
            if not t.get('id'):
                t['id'] = str(info.get('tradeId') or '') or bingx_trade_id(info, seen)
            if unit != 'COIN':
                continue
            volume = exchange.safe_string(info, 'volume')
            if volume is None:
                continue
            t['amount'] = exchange.parse_number(volume)
            price = exchange.safe_string(info, 'price')
            if price is not None:
                t['cost'] = exchange.parse_number(Precise.string_mul(price, volume))
        return trades

    exchange.fetch_my_trades = fetch_my_trades


def _fix_order_status(exchange: Any) -> None:
    original = exchange.parse_order_status

    def parse_order_status(status):
        mapped = _EXTRA_STATUSES.get(str(status or '').upper())
        return mapped if mapped is not None else original(status)

    exchange.parse_order_status = parse_order_status


def patch_bingx_instance(exchange: Any) -> None:
    """ccxt bingx instance fixes (other exchanges and repeated calls are a no-op)."""
    if getattr(exchange, 'id', None) != 'bingx' or getattr(exchange, _APPLIED, False):
        return
    setattr(exchange, _APPLIED, True)
    _fix_my_trades(exchange)
    _fix_order_status(exchange)
