"""Batch cancel of orders by id (:meth:`bytx.Exchange.cancel_orders`): batch size, the request and per-order response
parsing — per the exchange docs (ccxt 4.4.65 ``cancel_orders`` does not split a batch, and ``parse_orders`` sorts the
response by time — a rejection without an id moves to the front, and it can no longer be matched by position).

- Bybit «Batch Cancel Order» (POST /v5/order/cancel-batch, https://bybit-exchange.github.io/docs/v5/order/batch-cancel):
  «A maximum of 20 orders (option) & 10 orders (linear) & 10 orders (spot) can be cancelled per request» (inverse —
  none); the response — ``result.list[i]`` and ``retExtInfo.list[i]`` {code, msg} at the same index (a rejection has
  an empty ``orderId`` in the docs example, demo 2026-11-04 — the id from the request, code 110001).
- OKX «Cancel multiple orders» (POST /api/v5/trade/cancel-batch-orders,
  https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-cancel-multiple-orders): «Maximum 20 orders can be
  canceled per request»; a ``data[]`` item — {ordId, clOrdId, sCode, sMsg}, a rejection has ``ordId`` too; top-level
  ``code``: 0 — all canceled, 2 — partial (demo: «Bulk operation partially successful»), 1 — all rejected (ccxt raises
  an exception with the response body in its text — the item codes are taken from there). The exchange counts a batch
  of one order as a single cancel («it will consume the rate limit of Cancel order») — such a batch goes as a single
  cancel.
- Binance USDⓈ-M «Cancel Multiple Orders» (DELETE /fapi/v1/batchOrders): ``orderIdList`` «max length 10», IP weight 1
  (as a single cancel — a batch is 10 times cheaper in IP weight); the response — a list in ``orderIdList`` order: an
  order or {code, msg} (docs example: -2011 «Unknown order sent.»; demo 2026-11-04 — the request order). The Spot API
  has no batch cancel by id.
- Hyperliquid «Cancel order(s)» (POST /exchange, action {"type": "cancel", "cancels": [{"a", "o"}, …]},
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#cancel-order-s): one action —
  one signature (a ccxt signature takes ~50 ms in the event loop); the response ``response.data.statuses[i]`` —
  "success" or {error} at the same index. The docs set no batch limit; IP weight «1 + floor(batch_length / 40)» (Rate
  limits) — a batch of up to 40 costs as a single request.
"""
from __future__ import annotations

import json
from typing import Any, Optional

from bytx.exchanges.bybit.leverage import category as bybit_category

#: maximum orders in one batch cancel request (docs, see the module); not in the table — a ccxt batch (as a whole)
BATCH_MAX: dict[str, int] = {'bybit': 10, 'okx': 20, 'binance': 10, 'hyperliquid': 40}

#: response item: (order id | None, rejection code | None — canceled, rejection text, response row)
Element = tuple[Optional[str], Optional[str], str, dict]


def batch_size(ex_id: str, market: dict) -> int:
    """How many orders go into one cancel batch for the market (0 — no batch cancel per the docs: one by one)."""
    size = BATCH_MAX.get(ex_id, 0)
    if ex_id == 'binance' and not market.get('contract'):
        return 0                                     # Spot API: no batch cancel by id
    if ex_id == 'bybit' and market.get('inverse'):
        return 0                                     # Batch Cancel Order: spot / option / linear (not inverse)
    return size


def plan(indices: list[int], size: int) -> tuple[list[list[int]], list[int]]:
    """(batches, singles): batches of ``size``; a batch of one order — a single cancel (OKX counts it as a single
    one; a single cancel knows «already closed»)."""
    chunks = [indices[i:i + size] for i in range(0, len(indices), size)] if size > 1 else []
    singles = [c[0] for c in chunks if len(c) == 1] + ([] if size > 1 else list(indices))
    return [c for c in chunks if len(c) > 1], singles


def request(ex_id: str, market: dict, ids: list[str]) -> tuple[str, Any]:
    """(ccxt implicit method name, params) of a batch cancel request per the docs."""
    if ex_id == 'bybit':
        return 'privatePostV5OrderCancelBatch', {
            'category': bybit_category(market), 'request': [{'symbol': market['id'], 'orderId': i} for i in ids]}
    if ex_id == 'okx':
        return 'privatePostTradeCancelBatchOrders', [{'instId': market['id'], 'ordId': i} for i in ids]
    if ex_id == 'binance':
        # ccxt sign: DELETE batchOrders — orderidlist=[1,2] (encoded brackets, no spaces; demo: accepted)
        return 'fapiPrivateDeleteBatchOrders', {'symbol': market['id'], 'orderidlist': list(ids)}
    if ex_id == 'hyperliquid':
        # instance method (exchanges.hyperliquid.fast_sign.cancel_batch): ccxt cancel_orders returns parsed orders
        return 'bytx_cancel_batch', {'symbol': market['symbol'], 'ids': list(ids)}
    raise ValueError(f'no batch cancel per the docs: {ex_id}')


def _code(value: Any) -> Optional[str]:
    """Item rejection code; success (0, '0', empty) — None."""
    return None if value in (None, '', 0, '0') else str(value)


def elements(ex_id: str, raw: Any) -> list[Element]:
    """Batch cancel response items in the exchange order."""
    out: list[Element] = []
    if ex_id == 'bybit' and isinstance(raw, dict):
        result, ext = raw.get('result'), raw.get('retExtInfo')
        rows = result.get('list') if isinstance(result, dict) else None
        codes = ext.get('list') if isinstance(ext, dict) else None
        rows = rows if isinstance(rows, list) else []
        codes = codes if isinstance(codes, list) and len(codes) == len(rows) else [{}] * len(rows)
        for row, code in zip(rows, codes):
            row = row if isinstance(row, dict) else {}
            code = code if isinstance(code, dict) else {}
            failed = _code(code.get('code', row.get('code')))
            out.append((str(row.get('orderId') or '') or None, failed, str(code.get('msg') or row.get('msg') or ''),
                        row))
    elif ex_id == 'okx' and isinstance(raw, dict):
        data = raw.get('data')
        for row in data if isinstance(data, list) else []:
            row = row if isinstance(row, dict) else {}
            out.append((str(row.get('ordId') or '') or None, _code(row.get('sCode')), str(row.get('sMsg') or ''), row))
    elif ex_id == 'hyperliquid' and isinstance(raw, dict):
        data = (raw.get('response') or {}).get('data') if isinstance(raw.get('response'), dict) else None
        statuses = data.get('statuses') if isinstance(data, dict) else None
        for st in statuses if isinstance(statuses, list) else []:
            if isinstance(st, dict) and st.get('error'):
                out.append((None, str(st['error']), str(st['error']), {}))   # rejection code — the exchange text (no codes)
            else:
                out.append((None, None, '', {}))
    elif ex_id == 'binance' and isinstance(raw, list):
        for row in raw:
            row = row if isinstance(row, dict) else {}
            # a rejection — {code, msg} without orderId (docs example), a canceled one — the order with its status
            failed = _code(row.get('code')) if row.get('orderId') in (None, '') else None
            out.append((str(row['orderId']) if row.get('orderId') not in (None, '') else None, failed,
                        str(row.get('msg') or ''), row))
    return out


def error_body(error: BaseException) -> Any:
    """The response body from a ccxt exception text (``'<id> {json}'``): OKX code 1 «All operations failed» is an
    exception, and the item codes are in the body. None — no body."""
    text = str(getattr(error, 'message', None) or error)
    start = text.find('{')
    if start < 0:
        return None
    try:
        return json.JSONDecoder().raw_decode(text[start:])[0]
    except ValueError:
        return None


def match(ids: list[str], items: list[Element]) -> list[Optional[Element]]:
    """Items for the requested ids: by order id, the rest — by position (the response follows the request order: Bybit —
    «the same index», Binance — the docs example and demo) if the number of items equals the number of ids; otherwise —
    to the unmatched ids in order, only if their counts match. Not found — None (no cancel response)."""
    out: list[Optional[Element]] = [None] * len(ids)
    free: dict[str, list[int]] = {}
    for i, oid in enumerate(ids):
        free.setdefault(oid, []).append(i)
    rest: list[tuple[int, Element]] = []
    for k, item in enumerate(items):
        slots = free.get(item[0] or '')
        if slots:
            out[slots.pop(0)] = item
        else:
            rest.append((k, item))
    if rest and len(items) == len(ids):
        taken = []
        for k, item in rest:
            if out[k] is None:
                out[k] = item
            else:
                taken.append((k, item))
        rest = taken
    unmatched = [i for i, o in enumerate(out) if o is None]
    if rest and len(unmatched) == len(rest):
        for i, (_, item) in zip(unmatched, rest):
            out[i] = item
    return out


__all__ = ['BATCH_MAX', 'batch_size', 'elements', 'error_body', 'match', 'plan', 'request']
