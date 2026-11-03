"""Bybit V5: order lookup along the realtime → history chain.

Docs:
- «Get Open & Closed Orders» (GET /v5/order/realtime): ``openOnly`` «will be ignored when query
  by orderId or orderLinkId» — lookup by id returns both open and closed orders, but only
  «a maximum of recent 500 … under each account each category»;
- «Get Order History» (GET /v5/order/history, «2 years»): closed orders older than the realtime window.
  Canceled/rejected — for the last 24 h, other closed — 7 days, beyond that — only
  orders with fills.

ccxt 4.4.65 ``fetch_order`` (with ``acknowledged``) queries realtime only: an order pushed out
of the last 500 (a bot runs for weeks) yields OrderNotFound, and a filled level gets recreated.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges.bybit.leverage import category

# lookup order: open and last 500 closed → history
ENDPOINTS = ('private_get_v5_order_realtime', 'private_get_v5_order_history')


def order_request(market: dict, order_id: Optional[str] = None, client_id: Optional[str] = None) -> dict:
    """Order request params by ``orderId`` or ``orderLinkId``."""
    req: dict = {'category': category(market), 'symbol': market['id']}
    if order_id:
        req['orderId'] = str(order_id)
    else:
        req['orderLinkId'] = str(client_id)
    return req


def find_row(response: Any, order_id: Optional[str] = None, client_id: Optional[str] = None) -> Optional[dict]:
    """The ``result.list`` row with the given ``orderId`` / ``orderLinkId``, or None."""
    result = response.get('result') if isinstance(response, dict) else None
    rows = result.get('list') if isinstance(result, dict) else None
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if order_id and str(row.get('orderId')) == str(order_id):
            return row
        if not order_id and client_id and row.get('orderLinkId') == client_id:
            return row
    return None
