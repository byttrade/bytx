"""HTX USDT-M: order by exchange id, including finished ones.

[Cross] Get Information of order (POST /linear-swap-api/v1/swap_cross_order_info,
https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-get-information-of-order): «The order completed( 5.partially
fulfilled but cancelled by client; 6. Fully fulfilled; 7. Cancelled; ) will be deleted after the settlement of
funding rate on 00:00(GMT+8), 08:00(GMT+8) and 16:00(GMT+8)», canceled — «users can only query last 2-hour data».
Such an order is looked up in [Cross] Get History Orders(New) (POST /linear-swap-api/v3/swap_cross_hisorders, window — 48 h).
"""
from __future__ import annotations

from typing import Any

from bytx.errors import BytxError, OrderNotFound
from bytx.outcome import order_not_open
from bytx.types import Order

DOC = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-get-information-of-order'


async def order_by_id(ex: Any, s: str, order_id: str) -> Order:
    try:
        return ex._found(await ex._call(ex._ex.fetch_order, order_id, s))
    except BytxError as e:
        if not order_not_open(ex._ex.id, e):   # 1017 / 1061 / empty data — order not in order_info
            raise
        first = e
    for o in await ex.closed_orders(s):
        if o.id == str(order_id):
            return o
    raise OrderNotFound(f'order {order_id} ({s}) not found in order_info or in the last 48 h of history',
                        exchange=ex.name, details={'symbol': s, 'order_id': order_id}) from first
