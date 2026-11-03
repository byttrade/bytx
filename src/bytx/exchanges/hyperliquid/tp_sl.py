"""Hyperliquid: tpsl trigger orders.

Docs «Take profit and stop loss orders»: «The mark price is used to trigger TP/SL orders» — mark only.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): status ``siblingFilledCanceled`` — «TP/SL only. Canceled due to sibling
# ordering being filled» (orderStatus) — only for grouped TP/SL (grouping normalTpsl /
# positionTpsl). The SDK sends grouping "na": legs are independent — the SDK cancels the sibling.
OCO = ('emulated', 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint'
       '#query-order-status-by-oid-or-cloid', True, 'grouping na — legs are not linked')

# SL attached to the entry order (open_position): the order action has ``grouping``
# "normalTpsl", but the SDK sends "na" — entry and SL as separate orders, SL right after entry.
ATTACHED_SL = (False, 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint',
               True, 'grouping na — SL as a separate order after entry')


# status (info orderStatus / historicalOrders, value table in the «Query order status» docs): open |
# filled | triggered | canceled | rejected | …Canceled | …Rejected | scheduledCancel


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None.

    ``info`` — orderStatus response (``{"status": "order", "order": {"order": …, "status": …}}``),
    its ``order``, or a historicalOrders row.
    """
    status = info.get('status')
    inner = info.get('order')
    if isinstance(inner, dict) and isinstance(inner.get('status'), str):
        status = inner['status']
    text = str(status or '')
    if text == 'open':
        return 'open'
    if text in ('triggered', 'filled'):
        return 'triggered'
    if text == 'scheduledCancel' or text.lower().endswith(('canceled', 'rejected')):
        return 'canceled'
    return None
