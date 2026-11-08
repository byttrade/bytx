"""BingX: TAKE_PROFIT_MARKET / STOP_MARKET.

POST /openApi/swap/v2/trade/order: ``workingType`` «default set to MARK_PRICE». The legacy field isn't sent
(exchange default MARK_PRICE) — defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): TAKE_PROFIT_MARKET and STOP_MARKET are separate create_order orders;
# the Place order docs describe no link between them. Demo 2026-11-04
# (tests/ground_truth/bingx/demo_oco_legs.json): after one leg fires the other stays NEW while the position is
# open — no OCO, the SDK cancels the sibling; when the position is closed the exchange cancels the reduce-only legs
# (CANCELLED).
OCO = ('emulated', 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order', True,
       'separate conditional orders, not linked (demo 2026-11-04: after TP the SL leg stays alive while the position '
       'is open) — emulation; when the position is closed the exchange cancels the rest (CANCELLED)')

# SL attached to the entry order (open_position): Place order ``stopLoss`` / ``takeProfit``
# (JSON string, «Support setting take profit/stop loss while placing an order»). Field descriptions in the docs
# are shifted by one row (the stopLoss text sits next to the neighboring field) — verified on demo 2026-11-04
# (tests/ground_truth/bingx/demo_attached_sl.json): when the entry fills the exchange places STOP_MARKET /
# TAKE_PROFIT_MARKET legs on the whole size (reduceOnly, workingType MARK_PRICE, no clientOrderId), the SL
# fires and closes the position, the exchange cancels the legs itself when the position is closed; an entry with
# an SL on the wrong side of the price is rejected entirely (101400) — atomic. A partial entry fill can't be
# reproduced on demo.
ATTACHED_SL = (True, 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order', True,
               'stopLoss in Place order: STOP_MARKET / TAKE_PROFIT_MARKET legs on the entry size, an entry with an SL '
               'on the wrong side is rejected entirely (demo 2026-11-04)')

# Moving a TP/SL (modify_tp_sl): Modify Order (POST /openApi/swap/v1/trade/amend) — «currently supports only LIMIT
# orders, and only the order quantity can be modified»; cancelReplace — a cancel and a new order (a window inside the
# exchange, if the new one is rejected there is no stop). Place order, trigger conditions: «The accumulative quantity of
# the pending stop loss orders cannot be greater than the quantity of open positions». VST 2026-11-04 DOGE
# (tests/ground_truth/bingx/demo_two_sl.json): a second STOP_MARKET reduceOnly on the whole position was accepted, both
# stand — make-before-break; when the position is closed the exchange cancels the reduce-only legs. Live may reject the
# second SL by the rule above — then the new one is rejected: the previous SL stays, the error goes to the caller.
MODIFY_IN_PLACE = None
TWO_SL = (True, 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order', True,
          'VST 2026-11-04: a second SL on the whole position was accepted (the docs rule on the SL sum — in the trigger '
          'conditions)')


# status (Query Order details): NEW / PENDING / FILLED / PARTIALLYFILLED / CANCELLED / FAILED
_STATE = {'NEW': 'open', 'PENDING': 'open', 'TRIGGERED': 'triggered', 'FILLED': 'triggered',
          'PARTIALLYFILLED': 'triggered', 'PARTIALLY_FILLED': 'triggered',
          'CANCELLED': 'canceled', 'CANCELED': 'canceled', 'FAILED': 'canceled', 'EXPIRED': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('status') or '').upper())


# The exchange fills a triggered TAKE_PROFIT_MARKET / STOP_MARKET with a DIFFERENT order; only that order keeps the link —
# ``triggerOrderId`` = the conditional order's id (Query Order details, demo 2026-11-04). The conditional order's own
# record (lists, WS) has no such field — FILL_ORDER_ID is empty; Exchange.tp_sl_order by the conditional order's id gets
# the fill order and reports its id in fill_order_id.
FILL_ORDER_ID: tuple = ()
