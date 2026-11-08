"""HTX USDT-M: position TP/SL — Set a Take-profit and Stop-loss Order for an Existing Position.

POST /linear-swap-api/v1/swap_cross_tpsl_order (isolated — swap_tpsl_order,
https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-set-a-take-profit-and-stop-loss-order-for-an-existing-position):
«All take-profit and stop-loss orders are position closing orders»; ``direction`` — side of the closing order,
``volume`` — contracts, ``tp_order_price_type`` / ``sl_order_price_type`` «default is market». There is no trigger
price type parameter — last price only. No user id. List — swap_cross_tpsl_openorders,
cancel — swap_cross_tpsl_cancel (``order_id``). A single TP/SL by id in any status — open ones, then
swap_cross_tpsl_hisorders (``status`` «0:all», ``create_date`` up to 90 days): the regular order_info doesn't know TP/SL.
A triggered TP/SL — status 4 «Submit the orders success», ``relation_order_id`` — id of the fill order
(«The value is -1 before the trigger orders executed»), that order has ``order_source`` «tpsl».
"""
from __future__ import annotations

from typing import Optional

DOC = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-set-a-take-profit-and-stop-loss-order-for-an-existing-position'
DEFAULT_TRIGGER_BY = 'last'     # trigger price type can't be set — last price
HISTORY_DAYS = 90               # create_date: «Requesting data beyond 90 will not be supported»
HISTORY_PAGES = 10              # page_size «50 at most» — up to the last 500 TP/SL of the symbol

# relation_tpsl_order_id — «related take-profit and stop loss order id» of a pair placed in one request;
# mutual cancel on trigger is not described in the docs
OCO = ('emulated', DOC, False, 'relation_tpsl_order_id links the pair; canceling the second leg is not described in docs')

# Place An Order: tp_trigger_price / sl_trigger_price — «Only open orders can support setting take profit and stop loss»
ATTACHED_SL = (True, 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-place-an-order', True,
               'sl_trigger_price in the opening order')

# Moving a TP/SL (modify_tp_sl): the docs have no TP/SL amend, coexistence was not verified — a cancel and a new
# placement (a window without a stop).
MODIFY_IN_PLACE = None
TWO_SL = (False, DOC, False, 'not verified — cancel, then the new one')

# status «1.Not Activated, 2.Ready to submit the orders, 3.Submitting the orders, 4.Submit the orders success,
# 5.Submit the orders failed, 6.Orders cancelled, 8.Cancelled order not found, 9.Orders cancelling, 10.Failed,
# 11.Expired» (+ «12. Not Activated-Expired» — tpsl_order_info of the opening order, swap_cross_relation_tpsl_order)
_STATES = {1: 'open', 2: 'open', 3: 'triggered', 4: 'triggered', 5: 'canceled', 6: 'canceled', 8: 'canceled',
           9: 'canceled', 10: 'canceled', 11: 'canceled', 12: 'canceled'}


def state(info: dict) -> Optional[str]:
    try:
        return _STATES.get(int(info.get('status')))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# TP/SL history: «relation_order_id — … The value is -1 before the trigger orders executed».
FILL_ORDER_ID = ('relation_order_id',)
