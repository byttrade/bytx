"""Gate USDT-M: position TP/SL — price-triggered orders.

POST /futures/{settle}/price_orders (models FuturesPriceTriggeredOrder / FuturesInitialOrder / FuturesPriceTrigger
of the official APIv4 spec: https://github.com/gateio/gateapi-python/blob/master/docs/FuturesPriceTriggeredOrder.md):
``trigger.price_type`` «0 - Latest trade price, 1 - Mark price, 2 - Index price»; ``trigger.rule`` «1: … greater
than or equal to Trigger.Price … 2: … less than or equal»; ``order_type`` close-long-position /
close-short-position (whole position; ``initial.close`` = true in single mode, size 0) or
plan-close-long-position / plan-close-short-position (partial: size < 0 for long, > 0 for short);
``initial.price`` «Set to 0 to use market price», ``tif`` — «market orders currently only support ioc».
price_orders have no user id (``initial.text`` — «The source of the order»).
"""
from __future__ import annotations

from typing import Optional

DOC = 'https://github.com/gateio/gateapi-python/blob/master/docs/FuturesPriceTriggeredOrder.md'
DEFAULT_TRIGGER_BY = 'mark'
PRICE_TYPES = {'last': 0, 'mark': 1, 'index': 2}

OCO = ('emulated', DOC, False, 'separate price_orders: TP/SL link not described in docs')

# Gate: TP/SL can't be attached to the entry order — «The two types of order take-profit/stop-loss
# (close-long-order / close-short-order) are read-only and cannot be passed in requests»
ATTACHED_SL = (False, DOC, True, 'order TP/SL is read-only — SL as a separate price_order after entry')

# Moving a TP/SL (modify_tp_sl): the available spec has no price_order amend (the official APIv4 page —
# 403, the gateapi-python model has no update), two close-*-position coexisting is not described — a cancel and a new
# placement (a window without a stop — the time of one request).
MODIFY_IN_PLACE = None
TWO_SL = (False, DOC, False, 'not described in the available docs — cancel, then the new one')

# status «open: Active, finished, inactive, invalid»; finish_as «cancelled, succeeded, failed, expired»
_FINISH = {'succeeded': 'triggered', 'cancelled': 'canceled', 'failed': 'canceled', 'expired': 'canceled'}


def state(info: dict) -> Optional[str]:
    status = str(info.get('status') or '').lower()
    if status == 'open':
        return 'open'
    if status in ('finished', 'inactive', 'invalid'):
        return _FINISH.get(str(info.get('finish_as') or '').lower(), 'canceled')
    return None


# FuturesPriceTriggeredOrder: «trade_id — ID of the order created after trigger» (0 before triggering).
FILL_ORDER_ID = ('trade_id',)
