"""MEXC futures: position TP/SL — Place TP/SL Order by Position.

POST /api/v1/private/stoporder/place (https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-tpsl-order-by-position):
``positionId`` — required (position id from open_positions), ``vol`` — contracts, ``volType`` «1: partial TP/SL
2: position TP/SL», ``lossTrend`` / ``profitTrend`` «1 latest price 2 fair price 3 index price», ``stopLossType`` /
``takeProfitType`` «0 - market». No user id. List — GET stoporder/open_orders, cancel — POST
stoporder/cancel ``[{"stopPlanOrderId": …}]``.
"""
from __future__ import annotations

from typing import Optional

DOC = 'https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-tpsl-order-by-position'
DEFAULT_TRIGGER_BY = 'mark'
TRENDS = {'last': 1, 'mark': 2, 'index': 3}

# Each leg is a separate stoporder (one price); a link between two separate orders isn't described in the docs
OCO = ('emulated', DOC, False, 'separate stoporders for TP and SL: mutual cancel not described in docs')

# Place Order: «stopLossPrice — Stop-loss price» (and takeProfitPrice) — SL is attached to the entry order
ATTACHED_SL = (True, 'https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order', False,
               'stopLossPrice in Place Order; to verify on the exchange (MEXC has no testnet)')

# Moving a TP/SL (modify_tp_sl): not verified against the docs — a cancel and a new placement (a window without a stop).
MODIFY_IN_PLACE = None
TWO_SL = (False, DOC, False, 'not verified — cancel, then the new one')

# state «1: untriggered,2: canceled,3: executed,4: invalidated,5: execution failed»
_STATES = {1: 'open', 2: 'canceled', 3: 'triggered', 4: 'canceled', 5: 'canceled'}


def state(info: dict) -> Optional[str]:
    try:
        return _STATES.get(int(info.get('state')))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# Get TP/SL Order List: «placeOrderId | long | Order id after successful placement» (0 before triggering).
FILL_ORDER_ID = ('placeOrderId',)
