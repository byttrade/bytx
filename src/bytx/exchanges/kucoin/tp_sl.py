"""KuCoin Futures: stop orders.

POST /api/v1/orders: ``stopPriceType`` TP | MP | IP (the docs give no default). The legacy field doesn't
set it; ccxt 4.4.65 (pinned) sends ``MP`` (triggerPriceType='mark') — defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): the SDK places TP and SL as separate Add Order stop orders (``stop`` up/down,
# ``stopPrice``, ``reduceOnly`` — «If the reduce-only order size exceeds the position size, the extra size will
# be canceled»). The docs describe no link between two stop orders; for st-orders (TP and SL in one request:
# ``triggerStopUpPrice`` — «Take profit price», ``triggerStopDownPrice`` — «Stop loss price») canceling the second
# leg isn't described either; the Get Order List filter has types oco_limit / oco_stop without a description
# (kucoin-universal-sdk spec openapi-futures-order.json, verified 2026-11-04). The KuCoin demo/sandbox is closed — no
# live check: emulation (the SDK cancels the sibling) is safe with any exchange response, verified=False.
OCO = ('emulated', 'https://www.kucoin.com/docs-new/api-3470235', False,
       'separate stop orders: link not described in the docs (st-orders and oco_* too), no demo')

# SL attached to the entry order (open_position): «Add Take Profit And Stop Loss Order»
# POST /api/v1/st-orders — «supports both take-profit and stop-loss functions, and other functions are exactly
# the same as the place order interface»: entry and TP/SL in one request (ccxt sends an order with stopLoss there).
# Fields — kucoin-universal-sdk spec (openapi-futures-order.json, x-api-doc api-3470237; verified
# 2026-11-04): ``triggerStopUpPrice`` — «Take profit price», ``triggerStopDownPrice`` — «Stop loss price»,
# ``stopPriceType`` TP | MP | IP; ccxt puts the SL into Down and the TP into Up for either side — as in the field
# descriptions. The docs page doesn't describe the fields (only a buy example). Not described: a short's SL (above the
# price) in the «Down» field, behavior on a partial entry fill and on a stop rejection (atomicity), the kind of legs
# among stop orders. There is no KuCoin demo / sandbox — verified=False.
ATTACHED_SL = (True, 'https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-take-profit-and-stop-loss-order',
               False, 'st-orders: entry with TP/SL in one request; fields per the docs spec, no demo — short, partial '
               'fill and atomicity not verified')

# Moving a TP/SL (modify_tp_sl): the docs have no stop order amend; Add Order — «the maximum stop orders for a single
# contract are 200 per account», reduceOnly — the exchange cancels the excess over the position: two stop orders
# coexist — make-before-break. There is no KuCoin demo/sandbox — verified=False.
MODIFY_IN_PLACE = None
TWO_SL = (True, 'https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order', False,
          'up to 200 stop orders per contract; no demo')



def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None.

    Get Order (docs): ``status`` open | done, ``stopTriggered`` — stop fired, ``isActive``.
    """
    status = str(info.get('status') or '').lower()
    if info.get('stopTriggered') is True:
        return 'triggered'
    if status == 'open' or info.get('isActive') is True:
        return 'open'
    if status == 'done':
        return 'canceled'
    return None
