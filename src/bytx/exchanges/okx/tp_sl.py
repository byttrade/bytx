"""OKX v5: algo conditional.

``tpTriggerPxType`` / ``slTriggerPxType``: last | index | mark, «The default is last» — defaults to last.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'last'

# OCO TP/SL (bracket): set_tp_sl without own ids places ONE ``conditional`` algo with
# tpTriggerPx and slTriggerPx (tests/ground_truth/okx/algo_order_details.json: both fields on one algoId,
# state live → effective) — only one leg can fire, no need to cancel the sibling.
# With ids set, the legs are separate algo orders: then emulation.
OCO = ('native_pair', 'https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-get-algo-order-details',
       True, 'TP and SL are a single conditional algo order')

# SL attached to the entry order (open_position): Place order ``attachAlgoOrds[]``
# (``slTriggerPx``): «posted … when placing the attached algo order once the general order is filled
# completely» — attaching only works for an entry that fills completely (market).
ATTACHED_SL = (True, 'https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order', True,
               'attachAlgoOrds; the algo is placed after the entry fills completely')


# algo order state (Get algo order details: example "live"). The enum section can't be read
# via WebFetch — other values as in ccxt 4.4.65 OKX parse_order_status (effective = fired).
_STATE = {'live': 'open', 'pause': 'open', 'partially_effective': 'triggered', 'effective': 'triggered',
          'canceled': 'canceled', 'order_failed': 'canceled', 'partially_failed': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('state') or ''))


# Id of the fill order of a triggered algo (``ordIdList``, the deprecated ``ordId``) — closing trades go under it
# (demo 2026-10-27: ordIdList of the effective order = ordId of the closing trade, tests/ground_truth/okx/demo_algo_effective.json).
FILL_ORDER_ID = ('ordIdList', 'ordId')
