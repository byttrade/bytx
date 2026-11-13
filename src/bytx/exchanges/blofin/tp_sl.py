"""BloFin USDT-M: position TP/SL — «Place TPSL Order» (F#place-tpsl-order).

POST /api/v1/trade/order-tpsl: TP and/or SL in one request, ``size`` (contracts, -1 — the whole position),
``positionSide`` (net in one-way, long / short in hedge — required), ``tp/slTriggerPriceType`` last | mark | index
(«The default is last» — the SDK sets it explicitly), ``tp/slOrderPrice`` -1 — market execution, ``clientOrderId``.
Response ``data`` — an object ``{tpslId, clientOrderId, code, msg}``. List — orders-tpsl-pending, detail —
order-tpsl-detail, history — orders-tpsl-history (``state`` live | effective | canceled | order_failed), cancel —
cancel-tpsl (an array), in-place edit — amend-tpsl (F#amend-tp%2Fsl-order: ``newTp/SlTriggerPrice``, price type,
``newSize``; id and clientOrderId stay the same).
"""
from __future__ import annotations

from typing import Optional

DOC = 'https://docs.blofin.com/index.html#place-tpsl-order'
DEFAULT_TRIGGER_BY = 'mark'
TRIGGER_TYPES = {'mark': 'mark', 'last': 'last'}

# TP and SL in one order-tpsl request — one tpslId (like an OKX oco pair): demo 2026-11-09 — the legs of the pair live
# as one request, the exchange removes it itself when the position closes; only one leg can trigger
OCO = ('native_pair', DOC, True, 'TP and SL in one order-tpsl request — one tpslId (demo 2026-11-09: one request, '
                                 'removed by the exchange when the position closed)')

# SL on entry: Place Order — tpTriggerPrice / slTriggerPrice (+ OrderPrice, -1 — market)
ATTACHED_SL = (True, 'https://docs.blofin.com/index.html#place-order', False,
               'tpTriggerPrice / slTriggerPrice in Place Order (not verified on demo)')

# Move: amend-tpsl — in place (same tpslId); a whole-position stop (size -1) is moved by the trailing algo the same way
MODIFY_IN_PLACE = ('https://docs.blofin.com/index.html#amend-tp%2Fsl-order', False,
                   'amend-tpsl: trigger / price type / size of the same tpslId (clientOrderId does not change)')
SL_IN_PLACE = True
TWO_SL = (True, DOC, False, 'order-tpsl with a size — several per position (not verified)')

# state: live — waiting; effective — triggered; canceled; order_failed — not filled (F#get-tpsl-order-history)
_STATES = {'live': 'open', 'effective': 'triggered', 'canceled': 'canceled', 'order_failed': 'canceled'}
# the fill order of a triggered TP/SL is not given in the TP/SL record (link — algoId / orderCategory on the order):
# Exchange.tp_sl_order finds it in orders-history (protection._blofin_fill_order_ids, demo 2026-11-10)
FILL_ORDER_ID: tuple = ()


def state(info: dict) -> Optional[str]:
    return _STATES.get(str(info.get('state') or '').lower())
