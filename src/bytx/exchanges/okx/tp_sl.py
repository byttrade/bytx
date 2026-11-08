"""OKX v5: algo conditional, a TP+SL pair — oco.

``tpTriggerPxType`` / ``slTriggerPxType``: last | index | mark, «The default is last» — defaults to last.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'last'

# OCO TP/SL (bracket): set_tp_sl without own ids places ONE ``oco`` algo with tpTriggerPx and
# slTriggerPx — only one leg can fire, no need to cancel the sibling. Not ``conditional``: Place algo
# order — for a net conditional order with both TP and SL «only stop-loss logic will be performed and take-profit
# logic will be ignored» (demo 2026-11-04: the TP of a conditional pair did not fire, the oco one did;
# tests/ground_truth/okx/demo_tp_sl_pair_conditional_vs_oco.json). The exchange also keeps a TP+SL from the
# entry's attachAlgoOrds as an oco order (okx/demo_attached_oco.json) — the TP/SL list requests
# ordType=conditional,oco. With ids set, the legs are separate conditional algo orders: then emulation.
OCO = ('native_pair', 'https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-place-algo-order',
       True, 'TP and SL are a single oco algo order (a conditional one with both executes only the SL in one-way)')

# SL attached to the entry order (open_position): Place order ``attachAlgoOrds[]``
# (``slTriggerPx``): «posted … when placing the attached algo order once the general order is filled
# completely» — attaching only works for an entry that fills completely (market). Demo
# 2026-11-04: SL only — a conditional order, TP and SL — oco (reduceOnly, entry sz);
# the exchange cancels it when the position is closed.
ATTACHED_SL = (True, 'https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order', True,
               'attachAlgoOrds; the algo is placed after the entry fills completely')

# Moving a TP/SL (modify_tp_sl): POST /api/v5/trade/amend-algos — «Amend unfilled algo orders (Support Stop order
# and Trigger order only…)»: newSz, newTpTriggerPx / newSlTriggerPx (+ OrdPx), the same algoId and algoClOrdId;
# a rejection — the item's sCode, the order is unchanged (cxlOnFail is false by default). Demo 2026-11-04 SOL
# (tests/ground_truth/okx/amend_algos.json): the amend is visible immediately in GET order-algo, a price on the wrong
# side — 51280, the previous trigger stays. 51528 «Unable to modify trigger price type» — another trigger_by is not
# amended, nor is a new client_id (algoClOrdId doesn't change) — then make-before-break: «TP/SL order: 100 per
# instrument» (Trading account — limits), two conditional SL per position stand at once (the same demo).
MODIFY_IN_PLACE = ('https://www.okx.com/docs-v5/en/#order-book-trading-algo-trading-post-amend-algo-order', True,
                   'amend-algos: a new trigger / size on the same algoId (the price type and algoClOrdId do not change)')
TWO_SL = (True, 'https://www.okx.com/docs-v5/en/#overview-trading-account-limits', True,
          'up to 100 TP/SL per instrument; closeFraction (not used by the SDK) — one per position')


# algo order state (Get algo order details: example "live"). The enum section can't be read
# via WebFetch — other values as in ccxt 4.4.65 OKX parse_order_status (effective = fired).
_STATE = {'live': 'open', 'pause': 'open', 'partially_effective': 'triggered', 'effective': 'triggered',
          'canceled': 'canceled', 'order_failed': 'canceled', 'partially_failed': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('state') or ''))


# Ids of the fill orders of a triggered algo — closing trades go under them, not under algoId. Get algo order details:
# ``ordIdList`` — «Order ID list. There will be multiple order IDs when there is TP/SL splitting order» (all of them —
# in TpSlOrder.fill_order_ids), ``ordId`` — «Latest order ID. It will be deprecated soon». The reverse link is in the
# fill order itself (Get order details: ``algoId`` — «There will be a value when algo order is triggered», and
# ``algoClOrdId``). Demo 2026-10-27 (SOL, orders-algo-history) and 2026-11-04 (DOGE, a bot's TP leg, GET
# /trade/order-algo): ordIdList = ordId of the closing trade; the trade has its own exchange clOrdId «O…»
# (tests/ground_truth/okx/demo_algo_triggered_fill.json).
FILL_ORDER_ID = ('ordIdList', 'ordId')
