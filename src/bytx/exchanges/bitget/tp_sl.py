"""Bitget USDT-M: position TP/SL — «Stop-profit and stop-loss plan orders».

POST /api/v2/mix/order/place-tpsl-order (docs: https://www.bitget.com/docs/catalog/classic-contract-plan/classic-contract-plan#stop-profit-and-stop-loss-plan-orders):
``planType`` — profit_plan / loss_plan (with ``size``, partial) or pos_profit / pos_loss (whole position, no
size); ``holdSide`` — «Two-way position: long/short; one-way position: buy (long position) / sell (short
position)»; ``triggerType`` — fill_price | mark_price (the docs don't name a default; ccxt uses mark).
"""
from __future__ import annotations

from typing import Optional

DOC = 'https://www.bitget.com/docs/catalog/classic-contract-plan/classic-contract-plan#stop-profit-and-stop-loss-plan-orders'
DEFAULT_TRIGGER_BY = 'mark'
TRIGGER_TYPES = {'mark': 'mark_price', 'last': 'fill_price'}

# OCO TP/SL (bracket): TP and SL are separate place-tpsl-order requests; a «one fires —
# cancel the other» link is not described in the docs — the SDK cancels the sibling. Demo 2026-11-04
# (tests/ground_truth/bitget/demo_oco_legs.json): a fired profit_plan did NOT cancel the loss_plan while the position
# was open (live) — no OCO; when the position is closed the exchange cancels the remaining leg itself (planStatus
# cancelled; in another run the sibling was canceled by the exchange before the algorithm).
OCO = ('emulated', DOC, True,
       'profit_plan / loss_plan — separate requests, not linked (demo 2026-11-04: after TP the SL leg stays alive '
       'while the position is open) — emulation; when the position is closed the exchange cancels the remaining legs '
       '(cancelled)')

# SL on entry: Place Order — presetStopLossPrice «Stop-loss value» (and presetStopSurplusPrice).
ATTACHED_SL = (True, 'https://www.bitget.com/docs/catalog/classic-contract-trade/classic-contract-trade#place-order', True,
               'presetStopLossPrice / presetStopSurplusPrice in Place Order')

# Moving a TP/SL (modify_tp_sl): POST /api/v2/mix/order/modify-tpsl-order («Modify the stop-profit and stop-loss plan
# order»: orderId, triggerPrice, triggerType, size — «For the position take profit and position stop loss orders,
# the size should be ""») — an in-place amend. Demo 2026-11-04 (tests/ground_truth/bitget/demo_tpsl_modify.json): the
# same orderId and clientOid, pos_loss — size "", loss_plan — size. A new clientOid can't be set there.
MODIFY_IN_PLACE = (DOC, True, 'modify-tpsl-order: trigger / price type / size of the same orderId and clientOid '
                              '(demo 2026-11-04)')
# Two SLs on one side (demo 2026-11-04): loss_plan / profit_plan with a size — several, they coexist with each other and
# with pos_loss → make-before-break (a new client_id or a new size). pos_loss / pos_profit («whole position») — one per
# side: a second placement doesn't create an order but replaces the previous trigger (the same orderId, the PREVIOUS
# clientOid). For them make-before-break is impossible, cancel-first is a window without a stop; so a move with the same
# size is always in place, without a window, and the exchange won't apply a new client_id (the order keeps the previous
# one — a warning). A new size — make-before-break: a new loss_plan with a size, then removing pos_loss.
TWO_SL = (True, DOC, True, 'loss_plan / profit_plan with a size — several per side (demo 2026-11-04); pos_loss / '
                           'pos_profit — one per side (a second placement replaces the previous one) — moved in place')
WHOLE_POSITION_PLANS = frozenset({'pos_loss', 'pos_profit'})

# planStatus «Get Pending Trigger Order»: not_trigger / live — waiting; «Get History Trigger Order»: executed — «the
# order triggered», fail_execute — «Failed to trigger», cancelled — removed (docs orders-plan-pending /
# orders-plan-history; demo 2026-11-04: live, executed, cancelled)
_STATES = {'not_trigger': 'open', 'live': 'open', 'triggered': 'triggered', 'executed': 'triggered',
           'fail_trigger': 'canceled', 'fail_execute': 'canceled', 'cancel': 'canceled', 'cancelled': 'canceled'}

# a fired plan order is filled by another order: executeOrderId — «the order ID after plan triggered»
# (Get History Trigger Order)
FILL_ORDER_ID = ('executeOrderId',)

HISTORY_DOC = 'https://www.bitget.com/docs/catalog/classic-contract-plan/classic-contract-plan#get-history-trigger-order'


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state by ``planStatus``; no field — None."""
    return _STATES.get(str(info.get('planStatus') or '').lower())
