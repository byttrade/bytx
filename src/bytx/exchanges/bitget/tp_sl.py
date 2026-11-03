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
# cancel the other» link is not described in the docs — the SDK cancels the sibling.
OCO = ('emulated', DOC, False, 'pos_profit / pos_loss — separate requests')

# SL on entry: Place Order — presetStopLossPrice «Stop-loss value» (and presetStopSurplusPrice).
ATTACHED_SL = (True, 'https://www.bitget.com/docs/catalog/classic-contract-trade/classic-contract-trade#place-order', True,
               'presetStopLossPrice / presetStopSurplusPrice in Place Order')

# planStatus «Get Pending Trigger Order»: not_trigger — waiting, triggered — fired, fail_trigger,
# cancel — removed (docs orders-plan-pending / orders-plan-history)
_STATES = {'not_trigger': 'open', 'live': 'open', 'triggered': 'triggered', 'executed': 'triggered',
           'fail_trigger': 'canceled', 'cancel': 'canceled', 'cancelled': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state by ``planStatus``; no field — None."""
    return _STATES.get(str(info.get('planStatus') or '').lower())
