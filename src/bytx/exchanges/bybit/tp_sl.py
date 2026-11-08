"""Bybit v5: position TP/SL.

POST /v5/position/trading-stop: ``tpTriggerBy`` / ``slTriggerBy`` — the docs give no default —
we send MarkPrice explicitly, defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): in Full trading-stop mode TP/SL are position-level (enum cancelType
# CancelByOCOTpCanceledBySlTriggered / …SlCanceledByTpTriggered), but the bracket places legs on their own amount
# (Partial, one request per leg) — the link between such legs isn't described in the docs. Demo 2026-11-04
# (tests/ground_truth/bybit/demo_oco_partial.json): a fired PartialTakeProfit did NOT cancel the
# PartialStopLoss while the position was open (Untriggered) — no OCO, the SDK cancels the sibling; when the
# position is closed the exchange cancels the remaining legs itself (Deactivated, cancelType CancelByTpSlTsClear).
# The exchange replaces a full-position SL (trading-stop, tpslMode=Full) in place with the same placement (docs v5
# «Set Trading Stop»): moving it — modify_tp_sl; a separate cancel of the «previous» one would remove the live stop.
SL_IN_PLACE = True
OCO = ('emulated', 'https://bybit-exchange.github.io/docs/v5/position/trading-stop', True,
       'Partial legs are not linked (demo 2026-11-04: after TP the SL leg stays alive while the position is open) — '
       'emulation; when the position is closed the exchange cancels the remaining legs (CancelByTpSlTsClear)')

# SL attached to the entry order (open_position): Create Order ``stopLoss`` / ``takeProfit``
# («You can set TP/SL when placing an order»); ``tpslMode`` not set — per docs Full: SL on the whole position
# (open_position only opens a new position, so it is that position's SL).
ATTACHED_SL = (True, 'https://bybit-exchange.github.io/docs/v5/order/create-order', True,
               'stopLoss in Create Order')

# Moving a TP/SL (modify_tp_sl): Full — in place via the same /v5/position/trading-stop («Full: This API can be used to
# modify the parameters of existing TP/SL orders»; demo 2026-11-04 WLD: the StopLoss orderId is the same, the trigger
# is new); Partial — «This API can only add partial position TP/SL orders»: a new leg is added to the previous ones
# (demo: two PartialStopLoss on the whole position and a Full StopLoss stand at once) — make-before-break.
MODIFY_IN_PLACE = ('https://bybit-exchange.github.io/docs/v5/position/trading-stop', True,
                   'tpslMode=Full: the same placement changes the trigger in place (the orderId stays)')
TWO_SL = (True, 'https://bybit-exchange.github.io/docs/v5/position/trading-stop', True,
          'Partial adds legs (demo 2026-11-04: Partial + Partial + Full at once)')


# orderStatus (docs v5 Enums): Untriggered — conditional order created; Triggered — instant transition
# Untriggered → New; Deactivated — removed before triggering; Filled / Cancelled / Rejected — final.
_STATE = {'Untriggered': 'open', 'Triggered': 'triggered', 'New': 'triggered', 'PartiallyFilled': 'triggered',
          'Filled': 'triggered', 'Deactivated': 'canceled', 'Cancelled': 'canceled', 'Rejected': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('orderStatus') or ''))


# The fill order of a triggered TP/SL is THE SAME order: a trading-stop leg (Full and Partial) is a position conditional
# order, on the trigger it becomes a market order itself (orderStatus Untriggered → Triggered → Filled), the trade
# (execution) goes under its orderId (demo 2026-11-04, APT: PartialTakeProfit, createType CreateByPartialTakeProfit;
# tests/ground_truth/bybit/demo_tp_sl_fired_same_id.json). There is no separate order — FILL_ORDER_ID is empty,
# TpSlOrder.fill_order_id is None.
FILL_ORDER_ID: tuple = ()
