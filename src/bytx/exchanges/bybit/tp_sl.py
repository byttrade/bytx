"""Bybit v5: position TP/SL.

POST /v5/position/trading-stop: ``tpTriggerBy`` / ``slTriggerBy`` — the docs give no default —
we send MarkPrice explicitly, defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): in Full trading-stop mode TP/SL are position-level (enum cancelType
# CancelByOCOTpCanceledBySlTriggered / …SlCanceledByTpTriggered), but the bracket places legs on their own amount
# (Partial, one request per leg) — the link between such legs isn't described in the docs: the SDK cancels the sibling.
# The exchange replaces a full-position SL (trading-stop, tpslMode=Full) in place with the same placement (docs v5
# «Set Trading Stop»): moving it — modify_tp_sl; a separate cancel of the «previous» one would remove the live stop.
SL_IN_PLACE = True
OCO = ('emulated', 'https://bybit-exchange.github.io/docs/v5/enum#canceltype', False,
       'Partial legs are placed by separate requests; link not described — emulation')

# SL attached to the entry order (open_position): Create Order ``stopLoss`` / ``takeProfit``
# («You can set TP/SL when placing an order»); ``tpslMode`` not set — per docs Full: SL on the whole position
# (open_position only opens a new position, so it is that position's SL).
ATTACHED_SL = (True, 'https://bybit-exchange.github.io/docs/v5/order/create-order', True,
               'stopLoss in Create Order')


# orderStatus (docs v5 Enums): Untriggered — conditional order created; Triggered — instant transition
# Untriggered → New; Deactivated — removed before triggering; Filled / Cancelled / Rejected — final.
_STATE = {'Untriggered': 'open', 'Triggered': 'triggered', 'New': 'triggered', 'PartiallyFilled': 'triggered',
          'Filled': 'triggered', 'Deactivated': 'canceled', 'Cancelled': 'canceled', 'Rejected': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('orderStatus') or ''))
