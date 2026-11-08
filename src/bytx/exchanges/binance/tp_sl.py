"""Binance USDⓈ-M: TP/SL.

POST /fapi/v1/algoOrder: ``workingType`` MARK_PRICE | CONTRACT_PRICE, «Default: CONTRACT_PRICE»
(New Algo Order docs) — defaults to last.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'last'

# OCO TP/SL (bracket): Algo API conditional orders are independent — New Algo Order docs have no
# TP↔SL link, ``closePosition`` is incompatible with ``quantity`` (can't close a partial-position bracket with it).
# When one fires, the other stays: the SDK cancels the sibling (OCO emulation).
OCO = ('emulated', 'https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order',
       True, 'conditional orders are independent; closePosition not allowed with quantity')

# SL attached to the entry order (open_position): New Order (POST /fapi/v1/order) has no
# TP/SL fields — TP/SL are placed via New Algo Order. The SDK places the SL right after entry (with rollback).
ATTACHED_SL = (False, 'https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api',
               True, 'New Order has no TP/SL — SL as a separate algo order after entry')

# Moving a TP/SL (modify_tp_sl, make-before-break): the docs have no amend of a conditional order (Modify Order —
# «currently only LIMIT order modification is supported»). The SDK places TP/SL with ``quantity`` (one-way —
# reduceOnly, hedge — positionSide): such orders coexist (demo 2026-11-04 DOGE: two STOP_MARKET on the whole position
# accepted, tests/ground_truth/binance/demo_two_sl.json) — the new one is placed first, then the previous one removed.
# ``closePosition=true`` — one per side: the second is rejected with -4130 «An open stop or take profit order with GTE
# and closePosition in the direction is existing» (the same demo) — such an SL is moved by a cancel and a new placement
# (a window without a stop — the time of one request), see protection.modify_tp_sl.
MODIFY_IN_PLACE = None
TWO_SL = (True, 'https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Algo-Order',
          True, 'orders with quantity coexist; closePosition — one per side (-4130, demo 2026-11-04)')


# algoStatus (Algo Order API): the docs don't list the values (algoUpdate schema: «X — Algo Status»,
# examples NEW / CANCELED); the others are seen in exchange responses.
_STATE = {'NEW': 'open', 'WORKING': 'open',
          'TRIGGERING': 'triggered', 'TRIGGERED': 'triggered', 'FINISHED': 'triggered', 'FILLED': 'triggered',
          'CANCELED': 'canceled', 'CANCELLED': 'canceled', 'EXPIRED': 'canceled', 'REJECTED': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('algoStatus') or info.get('X') or '').upper())


# Id of the order that filled a triggered TP/SL (Query Algo Order: ``actualOrderId``; algoUpdate stream: ``ai``) —
# closing trades go under it, not under algoId (demo 2026-10-27).
FILL_ORDER_ID = ('actualOrderId', 'ai')
