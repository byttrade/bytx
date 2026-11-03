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
