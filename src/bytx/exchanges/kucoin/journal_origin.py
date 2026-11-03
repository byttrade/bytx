"""KuCoin Futures: order origin signals for the position journal.

Source — the official KuCoin spec (ground truth:
``tests/ground_truth/kucoin/journal_origin.json``):

- Get Trade History ``/api/v1/fills``: ``tradeType`` — ``trade`` / ``liquid`` / ``adl`` /
  ``settlement``; WS ``/contractMarket/tradeOrders``: ``tradeType`` — «normal order or liquid
  order» (``trade`` / ``liquid``);
- Add Order: ``clientOid`` is a required API parameter: an order without it wasn't placed via the order
  API (most likely the exchange UI), but the docs have no direct UI signal (``heuristic``).
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'kucoin'
AUTO_CLIENT_ID = False
CLIENT_ID_REQUIRED = True
CLIENT_ID_FIELD = 'clientOid'
TRADE_TYPES: dict[str, tuple[str, Optional[str]]] = {
    'liquid': ('liquidation', 'liquidated'), 'adl': ('adl', 'adl'), 'settlement': ('settlement', None)}


def client_id_of(info: dict) -> Optional[str]:
    value = info.get('clientOid')
    return value if isinstance(value, str) and value else None


def order_signal(info: dict) -> Optional[Signal]:
    tt = info.get('tradeType')
    if tt in TRADE_TYPES:
        origin, event = TRADE_TYPES[tt]
        return Signal(origin, f'kucoin: tradeType={tt}', event=event)
    return None


def fill_signal(info: dict) -> Optional[Signal]:
    return order_signal(info)


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def fill_position_side(info: dict) -> Optional[str]:
    ps = str(info.get('positionSide') or '').upper()
    return {'LONG': 'long', 'SHORT': 'short'}.get(ps)
