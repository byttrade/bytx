"""BloFin USDT-M: order origin signals for the position journal.

Source — Get Order Detail / Get Order History fields (F#get-order-detail, F#get-order-history, fixtures
``tests/ground_truth/blofin/order_detail.json``, ``demo_filled_fills_close.json``): ``orderCategory`` —
«normal / full_liquidation / partial_liquidation / adl / tp / sl»; ``algoId`` / ``algoClientOrderId`` — the conditional
order (TP/SL, trigger) that spawned this order; ``clientOrderId`` — own id. The response has no source (web / api) —
an order from the exchange UI cannot be told apart from another API order.
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'blofin'
AUTO_CLIENT_ID = False
CLIENT_ID_REQUIRED = False
CLIENT_ID_FIELD = 'clientOrderId'
_LIQUIDATION = frozenset({'full_liquidation', 'partial_liquidation'})
_CHILD = frozenset({'tp', 'sl'})


def client_id_of(info: dict) -> Optional[str]:
    value = info.get('clientOrderId')
    return value if isinstance(value, str) and value else None


def _system(info: dict) -> Optional[Signal]:
    category = str(info.get('orderCategory') or '').lower()
    if category in _LIQUIDATION:
        return Signal('liquidation', f'blofin: orderCategory={category}', event='liquidated')
    if category == 'adl':
        return Signal('adl', 'blofin: orderCategory=adl', event='adl')
    return None


def _child(info: dict) -> Optional[Signal]:
    category = str(info.get('orderCategory') or '').lower()
    algo = str(info.get('algoId') or '')
    if category in _CHILD or algo:
        return Signal(None, f'blofin: orderCategory={category or "-"} algoId={algo or "-"} — fill order of a '
                            'conditional order', child=True, parent_id=algo or None,
                      parent_client_id=str(info.get('algoClientOrderId') or '') or None)
    return None


def order_signal(info: dict) -> Optional[Signal]:
    return _system(info) or _child(info)


def fill_signal(info: dict) -> Optional[Signal]:
    return _system(info)


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def fill_position_side(info: dict) -> Optional[str]:
    side = str(info.get('positionSide') or '').lower()
    return side if side in ('long', 'short') else None
