"""MEXC futures: order origin signals for the position journal.

Source — order fields of Get Order by External ID / order/get (https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/get-order-by-external-id;
ground truth ``tests/ground_truth/mexc/order_external.json``): ``category`` — «Order category:1 limit,2 liquidation
custody,3 custody close,4 ADL reduction»; ``side`` — «1 open long,2 close short,3 open short,4 close long»;
``externalOid`` — own id. The docs have no «order from exchange UI» signal.
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'mexc'
AUTO_CLIENT_ID = False
CLIENT_ID_REQUIRED = False
CLIENT_ID_FIELD = 'externalOid'
LIQ_CATEGORIES = frozenset({2, 3})
ADL_CATEGORIES = frozenset({4})
_LONG_SIDES = frozenset({1, 4})


def client_id_of(info: dict) -> Optional[str]:
    value = info.get('externalOid')
    return value if isinstance(value, str) and value else None


def _category(info: dict) -> Optional[int]:
    try:
        return int(info.get('category'))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _system(info: dict) -> Optional[Signal]:
    category = _category(info)
    if category in ADL_CATEGORIES:
        return Signal('adl', f'mexc: category={category}', event='adl')
    if category in LIQ_CATEGORIES:
        return Signal('liquidation', f'mexc: category={category}', event='liquidated')
    return None


def order_signal(info: dict) -> Optional[Signal]:
    return _system(info)


def fill_signal(info: dict) -> Optional[Signal]:
    return _system(info)      # order_deals: the same category


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def fill_position_side(info: dict) -> Optional[str]:
    try:
        side = int(info.get('side'))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return 'long' if side in _LONG_SIDES else 'short'
