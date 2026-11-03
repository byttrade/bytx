"""HTX USDT-M: order origin signals for the position journal.

Source — order fields of [Cross] Get Information of order (https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-get-information-of-order):
``order_source`` — «system、web、api、m、risk、settlement、ios、android、windows、mac、trigger、tpsl»; ``order_type`` —
«1. Quotation; 2. Cancelled order; 3. Forced liquidation; 4. Delivery Order; 22. ADL reduction-only order»;
``liquidation_type`` — «0: Non-liquidated,1: Long and short netting,2: Partial liquidated,3: Full liquidated»;
``client_order_id`` — own id (integer), ``direction`` / ``offset`` — position side in hedge mode.
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'htx'
AUTO_CLIENT_ID = False
CLIENT_ID_REQUIRED = False
CLIENT_ID_FIELD = 'client_order_id'
UI_SOURCES = frozenset({'web', 'm', 'ios', 'android', 'windows', 'mac'})
LIQ_SOURCES = frozenset({'risk'})
# «“tpsl” indicates triggered by take-profit and stop-loss» (changelog 1.0.6, items 19/21/27/29), trigger — conditional
# order: the fill order of a triggered TP/SL is a child and has no cid (parent id — the TP/SL relation_order_id)
CHILD_SOURCES = frozenset({'tpsl', 'trigger'})


def client_id_of(info: dict) -> Optional[str]:
    value = info.get('client_order_id', info.get('client-order-id'))
    return str(value) if value not in (None, '', 0) else None


def _system(info: dict) -> Optional[Signal]:
    order_type = str(info.get('order_type') or '')
    source = str(info.get('order_source') or '').lower()
    liq = str(info.get('liquidation_type') or '0')
    if order_type == '22':
        return Signal('adl', f'htx: order_type={order_type}', event='adl')
    if order_type == '3' or source in LIQ_SOURCES or liq not in ('', '0'):
        return Signal('liquidation', f'htx: order_type={order_type or "-"} order_source={source or "-"} '
                                     f'liquidation_type={liq}', event='liquidated')
    return None


def order_signal(info: dict) -> Optional[Signal]:
    found = _system(info)
    if found is not None:
        return found
    source = str(info.get('order_source') or '').lower()
    if source in CHILD_SOURCES:
        return Signal(None, f'htx: order_source={source} — fill order of a conditional (TP/SL)', child=True)
    if source in UI_SOURCES:
        return Signal('exchange_ui', f'htx: order_source={source}')
    return None


def fill_signal(info: dict) -> Optional[Signal]:
    found = _system(info)
    if found is None and str(info.get('order_source') or '').lower() in CHILD_SOURCES:
        return Signal(None, f'htx: order_source={info.get("order_source")} — conditional (TP/SL) fill',
                      child=True)
    return found


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def fill_position_side(info: dict) -> Optional[str]:
    offset = str(info.get('offset') or '')
    direction = str(info.get('direction') or '')
    if offset not in ('open', 'close') or direction not in ('buy', 'sell'):
        return None    # one-way (offset both) — side by position sign
    opens_long = direction == 'buy'
    return ('long' if opens_long else 'short') if offset == 'open' else ('short' if opens_long else 'long')
