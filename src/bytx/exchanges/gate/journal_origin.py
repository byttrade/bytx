"""Gate USDT-M: order origin signals for the position journal.

Source — FuturesOrder model (``text``, ``is_liq``, ``finish_as``) of the official APIv4 spec
(https://github.com/gateio/gateapi-python/blob/master/docs/FuturesOrder.md; ground truth ``tests/ground_truth/gate/futures_order.json``):

- ``text`` — own id «Prefixed with `t-`»; service values: «web: Web - api: API call - app: Mobile app -
  auto_deleveraging - liquidation - liq-xxx … - hedge-liq-xxx … - pm_liquidate - comb_margin_liquidate -
  scm_liquidate - insurance»: web / app — exchange UI (``exact``), liquidations and ADL — by value;
- ``is_liq`` — «Is the order for liquidation»; ``finish_as`` auto_deleveraged — «finished by ADL».
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'gate'
AUTO_CLIENT_ID = False
CLIENT_ID_REQUIRED = False
CLIENT_ID_FIELD = 'text'
UI_TEXTS = frozenset({'web', 'app'})
LIQ_TEXTS = frozenset({'liquidation', 'pm_liquidate', 'comb_margin_liquidate', 'scm_liquidate', 'insurance'})
LIQ_PREFIXES = ('liq-', 'hedge-liq-')
ADL_TEXTS = frozenset({'auto_deleveraging'})


def client_id_of(info: dict) -> Optional[str]:
    value = info.get('text')
    return value if isinstance(value, str) and value.startswith('t-') else None


def _system(info: dict) -> Optional[Signal]:
    text = str(info.get('text') or '').lower()
    if text in ADL_TEXTS or str(info.get('finish_as') or '') == 'auto_deleveraged':
        return Signal('adl', f'gate: text={text or "-"}', event='adl')
    if text in LIQ_TEXTS or text.startswith(LIQ_PREFIXES) or info.get('is_liq') is True:
        return Signal('liquidation', f'gate: text={text or "-"} is_liq={info.get("is_liq")}', event='liquidated')
    return None


def order_signal(info: dict) -> Optional[Signal]:
    found = _system(info)
    if found is not None:
        return found
    text = str(info.get('text') or '').lower()
    if text in UI_TEXTS:
        return Signal('exchange_ui', f'gate: text={text}')
    return None


def fill_signal(info: dict) -> Optional[Signal]:
    return _system(info)


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def fill_position_side(info: dict) -> Optional[str]:
    return None    # one-way: position side by the sign of size (dual mode is not supported in the SDK)
