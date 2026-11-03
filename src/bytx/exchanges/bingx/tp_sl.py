"""BingX: TAKE_PROFIT_MARKET / STOP_MARKET.

POST /openApi/swap/v2/trade/order: ``workingType`` «default set to MARK_PRICE». The legacy field isn't sent
(exchange default MARK_PRICE) — defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): TAKE_PROFIT_MARKET and STOP_MARKET are separate create_order orders;
# no link between them found in the Place order docs (the page is script-rendered, verification incomplete) —
# the SDK cancels the sibling.
OCO = ('emulated', 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order', False,
       'separate conditional orders')

# SL attached to the entry order (open_position): Place order ``stopLoss`` / ``takeProfit``
# (JSON string, «Support setting take profit/stop loss while placing an order»). Field descriptions in the docs
# are shifted by one row (the stopLoss text sits next to the neighboring field) — verified=False until demo.
ATTACHED_SL = (True, 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order', False,
               'stopLoss in Place order (field descriptions in docs are shifted)')


# status (Query Order details): NEW / PENDING / FILLED / PARTIALLYFILLED / CANCELLED / FAILED
_STATE = {'NEW': 'open', 'PENDING': 'open', 'TRIGGERED': 'triggered', 'FILLED': 'triggered',
          'PARTIALLYFILLED': 'triggered', 'PARTIALLY_FILLED': 'triggered',
          'CANCELLED': 'canceled', 'CANCELED': 'canceled', 'FAILED': 'canceled', 'EXPIRED': 'canceled'}


def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None."""
    return _STATE.get(str(info.get('status') or '').upper())
