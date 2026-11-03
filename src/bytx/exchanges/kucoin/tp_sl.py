"""KuCoin Futures: stop orders.

POST /api/v1/orders: ``stopPriceType`` TP | MP | IP (the docs give no default). The legacy field doesn't
set it; ccxt 4.4.65 (pinned) sends ``MP`` (triggerPriceType='mark') — defaults to mark.
"""
from __future__ import annotations

from typing import Optional

DEFAULT_TRIGGER_BY = 'mark'

# OCO TP/SL (bracket): the SDK places TP and SL as separate stop orders (takeProfitPrice /
# stopLossPrice, reduce-only); a link between separate stop orders isn't described in the docs — the SDK cancels the sibling.
OCO = ('emulated', 'https://www.kucoin.com/docs-new/api-3470235', False, 'separate stop orders')

# SL attached to the entry order (open_position): «Add Take Profit And Stop Loss Order»
# POST /api/v1/st-orders (``triggerStopUpPrice`` / ``triggerStopDownPrice``; ccxt sends an order with
# stopLoss there). The field meaning is only in the docs example — verified=False until demo.
ATTACHED_SL = (True, 'https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-take-profit-and-stop-loss-order',
               False, 'st-orders: entry with TP/SL in one request')



def state(info: dict) -> Optional[str]:
    """Normalized TP/SL state: 'open' / 'triggered' / 'canceled' or None.

    Get Order (docs): ``status`` open | done, ``stopTriggered`` — stop fired, ``isActive``.
    """
    status = str(info.get('status') or '').lower()
    if info.get('stopTriggered') is True:
        return 'triggered'
    if status == 'open' or info.get('isActive') is True:
        return 'open'
    if status == 'done':
        return 'canceled'
    return None
