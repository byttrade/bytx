"""KuCoin Futures: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

POST_ONLY_CROSS = ('reject', 'https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order', False,
                   'postOnly crossing not described in Add Order docs — as before (reject), to verify on demo')
MARKET_NO_LIQUIDITY = ('reject', 'https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order', False,
                       'not described — as before (reject), to verify on demo')
