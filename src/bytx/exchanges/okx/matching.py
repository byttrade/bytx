"""OKX: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

POST_ONLY_CROSS = ('cancel', 'https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order', False,
                   'post_only: «can only provide liquidity to the market and be a maker»; that a crossing order '
                   'is accepted and then canceled — docs text not found, to verify on demo (cancelSource)')
MARKET_NO_LIQUIDITY = ('reject', 'https://www.okx.com/docs-v5/en/#order-book-trading-trade-post-place-order', False,
                       'not described in docs — as before (reject), to verify on demo')
