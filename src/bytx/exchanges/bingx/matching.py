"""BingX: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

# Error codes docs-v3: 101215 «Post Only order was cancelled because it would have been filled immediately»;
# demo 2026-11-04: the response to the Place order itself is error 101215, no order id — rejection
POST_ONLY_CROSS = ('reject', 'https://bingx-api.github.io/docs-v3/', True,
                   '101215 on placement (Error codes docs + demo 2026-11-04): no order')
MARKET_NO_LIQUIDITY = ('reject', 'https://bingx-api.github.io/docs-v3/#/en/Swap/Trades%20Endpoints/Place%20order',
                       False, 'not described — as before (reject), to verify on demo')
