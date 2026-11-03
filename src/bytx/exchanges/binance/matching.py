"""Binance USDⓈ-M: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

POST_ONLY_CROSS = ('reject', 'https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code', True,
                   '-5022 GTX_ORDER_REJECT: «the Post Only order will be rejected»')
MARKET_NO_LIQUIDITY = ('reject', 'https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api',
                       False, 'not described in New Order docs — as before (reject), to verify on demo')
