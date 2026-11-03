"""Hyperliquid: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

POST_ONLY_CROSS = ('cancel', 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint',
                   True, 'Alo: «ALO (add liquidity only, i.e. "post only") will be canceled instead of immediately '
                   'matching»; response format (canceled order or error in statuses) — to verify on demo')
MARKET_NO_LIQUIDITY = ('reject', 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint',
                       False, 'HL market is an IOC limit with a price; no-liquidity case not described, as before (reject)')
