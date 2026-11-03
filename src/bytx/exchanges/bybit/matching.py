"""Bybit v5: what the exchange does with an order that can't rest as specified (paper).

``POST_ONLY_CROSS`` — post-only would cross the book; ``MARKET_NO_LIQUIDITY`` — market with no opposite
liquidity. Mode: 'reject' — request rejected (no order), 'cancel' — order created and canceled by the exchange
(status 'canceled', no fill). (mode, docs URL, verified against docs text, note).
"""
from __future__ import annotations

POST_ONLY_CROSS = ('cancel', 'https://bybit-exchange.github.io/docs/v5/order/create-order', True,
                   'PostOnly: «If the order would be filled immediately when submitted, it will be cancelled»; '
                   'the request is asynchronous — the order is created and then canceled')
MARKET_NO_LIQUIDITY = ('cancel', 'https://bybit-exchange.github.io/docs/v5/order/create-order', True,
                       'Market: «if there is insufficient liquidity, the order will be cancelled»')
