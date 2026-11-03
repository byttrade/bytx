"""Hyperliquid: no numeric error codes (response ``{"status": "err", "response": "<text>"}``,
https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint).

Outcome by text — shared ``bytx.outcome`` signals («timed out» etc.).
"""
DOCS = 'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint'

UNKNOWN_OUTCOME: frozenset = frozenset()
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
DUPLICATE_CLIENT_ID: frozenset = frozenset()
POSITION_CLOSED: frozenset = frozenset()
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
REDUCE_EXCEEDS: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()

# Canceling an order that no longer exists: no code, status {"error": "Order was never
# placed, already canceled, or filled."} — ccxt 4.4.65 raises OrderNotFound itself
ORDER_NOT_OPEN: frozenset = frozenset()
