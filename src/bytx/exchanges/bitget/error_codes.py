"""Bitget (classic API v2) error codes that determine an order's outcome.

Source — «REST API Error Code» (https://www.bitget.com/docs/classic/error-code/restapi.md, verified
2026-10-26, fixture ``tests/ground_truth/bitget/error_codes.json``).
"""
DOCS = 'https://www.bitget.com/docs/classic/error-code/restapi.md'

# 40010 «Request timed out» — ccxt 4.4.65 raises AuthenticationError (ccxt comment: «sign signature
# error»), per docs it's a timeout: the order may have been placed — outcome unknown
UNKNOWN_OUTCOME = frozenset({'40010'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 43118 «clientOrderId duplicate»
DUPLICATE_CLIENT_ID = frozenset({'43118'})
# 22002 «No position to close»
POSITION_CLOSED = frozenset({'22002'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
# 40757 «Not enough position is available.»
REDUCE_EXCEEDS = frozenset({'40757'})
# 40798 «Insufficient contract account balance», 40800 «Insufficient amount of margin» — ccxt 4.4.65
# doesn't know them (bare ExchangeError)
INSUFFICIENT_FUNDS = frozenset({'40798', '40800'})
# Canceling an order that no longer exists: 22001 «No order to cancel», 43004 «There is no order
# to cancel», 43001 «The order does not exist»
ORDER_NOT_OPEN = frozenset({'22001', '43004', '43001'})
