"""Gate APIv4 error labels that determine an order's outcome.

Gate responds with ``{"label": "...", "message": "..."}`` — the error code is the label (:func:`bytx.errors.exchange_codes`
reads ``label`` too). Source — label tables in the docs https://www.gate.com/docs/developers/apiv4/en/ (verified
2026-10-26, fixture ``tests/ground_truth/gate/error_codes.json``).
"""
DOCS = 'https://www.gate.com/docs/developers/apiv4/en/'

# SERVER_ERROR / INTERNAL «Internal server error», TOO_BUSY «Server is too busy at the moment»,
# SERVER_TIMEOUT «Service timeout» — the order may have been placed
UNKNOWN_OUTCOME = frozenset({'SERVER_ERROR', 'INTERNAL', 'TOO_BUSY', 'SERVER_TIMEOUT'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# DUPLICATE_REQUEST «Duplicate request»
DUPLICATE_CLIENT_ID = frozenset({'DUPLICATE_REQUEST'})
# POSITION_EMPTY «Position is empty», POSITION_NOT_FOUND «Position not found»
POSITION_CLOSED = frozenset({'POSITION_EMPTY', 'POSITION_NOT_FOUND'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
# REDUCE_EXCEEDED «Reduce order would exceed position in dual-mode»
REDUCE_EXCEEDS = frozenset({'REDUCE_EXCEEDED'})
# INSUFFICIENT_AVAILABLE / BALANCE_NOT_ENOUGH «Balance is not enough»
INSUFFICIENT_FUNDS = frozenset({'INSUFFICIENT_AVAILABLE', 'BALANCE_NOT_ENOUGH'})
# Canceling an order that no longer exists: ORDER_NOT_FOUND «Order not found», ORDER_CLOSED «Order already
# closed», ORDER_FINISHED «Order already finished»
ORDER_NOT_OPEN = frozenset({'ORDER_NOT_FOUND', 'ORDER_CLOSED', 'ORDER_FINISHED'})
