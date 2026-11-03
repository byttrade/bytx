"""Binance USDⓈ-M error codes that determine an order's outcome.

Source — the official code table:
https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code
(verified 2026-10-25, fixture ``tests/ground_truth/binance/error_codes.json``).
"""
DOCS = 'https://developers.binance.com/docs/derivatives/usds-margined-futures/error-code'

# -1006 UNEXPECTED_RESP «Execution status unknown»; -1007 TIMEOUT «Send status unknown;
# execution status unknown» — the order may have been accepted
UNKNOWN_OUTCOME = frozenset({'-1006', '-1007'})
# -4116 DUPLICATED_CLIENT_ORDER_ID «clientOrderId is duplicated» — an order with this id already exists
DUPLICATE_CLIENT_ID = frozenset({'-4116'})
# Binance has no «no position» code: -2022 REDUCE_ONLY_REJECT is a conflict with open orders
POSITION_CLOSED: frozenset = frozenset()
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
REDUCE_EXCEEDS: frozenset = frozenset()
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()

# Canceling an order no longer in the book: -2011 CANCEL_REJECTED «Cancel request
# failure as open order not found in the orderbook: "Unknown order sent".»; -2013 NO_SUCH_ORDER
# «Order does not exist.» (ccxt 4.4.65 maps -2011 to OperationRejected, not OrderNotFound)
ORDER_NOT_OPEN = frozenset({'-2011', '-2013'})
