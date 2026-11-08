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
# Binance has no separate «no position» code. -2022 REDUCE_ONLY_REJECT «ReduceOnly Order is rejected.» on a
# closing order (reduce-only; in hedge — an order of the positionSide side, reduceOnly is not sent) — the same as Bybit
# 110017: demo 2026-11-04 (hedge) — no side: -2022; a closing order larger than the side is silently trimmed by the
# exchange; the side fully covered by resting closing orders: -2022. Do not retry: check the position and orders.
# Without reduce-only the code stays InvalidOrder.
POSITION_CLOSED: frozenset = frozenset()
POSITION_CLOSED_IF_REDUCE = frozenset({'-2022'})
REDUCE_EXCEEDS: frozenset = frozenset()
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()

# Canceling an order no longer in the book: -2011 CANCEL_REJECTED «Cancel request
# failure as open order not found in the orderbook: "Unknown order sent".»; -2013 NO_SUCH_ORDER
# «Order does not exist.» (ccxt 4.4.65 maps -2011 to OperationRejected, not OrderNotFound)
ORDER_NOT_OPEN = frozenset({'-2011', '-2013'})
