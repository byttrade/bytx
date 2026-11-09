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
# Explicit rejection before the engine (spot, https://developers.binance.com/docs/binance-spot-api-docs/errors, checked
# 2026-11-08; the same codes in the USDⓈ-M docs): -1013 INVALID_MESSAGE «The request is rejected by the API. (i.e. The
# request didn't reach the Matching Engine.)» (incl. «Filter failure: …»), -1020 UNSUPPORTED_OPERATION, -1100…-1106
# (characters, count, mandatory, extra/empty parameters), -1108 PARAM_OVERFLOW, -1111 BAD_PRECISION, -1114…-1118
# (timeInForce, orderType, side, empty client id), -1128 OPTIONAL_PARAMS_BAD_COMBO, -1130 INVALID_PARAMETER,
# -1134 BAD_STRATEGY_TYPE. ccxt 4.4.65 gives them BadRequest (→ ExchangeError, reconciliation) — refined to
# InvalidOrder, no reconciliation
INVALID_ORDER = frozenset({'-1013', '-1020', '-1100', '-1101', '-1102', '-1103', '-1104', '-1105', '-1106', '-1108',
                           '-1111', '-1114', '-1115', '-1116', '-1117', '-1118', '-1128', '-1130', '-1134'})
