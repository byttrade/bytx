"""HTX USDT-M error codes that determine an order's outcome.

Error response — ``{"status": "error", "err_code": 1047, "err_msg": …}`` (:func:`bytx.errors.exchange_codes`
reads ``err_code``). Source — «Details of Each Error Code» https://huobiapi.github.io/docs/usdt_swap/v1/en/#error-code
(fixture ``tests/ground_truth/htx/error_codes.json``).
"""
DOCS = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#error-code'

# 1000 «System error.», 1004 «System busy. Please try again later.», 1046 / 1108 «Abnormal service» — the order may have been placed
UNKNOWN_OUTCOME = frozenset({'1000', '1004', '1046', '1108'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 1050 «Customer's order number is repeated. Please try again later.»
DUPLICATE_CLIENT_ID = frozenset({'1050'})
POSITION_CLOSED: frozenset = frozenset()
# 1048 «Insufficient close amount available.» — the closing order's position is already gone
POSITION_CLOSED_IF_REDUCE = frozenset({'1048'})
# 1492 «Amount of Reduce Only order exceeds the amount available to close.»
REDUCE_EXCEEDS = frozenset({'1492'})
# 1047 «Insufficient margin available.»
INSUFFICIENT_FUNDS = frozenset({'1047'})
# 1017 «Order doesn't exist.», 1051 «No orders to cancel.», 1061 «This order doesn't exist.», 1063 «The order has
# been executed.», 1071 «Repeated cancellation. Your order has been canceled.» (on cancel — data.errors[].err_code)
ORDER_NOT_OPEN = frozenset({'1017', '1051', '1061', '1063', '1071'})
