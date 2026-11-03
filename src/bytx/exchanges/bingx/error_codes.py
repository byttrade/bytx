"""BingX perpetual error codes that determine an order's outcome.

Source — docs-v3 code table (https://bingx-api.github.io/docs-v3/, bundle
``static/js/app.a2976421d1f36b01e621.js``; verified 2026-10-25, fixture
``tests/ground_truth/bingx/error_codes.json``). HTTP 500/504 («Gateway Timeout — The
server failed to get a response; further confirmation required») is raised by ccxt as a network
class — the outcome is unknown even without the table.
"""
DOCS = 'https://bingx-api.github.io/docs-v3/'

# 109500 «Internal server error.»
UNKNOWN_OUTCOME = frozenset({'109500'})
# 500 / 504 in the code field and 100503 are seen in exchange responses; docs-v3 table has no
# 100503, and 500/504 there are HTTP statuses. A false match only means «outcome unknown».
UNKNOWN_OUTCOME_UNDOCUMENTED = frozenset({'500', '504', '100503'})
# docs-v3 has no separate code for a duplicate clientOrderID (part of 101400 «Order parameter
# validation failed … Duplicate clientOrderID») — recognized by the error text
DUPLICATE_CLIENT_ID: frozenset = frozenset()
# 101205 «No position exists to close.», 109420 «No position exists for the specified symbol.»
POSITION_CLOSED = frozenset({'101205', '109420'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
# 101290 «A Reduce Only order can only decrease an existing position. Ensure the order does
# not exceed the current position size.»
REDUCE_EXCEEDS = frozenset({'101290'})
# 101206 «Account available balance is insufficient. Add funds and retry.» — ccxt 4.4.65 raises a
# bare ExchangeError; without the class a service's circuit breaker would treat it as an exchange failure
INSUFFICIENT_FUNDS = frozenset({'101206'})

# Canceling an order no longer in the book: 109421 «The specified order does not
# exist — it may have been filled, cancelled, or the order ID is incorrect.» (ccxt 4.4.65 — bare
# ExchangeError)
ORDER_NOT_OPEN = frozenset({'109421'})
