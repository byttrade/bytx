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
# 101212 «Available balance is insufficient for this order. Reduce order amount or add funds.» (docs-v3) — on a
# closing order (reduce-only; in hedge — an order of the positionSide side) «Insufficient available amount to close»:
# demo 2026-11-04 (hedge) — the side is fully covered by resting closing orders (no side — 101205, a closing order
# larger than the side — accepted). Do not retry: check the position and orders. Without reduce-only — insufficient
# funds, as in the docs (INSUFFICIENT_FUNDS below: ccxt 4.4.65 raises a bare ExchangeError, without the class the
# rejection would go to "outcome unknown" reconciliation instead of a rejection).
POSITION_CLOSED_IF_REDUCE = frozenset({'101212'})
# 101290 «A Reduce Only order can only decrease an existing position. Ensure the order does
# not exceed the current position size.»
REDUCE_EXCEEDS = frozenset({'101290'})
# 101206 «Account available balance is insufficient. Add funds and retry.» — ccxt 4.4.65 raises a
# bare ExchangeError; without the class a service's circuit breaker would treat it as an exchange failure. 101212
# «Available balance is insufficient for this order. Reduce order amount or add funds.» — the same (for reduce-only —
# PositionAlreadyClosed above)
INSUFFICIENT_FUNDS = frozenset({'101206', '101212'})
# Explicit parameter rejection (docs-v3, bundle app.9149f5bb7dc21a77fc01.js, verified 2026-11-04): 101400 «Order parameter
# validation failed. Possible reasons: Order amount or quantity below minimum; TP/SL price does not match position
# direction; Duplicate clientOrderID; Trading pair suspended» (a duplicate clientOrderID is recognized by the text
# earlier); 110411 / 110412 — SL long / short on the wrong side of the current price, 110413 / 110414 — the same for TP.
# Demo 2026-11-04 (tests/ground_truth/bingx/demo_error_codes.json): an entry with an attached SL above the price —
# 101400, no position; a separate SL above the price — 110411. ccxt 4.4.65 — a bare ExchangeError.
INVALID_ORDER = frozenset({'101400', '110411', '110412', '110413', '110414'})

# Canceling an order no longer in the book: 109421 «The specified order does not
# exist — it may have been filled, cancelled, or the order ID is incorrect.» (ccxt 4.4.65 — bare
# ExchangeError)
ORDER_NOT_OPEN = frozenset({'109421'})
