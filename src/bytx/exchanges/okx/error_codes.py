"""OKX v5 error codes that determine an order's outcome.

Source — the «Error Code» section https://www.okx.com/docs-v5/en/#error-code (verified
2026-10-25, fixture ``tests/ground_truth/okx/error_codes.json``). The placement code arrives
in ``sCode`` of a ``data[]`` element (the top-level ``code`` is then ``1``).
"""
DOCS = 'https://www.okx.com/docs-v5/en/#error-code'

# 51149 «Order timed out … (does not mean that the request was successful or failed, please
# check the request result)»; 50004 «API endpoint request timeout. (does not mean …)»
UNKNOWN_OUTCOME = frozenset({'51149', '50004'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 51016 «Client order ID already exists.»
DUPLICATE_CLIENT_ID = frozenset({'51016'})
# 51169 «… you don't have any positions in this direction for this contract to reduce or close.»
POSITION_CLOSED = frozenset({'51169'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
REDUCE_EXCEEDS: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()
# Explicit rejection of the reduce-only parameter while the position may be open — NOT «no position»:
# 51170 «A reduce-only order can’t be the same trading direction as your existing positions.»
# (position exists, wrong order side); 51205 «Reduce Only is not available.» (reduceOnly
# unavailable, e.g. hedge long/short with a stale mode cache). ccxt 4.4.65 doesn't know 51170
# (bare ExchangeError) → refined to InvalidOrder.
# TP/SL against the best book price (Error Code, verified 2026-11-04): 51051 «The stop loss trigger price must
# be lower than the best ask price», 51052 «The take profit trigger price must be lower than the best bid
# price», 51053 «The stop loss trigger price must be higher than the best bid price» — an SL attached to the
# entry (attachAlgoOrds) on the wrong side: the entry is rejected entirely (demo 2026-11-04,
# tests/ground_truth/okx/demo_attached_sl_errors.json). ccxt 4.4.65 doesn't know them (bare ExchangeError).
# Parameters (Error Code, checked 2026-11-08): 51000 «Parameter {param} error», 50014 «Parameter {param0} can not be
# empty», 50016 «Parameter {param0} does not match parameter {param1}» — ccxt 4.4.65 BadRequest (→ reconciliation)
INVALID_ORDER = frozenset({'51170', '51205', '51051', '51052', '51053', '51000', '50014', '50016'})

# Canceling an order no longer in the book: 51400 «Order cancellation failed as
# the order has been filled, canceled or does not exist.», 51401/51402 (already canceled/filled),
# 51603 «Order does not exist.»
ORDER_NOT_OPEN = frozenset({'51400', '51401', '51402', '51603'})
# Rate limit (Error Code, verified 2026-11-04, tests/ground_truth/okx/cancel_multiple_orders.json): 50011 «Rate limit
# reached. Please refer to API documentation and throttle requests accordingly.», 50061 «You've reached the maximum
# order rate limit for this account.» — rejected before the engine, retry later: RateLimited. ccxt parses the whole
# response itself (RateLimitExceeded), but not a batch item code (sCode): refined by this table (outcome.refine).
RATE_LIMITED = frozenset({'50011', '50061'})
