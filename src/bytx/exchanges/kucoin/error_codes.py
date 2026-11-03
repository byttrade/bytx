"""KuCoin Futures error codes that determine an order's outcome.

Source — https://www.kucoin.com/docs-new/error-code/futures (verified 2026-10-25,
fixture ``tests/ground_truth/kucoin/error_codes.json``).
"""
DOCS = 'https://www.kucoin.com/docs-new/error-code/futures'

UNKNOWN_OUTCOME: frozenset = frozenset()
# 408000 «Network timeout» — seen in exchange responses (ccxt 4.4.65 → BadRequest);
# it is NOT in the futures/spot code tables. A false match only means «outcome unknown».
UNKNOWN_OUTCOME_UNDOCUMENTED = frozenset({'408000'})
# 300018 «clientOid parameter repeated»
DUPLICATE_CLIENT_ID = frozenset({'300018'})
# 300009 «Current position size: 0, unable to close the position.»
POSITION_CLOSED = frozenset({'300009'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
REDUCE_EXCEEDS: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()

# Canceling an order no longer in the book: 100004 «Order is in not cancelable state»
ORDER_NOT_OPEN = frozenset({'100004'})

# 330011 «Before initiating any Futures trades, please use the Switch Position Mode endpoint to update your
# position mode and confirm the change using the Get Position Mode endpoint.» — the account position mode is not
# selected: a retry won't help, the order isn't accepted → ConfigError with a hint (outcome.refine)
POSITION_MODE_NOT_SET = frozenset({'330011'})
POSITION_MODE_HINT = 'select One-Way position mode in KuCoin Futures settings (the SDK trades KuCoin in one-way mode only)'
