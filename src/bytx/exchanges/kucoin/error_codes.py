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

# Explicit spot placement rejection (https://www.kucoin.com/docs-new/error-code/spot, checked 2026-11-08): 400200
# «Forbidden to place an order», 400500 «Your located country/region is currently not supported for the trading of
# this token», 400600 «The trading pair has not yet started trading», 400700 «Transaction restricted, there's a risk
# problem in your account», 600203 «Symbol XXX-XXX cant be traded», 900001 «symbol does not exist». Not 400100: on spot
# it is both «Parameter Error» and «Insufficient balance» (ccxt — InsufficientFunds), on futures — another error
INVALID_ORDER = frozenset({'400200', '400500', '400600', '400700', '600203', '900001'})
# Spot 400100 per the same docs — both «Parameter Error» and «account.available.amount -- Insufficient balance»; ccxt
# 4.4.65 always gives InsufficientFunds. Without a funds marker in the text (FUNDS_TEXT) — a parameter rejection
FUNDS_AMBIGUOUS = frozenset({'400100'})
FUNDS_TEXT = ('available.amount', 'balance', 'insufficient')

# 330011 «Before initiating any Futures trades, please use the Switch Position Mode endpoint to update your
# position mode and confirm the change using the Get Position Mode endpoint.» — the account position mode is not
# selected: a retry won't help, the order isn't accepted → ConfigError with a hint (outcome.refine)
POSITION_MODE_NOT_SET = frozenset({'330011'})
POSITION_MODE_HINT = 'select One-Way position mode in KuCoin Futures settings (the SDK trades KuCoin in one-way mode only)'
