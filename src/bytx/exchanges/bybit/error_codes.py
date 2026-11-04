"""Bybit v5 error codes that determine an order's outcome.

Source — https://bybit-exchange.github.io/docs/v5/error (verified 2026-10-25,
fixture ``tests/ground_truth/bybit/error_codes.json``).
"""
DOCS = 'https://bybit-exchange.github.io/docs/v5/error'

# 10000 «Server Timeout», 10016 «Server error» (WS: «internal server error; Service is
# restarting») — the gateway didn't hear back from the engine, outcome unknown
UNKNOWN_OUTCOME = frozenset({'10000', '10016'})
# 5004 «Server Timeout» — seen in exchange responses; it is NOT in the docs table.
# A false match only yields «outcome unknown» instead of a rejection (the safe side).
UNKNOWN_OUTCOME_UNDOCUMENTED = frozenset({'5004'})
# 110072 «OrderLinkedID is duplicate», 10014 «Invalid duplicate request.»
DUPLICATE_CLIENT_ID = frozenset({'110072', '10014'})
POSITION_CLOSED: frozenset = frozenset()
# 110017 «orderQty will be truncated to zero»: for reduce-only the amount is truncated to the
# position size — zero means the position is already gone (a normal close race, don't retry).
# Seen on demo (hedge): the same code and text when the position exists but the side's resting reduce-only
# orders already cover all of it — Bybit truncates a new reduce-only order to «position − sum of reduce-only»
# (more — truncated silently, 0 — 110017). Don't retry either: reconcile the position and the orders.
POSITION_CLOSED_IF_REDUCE = frozenset({'110017'})
REDUCE_EXCEEDS: frozenset = frozenset()
# other exchange errors that ccxt 4.4.65 doesn't map to InsufficientFunds
INSUFFICIENT_FUNDS: frozenset = frozenset()
# 10001 «Request parameter error» (on the demo run 2026-10-26 — «position idx not match position
# mode» on /v5/order/create): request not accepted — explicit rejection. ccxt 4.4.65 gives BadRequest →
# ExchangeError (place_order), and a batch element gives InvalidOrder (place_orders): one class on both paths
INVALID_ORDER = frozenset({'10001'})
# Rate limit (error table): 10006 «Too many visits. Exceeded the API Rate Limit.» (UTA; in a batch — the item code in
# retExtInfo.list: Rate Limit docs — a batch is charged per order, «the part that exceeds the limit will fail»),
# 10018 «Exceeded the IP Rate Limit.» (UTA, struck through in the docs — same meaning), 30035 «Option: Too fast to
# cancel, Try it later» (UTA), 10429 «System level frequency protection» and 20003 «Too frequent requests under the
# same session» (WS OE General code), 170005 «Too many new orders; current limit is %s orders per %s.» and 170222
# «Too many requests in this time frame.» (Spot Trade). Rejected before the engine: no order, retry later —
# RateLimited. ccxt 4.4.65 maps 170005 and 20003 to InvalidOrder and doesn't know 10429, and the SDK builds a batch
# item error itself (InvalidOrder) — refined by this table (outcome.refine).
RATE_LIMITED = frozenset({'10006', '10018', '10429', '20003', '30035', '170005', '170222'})
# 110025 «Position mode is not modified», 110043 «Set leverage has not been modified» — the mode/leverage
# is already what was requested: for set_position_mode / set_leverage this is success
NOT_MODIFIED = frozenset({'110025', '110043'})

# Canceling an order no longer in the book, section «UTA / Derivatives Trade»:
# 110001 «Order does not exist», 110008 «The order has been completed or cancelled.»,
# 110010 «The order has been cancelled»; spot — 170213 «Order does not exist.»
ORDER_NOT_OPEN = frozenset({'110001', '110008', '110010', '170213'})
