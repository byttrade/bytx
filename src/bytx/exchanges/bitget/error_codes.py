"""Bitget (classic API v2) error codes that determine an order's outcome.

Source — «REST API Error Code» (https://www.bitget.com/docs/classic/error-code/restapi.md, verified
2026-10-26, fixture ``tests/ground_truth/bitget/error_codes.json``).
"""
DOCS = 'https://www.bitget.com/docs/classic/error-code/restapi.md'

# 40010 «Request timed out» — ccxt 4.4.65 raises AuthenticationError (ccxt comment: «sign signature
# error»), per docs it's a timeout: the order may have been placed — outcome unknown
UNKNOWN_OUTCOME = frozenset({'40010'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 43118 «clientOrderId duplicate»
DUPLICATE_CLIENT_ID = frozenset({'43118'})
# 22002 «No position to close»; TP/SL without a position (verified against the docs 2026-11-04): 40837 «There is no
# position in this position, so stop-profit and stop-loss orders cannot be made», 31008 «… no take profit or stop
# loss order can be made»
POSITION_CLOSED = frozenset({'22002', '40837', '31008'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
# 40757 «Not enough position is available.»; a TP/SL size above the position: 40891 «Insufficient position, can not set
# take profit or stop loss», 43023 «Insufficient position, can not set profit or stop loss»
REDUCE_EXCEEDS = frozenset({'40757', '40891', '43023'})
# 40798 «Insufficient contract account balance», 40800 «Insufficient amount of margin» — ccxt 4.4.65
# doesn't know them (bare ExchangeError)
INSUFFICIENT_FUNDS = frozenset({'40798', '40800'})
# Canceling an order that no longer exists: 22001 «No order to cancel», 43004 «There is no order
# to cancel», 43001 «The order does not exist»; a TP/SL / plan order (cancel-plan-order): 43020 «Stop profit and stop
# loss order does not exist», 43021 «The stop-profit and stop-loss order has been closed», 43025 «Plan order does not
# exist», 43026 «The planned order has been closed»
# batch-cancel-orders (failureList): 40819 «The order does not exist!» — not in the docs code table, demo 2026-11-04
# (a removed limit order; tests/ground_truth/bitget/demo_cancel_lists.json)
ORDER_NOT_OPEN = frozenset({'22001', '43004', '43001', '43020', '43021', '43025', '43026', '40819'})
# A TP/SL rejected by price — an explicit parameter rejection (REST API Error Code, verified 2026-11-04): 40829–40836 —
# long / short TP/SL against the average entry price and the current price («The stop loss price of the long position
# should be less than the current price» …), 40915 «Long position take profit price please > mark price», 40917 «Stop
# price for long positions please < mark price {0}», 45122 — the same for a short SL, 43013–43016 — TP/SL against the
# current price, 43034 / 43035 — a trigger against the market price. Demo 2026-11-04: an entry with
# presetStopLossPrice on the wrong side — 40834, the entry is rejected entirely; a separate position SL / TP — 40917 /
# 40915; tests/ground_truth/bitget/demo_tp_sl_price_errors.json. ccxt 4.4.65 — a bare ExchangeError (an entry —
# «outcome unknown» and reconciliation by client_id).
# A TP / SL of this kind already exists: 43030 «Take profit order already existed», 43031 «Stop loss order already
# existed»
INVALID_ORDER = frozenset({'40829', '40830', '40831', '40832', '40833', '40834', '40835', '40836', '40915', '40917',
                           '43013', '43014', '43015', '43016', '43034', '43035', '45122', '43030', '43031'})
