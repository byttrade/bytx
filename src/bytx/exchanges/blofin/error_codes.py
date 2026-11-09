"""BloFin (Futures API v1) error codes that determine an order's outcome.

Source — the «Errors» section (https://docs.blofin.com/index.html#errors, checked 2026-11-09) and demo responses
(``tests/ground_truth/blofin/demo_*.json``).
"""
DOCS = 'https://docs.blofin.com/index.html#errors'

# 500 «Internal Server Error», 103013 «Internal error; unable to process your request» — the order may have rested.
# For cancels (cancel-tpsl / cancel-algo) the docs show data[i].code 500 = «does not exist»; on demo 2026-11-09 cancels
# never returned 500 (they gave 102068 / 1000 / 148030) — per the docs 500 stays an unknown outcome.
UNKNOWN_OUTCOME = frozenset({'500', '103013'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 102002 «Duplicate customized order ID», 150003 «clientId already exist»
DUPLICATE_CLIENT_ID = frozenset({'102002', '150003'})
# 102005 «Position had been closed», 102022 «… You don't have any positions of this contract. Turn off Reduce-only»
POSITION_CLOSED = frozenset({'102005', '102022'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
REDUCE_EXCEEDS: frozenset = frozenset()
# 150004 «Insufficient balance», 103003 — not enough margin (ccxt 4.4.65: 150004 → InvalidOrder, 103003 unknown)
INSUFFICIENT_FUNDS = frozenset({'150004', '103003'})
# Explicit parameter rejection: 152001–152005 (parameters; 152002 — both size below minSize / off lotSize and leverage
# above maxLeverage — demo 2026-11-09, so the size is checked before the network), 152009 (clientOrderId format),
# 152014 (no instId), 542 / 102014 / 102015 (above the maximum), 102064 / 102065 (price band), 102037–102055 (TP/SL vs
# price), 102089 (wrong position mode); 102126 «SL trigger price should be lower than the mark price» — not in the docs
# table, demo 2026-11-09 (SL near the mark price)
INVALID_ORDER = frozenset({'152001', '152002', '152003', '152004', '152005', '152009', '152014', '542', '102014',
                           '102015', '102064', '102065', '102089', '102126'} | {str(c) for c in range(102037, 102056)})
# «The order is gone»: cancel 102068 (filled / triggered / canceled / not exist in one code), amend 102067, a batch
# cancel element 1000 (demo 2026-11-09, demo_cancel_missing)
ORDER_NOT_OPEN = frozenset({'102068', '102067', '1000'})
# Instrument closed for opening via API: 102167 (apiPermission=reduce_only), 102127 (temporarily)
TRADING_UNAVAILABLE = frozenset({'102167', '102127'})
RATE_LIMITED = frozenset({'429'})

# Broker (B#integration-flow, F#errors): key / broker binding — ConfigError with a hint, not an unknown outcome
CONFIG_ERRORS = {
    '152011': 'BloFin: a type=1 («Transaction») key does not accept brokerId — create a «Connect to Third-Party '
              'Applications» key for the broker application',
    '152012': 'BloFin: the broker key requires brokerId — set BLOFIN_BROKER_CODE',
    '152013': 'BloFin: brokerId does not match the broker the key is bound to — check BLOFIN_BROKER_CODE',
}
