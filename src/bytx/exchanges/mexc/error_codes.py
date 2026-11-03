"""MEXC Contract API error codes that determine an order's outcome.

Error response — ``{"success": false, "code": 2005, "message": …}`` (:func:`bytx.errors.exchange_codes` reads
``code``). Source — the «Error code» table https://mexcdevelop.github.io/apidocs/contract_v1_en/#error-code and the
Cancel Orders example of the new docs (errorCode 2040/2041 inside ``data``), fixture ``tests/ground_truth/mexc/error_codes.json``.
"""
DOCS = 'https://mexcdevelop.github.io/apidocs/contract_v1_en/#error-code'

# 500 «Internal error», 501 «System busy» — the order may have been placed
UNKNOWN_OUTCOME = frozenset({'500', '501'})
UNKNOWN_OUTCOME_UNDOCUMENTED: frozenset = frozenset()
# 2042 «Duplicate order ID» (Error Code of the new docs https://www.mexc.com/api-docs/futures/error-code); the order id
# is assigned by the exchange — only our own externalOid can be a duplicate
DUPLICATE_CLIENT_ID = frozenset({'2042'})
# 2009 «Positions do not exist or have been closed»
POSITION_CLOSED = frozenset({'2009'})
POSITION_CLOSED_IF_REDUCE: frozenset = frozenset()
# 2008 «Insufficient number of positions can be closed», 7005 «The position amount for closing is insufficient,
# please check the position or the pending order»
REDUCE_EXCEEDS = frozenset({'2008', '7005'})
# 2005 «Balance insufficient»
INSUFFICIENT_FUNDS = frozenset({'2005'})
# Cancel Orders: 2040 «order not exist», 2041 «order state cannot be cancelled»
ORDER_NOT_OPEN = frozenset({'2040', '2041'})
# API trading unavailable — a retry won't help (NotSupported): 6001 «Trading forbidden»,
# 6002 «Open forbidden», 6005 «The trading pair is not available», 511 «Endpoint inaccessible»,
# 1002 «Contract not activated». MEXC closed API order placement from 2022-07-25 to
# 2026-03-31 (futures API update log) — the rejection must be clear rather than go into retries.
TRADING_UNAVAILABLE = frozenset({'6001', '6002', '6005', '511', '1002'})
# Key lacks permissions / key not accepted (PermissionDenied): 401 «Unauthorized», 402 «Api_key expired»,
# 406 «Accessed IP is not in the whitelist», 701–704 «… permission is required»
# (704 — modifying trading information: placing and canceling orders).
PERMISSION = frozenset({'401', '402', '406', '701', '702', '703', '704'})
