"""Gate: an order that can't rest as specified (paper) — (mode, URL, verified, note)."""
from __future__ import annotations

_DOC = 'https://www.gate.com/docs/developers/apiv4/en/'
_ORDER = 'https://github.com/gateio/gateapi-python/blob/master/docs/FuturesOrder.md'
# error label ORDER_POC_IMMEDIATE «POC order will be finished immediately» (futures label table)
POST_ONLY_CROSS = ('reject', _DOC, True, 'tif=poc crossing — ORDER_POC_IMMEDIATE')
# market = price 0 + tif ioc: «ioc: ImmediateOrCancelled, taker only» — the unfilled remainder is canceled
MARKET_NO_LIQUIDITY = ('cancel', _ORDER, False, 'ioc: remainder canceled (finish_as=ioc); to verify on testnet')
