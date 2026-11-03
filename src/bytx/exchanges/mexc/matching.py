"""MEXC: an order that can't rest as specified (paper) — (mode, URL, verified, note)."""
from __future__ import annotations

_DOC = 'https://www.mexc.com/api-docs/futures/account-and-trading-endpoints/place-order'
# type 2 «Post Only (maker only)» — crossing behavior not described in docs
POST_ONLY_CROSS = ('cancel', _DOC, False, 'type 2 maker only: crossing outcome not described in docs')
# type 5 market — unfilled remainder not described in docs
MARKET_NO_LIQUIDITY = ('cancel', _DOC, False, 'market (type 5): remainder not described in docs')
