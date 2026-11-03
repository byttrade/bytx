"""HTX: an order that can't rest as specified (paper) — (mode, URL, verified, note)."""
from __future__ import annotations

_DOC = 'https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-place-an-order'
# «post_only … if not, the maker order will be cancelled»
POST_ONLY_CROSS = ('cancel', _DOC, True, 'post_only: «if not, the maker order will be cancelled»')
# market — remainder not described in docs
MARKET_NO_LIQUIDITY = ('cancel', _DOC, False, 'market: unfilled remainder not described in docs')
