"""BloFin: an order that can't rest as specified (paper) — (mode, URL, verified, note)."""
from __future__ import annotations

_DOC = 'https://docs.blofin.com/index.html#place-order'
# orderType post_only — «Post-only order»; behavior on crossing is not described in the docs
POST_ONLY_CROSS = ('reject', _DOC, False, 'post_only crossing is not described in the Place Order docs — reject')
MARKET_NO_LIQUIDITY = ('reject', _DOC, False, 'market without opposite liquidity is not described in the docs — reject')
