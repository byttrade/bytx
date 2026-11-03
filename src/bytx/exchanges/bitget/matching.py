"""Bitget: an order that can't rest as specified (paper) — (mode, URL, verified, note)."""
from __future__ import annotations

_DOC = 'https://www.bitget.com/docs/catalog/classic-contract-trade/classic-contract-trade#place-order'
POST_ONLY_CROSS = ('reject', _DOC, False,
                   'force=post_only crossing not described in Place Order docs — reject, to verify on demo')
MARKET_NO_LIQUIDITY = ('reject', _DOC, False,
                       'not described (error 22045 «Insufficient liquidity in the market») — reject')
