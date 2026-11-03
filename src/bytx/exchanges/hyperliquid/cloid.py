"""Hyperliquid cloid with a bot tag.

Hyperliquid docs (Exchange endpoint, Place an order): ``c`` — «Client Order ID», a 128-bit
hex number, ``0x`` + 32 hex (:data:`bytx.order_params.CLIENT_ID_FORMATS`). Not a single
non-hex character — so the order role isn't encoded in the cloid (it lives only in the journal), and
the bot tag (``d`` + 12 hex) is hex itself and goes at the start
(``0x`` + 13 tag chars + 19 hex tail).
"""
from __future__ import annotations

import re
import secrets
from typing import Optional

CLOID_HEX = 32
_HEX_RE = re.compile(r'[0-9a-f]+')
CLOID_RE = re.compile(r'0x[0-9a-fA-F]{32}')


def tagged_cloid(tag: str, tail: Optional[str] = None) -> str:
    """``0x`` + tag (hex, lowercase) + hex tail up to exactly 32 chars.

    ``tail`` — own tail (hex), otherwise random. Tag not hex or longer than 31 — ValueError.
    """
    body = tag.lower()
    if not body or not _HEX_RE.fullmatch(body) or len(body) >= CLOID_HEX:
        raise ValueError(f'Hyperliquid cloid tag must be hex shorter than {CLOID_HEX}, got {tag!r}')
    need = CLOID_HEX - len(body)
    rest = (tail.lower() if tail is not None else secrets.token_hex(16))[:need]
    if len(rest) != need or not _HEX_RE.fullmatch(rest):
        raise ValueError(f'Hyperliquid cloid tail must be {need} hex, got {tail!r}')
    return '0x' + body + rest


def cloid_body(cid: str) -> Optional[str]:
    """32-hex cloid in lowercase (the exchange may return a different case) or None."""
    if not isinstance(cid, str) or not CLOID_RE.fullmatch(cid):
        return None
    return cid[2:].lower()


__all__ = ['CLOID_HEX', 'CLOID_RE', 'cloid_body', 'tagged_cloid']
