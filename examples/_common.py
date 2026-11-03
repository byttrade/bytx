"""Shared helpers for the examples: a client from environment variables, DEMO by default.

Variables:
    BYTX_EXCHANGE     binance | bybit | okx | bingx | hyperliquid | kucoin (default binance)
    BYTX_MARKET       swap | spot (default swap)
    BYTX_API_KEY, BYTX_SECRET, BYTX_PASSWORD   keys (OKX, KuCoin — password)
    BYTX_WALLET, BYTX_PRIVATE_KEY                  Hyperliquid
    BYTX_PROXY        http://user:pass@host:port or socks5://…
    BYTX_MARGIN_MODE  cross | isolated (default cross)
    BYTX_LIVE=1       live account (without it — the exchange's demo/testnet)
"""
from __future__ import annotations

import os
import sys
from typing import Any, Optional

from bytx import Exchange, RiskGuard, create_exchange


def is_live() -> bool:
    return os.environ.get('BYTX_LIVE') == '1'


def exchange_from_env(default: str = 'binance', *, market: Optional[str] = None,
                      risk: Optional[RiskGuard] = None, keys: bool = True, **kwargs: Any) -> Exchange:
    """Client from environment variables. ``keys=False`` — public data only; ``kwargs`` —
    other :func:`create_exchange` arguments (``journal``, ``intents``, ``client_id_prefix``…)."""
    name = os.environ.get('BYTX_EXCHANGE', default)
    env = os.environ.get
    creds = {}
    if keys:
        creds = {'api_key': env('BYTX_API_KEY'), 'secret': env('BYTX_SECRET'),
                 'password': env('BYTX_PASSWORD'), 'wallet_address': env('BYTX_WALLET'),
                 'private_key': env('BYTX_PRIVATE_KEY')}
    if is_live():
        print(f'⚠️  BYTX_LIVE=1: {name} — LIVE account', file=sys.stderr)
    # margin mode is explicit (the SDK never assumes cross): BYTX_MARGIN_MODE
    return create_exchange(name, demo=not is_live(), market=market or env('BYTX_MARKET', 'swap'),
                           proxy=env('BYTX_PROXY'), risk=risk,
                           margin_mode=env('BYTX_MARGIN_MODE', 'cross'), **creds, **kwargs)
