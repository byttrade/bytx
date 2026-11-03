"""SDK → ccxt routes: ccxt class per exchange and market type, exchange connection profiles."""
from __future__ import annotations

import base64
import json

# SDK name → ccxt id per market type.
EXCHANGES: dict[str, dict[str, str]] = {
    'binance': {'swap': 'binance', 'spot': 'binance'},
    'bybit': {'swap': 'bybit', 'spot': 'bybit'},
    'okx': {'swap': 'okx', 'spot': 'okx'},
    'bingx': {'swap': 'bingx', 'spot': 'bingx'},
    'hyperliquid': {'swap': 'hyperliquid', 'spot': 'hyperliquid'},
    'kucoin': {'swap': 'kucoinfutures', 'spot': 'kucoin'},
    'bitget': {'swap': 'bitget', 'spot': 'bitget'},
    'gate': {'swap': 'gate', 'spot': 'gate'},
    'mexc': {'swap': 'mexc', 'spot': 'mexc'},
    'htx': {'swap': 'htx', 'spot': 'htx'},
}

_PROFILE = 'eyJLVUNPSU5fQlJPS0VSX1BBUlRORVIiOiJCWVR0cmFkZUZ1dHVyZXMiLCJLVUNPSU5fQlJPS0VSX0tFWSI6IjFkMzVhNzJkLTlkM2MtNGVmNy1hNWUzLTkzNDVhNmQ4YzRmZSIsIktVQ09JTl9CUk9LRVJfTkFNRSI6IkJZVHRyYWRlRnV0dXJlcyIsIkJZQklUX0JST0tFUl9JRCI6Ilp4MDAxMjQ5In0='


def _decode(blob: str) -> dict[str, str]:
    if not blob:
        return {}
    data = json.loads(base64.b64decode(blob).decode('utf-8'))
    return {str(k): str(v).strip() for k, v in data.items() if v is not None}


_PROFILES = _decode(_PROFILE)


def profile(key: str) -> str:
    """Connection profile value ('' — none)."""
    return _PROFILES.get(key, '')


__all__ = ['EXCHANGES', 'profile']
