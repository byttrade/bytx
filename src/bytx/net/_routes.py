"""SDK → ccxt routes: ccxt class per exchange and market type, exchange connection profiles."""
from __future__ import annotations

import base64
import hashlib
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
    'blofin': {'swap': 'blofin', 'spot': 'blofin'},   # spot — Spot API in exchanges/blofin/spot (ccxt 4.4.65 lacks it)
}

_PROFILE = 'fkI1iox+9ocEizRvwCRiMLhp4YfKRvJ0anlACB1umxlhBTiqu0TNrCjrSgLANHMgoWbskdZM6xMCBEkUEDjTWjQETequBo2tdvACE+hMBAqOH56ysWaTe2loNmQoLI1AZlQYuu0dnYIOiilpxT5yPadj9oHbTeEbFXk4cwtDvQx3ARq6iUTLvCmsFQKnQ3I2qmHnjMZR7x0VCV0YDTjTWl8YTu/+A4vweeVEYscudiamd/GBy0jlBA8YTRUMONNaZlAf761S3KhuqAVGvFUFW8oEkZzPW/8UAhRJFBtFqjdBJVzl7QCI/zn+BUS+VgFd0Gfwl8Eh3Q=='


# The profile is XOR'd with a fixed keystream before base64 so values don't appear verbatim in the package;
# this is obfuscation, not encryption — the keystream ships here too (:func:`bytx_profile.py` reuses this codec).
_KEYSTREAM = hashlib.sha256(b'bytx.net._routes/profile/v2').digest()


def _xor(data: bytes) -> bytes:
    return bytes(b ^ _KEYSTREAM[i % len(_KEYSTREAM)] for i, b in enumerate(data))


def _decode(blob: str) -> dict[str, str]:
    if not blob:
        return {}
    data = json.loads(_xor(base64.b64decode(blob)).decode('utf-8'))
    return {str(k): str(v).strip() for k, v in data.items() if v is not None}


def _encode(profile: dict) -> str:
    if not profile:
        return ''
    raw = json.dumps(profile, separators=(',', ':')).encode('utf-8')
    return base64.b64encode(_xor(raw)).decode('ascii')


_PROFILES = _decode(_PROFILE)


def profile(key: str) -> str:
    """Connection profile value ('' — none)."""
    return _PROFILES.get(key, '')


__all__ = ['EXCHANGES', 'profile']
