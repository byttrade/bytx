"""BloFin: API key — GET /api/v1/user/query-apikey («Get API Key Info», F#get-api-key-info).

Fields: ``uid``, ``readOnly`` (0 — read and trade, 1 — read only), ``ips``, ``type`` — «1: Transaction,
2: Connect to third-party» (a demo key is 3, demo 2026-11-09), ``expireTime``, ``parentUid``. The key type decides
whether brokerId is sent (B#integration-flow: a type=1 key with brokerId — 152011).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import BytxError

DOCS = ('https://docs.blofin.com/index.html#get-api-key-info',)
_KEY_TYPE_ATTR = '_bytx_blofin_key_type'
_LOADED_ATTR = '_bytx_blofin_key_loaded'


async def _query(ex: Any) -> dict:
    return await ex._call(ex.ccxt.privateGetUserQueryApikey)


async def load_key_type(ex: Any) -> Optional[int]:
    """Key type (``query-apikey.type``) — once per client (cached on the ccxt instance); not read — None:
    brokerId is sent then (it is not sent only for a type=1 key)."""
    c = ex.ccxt
    if getattr(c, _LOADED_ATTR, False):
        return getattr(c, _KEY_TYPE_ATTR, None)
    value: Optional[int] = None
    try:
        data = (await _query(ex) or {}).get('data')
        raw = data.get('type') if isinstance(data, dict) else None
        value = int(raw) if raw not in (None, '') else None
    except (BytxError, TypeError, ValueError):
        value = None
    setattr(c, _KEY_TYPE_ATTR, value)
    setattr(c, _LOADED_ATTR, True)
    return value


def parse(info: Any, response: Any) -> None:
    from bytx.key_check import KeyInfo
    assert isinstance(info, KeyInfo)
    r = (response or {}).get('data') or {}
    if not isinstance(r, dict):
        return
    info.raw['query-apikey'] = r
    if 'readOnly' in r:
        info.can_read = True
        info.can_trade = str(r.get('readOnly')) == '0'
    if 'ips' in r:
        ips = [str(ip).strip() for ip in (r.get('ips') or []) if str(ip).strip()]
        info.ips = ips
        info.ip_restricted = bool(ips)
    if r.get('uid') not in (None, ''):
        info.account_uid = str(r['uid'])
        parent = r.get('parentUid')
        if parent not in (None, ''):
            info.is_master = str(parent) == str(r['uid'])


async def account_uid(ex: Any) -> Optional[str]:
    from bytx.key_check import KeyInfo
    info = KeyInfo(exchange=ex.name)
    parse(info, await _query(ex))
    return info.account_uid


async def key_info(ex: Any) -> Any:
    """Key permissions: read / trade (``readOnly``), IP, uid. The endpoint does not report withdraw / transfer."""
    from bytx.key_check import KeyInfo, probe
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'can_trade', lambda: _query(ex))
    if response is not None:
        parse(info, response)
    for name in ('can_withdraw', 'can_transfer'):
        info.missing(name, 'query-apikey does not report withdraw / transfer permissions')
    return info
