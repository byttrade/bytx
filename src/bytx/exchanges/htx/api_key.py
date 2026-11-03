"""HTX: API key check — GET /v2/user/uid and GET /v2/user/api-key (spot, «API key query»).

``/v2/user/uid`` — «data | long | UID»; ``/v2/user/api-key`` (uid, accessKey) — ``permission`` «API key permission»
(readOnly, trade, withdraw comma-separated), ``ipAddresses`` «API key IP addresses», ``status`` normal / expired.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = ('https://huobiapi.github.io/docs/spot/v1/en/#api-key-query',
        'https://huobiapi.github.io/docs/spot/v1/en/#get-uid')


def parse(info: KeyInfo, response: Any, access_key: Optional[str]) -> None:
    rows = response.get('data') if isinstance(response, dict) else None
    rows = [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []
    row = next((r for r in rows if r.get('accessKey') == access_key), rows[0] if len(rows) == 1 else None)
    if row is None:
        info.missing('can_trade', 'key not found in the /v2/user/api-key response')
        return
    info.raw['api_key'] = {k: v for k, v in row.items() if k != 'accessKey'}
    perms = {p.strip() for p in str(row.get('permission') or '').split(',') if p.strip()}
    info.can_read = 'readOnly' in perms or bool(perms)
    info.can_trade = 'trade' in perms
    info.can_withdraw = 'withdraw' in perms
    ips = [ip.strip() for ip in str(row.get('ipAddresses') or '').split(',') if ip.strip()]
    info.ips = ips
    info.ip_restricted = bool(ips)


async def account_uid(ex: Any) -> Optional[str]:
    resp = await ex._call(ex.ccxt.v2PrivateGetUserUid)
    value = resp.get('data') if isinstance(resp, dict) else None
    return str(value) if value not in (None, '') else None


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    uid = await probe(info, 'account_uid', lambda: account_uid(ex))
    if uid:
        info.account_uid = uid
        response = await probe(info, 'can_trade', lambda: ex._call(ex.ccxt.v2PrivateGetUserApiKey,
                                                                   {'uid': uid, 'accessKey': ex.ccxt.apiKey}))
        if response is not None:
            parse(info, response, ex.ccxt.apiKey)
    info.missing('can_transfer', 'HTX does not report a separate transfer permission')
    return info
