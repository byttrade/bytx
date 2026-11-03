"""Gate: API key check — GET /account/detail («Get account detail»).

AccountDetail model (https://github.com/gateio/gateapi-python/blob/master/docs/AccountDetail.md): ``ip_whitelist`` — «IP Whitelist», ``user_id``, ``key.mode`` —
«1 - Classic mode, 2 - Legacy unified mode». The endpoint doesn't report key permissions (trade, withdraw) — ``can_trade``
/ ``can_withdraw`` are undetermined (withdrawal policy — via ``withdraw``, see :func:`bytx.key_check.apply_policy`).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = ('https://github.com/gateio/gateapi-python/blob/master/docs/AccountDetail.md', 'https://github.com/gateio/gateapi-python/blob/master/docs/AccountApi.md#get_account_detail')
MODES = {1: 'classic', 2: 'legacy_unified'}


def parse(info: KeyInfo, response: dict) -> None:
    r = response if isinstance(response, dict) else {}
    info.raw['detail'] = r
    if 'ip_whitelist' in r:
        ips = [str(ip) for ip in r.get('ip_whitelist') or [] if ip]
        info.ips = ips
        info.ip_restricted = bool(ips)
    if r.get('user_id') not in (None, ''):
        info.account_uid = str(r['user_id'])
    mode = (r.get('key') or {}).get('mode')
    if mode is not None:
        info.account_type_raw = {'key.mode': mode}
        info.account_type = MODES.get(mode, str(mode))


async def _query(ex: Any) -> dict:
    return await ex._call(ex.ccxt.privateAccountGetDetail)


async def account_uid(ex: Any) -> Optional[str]:
    info = KeyInfo(exchange=ex.name)
    parse(info, await _query(ex))
    return info.account_uid


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'account_uid', lambda: _query(ex))
    if response is None:
        info.missing('ip_restricted', info.unknown['account_uid'])
    else:
        parse(info, response)
        info.can_read = True
    reason = 'Gate APIv4 does not report key permissions (trade / withdraw)'
    for name in ('can_trade', 'can_withdraw', 'can_transfer'):
        info.missing(name, reason)
    return info
