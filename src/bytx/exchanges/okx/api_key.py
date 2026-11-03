"""OKX: API key check — GET /api/v5/account/config («Get account configuration»).

Docs: https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-account-configuration
``perm`` — «The permission of the current requesting API key or Access token read_only : Read trade :
Trade withdraw : Withdraw»; ``ip`` — «IP addresses that linked with current API key, separate with commas
… It is an empty string "" if there is no IP bonded»; ``uid`` / ``mainUid`` — «The current request
account is main account if uid = mainUid»; ``acctLv`` — «1 : Spot mode 2 : Futures mode 3 :
Multi-currency margin 4 : Portfolio margin»; ``posMode`` — «long_short_mode : long/short … net_mode : net».
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = ('https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-account-configuration',)
ACCOUNT_LEVELS = {'1': 'spot_mode', '2': 'futures_mode', '3': 'multi_currency_margin', '4': 'portfolio_margin'}
POSITION_MODES = {'long_short_mode': 'hedge', 'net_mode': 'one_way'}


def parse(info: KeyInfo, response: dict) -> None:
    rows = (response or {}).get('data') or []
    r = rows[0] if rows else {}
    info.raw['config'] = r
    perms = [p.strip() for p in str(r.get('perm') or '').split(',') if p.strip()]
    info.permissions = perms
    if perms:
        info.can_read = 'read_only' in perms
        info.can_trade = 'trade' in perms
        info.can_withdraw = 'withdraw' in perms
    ips = [ip.strip() for ip in str(r.get('ip') or '').split(',') if ip.strip()]
    info.ips = ips
    if 'ip' in r:
        info.ip_restricted = bool(ips)
    if r.get('uid'):
        info.account_uid = str(r['uid'])
        if r.get('mainUid'):
            info.is_master = str(r['uid']) == str(r['mainUid'])
    level = str(r.get('acctLv') or '')
    if level:
        info.account_type_raw = {'acctLv': level}
        info.account_type = ACCOUNT_LEVELS.get(level, level)
    mode = POSITION_MODES.get(str(r.get('posMode') or ''))
    if mode is not None:
        info.position_mode = mode  # type: ignore[assignment]


async def _config(ex: Any) -> dict:
    return await ex._call(ex.ccxt.privateGetAccountConfig)


async def account_uid(ex: Any) -> Optional[str]:
    info = KeyInfo(exchange=ex.name)
    parse(info, await _config(ex))
    return info.account_uid


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'can_withdraw', lambda: _config(ex))
    if response is None:
        for name in ('can_trade', 'ip_restricted', 'account_uid', 'account_type'):
            info.missing(name, info.unknown['can_withdraw'])
        return info
    parse(info, response)
    if not info.permissions:
        info.missing('can_withdraw', 'perm is empty in the response')
    return info
