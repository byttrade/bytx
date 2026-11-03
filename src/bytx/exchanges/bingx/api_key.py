"""BingX: API key check.

- GET /openApi/v1/account/apiPermissions («Query API KEY Permissions»), docs:
  https://bingx-api.github.io/docs-v3/#/en/Account%20and%20Wallet/Account%20Info/Query%20API%20KEY%20Permissions
  ``permissions`` — «1 - Spot trading, 2 - Read, 3 - Professional contract trading, 4 - Universal
  transfer, 5 - Withdraw, 7 - Allow intra-sub-account transfer»; ``ipAddresses`` — «IP whitelist».
  The docs example has no ``{code, data}`` wrapper; parsing accepts both forms.
- UID — GET /openApi/account/v1/uid: ``uid``, ``accountStatus`` — «0 = Normal, 1 = Deregistered, 2 = Locked».
- BingX docs have no account type (unified/classic); position mode — :meth:`Exchange.is_hedge_mode`.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = (
    'https://bingx-api.github.io/docs-v3/#/en/Account%20and%20Wallet/Account%20Info/Query%20API%20KEY%20Permissions',
    'https://bingx-api.github.io/docs-v3/#/en/Account%20and%20Wallet/Account%20Info/Query%20Account%20UID',
)
PERMISSIONS = {1: 'spot_trading', 2: 'read', 3: 'contract_trading', 4: 'universal_transfer', 5: 'withdraw',
               7: 'sub_account_transfer'}
ACCOUNT_STATUS = {0: 'normal', 1: 'deregistered', 2: 'locked'}


def _data(response: Any) -> dict:
    if isinstance(response, dict) and isinstance(response.get('data'), dict):
        return response['data']
    return response if isinstance(response, dict) else {}


def parse_permissions(info: KeyInfo, response: Any, market_type: str) -> None:
    r = _data(response)
    info.raw['apiPermissions'] = r
    codes = {int(p) for p in r.get('permissions') or [] if str(p).lstrip('-').isdigit()}
    info.permissions = [PERMISSIONS.get(c, str(c)) for c in sorted(codes)]
    info.can_read = 2 in codes
    info.can_trade = (3 in codes) if market_type == 'swap' else (1 in codes)
    info.can_withdraw = 5 in codes
    info.can_transfer = 4 in codes
    ips = [str(ip) for ip in r.get('ipAddresses') or []]
    info.ips = ips
    info.ip_restricted = bool(ips)


def parse_uid(info: KeyInfo, response: Any) -> None:
    r = _data(response)
    info.raw['uid'] = r
    if r.get('uid') not in (None, ''):
        info.account_uid = str(r['uid'])
    status = r.get('accountStatus')
    if status is not None and int(status) != 0:
        info.warnings.append(f'account: {ACCOUNT_STATUS.get(int(status), status)} (accountStatus)')


async def account_uid(ex: Any) -> Optional[str]:
    info = KeyInfo(exchange=ex.name)
    parse_uid(info, await ex._call(ex.ccxt.accountV1PrivateGetUid))
    return info.account_uid


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    perms = await probe(info, 'can_withdraw', lambda: ex._call(ex.ccxt.accountV1PrivateGetAccountApiPermissions))
    if perms is not None:
        parse_permissions(info, perms, ex.market_type)
    else:
        for name in ('can_trade', 'ip_restricted'):
            info.missing(name, info.unknown['can_withdraw'])
    uid = await probe(info, 'account_uid', lambda: ex._call(ex.ccxt.accountV1PrivateGetUid))
    if uid is not None:
        parse_uid(info, uid)
    info.missing('account_type', 'BingX docs have no unified/classic')
    return info
