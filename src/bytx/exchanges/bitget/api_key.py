"""Bitget: API key check — GET /api/v2/spot/account/info («Get Account Information»).

Docs: https://www.bitget.com/docs/catalog/classic-spot-account/classic-spot-account#get-account-information — ``ips`` «IP whitelist»,
``authorities`` — «Read only: coor: futures orders / cpor: futures holdings / stor: spot trade / … / wtor:
wallet transfer … Read and Write: coow: futures orders / cpow: futures holdings / stow: spot trade / … /
wtow: wallet transfer / wwow: wallet withdrawl …», ``userId``, ``parentId`` («Main account user ID»).
«REST API → Access Preparation»: «Withdraw permission … you can only withdraw coins through a whitelisted
IP address». The endpoint doesn't report the account type (classic / UTA).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = ('https://www.bitget.com/docs/catalog/classic-spot-account/classic-spot-account#get-account-information',
        'https://www.bitget.com/docs/classic/rest-api.md')
READ = frozenset({'coor', 'cpor', 'stor', 'smor', 'ttor', 'wtor', 'taxr', 'chor', 'p2pr', 'pllr'})
TRADE = {'swap': frozenset({'coow', 'cpow'}), 'spot': frozenset({'stow'})}
WITHDRAW = 'wwow'
TRANSFER = 'wtow'


def parse(info: KeyInfo, response: dict, market_type: str) -> None:
    r = (response or {}).get('data') or {}
    info.raw['account-info'] = r
    perms = [str(p) for p in r.get('authorities') or []]
    info.permissions = perms
    if 'authorities' in r:
        have = set(perms)
        info.can_read = bool(have)   # any of the authorities grants read access to its scope
        info.can_trade = bool(have & TRADE.get(market_type, TRADE['swap']))
        info.can_withdraw = WITHDRAW in have
        info.can_transfer = TRANSFER in have
    if 'ips' in r:
        ips = [ip.strip() for ip in str(r.get('ips') or '').split(',') if ip.strip()]
        info.ips = ips
        info.ip_restricted = bool(ips)
    if r.get('userId') not in (None, ''):
        info.account_uid = str(r['userId'])
        parent = r.get('parentId')
        if parent not in (None, ''):
            info.is_master = str(parent) == str(r['userId'])


async def _query(ex: Any) -> dict:
    return await ex._call(ex.ccxt.privateSpotGetV2SpotAccountInfo)


async def account_uid(ex: Any) -> Optional[str]:
    info = KeyInfo(exchange=ex.name)
    parse(info, await _query(ex), ex.market_type)
    return info.account_uid


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'can_withdraw', lambda: _query(ex))
    if response is None:
        for name in ('can_trade', 'ip_restricted', 'account_uid'):
            info.missing(name, info.unknown['can_withdraw'])
    else:
        parse(info, response, ex.market_type)
    info.missing('account_type', 'the Bitget key endpoint does not report the account type (classic / UTA)')
    return info
