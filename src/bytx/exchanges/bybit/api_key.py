"""Bybit v5: API key check — GET /v5/user/query-api («Get API Key Information»).

Docs: https://bybit-exchange.github.io/docs/v5/user/apikey-info — «Any permission can access this
endpoint». Fields: ``readOnly`` — «0: Read and Write. 1: Read only»; ``permissions.ContractTrade`` —
«Order, Position», ``permissions.Derivatives`` — «DerivativesTrade», ``permissions.Spot`` — «SpotTrade»,
``permissions.Wallet`` — «AccountTransfer, SubMemberTransfer…, Withdraw(master account)»; ``ips`` — «IP
bound»; ``uta`` — «0: regular account; 1: unified trade account»; ``userID``; ``isMaster``;
``deadlineDay`` — «remaining valid days of api key. Only for those api key with no IP bound or the
password has been changed». The value ``"*"`` in ``ips`` isn't described in the docs — treated as «any IP».
Position mode — :meth:`Exchange.is_hedge_mode`.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = ('https://bybit-exchange.github.io/docs/v5/user/apikey-info',)
EXPIRY_WARN_DAYS = 7


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse(info: KeyInfo, response: dict, market_type: str) -> None:
    r = (response or {}).get('result') or {}
    info.raw['query-api'] = r
    perms = r.get('permissions') or {}
    info.permissions = sorted(f'{group}.{p}' for group, items in perms.items() for p in items or [])
    read_only = r.get('readOnly')
    if market_type == 'swap':
        trade = bool(perms.get('ContractTrade') or perms.get('Derivatives'))
    else:
        trade = 'SpotTrade' in (perms.get('Spot') or [])
    info.can_read = True
    # docs show numbers, demo returns strings ('0'/'1') — compare as ints
    read_only_int = _int(read_only)
    info.can_trade = trade and read_only_int == 0 if read_only_int is not None else trade
    wallet = perms.get('Wallet') or []
    info.can_withdraw = 'Withdraw' in wallet
    info.can_transfer = 'AccountTransfer' in wallet
    ips = [str(ip) for ip in r.get('ips') or []]
    info.ips = ips
    info.ip_restricted = bool(ips) and '*' not in ips
    uta = r.get('uta')
    if uta is not None:
        info.account_type_raw = {'uta': uta}
        info.account_type = 'unified' if int(uta) == 1 else 'classic'
    if r.get('userID') not in (None, '', 0):
        info.account_uid = str(r['userID'])
    info.is_master = r.get('isMaster')
    days = _int(r.get('deadlineDay'))
    if days is not None and 0 <= days <= EXPIRY_WARN_DAYS:
        info.warnings.append(f'key expires in {days} days (deadlineDay)')


async def _query(ex: Any) -> dict:
    return await ex._call(ex.ccxt.privateGetV5UserQueryApi)


async def account_uid(ex: Any) -> Optional[str]:
    info = KeyInfo(exchange=ex.name)
    parse(info, await _query(ex), ex.market_type)
    return info.account_uid


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'can_withdraw', lambda: _query(ex))
    if response is None:
        for name in ('can_trade', 'ip_restricted', 'account_uid', 'account_type'):
            info.missing(name, info.unknown['can_withdraw'])
        return info
    parse(info, response, ex.market_type)
    return info
