"""KuCoin: API key check — GET /api/v1/user/api-key («Get Apikey Info»).

Docs: https://www.kucoin.com/docs-new/api-3470130 — «Both master and sub user's api key are
applicable». Fields: ``permission`` — «possible values: General, Spot, Margin, Futures, InnerTransfer,
Transfer, Earn»; ``ipWhitelist`` — «IP whitelist» (comma-separated string); ``uid``; ``isMaster``.
Change log 2026.02.15: «InnerTransfer (Universal Transfer), Transfer (withdrawal)»; 2026.08.12: «When the
permissions include `Transfer (Withdraw)`, `ipWhitelist` is required». ``General`` — read-only.
ccxt 4.4.65 kucoinfutures lacks this endpoint — the request goes via ``ccxt.request`` (instance signature).
The endpoint doesn't report the account type. Position mode — GET /api/v2/position/getPositionMode
(:mod:`bytx.exchanges.kucoin.position_mode`): hedge goes into ``position_mode`` and the warnings
(the SDK won't trade on such an account — :meth:`Exchange.is_hedge_mode` raises ConfigError).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.exchanges.kucoin import position_mode
from bytx.key_check import KeyInfo, probe

DOCS = ('https://www.kucoin.com/docs-new/api-3470130', 'https://www.kucoin.com/docs-new/change-log',
        position_mode.DOCS)


def parse(info: KeyInfo, response: dict, market_type: str) -> None:
    r = (response or {}).get('data') or {}
    info.raw['api-key'] = r
    perms = [p.strip() for p in str(r.get('permission') or '').split(',') if p.strip()]
    info.permissions = perms
    if perms:
        info.can_read = 'General' in perms
        info.can_trade = ('Futures' in perms) if market_type == 'swap' else ('Spot' in perms)
        # fail-safe: older KuCoin keys report «Withdrawal» — also a withdraw permission
        info.can_withdraw = 'Transfer' in perms or 'Withdrawal' in perms
        info.can_transfer = 'InnerTransfer' in perms
    if 'ipWhitelist' in r or perms:
        ips = [ip.strip() for ip in str(r.get('ipWhitelist') or '').split(',') if ip.strip()]
        info.ips = ips
        info.ip_restricted = bool(ips)
    if r.get('uid') not in (None, ''):
        info.account_uid = str(r['uid'])
    info.is_master = r.get('isMaster')


async def _query(ex: Any) -> dict:
    return await ex._call(ex.ccxt.request, 'user/api-key', 'private', 'GET', {})


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
    info.missing('account_type', 'the KuCoin key endpoint does not report the account type')
    if ex.market_type == 'swap':
        hedge = await probe(info, 'position_mode', lambda: position_mode.read(ex))
        if hedge is not None:
            info.position_mode = 'hedge' if hedge else 'one_way'
        if hedge:
            info.warnings.append(f'account is in hedge mode: {position_mode.POSITION_MODE_HINT}')
    return info
