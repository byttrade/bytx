"""Binance: API key check.

- GET /sapi/v1/account/apiRestrictions («Get API Key Permission»), docs:
  https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/account#get-api-key-permission
  ``enableWithdrawals`` — «allows you to withdraw via API. You must apply the IP Access Restriction
  filter in order to enable withdrawals»; ``ipRestrict``; ``enableFutures`` / ``enableSpotAndMarginTrading``
  — futures / spot trading; ``enableReading``; ``enableInternalTransfer``, ``permitsUniversalTransfer``.
- UID — GET /api/v3/account (Account information): field ``uid``.
- USDⓈ-M account type — GET /fapi/v1/multiAssetsMargin: ``multiAssetsMargin`` — «true: Multi-Assets
  Mode; false: Single-Asset Mode».
- Position mode — :meth:`Exchange.is_hedge_mode` (GET /fapi/v1/positionSide/dual).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = (
    'https://developers.binance.com/en/docs/catalog/core-trading-wallet/api/rest-api/account#get-api-key-permission',
    'https://developers.binance.com/docs/binance-spot-api-docs/rest-api/account-endpoints#account-information-user_data',
    'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/'
    'account#get-current-multi-assets-mode',
)


def parse_restrictions(info: KeyInfo, r: dict, market_type: str) -> None:
    info.raw['apiRestrictions'] = r
    info.can_read = r.get('enableReading')
    info.can_withdraw = r.get('enableWithdrawals')
    info.ip_restricted = r.get('ipRestrict')
    trade_flag = 'enableFutures' if market_type == 'swap' else 'enableSpotAndMarginTrading'
    info.can_trade = r.get(trade_flag)
    if r.get('enableInternalTransfer') is not None or r.get('permitsUniversalTransfer') is not None:
        info.can_transfer = bool(r.get('enableInternalTransfer') or r.get('permitsUniversalTransfer'))
    info.permissions = sorted(k for k, v in r.items() if (k.startswith('enable') or k.startswith('permits')) and v)


def parse_uid(account: dict) -> Optional[str]:
    uid = (account or {}).get('uid')
    return None if uid in (None, '') else str(uid)


def parse_multi_assets(info: KeyInfo, r: dict) -> None:
    value = (r or {}).get('multiAssetsMargin')
    if value is None:
        return
    info.account_type_raw = value
    info.account_type = 'multi_assets' if value else 'single_asset'


async def account_uid(ex: Any) -> Optional[str]:
    return parse_uid(await ex._call(ex.ccxt.privateGetAccount, {'omitZeroBalances': True}))


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    r = await probe(info, 'can_withdraw', lambda: ex._call(ex.ccxt.sapiGetAccountApiRestrictions))
    if r is not None:
        parse_restrictions(info, r, ex.market_type)
    else:
        for name in ('can_trade', 'ip_restricted', 'can_read'):
            info.missing(name, info.unknown['can_withdraw'])
    uid = await probe(info, 'account_uid', lambda: account_uid(ex))
    info.account_uid = uid
    if ex.market_type == 'swap':
        mam = await probe(info, 'account_type', lambda: ex._call(ex.ccxt.fapiPrivateGetMultiAssetsMargin))
        if mam is not None:
            parse_multi_assets(info, mam)
    return info
