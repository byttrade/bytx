"""MEXC: API key check.

The Contract API has no key info endpoint (permissions, IP list, UID); spot ``GET /api/v3/account`` returns
``canTrade`` / ``canWithdraw`` — «Can Trade» / «Can Withdraw» of the account, not the key. Check — reading futures
assets (GET /api/v1/private/account/assets): success — the key can read. Trade permission — probed by canceling
a nonexistent order (POST /api/v1/private/order/cancel, doesn't change account state): 2040 «order not
exist» — permission granted, 704 «Transaction information modify permission is required» — not granted. Withdraw permission and
IP list are undetermined (withdrawal policy — via ``withdraw``, see :func:`bytx.key_check.apply_policy`).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import BytxError, PermissionDenied
from bytx.key_check import KeyInfo, probe

DOCS = ('https://mexcdevelop.github.io/apidocs/contract_v1_en/#get-all-informations-of-user-39-s-asset',
        'https://mexcdevelop.github.io/apidocs/spot_v3_en/#account-information')
_REASON = 'MEXC does not report key permissions or IP list (spot canTrade/canWithdraw are account flags)'
# an id that certainly doesn't exist: canceling such an order changes nothing
PROBE_ORDER_ID = '1'
_NOT_EXIST = {2040, 2041}


async def account_uid(ex: Any) -> Optional[str]:
    return None      # the MEXC API doesn't expose the UID


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS))
    response = await probe(info, 'can_read', lambda: ex._call(ex.ccxt.contractPrivateGetAccountAssets))
    if response is not None:
        info.can_read = True
        info.raw['assets'] = response
    await _probe_trade(ex, info)
    for name in ('account_uid', 'ip_restricted', 'can_withdraw', 'can_transfer'):
        info.missing(name, _REASON)
    return info


async def _probe_trade(ex: Any, info: KeyInfo) -> None:
    try:
        response = await ex._call(ex.ccxt.contractPrivatePostOrderCancel, [PROBE_ORDER_ID], safe=False)
    except PermissionDenied as e:
        if e.code == '704':
            info.can_trade = False
        else:
            info.missing('can_trade', f'PermissionDenied: {e.message or e}')
        return
    except BytxError as e:
        info.missing('can_trade', f'{type(e).__name__}: {e.message or e}')
        return
    rows = response.get('data') if isinstance(response, dict) else None
    codes = {r.get('errorCode') for r in rows or [] if isinstance(r, dict)}
    if codes & _NOT_EXIST:
        info.can_trade = True
    else:
        info.missing('can_trade', 'probe by canceling a nonexistent order gave an unexpected response')
