"""Hyperliquid: key check. There are no API keys in the usual sense — there is the account wallet
and the key that signs actions.

Docs:
- «Nonces and API wallets»: «A master account can approve API wallets to sign on behalf of the master
  account or any of the sub-accounts»; «API wallets are only used to sign»; «Nonces are tracked per signer,
  which is the user address if signed with private key of the address, or the agent address if signed
  with an API wallet».
- Exchange endpoint: withdrawal (``withdraw3``) is a wallet-owner action; an agent may only use
  ``agentSendAsset`` — «Destination must match the source address». Hence: signing key = wallet
  address → can withdraw; API wallet (agent) → trades, can't withdraw to a foreign address.
- Info ``userRole``: «"missing", "user", "agent", "vault", or "subAccount"»; for an agent —
  ``{"role":"agent", "data": {"user": "0x..."}}`` (weight 60). Agent not registered / removed
  («API wallet pruning») — ``missing``: signatures will be rejected.
- Info ``userAbstraction``: «"unifiedAccount" | "portfolioMargin" | "disabled" | "default" | "dexAbstraction"».
No IP binding; position mode — one-way only; UID — the wallet address.
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.key_check import KeyInfo, probe

DOCS = (
    'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets',
    'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint',
    'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#query-a-users-role',
    'https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#query-a-users-abstraction-state',
)
ABSTRACTION = {'unifiedAccount': 'unified', 'portfolioMargin': 'portfolio_margin', 'disabled': 'disabled',
               'default': 'default', 'dexAbstraction': 'dex_abstraction'}


def signer_address(ex: Any) -> Optional[str]:
    key = getattr(ex.ccxt, 'privateKey', None)
    if not key:
        return None
    return str(ex.ccxt.privateKeyToAddress(ex.ccxt.remove0x_prefix(str(key)))).lower()


def parse_signer_role(info: KeyInfo, role: Any, wallet: str) -> None:
    """Role of the signing address if it differs from the wallet (must be this wallet's agent)."""
    r = role if isinstance(role, dict) else {}
    info.raw['signer_role'] = r
    kind = r.get('role')
    owner = str(((r.get('data') or {}).get('user')) or '').lower()
    info.agent = kind == 'agent'
    info.can_withdraw = False
    if kind == 'agent' and owner == wallet.lower():
        info.can_trade = True
    elif kind == 'agent':
        info.can_trade = False
        info.warnings.append(f'API wallet is approved for another account ({owner})')
    elif kind == 'missing':
        info.can_trade = False
        info.warnings.append('API wallet is not registered (not approved, expired or removed)')
    else:
        info.can_trade = False
        info.warnings.append(f'signing key is not an API wallet of this account (role={kind!r})')


def parse_wallet_role(info: KeyInfo, role: Any) -> None:
    r = role if isinstance(role, dict) else {}
    info.raw['wallet_role'] = r
    kind = r.get('role')
    if kind == 'user':
        info.is_master = True
    elif kind in ('subAccount', 'vault'):
        info.is_master = False
    elif kind == 'missing':
        info.warnings.append('account wallet is unknown to the exchange (role=missing)')


def parse_abstraction(info: KeyInfo, value: Any) -> None:
    if isinstance(value, str) and value:
        info.account_type_raw = value
        info.account_type = ABSTRACTION.get(value, value)


async def account_uid(ex: Any) -> Optional[str]:
    wallet = getattr(ex.ccxt, 'walletAddress', None)
    return str(wallet).lower() if wallet else None


async def key_info(ex: Any) -> KeyInfo:
    info = KeyInfo(exchange=ex.name, sources=list(DOCS), position_mode='one_way')
    wallet = await account_uid(ex)
    info.account_uid = wallet
    info.missing('ip_restricted', 'Hyperliquid has no key-to-IP binding')
    if not wallet:
        info.missing('can_withdraw', 'no wallet_address')
        return info
    info.can_read = True
    signer = signer_address(ex)
    if signer is None:
        info.can_trade = False
        info.can_withdraw = False
        info.warnings.append('no private_key: read-only')
    elif signer == wallet:
        info.agent = False
        info.can_trade = True
        info.can_withdraw = True     # the wallet key signs withdraw3 too
    else:
        role = await probe(info, 'can_withdraw',
                           lambda: ex._call(ex.ccxt.publicPostInfo, {'type': 'userRole', 'user': signer}))
        if role is not None:
            parse_signer_role(info, role, wallet)
        else:
            info.missing('can_trade', info.unknown['can_withdraw'])
    wallet_role = await probe(info, 'is_master',
                              lambda: ex._call(ex.ccxt.publicPostInfo, {'type': 'userRole', 'user': wallet}))
    if wallet_role is not None:
        parse_wallet_role(info, wallet_role)
    abstraction = await probe(info, 'account_type',
                              lambda: ex._call(ex.ccxt.publicPostInfo, {'type': 'userAbstraction', 'user': wallet}))
    parse_abstraction(info, abstraction)
    return info
