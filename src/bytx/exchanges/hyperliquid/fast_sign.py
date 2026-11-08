"""Hyperliquid: signing actions via libsecp256k1 (``coincurve``, extra ``bytx[fast]``).

ccxt 4.4.65 signs every action (order, cancel, leverage) in pure Python (``static_dependencies/ecdsa``):
~40–60 ms synchronously in the event loop; cancel_all of 8 orders blocked it for ~315 ms. The instance's ``sign_hash``
is replaced with coincurve: deterministic RFC6979 nonce (HMAC-SHA256), low-s, ``v = 27 + recid`` — byte-for-byte the
same signature as ccxt. Measured: ~4 ms per action instead of ~50 (the rest is ccxt keccak and EIP-712, not the main
share — not replaced). Without coincurve the behaviour is unchanged, one WARNING per process.
"""
from __future__ import annotations

from typing import Any

from loguru import logger

_warned = False


def install_fast_sign(exchange: Any) -> bool:
    """Installs fast signing on an HL instance; False — coincurve is not installed (ccxt signing)."""
    global _warned
    try:
        import coincurve
    except ImportError:
        if not _warned:
            _warned = True
            logger.warning('bytx: hyperliquid — pure Python signing (~50 ms in the event loop per action); '
                           'install bytx[fast] (coincurve)')
        return False
    keys: dict[str, Any] = {}

    def sign_hash(hash: str, privateKey: str) -> dict:
        secret = privateKey[-64:]
        key = keys.get(secret)
        if key is None:
            key = keys[secret] = coincurve.PrivateKey(bytes.fromhex(secret))
        sig = key.sign_recoverable(bytes.fromhex(hash[-64:]), hasher=None)
        return {'r': '0x' + sig[:32].hex(), 's': '0x' + sig[32:64].hex(), 'v': 27 + sig[64]}

    exchange.sign_hash = sign_hash
    return True


async def cancel_batch(exchange: Any, params: dict) -> Any:
    """One cancel action for several orders (docs «Cancel order(s)»: ``cancels`` — an array of {a, o}); the raw
    exchange response (statuses in order). ``params``: ``symbol`` (ccxt symbol), ``ids`` (oid)."""
    market = exchange.market(params['symbol'])
    nonce = exchange.milliseconds()
    asset = exchange.parse_to_numeric(market['baseId'])
    action = {'type': 'cancel', 'cancels': [{'a': asset, 'o': exchange.parse_to_numeric(i)} for i in params['ids']]}
    vault = exchange.format_vault_address(exchange.safe_string(params, 'vaultAddress'))
    request: dict = {'nonce': nonce, 'action': action, 'signature': exchange.sign_l1_action(action, nonce, vault)}
    if vault is not None:
        request['vaultAddress'] = vault
    return await exchange.privatePostExchange(request)


__all__ = ['cancel_batch', 'install_fast_sign']
