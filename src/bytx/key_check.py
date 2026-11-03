"""API key check: permissions (trading, withdrawal, transfers), IP binding, account type,
position mode and account UID — via the key-permission endpoints from each exchange's official docs
(``bytx.exchanges.<exchange>.api_key``).

    info = await ex.check_key()                 # withdrawal permission → UnsafeApiKey
    info = await ex.check_key(withdraw='warn')  # withdrawal — only a warning in info.warnings
    uid = await ex.account_uid()

Whatever could not be determined (endpoint unavailable, exchange does not return the field) goes to ``unknown``
(field → reason) and the value stays ``None``: "not checked" ≠ "no permission".
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Literal, Optional

from bytx.errors import BytxError, ConfigError, UnsafeApiKey

if TYPE_CHECKING:
    from bytx.exchange import Exchange

WithdrawPolicy = Literal['reject', 'warn', 'allow']
PositionMode = Literal['hedge', 'one_way']


@dataclass(slots=True)
class KeyInfo:
    """What the exchange reports about the key. ``None`` — undetermined (reason in ``unknown``)."""

    exchange: str
    account_uid: Optional[str] = None
    can_read: Optional[bool] = None
    can_trade: Optional[bool] = None
    can_withdraw: Optional[bool] = None
    can_transfer: Optional[bool] = None
    ip_restricted: Optional[bool] = None
    ips: list[str] = field(default_factory=list)
    #: account type per exchange docs: 'unified' / 'classic' (Bybit), 'futures_mode' / 'multi_currency_margin' /
    #: 'portfolio_margin' / 'spot_mode' (OKX acctLv), 'multi_assets' / 'single_asset' (Binance USDⓈ-M),
    #: userAbstraction values (Hyperliquid); raw value — ``account_type_raw``
    account_type: Optional[str] = None
    account_type_raw: Any = None
    position_mode: Optional[PositionMode] = None
    is_master: Optional[bool] = None
    #: Hyperliquid: signed by an API wallet (agent) — True; by the wallet's main key — False
    agent: Optional[bool] = None
    permissions: list[str] = field(default_factory=list)
    unknown: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    def missing(self, name: str, reason: str) -> None:
        self.unknown.setdefault(name, reason)

    @property
    def ok(self) -> bool:
        """The key can trade and cannot withdraw funds (both confirmed by the exchange)."""
        return self.can_trade is True and self.can_withdraw is False


def _reason(e: BaseException) -> str:
    if isinstance(e, BytxError):
        return f'{type(e).__name__}: {e.message or e}'
    return f'{type(e).__name__}: {e}'


async def probe(info: KeyInfo, name: str, fn: Callable[[], Awaitable[Any]]) -> Any:
    """Request for one part of the check: a failure does not break the whole check but is recorded in ``unknown``."""
    try:
        return await fn()
    except BytxError as e:
        info.missing(name, _reason(e))
    return None


def apply_policy(info: KeyInfo, withdraw: WithdrawPolicy) -> KeyInfo:
    """Warnings and policy rejection. ``withdraw='reject'`` plus withdrawal permission → :class:`UnsafeApiKey`."""
    if withdraw not in ('reject', 'warn', 'allow'):
        raise ConfigError(f"withdraw: 'reject' | 'warn' | 'allow', not {withdraw!r}", exchange=info.exchange)
    if info.can_withdraw is None:
        info.warnings.append('withdrawal permission not checked: ' + info.unknown.get('can_withdraw', 'exchange did not report'))
    if info.can_trade is False:
        info.warnings.append('key has no trading permission')
    elif info.can_trade is None:
        info.warnings.append('trading permission not checked: ' + info.unknown.get('can_trade', 'exchange did not report'))
    if info.ip_restricted is False:
        info.warnings.append('key is not bound to an IP')
    if info.can_withdraw:
        message = 'key has withdrawal permission'
        if withdraw == 'reject':
            raise UnsafeApiKey(f'{message}: create a key without withdrawal (policy withdraw="reject")',
                               exchange=info.exchange, details={'key': info})
        if withdraw == 'warn':
            info.warnings.append(message)
    return info


async def fill_position_mode(ex: 'Exchange', info: KeyInfo) -> None:
    """Position mode via :meth:`Exchange.is_hedge_mode` (mode endpoints per exchange docs)."""
    if info.position_mode is not None:
        return
    if ex.market_type != 'swap':
        info.missing('position_mode', 'derivatives only')
        return
    hedge = await probe(info, 'position_mode', lambda: ex.is_hedge_mode(refresh=True))
    if hedge is not None:
        info.position_mode = 'hedge' if hedge else 'one_way'


async def check_key(ex: 'Exchange', *, withdraw: WithdrawPolicy = 'reject') -> KeyInfo:
    from bytx.portfolio import exchange_module
    if withdraw not in ('reject', 'warn', 'allow'):
        raise ConfigError(f"withdraw: 'reject' | 'warn' | 'allow', not {withdraw!r}", exchange=ex.name)
    if getattr(ex, 'is_paper', False):
        raise ConfigError('check_key: paper account has no exchange key', exchange=ex.name)
    await ex.connect()
    info: KeyInfo = await exchange_module(ex, 'api_key').key_info(ex)
    await fill_position_mode(ex, info)
    return apply_policy(info, withdraw)


async def account_uid(ex: 'Exchange') -> str:
    """Exchange account UID (Hyperliquid — wallet address). Not provided by the exchange — :class:`ConfigError`."""
    from bytx.portfolio import exchange_module
    await ex.connect()
    uid = await exchange_module(ex, 'api_key').account_uid(ex)
    if not uid:
        raise ConfigError('exchange did not return the account UID', exchange=ex.name)
    return str(uid)


__all__ = ['KeyInfo', 'PositionMode', 'WithdrawPolicy', 'account_uid', 'apply_policy', 'check_key', 'probe']
