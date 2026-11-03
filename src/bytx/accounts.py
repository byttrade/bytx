"""Many accounts in one process: a proxy per account, shared markets.

    pool = AccountPool.from_file('accounts.json')        # or AccountPool([...])
    async with pool:
        balances = await pool.gather(lambda ex: ex.balance())
        await pool['scalper-1'].buy('BTC/USDT', notional=20, price=..., post_only=True)

Accounts file (JSON or YAML): a list of objects with :class:`AccountConfig` fields.
Secrets need not be stored in the file: a ``"${ENV_NAME}"`` value is read from the environment.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Iterable, Iterator, Optional, TypeVar, Union

from bytx.errors import BytxError, ConfigError, from_ccxt
from bytx.exchange import Exchange, create_exchange
from bytx.transport import Proxy

if TYPE_CHECKING:
    from bytx.health import HealthReport
    from bytx.key_check import KeyInfo
    from bytx.portfolio import PortfolioReport

T = TypeVar('T')
_ENV_REF = re.compile(r'^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$')


@dataclass(slots=True)
class AccountConfig:
    name: str
    exchange: str
    api_key: Optional[str] = None
    secret: Optional[str] = None
    password: Optional[str] = None
    wallet_address: Optional[str] = None
    private_key: Optional[str] = None
    proxy: Optional[str] = None
    demo: bool = False
    market: str = 'swap'
    options: dict = field(default_factory=dict)
    # derivatives margin mode: 'cross' / 'isolated'; None means not set
    margin_mode: Optional[str] = None

    def __repr__(self) -> str:  # no secrets
        proxy = repr(Proxy(self.proxy)) if self.proxy else None
        return f'AccountConfig({self.name!r}, {self.exchange}, {self.market}, demo={self.demo}, proxy={proxy})'

    @classmethod
    def from_dict(cls, data: dict) -> 'AccountConfig':
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ConfigError(f'unknown account fields: {sorted(unknown)}')
        values = {k: _expand_env(v) for k, v in data.items()}
        if not values.get('name') or not values.get('exchange'):
            raise ConfigError('account requires name and exchange')
        return cls(**values)


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        match = _ENV_REF.fullmatch(value.strip())
        if match:
            env = os.getenv(match.group(1))
            if env is None:
                raise ConfigError(f'environment variable {match.group(1)} is not set')
            return env
    return value


class AccountPool:
    """A set of accounts. Exchange clients are created lazily, on first access."""

    def __init__(self, accounts: Iterable[Union[AccountConfig, dict]], **exchange_kwargs: Any) -> None:
        self._configs: dict[str, AccountConfig] = {}
        for acc in accounts:
            cfg = acc if isinstance(acc, AccountConfig) else AccountConfig.from_dict(acc)
            if cfg.name in self._configs:
                raise ConfigError(f'duplicate account name: {cfg.name}')
            if cfg.proxy:
                Proxy(cfg.proxy)  # validate format now, not on the first request
            self._configs[cfg.name] = cfg
        self._exchange_kwargs = exchange_kwargs
        self._clients: dict[str, Exchange] = {}

    @classmethod
    def from_file(cls, path: Union[str, Path], **exchange_kwargs: Any) -> 'AccountPool':
        text = Path(path).read_text(encoding='utf-8')
        if str(path).endswith(('.yaml', '.yml')):
            try:
                import yaml  # type: ignore[import-untyped]
            except ImportError as e:
                raise ConfigError('install pyyaml for YAML') from e
            data = yaml.safe_load(text)
        else:
            data = json.loads(text)
        if isinstance(data, dict):
            data = data.get('accounts', [])
        return cls(data, **exchange_kwargs)

    # ─── access ─────────────────────────────────────────────────────────────

    @property
    def names(self) -> list[str]:
        return list(self._configs)

    def config(self, name: str) -> AccountConfig:
        try:
            return self._configs[name]
        except KeyError:
            raise ConfigError(f'account {name!r} not found') from None

    def __getitem__(self, name: str) -> Exchange:
        client = self._clients.get(name)
        if client is None:
            cfg = self.config(name)
            client = create_exchange(
                cfg.exchange, api_key=cfg.api_key, secret=cfg.secret, password=cfg.password,
                wallet_address=cfg.wallet_address, private_key=cfg.private_key, demo=cfg.demo,
                market=cfg.market, proxy=cfg.proxy, options=cfg.options or None,
                **{**self._exchange_kwargs,
                   **({'margin_mode': cfg.margin_mode} if cfg.margin_mode else {})})
            client.account = name   # account name in events, notifications, metrics
            self._clients[name] = client
        return client

    def __iter__(self) -> Iterator[str]:
        return iter(self._configs)

    def __len__(self) -> int:
        return len(self._configs)

    def by_exchange(self, exchange: str) -> list[str]:
        return [n for n, c in self._configs.items() if c.exchange == exchange.lower()]

    # ─── bulk operations ────────────────────────────────────────────────────

    async def gather(self, fn: Callable[[Exchange], Awaitable[T]], *,
                     names: Optional[Iterable[str]] = None,
                     concurrency: int = 20) -> dict[str, Union[T, BytxError]]:
        """Runs ``fn(exchange)`` per account. One failure does not affect the others:
        the dict holds a :class:`BytxError` exception instead of the result.

        Any exception from ``fn`` (``ValueError``, ``asyncio.TimeoutError``, ccxt…) is converted
        to :class:`BytxError` via :func:`~bytx.errors.from_ccxt`; the original is in
        ``__cause__``. Cancellation (``CancelledError``) is not caught."""
        sem = asyncio.Semaphore(concurrency)
        selected = list(names) if names is not None else self.names

        async def run(name: str) -> tuple[str, Any]:
            async with sem:
                try:
                    return name, await fn(self[name])
                except Exception as e:  # noqa: BLE001 — CancelledError (BaseException) is not caught
                    cfg = self._configs.get(name)
                    return name, from_ccxt(e, cfg.exchange if cfg else None)

        return dict(await asyncio.gather(*(run(n) for n in selected)))

    async def connect(self, names: Optional[Iterable[str]] = None) -> None:
        results = await self.gather(lambda ex: ex.connect(), names=names)
        failed = {n: r for n, r in results.items() if isinstance(r, BytxError)}
        if failed:
            first = next(iter(failed.values()))
            raise BytxError(f'failed to connect accounts: {sorted(failed)} ({first})') from first

    async def close(self) -> None:
        clients, self._clients = list(self._clients.values()), {}
        await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    async def __aenter__(self) -> 'AccountPool':
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ─── portfolio, keys, health ───────────────────────────────────

    async def portfolio(self, *, since: int, until: Optional[int] = None, names: Optional[Iterable[str]] = None,
                        concurrency: int = 10, keep_items: bool = False) -> 'PortfolioReport':
        """Portfolio per account over ``[since, until]`` (ms); see :func:`bytx.portfolio.portfolio`.
        ``report.complete`` means all data was fetched in full; CSV via ``report.to_csv(path)``."""
        from bytx.portfolio import portfolio
        return await portfolio(self, since=since, until=until, names=names, concurrency=concurrency,
                               keep_items=keep_items)

    async def check_keys(self, *, withdraw: str = 'reject', names: Optional[Iterable[str]] = None,
                         concurrency: int = 10) -> dict[str, Union['KeyInfo', BytxError]]:
        """:meth:`Exchange.check_key` per account: {name: KeyInfo or error} (a withdraw-enabled key with
        ``withdraw='reject'`` yields :class:`~bytx.errors.UnsafeApiKey` in the dict, not raised)."""
        return await self.gather(lambda ex: ex.check_key(withdraw=withdraw),  # type: ignore[arg-type]
                                 names=names, concurrency=concurrency)

    async def health(self, *, private: Optional[bool] = None, names: Optional[Iterable[str]] = None,
                     concurrency: int = 10) -> dict[str, Union['HealthReport', BytxError]]:
        """:meth:`Exchange.health` per account: {name: HealthReport or error}."""
        return await self.gather(lambda ex: ex.health(private=private), names=names, concurrency=concurrency)

    # ─── diagnostics ────────────────────────────────────────────────────────

    def ip_groups(self) -> dict[tuple[str, str], list[str]]:
        """Groups of accounts that hit the same exchange from the same external IP.

        Key: (exchange, proxy or 'direct'). A group of more than one account shares the exchange's
        per-IP rate limits; with many accounts this risks 429s and bans.
        """
        groups: dict[tuple[str, str], list[str]] = {}
        for name, cfg in self._configs.items():
            key = (cfg.exchange, Proxy(cfg.proxy).key if cfg.proxy else 'direct')
            groups.setdefault(key, []).append(name)
        return groups

    def shared_ip_warnings(self, threshold: int = 2) -> list[str]:
        return [f'{exchange}: {len(names)} accounts via {via}: {", ".join(names)}'
                for (exchange, via), names in self.ip_groups().items() if len(names) >= threshold]


__all__ = ['AccountConfig', 'AccountPool']
