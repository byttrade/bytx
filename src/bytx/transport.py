"""Creating a ccxt.pro instance: exchange, mode (demo/testnet), proxy, broker tags.

The ccxt.pro class inherits the async ccxt class, so one instance serves both REST
and WebSocket. Broker tags are set on the instance right after creation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Union
from urllib.parse import urlsplit

import ccxt.pro as ccxtpro

from bytx.net._sig.headers import apply_broker_ids
from bytx.net._sig.opts import BrokerConfig
from bytx.errors import ConfigError, NotSupported
from bytx.exchanges import apply_ccxt_fixes
from bytx.exchanges.hyperliquid.markets import patch_hyperliquid_markets
from bytx.exchanges.okx.patches import patch_okx_instance
from bytx.net._routes import EXCHANGES

# Where demo/testnet exists: (exchange, market type).
# KuCoin: sandbox (openapi-sandbox / api-sandbox-futures) is shut down — hosts don't resolve (2026-10-25)
# MEXC: no testnet/demo in the docs (spot and futures — only api.mexc.com / contract.mexc.com);
# HTX: "Testnet (Stopped)" — "we decide to shutdown the testnet environment" (Spot API docs).
_NO_DEMO = {('bingx', 'spot'), ('kucoin', 'spot'), ('kucoin', 'swap'), ('mexc', 'spot'), ('mexc', 'swap'),
            ('htx', 'spot'), ('htx', 'swap')}
_PROXY_SCHEMES = ('http', 'https', 'socks4', 'socks5', 'socks5h')


# create_exchange keys for private methods (HL: wallet + agent key; api_key/secret are synonyms).
_CREDENTIALS: dict[str, tuple[str, ...]] = {
    'okx': ('api_key', 'secret', 'password'),
    'kucoin': ('api_key', 'secret', 'password'),
    'bitget': ('api_key', 'secret', 'password'),   # Passphrase (Bitget docs "REST API → Signature")
    'hyperliquid': ('wallet_address', 'private_key'),
}


@dataclass(frozen=True, slots=True)
class ExchangeInfo:
    """Offline exchange info, no keys needed."""

    name: str
    credentials: tuple[str, ...]   # create_exchange keys for private methods
    markets: tuple[str, ...]       # market types: 'spot', 'swap'
    demo: dict[str, bool]          # market type → whether demo/testnet exists (create_exchange(demo=True))


def exchange_info(name: str) -> ExchangeInfo:
    """Info about exchange ``name`` (case-insensitive); unknown — ConfigError."""
    key = name.lower()
    if key not in EXCHANGES:
        raise ConfigError(f'exchange {name!r} is not supported; available: {", ".join(sorted(EXCHANGES))}')
    markets = tuple(sorted(EXCHANGES[key]))
    return ExchangeInfo(name=key, credentials=_CREDENTIALS.get(key, ('api_key', 'secret')), markets=markets,
                        demo={m: (key, m) not in _NO_DEMO for m in markets})


def exchanges_info() -> list[ExchangeInfo]:
    """Info about all supported exchanges (by name)."""
    return [exchange_info(name) for name in sorted(EXCHANGES)]



@dataclass(frozen=True, slots=True)
class Proxy:
    """Proxy for REST and WebSocket of one account."""

    url: str

    def __post_init__(self) -> None:
        parts = urlsplit(self.url)
        if parts.scheme not in _PROXY_SCHEMES or not parts.hostname or not parts.port:
            raise ConfigError(f'proxy must look like scheme://[user:pass@]host:port, '
                              f'scheme ∈ {_PROXY_SCHEMES}')

    @property
    def is_socks(self) -> bool:
        return self.url.startswith('socks')

    def __repr__(self) -> str:  # without login and password
        parts = urlsplit(self.url)
        return f'Proxy({parts.scheme}://{parts.hostname}:{parts.port})'

    @property
    def key(self) -> str:
        """Key for the limiter and cache: one proxy = one external IP."""
        parts = urlsplit(self.url)
        return f'{parts.hostname}:{parts.port}'


def _apply_proxy(ex: Any, proxy: Optional[Proxy]) -> None:
    if proxy is None:
        return
    if proxy.is_socks:
        ex.socksProxy = proxy.url
        ex.wsSocksProxy = proxy.url
    else:
        ex.httpsProxy = proxy.url
        ex.wsProxy = proxy.url


DemoArg = Union[bool, str]
_DEMO_VALUES = (True, 'demo', 'testnet')


def demo_env(name: str, demo: DemoArg) -> Optional[str]:
    """create_exchange ``demo`` → environment: None (live), 'demo' or 'testnet'.

    True and 'demo' — the exchange demo environment (Binance — Demo Mode demo.binance.com). 'testnet' — Binance
    only: the legacy testnet (testnet.binance.vision / testnet.binancefuture.com); other exchanges have
    a single demo environment — ConfigError.
    """
    if demo is False or demo is None:
        return None
    if isinstance(demo, str):
        demo = demo.strip().lower()
    if demo not in _DEMO_VALUES:
        raise ConfigError(f"demo: False, True, 'demo' or 'testnet' (binance only), not {demo!r}",
                          exchange=name.lower())
    if demo == 'testnet':
        if name.lower() != 'binance':
            raise ConfigError(f"demo='testnet' — binance only ({name.lower()} has a single demo environment: demo=True)",
                              exchange=name.lower())
        return 'testnet'
    return 'demo'


def _apply_demo(ex: Any, name: str, market_type: str, env: str = 'demo') -> None:
    if (name, market_type) in _NO_DEMO:
        raise NotSupported(f'demo/testnet is not available for {name} {market_type}', exchange=name)
    if name == 'bybit':
        ex.enable_demo_trading(True)
    elif name == 'binance':
        from bytx.exchanges.binance import demo as binance_demo
        if env == 'testnet':
            binance_demo.enable_testnet(ex)
        else:
            binance_demo.enable(ex)
    else:
        ex.set_sandbox_mode(True)


# swap client: only the market types the SDK allows for swap, no currencies. Before: Binance swap — 6 requests on
# connect (spot, margin, fapi, dapi, sapi capital), 4150 markets, 39 MiB; Bybit — spot, options, currencies; OKX —
# SPOT, FUTURES, OPTION, currencies. User values in ``options`` take precedence (OKX dated futures —
# options={'fetchMarkets': ['swap', 'future']}).
SWAP_MARKET_OPTIONS: dict[str, dict[str, Any]] = {
    'binance': {'fetchMarkets': ['linear', 'inverse'], 'fetchMargins': False, 'fetchCurrencies': False},
    'bybit': {'fetchMarkets': ['linear', 'inverse']},
    'okx': {'fetchMarkets': ['swap']},
}
#: for these exchanges ccxt loads currencies without an off switch: the instance's fetch_currencies → None (currencies
#: come from the markets)
_NO_CURRENCIES = frozenset({'bybit', 'okx'})


def apply_swap_market_options(ex: Any, name: str, user_options: Optional[dict] = None) -> None:
    """Market loading options of a swap client (:data:`SWAP_MARKET_OPTIONS`) on the instance; ``user_options`` — what
    the user set (those values are left alone)."""
    user = user_options or {}
    for k, v in SWAP_MARKET_OPTIONS.get(name, {}).items():
        if k not in user:
            ex.options[k] = list(v) if isinstance(v, list) else v
    if name in _NO_CURRENCIES and not user.get('fetchCurrencies'):
        async def fetch_currencies(params: Any = None) -> None:
            return None
        ex.fetch_currencies = fetch_currencies


def build_ccxt(name: str, *, market_type: str = 'swap', api_key: Optional[str] = None,
               secret: Optional[str] = None, password: Optional[str] = None,
               wallet_address: Optional[str] = None, private_key: Optional[str] = None,
               demo: DemoArg = False, proxy: Optional[Proxy] = None,
               broker: Optional[BrokerConfig] = None, timeout_ms: int = 10_000,
               recv_window_ms: Optional[int] = None, options: Optional[dict] = None) -> Any:
    """Configured ccxt.pro instance (does not load markets, no network)."""
    name = name.lower()
    if name not in EXCHANGES:
        raise ConfigError(f'exchange {name!r} is not supported; available: {", ".join(sorted(EXCHANGES))}')
    if market_type not in EXCHANGES[name]:
        raise ConfigError(f'market type {market_type!r} is not supported for {name}')
    ccxt_id = EXCHANGES[name][market_type]
    env = demo_env(name, demo)

    config: dict[str, Any] = {
        'enableRateLimit': True,
        'timeout': timeout_ms,
        'options': {'defaultType': market_type, **(options or {})},
    }
    if name == 'hyperliquid':
        wallet_address = wallet_address or api_key
        private_key = private_key or secret
        if wallet_address:
            config['walletAddress'] = wallet_address
        if private_key:
            config['privateKey'] = private_key
    else:
        if api_key:
            config['apiKey'] = api_key
        if secret:
            config['secret'] = secret
        if password:
            config['password'] = password
    if recv_window_ms is not None:
        config['options']['recvWindow'] = recv_window_ms

    ex = getattr(ccxtpro, ccxt_id)(config)
    if market_type == 'swap':
        apply_swap_market_options(ex, name, options)
    _apply_proxy(ex, proxy)
    # broker IDs — before sandbox
    apply_broker_ids(ex, broker)
    patch_hyperliquid_markets(ex)
    if name == 'hyperliquid':
        _install_hyperliquid_fast_paths(ex)
    patch_okx_instance(ex)
    if env is not None:
        _apply_demo(ex, name, market_type, env)
    apply_ccxt_fixes(ex)  # after demo: some fixes change WS URLs
    return ex


def _install_hyperliquid_fast_paths(ex: Any) -> None:
    """HL: signing via coincurve (if installed) and batch cancel as one action."""
    from bytx.exchanges.hyperliquid.fast_sign import cancel_batch, install_fast_sign
    install_fast_sign(ex)

    async def bytx_cancel_batch(params: dict) -> Any:
        return await cancel_batch(ex, params)
    ex.bytx_cancel_batch = bytx_cancel_batch


__all__ = ['EXCHANGES', 'Proxy', 'SWAP_MARKET_OPTIONS', 'apply_swap_market_options', 'build_ccxt']
