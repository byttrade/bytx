"""Unified registry of broker identifiers.

All ``bytx.net._sig`` modules get broker ID values only via
:func:`resolve`. Resolution order for each key:

1. ccxt instance configuration (``apply_broker_ids(exchange, config)``);
2. process configuration (:func:`set_broker_config`);
3. environment variable with the same name as the key;
4. built-in SDK value.

``BrokerConfig.disabled()`` at any level turns attribution off: the lookup goes no further
down the chain and neutral values are sent to the exchange. An empty string in a field means "not set
and stop looking". ``BYTX_BROKER_MODE=off`` in env is the same as ``disabled()``.
ccxt tags are never set, in any mode.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

from bytx.net._routes import profile

# Registry keys = environment variable names (compatibility with the previous configuration).
BINANCE_SPOT = 'BINANCE_BROKER_ID_SPOT'
BINANCE_FUTURES = 'BINANCE_BROKER_ID_FUTURES'
BINANCE_LEGACY = 'BINANCE_BROKER_ID_LEGACY'
OKX = 'OKX_BROKER_CODE'
BYBIT = 'BYBIT_BROKER_ID'
BINGX = 'BINGX_SOURCE_KEY'
BITGET = 'BITGET_BROKER_ID'     # X-CHANNEL-API-CODE header (Bitget API Broker, channel api code)
GATE = 'GATE_CHANNEL_ID'        # X-Gate-Channel-Id header (Gate broker channel id)
MEXC = 'MEXC_BROKER_ID'         # source header (MEXC Broker FAQ: "header 'source: Broker_ID'")
HL_ADDRESS = 'HL_BUILDER_ADDRESS'
HL_TESTNET_ADDRESS = 'HL_BUILDER_TESTNET_ADDRESS'
HL_FEE = 'HL_BUILDER_FEE_TENTHS_BP'
KUCOIN_PARTNER = 'KUCOIN_BROKER_PARTNER'
KUCOIN_KEY = 'KUCOIN_BROKER_KEY'
KUCOIN_NAME = 'KUCOIN_BROKER_NAME'
# KuCoin partner signature: 1/true/yes/on — enabled; enabled by default if
# partner and key are set.
KUCOIN_AVAILABLE = 'KUCOIN_BROKER_AVAILABLE'
_TRUTHY = frozenset({'1', 'true', 'yes', 'on'})

KEYS = frozenset({
    BINANCE_SPOT, BINANCE_FUTURES, BINANCE_LEGACY, OKX, BYBIT, BINGX, BITGET, GATE, MEXC,
    HL_ADDRESS, HL_TESTNET_ADDRESS, HL_FEE, KUCOIN_PARTNER, KUCOIN_KEY, KUCOIN_NAME, KUCOIN_AVAILABLE,
})

MODE_ENV = 'BYTX_BROKER_MODE'
MODES = ('full', 'off')

@dataclass(frozen=True)
class BrokerConfig:
    """Broker IDs. ``None`` in a field — the value is taken from the next level."""

    binance_spot: Optional[str] = None
    binance_futures: Optional[str] = None
    binance_legacy: Optional[str] = None
    okx: Optional[str] = None
    bybit: Optional[str] = None
    bingx: Optional[str] = None
    bitget: Optional[str] = None
    gate: Optional[str] = None
    mexc: Optional[str] = None
    hl_builder_address: Optional[str] = None
    hl_builder_testnet_address: Optional[str] = None
    hl_builder_fee_tenths_bp: Optional[int] = None
    kucoin_partner: Optional[str] = None
    kucoin_key: Optional[str] = None
    kucoin_name: Optional[str] = None
    kucoin_broker_available: Optional[bool] = None
    enabled: bool = True
    _by_key: dict = field(default_factory=dict, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self._by_key.update({
            BINANCE_SPOT: self.binance_spot,
            BINANCE_FUTURES: self.binance_futures,
            BINANCE_LEGACY: self.binance_legacy,
            OKX: self.okx,
            BYBIT: self.bybit,
            BINGX: self.bingx,
            BITGET: self.bitget,
            GATE: self.gate,
            MEXC: self.mexc,
            HL_ADDRESS: self.hl_builder_address,
            HL_TESTNET_ADDRESS: self.hl_builder_testnet_address,
            HL_FEE: None if self.hl_builder_fee_tenths_bp is None else str(self.hl_builder_fee_tenths_bp),
            KUCOIN_PARTNER: self.kucoin_partner,
            KUCOIN_KEY: self.kucoin_key,
            KUCOIN_NAME: self.kucoin_name,
            KUCOIN_AVAILABLE: (None if self.kucoin_broker_available is None
                               else '1' if self.kucoin_broker_available else '0'),
        })

    @classmethod
    def disabled(cls) -> 'BrokerConfig':
        """No broker attribution: neither own IDs nor ccxt IDs."""
        return cls(enabled=False)

    def get(self, key: str) -> Optional[str]:
        return self._by_key.get(key)


_process_config: Optional[BrokerConfig] = None


def set_broker_config(config: Optional[BrokerConfig]) -> None:
    """Process-wide broker ID configuration (None — reset)."""
    global _process_config
    _process_config = config


def get_broker_config() -> Optional[BrokerConfig]:
    return _process_config


def attach_config(exchange, config: Optional[BrokerConfig]) -> None:
    """Binds the configuration to a ccxt instance (None — leave unchanged)."""
    if config is not None:
        exchange._bytx_broker_config = config


def broker_mode() -> str:
    """``BYTX_BROKER_MODE`` from env: 'full' (default) / 'off'.

    Unknown value — 'full' and an ERROR in the log.
    """
    raw = (os.getenv(MODE_ENV) or '').strip().lower()
    if not raw:
        return 'full'
    if raw in MODES:
        return raw
    if raw not in _bad_modes_logged:
        _bad_modes_logged.add(raw)
        from loguru import logger
        logger.error(f"{MODE_ENV}={raw!r}: expected {'/'.join(MODES)} — using 'full'")
    return 'full'


_bad_modes_logged: set = set()


def resolve_source(key: str, exchange=None) -> tuple[str, str]:
    """(value, source) of a broker key.

    Source: 'instance' / 'process' (instance / process BrokerConfig), 'env', 'builtin',
    'mode:off' (suppressed by ``BYTX_BROKER_MODE=off``), 'disabled:instance' / 'disabled:process'
    (``BrokerConfig.disabled()``), 'none' — not set anywhere. Empty value — no tag.
    """
    if key not in KEYS:
        raise KeyError(key)
    for level, config in (('instance', getattr(exchange, '_bytx_broker_config', None)),
                          ('process', _process_config)):
        if config is None:
            continue
        if not config.enabled:
            return '', f'disabled:{level}'
        value = config.get(key)
        if value is not None:
            return str(value).strip(), level
    if broker_mode() == 'off':
        return '', 'mode:off'
    value = (os.getenv(key) or '').strip()
    if value:
        return value, 'env'
    value = _builtin(key, exchange)
    return value, 'builtin' if value else 'none'


def _builtin(key: str, exchange=None) -> str:
    if key == KUCOIN_AVAILABLE:
        return '1' if resolve(KUCOIN_PARTNER, exchange) and resolve(KUCOIN_KEY, exchange) else ''
    return profile(key)


def resolve(key: str, exchange=None) -> str:
    """Broker key value ('' — not set or attribution disabled)."""
    return resolve_source(key, exchange)[0]


def sources(keys, exchange=None) -> dict[str, str]:
    """{key: source} for the startup log: where each broker ID came from (values are not logged)."""
    return {key: resolve_source(key, exchange)[1] for key in keys}


def broker_available(exchange_name: str, exchange=None) -> bool:
    """``{EX}_BROKER_AVAILABLE`` gate for the partner signature (currently KuCoin only): 1/true/yes/on."""
    key = f'{exchange_name.upper()}_BROKER_AVAILABLE'
    if key not in KEYS:
        return False
    return resolve(key, exchange).lower() in _TRUTHY


__all__ = ['BrokerConfig', 'set_broker_config', 'get_broker_config', 'resolve', 'resolve_source', 'sources',
           'attach_config', 'broker_mode', 'broker_available', 'MODE_ENV']
