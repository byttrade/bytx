"""bytx — async Python SDK for crypto exchanges.

    from bytx import create_exchange

    async with create_exchange('bybit', api_key=..., secret=..., demo=True) as ex:
        print(await ex.ticker('BTC/USDT'))

"""
from loguru import logger as _logger

# The library does not write to the user's log until the user enables it:
# loguru.logger.enable('bytx')
_logger.disable('bytx')

from bytx.exchange import Exchange, create_exchange
from bytx.accounts import AccountConfig, AccountPool
from bytx.protection import DeadManSwitch, RiskGuard, TpSlModifyResult, TpSlOrder
from bytx.streams import StateEvent, StreamConfig, Streams
from bytx.net import MetricsSnapshot
from bytx.trackers import OrderTracker, PositionTracker
from bytx.transport import ExchangeInfo, Proxy, exchange_info, exchanges_info
from bytx import errors, types
from bytx.net._sig import apply_broker_ids, BrokerConfig, set_broker_config
from bytx.exchanges.binance.broker_cid import (
    BINANCE_CID_MAX_LEN,
    binance_max_logical_cid_len,
    binance_orphan_counts,
    binance_unattributed_counts,
)
from bytx.exchanges.hyperliquid.builder import (
    HyperliquidBuilderCheckUnavailableError,
    HyperliquidBuilderFeeRejected,
    HyperliquidBuilderNotApprovedError,
    HyperliquidBuilderStartError,
    builder_approval_typed_data,
    builder_config as hl_builder_config,
    builder_require_approval as hl_builder_require_approval,
    get_builder_approval,
    hl_builder_counts,
    reset_approval_cache as hl_reset_builder_approval_cache,
    set_builder_fallback_hook as hl_set_builder_fallback_hook,
)
from bytx.exchanges.hyperliquid.markets import patch_hyperliquid_markets
from bytx.utils.text import to_exchange_symbol
from bytx.order_params import exchange_client_id, generate_client_id, max_client_id_len, owns
from bytx.__version__ import (
    __version__,
    __version_info__,
    __title__,
    __description__,
    __url__,
    __author__,
    __author_email__,
    __license__,
    __copyright__,
)
from bytx.errors import PositionOpenError

__all__ = [
    'Exchange',
    'create_exchange',
    'AccountConfig',
    'AccountPool',
    'DeadManSwitch',
    'RiskGuard',
    'TpSlOrder',
    'TpSlModifyResult',
    'StateEvent',
    'StreamConfig',
    'Streams',
    'MetricsSnapshot',
    'OrderTracker',
    'PositionTracker',
    'Proxy',
    'ExchangeInfo',
    'exchange_info',
    'exchanges_info',
    'errors',
    'types',
    # Broker
    'apply_broker_ids',
    'BrokerConfig',
    'set_broker_config',
    'BINANCE_CID_MAX_LEN',
    'binance_max_logical_cid_len',
    'binance_unattributed_counts',
    'binance_orphan_counts',
    'patch_hyperliquid_markets',
    # Hyperliquid builder fee
    'HyperliquidBuilderFeeRejected',
    'HyperliquidBuilderNotApprovedError',
    'HyperliquidBuilderCheckUnavailableError',
    'HyperliquidBuilderStartError',
    'builder_approval_typed_data',
    'hl_builder_config',
    'hl_builder_require_approval',
    'get_builder_approval',
    'hl_builder_counts',
    'hl_reset_builder_approval_cache',
    'hl_set_builder_fallback_hook',
    'to_exchange_symbol',
    # client_id
    'exchange_client_id',
    'generate_client_id',
    'max_client_id_len',
    'owns',
    # errors
    'PositionOpenError',
    # Version metadata
    '__version__',
    '__version_info__',
    '__title__',
    '__description__',
    '__url__',
    '__author__',
    '__author_email__',
    '__license__',
    '__copyright__',
]

