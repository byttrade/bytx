"""Service headers and request tags of the ccxt instance."""
from bytx.net._sig import opts as registry
from bytx.net._sig.headers import apply_broker_ids
from bytx.net._sig.opts import BrokerConfig, get_broker_config, set_broker_config

__all__ = ['registry', 'apply_broker_ids', 'BrokerConfig', 'set_broker_config', 'get_broker_config']
