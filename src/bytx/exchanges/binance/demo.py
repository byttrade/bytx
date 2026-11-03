"""Binance Demo Mode (demo.binance.com) — the default ``create_exchange(..., demo=True)`` mode.

Docs (verified 2026-10-26):

- Spot «Demo Mode → General Info»
  (https://developers.binance.com/docs/binance-spot-api-docs/demo-mode/general-info):
  REST ``https://demo-api.binance.com/api``, WebSocket API ``wss://demo-ws-api.binance.com/ws-api/v3``,
  streams ``wss://demo-stream.binance.com/ws`` (and ``/stream``, port 9443).
- USDⓈ-M «General Info» (https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info):
  REST ``https://demo-fapi.binance.com``, WS ``wss://demo-fstream.binance.com``.
  «WebSocket API General Info» — only ``wss://testnet.binancefuture.com/ws-fapi/v1``: there is no
  ``demo-ws-fapi`` host (handshake 202 → www.binance.com, 2026-10-26), and testnet.binancefuture.com is
  the same backend as demo-fapi (one book and lastUpdateId, live check without keys).
- COIN-M «General Info»: REST ``https://demo-dapi.binance.com``, WS ``wss://demo-dstream.binance.com``.

Difference from testnet (``demo='testnet'``, testnet.binance.vision / testnet.binancefuture.com), per the
Demo Mode docs: prices and books are close to live (but not real trades), filters, IP limits and the number
of open orders are «exactly the same as live», the feature set matches live; the balance
can be reset in the UI at any time. Spot testnet is independent of live (own prices and books), the balance
resets monthly, new features sometimes appear before live. A demo.binance.com key
is shared by spot and futures.

Demo Mode has no ``/sapi`` (wallet, system, keys), ``/papi``, ``/eapi`` or ``/futures/data``
(demo-fapi answers the latter with an «ok» stub): there are no such keys in ``urls['api']``, the request raises ccxt
``NotSupported`` with a reason (before hitting the network).

ccxt 4.4.65 doesn't know Demo Mode: we enable its sandbox (the ``sandboxMode`` flag — ccxt doesn't call
sapi for currencies and margin, ``apiBackup`` holds live URLs) and swap the URL table. Parts of ccxt
tied to the host name are fixed here as well:

- ``get_exceptions_by_url`` — market type by host (``api.`` / ``fapi.`` / ``dapi.``): demo hosts
  are mapped to live ones, otherwise market error codes (-2019, -4164…) are not recognized;
- spot streams: ``handle_order_book`` / ``handle_trade`` / ``handle_ohlcv`` /
  ``handle_tickers_and_bids_asks`` detect spot by the client URL (``wss://stream.binance.com``,
  ``/stream.binance.``, ``/testnet.binance``, ``/stream``) — neither demo-stream nor
  stream.testnet.binance.vision matches, so a spot event was parsed as a contract (symbol
  ``BTC/USDT:USDT`` instead of ``BTC/USDT``). During synchronous parsing the client URL is the live spot URL.
"""
from __future__ import annotations

from typing import Any

DEMO = 'demo'
TESTNET = 'testnet'
ENV_OPTION = 'bytxDemoEnv'          # ccxt options: 'demo' | 'testnet' (markets cache key)

DOCS = (
    'https://developers.binance.com/docs/binance-spot-api-docs/demo-mode/general-info',
    'https://developers.binance.com/docs/derivatives/usds-margined-futures/general-info',
    'https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-api-general-info',
    'https://developers.binance.com/docs/derivatives/coin-margined-futures/general-info',
)

_SPOT_REST = 'https://demo-api.binance.com/api'
_FAPI = 'https://demo-fapi.binance.com/fapi'
_DAPI = 'https://demo-dapi.binance.com/dapi'

DEMO_SPOT_STREAMS = 'wss://demo-stream.binance.com/ws'
DEMO_SPOT_WS_API = 'wss://demo-ws-api.binance.com/ws-api/v3'
DEMO_FUTURES_STREAMS = 'wss://demo-fstream.binance.com/ws'
DEMO_DELIVERY_STREAMS = 'wss://demo-dstream.binance.com/ws'
DEMO_FUTURES_WS_API = 'wss://testnet.binancefuture.com/ws-fapi/v1'     # per WS API docs (see above)
DEMO_DELIVERY_WS_API = 'wss://testnet.binancefuture.com/ws-dapi/v1'

DEMO_API: dict[str, Any] = {
    'public': _SPOT_REST + '/v3',
    'private': _SPOT_REST + '/v3',
    'v1': _SPOT_REST + '/v1',
    'fapiPublic': _FAPI + '/v1',
    'fapiPublicV2': _FAPI + '/v2',
    'fapiPublicV3': _FAPI + '/v3',
    'fapiPrivate': _FAPI + '/v1',
    'fapiPrivateV2': _FAPI + '/v2',
    'fapiPrivateV3': _FAPI + '/v3',
    'dapiPublic': _DAPI + '/v1',
    'dapiPrivate': _DAPI + '/v1',
    'dapiPrivateV2': _DAPI + '/v2',
    'ws': {
        'spot': DEMO_SPOT_STREAMS,
        'margin': DEMO_SPOT_STREAMS,       # no margin (sapi) in Demo Mode; as with ccxt testnet — spot URL
        'future': DEMO_FUTURES_STREAMS,
        'delivery': DEMO_DELIVERY_STREAMS,
        'ws-api': {
            'spot': DEMO_SPOT_WS_API,
            'future': DEMO_FUTURES_WS_API,
            'delivery': DEMO_DELIVERY_WS_API,
        },
    },
}

# demo host → live host (for ccxt get_exceptions_by_url)
_REST_HOSTS = (('https://demo-api.binance.com/', 'https://api.binance.com/'),
               ('https://demo-fapi.binance.com/', 'https://fapi.binance.com/'),
               ('https://demo-dapi.binance.com/', 'https://dapi.binance.com/'))

# Spot streams not on the live host: ccxt doesn't recognize them as spot (see module docstring)
_SPOT_STREAM_PREFIXES = ('wss://demo-stream.binance.com', 'wss://stream.testnet.binance.vision')
_LIVE_SPOT_STREAM = 'wss://stream.binance.com:9443/ws'
_SPOT_URL_HANDLERS = ('handle_order_book', 'handle_trade', 'handle_ohlcv', 'handle_tickers_and_bids_asks')


def enable(exchange: Any) -> None:
    """ccxt binance instance → Demo Mode (before apply_ccxt_fixes: USDⓈ-M WS routes are set there)."""
    exchange.set_sandbox_mode(True)
    exchange.urls['api'] = exchange.deep_extend({}, DEMO_API)
    exchange.options[ENV_OPTION] = DEMO
    _patch_sign(exchange)
    _patch_exceptions_by_url(exchange)


def enable_testnet(exchange: Any) -> None:
    """Legacy testnet (testnet.binance.vision / testnet.binancefuture.com) — ``demo='testnet'``."""
    exchange.set_sandbox_mode(True)
    exchange.options[ENV_OPTION] = TESTNET


def env(exchange: Any) -> Any:
    options = getattr(exchange, 'options', None)
    return options.get(ENV_OPTION) if isinstance(options, dict) else None


def _patch_sign(exchange: Any) -> None:
    from ccxt.base.errors import NotSupported

    original = exchange.sign

    def sign(path, api='public', method='GET', params={}, headers=None, body=None):  # noqa: B006 — ccxt signature
        if isinstance(api, str) and api not in exchange.urls['api']:
            raise NotSupported(
                f'binance Demo Mode: {api} (/{path}) is unavailable — per the Demo Mode docs only REST '
                f'/api (spot), /fapi (USDⓈ-M) and /dapi (COIN-M) exist; no /sapi, /papi, /eapi, /futures/data')
        return original(path, api, method, params, headers, body)

    exchange.sign = sign


def _patch_exceptions_by_url(exchange: Any) -> None:
    original = exchange.get_exceptions_by_url

    def get_exceptions_by_url(url, exactOrBroad):
        if isinstance(url, str):
            for demo_host, live_host in _REST_HOSTS:
                if url.startswith(demo_host):
                    url = live_host + url[len(demo_host):]
                    break
        return original(url, exactOrBroad)

    exchange.get_exceptions_by_url = get_exceptions_by_url


def patch_spot_stream_detection(exchange: Any) -> None:
    """Demo/testnet spot streams: while an event is parsed, the client URL is the live spot URL.

    Handlers are synchronous (asyncio is single-threaded), so other tasks don't see the swap; the
    book snapshot is requested by ccxt in a separate task with the real URL.
    """
    for name in _SPOT_URL_HANDLERS:
        original = getattr(exchange, name, None)
        if original is None:
            continue
        setattr(exchange, name, _with_live_spot_url(original))


def _with_live_spot_url(original: Any) -> Any:
    def handler(client, message, *args):
        url = getattr(client, 'url', None)
        if not (isinstance(url, str) and url.startswith(_SPOT_STREAM_PREFIXES)):
            return original(client, message, *args)
        client.url = _LIVE_SPOT_STREAM
        try:
            return original(client, message, *args)
        finally:
            client.url = url

    return handler
