"""ccxt 4.4.65 fixes for Hyperliquid, based on official HL docs.

- WS ticker: ccxt subscribes to ``webData2`` with the zero address — HL answers «Error
  parsing JSON into valid websocket request» (live check 2026-10-25). Per the docs
  (websocket/subscriptions) public ticker data is ``activeAssetCtx`` (mark, oracle,
  funding, OI, daily volume) and ``bbo`` (best bid/ask with block time).
- ccxt has no ``watch_bids_asks`` — built on ``bbo``.
- REST ticker (metaAndAssetCtxs): ccxt puts ``impactPxs`` into bid/ask — these are execution prices
  for an «impact» notional, not the best book prices (spot has none at all), and there is no time.
  Per docs the best prices come only from ``l2Book`` (info type=l2Book: levels and time): bid/ask and
  time are taken from there, impactPxs stay in info.
- Balance: in the unifiedAccount / portfolioMargin account modes (info type=userAbstraction) the docs
  «Retrieve user's perpetuals account summary» say to take the trading balance from spot
  (spotClearinghouseState) — clearinghouseState is 0 there with money in the account (testnet 2026-10-28).
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from loguru import logger

_APPLIED = '_bytx_hl_fixes'


def _coin(ex: Any, market: dict) -> str:
    # as in ccxt watch_order_book: perp — base, spot — id (@<index> / PURR/USDC)
    return market['base'] if market.get('swap') else market['id']


def _level(level: Any) -> tuple:
    if not isinstance(level, dict):
        return None, None
    return _num(level.get('px')), _num(level.get('sz'))


def _num(x: Any) -> Any:
    try:
        return None if x in (None, '') else float(x)
    except (TypeError, ValueError):
        return None


def _patch_rest_ticker(ex: Any) -> None:
    original = ex.fetch_ticker

    async def fetch_ticker(symbol, params={}):  # noqa: B006 — ccxt signature
        ticker = await original(symbol, params)
        market = ex.market(ticker.get('symbol') or symbol)
        book = await ex.publicPostInfo({'type': 'l2Book', 'coin': _coin(ex, market)})
        levels = (book or {}).get('levels') or [[], []]
        bids, asks = (list(levels) + [[], []])[:2]
        bid, bid_size = _level(bids[0] if bids else None)
        ask, ask_size = _level(asks[0] if asks else None)
        ticker.update({'bid': bid, 'bidVolume': bid_size, 'ask': ask, 'askVolume': ask_size})
        if (book or {}).get('time') is not None:
            ticker['timestamp'] = int(book['time'])
            ticker['datetime'] = ex.iso8601(ticker['timestamp'])
        return ticker

    ex.fetch_ticker = fetch_ticker


def _patch_ws(ex: Any) -> None:
    if not hasattr(ex, 'handle_message'):
        return  # REST instance
    state: dict = {}          # symbol -> {'ctx': {...}, 'bbo': {...}}
    coins: dict = {}          # coin -> symbol
    keep: set = set()         # references to background bbo subscriptions
    served: set = set()       # symbols watch_bids_asks has already served a snapshot to
    original_handle = ex.handle_message

    def url() -> str:
        return ex.urls['api']['ws']['public']

    def subscribe(kind: str, coin: str, symbol: str, message_hash: str) -> Any:
        request = {'method': 'subscribe', 'subscription': {'type': kind, 'coin': coin}}
        return ex.watch(url(), message_hash, request, f'{kind}:{coin}')

    def ticker_of(symbol: str) -> dict:
        st = state.get(symbol, {})
        ctx, bbo = st.get('ctx') or {}, st.get('bbo') or {}
        mark, mid, prev = _num(ctx.get('markPx')), _num(ctx.get('midPx')), _num(ctx.get('prevDayPx'))
        last = mid if mid is not None else mark
        change = None if last is None or not prev else last - prev
        return ex.safe_ticker({
            'symbol': symbol,
            'timestamp': bbo.get('time'),
            'datetime': ex.iso8601(bbo.get('time')) if bbo.get('time') else None,
            'bid': bbo.get('bid'), 'bidVolume': bbo.get('bidVolume'),
            'ask': bbo.get('ask'), 'askVolume': bbo.get('askVolume'),
            'last': last, 'close': last, 'previousClose': prev,
            'change': change, 'percentage': None if change is None else change / prev * 100,
            'markPrice': mark, 'indexPrice': _num(ctx.get('oraclePx')),
            'quoteVolume': _num(ctx.get('dayNtlVlm')),
            'info': {'ctx': ctx, 'bbo': bbo.get('raw')},
        })

    async def seed_bbo(symbol: str, coin: str) -> dict:
        """Best-price snapshot from REST l2Book: per docs bbo is sent only on change — on a
        quiet market without a snapshot the ticker/best prices would never arrive."""
        st = state.setdefault(symbol, {})
        if st.get('bbo') is None:
            book = await ex.publicPostInfo({'type': 'l2Book', 'coin': coin})
            levels = list((book or {}).get('levels') or []) + [[], []]
            bid, bid_size = _level(levels[0][0] if levels[0] else None)
            ask, ask_size = _level(levels[1][0] if levels[1] else None)
            st['bbo'] = {'bid': bid, 'bidVolume': bid_size, 'ask': ask, 'askVolume': ask_size,
                         'time': (book or {}).get('time'), 'raw': book}
        return st

    async def watch_ticker(symbol, params={}):  # noqa: B006 — ccxt signature
        await ex.load_markets()
        market = ex.market(symbol)
        symbol, coin = market['symbol'], _coin(ex, market)
        coins[coin] = symbol
        await seed_bbo(symbol, coin)
        bbo_task = asyncio.ensure_future(subscribe('bbo', coin, symbol, f'hlbbo:{symbol}'))
        keep.add(bbo_task)
        bbo_task.add_done_callback(lambda f: (keep.discard(f), f.cancelled() or f.exception()))
        return await subscribe('activeAssetCtx', coin, symbol, f'ticker:{symbol}')

    async def watch_bids_asks(symbols=None, params={}):  # noqa: B006 — ccxt signature
        await ex.load_markets()
        if not symbols:
            raise ex.ArgumentsRequired(ex.id + ' watchBidsAsks() requires symbols') \
                if hasattr(ex, 'ArgumentsRequired') else ValueError('symbols are required')
        market = ex.market(symbols[0])
        symbol, coin = market['symbol'], _coin(ex, market)
        coins[coin] = symbol
        first = symbol not in served
        served.add(symbol)
        await seed_bbo(symbol, coin)
        future = subscribe('bbo', coin, symbol, f'bidsasks:{symbol}')
        if first:  # first call — snapshot right away, then changes from WS
            keep.add(future := asyncio.ensure_future(future))
            future.add_done_callback(lambda f: (keep.discard(f), f.cancelled() or f.exception()))
            return {symbol: ticker_of(symbol)}
        ticker = await future
        return {symbol: ticker}

    def handle_message(client, message):
        channel = message.get('channel') if isinstance(message, dict) else None
        if channel in ('bbo', 'activeAssetCtx', 'activeSpotAssetCtx'):
            data = message.get('data') or {}
            symbol = coins.get(data.get('coin'))
            if symbol is None:
                return None
            st = state.setdefault(symbol, {})
            if channel == 'bbo':
                levels = (data.get('bbo') or [None, None]) + [None, None]
                bid, bid_size = _level(levels[0])
                ask, ask_size = _level(levels[1])
                st['bbo'] = {'bid': bid, 'bidVolume': bid_size, 'ask': ask, 'askVolume': ask_size,
                             'time': data.get('time'), 'raw': data}
                ticker = ticker_of(symbol)
                client.resolve(ticker, f'bidsasks:{symbol}')
                if st.get('ctx') is not None:
                    client.resolve(ticker, f'ticker:{symbol}')
            else:
                st['ctx'] = data.get('ctx') or {}
                if st.get('bbo') is not None:
                    client.resolve(ticker_of(symbol), f'ticker:{symbol}')
            return None
        return original_handle(client, message)

    ex.watch_ticker = watch_ticker
    ex.watch_bids_asks = watch_bids_asks
    ex.handle_message = handle_message
    ex.has['watchBidsAsks'] = True


#: HL account modes where perp collateral is the spot balance (Info endpoint docs, userAbstraction)
SPOT_COLLATERAL_MODES = ('unifiedAccount', 'portfolioMargin')
ABSTRACTION_TTL_SEC = 300.0


async def spot_collateral(ex: Any) -> bool:
    """True — perp collateral is spot (unifiedAccount / portfolioMargin); mode cached per instance.

    Mode read failure — False (the old perp balance): zero on a unified account blocks entry
    (fail-closed) rather than opening a position beyond available funds.
    """
    now = time.monotonic()
    cache = getattr(ex, '_bytx_hl_abstraction', None)
    if cache is not None and now - cache[1] < ABSTRACTION_TTL_SEC:
        return cache[0] in SPOT_COLLATERAL_MODES
    user = getattr(ex, 'walletAddress', None)
    if not user:
        return False
    try:
        mode = await ex.publicPostInfo({'type': 'userAbstraction', 'user': user})
    except Exception:
        return False
    # the response is a bare JSON string; ccxt 4.4.65 returns it with quotes ('"unifiedAccount"')
    mode = str(mode).strip().strip('"')
    ex._bytx_hl_abstraction = (mode, now)
    return mode in SPOT_COLLATERAL_MODES


async def resolve_master_address(ex: Any) -> None:
    """walletAddress — the main account address when the key is an API wallet (agent).

    Docs «Nonces and API wallets»: the agent signs actions on behalf of the main account, while
    info requests (balance, positions, orders, trades) must use the main account address —
    they are empty for the agent address. ccxt takes ``user`` from walletAddress (handle_public_address),
    and the signature from privateKey, so swapping the address doesn't change the signature. testnet 2026-10-29:
    the config had the agent address, balance and positions read as zero (the main one had 1006.9 USDC).
    userRole failure — address as is (previous behavior), retried on the next connect.
    """
    if getattr(ex, '_bytx_hl_master_resolved', False):
        return
    wallet = getattr(ex, 'walletAddress', None)
    if not wallet:
        return
    try:
        role = await ex.publicPostInfo({'type': 'userRole', 'user': wallet})
    except Exception as e:  # noqa: BLE001
        logger.warning(f'hyperliquid: userRole not read ({type(e).__name__}), wallet address as given')
        return
    ex._bytx_hl_master_resolved = True
    if isinstance(role, dict) and role.get('role') == 'agent':
        master = (role.get('data') or {}).get('user')
        if master and str(master).lower() != str(wallet).lower():
            ex._bytx_hl_agent_address = wallet
            ex.walletAddress = master
            ex._bytx_hl_abstraction = None      # account mode — by the main address
            logger.info('hyperliquid: API wallet (agent) — info requests use the master account address')


def _patch_balance(ex: Any) -> None:
    """fetch_balance without an explicit type: on unified / portfolio margin — the spot balance (USDC: total − hold)."""
    original = ex.fetch_balance

    async def fetch_balance(params: Any = None) -> Any:
        params = dict(params or {})
        if 'type' not in params and await spot_collateral(ex):
            params['type'] = 'spot'
        return await original(params)

    ex.fetch_balance = fetch_balance


_CHS_CHANNEL = 'clearinghouseState'
_POSITIONS_HASH = 'positions'


def _ws_positions(exchange: Any, message: dict, previous: dict) -> list:
    """WS clearinghouseState → ccxt positions (the same assetPositions as REST; testnet 2026-10-29).

    HL simply drops a closed position from the list — for symbols present in the previous
    message a zero position with the previous side is emitted (the consumer sees the close).
    """
    data = exchange.safe_dict(message, 'data', {})
    state = exchange.safe_dict(data, _CHS_CHANNEL, data)
    result = [exchange.parse_position(p) for p in exchange.safe_list(state, 'assetPositions', []) or []]
    current = {p['symbol']: p['side'] for p in result if p.get('contracts')}
    for symbol, side in previous.items():
        if symbol not in current:
            result.append(exchange.safe_position({'info': {}, 'symbol': symbol, 'side': side, 'contracts': 0.0,
                                                  'hedged': None}))
    previous.clear()
    previous.update(current)
    return result


def _add_watch_positions(exchange: Any) -> None:
    """watchPositions via the clearinghouseState subscription (missing in ccxt 4.4.65).

    Address — the main wallet: user subscriptions for an API agent address are empty (WS
    subscriptions docs); the agent is determined once via info userRole (weight 60). HL limit —
    10 unique users per IP across all user subscriptions.
    """
    if not hasattr(exchange, 'handle_order'):
        return  # REST instance without ccxt.pro
    previous: dict = {}
    state: dict = {}
    original = exchange.handle_message

    def handle_message(client, message):
        if isinstance(message, dict) and message.get('channel') == _CHS_CHANNEL:
            client.resolve(_ws_positions(exchange, message, previous), _POSITIONS_HASH)
            return None
        return original(client, message)

    exchange.handle_message = handle_message

    async def _master_address() -> str:
        if 'user' not in state:
            wallet = exchange.walletAddress
            role = await exchange.publicPostInfo({'type': 'userRole', 'user': wallet})
            master = (role.get('data') or {}).get('user') if isinstance(role, dict) and role.get('role') == 'agent' else None
            state['user'] = master or wallet
        return state['user']

    async def watch_positions(symbols=None, since=None, limit=None, params={}):
        await exchange.load_markets()
        user = await _master_address()
        url = exchange.urls['api']['ws']['public']
        request = {'method': 'subscribe', 'subscription': {'type': _CHS_CHANNEL, 'user': user}}
        positions = await exchange.watch(url, _POSITIONS_HASH, request, _CHS_CHANNEL + ':' + user)
        if symbols:
            wanted = set(symbols)
            positions = [p for p in positions if p.get('symbol') in wanted]
        return positions

    exchange.watch_positions = watch_positions
    exchange.has = dict(exchange.has, watchPositions=True)


def apply(exchange: Any) -> None:
    """ccxt hyperliquid instance patches (repeated call is a no-op)."""
    if getattr(exchange, _APPLIED, False):
        return
    setattr(exchange, _APPLIED, True)
    _patch_rest_ticker(exchange)
    _patch_ws(exchange)
    _add_watch_positions(exchange)
    _patch_balance(exchange)
