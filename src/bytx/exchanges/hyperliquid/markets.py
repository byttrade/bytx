"""Hyperliquid: workaround for the ccxt 4.4.65 load_markets crash (``None + '/'``).

Root cause (ccxt 4.4.65 ``hyperliquid.fetch_spot_markets``): a spot pair's token is taken
by POSITION in the ``spotMeta.tokens`` list — ``tokens[universe[i].tokens[0]]`` — while
in the exchange response ``universe[i].tokens`` is the token's ``index`` field. While the list was
dense (``tokens[k].index == k``) these matched. In the live response the list is
sparse (as of 2026-10: 503 tokens with index up to 1022, the first gap at position 458):

- pairs with token index ≥ len(tokens) get ``baseTokenInfo = {}`` → name None →
  ``symbol = None + '/' + quote`` → TypeError, and ``load_markets`` fails ENTIRELY
  (swap markets, tickers, funding — everything that goes through load_markets/fetch_markets);
- pairs with an index inside the list but after a gap silently get SOMEONE ELSE's token —
  wrong base/symbol and szDecimals (amount precision).

The workaround sits on the ccxt hyperliquid instance transport (the exchange.request wrapper, which
publicPostInfo / public_post_info and any direct info requests go through):

- the ``spotMeta`` / ``spotMetaAndAssetCtxs`` response is normalized before ccxt parses it:
  ``tokens`` is reordered into a dense list by the ``index`` field (position ==
  index); pairs whose token isn't found, has no name or no szDecimals are removed from
  ``universe`` (WARNING). Asset ctxs are indexed by ``universe[i].index``
  and are not shifted;
- ``parse_markets`` skips a record on which ``parse_market`` fails (a perp
  record without name/szDecimals) instead of failing the whole load_markets (WARNING);
  ``fetch_currencies`` drops a currency without a code (the same record without name).

A response that needs no fixing passes unchanged. Idempotent.
"""
from typing import Any, Optional

from loguru import logger

_SPOT_META_TYPES = frozenset({'spotMeta', 'spotMetaAndAssetCtxs'})
# Guard against an abnormal index (the dense list is built up to max index).
_MAX_TOKEN_INDEX = 1_000_000


def _int(value) -> Optional[int]:
    """Integer from an int or a digit string: without orjson ccxt parses JSON with quoteJsonNumbers and
    indices arrive as strings ("0")."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _token_ok(token) -> bool:
    if not isinstance(token, dict):
        return False
    name = token.get('name')
    sz = token.get('szDecimals')
    spec = token.get('spec')
    if isinstance(spec, dict) and spec.get('szDecimals') is not None:
        sz = spec.get('szDecimals')
    if not isinstance(name, str) or not name:
        return False
    try:
        int(str(sz))
    except (TypeError, ValueError):
        return False
    return True


def normalize_spot_meta(meta: dict) -> dict:
    """spotMeta → a copy with dense tokens (position == index) and only parseable pairs."""
    tokens = meta.get('tokens')
    universe = meta.get('universe')
    if not isinstance(tokens, list) or not isinstance(universe, list):
        return meta
    by_index: dict = {}
    for pos, token in enumerate(tokens):
        if not isinstance(token, dict):
            continue
        idx = _int(token.get('index', pos))
        if idx is not None and 0 <= idx <= _MAX_TOKEN_INDEX and idx not in by_index:
            by_index[idx] = token
    dense_ok = all(isinstance(t, dict) and _int(t.get('index', i)) == i for i, t in enumerate(tokens))
    kept, dropped = [], []
    for pair in universe:
        refs = pair.get('tokens') if isinstance(pair, dict) else None
        good = (isinstance(refs, list) and len(refs) >= 2 and all(_int(r) is not None for r in refs[:2])
                and all(_token_ok(by_index.get(_int(r))) for r in refs[:2])
                and _int(pair.get('index')) is not None)
        (kept if good else dropped).append(pair)
    if dense_ok and not dropped:
        return meta
    if dropped:
        names = [p.get('name') if isinstance(p, dict) else p for p in dropped[:5]]
        logger.warning(
            f"hyperliquid spotMeta: skipped {len(dropped)} spot pairs with unresolvable tokens "
            f"(e.g. {names}) — the other markets are loaded"
        )
    dense = [by_index.get(i, {}) for i in range(max(by_index) + 1)] if by_index else []
    fixed = dict(meta)
    fixed['tokens'] = dense
    fixed['universe'] = kept
    return fixed


def normalize_spot_meta_response(response: Any) -> Any:
    """info type=spotMeta response (dict) or spotMetaAndAssetCtxs ([meta, ctxs])."""
    if isinstance(response, dict):
        return normalize_spot_meta(response)
    if isinstance(response, list) and response and isinstance(response[0], dict):
        fixed = normalize_spot_meta(response[0])
        if fixed is not response[0]:
            return [fixed, *response[1:]]
    return response


def patch_hyperliquid_markets(exchange) -> None:
    """Installs the workaround on a ccxt hyperliquid instance (other exchanges untouched). Idempotent."""
    if exchange is None or str(getattr(exchange, 'id', '') or '').lower() != 'hyperliquid':
        return
    if getattr(exchange, '_bytx_hl_markets_patched', False):
        return
    original_request = exchange.request

    async def request(path, api='public', method='GET', params={}, *rest, **kw):
        response = await original_request(path, api, method, params, *rest, **kw)
        kind = params.get('type') if isinstance(params, dict) else None
        if path == 'info' and kind in _SPOT_META_TYPES:
            response = normalize_spot_meta_response(response)
        return response

    def parse_markets(markets):
        result = []
        for market in markets or []:
            try:
                parsed = exchange.parse_market(market)
            except (TypeError, ValueError, KeyError, IndexError) as e:
                name = market.get('name') if isinstance(market, dict) else None
                logger.warning(f"hyperliquid: market {name!r} skipped — ccxt couldn't parse the record: {e}")
                continue
            result.append(parsed)
        return result

    original_fetch_currencies = exchange.fetch_currencies

    async def fetch_currencies(params={}):
        # a perp record without name would yield a currency with code None → set_markets fails on sorting
        result = await original_fetch_currencies(params)
        if isinstance(result, dict) and any(not isinstance(code, str) or not code for code in result):
            logger.warning("hyperliquid: currencies without a code skipped (meta.universe record without name)")
            result = {code: cur for code, cur in result.items() if isinstance(code, str) and code}
        return result

    exchange.request = request
    exchange.parse_markets = parse_markets
    exchange.fetch_currencies = fetch_currencies
    exchange._bytx_hl_markets_patched = True


__all__ = ['patch_hyperliquid_markets', 'normalize_spot_meta', 'normalize_spot_meta_response']
