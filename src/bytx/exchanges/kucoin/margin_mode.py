"""KuCoin Futures: symbol margin mode — Get Margin Mode / Switch Margin Mode.

Docs (spec github.com/Kucoin/kucoin-universal-sdk ``openapi-futures-positions.json``, pages www.kucoin.com/docs-new):

- GET /api/v2/position/getMarginMode (https://www.kucoin.com/docs-new/api-3470259): «This interface can query the
  margin mode of the current symbol.» Parameter ``symbol`` (query, required); response ``data.marginMode`` — «Margin
  mode: ISOLATED (isolated), CROSS (cross margin).»
- POST /api/v2/position/changeMarginMode (https://www.kucoin.com/docs-new/api-3470262): «This interface can modify the
  margin mode of the current symbol.» Body ``symbol`` and ``marginMode`` (both required, enum ISOLATED | CROSS);
  response — ``data`` {symbol, marginMode} with the new mode.

The mode belongs to the symbol and is also sent with every order (Add Order ``marginMode``, «default: ISOLATED»): an
order whose mode differs from the symbol's is rejected (330005 «Please switch your margin mode to cross margin mode and
try again», table https://www.kucoin.com/docs-new/error-code/futures). So :meth:`Exchange.set_margin_mode` switches
the symbol's mode on the exchange instead of only remembering it. A symbol already in the requested mode gets no
switch request (the endpoint docs don't state switch conditions with a position or orders). ccxt 4.4.65
``set_margin_mode`` sends the same request but neither checks the mode in the response nor reads the current one —
requests follow the docs via ``ccxt.request`` (instance signature, version v2 as in the docs).
"""
from __future__ import annotations

from typing import Any, Optional

from bytx.errors import ExchangeError

DOCS_GET = 'https://www.kucoin.com/docs-new/api-3470259'
DOCS_SWITCH = 'https://www.kucoin.com/docs-new/api-3470262'
GET_PATH = 'position/getMarginMode'
SWITCH_PATH = 'position/changeMarginMode'
# SDK mode → docs enum and back
WIRE = {'cross': 'CROSS', 'isolated': 'ISOLATED'}
MODES = {v: k for k, v in WIRE.items()}


def parse(response: Any) -> Optional[str]:
    """'cross' | 'isolated' from ``data.marginMode``; None — no field or a value outside the docs."""
    data = (response or {}).get('data') if isinstance(response, dict) else None
    mode = data.get('marginMode') if isinstance(data, dict) else None
    return MODES.get(str(mode).upper()) if mode is not None else None


async def read(ex: Any, s: str) -> Optional[str]:
    """The symbol's margin mode on the exchange (Get Margin Mode); None — no documented mode in the response."""
    market_id = ex.ccxt.market(s)['id']
    raw = await ex._call(ex.ccxt.request, GET_PATH, 'futuresPrivate', 'GET', {'symbol': market_id, 'version': 'v2'})
    return parse(raw)


async def switch(ex: Any, s: str, mode: str) -> bool:
    """Puts the symbol in ``mode`` on the exchange. True — switched, False — already in this mode (no switch request).
    An exchange rejection raises (ccxt → SDK errors); a response without the requested mode — ExchangeError."""
    if await read(ex, s) == mode:
        return False
    market_id = ex.ccxt.market(s)['id']
    raw = await ex._call(ex.ccxt.request, SWITCH_PATH, 'futuresPrivate', 'POST',
                         {'symbol': market_id, 'marginMode': WIRE[mode], 'version': 'v2'})
    if parse(raw) != mode:
        raise ExchangeError(f'{s} margin mode not confirmed: {SWITCH_PATH} → {raw!r}', exchange=ex.name,
                            details={'symbol': s, 'margin_mode': mode, 'response': raw, 'docs': DOCS_SWITCH})
    return True
