"""Offline exchange emulator: a regular ``Exchange`` for every supported exchange, with ccxt instance methods stubbed.

    ex, venue = fake_exchange('bybit', price=50_000)
    order = await ex.buy('BTC/USDT:USDT', size=0.01)          # filled immediately at venue.price
    await ex.set_tp_sl('BTC/USDT:USDT', stop_loss=49_000)     # same SL path as the SDK uses on the exchange
    assert venue.position('BTC/USDT:USDT') == 0.01

Markets are :data:`MARKETS_DIR` snapshots (ccxt market dicts built from verbatim exchange doc examples:
contractSize, tick, minimums), so rounding, contracts and minimums match the exchange. State:
position per symbol and orders by id: market fills immediately at ``price`` (``fill_ratio`` is the fill
share), reduce-only closes the position, conditional orders (TP/SL) wait. Each exchange's SL path matches
the SDK: Binance via Algo API (``ccxt.request('algoOrder')``), Bybit via trading-stop, OKX via ``order-algo``,
BingX / KuCoin / Hyperliquid via ``create_order`` with a trigger, Bitget via ``place-tpsl-order``, Gate via ``price_orders``,
MEXC via ``stoporder/place`` by positionId, HTX via ``swap_cross_tpsl_order``. ``fail_stop`` / ``fail_close``: exchange
rejection; ``calls``: all calls in order.

Network is cut: any non-emulated ccxt request → :class:`NetworkDisabled` (a test cannot hit the
exchange by accident). WS streams are not emulated; use :func:`bytx.testing.paper_exchange` for them.
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import ccxt

MARKETS_DIR = Path(__file__).resolve().parent / 'markets'
#: SDK name → ccxt id
CCXT_IDS = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okx', 'bingx': 'bingx', 'kucoin': 'kucoinfutures',
            'hyperliquid': 'hyperliquid', 'bitget': 'bitget', 'gate': 'gate', 'mexc': 'mexc', 'htx': 'htx'}
NAMES = {v: k for k, v in CCXT_IDS.items()}
#: default symbol in snapshots
SYMBOLS = {k: ('BTC/USDC:USDC' if k == 'hyperliquid' else 'BTC/USDT:USDT') for k in CCXT_IDS}
#: dummy keys (signatures are computed, but requests are never sent)
CREDS = {'api_key': 'test-key', 'secret': 'test-secret', 'password': 'test-pass'}
HL_CREDS = {'wallet_address': '0x19E7E376E7C213B7E7e7e46cc70A5dD086DAff2A', 'private_key': '0x' + '11' * 32}
_TRIGGER_KEYS = ('stopLossPrice', 'takeProfitPrice', 'triggerPrice', 'stopPrice')


class NetworkDisabled(RuntimeError):
    """Emulator: an exchange request that is not emulated (no network in tests)."""


def ccxt_id(name: str) -> str:
    """SDK name ('kucoin') or ccxt id ('kucoinfutures') → ccxt id."""
    key = name.lower()
    if key in NAMES:
        return key
    if key not in CCXT_IDS:
        raise ValueError(f'exchange {name!r}: must be one of {sorted(CCXT_IDS)}')
    return CCXT_IDS[key]


@lru_cache(maxsize=None)
def _snapshot(cid: str) -> str:
    return (MARKETS_DIR / f'{cid}.json').read_text(encoding='utf-8')


def markets(name: str) -> list[dict]:
    """Exchange market snapshot (ccxt market dicts), a fresh copy on every call."""
    return list(json.loads(_snapshot(ccxt_id(name)))['markets'])


def offline(ex: Any) -> Any:
    """Cut network access for an ``Exchange``'s ccxt instance: a non-emulated request → :class:`NetworkDisabled`."""
    c = ex.ccxt

    async def no_network(url: str, method: str = 'GET', headers: Any = None, body: Any = None) -> Any:
        raise NetworkDisabled(f'bytx.testing: exchange request not emulated: {method} {url}')

    c.fetch = no_network
    c.enableRateLimit = False
    return ex


def prepare(ex: Any, name: str, symbols: Optional[list[str]] = None) -> Any:
    """Markets from the snapshot, without network or connecting to the exchange (as after ``await ex.connect()``)."""
    from bytx.markets import clear_cache
    clear_cache()
    rows = markets(name)
    if symbols:
        rows = [m for m in rows if m['symbol'] in symbols]
    ex.ccxt.set_markets(rows)
    ex._connected = True
    return offline(ex)


class FakeVenue:
    """A minimal exchange on top of the instance's ccxt methods (see module)."""

    def __init__(self, ex: Any, price: float = 50_000.0, *, free: float = 1_000_000.0,
                 fail_stop: bool = False) -> None:
        self.ex = ex
        self.price = price
        self.free = free
        self.fail_stop = fail_stop
        self.fail_close = False
        self.fill_ratio = 1.0                    # fill share of a market entry (rest canceled by the exchange)
        self.pos: dict[str, float] = {}         # symbol → signed contracts
        self.orders: dict[str, dict] = {}
        self.calls: list[tuple] = []
        self.n = 0
        c = ex.ccxt
        c.fetch_ticker = self.fetch_ticker
        c.fetch_order_book = self.fetch_order_book
        c.fetch_positions = self.fetch_positions
        c.create_order = self.create_order
        c.fetch_order = self.fetch_order
        c.fetch_open_orders = self.fetch_open_orders
        c.cancel_order = self.cancel_order
        c.set_leverage = self.set_leverage
        c.fetch_balance = self.fetch_balance
        c.request = self.request
        c.privatePostV5PositionTradingStop = self.bybit_trading_stop
        c.private_post_trade_order_algo = self.okx_order_algo
        self.plans: dict[str, dict] = {}        # Bitget (plan) and Gate (price_orders) TP/SL in exchange format
        if c.id == 'bitget':
            c.privateMixPostV2MixOrderPlaceTpslOrder = self.bitget_place_tpsl
            c.privateMixGetV2MixOrderOrdersPlanPending = self.bitget_plan_pending
            c.privateMixPostV2MixOrderCancelPlanOrder = self.bitget_cancel_plan
        elif c.id == 'gate':
            c.privateFuturesPostSettlePriceOrders = self.gate_price_order
            c.privateFuturesGetSettlePriceOrders = self.gate_price_orders
            c.privateFuturesDeleteSettlePriceOrdersOrderId = self.gate_cancel_price_order
        elif c.id == 'mexc':
            c.contractPrivateGetPositionOpenPositions = self.mexc_open_positions
            c.contractPrivatePostStoporderCancel = self.mexc_cancel_stop
        elif c.id == 'htx':
            for margin in ('cross_', ''):
                setattr(c, f'contract_private_post_linear_swap_api_v1_swap_{margin}tpsl_order', self.htx_tpsl_order)
                setattr(c, f'contract_private_post_linear_swap_api_v1_swap_{margin}tpsl_openorders',
                        self.htx_tpsl_openorders)
                setattr(c, f'contract_private_post_linear_swap_api_v1_swap_{margin}tpsl_cancel', self.htx_tpsl_cancel)

    # ─── state for tests ───────────────────────────────────────────────────
    def position(self, symbol: str) -> float:
        """Symbol position in the base coin (contracts × contractSize), signed."""
        cs = self.ex.ccxt.market(symbol).get('contractSize') or 1.0
        return round(self.pos.get(symbol, 0.0) * cs, 12)

    def open_orders(self) -> list[dict]:
        return [dict(o) for o in self.orders.values() if o['status'] == 'open']

    # ─── market and account ────────────────────────────────────────────────
    async def fetch_ticker(self, symbol: str, params: Any = None) -> dict:
        return {'symbol': symbol, 'bid': self.price, 'ask': self.price, 'last': self.price, 'info': {}}

    async def fetch_order_book(self, symbol: str, limit: Any = None, params: Any = None) -> dict:
        return {'symbol': symbol, 'bids': [[self.price, 100.0]], 'asks': [[self.price, 100.0]],
                'timestamp': 1, 'nonce': 1}

    async def fetch_balance(self, params: Any = None) -> dict:
        self.calls.append(('fetch_balance', params))
        cur = {c: {'free': self.free, 'used': 0.0, 'total': self.free} for c in ('USDT', 'USDC')}
        return {**cur, 'free': {c: self.free for c in cur}, 'used': {c: 0.0 for c in cur},
                'total': {c: self.free for c in cur}, 'info': {}}

    async def set_leverage(self, leverage: Any, symbol: Any = None, params: Any = None) -> dict:
        self.calls.append(('set_leverage', leverage, symbol, dict(params or {})))
        return {}

    async def fetch_positions(self, symbols: Any = None, params: Any = None) -> list[dict]:
        out = []
        for s, c in self.pos.items():
            if c:
                out.append({'symbol': s, 'side': 'long' if c > 0 else 'short', 'contracts': abs(c),
                            'contractSize': self.ex.ccxt.market(s).get('contractSize'), 'entryPrice': self.price,
                            'info': {}})
        return out

    # ─── orders ────────────────────────────────────────────────────────────
    def _id(self) -> str:
        self.n += 1
        return str(self.n)

    def _row(self, oid: str, symbol: str, type: str, side: str, amount: float, price: Any, status: str,
             params: dict) -> dict:
        cid = (params.get('clientOrderId') or params.get('clOrdId') or params.get('orderLinkId')
               or params.get('clientOid') or params.get('newClientOrderId'))
        filled = amount if status == 'closed' else 0.0
        trig = next((params[k] for k in _TRIGGER_KEYS if params.get(k) is not None), None)
        return {'id': oid, 'clientOrderId': cid, 'symbol': symbol, 'side': side, 'type': type, 'status': status,
                'price': price, 'average': self.price if status == 'closed' else None, 'amount': amount,
                'filled': filled, 'remaining': amount - filled, 'reduceOnly': bool(params.get('reduceOnly')),
                'triggerPrice': trig, 'info': {}}

    async def create_order(self, symbol: str, type: str, side: str, amount: float, price: Any = None,
                           params: Any = None) -> dict:
        params = dict(params or {})
        self.calls.append(('create_order', symbol, type, side, amount, price, params))
        oid = self._id()
        trigger = any(params.get(k) is not None for k in _TRIGGER_KEYS)
        if trigger:
            # SL: HL / KuCoin use stopLossPrice, BingX uses type STOP_MARKET
            if self.fail_stop and (params.get('stopLossPrice') is not None or type == 'STOP_MARKET'):
                raise ccxt.InvalidOrder(f'{self.ex.ccxt.id} stop rejected')
            row = self._row(oid, symbol, type, side, amount, price, 'open', params)
        else:
            if params.get('reduceOnly') and self.fail_close:
                raise ccxt.InvalidOrder(f'{self.ex.ccxt.id} close rejected')
            sign = 1 if side == 'buy' else -1
            if params.get('reduceOnly'):
                cur = self.pos.get(symbol, 0.0)
                amount = min(amount, abs(cur))
            done = amount if params.get('reduceOnly') else round(amount * self.fill_ratio, 12)
            self.pos[symbol] = round(self.pos.get(symbol, 0.0) + sign * done, 12)
            row = self._row(oid, symbol, type, side, amount, price, 'closed', params)
            if done < amount:   # partial fill: the rest of the market entry is canceled by the exchange
                row.update(status='canceled', filled=done, remaining=amount - done)
        self.orders[oid] = row
        return dict(row)

    async def fetch_order(self, oid: str, symbol: Any = None, params: Any = None) -> dict:
        if oid not in self.orders:
            raise ccxt.OrderNotFound(oid)
        return dict(self.orders[oid])

    async def fetch_open_orders(self, symbol: Any = None, since: Any = None, limit: Any = None,
                                params: Any = None) -> list[dict]:
        return self.open_orders()

    async def cancel_order(self, oid: str, symbol: Any = None, params: Any = None) -> dict:
        self.calls.append(('cancel_order', oid))
        if oid not in self.orders:
            raise ccxt.OrderNotFound(oid)
        o = self.orders[oid]
        o['status'] = 'canceled'
        return dict(o)

    # ─── SL via exchange paths ─────────────────────────────────────────────
    async def request(self, path: str, api: Any = 'public', method: str = 'GET', params: Any = None,
                      *args: Any, **kwargs: Any) -> Any:
        params = dict(params or {})
        self.calls.append(('request', path, method, params))
        if path == 'algoOrder' and method == 'POST':
            if self.fail_stop and params.get('type') == 'STOP_MARKET':
                raise ccxt.InvalidOrder('binance {"code":-2021,"msg":"Order would immediately trigger."}')
            oid = self._id()
            row = {'algoId': int(oid), 'clientAlgoId': params.get('clientAlgoId'), 'algoStatus': 'NEW',
                   'orderType': params['type'], 'side': params['side'], 'symbol': params['symbol'],
                   'quantity': params.get('quantity'), 'triggerPrice': params['triggerPrice']}
            self.orders[oid] = {**row, 'status': 'open'}
            return row
        if path == 'algoOrder' and method == 'DELETE':
            return {'algoId': params.get('algoId'), 'code': '200'}
        if path == 'position/getPositionMode':
            # KuCoin Get Position Mode: the emulator is one-way ("0 -- One-Way Mode")
            return {'code': '200000', 'data': {'positionMode': 0}}
        if path == 'stoporder/place':
            return self.mexc_place_stop(params)
        if path == 'stoporder/open_orders':
            return {'success': True, 'code': 0, 'data': [
                {k: v for k, v in p.items() if k != '_active'} for p in self.plans.values()
                if p.get('_active') and (not params.get('symbol') or p.get('symbol') == params['symbol'])]}
        raise NetworkDisabled(f'bytx.testing: request not emulated: {method} {path}')

    async def bybit_trading_stop(self, req: dict) -> dict:
        self.calls.append(('trading_stop', dict(req)))
        if self.fail_stop and req.get('stopLoss'):
            raise ccxt.InvalidOrder('bybit {"retCode":10001,"retMsg":"StopLoss invalid"}')
        return {'retCode': 0, 'retMsg': 'OK', 'result': {}, 'time': 1}

    async def okx_order_algo(self, req: dict) -> dict:
        self.calls.append(('order_algo', dict(req)))
        if self.fail_stop and req.get('slTriggerPx'):
            raise ccxt.InvalidOrder('okx {"code":"51000","msg":"Parameter slTriggerPx error"}')
        oid = self._id()
        return {'code': '0', 'msg': '', 'data': [{'algoId': oid, 'algoClOrdId': req.get('algoClOrdId', ''),
                                                  'sCode': '0', 'sMsg': ''}]}

    async def bitget_place_tpsl(self, req: dict) -> dict:
        self.calls.append(('place_tpsl', dict(req)))
        if self.fail_stop and req.get('planType') in ('pos_loss', 'loss_plan'):
            raise ccxt.InvalidOrder('bitget {"code":"43023","msg":"Insufficient position, can not set profit or stop '
                                    'loss","requestTime":1,"data":null}')
        oid = self._id()
        hold = str(req.get('holdSide') or '')
        self.plans[oid] = {'orderId': oid, 'clientOid': req.get('clientOid', ''), 'planType': req['planType'],
                           'symbol': req['symbol'], 'size': req.get('size', ''), 'triggerPrice': req['triggerPrice'],
                           'triggerType': req.get('triggerType'), 'planStatus': 'not_trigger',
                           'posSide': hold if hold in ('long', 'short') else 'net',
                           # one-way (demo 2026-10-27): holdSide buy → side sell, tradeSide sell_single —
                           # side of the closing order
                           'side': {'long': 'buy', 'short': 'sell', 'buy': 'sell', 'sell': 'buy'}.get(hold, hold),
                           **({'tradeSide': f"{'sell' if hold == 'buy' else 'buy'}_single"}
                              if hold in ('buy', 'sell') else {}),
                           'cTime': '1'}
        return {'code': '00000', 'msg': 'success', 'requestTime': 1,
                'data': {'orderId': oid, 'clientOid': req.get('clientOid', '')}}

    async def bitget_plan_pending(self, req: Any = None) -> dict:
        req = dict(req or {})
        rows = [dict(p) for p in self.plans.values() if p['planStatus'] == 'not_trigger'
                and (not req.get('symbol') or p['symbol'] == req['symbol'])]
        return {'code': '00000', 'msg': 'success', 'requestTime': 1, 'data': {'entrustedList': rows, 'endId': None}}

    async def bitget_cancel_plan(self, req: dict) -> dict:
        self.calls.append(('cancel_plan', dict(req)))
        ok, failed = [], []
        for item in req.get('orderIdList') or []:
            plan = self.plans.get(str(item.get('orderId')))
            if plan is None or plan['planStatus'] != 'not_trigger':
                failed.append({'orderId': item.get('orderId'), 'clientOid': '', 'errorMsg': 'notExistend'})
                continue
            plan['planStatus'] = 'cancel'
            ok.append({'orderId': plan['orderId'], 'clientOid': plan['clientOid']})
        return {'code': '00000', 'msg': 'success', 'requestTime': 1,
                'data': {'successList': ok, 'failureList': failed}}

    async def gate_price_order(self, req: dict) -> dict:
        self.calls.append(('price_order', dict(req)))
        long = 'long' in str(req.get('order_type'))
        rule = (req.get('trigger') or {}).get('rule')
        if self.fail_stop and (rule == 2) == long:      # SL: long — ≤ (2), short — ≥ (1)
            raise ccxt.InvalidOrder('gate {"label":"INVALID_PARAM_VALUE","message":"invalid argument: trigger"}')
        oid = self._id()
        self.plans[oid] = {'id': int(oid), 'initial': dict(req['initial']), 'trigger': dict(req['trigger']),
                           'order_type': req['order_type'], 'status': 'open', 'finish_as': '', 'create_time': 1.0}
        return {'id': int(oid)}

    async def gate_price_orders(self, req: Any = None) -> list:
        req = dict(req or {})
        return [dict(p) for p in self.plans.values() if p.get('status') == 'open'
                and (not req.get('contract') or p['initial'].get('contract') == req['contract'])]

    async def gate_cancel_price_order(self, req: dict) -> dict:
        self.calls.append(('cancel_price_order', dict(req)))
        plan = self.plans.get(str(req.get('order_id')))
        if plan is None or plan.get('status') != 'open':
            raise ccxt.OrderNotFound('gate {"label":"ORDER_NOT_FOUND","message":"Order not found"}')
        plan.update(status='finished', finish_as='cancelled')
        return dict(plan)

    # MEXC: position is a positionId per symbol and side; TP/SL via stoporder (state 1 untriggered, 2 canceled)
    def _mexc_position_id(self, symbol: str, side: int) -> int:
        return 1000 + sorted(self.pos).index(symbol) * 2 + side if symbol in self.pos else 0

    async def mexc_open_positions(self, req: Any = None) -> dict:
        rows = []
        for s, contracts in self.pos.items():
            if contracts:
                m = self.ex.ccxt.market(s)
                side = 1 if contracts > 0 else 2
                rows.append({'positionId': self._mexc_position_id(s, side), 'symbol': m['id'], 'positionType': side,
                             'openType': 2, 'state': 1, 'holdVol': abs(contracts), 'openAvgPrice': self.price})
        return {'success': True, 'code': 0, 'data': rows}

    def mexc_place_stop(self, req: dict) -> dict:
        self.calls.append(('stoporder_place', dict(req)))
        if self.fail_stop and req.get('stopLossPrice') is not None:
            raise ccxt.InvalidOrder('mexc {"success":false,"code":5003,"message":"Take-profit and stop-loss price '
                                    'setting is wrong"}')
        by_id = {str(self._mexc_position_id(s, 1 if c > 0 else 2)): s for s, c in self.pos.items() if c}
        symbol = by_id.get(str(req.get('positionId')))
        if symbol is None:
            raise ccxt.InvalidOrder('mexc {"success":false,"code":2009,"message":"Positions do not exist or have been '
                                    'closed"}')
        oid = self._id()
        self.plans[oid] = {'id': int(oid), 'symbol': self.ex.ccxt.market(symbol)['id'],
                           'positionId': req['positionId'], 'positionType': 1 if self.pos[symbol] > 0 else 2,
                           'vol': req.get('vol'), 'stopLossPrice': req.get('stopLossPrice'),
                           'takeProfitPrice': req.get('takeProfitPrice'), 'state': 1, 'isFinished': 0,
                           'createTime': 1, '_active': True}
        return {'success': True, 'code': 0, 'data': int(oid)}

    async def mexc_cancel_stop(self, req: Any = None) -> dict:
        self.calls.append(('stoporder_cancel', list(req or [])))
        for item in req or []:
            plan = self.plans.get(str(item.get('stopPlanOrderId')))
            if plan is None or not plan.get('_active'):
                raise ccxt.InvalidOrder('mexc {"success":false,"code":5002,"message":"The Stop-Limit order does not '
                                        'exist or has closed"}')
            plan.update(state=2, isFinished=1, _active=False)
        return {'success': True, 'code': 0}

    # HTX: tpsl_order / tpsl_openorders / tpsl_cancel (status 2 pending, 6 canceled)
    async def htx_tpsl_order(self, req: dict) -> dict:
        self.calls.append(('tpsl_order', dict(req)))
        if self.fail_stop and req.get('sl_trigger_price') is not None:
            raise ccxt.InvalidOrder('htx {"status":"error","err_code":1067,"err_msg":"Illegal parameter sl_trigger_price"}')
        data = {}
        for prefix in ('tp', 'sl'):
            if req.get(f'{prefix}_trigger_price') is None:
                continue
            oid = self._id()
            closes_long = req.get('direction') == 'sell'
            rises = (prefix == 'tp') == closes_long
            self.plans[oid] = {'order_id': int(oid), 'order_id_str': oid, 'contract_code': req['contract_code'],
                               'direction': req['direction'], 'volume': req['volume'], 'tpsl_order_type': prefix,
                               'trigger_type': 'ge' if rises else 'le',
                               'trigger_price': float(req[f'{prefix}_trigger_price']), 'status': 2,
                               'order_price_type': req.get(f'{prefix}_order_price_type'), 'created_at': 1,
                               '_active': True}
            data[f'{prefix}_order'] = {'order_id': int(oid), 'order_id_str': oid}
        return {'status': 'ok', 'data': data, 'ts': 1}

    async def htx_tpsl_openorders(self, req: Any = None) -> dict:
        req = dict(req or {})
        rows = [{k: v for k, v in p.items() if k != '_active'} for p in self.plans.values() if p.get('_active')
                and (not req.get('contract_code') or p.get('contract_code') == req['contract_code'])]
        return {'status': 'ok', 'data': {'orders': rows, 'total_page': 1, 'current_page': 1, 'total_size': len(rows)},
                'ts': 1}

    async def htx_tpsl_cancel(self, req: dict) -> dict:
        self.calls.append(('tpsl_cancel', dict(req)))
        ok, failed = [], []
        for oid in str(req.get('order_id') or '').split(','):
            plan = self.plans.get(oid)
            if plan is None or not plan.get('_active'):
                failed.append({'order_id': oid, 'err_code': 1061, 'err_msg': "This order doesn't exist."})
                continue
            plan.update(status=6, _active=False)
            ok.append(oid)
        return {'status': 'ok', 'data': {'errors': failed, 'successes': ','.join(ok)}, 'ts': 1}

    def stops(self) -> list[dict]:
        """Active Bitget / Gate / MEXC / HTX TP/SL in exchange format."""
        return [{k: v for k, v in p.items() if k != '_active'} for p in self.plans.values()
                if p.get('_active', p.get('planStatus', p.get('status')) in ('not_trigger', 'open'))]

    # ─── selections for tests ──────────────────────────────────────────────
    def entries(self) -> list[tuple]:
        """create_order calls for entries (not reduce-only, no trigger)."""
        return [c for c in self.calls if c[0] == 'create_order' and not c[6].get('reduceOnly')
                and not any(c[6].get(k) is not None for k in _TRIGGER_KEYS)]

    def closes(self) -> list[tuple]:
        """create_order calls for closes (reduce-only, no trigger)."""
        return [c for c in self.calls if c[0] == 'create_order' and c[6].get('reduceOnly')
                and not any(c[6].get(k) is not None for k in _TRIGGER_KEYS)]


def make_exchange(name: str, *, symbols: Optional[list[str]] = None, **kwargs: Any) -> Any:
    """An ``Exchange`` on a market snapshot, offline (dummy keys, one-way, cross)."""
    from bytx import create_exchange
    cid = ccxt_id(name)
    kwargs.setdefault('margin_mode', 'cross')
    kwargs.setdefault('hedge', False)
    creds = HL_CREDS if cid == 'hyperliquid' else CREDS
    for k, v in creds.items():
        kwargs.setdefault(k, v)
    ex = create_exchange(NAMES[cid], **kwargs)
    prepare(ex, cid, symbols)
    if cid in ('kucoinfutures', 'mexc', 'htx'):
        # KuCoin / MEXC / HTX take leverage from each order, as after set_leverage
        ex._leverage[SYMBOLS[NAMES[cid]]] = 5
    if cid == 'kucoinfutures':
        ex._hedge = False   # account position mode already read: one-way (KuCoin getPositionMode → 0)
    return ex


def fake_exchange(name: str = 'binance', *, price: float = 50_000.0, free: float = 1_000_000.0,
                  fail_stop: bool = False, **kwargs: Any) -> tuple[Any, FakeVenue]:
    """(``Exchange``, :class:`FakeVenue`): emulator of exchange ``name`` (binance, bybit, okx, bingx, kucoin,
    hyperliquid, bitget, gate, mexc, htx). ``kwargs`` as in :func:`bytx.create_exchange` (client_id_prefix, risk, journal…)."""
    ex = make_exchange(name, **kwargs)
    return ex, FakeVenue(ex, price, free=free, fail_stop=fail_stop)


__all__ = ['CCXT_IDS', 'CREDS', 'FakeVenue', 'HL_CREDS', 'MARKETS_DIR', 'NAMES', 'NetworkDisabled', 'SYMBOLS',
           'ccxt_id', 'fake_exchange', 'make_exchange', 'markets', 'offline', 'prepare']
