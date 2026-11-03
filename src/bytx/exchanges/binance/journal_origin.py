"""Binance USDⓈ-M: order origin signals for the position journal.

Source — official docs (ground truth: ``tests/ground_truth/binance/journal_origin.json``):

- User Data Stream, Event: Order Update (``ORDER_TRADE_UPDATE``): special client order ids —
  ``autoclose-…`` (liquidation), ``adl_autoclose`` (ADL), ``settlement_autoclose-…`` (settlement on
  delisting/delivery); order type ``LIQUIDATION``; execution type ``CALCULATED - Liquidation
  Execution`` and ``AMENDMENT - Order Modified``;
- New Order: ``newClientOrderId`` — «Automatically generated if not sent»: every order has a
  cid, so a foreign cid doesn't distinguish a manual order from software (``AUTO_CLIENT_ID``).

The docs have no «order from web UI / app» signal. Cancels carry no origin.
"""
from __future__ import annotations

from typing import Optional

from bytx.journal.origin import Signal

EXCHANGE = 'binance'
AUTO_CLIENT_ID = True
CLIENT_ID_REQUIRED = False
CLIENT_ID_FIELD = 'newClientOrderId'
_CID_KEYS = ('clientOrderId', 'c', 'origClientOrderId', 'clientAlgoId', 'caid')


def client_id_of(info: dict) -> Optional[str]:
    for key in _CID_KEYS:
        value = info.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def special_client_id(cid: str) -> Optional[Signal]:
    if cid.startswith('autoclose-'):
        return Signal('liquidation', 'binance: client order id "autoclose-…" — liquidation', event='liquidated')
    if cid.startswith('adl_autoclose'):
        return Signal('adl', 'binance: client order id "adl_autoclose" — ADL', event='adl')
    if cid.startswith('settlement_autoclose-'):
        return Signal('settlement', 'binance: client order id "settlement_autoclose-…" — settlement (delisting/delivery)')
    return None


def order_signal(info: dict) -> Optional[Signal]:
    cid = client_id_of(info)
    if cid:
        sig = special_client_id(cid)
        if sig is not None:
            return sig
    if 'LIQUIDATION' in (info.get('type'), info.get('o'), info.get('origType'), info.get('ot')):
        return Signal('liquidation', 'binance: order type LIQUIDATION', event='liquidated')
    if info.get('x') == 'CALCULATED':
        return Signal('liquidation', 'binance: execution type CALCULATED (Liquidation Execution)', event='liquidated')
    return None


def fill_signal(info: dict) -> Optional[Signal]:
    # REST userTrades carry no signals; WS ORDER_TRADE_UPDATE — the same order dict
    return order_signal(info)


def cancel_signal(info: dict) -> Optional[Signal]:
    return None


def is_amendment(info: dict) -> bool:
    """WS ``x = AMENDMENT`` — order modified (by whom is not stated)."""
    return info.get('x') == 'AMENDMENT'


def fill_position_side(info: dict) -> Optional[str]:
    ps = str(info.get('positionSide') or info.get('ps') or '').upper()
    return {'LONG': 'long', 'SHORT': 'short'}.get(ps)
