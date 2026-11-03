"""HTX USDT-M: current leverage and account position mode.

- [Cross] Query User's Account Information (POST /linear-swap-api/v1/swap_cross_account_info): ``position_mode`` —
  «single_side; dual_side», ``contract_detail[].lever_rate`` — contract leverage in cross;
- [Isolated] Query User's Account Information (POST /linear-swap-api/v1/swap_account_info, ``contract_code``):
  contract ``lever_rate`` in isolated.

Place An Order: «the leverage you choose when placing an order should be the same as the leverage of your current
positions, otherwise, the order will fail» — ``lever_rate`` is taken from here if leverage isn't set (ccxt sends 1).
"""
from __future__ import annotations

from typing import Any, Optional

DOCS = ('https://huobiapi.github.io/docs/usdt_swap/v1/en/#cross-query-user-39-s-account-information',
        'https://huobiapi.github.io/docs/usdt_swap/v1/en/#isolated-query-user-s-account-information')
MARGIN_ACCOUNT = 'USDT'


def _rows(response: Any) -> list:
    data = response.get('data') if isinstance(response, dict) else None
    return [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []


def cross_lever_rate(response: Any, contract_code: str) -> Optional[float]:
    for account in _rows(response):
        for row in account.get('contract_detail') or []:
            if str(row.get('contract_code') or '').upper() == contract_code.upper() and row.get('lever_rate'):
                return float(row['lever_rate'])
    return None


def isolated_lever_rate(response: Any, contract_code: str) -> Optional[float]:
    for row in _rows(response):
        if str(row.get('contract_code') or '').upper() == contract_code.upper() and row.get('lever_rate'):
            return float(row['lever_rate'])
    return None


def parse_position_mode(response: Any) -> Optional[str]:
    for row in _rows(response):
        if row.get('position_mode'):
            return str(row['position_mode'])
    return None


async def current_leverage(ex: Any, s: str, mode: str) -> Optional[float]:
    contract = ex.ccxt.market(s)['id']
    if mode == 'isolated':
        raw = await ex._call(ex.ccxt.contract_private_post_linear_swap_api_v1_swap_account_info,
                             {'contract_code': contract})
        return isolated_lever_rate(raw, contract)
    raw = await ex._call(ex.ccxt.contract_private_post_linear_swap_api_v1_swap_cross_account_info,
                         {'margin_account': MARGIN_ACCOUNT})
    return cross_lever_rate(raw, contract)


async def position_mode(ex: Any) -> Optional[str]:
    raw = await ex._call(ex.ccxt.contract_private_post_linear_swap_api_v1_swap_cross_account_info,
                         {'margin_account': MARGIN_ACCOUNT})
    return parse_position_mode(raw)
