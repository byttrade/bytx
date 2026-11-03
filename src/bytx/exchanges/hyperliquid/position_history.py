"""
Hyperliquid Position History Reconstruction Module

This module provides functionality to reconstruct closed position history for Hyperliquid exchange.
Since Hyperliquid doesn't support fetch_positions_history() API, we reconstruct positions by
analyzing trades and tracking cumulative position size.

Hyperliquid specifics:
- One-way mode only (no hedge mode)
- Trade info contains 'closedPnl' (string) for realized PNL
- Trade info contains 'dir' field: 'Open Long', 'Close Long', 'Open Short', 'Close Short'
- Fee currency comes from 'feeToken' in raw trade info
"""

import time
from decimal import Decimal, InvalidOperation

from bytx.exchanges._records.position import TradeHistory, OrderFee
from loguru import logger

from bytx.utils.history_paging import (
    _HL_HORIZON,
    _HL_PAGE,
    HistoryBudget,
    get_history_semaphore,
    log_fetch,
    new_budget,
)

# KuCoin futures /api/v1/fills: pageSize defaults to 50 (ccxt 4.4.65 doesn't set it without limit).
_KUCOIN_FILLS_DEFAULT_PAGE = 50
_ZERO = Decimal(0)


def _dec(value) -> Decimal | None:
    if value is None or value == '':
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _tid(trade: dict) -> int:
    try:
        return int((trade.get('info') or {}).get('tid'))
    except (TypeError, ValueError):
        return 0


def _fill_sort_key(trade: dict):
    return (trade.get('timestamp') or 0, _tid(trade))


def _signed_size(trade: dict) -> Decimal:
    sz = _dec((trade.get('info') or {}).get('sz'))
    if sz is None:
        sz = _dec(trade.get('amount')) or _ZERO
    return sz if trade.get('side') == 'buy' else -sz


def _order_by_chain(fills: list[dict]) -> list[dict]:
    """
    Order of fills within one ms follows the startPosition chain: tid need not match
    execution order, and fills within one ms share the same time.
    """
    out: list[dict] = []
    expected: Decimal | None = None
    i = 0
    ordered = sorted(fills, key=_fill_sort_key)
    while i < len(ordered):
        j = i
        while j < len(ordered) and ordered[j].get('timestamp') == ordered[i].get('timestamp'):
            j += 1
        group = ordered[i:j]
        while group:
            idx = 0
            if expected is not None:
                for k, t in enumerate(group):
                    if _dec((t.get('info') or {}).get('startPosition')) == expected:
                        idx = k
                        break
            t = group.pop(idx)
            out.append(t)
            sp = _dec((t.get('info') or {}).get('startPosition'))
            expected = (sp + _signed_size(t)) if sp is not None else None
        i = j
    return out


class HyperliquidPositionHistoryReconstructor:
    """
    Reconstructs closed position history for Hyperliquid from trade data.

    Hyperliquid (exchange_name='hyperliquid'):
    1. userFillsByTime pages forward (up to 2000 fills per response across ALL account coins),
       cursor = max(time) inclusive, dedup by tid, the symbol is filtered locally.
    2. Reconstruction along the info.startPosition chain (Decimal): a position opens with a fill with
       startPosition == 0; a chain break (gap/different delivery order) drops the current
       unfinished position with a warning; a flip is split into a close and an
       open.

    KuCoin futures (and HL with an explicit limit) — old behavior: a single fetch_my_trades call and
    cumulative reconstruction; for KuCoin a full page gives a warning.
    """

    def __init__(self, exchange, symbol: str, bot_id: str, exchange_name: str | None = None,
                 max_calls: int | None = None, deadline_s: float | None = None):
        """
        Args:
            exchange: CCXT async exchange instance (hyperliquid / kucoinfutures)
            symbol: Trading symbol (e.g., 'BTC/USDC:USDC')
            bot_id: Bot identifier for logging
            exchange_name: exchange name (default str(exchange).lower())
            max_calls / deadline_s: HL budget override (6 / 20 s)
        """
        self.exchange = exchange
        self.symbol = symbol
        self.bot_id = bot_id
        self.exchange_name = (exchange_name or str(exchange)).lower()
        self._max_calls = max_calls
        self._deadline_s = deadline_s
        self.last_history_incomplete = False

    def new_budget(self) -> HistoryBudget:
        return new_budget('hyperliquid', self._max_calls, self._deadline_s)

    def _log(self):
        return logger.bind(bot_id=self.bot_id)

    async def fetch_closed_positions(self, since: int | None = None, limit: int | None = None,
                                     budget: HistoryBudget | None = None) -> list[dict]:
        """
        Fetch and reconstruct closed positions from trade history.

        Args:
            since: Timestamp in milliseconds to fetch trades from (None — all available history)
            limit: Maximum number of trades to fetch (if set — old single call)
            budget: shared budget of the top-level call

        Returns:
            List of position-like dictionaries compatible with PositionModel
        """
        self.last_history_incomplete = False
        if self.exchange_name != 'hyperliquid' or limit is not None:
            return await self._fetch_closed_positions_single(since, limit)

        try:
            budget = budget or self.new_budget()
            start = int(since) if since is not None else 0
            end = int(time.time() * 1000)
            fills, complete = await self._fetch_fills(start, end, budget)

            if not fills:
                self.last_history_incomplete = not complete
                self._log().warning(f"[{self.bot_id}]\tNo trades found for {self.symbol}")
                return []

            closed_positions, chain_ok = self._reconstruct_by_start_position(fills)
            self.last_history_incomplete = not (complete and chain_ok)

            self._log().info(
                f"[{self.bot_id}]\tReconstructed {len(closed_positions)} closed position(s) "
                f"from Hyperliquid history ({len(fills)} fills)"
            )
            return closed_positions

        except Exception as e:
            self.last_history_incomplete = True
            self._log().error(f"[{self.bot_id}]\tError fetching Hyperliquid position history: {e}")
            return []

    async def _fetch_closed_positions_single(self, since: int | None, limit: int | None) -> list[dict]:
        """Old behavior: a single fetch_my_trades call for the symbol."""
        try:
            trades = await self.exchange.fetch_my_trades(
                symbol=self.symbol,
                since=since,
                limit=limit
            )

            if not trades:
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\tNo trades found for {self.symbol}"
                )
                return []

            logger.bind(bot_id=self.bot_id).debug(
                f"[{self.bot_id}]\t{self.exchange_name} trades found: {len(trades)}"
            )

            page = limit if limit is not None else (
                _KUCOIN_FILLS_DEFAULT_PAGE if self.exchange_name == 'kucoin futures' else None
            )
            if page is not None and len(trades) >= page:
                self.last_history_incomplete = True
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\t⚠️ {self.exchange_name} fills: full page ({len(trades)}) in a single "
                    f"request — history incomplete, older positions may be missing"
                )

            # Sort trades by timestamp (ascending — chronological order)
            trades.sort(key=lambda x: x['timestamp'])

            if self.exchange_name == 'hyperliquid' and all(
                    _dec((t.get('info') or {}).get('startPosition')) is not None for t in trades):
                closed_positions, chain_ok = self._reconstruct_by_start_position(trades)
                if not chain_ok:
                    self.last_history_incomplete = True
            else:
                # One-way: cumulative signed qty
                closed_positions = self._reconstruct_positions_from_trades(trades)

            logger.bind(bot_id=self.bot_id).info(
                f"[{self.bot_id}]\tReconstructed {len(closed_positions)} closed position(s) from {self.exchange_name} history"
            )

            return closed_positions

        except Exception as e:
            logger.bind(bot_id=self.bot_id).error(
                f"[{self.bot_id}]\tError fetching {self.exchange_name} position history: {e}"
            )
            return []

    async def _fetch_fills(self, start: int, end: int, budget: HistoryBudget) -> tuple[list[dict], bool]:
        """
        All fills of a symbol over [start, end]: userFillsByTime forward in pages of 2000.

        A page is full if the raw length (all account coins) == 2000. Cursor =
        max(time) inclusive (same-ms fills that didn't fit the page come
        in the next one); if the cursor didn't move — +1 ms with a warning. Zero new tids —
        exit. Page error — stop, complete=False.

        Returns:
            (symbol fills in ascending time order, complete)
        """
        sem = get_history_semaphore('hyperliquid')
        cursor = int(start)
        seen: set = set()
        fills: list[dict] = []
        complete = True
        pages = 0
        raw_total = 0
        reached = cursor

        while cursor <= end:
            async with sem:
                if not budget.take():
                    complete = False
                    break
                try:
                    page = await self.exchange.fetch_my_trades(None, since=cursor, params={'until': end})
                except Exception as e:
                    complete = False
                    budget.mark_incomplete('error')
                    self._log().error(
                        f"[{self.bot_id}]\tError fetching Hyperliquid fills page {pages + 1} "
                        f"(since={cursor}): {e}"
                    )
                    break
            pages += 1
            page = page or []
            new = 0
            for t in page:
                info = t.get('info') or {}
                tid = info.get('tid', t.get('id'))
                key = str(tid) if tid is not None else (
                    t.get('timestamp'), t.get('order'), t.get('side'), t.get('amount'), t.get('price'))
                if key in seen:
                    continue
                seen.add(key)
                new += 1
                raw_total += 1
                if t.get('symbol') == self.symbol:
                    fills.append(t)

            if len(page) < _HL_PAGE:
                reached = end
                break
            if new == 0:
                self._log().warning(
                    f"[{self.bot_id}]\t⚠️ Hyperliquid fills: page without new tids at since={cursor}; stop"
                )
                complete = False
                budget.mark_incomplete('no_progress')
                break
            last_ts = max(t.get('timestamp') or 0 for t in page)
            if last_ts > cursor:
                cursor = last_ts
            else:
                self._log().warning(
                    f"[{self.bot_id}]\t⚠️ Hyperliquid fills: full page within one ms ({cursor}); "
                    f"fills of that ms beyond the page may be lost"
                )
                cursor += 1
            reached = cursor

        if raw_total >= _HL_HORIZON * 0.9:
            self._log().warning(
                f"[{self.bot_id}]\t⚠️ Hyperliquid fills: {raw_total} fills fetched — close to the API "
                f"horizon of {_HL_HORIZON} most recent fills; older history is unavailable"
            )

        fills.sort(key=_fill_sort_key)
        log_fetch(self.bot_id, 'hyperliquid fills', self.symbol, pages, len(fills),
                  budget.elapsed_ms(), complete, budget.reason, reached)
        return fills, complete

    def _reconstruct_by_start_position(self, fills: list[dict]) -> tuple[list[dict], bool]:
        """
        Reconstructs closed positions along the info.startPosition chain (Decimal).

        - a position opens with a fill with startPosition == 0;
        - expected startPosition of the next fill = start + signed sz; on a mismatch
          the current unfinished position is dropped (warning), chain_ok=False;
        - the prefix before the first startPosition == 0 is dropped (warning about the API horizon);
        - a fill crossing zero is split into a close and an open (amount and fee
          pro rata, closedPnl entirely to the close).

        Returns:
            (closed positions, chain_ok)
        """
        positions: list[dict] = []
        current = None
        expected: Decimal | None = None
        chain_ok = True
        prefix_skipped = 0

        for trade in _order_by_chain(fills):
            info = trade.get('info') or {}
            sp = _dec(info.get('startPosition'))
            if sp is None:
                chain_ok = False
                self._log().warning(
                    f"[{self.bot_id}]\t⚠️ Hyperliquid fill {info.get('tid')} without startPosition; skipped"
                )
                continue
            signed = _signed_size(trade)

            if current is not None and expected is not None and sp != expected:
                self._log().warning(
                    f"[{self.bot_id}]\t⚠️ Hyperliquid startPosition chain broken at tid={info.get('tid')}: "
                    f"expected {expected}, got {sp}; dropping unfinished position opened at "
                    f"{current['start_time']}"
                )
                chain_ok = False
                current = None

            if current is None:
                if sp != _ZERO:
                    prefix_skipped += 1
                    expected = sp + signed
                    continue
                current = self._new_position(trade, 'long' if signed > 0 else 'short')
                expected = signed
                continue

            new_pos = sp + signed
            if new_pos == _ZERO:
                current['closing_trades'].append(trade)
                positions.append(self._finalize_from(current, trade['timestamp']))
                current = None
            elif (new_pos > 0) == (sp > 0):
                if abs(new_pos) > abs(sp):
                    current['opening_trades'].append(trade)
                else:
                    current['closing_trades'].append(trade)
            else:
                # Flip in a single trade: close |sp|, open |new_pos| on the other side.
                closing_part, opening_part = self._split_fill(trade, float(abs(sp)))
                current['closing_trades'].append(closing_part)
                positions.append(self._finalize_from(current, trade['timestamp']))
                current = self._new_position(opening_part, 'long' if new_pos > 0 else 'short')
            expected = new_pos

        if prefix_skipped:
            self._log().warning(
                f"[{self.bot_id}]\t⚠️ Hyperliquid: {prefix_skipped} fill(s) before the first flat "
                f"position (startPosition != 0) skipped — history starts mid-position "
                f"(since inside a position or API horizon of {_HL_HORIZON} fills)"
            )

        return [p for p in positions if p], chain_ok

    @staticmethod
    def _new_position(trade: dict, side: str) -> dict:
        return {
            'opening_trades': [trade],
            'closing_trades': [],
            'side': side,
            'start_time': trade['timestamp'],
        }

    def _finalize_from(self, position_data: dict, close_time: int) -> dict | None:
        opening = position_data['opening_trades']
        total_entry_qty = sum(float(t['amount']) for t in opening)
        total_entry_cost = sum(float(t['amount']) * float(t['price']) for t in opening)
        return self._finalize_position(position_data, total_entry_cost, total_entry_qty, close_time)

    @staticmethod
    def _split_fill(trade: dict, closing_qty: float) -> tuple[dict, dict]:
        """
        Split a flip fill into the closing part (closing_qty) and the opening part.

        Hyperliquid closedPnl refers to the closed volume — entirely in the closing
        part; the fee is split pro rata by volume. The id is kept in both parts.
        """
        qty = float(trade['amount'])
        opening_qty = qty - closing_qty
        ratio_close = closing_qty / qty if qty else 0.0

        def _part(amount: float, ratio: float, closed_pnl) -> dict:
            part = dict(trade)
            info = dict(trade.get('info') or {})
            info['closedPnl'] = closed_pnl
            part['info'] = info
            part['amount'] = amount
            if trade.get('price') is not None:
                part['cost'] = amount * float(trade['price'])
            fee = trade.get('fee')
            if isinstance(fee, dict) and fee.get('cost') is not None:
                part['fee'] = {**fee, 'cost': float(fee['cost']) * ratio}
            return part

        closing = _part(closing_qty, ratio_close, (trade.get('info') or {}).get('closedPnl', '0'))
        closing['info']['flipSplit'] = 'close'
        opening = _part(opening_qty, 1.0 - ratio_close, '0')
        opening['info']['flipSplit'] = 'open'
        return closing, opening

    def _reconstruct_positions_from_trades(self, trades: list[dict]) -> list[dict]:
        """
        Reconstruct closed positions from trades.

        Hyperliquid is one-way mode: cumulative signed qty tracks direction.
        - Positive cumulative_qty = LONG position open
        - Negative cumulative_qty = SHORT position open
        - When cumulative_qty returns to ~0, position is fully closed

        Args:
            trades: List of trades sorted by timestamp (ascending)

        Returns:
            List of closed position dictionaries
        """
        positions = []
        current_position = None
        cumulative_qty = 0.0
        total_entry_cost = 0.0
        total_entry_qty = 0.0

        for trade in trades:
            side = trade['side']  # 'buy' or 'sell'
            qty = trade['amount']
            price = trade['price']
            timestamp = trade['timestamp']

            if side == 'buy':
                if cumulative_qty < 0:
                    # Closing a SHORT position
                    if current_position is not None:
                        current_position['closing_trades'].append(trade)
                        cumulative_qty += qty

                        # Position fully closed
                        if abs(cumulative_qty) < 1e-8:
                            position_dict = self._finalize_position(
                                current_position,
                                total_entry_cost,
                                total_entry_qty,
                                trade['timestamp']
                            )
                            if position_dict:
                                positions.append(position_dict)

                            # Reset for next position
                            current_position = None
                            cumulative_qty = 0.0
                            total_entry_cost = 0.0
                            total_entry_qty = 0.0
                else:
                    # Opening/adding to a LONG position
                    current_position = self._handle_opening_trade(
                        current_position, trade, 'long', timestamp
                    )
                    cumulative_qty += qty
                    total_entry_cost += qty * price
                    total_entry_qty += qty

            else:  # sell
                if cumulative_qty > 0:
                    # Closing a LONG position
                    if current_position is not None:
                        current_position['closing_trades'].append(trade)
                        cumulative_qty -= qty

                        # Position fully closed
                        if abs(cumulative_qty) < 1e-8:
                            position_dict = self._finalize_position(
                                current_position,
                                total_entry_cost,
                                total_entry_qty,
                                trade['timestamp']
                            )
                            if position_dict:
                                positions.append(position_dict)

                            # Reset for next position
                            current_position = None
                            cumulative_qty = 0.0
                            total_entry_cost = 0.0
                            total_entry_qty = 0.0
                else:
                    # Opening/adding to a SHORT position
                    current_position = self._handle_opening_trade(
                        current_position, trade, 'short', timestamp
                    )
                    cumulative_qty -= qty
                    total_entry_cost += qty * price
                    total_entry_qty += qty

        return positions

    def _handle_opening_trade(
        self,
        current_position: dict | None,
        trade: dict,
        side: str,
        timestamp: int
    ) -> dict:
        """
        Handle an opening trade: create position if needed, add trade to opening list.

        Args:
            current_position: Current position data or None
            trade: Trade dictionary from CCXT
            side: Normalized side ('long' or 'short')
            timestamp: Trade timestamp

        Returns:
            Updated or new position dictionary
        """
        if current_position is None:
            current_position = {
                'opening_trades': [],
                'closing_trades': [],
                'side': side,
                'start_time': timestamp,
            }
        current_position['opening_trades'].append(trade)
        return current_position

    def _finalize_position(
        self,
        position_data: dict,
        total_entry_cost: float,
        total_entry_qty: float,
        close_time: int
    ) -> dict | None:
        """
        Finalize a closed position and create PositionModel-compatible dictionary.

        Calculates:
        - Average entry/exit prices
        - Realized PNL from closing trade info (info.closedPnl)
        - Complete trade history in TradeHistory format

        Args:
            position_data: Dict with opening_trades, closing_trades, side, start_time
            total_entry_cost: Total cost of entry trades
            total_entry_qty: Total quantity of entry trades
            close_time: Timestamp of position close

        Returns:
            Dictionary compatible with PositionModel or None
        """
        if not position_data['opening_trades'] or not position_data['closing_trades']:
            return None

        # Get position ID from first opening trade
        first_opening_trade = position_data['opening_trades'][0]
        position_id = first_opening_trade.get('id', None)

        # Calculate average entry price
        avg_entry_price = total_entry_cost / total_entry_qty if total_entry_qty > 0 else 0

        # Calculate average exit price
        total_exit_cost = sum(t['amount'] * t['price'] for t in position_data['closing_trades'])
        total_exit_qty = sum(t['amount'] for t in position_data['closing_trades'])
        avg_exit_price = total_exit_cost / total_exit_qty if total_exit_qty > 0 else 0

        # Calculate realized PNL from closing trades (Hyperliquid stores it in info.closedPnl)
        realized_pnl = sum(
            float(t['info'].get('closedPnl', 0) or 0)
            for t in position_data['closing_trades']
        )

        # Build complete trade history
        trade_history = self._build_trade_history(
            position_data['opening_trades'],
            position_data['closing_trades']
        )

        # Create position dictionary compatible with PositionModel
        position = {
            'id': position_id,
            'symbol': self.symbol,
            'contracts': total_entry_qty,
            'contractSize': 1.0,
            'entryPrice': avg_entry_price,
            'markPrice': avg_exit_price,
            'side': position_data['side'],
            'percentage': self._calculate_roi_percentage(realized_pnl, avg_entry_price, total_entry_qty),
            'info': {
                'positionSide': 'BOTH',
                'avgEntryPrice': avg_entry_price,
                'avgExitPrice': avg_exit_price,
                'opening_trades': len(position_data['opening_trades']),
                'closing_trades': len(position_data['closing_trades']),
                'first_trade_id': position_id,
            },
            'timestamp': position_data['start_time'],
            'datetime': self.exchange.iso8601(position_data['start_time']),
            'lastUpdateTimestamp': close_time,
            'hedged': False,
            'leverage': 1,
            'unrealizedPnl': 0,
            'realizedPnl': realized_pnl,
            'marginMode': 'cross',
            'trade_history': trade_history,
        }

        return position

    def _build_trade_history(
        self,
        opening_trades: list[dict],
        closing_trades: list[dict]
    ) -> list[TradeHistory]:
        """
        Convert all trades to TradeHistory format.

        Constructs TradeHistory objects directly (TradeHistory.from_exchange
        does not yet support 'hyperliquid').

        Args:
            opening_trades: List of opening trade dictionaries from CCXT
            closing_trades: List of closing trade dictionaries from CCXT

        Returns:
            List of TradeHistory objects
        """
        trade_history = []
        all_trades = opening_trades + closing_trades

        for trade in all_trades:
            try:
                info = trade.get('info', {})

                # Build fee object
                fee_obj = None
                fee_data = trade.get('fee')
                if isinstance(fee_data, dict) and fee_data:
                    fee_cost = fee_data.get('cost', 0)
                    fee_currency = fee_data.get('currency') or info.get('feeToken', 'USDC')
                    try:
                        fee_obj = OrderFee(
                            cost=float(fee_cost) if fee_cost is not None else 0.0,
                            currency=str(fee_currency)
                        )
                    except Exception:
                        fee_obj = None

                # Extract realized PNL from info.closedPnl (only non-zero on closing trades)
                raw_pnl = info.get('closedPnl')
                realized_pnl = float(raw_pnl) if raw_pnl and float(raw_pnl) != 0 else None

                trade_obj = TradeHistory(
                    exchange='hyperliquid',
                    id=str(trade.get('id', '')),
                    order_id=str(trade.get('order', '')) if trade.get('order') is not None else None,
                    timestamp=trade['timestamp'],
                    datetime=trade.get('datetime', ''),
                    symbol=trade.get('symbol', self.symbol),
                    side=trade['side'],
                    price=float(trade['price']),
                    amount=float(trade['amount']),
                    cost=float(trade.get('cost') or trade['amount'] * trade['price']),
                    taker_or_maker=trade.get('takerOrMaker'),
                    type=trade.get('type'),
                    position_side=None,  # Hyperliquid is one-way only
                    fee=fee_obj,
                    realized_pnl=realized_pnl,
                )
                trade_history.append(trade_obj)
            except Exception as e:
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\tFailed to convert trade {trade.get('id')} to TradeHistory: {e}"
                )

        return trade_history

    @staticmethod
    def _calculate_roi_percentage(realized_pnl: float, avg_entry_price: float, contracts: float) -> float:
        """
        Calculate ROI percentage.

        Args:
            realized_pnl: Realized profit/loss
            avg_entry_price: Average entry price
            contracts: Number of contracts

        Returns:
            ROI percentage
        """
        position_value = avg_entry_price * contracts
        if position_value > 0:
            return (realized_pnl / position_value) * 100
        return 0.0
