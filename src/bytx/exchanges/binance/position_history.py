"""
Binance Position History Reconstruction Module

This module provides functionality to reconstruct closed position history for Binance exchange.
Since Binance doesn't support fetch_positions_history() API, we reconstruct positions by analyzing
trades and tracking cumulative position size.
"""

import time

from bytx.exchanges._records import TradeHistory
from loguru import logger


# /fapi/v1/userTrades: startTime..endTime window at most 7 days, limit at most 1000
# (default 500). So history over a long period is fetched in windows and pages.
_TRADES_WINDOW_MS = 7 * 24 * 60 * 60 * 1000 - 1
_TRADES_PAGE_LIMIT = 1000
# Guard against an infinite loop and burning the rate limit (~5.7 years of history in 7-day windows).
_TRADES_MAX_CALLS = 300
_QTY_EPS = 1e-8


class BinancePositionHistoryReconstructor:
    """
    Reconstructs closed position history for Binance from trade data.

    Algorithm:
    1. Fetch all trades for a symbol
    2. Group trades by positionSide (LONG/SHORT/BOTH for hedge mode)
    3. Track cumulative position size to detect open/close cycles
    4. When position size returns to 0, position is fully closed
    5. Build PositionModel-compatible dictionaries with trade history
    """

    def __init__(self, exchange, symbol: str, bot_id: str, hedge_mode: bool, is_cross: bool):
        """
        Initialize the reconstructor.

        Args:
            exchange: CCXT exchange instance
            symbol: Trading symbol (e.g., 'BTC/USDT:USDT')
            bot_id: Bot identifier for logging
            hedge_mode: Whether hedge mode is enabled
            is_cross: Whether cross margin mode is used
        """
        self.exchange = exchange
        self.symbol = symbol
        self.bot_id = bot_id
        self.hedge_mode = hedge_mode
        self.is_cross = is_cross

    async def fetch_position_history(self, since: int | None = None) -> list[dict]:
        """
        Fetch and reconstruct closed positions from trade history.

        Args:
            since: Timestamp in milliseconds to fetch from

        Returns:
            List of position-like dictionaries compatible with PositionModel
        """
        try:
            # Fetch all trades (paginated by time and by limit, review id 70)
            trades = await self._fetch_all_trades(since)

            if not trades:
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\t⚠️ No trades found for {self.symbol}"
                )
                return []

            logger.bind(bot_id=self.bot_id).debug(
                f"[{self.bot_id}]\tBinance trades found: {len(trades)}"
            )

            # Sort trades by timestamp
            trades.sort(key=lambda x: x['timestamp'])

            # Group trades by positionSide for hedge mode
            positions_by_side = self._group_trades_by_position_side(trades)

            # Reconstruct closed positions
            closed_positions = []
            for position_side, side_trades in positions_by_side.items():
                closed_positions.extend(
                    self._reconstruct_positions_from_trades(side_trades, position_side)
                )

            logger.bind(bot_id=self.bot_id).info(
                f"[{self.bot_id}]\t✅ Reconstructed {len(closed_positions)} closed position(s) from Binance history"
            )

            return closed_positions

        except Exception as e:
            logger.bind(bot_id=self.bot_id).error(
                f"[{self.bot_id}]\tError fetching Binance position history: {e}"
            )
            return []

    async def _fetch_all_trades(self, since: int | None) -> list[dict]:
        """
        Fetch all trades for a symbol starting from since.

        A single fetch_my_trades(since=...) on Binance linear returns only the window
        [since, since + 7 days] and at most 500 records (review id 70). Here we walk 7-day
        windows up to now, and within a window — pages of 1000, moving the cursor
        to the last trade's time. Duplicates at page boundaries are removed by trade id.
        """
        if since is None:
            # Without since Binance returns the last 7 days; there's nothing to anchor a window to.
            return await self.exchange.fetch_my_trades(
                symbol=self.symbol, limit=_TRADES_PAGE_LIMIT
            ) or []

        now = max(int(time.time() * 1000), int(since))
        collected: dict = {}
        calls = 0
        window_start = int(since)

        while window_start <= now:
            window_end = min(window_start + _TRADES_WINDOW_MS, now)
            cursor = window_start
            while True:
                if calls >= _TRADES_MAX_CALLS:
                    logger.bind(bot_id=self.bot_id).warning(
                        f"[{self.bot_id}]\t⚠️ Binance trades pagination stopped after {calls} calls "
                        f"(reached {self.exchange.iso8601(cursor)}); history may be incomplete"
                    )
                    return list(collected.values())
                batch = await self.exchange.fetch_my_trades(
                    symbol=self.symbol,
                    since=cursor,
                    limit=_TRADES_PAGE_LIMIT,
                    params={'until': window_end},
                ) or []
                calls += 1
                for trade in batch:
                    key = trade.get('id')
                    if key is None:
                        key = (trade.get('timestamp'), trade.get('order'), trade.get('side'),
                               trade.get('amount'), trade.get('price'))
                    collected[key] = trade
                if len(batch) < _TRADES_PAGE_LIMIT:
                    break
                last_ts = max(t['timestamp'] for t in batch)
                # The next page starts at the last trade's time (trades in the same ms
                # may not have fit the page); if the whole page is in one ms — advance by 1 ms.
                cursor = last_ts if last_ts > cursor else cursor + 1
                if cursor > window_end:
                    break
            window_start = window_end + 1

        return list(collected.values())

    def _group_trades_by_position_side(self, trades: list[dict]) -> dict[str, list[dict]]:
        """
        Group trades by positionSide field.

        Args:
            trades: List of trade dictionaries from CCXT

        Returns:
            Dictionary mapping positionSide to list of trades
        """
        positions_by_side = {}

        for trade in trades:
            position_side = trade['info'].get('positionSide', 'BOTH')
            if position_side not in positions_by_side:
                positions_by_side[position_side] = []
            positions_by_side[position_side].append(trade)

        return positions_by_side

    def _reconstruct_positions_from_trades(
        self,
        trades: list[dict],
        position_side: str
    ) -> list[dict]:
        """
        Reconstruct closed positions from trades for a specific positionSide.

        Tracks cumulative position size:
        - For LONG: buy increases, sell decreases
        - For SHORT: sell increases, buy decreases
        - When size returns to 0, position is closed

        Args:
            trades: List of trades sorted by timestamp
            position_side: 'LONG', 'SHORT', or 'BOTH'

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

            # Process based on position side
            if position_side == 'LONG':
                if side == 'buy':
                    # Opening/Adding to LONG position
                    current_position = self._handle_opening_trade(
                        current_position, trade, position_side, 'long', timestamp
                    )
                    cumulative_qty += qty
                    total_entry_cost += qty * price
                    total_entry_qty += qty
                else:  # sell
                    # Closing LONG position
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

            elif position_side == 'BOTH':
                # One-Way mode: need to track direction based on cumulative position
                # Positive cumulative_qty = LONG, Negative = SHORT
                signed_qty = qty if side == 'buy' else -qty
                is_closing = (
                    current_position is not None
                    and abs(cumulative_qty) >= _QTY_EPS
                    and (cumulative_qty > 0) != (signed_qty > 0)
                )
                if not is_closing:
                    # Opening/Adding to LONG (buy) or SHORT (sell) position
                    current_position = self._handle_opening_trade(
                        current_position, trade, position_side,
                        'long' if side == 'buy' else 'short', timestamp
                    )
                    cumulative_qty += signed_qty
                    total_entry_cost += qty * price
                    total_entry_qty += qty
                    continue

                remaining = abs(cumulative_qty)
                excess = qty - remaining
                if excess > max(_QTY_EPS, remaining * 1e-9):
                    # Position flip in a single trade (review id 70): close the current
                    # position on the remainder, the excess opens the opposite position.
                    closing_part, opening_part = self._split_trade(trade, remaining)
                    current_position['closing_trades'].append(closing_part)
                    position_dict = self._finalize_position(
                        current_position, total_entry_cost, total_entry_qty, timestamp
                    )
                    if position_dict:
                        positions.append(position_dict)

                    current_position = self._handle_opening_trade(
                        None, opening_part, position_side,
                        'long' if side == 'buy' else 'short', timestamp
                    )
                    cumulative_qty = excess if side == 'buy' else -excess
                    total_entry_cost = excess * price
                    total_entry_qty = excess
                    continue

                current_position['closing_trades'].append(trade)
                cumulative_qty += signed_qty

                # Position fully closed
                if abs(cumulative_qty) < _QTY_EPS:
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

            elif position_side == 'SHORT':
                if side == 'sell':
                    # Opening/Adding to SHORT position
                    current_position = self._handle_opening_trade(
                        current_position, trade, position_side, 'short', timestamp
                    )
                    cumulative_qty += qty
                    total_entry_cost += qty * price
                    total_entry_qty += qty
                else:  # buy
                    # Closing SHORT position
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

        return positions

    @staticmethod
    def _split_trade(trade: dict, closing_qty: float) -> tuple[dict, dict]:
        """
        Split a flip trade into the closing part (closing_qty) and the opening part (remainder).

        Binance realizedPnl refers to the closed volume, so it stays entirely
        in the closing part; the fee is split pro rata by volume. The trade id
        is kept in both parts (it's the real exchange id), and a flag is set in info.
        """
        qty = trade['amount']
        opening_qty = qty - closing_qty
        ratio_close = closing_qty / qty if qty else 0.0

        def _part(amount: float, ratio: float, realized_pnl) -> dict:
            part = dict(trade)
            info = dict(trade.get('info') or {})
            info['realizedPnl'] = realized_pnl
            part['info'] = info
            part['amount'] = amount
            if trade.get('price') is not None:
                part['cost'] = amount * trade['price']
            fee = trade.get('fee')
            if isinstance(fee, dict) and fee.get('cost') is not None:
                part['fee'] = {**fee, 'cost': fee['cost'] * ratio}
            fees = trade.get('fees')
            if isinstance(fees, list):
                part['fees'] = [
                    {**f, 'cost': f['cost'] * ratio} if isinstance(f, dict) and f.get('cost') is not None else f
                    for f in fees
                ]
            return part

        closing = _part(closing_qty, ratio_close, (trade.get('info') or {}).get('realizedPnl', '0'))
        closing['info']['flipSplit'] = 'close'
        opening = _part(opening_qty, 1.0 - ratio_close, '0')
        opening['info']['flipSplit'] = 'open'
        return closing, opening

    def _handle_opening_trade(
        self,
        current_position: dict | None,
        trade: dict,
        position_side: str,
        side: str,
        timestamp: int
    ) -> dict:
        """
        Handle an opening trade (create position if needed, add trade to opening list).

        Args:
            current_position: Current position data or None
            trade: Trade dictionary
            position_side: Position side ('LONG', 'SHORT', 'BOTH')
            side: Normalized side ('long', 'short')
            timestamp: Trade timestamp

        Returns:
            Updated or new position dictionary
        """
        if current_position is None:
            current_position = {
                'opening_trades': [],
                'closing_trades': [],
                'position_side': position_side,
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
        - Realized PNL from trade data
        - Complete trade history in TradeHistory format

        Args:
            position_data: Dict with opening_trades, closing_trades, etc.
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

        # Calculate realized PNL from closing trades
        realized_pnl = sum(
            float(t['info'].get('realizedPnl', 0))
            for t in position_data['closing_trades']
        )

        # Build complete trade history
        trade_history = self._build_trade_history(
            position_data['opening_trades'],
            position_data['closing_trades']
        )

        # Create position dictionary
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
                'positionSide': position_data['position_side'],
                'avgEntryPrice': avg_entry_price,
                'avgExitPrice': avg_exit_price,
                'opening_trades': len(position_data['opening_trades']),
                'closing_trades': len(position_data['closing_trades']),
                'first_trade_id': position_id,
            },
            'timestamp': position_data['start_time'],
            'datetime': self.exchange.iso8601(position_data['start_time']),
            'lastUpdateTimestamp': close_time,
            'hedged': self.hedge_mode,
            'leverage': 1,
            'unrealizedPnl': 0,
            'realizedPnl': realized_pnl,
            'marginMode': 'cross' if self.is_cross else 'isolated',
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

        Args:
            opening_trades: List of opening trade dictionaries
            closing_trades: List of closing trade dictionaries

        Returns:
            List of TradeHistory objects
        """
        trade_history = []
        all_trades = opening_trades + closing_trades

        for trade in all_trades:
            try:
                trade_obj = TradeHistory.from_exchange(
                    data=trade,
                    exchange='binance'
                )
                trade_history.append(trade_obj)
            except Exception as e:
                logger.bind(bot_id=self.bot_id).warning(
                    f"[{self.bot_id}]\t⚠️ Failed to convert trade {trade.get('id')} to TradeHistory: {e}"
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
