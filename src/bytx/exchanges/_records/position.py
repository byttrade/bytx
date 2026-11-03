import re

from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union
from datetime import datetime as dt
from datetime import timezone
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class OrderFee(BaseModel):
    cost: float
    currency: str


class TradeHistory(BaseModel):
    exchange: str
    id: str
    order_id: Optional[str] = Field(None, description="Exchange order ID")
    timestamp: int = Field(..., description="Unix timestamp (ms)")
    datetime: Optional[Union[dt, str]] = Field(..., description="Execution time")

    symbol: str = Field(..., description="Trading pair (BTC/USDT)")
    side: str = Field(..., description="Trade side: buy/sell")
    price: float = Field(..., description="Execution price")
    amount: float = Field(..., description="Asset amount")
    cost: float = Field(..., description="Total order cost")

    taker_or_maker: Optional[str] = Field(None, description="Trade type: taker or maker")
    type: Optional[str] = Field(None, description="Order type: limit/market etc.")
    position_side: Optional[str] = Field(None, description="Position side (futures): LONG/SHORT")

    fee: Optional[OrderFee] = None
    realized_pnl: Optional[float] = Field(None, description="Realized profit/loss")

    @classmethod
    def from_exchange(cls, data: Dict[str, Any], exchange: str) -> Self:
        exchange = exchange.lower()
        match exchange:
            case "binance":
                return cls._parse_binance_trade(data, exchange)
            case "bybit":
                return cls._parse_bybit_trade(data, exchange)
            case "okx":
                return cls._parse_okx_trade(data, exchange)
            # ─── KuCoin Futures ───
            case "kucoin" | "kucoin futures" | "kucoinfutures":
                return cls._parse_kucoin_trade(data, "kucoin")
            # ─── Hyperliquid / BingX ───
            case "hyperliquid":
                return cls._parse_hyperliquid_trade(data, "hyperliquid")
            case "bingx":
                return cls._parse_bingx_trade(data, "bingx")
            case _:
                raise ValueError(f"Unsupported exchange: {exchange}")

    @classmethod
    def _parse_kucoin_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """KuCoin Futures: common fields + position_side derived from side (same as Bybit)."""
        common_fields = cls._parse_common_fields(data, exchange)
        position_side = None
        if data.get('side'):
            position_side = 'LONG' if data['side'].lower() == 'buy' else 'SHORT'
        return cls(
            **common_fields,
            position_side=position_side,
            realized_pnl=None,  # KuCoin doesn't provide realizedPnl in trades
        )

    @classmethod
    def _parse_common_fields(cls, data: Dict[str, Any], exchange: str) -> Dict[str, Any]:
        """Extracts fields common to all exchanges"""
        fee_data = data.get('fee')
        fee = OrderFee(**fee_data) if fee_data else None

        return {
            'exchange': exchange,
            'id': data['id'],
            'order_id': data.get('order'),
            'timestamp': data['timestamp'],
            'datetime': dt.fromisoformat(data['datetime'].replace('Z', '+00:00')),
            'symbol': data['symbol'],
            'side': data['side'],
            'price': data['price'],
            'amount': data['amount'],
            'cost': data['cost'],
            'taker_or_maker': data.get('takerOrMaker'),
            'type': data.get('type'),
            'fee': fee
        }

    @classmethod
    def _parse_binance_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """Parses Binance trade data"""
        common_fields = cls._parse_common_fields(data, exchange)
        pnl = float(data.get('info', {}).get('realizedPnl', 0))

        return cls(
            **common_fields,
            position_side=data.get('info', {}).get('positionSide'),
            realized_pnl=pnl
        )

    @classmethod
    def _parse_bybit_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """Parses Bybit trade data"""
        common_fields = cls._parse_common_fields(data, exchange)

        # Derive position_side from the trade side
        position_side = None
        if data.get('side'):
            position_side = 'LONG' if data['side'].lower() == 'buy' else 'SHORT'

        return cls(
            **common_fields,
            position_side=position_side,
            realized_pnl=None  # Bybit does not report realizedPnl in trades directly
        )

    @classmethod
    def _parse_okx_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """Parses OKX trade data"""
        common_fields = cls._parse_common_fields(data, exchange)

        info = data.get('info', {})
        # OKX reports position_side directly in info.posSide
        position_side = info.get('posSide')
        if position_side:
            position_side = position_side.upper()  # Uppercase for consistency

        # Extract PnL from fillPnl
        pnl = None
        fill_pnl = info.get('fillPnl')
        if fill_pnl and fill_pnl != '0':
            pnl = float(fill_pnl)

        return cls(
            **common_fields,
            position_side=position_side,
            realized_pnl=pnl
        )

    # ─── Hyperliquid / BingX ───
    # Below are ADDED helpers only. The strict _parse_common_fields and the
    # binance/bybit/okx/kucoin parsers are unchanged.

    @staticmethod
    def _to_float_or_none(v: Any) -> Optional[float]:
        """None, '' and values not convertible to float -> None, else float(v). Never raises."""
        if v is None or v == '':
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_timestamp_ms(data: Dict[str, Any], info: Dict[str, Any]) -> Optional[int]:
        """
        Trade time in ms. Order: data['timestamp'] -> info tradeTime/time/filledTm/T
        (int or numeric string) -> info['filledTime'] (number or ISO string like
        '2023-07-04T20:56:01.000+0800'; no tzinfo means UTC). Parse errors -> None.
        """
        def _num(v: Any) -> Optional[int]:
            if isinstance(v, bool):
                return None
            if isinstance(v, (int, float)):
                return int(v)
            if isinstance(v, str) and v.strip():
                try:
                    return int(float(v))
                except ValueError:
                    return None
            return None

        ts = data.get('timestamp')
        if isinstance(ts, int) and not isinstance(ts, bool):
            return ts

        for key in ('tradeTime', 'time', 'filledTm', 'T'):
            ts = _num(info.get(key))
            if ts is not None:
                return ts

        filled_time = info.get('filledTime')
        ts = _num(filled_time)
        if ts is not None:
            return ts
        if isinstance(filled_time, str) and filled_time.strip():
            try:
                s = re.sub(r'([+-]\d{2})(\d{2})$', r'\1:\2', filled_time.strip())
                s = s.replace('Z', '+00:00')
                parsed = dt.fromisoformat(s)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1000)
            except (ValueError, TypeError):
                return None
        return None

    @classmethod
    def _parse_common_fields_lenient(cls, data: Dict[str, Any], exchange: str,
                                     default_fee_currency: str) -> Dict[str, Any]:
        """
        Lenient common-field collection. Used ONLY for hyperliquid and bingx;
        the strict _parse_common_fields for other exchanges is unchanged.

        - order_id: data.order -> info.orderId -> info.oid;
        - id: data.id -> info.tradeId -> info.tid, otherwise a deterministic
          f"{order_id}:{timestamp}:{amount}:{price}" (distinguishes partial fills —
          consumers dedupe trade_history by id);
        - datetime always from timestamp (tz-aware UTC), strings are not parsed;
        - fee=None if fee is missing or cost=None; the cost sign is kept (maker rebate < 0),
          currency: fee.currency -> default_fee_currency;
        - side/time/price/amount/symbol are not guessed: if missing — explicit ValueError.
        """
        info = data.get('info') or {}

        order = data.get('order') or info.get('orderId') or info.get('oid')
        order_id = str(order) if order is not None else None

        timestamp = cls._extract_timestamp_ms(data, info)
        side = (data.get('side') or info.get('side') or '').lower() or None
        price = cls._to_float_or_none(data.get('price'))
        amount = cls._to_float_or_none(data.get('amount'))
        symbol = data.get('symbol')

        missing = [name for name, value in (('timestamp', timestamp), ('side', side), ('price', price),
                                            ('amount', amount), ('symbol', symbol)) if value is None]
        if missing:
            raise ValueError(f"{exchange}: cannot parse trade (order={order_id}): missing {', '.join(missing)}")

        trade_id = data.get('id') or info.get('tradeId') or info.get('tid')
        if trade_id is None:
            trade_id = f"{order_id}:{timestamp}:{amount}:{price}"

        cost = cls._to_float_or_none(data.get('cost'))
        if cost is None:
            cost = price * amount

        fee = None
        fee_data = data.get('fee')
        if isinstance(fee_data, dict):
            fee_cost = cls._to_float_or_none(fee_data.get('cost'))
            if fee_cost is not None:
                fee = OrderFee(cost=fee_cost, currency=str(fee_data.get('currency') or default_fee_currency))

        return {
            'exchange': exchange,
            'id': str(trade_id),
            'order_id': order_id,
            'timestamp': timestamp,
            'datetime': dt.fromtimestamp(timestamp / 1000, tz=timezone.utc),
            'symbol': symbol,
            'side': side,
            'price': price,
            'amount': amount,
            'cost': cost,
            'taker_or_maker': data.get('takerOrMaker'),
            'type': data.get('type'),
            'fee': fee
        }

    @classmethod
    def _parse_hyperliquid_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """
        Hyperliquid (ccxt parse_trade over userFills).

        position_side — only from info.dir: 'Open/Close Long' and 'Liquidated ... Long' -> LONG,
        likewise SHORT; flip ('Long > Short'), spot Buy/Sell, empty/unknown -> None.
        Not derived from side.

        realized_pnl = float(info.closedPnl) — gross PnL without fees, like Binance
        realizedPnl ('0' -> 0.0).
        """
        info = data.get('info') or {}
        common_fields = cls._parse_common_fields_lenient(data, exchange, info.get('feeToken') or 'USDC')

        position_side = None
        direction = str(info.get('dir') or '').strip()
        if '>' not in direction:
            if direction in ('Open Long', 'Close Long') or (
                    direction.startswith('Liquidated') and direction.endswith('Long')):
                position_side = 'LONG'
            elif direction in ('Open Short', 'Close Short') or (
                    direction.startswith('Liquidated') and direction.endswith('Short')):
                position_side = 'SHORT'

        return cls(
            **common_fields,
            position_side=position_side,
            realized_pnl=cls._to_float_or_none(info.get('closedPnl'))
        )

    @classmethod
    def _parse_bingx_trade(cls, data: Dict[str, Any], exchange: str) -> Self:
        """
        BingX (swap). ccxt may return id/timestamp/datetime/side = None (allFillOrders format),
        hence lenient field collection only.

        position_side = info.positionSide, LONG/SHORT only; BOTH (one-way) -> None,
        not derived from side (for closing trades that would give the wrong direction).
        realized_pnl — first of info.realizedPnl / realisedPNL / realisedPnl.
        """
        info = data.get('info') or {}
        common_fields = cls._parse_common_fields_lenient(data, exchange, info.get('currency') or 'USDT')

        position_side = str(info.get('positionSide') or '').upper()
        if position_side not in ('LONG', 'SHORT'):
            position_side = None

        realized_pnl = None
        for key in ('realizedPnl', 'realisedPNL', 'realisedPnl'):
            realized_pnl = cls._to_float_or_none(info.get(key))
            if realized_pnl is not None:
                break

        return cls(
            **common_fields,
            position_side=position_side,
            realized_pnl=realized_pnl
        )


class PositionHistoryStats(BaseModel):
    trades: List[TradeHistory]
    exchange: str
    total_trades: int
    total_pnl: float
    total_fee: float
    total_take_profit: float

    @classmethod
    def from_exchange(cls, data: List[dict], exchange: str) -> "PositionHistoryStats":
        trades = [TradeHistory.from_exchange(trade_data, exchange) for trade_data in data]

        total_trades = len(trades)
        total_pnl = sum(trade.realized_pnl or 0 for trade in trades)
        total_fee = sum(trade.fee.cost if trade.fee else 0 for trade in trades)
        total_take_profit = total_pnl - total_fee  # This is the correct way

        return cls(
            trades=trades,
            exchange=exchange,
            total_trades=total_trades,
            total_pnl=total_pnl,
            total_fee=total_fee,
            total_take_profit=total_take_profit
        )


class PositionModel(BaseModel):
    bot_id: Optional[str] = None  # BOT ID used for tagging inside the trader
    collateral: Optional[float] = None  # Position collateral
    contractSize: Optional[float] = None  # Size of one contract
    contracts: Optional[float] = None  # Number of contracts in the position
    datetime: Optional[str] = None  # Position creation time, ISO format
    entryPrice: Optional[float] = None  # Entry price
    hedged: Optional[bool] = None  # Whether hedging is used (True/False)
    id: Optional[str] = None  # Unique position ID
    info: Optional[Dict[str, Any]] = None  # Raw exchange JSON response
    initialMargin: Optional[float] = None  # Initial position margin
    initialMarginPercentage: Optional[float] = None  # Margin as a percentage of position size
    lastPrice: Optional[float] = None  # Last instrument price (on update)
    lastUpdateTimestamp: Optional[int] = None  # Last position update time (UNIX timestamp)
    leverage: Optional[float] = None  # Leverage used
    liquidationPrice: Optional[float] = None  # Liquidation price
    maintenanceMargin: Optional[float] = None  # Maintenance margin
    maintenanceMarginPercentage: Optional[float] = None  # Maintenance margin as % of position
    marginMode: Optional[str] = None  # Margin mode (cross or isolated)
    marginRatio: Optional[float] = None  # Margin ratio (used to available)
    markPrice: Optional[float] = None  # Mark price (current fair price of the asset)
    notional: Optional[float] = None  # Position notional value
    percentage: Optional[float] = None  # Position change percentage (PnL as % of margin)
    realizedPnl: Optional[float] = None  # Realized profit/loss (PnL)
    side: Optional[str] = None  # Position side ('long' or 'short')
    stopLossPrice: Optional[float] = None  # Stop-loss level
    symbol: Optional[str] = None  # Trading pair (e.g. "BTC/USDT:USDT")
    takeProfitPrice: Optional[float] = None  # Take-profit level
    timestamp: Optional[int] = None  # Position creation time (UNIX timestamp)
    unrealizedPnl: Optional[float] = None  # Unrealized PnL (profit/loss)
    trade_history: Optional[List[TradeHistory]] = Field(default_factory=list,
                                                        description='Position trade information')

    class Config:
        arbitrary_types_allowed = True  # Allows complex types in the model

    def __str__(self):
        """Readable position representation"""
        return (f"Position {self.id} on {self.symbol}: {self.side.upper()} {self.contracts} SOL @ {self.entryPrice}")

    def set_bot_id(self, bot_id: Optional[str] = None):
        self.bot_id = bot_id

    def calculate_pnl(self, mark_price: float = 1, exchange_name: str = 'mexc'):
        """
        Calculates unrealized and realized PnL from the current asset price.

        :param mark_price: Current asset price
        :param exchange_name: Exchange name (used for calculation differences)
        """
        if not self.unrealizedPnl:
            entry_price = float(self.entryPrice)
            hold_vol = float(self.contracts)

            match exchange_name:
                case 'mexc':
                    match self.side:
                        case 'long':  # Long: PnL = (current price - entry price) * contracts
                            pnl = (mark_price - entry_price) * hold_vol
                        case 'short':  # Short: PnL = (entry price - current price) * contracts
                            pnl = (entry_price - mark_price) * hold_vol
                        case _:
                            pnl = 0
                    self.unrealizedPnl = pnl

        if not self.realizedPnl:
            match exchange_name:
                case 'mexc':
                    if 'realised' in self.info:
                        self.realizedPnl = self.info['realised']
        if not self.percentage:
            if not self.realizedPnl:
                self.realizedPnl = 0
            try:
                pnl = self.realizedPnl + self.unrealizedPnl
                self.percentage = round((pnl / self.initialMargin) * 100, 3)  # PnL in %
            except Exception as e:
                print(e)
                self.percentage = 0

    def update_from_dict(self, data: dict, exchange: str):
        match exchange.lower():
            # ─── kucoin / hyperliquid / bingx ───
            # CRITICAL: Without these added cases, after reduce_position or any partial state change,
            # self.contracts (and other fields) stay STALE for these exchanges. close_position then
            # sends the old amount and KuCoin/Hyperliquid reject with "Insufficient margin for order".
            case 'bybit' | 'binance' | 'okx' | 'kucoin' | 'kucoin futures' | 'kucoinfutures' | 'hyperliquid' | 'bingx':
                # Common update logic for all supported exchanges
                self.info = data.get('info', self.info)
                self.lastUpdateTimestamp = data.get('lastUpdateTimestamp', self.lastUpdateTimestamp)
                self.initialMargin = data.get('initialMargin', self.initialMargin)
                self.initialMarginPercentage = data.get('initialMarginPercentage', self.initialMarginPercentage)
                self.lastPrice = data.get('lastPrice', self.lastPrice)
                self.collateral = data.get('collateral', self.collateral)
                self.contractSize = data.get('contractSize', self.contractSize)
                self.contracts = data.get('contracts', self.contracts)
                self.entryPrice = data.get('entryPrice', self.entryPrice)
                self.hedged = data.get('hedged', self.hedged)
                self.leverage = data.get('leverage', self.leverage)
                self.liquidationPrice = data.get('liquidationPrice', self.liquidationPrice)
                self.maintenanceMargin = data.get('maintenanceMargin', self.maintenanceMargin)
                self.maintenanceMarginPercentage = data.get('maintenanceMarginPercentage',
                                                            self.maintenanceMarginPercentage)
                self.marginMode = data.get('marginMode', self.marginMode)
                self.marginRatio = data.get('marginRatio', self.marginRatio)
                self.markPrice = data.get('markPrice', self.markPrice)
                self.notional = data.get('notional', self.notional)
                self.percentage = data.get('percentage', self.percentage)
                # ccxt always sets the realizedPnl key (Bybit — from closedPnl, which a live
                # position lacks), and None wiped the accumulated number. Only number → None is forbidden.
                if data.get('realizedPnl') is not None:
                    self.realizedPnl = data['realizedPnl']
                self.side = data.get('side', self.side)
                self.stopLossPrice = data.get('stopLossPrice', self.stopLossPrice)
                self.symbol = data.get('symbol', self.symbol)
                self.takeProfitPrice = data.get('takeProfitPrice', self.takeProfitPrice)
                self.unrealizedPnl = data.get('unrealizedPnl', self.unrealizedPnl)

            case _:
                # For unknown exchanges, just store raw data
                self.info = data
