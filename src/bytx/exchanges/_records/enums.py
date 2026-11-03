from enum import Enum


class OrderType(str, Enum):
    TAKE_PROFIT = 'TakeProfit'
    STOP_LOSS = 'StopLoss'
    LIMIT = 'Limit'
    MARKET = 'Market'


class SideEnum(str, Enum):
    BUY = "buy"
    SELL = "sell"
    SHORT = "short"
    LONG = "long"
