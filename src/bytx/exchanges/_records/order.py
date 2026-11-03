from typing import Optional, List, Dict, Any
from pydantic import BaseModel, model_validator
from bytx.exchanges._records import OrderType

# Bybit info.stopOrderType -> protective order type
_BYBIT_SL_STOP_ORDER_TYPES = {'StopLoss', 'PartialStopLoss', 'TrailingStop'}
_BYBIT_TP_STOP_ORDER_TYPES = {'TakeProfit', 'PartialTakeProfit'}
_CLOSE_SL_TYPES = {'stop', 'stop_limit'}
_CLOSE_TP_TYPES = {'take_profit', 'take_profit_limit'}


def _classify_conditional(model: 'OrderModel') -> Optional[OrderType]:
    """Classifies a conditional order (triggerPrice) without SL/TP prices.

    Order: explicit consumer classification (STOP_LOSS/TAKE_PROFIT only),
    Bybit info.stopOrderType, then type stop/take_profit with a closing flag
    (reduceOnly / closeOnTrigger / closePosition). Otherwise None: a conditional order
    must not be treated as MARKET/LIMIT (it is a triggered entry/exit, not a filled
    market order), nor guessed as SL/TP — a real stop could get canceled.
    """
    # (a) explicit classification by the caller:
    # o.order_type = STOP_LOSS, or a round-trip through model_dump.
    if 'order_type' in model.model_fields_set:
        explicit = model.__dict__.get('order_type')
        if isinstance(explicit, str) and not isinstance(explicit, OrderType):
            try:
                explicit = OrderType(explicit)
            except ValueError:
                explicit = None
        # MARKET/LIMIT are ignored: a stale 'Market' from Mongo and the consumers'
        # default value would mask a conditional order.
        if explicit in (OrderType.STOP_LOSS, OrderType.TAKE_PROFIT):
            return explicit

    info = model.info if isinstance(model.info, dict) else {}

    # (b) Bybit
    stop_order_type = info.get('stopOrderType')
    if stop_order_type in _BYBIT_SL_STOP_ORDER_TYPES:
        return OrderType.STOP_LOSS
    if stop_order_type in _BYBIT_TP_STOP_ORDER_TYPES:
        return OrderType.TAKE_PROFIT

    # (c) stop/take_profit that close the position
    is_close = (
        bool(model.reduceOnly)
        or str(info.get('closeOnTrigger', '')).lower() == 'true'
        or str(info.get('closePosition', '')).lower() == 'true'
    )
    if is_close:
        if model.type in _CLOSE_SL_TYPES:
            return OrderType.STOP_LOSS
        if model.type in _CLOSE_TP_TYPES:
            return OrderType.TAKE_PROFIT

    return None


class OrderModel(BaseModel):
    bot_id: Optional[str] = None
    id: Optional[str] = None
    symbol: Optional[str] = None
    type: Optional[str] = None
    side: Optional[str] = None
    price: Optional[float] = None
    amount: Optional[float] = None
    filled: Optional[float] = None
    remaining: Optional[float] = None
    cost: Optional[float] = None
    average: Optional[float] = None
    status: Optional[str] = None
    timestamp: Optional[int] = None
    datetime: Optional[str] = None
    lastTradeTimestamp: Optional[int] = None
    lastUpdateTimestamp: Optional[int] = None
    reduceOnly: Optional[bool] = None
    postOnly: Optional[bool] = None
    triggerPrice: Optional[float] = None
    stopLossPrice: Optional[float] = None
    takeProfitPrice: Optional[float] = None
    timeInForce: Optional[str] = None
    fee: Optional[Dict[str, Any]] = None
    fees: Optional[List[Dict[str, Any]]] = None
    trades: Optional[List[Dict[str, Any]]] = None
    info: Optional[Dict[str, Any]] = None
    clientOrderId: Optional[str] = None
    order_type: Optional[OrderType] = None
    position_id: Optional[str] = None

    @model_validator(mode="after")
    def set_order_type(cls, model: 'OrderModel') -> 'OrderModel':
        """
        Auto-detects the order type (MARKET, LIMIT, SL, TP)
        after the model is fully initialized.
        """
        # Use __dict__ for direct field access
        # to avoid a recursive validator call

        # A plain limit order (no trigger, not reduceOnly) with attached
        # SL/TP (ccxt OKX/Bybit fill its stopLossPrice/takeProfitPrice) is
        # an entry LIMIT, not a protective order. It used to be classified as
        # STOP_LOSS, and a consumer could mistake the entry for an SL (cancel/move it).
        is_plain_limit = (
            model.type == "limit"
            and model.triggerPrice is None
            and not model.reduceOnly
        )

        if is_plain_limit:
            model.__dict__["order_type"] = OrderType.LIMIT
        elif model.stopLossPrice is not None or model.type == "stop_market":
            model.__dict__["order_type"] = OrderType.STOP_LOSS
        elif model.takeProfitPrice is not None or model.type == "take_profit_market":
            model.__dict__["order_type"] = OrderType.TAKE_PROFIT
        elif model.triggerPrice is not None:
            # Conditional order without SL/TP prices (#45): not MARKET/LIMIT. SL/TP —
            # only on explicit flags, otherwise None (see _classify_conditional).
            model.__dict__["order_type"] = _classify_conditional(model)
        elif model.type == "market":
            model.__dict__["order_type"] = OrderType.MARKET
        elif model.type == "limit":
            model.__dict__["order_type"] = OrderType.LIMIT
        else:
            model.__dict__["order_type"] = None

        # Set prices from triggerPrice
        if model.__dict__["order_type"] == OrderType.STOP_LOSS:
            if model.stopLossPrice is None and model.triggerPrice is not None:
                model.__dict__["stopLossPrice"] = model.triggerPrice

        if model.__dict__["order_type"] == OrderType.TAKE_PROFIT:
            if model.takeProfitPrice is None and model.triggerPrice is not None:
                model.__dict__["takeProfitPrice"] = model.triggerPrice
        return model

    class Config:
        title = "Order Model"
        description = "Trading order data model."
        validate_assignment = True
        use_enum_values = True
        arbitrary_types_allowed = True
        populate_by_name = True
        json_encoders = {
            OrderType: lambda v: v.value,
        }

    def __str__(self):
        return (
            f"Order {self.id}: {self.side.upper() if self.side else 'UNKNOWN'} {self.amount} {self.symbol} @ {self.price}\n"
            f"Status: {self.status}, Filled: {self.filled}/{self.amount}\n"
            f"Average price: {self.average}, Fee: {self.fee}, Time: {self.datetime}\n"
            f"Order type: {self.order_type.value if self.order_type is not None else 'Unknown'}"
        )

    @property
    def is_conditional(self) -> bool:
        """Conditional (trigger) order. Not a model field — excluded from model_dump."""
        return self.triggerPrice is not None

    def set_bot_id(self, bot_id: Optional[str] = None):
        self.bot_id = bot_id