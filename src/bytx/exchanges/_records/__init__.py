"""SDK data models: order, position, trades.

An application may pass models from its own copy (same field set), so an order
is recognized by shape (:func:`is_order_model`), not by class.
"""
from bytx.exchanges._records.enums import OrderType, SideEnum
from bytx.exchanges._records.errors import OrderValidationError
from bytx.exchanges._records.order import OrderModel
from bytx.exchanges._records.position import OrderFee, PositionHistoryStats, PositionModel, TradeHistory

# Fields that identify an order model regardless of the package declaring the class.
_ORDER_MODEL_FIELDS = frozenset({'id', 'status', 'clientOrderId', 'order_type', 'reduceOnly', 'triggerPrice'})


def is_order_model(obj: object) -> bool:
    """SDK OrderModel or a copy of it from another package.

    A Pydantic model with order fields; dict and others — False (they have their own parsing branch).
    """
    if isinstance(obj, OrderModel):
        return True
    fields = getattr(type(obj), 'model_fields', None)
    return isinstance(fields, dict) and _ORDER_MODEL_FIELDS <= fields.keys()


__all__ = [
    'OrderType',
    'SideEnum',
    'OrderValidationError',
    'OrderModel',
    'OrderFee',
    'PositionHistoryStats',
    'PositionModel',
    'TradeHistory',
    'is_order_model',
]
