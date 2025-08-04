import math
import random
from typing import Protocol, Optional, Callable

import numpy as np

from dxlib.core import Instrument
from dxlib.market import OrderTransaction, Order, Size, SizeType
from dxlib.interfaces import TradingInterface


class FillModel(Protocol):
    def fill(self, order: Order, context: TradingInterface) -> Optional[OrderTransaction]:
        ...


class QuantityResolver:
    def resolve(self, order: Order, context: TradingInterface) -> float:
        price = context.market.quote(order.instrument)
        if isinstance(order.quantity, Size) and order.quantity.is_relative:
            if order.quantity.kind == SizeType.PercentOfEquity:
                equity = context.account.equity()
                return order.quantity.value * equity / price
            elif order.quantity.kind == SizeType.PercentOfPosition:
                raise NotImplementedError()
            else:
                raise TypeError(f"Unknown relative size kind {order.quantity.kind}")
        else:
            return float(order.quantity.value)


class ImmediateMarketFillModel:
    def fill(self, order: Order, context: TradingInterface) -> OrderTransaction:
        assert order.price is None, "Fill model does not accept non-market orders."
        quantity = QuantityResolver().resolve(order, context)
        executed_price = context.market.quote(order.instrument).item()
        return OrderTransaction(order, executed_price, quantity)


def exponential_decay(base_rate: float, decay_rate: float) -> Callable:
    def rate_func(order, context) -> float:
        price = context.market.quote(order.instrument).item()
        spread = abs(price - order.price)
        return base_rate * np.exp(-decay_rate * spread)
    return rate_func


class PoissonLimitFillModel:
    def __init__(self, dt: float, rate_func: Callable[[Order, TradingInterface], float]):
        self.dt = dt
        self.rate_func = rate_func

    def _fill_probability(self, dt: float, order: Order, context: TradingInterface) -> float:
        lam = self.rate_func(order, context)
        return 1 - np.exp(-lam * dt)

    def fill(self, order: Order, context: TradingInterface) -> Optional[OrderTransaction]:
        prob = self._fill_probability(self.dt, order, context)
        if random.random() < prob:
            return OrderTransaction(order, order.price, order.quantity)
        return None


class FillModelRegistry:
    def __init__(self, default: FillModel):
        self._by_instrument: dict[Instrument, FillModel] = {}
        self.default = default

    def register(self, instrument: Instrument, model: FillModel):
        self._by_instrument[instrument] = model

    def get(self, instrument: Instrument) -> FillModel:
        return self._by_instrument.get(instrument, self.default)
