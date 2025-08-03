from dataclasses import dataclass

from dxlib.core import Portfolio
from dxlib.interfaces import TradingInterface
from ..strategy import StrategyContext


@dataclass(slots=True)
class PortfolioContext(StrategyContext):
    portfolio: Portfolio

    @classmethod
    def from_interface(cls, interface: TradingInterface):
        context = cls(
            portfolio=interface.account.portfolio()
        )
        return context

    @classmethod
    def build(cls, interface: TradingInterface) -> "PortfolioContext":
        return cls(portfolio=interface.account.portfolio())

    @classmethod
    def bind(cls, interface: TradingInterface):
        return lambda observation, history, history_view: cls.build(interface)