from dxlib.core import Portfolio
from .interface import Interface


class AccountInterface(Interface):
    def portfolio(self, *args, **kwargs) -> Portfolio:
        """
        Get the current position of the instruments.
        """
        raise NotImplementedError

    def equity(self, *args, **kwargs) -> float:
        raise NotImplementedError