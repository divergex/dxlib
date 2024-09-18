from .interface import Interface


class MarketInterface(Interface):
    def quote(self) -> float:
        """
        Get the current price of the security.
        """
        raise NotImplementedError

    def bar(self) -> float:
        """
        Get the current price of the security.
        """
        raise NotImplementedError
