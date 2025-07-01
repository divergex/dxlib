from .interface import Interface


class AccountInterface(Interface):
    def portfolio(self, *args, **kwargs) -> float:
        """
        Get the current position of the security.
        """
        raise NotImplementedError

    def equity(self, *args, **kwargs) -> float:
        raise NotImplementedError