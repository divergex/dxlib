from . import Interface, MarketInterface, AccountInterface, OrderInterface


class TradingInterface(Interface):
    def __init__(self,
                 market_interface: MarketInterface = None,
                 account_interface: AccountInterface = None,
                 order_interface: OrderInterface = None,
                 *args, **kwargs):
        self.market_interface = market_interface
        self.account_interface = account_interface
        self.order_interface = order_interface

    def __getattr__(self, name):
        # Fallback to subinterfaces
        for component in (self.market_interface, self.account_interface, self.order_interface):
            if hasattr(component, name):
                return getattr(component, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
