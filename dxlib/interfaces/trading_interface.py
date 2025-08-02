from . import Interface, MarketInterface, AccountInterface, OrderInterface


class TradingInterface(Interface):
    def __init__(self,
                 market: MarketInterface,
                 account: AccountInterface,
                 order: OrderInterface,
                 *args, **kwargs):
        self.market = market
        self.account = account
        self.order = order
