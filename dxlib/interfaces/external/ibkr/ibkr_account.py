from ibapi.client import EClient

from .wrapper import IbkrWrapper
from ...account_interface import AccountInterface


class IbkrAccount(AccountInterface):
    def __init__(self, client: EClient):
        self.client = client
        self.wrapper: IbkrWrapper = client.wrapper

    def position(self):
        self.client.reqPositions()

        while not self.wrapper.get_end("position"):
            pass

        return self.wrapper.get_data("account")

    def accounts(self):
        self.client.reqManagedAccts()

        while not self.wrapper.get_end("account"):
            pass

        return self.wrapper.get_data("account")

    def cancel(self):
        self.client.cancelPositions()
