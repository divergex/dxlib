import unittest

from dxlib.interfaces.external import ibkr


class InteractiveBrokersApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = ibkr.InteractiveBrokersAPI()

    def test_get_historical(self):
        quote, response = self.api.quote("AAPL")
        print(quote)

    def test_send_order(self):
        order_id = self.api.next_id()
        order, response = self.api.send_order(order_id, "AAPL", "SELL", 100, "MKT")
        print(order)

    def test_cancel_order(self):
        order_id = 0
        response = self.api.cancel_order(order_id)
        print(response)


if __name__ == '__main__':
    unittest.main()
