import unittest

from dxlib.interfaces import ibkr


class InteractiveBrokersApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api = ibkr.InteractiveBrokersApi()

    def connect(self):
        self.api.connect()

    def test_connect(self):
        self.connect()
        self.assertTrue(self.api.connected)
        self.api.disconnect()

    def test_get_historical(self):
        self.connect()
        quote = self.api.historical("AAPL")
        self.api.disconnect()


if __name__ == '__main__':
    unittest.main()
