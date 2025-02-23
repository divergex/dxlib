import time
import unittest

from dxlib.interfaces import MockMarket


class TestMock(unittest.TestCase):
    def test_historical(self):
        api = MockMarket()

        print(api.historical())

