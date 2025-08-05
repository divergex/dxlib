import unittest

from test.data import Mock


class TestIndicatorFilters(unittest.TestCase):
    def setUp(self):
        self.mock = Mock()
        self.data = self.mock.large_data()
        self.schema = self.mock.large_schema()


    def test_volatility(self):
        from dxlib.core.indicators.filters import Volatility
        from dxlib.history import History

        history = History(self.schema, self.data)

        volatility_indicator = Volatility(window=3, quantile=0.5)
        signals = volatility_indicator.get_signals(history)

        self.assertIsInstance(signals, History)
        self.assertEqual(signals.data.shape, (10, 2))
