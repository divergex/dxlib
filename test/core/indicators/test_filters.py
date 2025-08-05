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

        indicator = Volatility(window=2, quantile=0.5)
        signals = indicator.get_signals(history)

        self.assertIsInstance(signals, History)
        self.assertEqual((4, 2), signals.data.shape)

    def test_reversion(self):
        from dxlib.core.indicators.filters import Reversion, zscore
        from dxlib.history import History

        history = History(self.schema, self.data)

        indicator = Reversion(upper=0.4, lower=-0.4, score=zscore(2, 4))
        signals = indicator.get_signals(history)

        self.assertIsInstance(signals, History)
        self.assertEqual(signals.data.shape, (2, 2))


if __name__ == "__main__":
    unittest.main()
