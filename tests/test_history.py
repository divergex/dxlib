import unittest

from dxlib import History
from benchmark import Benchmark
from tests.mock_data import Mock


class TestHistory(unittest.TestCase):
    def test_create(self):
        h = History(Mock.schema, Mock.tight_data)
        self.assertEqual(["security", "date"], h.data.index.names)
        self.assertEqual([""], h.data.columns.names)
        self.assertEqual(Mock.stocks, h.levels("security"))

    @Benchmark.timeit
    def test_add(self):
        h = History(Mock.schema, Mock.tight_data)
        h2 = History(Mock.schema, Mock.small_data)

        h.add(h2)
        self.assertEqual(8, len(h.data))
        self.assertEqual(1, len(h.data.columns))
        self.assertEqual(2, len(h.data.index.names))

    @Benchmark.timeit
    def test_extend(self):  # Expand columns and ignore repeated columns
        h = History(Mock.schema, Mock.tight_data)
        h2 = History(Mock.schema, Mock.large_data)

        h.extend(h2)
        self.assertEqual(17, len(h.data))
        self.assertEqual(2, len(h.data.columns))
        self.assertEqual(2, len(h.data.index.names))

    @Benchmark.timeit
    def test_get(self):
        h = History(Mock.schema, Mock.large_data)

        h2 = h.get(index={"security": ["FB", "AMZN"]})
        self.assertEqual(6, len(h2.data))

    @Benchmark.timeit
    def test_get_range(self):
        date_range = {"date": slice("2021-01-01", "2021-01-03")}

        h = History(Mock.schema, Mock.large_data)
        h2 = h.get(index=date_range)
        self.assertEqual(6, len(h2.data))


if __name__ == '__main__':
    unittest.main()
