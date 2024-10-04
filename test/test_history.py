import json
import unittest

from dxlib import History, HistorySchema, Security
from test.benchmark import Benchmark
from test.mock_data import Mock


class TestSchema(unittest.TestCase):
    def test_create(self):
        schema = Mock.schema
        self.assertEqual(["security", "date"], schema.index_names)
        self.assertEqual(["open", "close"], schema.column_names)
        self.assertIs(schema.index["security"], str)

    def test_serialize(self):
        schema = Mock.schema
        data = schema.to_dict()
        schema2 = HistorySchema.from_dict(data)
        self.assertEqual(schema.index_names, schema2.index_names)
        self.assertEqual(schema.column_names, schema2.column_names)

    def test_to_json(self):
        schema = Mock.schema
        to_json = schema.__json__()
        expected_json = ('{"index": {"security": "str", "date": "Timestamp"}, '
                         '"columns": {"open": "float", "close": "float"}}')
        self.assertEqual(json.loads(to_json), json.loads(expected_json))


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
