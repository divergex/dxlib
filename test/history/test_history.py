import json
import unittest
from datetime import datetime
from numbers import Number

import pandas as pd

from dxlib import History, HistorySchema

from data import Mock


class TestSchema(unittest.TestCase):
    def test_create(self):
        schema = Mock.schema()
        self.assertEqual({"instruments": str, "date": pd.Timestamp}, schema.index)
        self.assertEqual({"open": float, "close": float}, schema.columns)
        self.assertIs(schema.index["instruments"], str)


class TestHistory(unittest.TestCase):
    def test_create(self):
        h = History(Mock.schema(), Mock.tight_data())
        self.assertEqual(["instruments", "date"], h.indices)
        self.assertEqual(["open"], h.columns)
        self.assertEqual(Mock().stocks, h.levels("instruments"))

    def test_concat(self):
        h = History(Mock.schema(), Mock.tight_data())
        h2 = History(Mock.schema(), Mock.small_data())

        h.concat(h2)
        self.assertEqual(8, len(h.data))
        self.assertEqual(1, len(h.data.columns))
        self.assertEqual(2, len(h.data.index.names))

    def test_extend(self):  # Expand columns and ignore repeated columns
        h = History(Mock.schema(), Mock.tight_data())
        h2 = History(Mock.large_schema(), Mock.large_data())

        h.extend(h2)
        self.assertEqual(17, len(h.data))
        self.assertEqual(2, len(h.data.columns))
        self.assertEqual(2, len(h.data.index.names))

    def test_get(self):
        h = History(Mock.large_schema(), Mock.large_data())

        h2 = h.get(index={"instruments": ["FB", "AMZN"]})
        self.assertEqual(6, len(h2.data))

    def test_get_range(self):
        date_range = {"date": slice(None)}

        h = History(Mock.large_schema(), Mock.large_data())
        h2 = h.get(index=date_range)
        self.assertEqual(17, len(h2.data))

    def test_apply(self):
        h = History(Mock.large_schema(), Mock.large_data())

        result = h.apply({"instruments": lambda df: df.pct_change()})

        print(result.data.sort_index().dropna())

class TestOperate(unittest.TestCase):
    @staticmethod
    def multiply_operation(df, quantity):
        return df * quantity  # Multiply the specified columns by the quantity

    @staticmethod
    # Custom operation function for addition
    def add_operation(df, quantity):
        return df + quantity

    def setUp(self):
        # Set up common test data
        df1 = pd.DataFrame({
            'close': [110, 105, 220],
            'open': [100, 110, 200]
        }, index=pd.MultiIndex.from_tuples([
            ('date1', 'sec1'),
            ('date2', 'sec1'),
            ('date2', 'sec2')
        ], names=['date', 'instruments']))

        df2 = pd.DataFrame({
            'quantity': [50, 75],
        }, index=pd.Index(['sec1', 'sec2'], name='instruments'))

        schema1 = HistorySchema(
            index={"date": str, "instruments": str},
            columns={"open": Number, "close": Number},
        )
        schema2 = HistorySchema(
            index={"instruments": str},
            columns={"quantity": Number},
        )

        self.history1 = History(schema1, df1)
        self.history2 = History(schema2, df2)

    def test_basic_multiplication(self):
        result = self.history1.on(self.history2, columns=['close', 'open'], other_columns=['quantity'],
                                       operation=self.multiply_operation)
        expected = pd.DataFrame({
            'close': [5500, 5250, 16500],
            'open': [5000, 5500, 15000]
        }, index=pd.MultiIndex.from_tuples([
            ('date1', 'sec1'),
            ('date2', 'sec1'),
            ('date2', 'sec2')
        ], names=['date', 'instruments']))

        pd.testing.assert_frame_equal(result.data, expected)

        expected_valued = pd.DataFrame({
            'close': [5500, 5250 + 16500],
            'open': [5000, 5500 + 15000]
        }, index=pd.Index(['date1', 'date2'], name='date'))

        pd.testing.assert_frame_equal(result.apply({("date",): lambda x: x.sum(axis=0)}).data, expected_valued)

    def test_basic_addition(self):
        result = self.history1.on(self.history2, columns=['close', 'open'], other_columns=['quantity'],
                                       operation=self.add_operation).data
        expected = pd.DataFrame({
            'close': [160, 155, 295],
            'open': [150, 160, 275]
        }, index=pd.MultiIndex.from_tuples([
            ('date1', 'sec1'),
            ('date2', 'sec1'),
            ('date2', 'sec2')
        ], names=['date', 'instruments']))

        pd.testing.assert_frame_equal(result, expected)

if __name__ == "__main__":
    unittest.main()
