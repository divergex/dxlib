# import unittest
#
# import pandas as pd
#
# from dxlib.storage import Registry, Serializable, Cache
# from test.benchmark import Benchmark
#
#
# class TestSerializer(unittest.TestCase):
#     def setUp(self):
#         pass
#
#     def test_registry(self):
#         class SampleData(metaclass=RegistryBase):
#             def __init__(self, value):
#                 self.value = value
#
#         sample_data = SampleData(1)
#
#         self.assertEqual(RegistryBase.get("SampleData"), SampleData)
#         self.assertEqual(RegistryBase.get("SampleData")(1).value, sample_data.value)
#
#     def test_serializable(self):
#         class SampleData(Serializable):
#             def __init__(self, value, date: pd.Timestamp = None):
#                 self.value = value
#                 self.date = date
#
#             def to_dict(self):
#                 return {"value": self.value, "date": self.date}
#
#             @classmethod
#             def from_dict(cls, data):
#                 return cls(data["value"], data["date"])
#
#         sample_data = SampleData(1)
#         serialized = sample_data.serialize()
#
#         self.assertEqual(SampleData.deserialize(serialized).value, sample_data.value)
#
#     def test_registered_serializable(self):
#         class CustomField(metaclass=RegistryBase):
#             def __init__(self, value):
#                 self.value = value
#
#         class SampleData(Serializable):
#             def __init__(self, field_type, date_type):
#                 self.field_type = field_type
#                 self.date_type = date_type
#
#             def to_dict(self):
#                 return {"field_type": self.field_type.__name__}
#
#             @classmethod
#             def from_dict(cls, data):
#                 return cls(RegistryBase.get(data["field_type"]), RegistryBase.get("pd.Timestamp"))
#
#         sample_data = SampleData(CustomField, pd.Timestamp)
#
#         self.assertEqual(SampleData.from_dict(sample_data.to_dict()).field_type, CustomField)
#
#     def test_complex_serializable(self):
#         class ComplexKey(Serializable, metaclass=RegistryBase):
#             def __init__(self, value):
#                 self.value = value
#                 self.additional = 3
#
#             def op(self):
#                 return self.value + self.additional
#
#             def to_dict(self):
#                 return {"value": self.value, "additional": self.additional}
#
#         class ComplexDf(Serializable, metaclass=RegistryBase):
#             def __init__(self, key: ComplexKey, value):
#                 self.key = key
#                 self.value = value
#
#             def to_dict(self):
#                 return {self.key: self.value}
#
#             @classmethod
#             def from_dict(cls, data):
#                 return cls(RegistryBase.get(data["field_type"]), RegistryBase.get("pd.Timestamp"))
#
#         key = ComplexKey(2)
#         df = ComplexDf(key, 2)
#
#         serialized = df.serialize()
#
#
#
# class TestCache(unittest.TestCase):
#
#     def setUp(self):
#         """Set up a Cache instance for testing."""
#         self.cache = Cache()
#
#         # Create a sample DataFrame
#         self.data = pd.DataFrame({
#             'date': pd.date_range('2021-01-01', periods=10),
#             'symbol': 'AAPL',
#             'close': [100, 105, 110, 115, 120, 125, 130, 135, 140, 145],
#             'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
#         })
#
#         self.data.set_index(['date', 'symbol'], inplace=True)
#
#         # Create new data for extending the cache
#         self.new_data = pd.DataFrame({
#             'date': pd.date_range('2021-01-11', periods=5),
#             'symbol': 'AAPL',
#             'close': [150, 155, 160, 165, 170],
#             'volume': [2000, 2100, 2200, 2300, 2400]
#         })
#
#         self.new_data.set_index(['date', 'symbol'], inplace=True)
#
#     @classmethod
#     def tearDownClass(cls):
#         # remove the cache file
#         cache = Cache()
#         cache.remove_cache()
#
#     def test_cache_store_and_exists(self):
#         """Test storing data in cache and checking existence."""
#         self.cache.store('AAPL', self.data)
#         self.assertTrue(self.cache.exists('AAPL'))
#
#     def test_cache_load(self):
#         """Test loading data from cache."""
#         self.cache.store('AAPL', self.data)
#         loaded_data = self.cache.load('AAPL')
#         self.assertTrue(loaded_data.equals(self.data))
#
#     def test_cache_extend(self):
#         """Test extending cached data."""
#         self.cache.store('AAPL', self.data)
#         self.cache.extend('AAPL', self.new_data)
#
#         # Load the extended data
#         extended_data = self.cache.load('AAPL')
#
#         # Combine the original and new data for comparison
#
#         combined_data = pd.concat([self.data, self.new_data])
#         self.assertTrue(extended_data.equals(combined_data))
#
#     def test_decorator(self):
#         @Benchmark.timeit
#         @self.cache.cache('AAPL')
#         def get_data():
#             return self.data
#
#         get_data()
#
#         cached = get_data()  # timeit should take less time since data is not written to file after first call
#
#         self.assertTrue(cached.equals(self.data))
