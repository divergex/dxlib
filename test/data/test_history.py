import unittest

from dxlib import History
from dxlib.data import Registry
from test.data import Mock


class TestHistoryDto(unittest.TestCase):
    def setUp(self):
        pass

    def test_history(self):
        data = Mock.large_data()
        schema = Mock.large_schema()

        history = History(schema, data)
        print(history)

        dto = Registry.from_domain(history)
        print(dto)
        json_data = dto.model_dump_json()
        print(json_data)

        dto = Registry.get(history).model_validate_json(json_data)
        print(dto)
        print(dto.to_domain())
