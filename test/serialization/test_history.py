import unittest

from dxlib import History
from dxlib.data.dtos.history_dto import HistoryDto
from test.mock_data import Mock


class TestHistoryDto(unittest.TestCase):
    def setUp(self):
        pass

    def test_history(self):
        data = Mock.large_data()
        schema = Mock.schema()

        history = History(schema, data)
        print(history)

        dto = HistoryDto.from_domain(history)
        print(dto)
        json_data = dto.model_dump_json()
        print(json_data)

        dto = HistoryDto.model_validate_json(json_data)
        print(dto)

        print(dto.to_domain())
