from dxlib import History
from dxlib.data.dtos.history_dto import HistoryDto
from dxlib.data import Storage
from test.utils import Mock

def main():
    mock = Mock()

    history = History(mock.schema, mock.large_data)
    print(history)

    dto = HistoryDto.from_domain(history)
    print(dto)
    dto.model_dump_json()

    storage = Storage()
    store = "dtos"

    dto = storage.cached(store, HistoryDto.from_domain, HistoryDto, history)
    print(dto)

    print(dto.to_domain())


if __name__ == "__main__":
    main()