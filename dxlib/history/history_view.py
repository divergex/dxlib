from abc import ABC, abstractmethod

from . import History, HistorySchema


class HistoryView(ABC):
    @staticmethod
    @abstractmethod
    def len(history: History):
        pass

    @staticmethod
    @abstractmethod
    def apply(history: History, function: callable, output_schema: HistorySchema = None):
        pass

    @staticmethod
    @abstractmethod
    def get(origin: History, idx):
        pass

    @staticmethod
    @abstractmethod
    def iter(origin: History):
        pass

    @staticmethod
    def price(origin: History, idx: int):
        pass

    @staticmethod
    def slice(origin: History, size: int):
        pass

    @staticmethod
    @abstractmethod
    def history_schema(history_schema: HistorySchema):
        pass