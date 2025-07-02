from abc import ABC, abstractmethod

from . import History


class HistoryView(ABC):
    @staticmethod
    @abstractmethod
    def len(history: History):
        pass

    @staticmethod
    @abstractmethod
    def apply(history: History, function: callable):
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