from dxlib import History
from dxlib.execution import HistoryView


class SecurityQuotes(HistoryView):
    @staticmethod
    def len(history: History):
        # Unique timestamps
        return len(history.index(name="timestamp").unique())

    @staticmethod
    def apply(history: History, function: callable):
        # Apply a function to each timestamp slice across instruments
        return history.get(columns=["bid", "ask"]).apply({"timestamp": function})

    @staticmethod
    def get(origin: History, idx):
        # Get all quotes at a specific timestamp
        return origin.get({"timestamp": [idx]}, ["bid", "ask"])

    @classmethod
    def iter(cls, origin: History):
        for idx in origin.index(name="timestamp"):
            yield cls.get(origin, idx)
