from dxlib import History
from dxlib.execution.history_view import HistoryView


class SecuritySignalView(HistoryView):
    @staticmethod
    def len(history: History):
        indices = history.index(name="date")
        return len(indices.unique())

    @staticmethod
    def apply(history: History, function: callable):
        return history.get(columns=["close"]).apply({"security": function})

    @staticmethod
    def get(origin: History, idx):
        return origin.get({"date": [idx]}, ["close"])

    @classmethod
    def iter(cls, origin: History):
        for idx in origin.index(name="date"):
            yield cls.get(origin, idx)
