from dxlib import History, HistorySchema
from dxlib.strategy import HistoryView


class SecurityQuotes(HistoryView):
    @staticmethod
    def history_schema(history_schema: HistorySchema):
        return history_schema.copy()

    @staticmethod
    def len(history: History):
        # Unique timestamps
        return len(history.index(name="datetime").unique())

    @staticmethod
    def apply(history: History, function: callable, output_schema: HistorySchema = None):
        # Apply a function to each timestamp slice across instruments
        return history.get(columns=["bid", "ask"]).apply({"datetime": function}, output_schema=output_schema)

    @staticmethod
    def get(origin: History, idx):
        # Get all quotes at a specific timestamp
        return origin.get({"datetime": [idx]}, ["bid", "ask"])

    @classmethod
    def iter(cls, origin: History):
        for idx in origin.index(name="datetime"):
            yield cls.get(origin, idx)
