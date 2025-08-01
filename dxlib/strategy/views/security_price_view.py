from dxlib.history import HistoryView, HistorySchema, History


class SecurityPriceView(HistoryView):
    def __init__(self, time_index = "time"):
        self.time_index = time_index

    @staticmethod
    def history_schema(history_schema: HistorySchema):
        return history_schema.copy()

    def len(self, history: History):
        # Unique timestamps
        return len(history.index(name=self.time_index).unique())

    def apply(self, history: History, function: callable, output_schema: HistorySchema = None):
        # Apply a function to each timestamp slice across instruments
        return history.get(columns=["price"]).apply({self.time_index: function}, output_schema=output_schema)

    def get(self, origin: History, idx):
        if isinstance(idx, int) and idx < 0:
            times = sorted(origin.index(name=self.time_index).unique())
            try:
                idx = times[idx]
            except IndexError:
                raise IndexError(f"idx {idx} out of range for time index with {len(times)} unique timestamps")
        return origin.get({self.time_index: [idx]}, ["price"])

    def iter(self, origin: History):
        for idx in origin.index(name=self.time_index):
            yield self.get(origin, idx)
