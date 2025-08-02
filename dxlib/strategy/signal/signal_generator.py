from abc import abstractmethod, ABC

import pandas as pd

from dxlib.history import HistorySchema


class SignalGenerator(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame, history_schema: HistorySchema):
        pass

    def output_schema(self, history_schema: HistorySchema):
        return history_schema
