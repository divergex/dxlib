from abc import abstractmethod, ABC

import pandas as pd

from dxlib import History, HistorySchema


class SignalGenerator(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame):
        pass

    def output_schema(self, history_schema: HistorySchema):
        return history_schema
