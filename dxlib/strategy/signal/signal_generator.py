from abc import abstractmethod, ABC

import pandas as pd

from dxlib import History


class SignalGenerator(ABC):
    @abstractmethod
    def generate(self, data: pd.DataFrame):
        pass

    @abstractmethod
    def output_schema(self, history: History):
        pass