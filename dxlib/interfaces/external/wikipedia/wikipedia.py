from typing import List, overload, Literal

import pandas as pd

from dxlib.core import Instrument


class Wikipedia:
    def __init__(self):
        self.client = None

    @property
    def base_url(self) -> str:
        return "https://en.wikipedia.org"

    @overload
    def sp500(self, is_instrument: Literal[False] = False) -> List[str]:
        ...

    @overload
    def sp500(self, is_instrument: Literal[True]) -> List[Instrument]:
        ...
    
    def sp500(self, is_instrument: bool = False) -> List[str] | List[Instrument]:
        url = f"{self.base_url}/wiki/List_of_S%26P_500_companies"
        symbols = pd.read_html(url)[0]["Symbol"].tolist()
        if not is_instrument:
            return symbols
        else:
            return [Instrument(symbol) for symbol in symbols]
