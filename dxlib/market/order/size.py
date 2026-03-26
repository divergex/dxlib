from enum import Enum
from typing import Literal, Union


class SizeType(Enum):
    Absolute = "absolute"
    PercentOfEquity = "percent_of_equity"
    PercentOfPosition = "percent_of_position"

class Size:
    def __init__(self, value, kind: Union[Literal["absolute", "percent_of_equity", "percent_of_position"], SizeType]):
        self.value = value
        try:
            self.kind = SizeType(kind)
        except ValueError:
            raise ValueError(f"Invalid kind: {kind}. Must be one of {[e.value for e in SizeType]}")

    @property
    def is_relative(self):
        return self.kind != 'absolute'

    def __repr__(self):
        return f"<Size {self.kind}: {self.value}>"

    def __float__(self):
        return self.value
