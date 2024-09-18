from typing import Dict

import numpy as np

from dxlib.security import Security


class Portfolio:
    """
    A portfolio is a term used to describe a collection of investments held by an individual or institution.
    Such investments include but are not limited to stocks, bonds, commodities, and cash.

    A portfolio in the context of this library is a collection of positions, that is, the number of each security held.
    """
    def __init__(self):
        self.position: Dict[Security, float] = {}

    def value(self):
        """
        Calculate the total value of the portfolio.
        """
        return np.sum([position.value for position in self.position])
