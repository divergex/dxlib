from datetime import datetime

import dxlib as dx
from dxlib import History
from dxlib.interfaces import InvestingCom


api = InvestingCom()

historical = api.market_interface.history(["AAPL"],
                                          datetime(2024, 9, 1),
                                          datetime(2024, 9, 10)
                                          )

print(historical)


aapl = dx.Security("AAPL")
portfolio = dx.Portfolio({aapl: 10})

print(portfolio.value(historical.get(columns=["close"])))

