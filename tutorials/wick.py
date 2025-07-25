import datetime
import itertools

import numpy as np
import pandas as pd

from dxlib import Executor, History, Portfolio, Instrument
from dxlib.interfaces import BacktestInterface
from dxlib.interfaces.external.yfinance import YFinance
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.order_generator.order_generator import OrderGenerator
from dxlib.strategy.views import SecuritySignalView
from dxlib.strategy.signal import SignalStrategy
from dxlib.data import Storage


def main():
    api = YFinance()
    api.start()

    symbols = ["AAPL", "MSFT", "PETR4.SA"]
    end = datetime.datetime(2025, 3, 1)
    start = datetime.datetime(2025, 1, 1)
    storage = Storage()
    store = "yfinance"

    def run_backtest(range_multiplier, close_multiplier):
        history = storage.cached(store, api.historical, History, symbols, start, end)
        history_view = SecuritySignalView()

        strat = SignalStrategy(WickReversal(range_multiplier=range_multiplier, close_multiplier=close_multiplier), OrderGenerator())
        portfolio = Portfolio({Instrument("USD"): 1000})
        interface = BacktestInterface(history, portfolio, history_view)
        executor = Executor(strat, interface)
        orders, portfolio = executor.run(history_view)
        value = portfolio.value(interface.price_history, "close")
        final_value = value.data.iloc[-1].item()
        return final_value

    range_multipliers = np.arange(0.4, 0.8, 0.05)  # range(0.1, 0.5, 0.05)
    close_multipliers = np.arange(0.1, 0.7, 0.1)

    results = []

    eq = [0, None, None]

    for rm, cm in itertools.product(range_multipliers, close_multipliers):
        rm = round(rm, 4)
        cm = round(cm, 4)
        if rm != eq[2]:
            eq = [0, None, None]
        if eq[0] >= 3:
            print(f"Skipping for {rm}, {cm} range")
            continue
        val = round(run_backtest(rm, cm), 4)
        results.append({'range_multiplier': rm, 'close_multiplier': cm, 'final_value': val})
        eq[2] = rm
        if val == eq[1]:
            eq[0] += 1
        else:
            eq[0] = 0
        eq[1] = val
        print(f"({rm}, {cm}) : {val}" + (f" ({eq[0]})" if eq[0] > 0 else ""))

    results_df = pd.DataFrame(results)

    best = results_df.sort_values('final_value', ascending=False).iloc[0]
    print("Best parameters:", best.to_dict())

if __name__ == "__main__":
    main()
