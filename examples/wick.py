import datetime

from dxlib import Executor, History, Portfolio, Instrument
from dxlib.data import Storage
from dxlib.interfaces import BacktestInterface
from dxlib.interfaces.external.yfinance import YFinance
from dxlib.strategy.signal.custom.wick_reversal import WickReversal
from dxlib.strategy.order_generator.order_generator import OrderGenerator
from dxlib.strategy.views import SecuritySignalView
from dxlib.strategy.signal import SignalStrategy


def main():
    api = YFinance()
    api.start()

    symbols = ["AAPL", "MSFT", "PETR4.SA"]
    end = datetime.datetime(2025, 3, 1)
    start = datetime.datetime(2025, 1, 1)
    storage = Storage()
    store = "yfinance"

    strat = SignalStrategy(WickReversal(range_multiplier=0.4, close_multiplier=0.7), OrderGenerator())
    view = SecuritySignalView(time_index="datetime")

    def run_backtest():
        history = storage.cached(store, History, api.historical, symbols, start, end)

        portfolio = Portfolio({Instrument("USD"): 1000})
        interface = BacktestInterface(history, view, portfolio)
        executor = Executor(strat, interface)
        orders, portfolio_history = executor.run(view, interface.iter())
        value = portfolio_history.value(interface.market.price_history.data)
        final_value = value.data.iloc[-1].item()
        return final_value

    print("PnL", run_backtest())

if __name__ == "__main__":
    main()
