# access cboe book
import time

import threading
import numpy as np
import requests
from fastapi import FastAPI
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel

from dxlib.core.book.book import OrderBook
from dxlib.core.formatter import Formatter
from dxlib.core.strategy.strategy import Strategy


def get_cboe_book(symbol: str):
    url = f"https://www.cboe.com/json/edgx/book/{symbol}"
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.7",
        "priority": "u=1, i",
        "referer": f"https://www.cboe.com/us/equities/market_statistics/book/{symbol}/?mkt=edgx",
        "sec-ch-ua": '"Brave";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "user-agent": 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }

    response = requests.get(url, headers=headers)
    return response.json()


# format = {'reload': 5500, 'data': {'symbol': 'MSFT', 'timestamp': '20:00:39 US/Eastern', 'company': 'MICROSOFT CORP COM', 'volume': 830843, 'market': 1899774, 'asks': [], 'bids': [], 'tick_type': '', 'prev': 418.79, 'high': 429.05, 'low': 416.75, 'status': 'Closed', 'trades': [['19:59:11', 1, 426.39], ['19:19:55', 1, 426.28], ['18:55:05', 5, 426.0], ['18:54:30', 50, 426.08], ['18:46:21', 1, 426.2], ['18:46:21', 13, 426.17], ['18:30:17', 10, 425.75], ['18:14:54', 23, 426.2], ['18:14:54', 2, 426.2], ['18:14:54', 25, 426.2]], 'hrname': ''}, 'success': True, 'statusText': '', 'statusCode': '200', 'status': '200: '}


def format_cboe_book(data = None) -> OrderBook:
    book = OrderBook()
    if data:
        asks = data["data"]["asks"]  # list of [price, size]
        bids = data["data"]["bids"]  # list of [price, size]
        book.asks = {price: size for price, size in asks}
        book.bids = {price: size for price, size in bids}

    return book


class AvStoikov(Strategy):
    def __init__(self):
        self.market_impact = 0.1
        self.risk_aversion = 1.0

        self.volatility = None
        self.liquidity = None

        self.prices = []
        self.dt = 1

    def midprice(self, lob: OrderBook) -> float:
        best_bid = max(lob.bids.keys())
        best_ask = min(lob.asks.keys())
        return (best_bid + best_ask) / 2

    def contains(self, quotes: np.ndarray):
        assert len(quotes) == 2
        assert quotes[0] < quotes[1]

    def execute(self, lob: OrderBook, *args, **kwargs) -> np.ndarray:
        self.prices.append(self.midprice(lob))

        if len(self.prices) > 100:
            self.prices.pop(0)

        if not self.volatility:
            self.volatility = np.std(self.prices)

        midprice = self.midprice(lob)

        optimal_spread = self.risk_aversion * np.power(self.volatility, 2) * self.dt
        optimal_spread += 2 / self.market_impact * np.log(1 + self.risk_aversion / self.market_impact)

        quotes = np.array([midprice - optimal_spread / 2, midprice + optimal_spread / 2])
        self.contains(quotes)

        return quotes


def set_random(book: OrderBook, tick_size=2):
    book.asks = {i: np.random.randint(1, 10) for i in np.random.uniform(400, 500, 5)}
    book.bids = {i: np.random.randint(1, 10) for i in np.random.uniform(400, 500, 5)}

    # round
    book.asks = {round(price, tick_size): size for price, size in book.asks.items()}
    book.bids = {round(price, tick_size): size for price, size in book.bids.items()}

class Executor():
    def __init__(self, draw=False):
        self.symbol = "MSFT"
        self.formatter = Formatter()
        self.strategy = AvStoikov()

        self.book = OrderBook()
        self.quotes = np.array([0, 0])

        self.draw = draw

        self.running = threading.Event()
        self.thread = None

    def start(self):
        self.running.set()

        self.thread = threading.Thread(target=self._start)
        self.thread.start()

        return self.thread

    def stop(self):
        self.running.clear()
        self.thread.join()

    def _start(self):
        symbol = "MSFT"

        formatter = Formatter()
        strategy = AvStoikov()

        with Live(refresh_per_second=1) as live:
            while self.running.is_set():
                # data = get_cboe_book(symbol)
                self.book = format_cboe_book()

                if not self.book.asks or not self.book.bids:
                    set_random(self.book)

                table, chart = formatter.order_book(self.book, show_bars=True)
                self.quotes = strategy.execute(self.book)
                strategy.contains(self.quotes)

                quotes_str = str(self.quotes)
                layout = Layout()
                layout.split_column(
                    Layout(Panel(quotes_str, title="Quotes")),
                    Layout(Panel(table, title="Order Book")),
                    Layout(Panel(chart, title="Bars"))
                )

                if self.draw:
                    live.update(layout)

                time.sleep(1)


app = FastAPI()
executor = Executor()


@app.get("/book")
def get_book():
    return executor.book


@app.get("/quotes")
def get_quotes():
    return executor.quotes.tolist()


if __name__ == "__main__":
    import uvicorn
    executor.start()
    uvicorn.run(app, host="0.0.0.0", port=8000)
    executor.stop()
