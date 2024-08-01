import asyncio
import threading
from typing import Any, Coroutine

from .api import API


class SocketAPI(API):
    def __init__(self):
        super().__init__()

    async def get_data(self, tickers, dt=60, keep_alive=None):
        while keep_alive():
            yield self.quote(tickers)
            await asyncio.sleep(dt)

    # since no actual websocket exists for api, simulate a websocket by querying every 1 minute
    def listen(self,
               tickers,
               callback,
               keep_alive,
               threaded=False,
               dt=60
               ) -> Coroutine[Any, Any, None] | threading.Thread:

        # use self.get_data, call callback with data
        async def run():
            async for data in self.get_data(tickers, dt, keep_alive):
                callback(data)

        # if threaded, create and return thread
        # else, return coroutine
        if threaded:
            # create thread and run await
            t = threading.Thread(target=lambda: asyncio.run(run()))
            return t
        else:
            return run()


class Badge:
    def __init__(self, countdown=100):
        self.countdown = countdown
        self.current_price = None
        self.previous_price = None

    def callback(self, data):
        # Extract the price from the data
        new_price = data.df['price'].values[0]

        # Save the current price as previous before updating
        if self.current_price is not None:
            self.previous_price = self.current_price

        # Update the current price
        self.current_price = new_price

    def keep_alive(self):
        if self.countdown > 0:
            self.countdown -= 1
            return True
        return False

    def show(self):
        if self.current_price is None:
            return "Price not available"

        if self.previous_price is None:
            return f"Current price: {self.current_price} ~"

        if self.current_price > self.previous_price:
            arrow = "↑"
        elif self.current_price < self.previous_price:
            arrow = "↓"
        else:
            arrow = "~"

        return f"Current price: {self.current_price} {arrow}"
