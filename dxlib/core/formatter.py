from rich.console import Console
from rich.table import Table

from dxlib.core.book import OrderBook


class Formatter:
    def __init__(self, console: Console = None):
        # Initialize the rich console
        self.console = console or Console()

    def order_book(self, book: OrderBook, show_bars: bool = False):
        """
        Print the order book in a pretty format.
        Optionally show bars to represent quantities.
        """
        # Create a table to show the order book
        table = Table(title="Order Book")
        table.add_column("Price", justify="right", style="cyan")
        table.add_column("Size", justify="right", style="magenta")

        # Add bids to the table
        for price, qty in sorted(book.bids.items(), reverse=True):
            table.add_row(f"{price:.{book.tick_size}f}", str(qty))

        if len(book.bids) > 0:
            midprice = (list(book.bids.keys())[0] + list(book.asks.keys())[0]) / 2
            table.add_row(str(round(midprice, book.tick_size)), "")

        # Add asks to the table
        for price, qty in sorted(book.asks.items()):
            table.add_row(f"{price:.{book.tick_size}f}", str(qty))

        # If show_bars is true, add a bar chart-like representation
        if show_bars:
            # chart is a grid
            chart = Table.grid(padding=0)
            # Print the bids as bars
            acc_size = sum(book.bids.values())
            for price, qty in sorted(book.bids.items()):
                chart.add_row(f"{price:.{book.tick_size}f}", "█" * (acc_size // 10))
                chart.add_row("", "")
                acc_size -= qty

            # Print the asks as bars
            acc_size = 0
            for price, qty in sorted(book.asks.items()):
                acc_size += qty
                chart.add_row(f"{price:.{book.tick_size}f}", "█" * (qty // 10))
            return table, chart
        else:
            return table
