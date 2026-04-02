from uuid import uuid4
from dxcore.market import OrderBook, Instrument, Order, OrderType, Side


class Portfolio:
    def __init__(self, initial_cash: float):
        self.cash = initial_cash
        self.inventory = {}  # {symbol: quantity}

    def apply_trade(self, symbol: str, quantity_delta: float, cash_delta: float):
        self.inventory[symbol] = self.inventory.get(symbol, 0) + quantity_delta
        self.cash += cash_delta

    def __repr__(self):
        return f"Cash: ${self.cash:.2f} | Holdings: {self.inventory}"


class User:
    def __init__(self, name: str, initial_cash: float):
        self.name = name
        self.id = uuid4()
        self.portfolio = Portfolio(initial_cash)


class Exchange:
    def __init__(self):
        self.books = {}  # {symbol: order_book.OrderBook}
        self.users = {}  # {user_id: User}
        self.order_owners = {}  # {order_id: user_id} (The "Glue" for settlement)

    def register_user(self, user: User):
        self.users[user.id] = user

    def add_instrument(self, symbol: str):
        self.books[symbol] = OrderBook(Instrument(symbol))

    def place_order(self, user_id, symbol, side, price, quantity):
        if user_id not in self.users or symbol not in self.books:
            return []

        new_order = Order(OrderType.Limit, side, price, quantity)
        self.order_owners[new_order.id] = user_id

        transactions = self.books[symbol].insert(new_order)

        # 4. Settle any trades immediately
        for tx in transactions:
            self._settle_trade(symbol, tx)

        return transactions

    def _settle_trade(self, symbol, tx):
        """
        The Core Logic:
        Maps bid_id and ask_id back to Users and moves the money/assets.
        """
        buyer_id = self.order_owners.get(tx.bid_id)
        seller_id = self.order_owners.get(tx.ask_id)

        if not buyer_id or not seller_id:
            return  # Should not happen in a closed system

        buyer = self.users[buyer_id]
        seller = self.users[seller_id]

        trade_value = tx.price * tx.quantity

        # Buyer: Gets assets, loses cash
        buyer.portfolio.apply_trade(symbol, tx.quantity, -trade_value)

        # Seller: Loses assets, gets cash
        seller.portfolio.apply_trade(symbol, -tx.quantity, trade_value)

        print(f"--- SETTLEMENT EXECUTED ---")
        print(f"Match: {tx.quantity} units of {symbol} @ ${tx.price}")
        print(f"Buyer ({buyer.name}) paid ${trade_value}")
        print(f"Seller ({seller.name}) received ${trade_value}")
        print("---------------------------")


if __name__ == "__main__":
    nx = Exchange()
    nx.add_instrument("BTC")

    alice = User("Alice", 50000.0)
    bob = User("Bob", 50000.0)
    nx.register_user(alice)
    nx.register_user(bob)

    nx.place_order(alice.id, "BTC", Side.Sell, 45000.0, 1)
    nx.place_order(bob.id, "BTC", Side.Buy, 45000.0, 5)

    print(f"Alice's Portfolio: {alice.portfolio}")
    print(f"Bob's Portfolio: {bob.portfolio}")