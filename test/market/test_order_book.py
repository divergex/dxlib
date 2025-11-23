import pytest
from uuid import uuid4

from dxlib import Instrument
from dxlib.market import OrderBook, OrderBookV1, PriceLevel, Order, Side, Transaction  # adjust import

instrument = Instrument("MSFT")

@pytest.fixture
def sample_orders():
    return [
        Order(instrument, uuid=uuid4(), price=1.01, quantity=10, side=Side.BUY, client="A"),
        Order(instrument, uuid=uuid4(), price=1.02, quantity=5, side=Side.BUY, client="B"),
        Order(instrument, uuid=uuid4(), price=1.03, quantity=7, side=Side.SELL, client="C"),
    ]


def test_price_level_add_pop():
    from llist import dllist
    pl = PriceLevel(1.0)
    o1 = Order(instrument, uuid=uuid4(), price=1.0, quantity=10, side=Side.BUY, client="A")
    o2 = Order(instrument, uuid=uuid4(), price=1.0, quantity=5, side=Side.BUY, client="B")

    pl.add_order(o1)
    pl.add_order(o2)
    assert len(pl) == 2
    assert list(pl) == [o1, o2]

    pl.pop(o1)
    assert len(pl) == 1
    assert list(pl) == [o2]
    assert not pl.empty()

    pl.pop(o2)
    assert pl.empty()


def test_orderbook_add_and_quantity(sample_orders):
    ob = OrderBook(tick_size=0.01)
    o1, o2, o3 = sample_orders

    ob.send_limit(o1)
    ob.send_limit(o2)
    ob.send_limit(o3)

    # Check quantities at each price
    assert ob.quantity(1.01, Side.BUY) == 10
    assert ob.quantity(1.02, Side.BUY) == 5
    assert ob.quantity(1.03, Side.SELL) == 7

    # Check shape
    bids_len, asks_len = ob.shape
    assert bids_len == 2
    assert asks_len == 1


def test_orderbook_cancel(sample_orders):
    ob = OrderBook()
    o1 = sample_orders[0]
    ob.send_limit(o1)

    assert ob.quantity(o1.price, Side.BUY) == o1.quantity

    ob.cancel_order(o1.uuid)
    assert ob.quantity(o1.price, Side.BUY) == 0
    with pytest.raises(KeyError):
        ob.cancel_order(o1.uuid)


def test_orderbook_rounding():
    ob = OrderBook(tick_size=0.02)
    assert ob.round(1.037) == 1.04
    assert ob.round(1.029) == 1.02


def test_send_market_matches():
    ob = OrderBook()
    buy_order = Order(instrument, uuid=uuid4(), price=1.0, quantity=10, side=Side.BUY, client="B")
    sell_order = Order(instrument, uuid=uuid4(), price=1.0, quantity=5, side=Side.SELL, client="S")
    ob.send_limit(buy_order)
    transactions = ob.send_market(sell_order)

    assert len(transactions) == 1
    t = transactions[0]
    assert t.quantity == 5
    # remaining buy order quantity
    remaining_bids = ob.bids[1.0]
    remaining_qty = sum(o.quantity for o in remaining_bids)
    assert remaining_qty == 5



if __name__ == '__main__':
    pytest.main()
