import unittest

from matplotlib import pyplot as plt

from env.lob.red_black_tree import RedBlackTree, TNULL
from env.lob import LimitOrderBook, Order


class TestRedBlackTree(unittest.TestCase):
    def setUp(self):
        self.tree = RedBlackTree()

    def test_insert_multiple(self):
        """Test inserting multiple nodes to ensure balancing and correct structure."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)
        self.tree.insert(15)
        self.tree.insert(25)
        self.tree.insert(5)

        # Check the structure after inserting multiple nodes
        self.assertEqual(self.tree.root.key, 20)  # Root should be 15 after balancing
        self.assertEqual(self.tree.root.left.key, 10)  # Left child should be 10
        self.assertEqual(self.tree.root.right.key, 30)  # Right child should be 25
        self.assertEqual(self.tree.root.left.left.key, 5)  # Left of 10 should be 5
        self.assertEqual(self.tree.root.right.left.key, 25)  # Left of 25 should be 20
        self.assertEqual(self.tree.root.left.right.key, 15)  # Right of 25 should be 30

    def test_delete_root(self):
        """Test deleting the root node when the tree has a larger structure."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)
        self.tree.insert(25)
        self.tree.insert(35)
        self.tree.insert(15)

        # Deleting root node
        self.tree.delete(20)
        self.assertEqual(self.tree.root.key, 15)  # After deletion, 25 should be the new root
        self.assertEqual(self.tree.root.left.key, 10)  # Left child should be 10
        self.assertEqual(self.tree.root.right.key, 30)  # Right child should be 30
        self.assertEqual(self.tree.root.right.right.key, 35)  # Right of 10 should be 15
        self.assertEqual(self.tree.root.right.left.key, 25)  # Left of 30 should be 25

    def test_delete_leaf(self):
        """Test deleting a leaf node (node with no children)."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)

        self.tree.delete(30)
        self.assertEqual(self.tree.search(30), None)  # Node should no longer exist
        self.assertEqual(self.tree.root.key, 20)  # Root should be 20
        self.assertEqual(self.tree.root.left.key, 10)  # Left child should be 10
        self.assertEqual(self.tree.root.right, TNULL)  # Right child should be None

    def test_delete_node_with_two_children(self):
        """Test deleting a node with two children."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)

        # Deleting a node with two children (20)
        self.tree.delete(20)

        self.assertEqual(self.tree.root.key, 10)  # Root should be 10
        self.assertEqual(self.tree.root.right.key, 30)  # Right child should be 30

    def test_search_after_deletion(self):
        """Test that the search method works after deletions."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)

        self.assertEqual(self.tree.search(20).key, 20)  # Node 20 should be found
        self.tree.delete(20)
        self.assertEqual(self.tree.search(20), None)  # Node 20 should be deleted

        # Searching for a node that doesn't exist
        self.assertEqual(self.tree.search(40), None)

    def test_delete_complex(self):
        """Test deleting a node in a more complex tree structure."""
        self.tree.insert(50)
        self.tree.insert(30)
        self.tree.insert(70)
        self.tree.insert(20)
        self.tree.insert(40)
        self.tree.insert(60)
        self.tree.insert(80)
        self.tree.insert(10)

        # Tree structure before deletion
        #         50
        #       /    \
        #     30      70
        #    /  \    /  \
        #  20    40 60   80
        # /
        # 10

        # Delete node 30 which has two children (20 and 40)
        self.tree.delete(30)

        # Check the structure after deletion
        self.assertEqual(self.tree.root.key, 50)  # Root should still be 50
        self.assertEqual(self.tree.root.left.right.key, 40)  # Left child of 50 should be 40
        self.assertEqual(self.tree.root.left.key, 20)  # Left child of 40 should be 20
        self.assertEqual(self.tree.root.left.left.key, 10)  # Left child of 20 should be 10

    def test_edge_case_insert_delete(self):
        """Test edge case of inserting and deleting in a specific order."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(15)
        self.tree.insert(25)

        # Deleting in reverse order to test rotations
        self.tree.delete(25)
        self.tree.delete(15)
        self.tree.delete(10)
        self.tree.delete(20)

        self.assertEqual(self.tree.root, TNULL)

    def test_ordered_traversal(self):
        """Test in-order traversal of the tree."""
        self.tree.insert(10)
        self.tree.insert(20)
        self.tree.insert(30)
        self.tree.insert(15)
        self.tree.insert(25)
        self.tree.insert(5)

        # In-order traversal should return a sorted list
        self.assertEqual([5, 10, 15, 20, 25, 30], [node.key for node in self.tree.ordered_traversal()])

class TestLOB(unittest.TestCase):
    def setUp(self):
        self.lob = LimitOrderBook()

    def test_send_order(self):
        """Test sending an order to the limit order book."""
        # Send a bid order
        self.lob.send_order(Order(uuid=1, side='bid', price=100, quantity=10))
        self.assertEqual(100, self.lob.bids.root.key)

    def test_send_multiple_orders(self):
        self.lob.send_order(Order(uuid=1, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=2, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=3, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=4, side='bid', price=100, quantity=10))

    def test_send_crossing_orders(self):
        """Test sending orders that cross the spread."""
        # Send a bid order
        self.lob.send_order(Order(uuid=1, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=2, side='ask', price=101, quantity=10))
        self.assertEqual(101, self.lob.asks.root.key)
        self.assertEqual(100, self.lob.bids.root.key)

        # cross the spread by an aggressive ask
        transactions = self.lob.send_order(Order(uuid=3, side='ask', price=100, quantity=5))
        self.assertEqual(5, transactions[0].quantity)

    def test_book_walking(self):
        """Test walking the book."""
        self.lob.send_order(Order(uuid=1, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=2, side='bid', price=99, quantity=10))
        self.lob.send_order(Order(uuid=3, side='bid', price=98, quantity=10))

        transactions = self.lob.send_order(Order(uuid=4, side='ask', price=-1, quantity=30))

        self.assertEqual(3, len(transactions))
        self.assertEqual(30, sum([t.quantity for t in transactions]))

        # assert prices are of the passive side
        self.assertEqual(transactions[0].price, 100)
        self.assertEqual(transactions[1].price, 99)
        self.assertEqual(transactions[2].price, 98)

        # assert seller of transaction is always the ask and buyer is always the bid
        self.assertEqual(transactions[0].seller, 4)

    def test_midprice(self):
        self.lob.send_order(Order(uuid=0, side='bid', price=99, quantity=10))
        self.lob.send_order(Order(uuid=1, side='bid', price=100, quantity=10))
        self.lob.send_order(Order(uuid=2, side='ask', price=101, quantity=10))
        self.lob.send_order(Order(uuid=3, side='ask', price=102, quantity=10))

        best_ask = self.lob.asks.bottom()
        best_bid = self.lob.bids.top()

        self.assertEqual(100.5, (best_ask.value.price + best_bid.value.price) / 2)

    def test_plot(self):
        for i in range(10):
            self.lob.send_order(Order(uuid=i, side='bid', price=100 - i, quantity=10))
            self.lob.send_order(Order(uuid=i, side='ask', price=100 + i, quantity=10))

        self.lob.plot()
        plt.show()


if __name__ == '__main__':
    unittest.main()
