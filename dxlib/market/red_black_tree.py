class Node:
    def __init__(self, key, value=None, color='red'):
        self.key = key
        self.value = value
        self.color = color
        self.left = None
        self.right = None
        self.parent = None


TNULL = Node(0)


class RedBlackTree:
    def __init__(self):
        TNULL.color = 'black'
        self.root = TNULL
        self.len = 0

    def left_rotate(self, x):
        y = x.right
        x.right = y.left
        if y.left != TNULL:
            y.left.parent = x
        y.parent = x.parent
        if x.parent is None:
            self.root = y
        elif x == x.parent.left:
            x.parent.left = y
        else:
            x.parent.right = y
        y.left = x
        x.parent = y

    def __len__(self):
        return self.len

    def right_rotate(self, x):
        y = x.left
        x.left = y.right
        if y.right != TNULL:
            y.right.parent = x
        y.parent = x.parent
        if x.parent is None:
            self.root = y
        elif x == x.parent.right:
            x.parent.right = y
        else:
            x.parent.left = y
        y.right = x
        x.parent = y

    def insert(self, key, value=None):
        node = Node(key, value)
        node.parent = None
        node.key = key
        node.left = TNULL
        node.right = TNULL
        node.color = 'red'

        y = None
        x = self.root

        while x != TNULL:
            y = x
            if node.key < x.key:
                x = x.left
            else:
                x = x.right

        node.parent = y
        if y is None:
            self.root = node
        elif node.key < y.key:
            y.left = node
        else:
            y.right = node

        self.len += 1

        if node.parent is None:
            node.color = 'black'
            return

        if node.parent.parent is None:
            return

        self.fix_insert(node)
        return node

    def fix_insert(self, k):
        while k.parent.color == 'red':
            if k.parent == k.parent.parent.right:
                u = k.parent.parent.left
                if u.color == 'red':
                    u.color = 'black'
                    k.parent.color = 'black'
                    k.parent.parent.color = 'red'
                    k = k.parent.parent
                else:
                    if k == k.parent.left:
                        k = k.parent
                        self.right_rotate(k)
                    k.parent.color = 'black'
                    k.parent.parent.color = 'red'
                    self.left_rotate(k.parent.parent)
            else:
                u = k.parent.parent.right
                if u.color == 'red':
                    u.color = 'black'
                    k.parent.color = 'black'
                    k.parent.parent.color = 'red'
                    k = k.parent.parent
                else:
                    if k == k.parent.right:
                        k = k.parent
                        self.left_rotate(k)
                    k.parent.color = 'black'
                    k.parent.parent.color = 'red'
                    self.right_rotate(k.parent.parent)
            if k == self.root:
                break
        self.root.color = 'black'

    def search(self, key):
        node = self._search_tree_helper(self.root, key)
        return node if node != TNULL else None

    def _search_tree_helper(self, node, key):
        if node == TNULL or key == node.key:
            return node
        if key < node.key:
            return self._search_tree_helper(node.left, key)
        return self._search_tree_helper(node.right, key)

    def delete(self, key):
        self._delete_node_helper(self.root, key)

    def _delete_node_helper(self, node, key):
        z = TNULL
        while node != TNULL:
            if node.key == key:
                z = node
            if node.key < key:
                node = node.right
            else:
                node = node.left

        if z == TNULL:
            print("Couldn't find key in the tree")
            return

        y = z
        y_original_color = y.color
        if z.left == TNULL:
            x = z.right
            self.transplant(z, z.right)
        elif z.right == TNULL:
            x = z.left
            self.transplant(z, z.left)
        else:
            y = self.maximum(z.left)
            y_original_color = y.color
            x = y.left
            if y.parent == z:
                x.parent = y
            else:
                self.transplant(y, y.left)
                y.left = z.left
                y.left.parent = y

            self.transplant(z, y)
            y.right = z.right
            y.right.parent = y
            y.color = z.color

        if y_original_color == 'black':
            self.fix_delete(x)

        self.len -= 1

    def fix_delete(self, x):
        while x != self.root and x.color == 'black':
            if x == x.parent.left:
                s = x.parent.right
                if s.color == 'red':
                    s.color = 'black'
                    x.parent.color = 'red'
                    self.left_rotate(x.parent)
                    s = x.parent.right

                if s.left.color == 'black' and s.right.color == 'black':
                    s.color = 'red'
                    x = x.parent
                else:
                    if s.right.color == 'black':
                        s.left.color = 'black'
                        s.color = 'red'
                        self.right_rotate(s)
                        s = x.parent.right

                    s.color = x.parent.color
                    x.parent.color = 'black'
                    s.right.color = 'black'
                    self.left_rotate(x.parent)
                    x = self.root
            else:
                s = x.parent.left
                if s.color == 'red':
                    s.color = 'black'
                    x.parent.color = 'red'
                    self.right_rotate(x.parent)
                    s = x.parent.left

                if s.left.color == 'black' and s.right.color == 'black':
                    s.color = 'red'
                    x = x.parent
                else:
                    if s.left.color == 'black':
                        s.right.color = 'black'
                        s.color = 'red'
                        self.left_rotate(s)
                        s = x.parent.left

                    s.color = x.parent.color
                    x.parent.color = 'black'
                    s.left.color = 'black'
                    self.right_rotate(x.parent)
                    x = self.root
        x.color = 'black'

    def maximum(self, node):
        while node.right != TNULL:
            node = node.right
        return node

    def minimum(self, node):
        while node.left != TNULL:
            node = node.left
        return node

    def transplant(self, u, v):
        if u.parent is None:
            self.root = v
        elif u == u.parent.left:
            u.parent.left = v
        else:
            u.parent.right = v
        v.parent = u.parent

    def successor(self, key: any):  # next highest key
        node = self.search(key)
        if node.right != TNULL:
            return self.minimum(node.right).key
        parent = node.parent
        while parent != None and node == parent.right:
            node = parent
            parent = parent.parent
        return parent.key if parent != None else None

    def predecessor(self, key: any):  # next lowest key
        node = self.search(key)
        if node.left != TNULL:
            return self.maximum(node.left).key
        parent = node.parent
        while parent != None and node == parent.left:
            node = parent
            parent = parent.parent
        return parent.key if parent != None else None

    def top(self):
        if self.root == TNULL:
            return None
        return self.maximum(self.root)

    def bottom(self):
        if self.root == TNULL:
            return None
        return self.minimum(self.root)

    def _ordered_transversal(self, node):  # generator for producing nodes in order
        if node != TNULL:
            yield from self._ordered_transversal(node.left)
            yield node
            yield from self._ordered_transversal(node.right)

    def _reverse_transversal(self, node):
        if node != TNULL:
            yield from self._reverse_transversal(node.right)
            yield node
            yield from self._reverse_transversal(node.left)

    def ordered_traversal(self, reverse=False):
        if reverse:
            return self._reverse_transversal(self.root)
        return self._ordered_transversal(self.root)
