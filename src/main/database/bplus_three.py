# * Imports
from src.main.utils.constants import *

# * The BPlusTreeIndex class represents the top-level B+ tree structure and provides methods for inserting and
# searching for keys.
class BPlusTreeIndex:
    # Initialize a three with empty root node
    def __init__(self):
        self.root = BPlusTreeNode()

    #  Insert key and page number to the three, handle root node split if needed.
    def insert(self, key, page_number):
        new_root = self.root.insert(key, page_number)
        if new_root:
            new_internal_node = BPlusTreeInternalNode([new_root.keys[0]], [self.root, new_root])
            self.root = new_internal_node

    # Search for a key in the B+ tree.
    def search(self, key):
        return self.root.search(key)


# * The BPlusTreeNode and BPlusTreeInternalNode classes represent the nodes in the B+ tree. The BPlusTreeNode class
# is used for leaf nodes, while the BPlusTreeInternalNode class is used for internal nodes.
class BPlusTreeNode:
    # Initialize an empty leaf node.
    def __init__(self):
        self.keys = []
        self.children = []
        self.is_leaf = True
        self.next_leaf = None

    # Insert key and page number to the node, handle leaf node split if needed.
    def insert(self, key, page_number):
        if self.is_leaf:
            self.keys = list(self.keys)
            self.children = list(self.children)

            self.keys.append(key)
            self.children.append(page_number)
            self.sort_node()

            if len(self.keys) > PAGE_SIZE // LENGTH_SIZE:
                return self.split()
        else:
            # If not a leaf, find the appropriate child and recursively insert.
            index = self.find_child_index(key)
            child = self.children[index]
            new_child = child.insert(key, page_number)

            self.keys = list(self.keys)
            self.children = list(self.children)
            self.children[index] = new_child

            if len(new_child.keys) > PAGE_SIZE // LENGTH_SIZE:
                return self.split_child(index)

        return self

    #  Split a leaf node.
    def split(self):
        new_node = BPlusTreeNode()
        split_index = len(self.keys) // 2
        new_node.keys = self.keys[split_index:]
        new_node.children = self.children[split_index:]
        self.keys = self.keys[:split_index]
        self.children = self.children[:split_index]
        # Here we use some pointers
        new_node.next_leaf = self.next_leaf
        self.next_leaf = new_node
        return BPlusTreeInternalNode([new_node.keys[0]], [self, new_node])

    # Split a child node.
    def split_child(self, child_index):
        # It is called upon at the insert module
        new_node = BPlusTreeNode()
        split_index = len(self.children[child_index].keys) // 2
        new_node.keys = self.children[child_index].keys[split_index:]
        new_node.children = self.children[child_index].children[split_index:]
        self.children[child_index].keys = self.children[child_index].keys[:split_index]
        self.children[child_index].children = self.children[child_index].children[:split_index]
        # The new node has to be inserted into the parent
        self.keys.insert(child_index, new_node.keys[0])
        self.children.insert(child_index + 1, new_node)
        return self

    # Search for a key in leaf nodes.
    def search(self, key):
        if self.is_leaf:
            index = self.find_key_index(key)
            if index != -1:
                return self.children[index]
            else:
                return None
        else:  # If not a leaf, recursively search in the appropriate child.
            "We search the node and dive deeper and search the child"
            index = self.find_child_index(key)
            return self.children[index].search(key)

    # Find the index of a key in the node.
    def find_key_index(self, key):
        for i, node_key in enumerate(self.keys):
            if key == node_key:
                return i
            elif key < node_key:
                return -1
        return -1

    # Find the index of the child to dive deeper into.
    def find_child_index(self, key):
        "Which child do we need to dive deeper into"
        for i, node_key in enumerate(self.keys):
            if key < node_key:
                return i
        return len(self.children) - 1

    # Sort keys and children in the node.
    def sort_node(self):
        combined = sorted(zip(self.keys, self.children), key=lambda x: x[0])
        self.keys, self.children = zip(*combined)

        if not self.is_leaf:
            for child in self.children:
                child.sort_node()


# * The BPlusTreeNode and BPlusTreeInternalNode classes represent the nodes in the B+ tree. The BPlusTreeNode class
# is used for leaf nodes, while the BPlusTreeInternalNode class is used for internal nodes.
class BPlusTreeInternalNode(BPlusTreeNode):
    # Initialize an internal node with keys and children.
    def __init__(self, keys, children):
        super().__init__()
        self.keys = keys
        self.children = children
        self.is_leaf = False

    # Insert key and page number, handle child node split if needed.
    def insert(self, key, page_number):
        index = self.find_child_index(key)
        self.children[index] = self.children[index].insert(key, page_number)
        "If the child is full we split again"
        if len(self.children[index].keys) > PAGE_SIZE // LENGTH_SIZE:
            return self.split_child(index)

        return self

    # Split a child node for internal nodes.
    def split_child(self, child_index):
        "Code to split the child"
        new_node = BPlusTreeInternalNode([], [])
        split_index = len(self.children[child_index].keys) // 2
        new_node.keys = self.children[child_index].keys[split_index:]
        new_node.children = self.children[child_index].children[split_index:]
        self.children[child_index].keys = self.children[child_index].keys[:split_index]
        self.children[child_index].children = self.children[child_index].children[:split_index]
        "The newly created node has to be added to the parent level"
        self.keys.insert(child_index, new_node.keys[0])
        self.children.insert(child_index + 1, new_node)

        return self
