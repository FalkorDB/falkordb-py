from typing import List
from .edge import Edge
from .node import Node


class Path:
    """
    Path Class for representing a path in a graph.

    This class defines a path consisting of nodes and edges. It provides methods for managing and manipulating the path.

    Example:
        node1 = Node()
        node2 = Node()
        edge1 = Edge(node1, "R", node2)

        path = Path.new_empty_path()
        path.add_node(node1).add_edge(edge1).add_node(node2)
        print(path)
        # Output: <(node1)-(edge1)->(node2)>
    """
    def __init__(self, nodes: List[Node], edges: List[Edge]):
        if not (isinstance(nodes, list) and isinstance(edges, list)):
            raise TypeError("nodes and edges must be list")

        self._nodes = nodes
        self._edges = edges
        self.append_type = Node

    def nodes(self) -> List[Node]:
        """
        Returns the list of nodes in the path.

        Returns:
            list: List of nodes in the path.
        """
        return self._nodes

    def edges(self) -> List[Edge]:
        """
        Returns the list of edges in the path.

        Returns:
            list: List of edges in the path.
        """
        return self._edges

    def get_node(self, index) -> Node:
        """
        Returns the node at the specified index in the path.

        Args:
            index (int): Index of the node.

        Returns:
            Node: The node at the specified index.
        """
        if 0 <= index < self.node_count():
            return self._nodes[index]

        return None

    def get_edge(self, index) -> Edge:
        """
        Returns the edge at the specified index in the path.

        Args:
            index (int): Index of the edge.

        Returns:
            Edge: The edge at the specified index.
        """
        if 0 <= index < self.edge_count():
            return self._edges[index]

        return None

    def first_node(self) -> Node:
        """
        Returns the first node in the path.

        Returns:
            Node: The first node in the path.
        """
        return self._nodes[0] if self.node_count() > 0 else None

    def last_node(self) -> Node:
        """
        Returns the last node in the path.

        Returns:
            Node: The last node in the path.
        """
        return self._nodes[-1] if self.node_count() > 0 else None

    def edge_count(self) -> int:
        """
        Returns the number of edges in the path.

        Returns:
            int: Number of edges in the path.
        """
        return len(self._edges)

    def node_count(self) -> int:
        """
        Returns the number of nodes in the path.

        Returns:
            int: Number of nodes in the path.
        """
        return len(self._nodes)

    def __eq__(self, other) -> bool:
        """
        Compares two Path instances for equality based on their nodes and edges.

        Args:
            other (Path): Another Path instance for comparison.

        Returns:
            bool: True if the paths are equal, False otherwise.
        """
        # Type checking
        if not isinstance(other, Path):
            return False

        return self.nodes() == other.nodes() and self.edges() == other.edges()

    def __str__(self) -> str:
        """
        Returns a string representation of the path, including nodes and edges.

        Returns:
            str: String representation of the path.
        """
        res = "<"
        edge_count = self.edge_count()
        for i in range(0, edge_count):
            node_id = self.get_node(i).id
            res += "(" + str(node_id) + ")"
            edge = self.get_edge(i)
            res += (
                "-[" + str(int(edge.id)) + "]->"
                if edge.src_node == node_id
                else "<-[" + str(int(edge.id)) + "]-"
            )
        node_id = self.get_node(edge_count).id
        res += "(" + str(node_id) + ")"
        res += ">"
        return res
