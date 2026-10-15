from .edge import Edge
from .node import Node


class Path:
    """
    Path Class for representing a path in a graph.

    This class defines a path consisting of nodes and edges. A path is normally
    obtained from a query result rather than built by hand.

    Example:
        node1 = Node(node_id=1)
        node2 = Node(node_id=2)
        edge1 = Edge(node1, "R", node2, edge_id=0)

        path = Path([node1, node2], [edge1])
        print(path)
        # Output: <(1)-[0]->(2)>
    """

    def __init__(self, nodes: list[Node], edges: list[Edge]):
        if not (isinstance(nodes, list) and isinstance(edges, list)):
            raise TypeError("nodes and edges must be list")

        self._nodes = nodes
        self._edges = edges
        self.append_type = Node

    def nodes(self) -> list[Node]:
        """
        Returns the list of nodes in the path.

        Returns:
            list: List of nodes in the path.
        """
        return self._nodes

    def edges(self) -> list[Edge]:
        """
        Returns the list of edges in the path.

        Returns:
            list: List of edges in the path.
        """
        return self._edges

    def get_node(self, index) -> Node | None:
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

    def get_edge(self, index) -> Edge | None:
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

    def first_node(self) -> Node | None:
        """
        Returns the first node in the path.

        Returns:
            Node: The first node in the path.
        """
        return self._nodes[0] if self.node_count() > 0 else None

    def last_node(self) -> Node | None:
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

    def __hash__(self) -> int:
        """
        Hash the path so it can be used in sets and as a dict key.

        Returns:
            int: The path hash.
        """
        return hash((tuple(self._nodes), tuple(self._edges)))

    def __repr__(self) -> str:
        """
        Get an unambiguous representation of the path.

        Returns:
            str: A representation useful in tracebacks and debuggers.
        """
        return f"Path(nodes={self._nodes!r}, edges={self._edges!r})"

    def __str__(self) -> str:
        """
        Returns a string representation of the path, including nodes and edges.

        Returns:
            str: String representation of the path.
        """
        if self.node_count() == 0:
            return "<>"

        res = "<"
        edge_count = self.edge_count()
        for i in range(0, edge_count):
            node = self._nodes[i]
            node_id = node.id
            res += "(" + str(node_id) + ")"
            edge = self._edges[i]
            edge_id_str = str(int(edge.id)) if edge.id is not None else ""
            # src_node may be a Node or a raw node id depending on how the
            # path was built, normalize before comparing
            src = edge.src_node
            src_id = src.id if isinstance(src, Node) else src
            res += (
                "-[" + edge_id_str + "]->"
                if src_id == node_id
                else "<-[" + edge_id_str + "]-"
            )
        last_node = self._nodes[edge_count]
        res += "(" + str(last_node.id) + ")"
        res += ">"
        return res
