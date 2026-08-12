from .helpers import quote_string
from .node import Node


class Edge:
    """
    An edge connecting two nodes.
    """

    def __init__(
        self,
        src_node: Node | int,
        relation: str,
        dest_node: Node | int,
        edge_id: int | None = None,
        alias: str | None = "",
        properties=None,
    ):
        """
        Create a new edge.

        Args:
            src_node: The source node of the edge.
            relation: The relationship type of the edge.
            dest_node: The destination node of the edge.
            edge_id: The ID of the edge.
            alias: An alias for the edge (default is empty string).
            properties: The properties of the edge.

        Raises:
            AssertionError: If either src_node or dest_node is not provided.

        Returns:
            None
        """
        if src_node is None or dest_node is None:
            raise AssertionError("Both src_node & dest_node must be provided")

        self.id = edge_id
        self.alias = alias
        self.src_node = src_node
        self.dest_node = dest_node
        self.relation = relation
        self.properties = properties or {}

    def to_string(self) -> str:
        """
        Get a string representation of the edge's properties.

        Returns:
            str: A string representation of the edge's properties.
        """
        res = ""
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += "{" + props + "}"

        return res

    def __str__(self) -> str:
        """
        Get a string representation of the edge.

        Returns:
            str: A string representation of the edge.
        """
        # Source node
        res = f"({self.src_node.alias})" if isinstance(self.src_node, Node) else "()"

        # Edge
        res += f"-[{self.alias}"
        if self.relation:
            res += ":" + self.relation
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += f"{{{props}}}"
        res += "]->"

        # Dest node
        if isinstance(self.dest_node, Node):
            res += f"({self.dest_node.alias})"
        else:
            res += "()"

        return res

    def __repr__(self) -> str:
        """
        Get an unambiguous representation of the edge.

        Returns:
            str: A representation useful in tracebacks and debuggers.
        """
        return (
            f"Edge(id={self.id!r}, alias={self.alias!r}, "
            f"relation={self.relation!r}, src_node={self.src_node!r}, "
            f"dest_node={self.dest_node!r}, properties={self.properties!r})"
        )

    def __eq__(self, rhs) -> bool:
        """
        Check if two edges are equal.

        Args:
            rhs: The edge to compare.

        Returns:
            bool: True if the edges are equal, False otherwise.
        """
        # Type checking
        if not isinstance(rhs, Edge):
            return False

        # Quick positive check, if both IDs are set
        if self.id is not None and rhs.id is not None and self.id == rhs.id:
            return True

        # Source and destination nodes should match
        if self.src_node != rhs.src_node:
            return False

        if self.dest_node != rhs.dest_node:
            return False

        # Relation should match
        if self.relation != rhs.relation:
            return False

        # Quick check for the number of properties
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties
        return self.properties == rhs.properties

    def __hash__(self) -> int:
        """
        Hash the edge so it can be used in sets and as a dict key.

        Only the edge id and relationship type take part, properties are
        mutable and equality tolerates a differing id, so the hash is
        deliberately coarse.

        Returns:
            int: The edge hash.
        """
        return hash((self.id, self.relation))
