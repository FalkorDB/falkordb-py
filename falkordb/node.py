from .helpers import quote_string


class Node:
    """
    A graph node.
    """

    def __init__(
        self,
        node_id: int | None = None,
        alias: str | None = "",
        labels: str | list[str] | None = None,
        properties=None,
    ):
        """
        Create a new node.

        Args:
            node_id: The ID of the node.
            alias: An alias for the node (default is empty string).
            labels: The label or list of labels for the node.
            properties: The properties of the node.

        Returns:
            None
        """
        self.id = node_id
        self.alias = alias
        self.labels = None

        if isinstance(labels, list):
            self.labels = [
                label for label in labels if isinstance(label, str) and label != ""
            ]
        elif isinstance(labels, str) and labels != "":
            self.labels = [labels]

        self.properties = properties or {}

    def to_string(self) -> str:
        """
        Get a string representation of the node's properties.

        Returns:
            str: A string representation of the node's properties.
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
        Get a string representation of the node.

        Returns:
            str: A string representation of the node.
        """
        res = "("
        if self.alias:
            res += self.alias
        if self.labels:
            res += ":" + ":".join(self.labels)
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += "{" + props + "}"
        res += ")"

        return res

    def __repr__(self) -> str:
        """
        Get an unambiguous representation of the node.

        Returns:
            str: A representation useful in tracebacks and debuggers.
        """
        return (
            f"Node(id={self.id!r}, alias={self.alias!r}, "
            f"labels={self.labels!r}, properties={self.properties!r})"
        )

    def __eq__(self, rhs) -> bool:
        """
        Check if two nodes are equal.

        Args:
            rhs: The node to compare.

        Returns:
            bool: True if the nodes are equal, False otherwise.
        """
        # Type checking
        if not isinstance(rhs, Node):
            return False

        # Quick positive check, if both IDs are set
        if self.id is not None and rhs.id is not None and self.id != rhs.id:
            return False

        # Labels should match.
        if self.labels != rhs.labels:
            return False

        # Quick check for the number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        return self.properties == rhs.properties

    def __hash__(self) -> int:
        """
        Hash the node so it can be used in sets and as a dict key.

        Only the node id and labels take part, properties are mutable and
        equality tolerates a differing id, so the hash is deliberately coarse.

        Returns:
            int: The node hash.
        """
        return hash((self.id, tuple(self.labels) if self.labels else None))
