from .helpers import quote_string, random_string

class Node:
    """
    A graph node.
    """

    def __init__(self, node_id=None, alias=None, label=None, properties=None):
        """
        Create a new node.

        Args:
            node_id: The ID of the node.
            alias: An alias for the node (default is a random string).
            label: The label or list of labels for the node.
            properties: The properties of the node.

        Returns:
            None
        """
        self.id    = node_id
        self.alias = alias or random_string()

        if isinstance(label, list):
            self.label = [l for l in label if isinstance(l, str) and l != ""]
        elif isinstance(label, str):
            self.label = [label]
        else:
            self.label = []

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

    def __str__(self):
        """
        Get a string representation of the node.

        Returns:
            str: A string representation of the node.
        """
        res = "("
        if self.alias:
            res += self.alias
        if self.label:
            res += ":" + ":".join(self.label)
        if self.properties:
            props = ",".join(
                key + ":" + str(quote_string(val))
                for key, val in sorted(self.properties.items())
            )
            res += "{" + props + "}"
        res += ")"

        return res

    def __eq__(self, rhs):
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

        # Label should match.
        if self.label != rhs.label:
            return False

        # Quick check for the number of properties.
        if len(self.properties) != len(rhs.properties):
            return False

        # Compare properties.
        if self.properties != rhs.properties:
            return False

        return True
