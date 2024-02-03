from typing import List
from falkordb.exceptions import SchemaVersionMismatchException

# procedures
DB_LABELS             = "DB.LABELS"
DB_PROPERTYKEYS       = "DB.PROPERTYKEYS"
DB_RELATIONSHIPTYPES  = "DB.RELATIONSHIPTYPES"


class GraphSchema():
    """
    The graph schema.
    Maintains the labels, properties and relationships of the graph.
    """

    def __init__(self, graph: 'Graph'):
        """
        Initialize the graph schema.

        Args:
            graph (Graph): The graph.

        Returns:
           GraphSchema: The graph schema. 
        """

        self.graph = graph
        self.clear()

    def clear(self):
        """
        Clear the graph schema.

        Returns:
            None

        """

        self.version       = 0
        self.labels        = []
        self.properties    = []
        self.relationships = []

    async def refresh_labels(self) -> None:
        """
        Refresh labels.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_LABELS)).result_set
        self.labels = [l[0] for l in result_set]

    async def refresh_relations(self) -> None:
        """
        Refresh relationship types.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_RELATIONSHIPTYPES)).result_set
        self.relationships = [r[0] for r in result_set]

    async def refresh_properties(self) -> None:
        """
        Refresh property keys.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_PROPERTYKEYS)).result_set
        self.properties = [p[0] for p in result_set]

    async def refresh(self, version: int) -> None:
        """
        Refresh the graph schema.

        Args:
            version (int): The version of the graph schema.

        Returns:
            None

        """

        self.clear()
        self.version = version
        await self.refresh_labels()
        await self.refresh_relations()
        await self.refresh_properties()

    async def get_label(self, idx: int) -> str:
        """
        Returns a label by its index.

        Args:
            idx (int): The index of the label.

        Returns:
            str: The label.

        """

        try:
            l = self.labels[idx]
        except IndexError:
            # refresh labels
            await self.refresh_labels()
            l = self.labels[idx]
        return l

    async def get_relation(self, idx: int) -> str:
        """
        Returns a relationship type by its index.

        Args:
            idx (int): The index of the relation.

        Returns:
            str: The relationship type.

        """

        try:
            r = self.relationships[idx]
        except IndexError:
            # refresh relationship types
            await self.refresh_relations()
            r = self.relationships[idx]
        return r

    async def get_property(self, idx: int) -> str:
        """
        Returns a property by its index.

        Args:
            idx (int): The index of the property.

        Returns:
            str: The property.

        """

        try:
            p = self.properties[idx]
        except IndexError:
            # refresh properties
            await self.refresh_properties()
            p = self.properties[idx]
        return p
