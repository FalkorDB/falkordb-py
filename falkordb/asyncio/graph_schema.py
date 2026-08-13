# procedures
DB_LABELS = "DB.LABELS"
DB_PROPERTYKEYS = "DB.PROPERTYKEYS"
DB_RELATIONSHIPTYPES = "DB.RELATIONSHIPTYPES"


class GraphSchema:
    """
    The graph schema.
    Maintains the labels, properties and relationships of the graph.
    """

    def __init__(self, graph):
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

        self.version = 0
        self.labels = []
        self.properties = []
        self.relationships = []
        self._dirty_labels = True
        self._dirty_properties = True
        self._dirty_relations = True

    async def refresh_labels(self) -> None:
        """
        Refresh labels.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_LABELS)).result_set
        self.labels = [label[0] for label in result_set]
        self._dirty_labels = False

    async def refresh_relations(self) -> None:
        """
        Refresh relationship types.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_RELATIONSHIPTYPES)).result_set
        self.relationships = [r[0] for r in result_set]
        self._dirty_relations = False

    async def refresh_properties(self) -> None:
        """
        Refresh property keys.

        Returns:
            None

        """

        result_set = (await self.graph.call_procedure(DB_PROPERTYKEYS)).result_set
        self.properties = [p[0] for p in result_set]
        self._dirty_properties = False

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

        if self._dirty_labels or not self.labels or idx >= len(self.labels):
            await self.refresh_labels()
        try:
            return self.labels[idx]
        except IndexError:
            await self.refresh_labels()
            return self.labels[idx]

    async def get_relation(self, idx: int) -> str:
        """
        Returns a relationship type by its index.

        Args:
            idx (int): The index of the relation.

        Returns:
            str: The relationship type.

        """

        if (
            self._dirty_relations
            or not self.relationships
            or idx >= len(self.relationships)
        ):
            await self.refresh_relations()
        try:
            return self.relationships[idx]
        except IndexError:
            await self.refresh_relations()
            return self.relationships[idx]

    async def get_property(self, idx: int) -> str:
        """
        Returns a property by its index.

        Args:
            idx (int): The index of the property.

        Returns:
            str: The property.

        """

        if self._dirty_properties or not self.properties or idx >= len(self.properties):
            await self.refresh_properties()
        try:
            return self.properties[idx]
        except IndexError:
            await self.refresh_properties()
            return self.properties[idx]
