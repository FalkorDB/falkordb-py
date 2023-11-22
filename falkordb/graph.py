from typing import List, Dict, Optional
from .node import Node
from .edge import Edge
from .query_result import QueryResult
from .execution_plan import ExecutionPlan
from .exceptions import VersionMismatchException
from .helpers import quote_string, stringify_param_value

# procedures
DB_LABELS             = "DB.LABELS"
GRAPH_INDEXES         = "DB.INDEXES"
DB_PROPERTYKEYS       = "DB.PROPERTYKEYS"
DB_RELATIONSHIPTYPES  = "DB.RELATIONSHIPTYPES"
QUERY_VECTOR_NODE_IDX = "DB.IDX.VECTOR.QUERYNODES"
QUERY_VECTOR_EDGE_IDX = "DB.IDX.VECTOR.QUERYRELATIONSHIPS"

# commands
QUERY_CMD             = "GRAPH.QUERY"
DELETE_CMD            = "GRAPH.DELETE"
EXPLAIN_CMD           = "GRAPH.EXPLAIN"
SLOWLOG_CMD           = "GRAPH.SLOWLOG"
PROFILE_CMD           = "GRAPH.PROFILE"
RO_QUERY_CMD          = "GRAPH.RO_QUERY"


class Graph():
    """
    Graph, collection of nodes and edges.
    """

    def __init__(self, client, key: str):
        """
        Create a new graph.

        Args:
            client: The client object.
            key (str): Graph ID

        """

        self._key            = key # graph ID
        self.client          = client
        self.execute_command = client.execute_command

        self.version = 0               # graph version
        self._nodes = {}               # set of nodes
        self._edges = {}               # set of edges
        self._labels = []              # list of node labels
        self._properties = []          # list of properties
        self._relationship_types = []  # list of relation types

    @property
    def key(self) -> str:
        """
        Get the graph key.

        Returns:
            str: The graph key.

        """

        return self._key

    def add_node(self, node: Node) -> None:
        """
        Adds a new node to the graph.

        Args:
            node (Node): The new node to add.

        Returns:
            None

        """

        self._nodes[node.alias] = node

    def add_edge(self, edge: Edge) -> None:
        """
        Adds a new edge to the graph.
        Edge ends are also added in case they're missing.

        Args:
            edge (Edge): The new edge to add.

        Returns:
            None

        """

        if not self._nodes[edge.src_node.alias]:
            self.add_node(edge.src_node)

        if not self._nodes[edge.dest_node.alias]:
            self.add_node(edge.dest_node)

        self._edges[edge.alias] = edge

    def number_of_nodes(self) -> int:
        """
        Returns the number of nodes in the graph.

        Returns:
            int: Number of nodes in the graph.

        """

        return len(self._nodes)

    def number_of_edges(self) -> int:
        """
        Returns the number of edges in the graph.

        Returns:
            int: Number of edges in the graph.

        """

        return len(self._edges)

    def remove_node(self, node: Node) -> None:
        """
        Removes a node and all of its incoming and outgoing edges from the graph.

        Args:
            node (Node): The node to remove.

        Returns:
            None

        """

        # remove all incoming and outgoing edges of 'node'
        edges_to_remove = []
        for e in self._edges:
            if node in (e.src_node, e.dest_node):
                edges_to_remove.append(e)

        for e in edges_to_remove:
            del self._edges[e]

        # remove node
        del self._nodes[node.alias]

    def remove_edge(self, edge: Edge) -> None:
        """
        Removes an edge from the graph.

        Args:
            edge (Edge): The edge to remove.

        Returns:
            None

        """

        del self._edges[edge.alias]

    def commit(self) -> QueryResult:
        """
        Commits the graph into the database.

        Returns:
            None

        """

        node_count = len(self._nodes)
        edge_count = len(self._edges)

        # empty graph
        if node_count == 0 and edge_count == 0:
            return None

        query = "CREATE "

        nodes_str = ""
        if node_count:
            nodes_str = ",".join([str(n[1]) for n in self._nodes.items()])

        edges_str = ""
        if edge_count:
            edges_str= ",".join([str(e[1]) for e in self._edges.items()])

        if node_count > 0 and edge_count > 0:
            query += nodes_str + "," + edges_str
        else:
            query += nodes_str + edges_str

        return self.query(query)

    def flush(self) -> None:
        """
        Flushes the graph into the database and clears the graph.

        Returns:
            None

        """

        self.commit()
        self._nodes = {}
        self._edges = {}

    def query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None, read_only: bool = False,
              profile: bool = False) -> QueryResult:
        """
        Executes a query against the graph.

        Args:
            q (str): The query.
            params (dict): Query parameters.
            timeout (int): Maximum query runtime in milliseconds.
            read_only (bool): Executes a readonly query if set to True.
            profile (bool): Profiles the query.

        Returns:
            QueryResult: query result set.

        """

        # maintain original 'q'
        query = q

        # handle query parameters
        query = self.__build_params_header(params) + query

        # construct query command
        # ask for compact result-set format
        # specify known graph version
        if profile:
            cmd = PROFILE_CMD
        else:
            cmd = RO_QUERY_CMD if read_only else QUERY_CMD
        command = [cmd, self.key, query, "--compact"]

        # include timeout is specified
        if isinstance(timeout, int):
            command.extend(["timeout", timeout])
        elif timeout is not None:
            raise Exception("Timeout argument must be a positive integer")

        # issue query
        try:
            response = self.execute_command(*command)
            return QueryResult(self, response, profile)
        except VersionMismatchException as e:
            # client view over the graph schema is out of sync
            # set client version and refresh local schema
            self.version = e.version
            self.__refresh_schema()
            # re-issue query
            return self.query(q, params, timeout, read_only)

    def merge(self, pattern) -> QueryResult:
        """
        Merge pattern.

        Args:
            pattern: The pattern to merge.

        Returns:
            QueryResult: The result of the merge.

        """

        query = "MERGE "
        query += str(pattern)

        return self.query(query)

    def delete(self) -> None:
        """
        Deletes the graph.

        Returns:
            None

        """

        self.__clear_schema()
        return self.execute_command(DELETE_CMD, self.key)

    def profile(self, query: str):
        """
        Execute a query and produce an execution plan augmented with metrics
        for each operation's execution. Return a string representation of a
        query execution plan, with details on results produced by and time
        spent in each operation.

        Args:
            query (str): The query to profile.

        Returns:
            str: The profile information.

        """

        return self.query(query, profile=True)

    def slowlog(self):
        """
        Get a list containing up to 10 of the slowest queries issued
        against the graph.

        Each item in the list has the following structure:
        1. a unix timestamp at which the log entry was processed
        2. the issued command
        3. the issued query
        4. the amount of time needed for its execution, in milliseconds.

        Returns:
            List: List of slow log entries.

        """

        return self.execute_command(SLOWLOG_CMD, self.key)

    def explain(self, query: str, params=None) -> ExecutionPlan:
        """
        Get the execution plan for a given query.
        GRAPH.EXPLAIN returns an ExecutionPlan object.

        Args:
            query (str): The query for which to get the execution plan.
            params (dict): Query parameters.

        Returns:
            ExecutionPlan: The execution plan.

        """

        query = self.__build_params_header(params) + query

        plan = self.execute_command(EXPLAIN_CMD, self.key, query)
        return ExecutionPlan(plan)

    def __clear_schema(self) -> None:
        """
        Clear the graph schema.

        Returns:
            None

        """

        self._labels = []
        self._properties = []
        self._relationship_types = []

    def __refresh_schema(self) -> None:
        """
        Refresh the graph schema.

        Returns:
            None

        """

        self.__clear_schema()
        self.__refresh_labels()
        self.__refresh_relations()
        self.__refresh_attributes()

    def __refresh_labels(self) -> None:
        """
        Refresh labels.

        Returns:
            None

        """
        lbls = self.labels()

        # unpack data
        self._labels = [l[0] for _, l in enumerate(lbls)]

    def __refresh_relations(self) -> None:
        """
        Refresh relationship types.

        Returns:
            None

        """

        rels = self.relationship_types()

        # unpack data
        self._relationship_types = [r[0] for _, r in enumerate(rels)]

    def __refresh_attributes(self) -> None:
        """
        Refresh property keys.

        Returns:
            None

        """

        props = self.property_keys()

        # unpack data
        self._properties = [p[0] for _, p in enumerate(props)]

    def get_label(self, idx: int) -> str:
        """
        Returns a label by its index.

        Args:
            idx (int): The index of the label.

        Returns:
            str: The label.

        """

        try:
            label = self._labels[idx]
        except IndexError:
            # refresh labels
            self.__refresh_labels()
            label = self._labels[idx]
        return label

    def get_relation(self, idx: int) -> str:
        """
        Returns a relationship type by its index.

        Args:
            idx (int): The index of the relation.

        Returns:
            str: The relationship type.

        """

        try:
            relationship_type = self._relationship_types[idx]
        except IndexError:
            # refresh relationship types
            self.__refresh_relations()
            relationship_type = self._relationship_types[idx]
        return relationship_type

    def get_property(self, idx: int) -> str:
        """
        Returns a property by its index.

        Args:
            idx (int): The index of the property.

        Returns:
            str: The property.

        """

        try:
            p = self._properties[idx]
        except IndexError:
            # refresh properties
            self.__refresh_attributes()
            p = self._properties[idx]
        return p

    def __build_params_header(self, params: dict) -> str:
        """
        Build parameters header.

        Args:
            params (dict): The parameters.

        Returns:
            str: The parameters header.

        """

        if params is None:
            return ""
        if not isinstance(params, dict):
            raise TypeError("'params' must be a dict")
        # header starts with "CYPHER"
        params_header = "CYPHER "
        for key, value in params.items():
            params_header += str(key) + "=" + stringify_param_value(value) + " "
        return params_header

    # procedures
    def call_procedure(self, procedure: str, read_only: bool = True,
                       args: Optional[List] = None,
                       emit: Optional[List[str]] = None) -> QueryResult:
        """
        Call a procedure.

        Args:
            procedure (str): The procedure to call.
            read_only (bool): Whether the procedure is read-only.
            args: Procedure arguments.
            emit: Procedure yield.

        Returns:
            QueryResult: The result of the procedure call.

        """
        args = args or []
        args = [quote_string(arg) for arg in args]
        q = f"CALL {procedure}({','.join(args)})"

        if emit is not None and len(emit) > 0:
            q += f"YIELD {','.join(emit)}"

        return self.query(q, read_only=read_only)

    def labels(self) -> List[str]:
        """
        Get node labels.

        Returns:
            List: List of node labels.

        """
        return self.call_procedure(DB_LABELS).result_set

    def relationship_types(self) -> List[str]:
        """
        Get relationship types.

        Returns:
            List: List of relationship types.

        """
        return self.call_procedure(DB_RELATIONSHIPTYPES).result_set

    def property_keys(self) -> List[str]:
        """
        Get property keys.

        Returns:
            List: List of property keys.

        """
        return self.call_procedure(DB_PROPERTYKEYS).result_set

    # index operations

    def _drop_index(self, idx_type: str, entity_type: str, label: str,
                    attribute: str) -> QueryResult:
        """Drop a graph index.

        Args:
            idx_type (str): The type of index ("RANGE", "FULLTEXT", "VECTOR").
            entity_type (str): The type of entity ("NODE" or "EDGE").
            label (str): The label of the node or edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        # set pattern
        if entity_type == "NODE":
            pattern = f"(e:{label})"
        elif entity_type == "EDGE":
            pattern = f"()-[e:{label}]->()"
        else:
            raise ValueError("Invalid entity type")

        # build drop index command
        if idx_type == "RANGE":
            q = f"DROP INDEX FOR {pattern} ON (e.{attribute})"
        elif idx_type == "VECTOR":
            q = f"DROP VECTOR INDEX FOR {pattern} ON (e.{attribute})"
        elif idx_type == "FULLTEXT":
            q = f"DROP FULLTEXT INDEX FOR {pattern} ON (e.{attribute})"
        else:
            raise ValueError("Invalid index type")

        return self.query(q)

    def drop_node_range_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a range index for a node.

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("RANGE", "NODE", label, attribute)

    def drop_node_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for a node.

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("FULLTEXT", "NODE", label, attribute)

    def drop_node_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for a node.

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("VECTOR", "NODE", label, attribute)

    def drop_edge_range_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a range index for an edge.

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("RANGE", "EDGE", label, attribute)

    def drop_edge_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for an edge.

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("FULLTEXT", "EDGE", label, attribute)

    def drop_edge_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for an edge.

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("VECTOR", "EDGE", label, attribute)

    def list_indices(self) -> QueryResult:
        """Retrieve a list of graph indices.

        Returns:
            list: List of graph indices.
        """
        return self.call_procedure(GRAPH_INDEXES, read_only=True)

    def _create_typed_index(self, idx_type: str, entity_type: str, label: str,
                            *properties: List[str], options=None) -> QueryResult:
        """Create a typed index for nodes or edges.

        Args:
            idx_type (str): The type of index ("RANGE", "FULLTEXT", "VECTOR").
            entity_type (str): The type of entity ("NODE" or "EDGE").
            label (str): The label of the node or edge.
            properties: Variable number of property names to be indexed.
            options (dict, optional): Additional options for the index.

        Returns:
            Any: The result of the index creation query.
        """
        if entity_type == "NODE":
            pattern = f"(e:{label})"
        elif entity_type == "EDGE":
            pattern = f"()-[e:{label}]->()"
        else:
            raise ValueError("Invalid entity type")

        if idx_type == "RANGE":
            idx_type = ""

        q = f"CREATE {idx_type} INDEX FOR {pattern} ON ("
        q += ",".join(map("e.{0}".format, properties))
        q += ")"

        if options is not None:
            # convert options to a Cypher map
            options_map = "{"
            for key, value in options.items():
                if isinstance(value, str):
                    options_map += key + ":'" + value + "',"
                else:
                    options_map += key + ':' + str(value) + ','
            options_map = options_map[:-1] + "}"
            q += f" OPTIONS {options_map}"

        return self.query(q)

    def create_node_range_index(self, label: str, *properties) -> QueryResult:
        """Create a range index for a node.

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("RANGE", "NODE", label, *properties)

    def create_node_fulltext_index(self, label: str, *properties) -> QueryResult:
        """Create a full-text index for a node.

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("FULLTEXT", "NODE", label, *properties)

    def create_node_vector_index(self, label: str, *properties, dim: int = 0,
                                 similarity_function: str = "euclidean") -> QueryResult:
        """Create a vector index for a node.

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.
            dim (int, optional): The dimension of the vector.
            similarity_function (str, optional): The similarity function for the vector.

        Returns:
            Any: The result of the index creation query.
        """
        options = {'dimension': dim, 'similarityFunction': similarity_function}
        return self._create_typed_index("VECTOR", "NODE", label, *properties, options=options)

    def create_edge_range_index(self, relation: str, *properties) -> QueryResult:
        """Create a range index for an edge.

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("RANGE", "EDGE", relation, *properties)

    def create_edge_fulltext_index(self, relation: str, *properties) -> QueryResult:
        """Create a full-text index for an edge.

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("FULLTEXT", "EDGE", relation, *properties)

    def create_edge_vector_index(self, relation: str, *properties, dim: int = 0,
                                 similarity_function: str = "euclidean") -> QueryResult:
        """Create a vector index for an edge.

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.
            dim (int, optional): The dimension of the vector.
            similarity_function (str, optional): The similarity function for the vector.

        Returns:
            Any: The result of the index creation query.
        """
        options = {'dimension': dim, 'similarityFunction': similarity_function}
        return self._create_typed_index("VECTOR", "EDGE", relation, *properties, options=options)

    def query_node_vector_index(self, label: str, attribute: str, k: int,
                                q: List[float]) -> QueryResult:
        """Query a vector index for nodes.

        Args:
            label (str): The label of the node.
            attribute (str): The attribute of the vector.
            k (int): The number of results to retrieve.
            q (str): The query string.

        Returns:
            Any: The result of the vector index query.
        """
        return self.call_procedure(QUERY_VECTOR_NODE_IDX, args=[label, attribute, k, q])

    def query_edge_vector_index(self, relation: str, attribute: str, k: int,
                                q: List[float]) -> QueryResult:
        """Query a vector index for edges.

        Args:
            relation (str): The relation of the edge.
            attribute (str): The attribute of the vector.
            k (int): The number of results to retrieve.
            q (str): The query string.

        Returns:
            Any: The result of the vector index query.
        """
        return self.call_procedure(QUERY_VECTOR_EDGE_IDX, args=[relation, attribute, k, q])
