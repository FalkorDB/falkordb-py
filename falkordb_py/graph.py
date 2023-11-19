from typing import List
from .node import Node
from .edge import Edge
from .query_result import QueryResult
from .execution_plan import ExecutionPlan
from .exceptions import VersionMismatchException
from .helpers import quote_string, stringify_param_value

# procedures
DB_LABELS             = "DB.LABELS"
DB_PROPERTYKEYS       = "DB.PROPERTYKEYS"
DB_RELATIONSHIPTYPES = "DB.RELATIONSHIPTYPES"

# commands
LIST_CMD              = "GRAPH.LIST"
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

    def __init__(self, client, key):
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
    def key(self):
        """
        Get the graph key.

        Returns:
            str: The graph key.

        """

        return self._key

    def add_node(self, node: Node):
        """
        Adds a new node to the graph.

        Args:
            node (Node): The new node to add.

        Returns:
            None

        """

        self._nodes[node.alias] = node

    def add_edge(self, edge: Edge):
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

    def remove_node(self, node: Node):
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

    def remove_edge(self, edge: Edge):
        """
        Removes an edge from the graph.

        Args:
            edge (Edge): The edge to remove.

        Returns:
            None

        """

        del self._edges[edge.alias]

    def commit(self):
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

    def flush(self):
        """
        Flushes the graph into the database and clears the graph.

        Returns:
            None

        """

        self.commit()
        self._nodes = {}
        self._edges = {}

    def query(self, q: str, params=None, timeout=None, read_only=False, profile=False) -> QueryResult:
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

    def delete(self):
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

    def __clear_schema(self):
        """
        Clear the graph schema.

        Returns:
            None

        """

        self._labels = []
        self._properties = []
        self._relationship_types = []

    def __refresh_schema(self):
        """
        Refresh the graph schema.

        Returns:
            None

        """

        self.__clear_schema()
        self.__refresh_labels()
        self.__refresh_relations()
        self.__refresh_attributes()

    def __refresh_labels(self):
        """
        Refresh labels.

        Returns:
            None

        """
        lbls = self.labels()

        # unpack data
        self._labels = [l[0] for _, l in enumerate(lbls)]

    def __refresh_relations(self):
        """
        Refresh relationship types.

        Returns:
            None

        """

        rels = self.relationship_types()

        # unpack data
        self._relationship_types = [r[0] for _, r in enumerate(rels)]

    def __refresh_attributes(self):
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
    def call_procedure(self, procedure: str, *args, read_only=True, **kwargs) -> QueryResult:
        """
        Call a procedure.

        Args:
            procedure (str): The procedure to call.
            args: Procedure arguments.
            read_only (bool): Whether the procedure is read-only.
            kwargs: Additional keyword arguments.

        Returns:
            QueryResult: The result of the procedure call.

        """
        args = [quote_string(arg) for arg in args]
        q = f"CALL {procedure}({','.join(args)})"

        y = kwargs.get("y", None)
        if y is not None:
            q += f"YIELD {','.join(y)}"

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
