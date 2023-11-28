from typing import List, Dict, Optional
from .graph_schema import GraphSchema
from .query_result import QueryResult
from .execution_plan import ExecutionPlan
from .exceptions import SchemaVersionMismatchException
from .helpers import quote_string, stringify_param_value

# procedures
GRAPH_INDEXES         = "DB.INDEXES"

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

    def __init__(self, client, name: str):
        """
        Create a new graph.

        Args:
            client: The client object.
            name (str): Graph ID

        """

        self._name           = name
        self.client          = client
        self.schema          = GraphSchema(self)
        self.execute_command = client.execute_command

    @property
    def name(self) -> str:
        """
        Get the graph name.

        Returns:
            str: The graph name.

        """

        return self._name

    def _query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None, read_only: bool = False) -> QueryResult:
        """
        Executes a query against the graph.
        See: https://docs.falkordb.com/commands/graph.query.html

        Args:
            q (str): The query.
            params (dict): Query parameters.
            timeout (int): Maximum query runtime in milliseconds.
            read_only (bool): Whether the query is read-only.

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
        cmd = RO_QUERY_CMD if read_only else QUERY_CMD
        command = [cmd, self.name, query, "--compact"]

        # include timeout is specified
        if isinstance(timeout, int):
            command.extend(["timeout", timeout])
        elif timeout is not None:
            raise Exception("Timeout argument must be a positive integer")

        # issue query
        try:
            response = self.execute_command(*command)
            return QueryResult(self, response)
        except SchemaVersionMismatchException as e:
            # client view over the graph schema is out of sync
            # set client version and refresh local schema
            self.schema.refresh(e.version)
            raise e

    def query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None) -> QueryResult:
        """
        Executes a query against the graph.
        See: https://docs.falkordb.com/commands/graph.query.html

        Args:
            q (str): The query.
            params (dict): Query parameters.
            timeout (int): Maximum query runtime in milliseconds.

        Returns:
            QueryResult: query result set.

        """

        return self._query(q, params=params, timeout=timeout, read_only=False)

    def ro_query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None) -> QueryResult:
        """
        Executes a read-only query against the graph.
        See: https://docs.falkordb.com/commands/graph.ro_query.html

        Args:
            q (str): The query.
            params (dict): Query parameters.
            timeout (int): Maximum query runtime in milliseconds.

        Returns:
            QueryResult: query result set.

        """

        return self._query(q, params=params, timeout=timeout, read_only=True)

    def delete(self) -> None:
        """
        Deletes the graph.
        See: https://docs.falkordb.com/commands/graph.delete.html

        Returns:
            None

        """

        self.schema.clear()
        return self.execute_command(DELETE_CMD, self._name)

    def slowlog(self):
        """
        Get a list containing up to 10 of the slowest queries issued
        against the graph.

        Each item in the list has the following structure:
        1. a unix timestamp at which the log entry was processed
        2. the issued command
        3. the issued query
        4. the amount of time needed for its execution, in milliseconds.

        See: https://docs.falkordb.com/commands/graph.slowlog.html

        Returns:
            List: List of slow log entries.

        """

        return self.execute_command(SLOWLOG_CMD, self._name)

    def slowlog_reset(self):
        """
        Reset the slowlog.
        See: https://docs.falkordb.com/commands/graph.slowlog.html

        Returns:
            None

        """
        self.execute_command(SLOWLOG_CMD, self._name, "RESET")

    def profile(self, query: str, params=None) -> ExecutionPlan:
        """
        Execute a query and produce an execution plan augmented with metrics
        for each operation's execution. Return an execution plan,
        with details on results produced by and time spent in each operation.
        See: https://docs.falkordb.com/commands/graph.profile.html

        Args:
            query (str): The query to profile.
            params (dict): Query parameters.

        Returns:
            ExecutionPlan: The profile information.

        """

        query = self.__build_params_header(params) + query
        plan = self.execute_command(PROFILE_CMD, self._name, query)
        return ExecutionPlan(plan)

    def explain(self, query: str, params=None) -> ExecutionPlan:
        """
        Get the execution plan for a given query.
        GRAPH.EXPLAIN returns an ExecutionPlan object.
        See: https://docs.falkordb.com/commands/graph.explain.html

        Args:
            query (str): The query for which to get the execution plan.
            params (dict): Query parameters.

        Returns:
            ExecutionPlan: The execution plan.

        """

        query = self.__build_params_header(params) + query

        plan = self.execute_command(EXPLAIN_CMD, self._name, query)
        return ExecutionPlan(plan)

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

        # make sure strings arguments are quoted
        args = args or []
        # args = [quote_string(arg) for arg in args]

        params = None
        if(len(args) > 0):
            params = {}
            # convert arguments to query parameters
            # CALL <proc>(1) -> CYPHER param_0=1 CALL <proc>($param_0)
            for i, arg in enumerate(args):
                param_name = f'param{i}'
                params[param_name] = arg
                args[i] = '$' + param_name

        q = f"CALL {procedure}({','.join(args)})"

        if emit is not None and len(emit) > 0:
            q += f"YIELD {','.join(emit)}"

        return self._query(q, params=params, read_only=read_only)

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
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("RANGE", "NODE", label, attribute)

    def drop_node_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("FULLTEXT", "NODE", label, attribute)

    def drop_node_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("VECTOR", "NODE", label, attribute)

    def drop_edge_range_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a range index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("RANGE", "EDGE", label, attribute)

    def drop_edge_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("FULLTEXT", "EDGE", label, attribute)

    def drop_edge_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return self._drop_index("VECTOR", "EDGE", label, attribute)

    def list_indices(self) -> QueryResult:
        """Retrieve a list of graph indices.
        See: https://docs.falkordb.com/commands/graph.query.html#procedures

        Returns:
            list: List of graph indices.
        """
        return self.call_procedure(GRAPH_INDEXES)

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
        See: https://docs.falkordb.com/commands/graph.query.html#creating-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("RANGE", "NODE", label, *properties)

    def create_node_fulltext_index(self, label: str, *properties) -> QueryResult:
        """Create a full-text index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#creating-a-full-text-index-for-a-node-label

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
        See: https://docs.falkordb.com/commands/graph.query.html#vector-indexing

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
        See: https://docs.falkordb.com/commands/graph.query.html#creating-an-index-for-a-relationship-type

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        return self._create_typed_index("RANGE", "EDGE", relation, *properties)

    def create_edge_fulltext_index(self, relation: str, *properties) -> QueryResult:
        """Create a full-text index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#full-text-indexing

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
        See: https://docs.falkordb.com/commands/graph.query.html#vector-indexing

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
