from typing import List, Dict, Optional
from .graph_schema import GraphSchema
from .query_result import QueryResult

from falkordb.graph          import Graph
from falkordb.helpers        import quote_string, stringify_param_value
from falkordb.exceptions     import SchemaVersionMismatchException
from falkordb.execution_plan import ExecutionPlan

# procedures
GRAPH_INDEXES = "DB.INDEXES"
GRAPH_LIST_CONSTRAINTS = "DB.CONSTRAINTS"

# commands
COPY_CMD      = "GRAPH.COPY"
QUERY_CMD     = "GRAPH.QUERY"
DELETE_CMD    = "GRAPH.DELETE"
EXPLAIN_CMD   = "GRAPH.EXPLAIN"
SLOWLOG_CMD   = "GRAPH.SLOWLOG"
PROFILE_CMD   = "GRAPH.PROFILE"
RO_QUERY_CMD  = "GRAPH.RO_QUERY"


class AsyncGraph(Graph):
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

        super().__init__(client, name)
        self.schema = GraphSchema(self)

    async def _query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None, read_only: bool = False) -> QueryResult:
        """
        Executes a query asynchronously against the graph.
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
        query = self._build_params_header(params) + query

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
            response = await self.execute_command(*command)
            query_result = QueryResult(self)
            await query_result.parse(response)
            return query_result
        except SchemaVersionMismatchException as e:
            # client view over the graph schema is out of sync
            # set client version and refresh local schema
            self.schema.refresh(e.version)
            raise e

    async def query(self, q: str, params: Optional[Dict[str, object]] = None,
              timeout: Optional[int] = None) -> QueryResult:
        """
        Executes a query asynchronously against the graph.
        See: https://docs.falkordb.com/commands/graph.query.html

        Args:
            q (str): The query.
            params (dict): Query parameters.
            timeout (int): Maximum query runtime in milliseconds.

        Returns:
            QueryResult: query result set.

        """

        return await self._query(q, params=params, timeout=timeout, read_only=False)

    async def ro_query(self, q: str, params: Optional[Dict[str, object]] = None,
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

        return await self._query(q, params=params, timeout=timeout, read_only=True)

    async def copy(self, clone: str):
        """
        Creates a copy of graph

        Args:
            clone (str): Name of cloned graph

        Returns:
            AsyncGraph: the cloned graph
        """

        await self.execute_command(COPY_CMD, self._name, clone)
        return AsyncGraph(self.client, clone)

    async def delete(self) -> None:
        """
        Deletes the graph.
        See: https://docs.falkordb.com/commands/graph.delete.html

        Returns:
            None

        """

        self.schema.clear()
        return await self.execute_command(DELETE_CMD, self._name)

    async def slowlog(self):
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

        return await self.execute_command(SLOWLOG_CMD, self._name)

    async def slowlog_reset(self):
        """
        Reset the slowlog.
        See: https://docs.falkordb.com/commands/graph.slowlog.html

        Returns:
            None

        """
        await self.execute_command(SLOWLOG_CMD, self._name, "RESET")

    async def profile(self, query: str, params=None) -> ExecutionPlan:
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

        query = self._build_params_header(params) + query
        plan = await self.execute_command(PROFILE_CMD, self._name, query)
        return ExecutionPlan(plan)

    async def explain(self, query: str, params=None) -> ExecutionPlan:
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

        query = self._build_params_header(params) + query

        plan = await self.execute_command(EXPLAIN_CMD, self._name, query)
        return ExecutionPlan(plan)

    # procedures
    async def call_procedure(self, procedure: str, read_only: bool = True,
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

        return await self._query(q, params=params, read_only=read_only)

    # index operations

    async def _drop_index(self, idx_type: str, entity_type: str, label: str,
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

        return await self.query(q)

    async def drop_node_range_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a range index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("RANGE", "NODE", label, attribute)

    async def drop_node_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("FULLTEXT", "NODE", label, attribute)

    async def drop_node_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("VECTOR", "NODE", label, attribute)

    async def drop_edge_range_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a range index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("RANGE", "EDGE", label, attribute)

    async def drop_edge_fulltext_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a full-text index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("FULLTEXT", "EDGE", label, attribute)

    async def drop_edge_vector_index(self, label: str, attribute: str) -> QueryResult:
        """Drop a vector index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#deleting-an-index-for-a-relationship-type

        Args:
            label (str): The label of the edge.
            attribute (str): The attribute to drop the index on.

        Returns:
            Any: The result of the index dropping query.
        """
        return await self._drop_index("VECTOR", "EDGE", label, attribute)

    async def list_indices(self) -> QueryResult:
        """Retrieve a list of graph indices.
        See: https://docs.falkordb.com/commands/graph.query.html#procedures

        Returns:
            list: List of graph indices.
        """
        return await self.call_procedure(GRAPH_INDEXES)

    async def _create_typed_index(self, idx_type: str, entity_type: str, label: str,
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

        return await self.query(q)

    async def create_node_range_index(self, label: str, *properties) -> QueryResult:
        """Create a range index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#creating-an-index-for-a-node-label

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        res = await self._create_typed_index("RANGE", "NODE", label, *properties)
        return res

    async def create_node_fulltext_index(self, label: str, *properties) -> QueryResult:
        """Create a full-text index for a node.
        See: https://docs.falkordb.com/commands/graph.query.html#creating-a-full-text-index-for-a-node-label

        Args:
            label (str): The label of the node.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        res = await self._create_typed_index("FULLTEXT", "NODE", label, *properties)
        return res

    async def create_node_vector_index(self, label: str, *properties, dim: int = 0,
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
        res = await self._create_typed_index("VECTOR", "NODE", label, *properties, options=options)
        return res

    async def create_edge_range_index(self, relation: str, *properties) -> QueryResult:
        """Create a range index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#creating-an-index-for-a-relationship-type

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        res = await self._create_typed_index("RANGE", "EDGE", relation, *properties)
        return res

    async def create_edge_fulltext_index(self, relation: str, *properties) -> QueryResult:
        """Create a full-text index for an edge.
        See: https://docs.falkordb.com/commands/graph.query.html#full-text-indexing

        Args:
            relation (str): The relation of the edge.
            properties: Variable number of property names to be indexed.

        Returns:
            Any: The result of the index creation query.
        """
        res = await self._create_typed_index("FULLTEXT", "EDGE", relation, *properties)
        return res

    async def create_edge_vector_index(self, relation: str, *properties, dim: int = 0,
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
        res = await self._create_typed_index("VECTOR", "EDGE", relation, *properties, options=options)
        return res

    async def _create_constraint(self, constraint_type: str, entity_type: str, label: str, *properties):
        """
        Create a constraint
        """

        # GRAPH.CONSTRAINT CREATE key constraintType {NODE label | RELATIONSHIP reltype} PROPERTIES propCount prop [prop...]
        return await self.execute_command("GRAPH.CONSTRAINT", "CREATE", self.name,
                                    constraint_type, entity_type, label,
                                    "PROPERTIES", len(properties), *properties)

    async def create_node_unique_constraint(self, label: str, *properties):
        """
        Create node unique constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        The constraint is created asynchronously, use list constraints to pull on
        constraint creation status

        Note: unique constraints require a the existance of a range index
        over the constraint properties, this function will create any missing range indices

        Args:
            label (str): Node label to apply constraint to
            properties: Variable number of property names to constrain
        """

        # create required range indices
        try:
            await self.create_node_range_index(label, *properties)
        except Exception:
            pass

        # create constraint
        return await self._create_constraint("UNIQUE", "NODE", label, *properties)

    async def create_edge_unique_constraint(self, relation: str, *properties):
        """
        Create edge unique constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        The constraint is created asynchronously, use list constraints to pull on
        constraint creation status

        Note: unique constraints require a the existance of a range index
        over the constraint properties, this function will create any missing range indices

        Args:
            relation (str): Edge relationship-type to apply constraint to
            properties: Variable number of property names to constrain
        """

        # create required range indices
        try:
            await self.create_edge_range_index(relation, *properties)
        except Exception:
            pass

        return await self._create_constraint("UNIQUE", "RELATIONSHIP", relation, *properties)

    async def create_node_mandatory_constraint(self, label: str, *properties):
        """
        Create node mandatory constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        The constraint is created asynchronously, use list constraints to pull on
        constraint creation status

        Args:
            label (str): Node label to apply constraint to
            properties: Variable number of property names to constrain
        """

        return await self._create_constraint("MANDATORY", "NODE", label, *properties)

    async def create_edge_mandatory_constraint(self, relation: str, *properties):
        """
        Create edge mandatory constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        The constraint is created asynchronously, use list constraints to pull on
        constraint creation status

        Args:
            relation (str): Edge relationship-type to apply constraint to
            properties: Variable number of property names to constrain
        """
        return await self._create_constraint("MANDATORY", "RELATIONSHIP", relation, *properties)

    async def _drop_constraint(self, constraint_type: str, entity_type: str, label: str, *properties):
        """
        Drops a constraint

        Args:
        constraint_type (str): Type of constraint to drop
        entity_type (str): Type of entity to drop constraint from
        label (str): entity's label / relationship-type
        properties: entity's properties to remove constraint from
        """

        return await self.execute_command("GRAPH.CONSTRAINT", "DROP", self.name,
                                    constraint_type, entity_type, label,
                                    "PROPERTIES", len(properties), *properties)

    async def drop_node_unique_constraint(self, label: str, *properties):
        """
        Drop node unique constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        Note: the constraint supporting range index is not removed

        Args:
            label (str): Node label to remove the constraint from
            properties: properties to remove constraint from
        """

        # drop constraint
        return await self._drop_constraint("UNIQUE", "NODE", label, *properties)

    async def drop_edge_unique_constraint(self, relation: str, *properties):
        """
        Drop edge unique constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        Note: the constraint supporting range index is not removed

        Args:
            label (str): Edge relationship-type to remove the constraint from
            properties: properties to remove constraint from
        """

        return await self._drop_constraint("UNIQUE", "RELATIONSHIP", relation, *properties)

    async def drop_node_mandatory_constraint(self, label: str, *properties):
        """
        Drop node mandatory constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        Args:
            label (str): Node label to remove the constraint from
            properties: properties to remove constraint from
        """

        return await self._drop_constraint("MANDATORY", "NODE", label, *properties)

    async def drop_edge_mandatory_constraint(self, relation: str, *properties):
        """
        Drop edge mandatory constraint
        See: https://docs.falkordb.com/commands/graph.constraint-create.html

        Args:
            label (str): Edge relationship-type to remove the constraint from
            properties: properties to remove constraint from
        """
        return await self._drop_constraint("MANDATORY", "RELATIONSHIP", relation, *properties)

    async def list_constraints(self) -> [Dict[str, object]]:
        """
        Lists graph's constraints

        See: https://docs.falkordb.com/commands/graph.constraint-create.html#listing-constraints

        Returns:
            [Dict[str, object]]: list of constraints
        """

        result = (await self.call_procedure(GRAPH_LIST_CONSTRAINTS)).result_set

        constraints = []
        for row in result:
            constraints.append({"type":       row[0],
                                "label":      row[1],
                                "properties": row[2],
                                "entitytype": row[3],
                                "status":     row[4]})
        return constraints

