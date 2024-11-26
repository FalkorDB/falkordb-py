import sys
from enum import Enum
from typing import List
from collections import OrderedDict

from redis import ResponseError

from .edge import Edge
from .node import Node
from .path import Path
from .exceptions import SchemaVersionMismatchException

# statistics
LABELS_ADDED            = "Labels added"
LABELS_REMOVED          = "Labels removed"
NODES_CREATED           = "Nodes created"
NODES_DELETED           = "Nodes deleted"
PROPERTIES_SET          = "Properties set"
INDICES_CREATED         = "Indices created"
INDICES_DELETED         = "Indices deleted"
CACHED_EXECUTION        = "Cached execution"
PROPERTIES_REMOVED      = "Properties removed"
RELATIONSHIPS_DELETED   = "Relationships deleted"
RELATIONSHIPS_CREATED   = "Relationships created"
INTERNAL_EXECUTION_TIME = "internal execution time"

STATS = [
    LABELS_ADDED,
    NODES_CREATED,
    NODES_DELETED,
    LABELS_REMOVED,
    PROPERTIES_SET,
    INDICES_CREATED,
    INDICES_DELETED,
    CACHED_EXECUTION,
    PROPERTIES_REMOVED,
    RELATIONSHIPS_CREATED,
    RELATIONSHIPS_DELETED,
    INTERNAL_EXECUTION_TIME,
]

class ResultSetScalarTypes(Enum):
    """
    Enumeration representing different scalar types in the query result set.

    Attributes:
        VALUE_UNKNOWN   (int): Unknown scalar type (0)
        VALUE_NULL      (int): Null scalar type    (1)
        VALUE_STRING    (int): String scalar type  (2)
        VALUE_INTEGER   (int): Integer scalar type (3)
        VALUE_BOOLEAN   (int): Boolean scalar type (4)
        VALUE_DOUBLE    (int): Double scalar type  (5)
        VALUE_ARRAY     (int): Array scalar type   (6)
        VALUE_EDGE      (int): Edge scalar type    (7)
        VALUE_NODE      (int): Node scalar type    (8)
        VALUE_PATH      (int): Path scalar type    (9)
        VALUE_MAP       (int): Map scalar type     (10)
        VALUE_POINT     (int): Point scalar type   (11)
        VALUE_VECTORF32 (int): Vector scalar type  (12)
    """

    VALUE_UNKNOWN   = 0
    VALUE_NULL      = 1
    VALUE_STRING    = 2
    VALUE_INTEGER   = 3
    VALUE_BOOLEAN   = 4
    VALUE_DOUBLE    = 5
    VALUE_ARRAY     = 6
    VALUE_EDGE      = 7
    VALUE_NODE      = 8
    VALUE_PATH      = 9
    VALUE_MAP       = 10
    VALUE_POINT     = 11
    VALUE_VECTORF32 = 12

def __parse_unknown(value, graph):
    """
    Parse a value of unknown type.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        None
    """
    sys.stderr.write("Unknown type\n")

def __parse_null(value, graph) -> None:
    """
    Parse a null value.

    Args:
        value: The null value.
        graph: The graph instance.

    Returns:
        None: Always returns None.
    """
    return None

def __parse_string(value, graph) -> str:
    """
    Parse the value as a string.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        str: The parsed string value.
    """
    if isinstance(value, bytes):
        return value.decode()

    if not isinstance(value, str):
        return str(value)

    return value

def __parse_integer(value, graph) -> int:
    """
    Parse the integer value from the value.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        int: The parsed integer value.
    """
    return int(value)

def __parse_boolean(value, graph) -> bool:
    """
    Parse the value as a boolean.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        bool: The parsed boolean value.
    """
    value = value.decode() if isinstance(value, bytes) else value
    return value == "true"

def __parse_double(value, graph) -> float:
    """
    Parse the value as a double.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        float: The parsed double value.
    """
    return float(value)

def __parse_array(value, graph) -> List:
    """
    Parse an array of values.

    Args:
        value: The array value to parse.
        graph: The graph instance.

    Returns:
        list: The parsed list of values.
    """
    scalar = [parse_scalar(value[i], graph) for i in range(len(value))]
    return scalar

def __parse_vectorf32(value, graph) -> List:
    """
    Parse a vector32f.

    Args:
        value: The vector to parse.
        graph: The graph instance.

    Returns:
        list: The parsed vector.
    """

    return [float(v) for v in value]

def __parse_entity_properties(props, graph):
    """
    Parse node/edge properties.

    Args:
        props (List): List of properties.
        graph: The graph instance.

    Returns:
        dict: Dictionary containing parsed properties.
    """
    properties = {}
    for prop in props:
        prop_name = graph.schema.get_property(prop[0])
        prop_value = parse_scalar(prop[1:], graph)
        properties[prop_name] = prop_value

    return properties

def __parse_node(value, graph) -> Node:
    """
    Parse the value to a node.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        Node: The parsed Node instance.
    """
    node_id = int(value[0])
    labels = None
    if len(value[1]) > 0:
        labels = [graph.schema.get_label(inner_label) for inner_label in value[1]]
    properties = __parse_entity_properties(value[2], graph)
    return Node(node_id=node_id, alias="", labels=labels, properties=properties)

def __parse_edge(value, graph) -> Edge:
    """
    Parse the value to an edge.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        Edge: The parsed Edge instance.
    """
    edge_id = int(value[0])
    relation = graph.schema.get_relation(value[1])
    src_node_id = int(value[2])
    dest_node_id = int(value[3])
    properties = __parse_entity_properties(value[4], graph)
    return Edge(src_node_id, relation, dest_node_id, edge_id=edge_id, properties=properties)

def __parse_path(value, graph) -> Path:
    """
    Parse the value to a path.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        Path: The parsed Path instance.
    """
    nodes = parse_scalar(value[0], graph)
    edges = parse_scalar(value[1], graph)
    return Path(nodes, edges)

def __parse_map(value, graph) -> OrderedDict:
    """
    Parse the value as a map.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        OrderedDict: The parsed OrderedDict.
    """
    m = OrderedDict()
    n_entries = len(value)

    for i in range(0, n_entries, 2):
        key = __parse_string(value[i], graph)
        m[key] = parse_scalar(value[i + 1], graph)

    return m

def __parse_point(value, graph):
    """
    Parse the value to point.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        dict: The parsed dictionary representing a point.
    """
    p = {"latitude": float(value[0]), "longitude": float(value[1])}
    return p

def parse_scalar(value, graph):
    """
    Parse a scalar value from a value in the result set.

    Args:
        value: The value to parse.
        graph: The graph instance.

    Returns:
        Any: The parsed scalar value.
    """
    scalar_type = int(value[0])
    value = value[1]
    scalar = PARSE_SCALAR_TYPES[scalar_type](value, graph)

    return scalar


PARSE_SCALAR_TYPES = [
    __parse_unknown,  # VALUE_UNKNOWN
    __parse_null,     # VALUE_NULL
    __parse_string,   # VALUE_STRING
    __parse_integer,  # VALUE_INTEGER
    __parse_boolean,  # VALUE_BOOLEAN
    __parse_double,   # VALUE_DOUBLE
    __parse_array,    # VALUE_ARRAY
    __parse_edge,     # VALUE_EDGE
    __parse_node,     # VALUE_NODE
    __parse_path,     # VALUE_PATH
    __parse_map,      # VALUE_MAP
    __parse_point,    # VALUE_POINT
    __parse_vectorf32 # VALUE_VECTORF32
]

class QueryResult:
    """
        Represents the result of a query operation on a graph.
    """
    def __init__(self, graph, response):
        """
        Initializes a QueryResult instance.

        Args:
            graph: The graph on which the query was executed.
            response: The response from the server.
        """
        self.graph      = graph
        self.header     = []
        self.result_set = []
        self._raw_stats = []

        # in case of an error, an exception will be raised
        self.__check_for_errors(response)

        if len(response) == 1:
            self._raw_stats = response[0]
        else:
            # start by parsing statistics, matches the one we have
            self._raw_stats = response[-1]
            self.__parse_results(response)

    def __check_for_errors(self, response):
        """
        Checks if the response contains an error.

        Args:
            response: The response from the server.

        Raises:
            ResponseError: If an error is encountered.
        """
        if isinstance(response[0], ResponseError):
            error = response[0]
            if str(error) == "version mismatch":
                version = response[1]
                error = VersionMismatchException(version)
            raise error

        # if we encountered a run-time error, the last response
        # element will be an exception
        if isinstance(response[-1], ResponseError):
            raise response[-1]

    def __parse_results(self, raw_result_set):
        """
        Parse the query execution result returned from the server.

        Args:
            raw_result_set: The raw result set from the server.
        """
        self.header = self.__parse_header(raw_result_set)

        # empty header
        if len(self.header) == 0:
            return

        self.result_set = self.__parse_records(raw_result_set)

    def __get_statistics(self, s):
        """
        Get the value of a specific statistical metric.

        Args:
            s (str): The statistical metric to retrieve.

        Returns:
            float: The value of the specified statistical metric. Returns 0 if the metric is not found.
        """
        for stat in self._raw_stats:
            if s in stat:
                return float(stat.split(": ")[1].split(" ")[0])

        return 0

    def __parse_header(self, raw_result_set):
        """
        Parse the header of the result.

        Args:
            raw_result_set: The raw result set from the server.

        Returns:
            list: An array of column name/column type pairs.
        """
        # an array of column name/column type pairs
        header = raw_result_set[0]
        return header

    def __parse_records(self, raw_result_set):
        """
        Parses the result set and returns a list of records.

        Args:
            raw_result_set: The raw result set from the server.

        Returns:
            list: A list of records.
        """
        records = [
            [parse_scalar(cell, self.graph) for cell in row]
            for row in raw_result_set[1]
        ]

        return records

    @property
    def labels_added(self) -> int:
        """
        Get the number of labels added in the query.

        Returns:
        int: The number of labels added.
        """

        return self.__get_statistics(LABELS_ADDED)

    @property
    def labels_removed(self) -> int:
        """
        Get the number of labels removed in the query.

        Returns:
            int: The number of labels removed.
        """
        return self.__get_statistics(LABELS_REMOVED)

    @property
    def nodes_created(self) -> int:
        """
        Get the number of nodes created in the query.

        Returns:
            int: The number of nodes created.
        """
        return self.__get_statistics(NODES_CREATED)

    @property
    def nodes_deleted(self) -> int:
        """
        Get the number of nodes deleted in the query.

        Returns:
            int: The number of nodes deleted.
        """
        return self.__get_statistics(NODES_DELETED)

    @property
    def properties_set(self) -> int:
        """
        Get the number of properties set in the query.

        Returns:
            int: The number of properties set.
        """
        return self.__get_statistics(PROPERTIES_SET)

    @property
    def properties_removed(self) -> int:
        """
        Get the number of properties removed in the query.

        Returns:
            int: The number of properties removed.
        """
        return self.__get_statistics(PROPERTIES_REMOVED)

    @property
    def relationships_created(self) -> int:
        """
        Get the number of relationships created in the query.

        Returns:
            int: The number of relationships created.
        """
        return self.__get_statistics(RELATIONSHIPS_CREATED)

    @property
    def relationships_deleted(self) -> int:
        """
        Get the number of relationships deleted in the query.

        Returns:
            int: The number of relationships deleted.
        """
        return self.__get_statistics(RELATIONSHIPS_DELETED)

    @property
    def indices_created(self) -> int:
        """
        Get the number of indices created in the query.

        Returns:
            int: The number of indices created.
        """
        return self.__get_statistics(INDICES_CREATED)

    @property
    def indices_deleted(self) -> int:
        """
        Get the number of indices deleted in the query.

        Returns:
            int: The number of indices deleted.
        """
        return self.__get_statistics(INDICES_DELETED)

    @property
    def cached_execution(self) -> bool:
        """
        Check if the query execution plan was cached.

        Returns:
            bool: True if the query execution plan was cached, False otherwise.
        """
        return self.__get_statistics(CACHED_EXECUTION) == 1

    @property
    def run_time_ms(self) -> float:
        """
        Get the server execution time of the query.

        Returns:
            float: The server execution time of the query in milliseconds.
        """
        return self.__get_statistics(INTERNAL_EXECUTION_TIME)
