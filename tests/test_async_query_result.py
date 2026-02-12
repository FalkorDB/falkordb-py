import pytest
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
from unittest.mock import AsyncMock, MagicMock, patch
from redis import ResponseError

from falkordb.asyncio.query_result import (
    QueryResult,
    ResultSetScalarTypes,
    parse_scalar,
    __parse_unknown,
    __parse_null,
    __parse_string,
    __parse_integer,
    __parse_boolean,
    __parse_double,
    __parse_array,
    __parse_vectorf32,
    __parse_datetime,
    __parse_date,
    __parse_time,
    __parse_duration,
    __parse_entity_properties,
    __parse_node,
    __parse_edge,
    __parse_path,
    __parse_map,
    __parse_point,
)
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb import Node, Edge, Path


# Helper function to create a mock graph with schema
def create_mock_graph():
    """Create a mock graph object with a mock schema."""
    graph = MagicMock()
    schema = MagicMock()

    # Mock schema methods to return async coroutines
    async def get_label_mock(idx):
        labels = ["person", "country", "L1", "L2"]
        return labels[idx] if idx < len(labels) else f"label_{idx}"

    async def get_property_mock(idx):
        props = ["name", "age", "value", "gender"]
        return props[idx] if idx < len(props) else f"prop_{idx}"

    async def get_relation_mock(idx):
        relations = ["visited", "R1", "KNOWS"]
        return relations[idx] if idx < len(relations) else f"rel_{idx}"

    schema.get_label = AsyncMock(side_effect=get_label_mock)
    schema.get_property = AsyncMock(side_effect=get_property_mock)
    schema.get_relation = AsyncMock(side_effect=get_relation_mock)

    graph.schema = schema
    return graph


# Test parse_unknown
@pytest.mark.asyncio
async def test_parse_unknown(capsys):
    """Test parsing unknown scalar type."""
    graph = create_mock_graph()
    result = await __parse_unknown("unknown_value", graph)
    captured = capsys.readouterr()
    assert "Unknown type" in captured.err


# Test parse_null
@pytest.mark.asyncio
async def test_parse_null():
    """Test parsing null scalar type."""
    graph = create_mock_graph()
    result = await __parse_null(None, graph)
    assert result is None


# Test parse_string
@pytest.mark.asyncio
async def test_parse_string():
    """Test parsing string scalar type."""
    graph = create_mock_graph()

    # Test regular string
    result = await __parse_string("hello", graph)
    assert result == "hello"

    # Test bytes
    result = await __parse_string(b"hello", graph)
    assert result == "hello"

    # Test non-string type
    result = await __parse_string(123, graph)
    assert result == "123"


# Test parse_integer
@pytest.mark.asyncio
async def test_parse_integer():
    """Test parsing integer scalar type."""
    graph = create_mock_graph()

    result = await __parse_integer(42, graph)
    assert result == 42

    result = await __parse_integer("123", graph)
    assert result == 123


# Test parse_boolean
@pytest.mark.asyncio
async def test_parse_boolean():
    """Test parsing boolean scalar type."""
    graph = create_mock_graph()

    result = await __parse_boolean("true", graph)
    assert result is True

    result = await __parse_boolean("false", graph)
    assert result is False

    result = await __parse_boolean(b"true", graph)
    assert result is True

    result = await __parse_boolean(b"false", graph)
    assert result is False


# Test parse_double
@pytest.mark.asyncio
async def test_parse_double():
    """Test parsing double scalar type."""
    graph = create_mock_graph()

    result = await __parse_double(3.14, graph)
    assert result == 3.14

    result = await __parse_double("2.718", graph)
    assert result == 2.718


# Test parse_array
@pytest.mark.asyncio
async def test_parse_array():
    """Test parsing array scalar type."""
    graph = create_mock_graph()

    # Array with integers
    value = [
        [ResultSetScalarTypes.VALUE_INTEGER.value, 1],
        [ResultSetScalarTypes.VALUE_INTEGER.value, 2],
        [ResultSetScalarTypes.VALUE_INTEGER.value, 3],
    ]
    result = await __parse_array(value, graph)
    assert result == [1, 2, 3]

    # Array with mixed types
    value = [
        [ResultSetScalarTypes.VALUE_INTEGER.value, 1],
        [ResultSetScalarTypes.VALUE_STRING.value, "hello"],
        [ResultSetScalarTypes.VALUE_BOOLEAN.value, "true"],
    ]
    result = await __parse_array(value, graph)
    assert result == [1, "hello", True]


# Test parse_vectorf32
@pytest.mark.asyncio
async def test_parse_vectorf32():
    """Test parsing vector float32 scalar type."""
    graph = create_mock_graph()

    result = await __parse_vectorf32([1.0, 2.5, 3.14], graph)
    assert result == [1.0, 2.5, 3.14]

    result = await __parse_vectorf32([1, 2, 3], graph)
    assert result == [1.0, 2.0, 3.0]


# Test parse_datetime
@pytest.mark.asyncio
async def test_parse_datetime():
    """Test parsing datetime scalar type."""
    graph = create_mock_graph()

    timestamp = 1609459200  # 2021-01-01 00:00:00 UTC
    result = await __parse_datetime(timestamp, graph)
    expected = datetime.utcfromtimestamp(timestamp)
    assert result == expected


# Test parse_date
@pytest.mark.asyncio
async def test_parse_date():
    """Test parsing date scalar type."""
    graph = create_mock_graph()

    timestamp = 1609459200  # 2021-01-01
    result = await __parse_date(timestamp, graph)
    expected = datetime.utcfromtimestamp(timestamp).date()
    assert result == expected


# Test parse_time
@pytest.mark.asyncio
async def test_parse_time():
    """Test parsing time scalar type."""
    graph = create_mock_graph()

    timestamp = 3661  # 01:01:01
    result = await __parse_time(timestamp, graph)
    expected = datetime.utcfromtimestamp(timestamp).time()
    assert result == expected


# Test parse_duration
@pytest.mark.asyncio
async def test_parse_duration():
    """Test parsing duration scalar type."""
    graph = create_mock_graph()

    # 1 day in seconds
    timestamp = 86400
    result = await __parse_duration(timestamp, graph)
    expected_timestamp = datetime.utcfromtimestamp(timestamp)
    epoch = datetime(1970, 1, 1)
    expected = relativedelta(expected_timestamp, epoch)

    # Check that days are approximately equal
    assert result.days == expected.days


# Test parse_entity_properties
@pytest.mark.asyncio
async def test_parse_entity_properties():
    """Test parsing entity properties."""
    graph = create_mock_graph()

    props = [
        [0, ResultSetScalarTypes.VALUE_STRING.value, "John"],
        [1, ResultSetScalarTypes.VALUE_INTEGER.value, 30],
    ]

    result = await __parse_entity_properties(props, graph)
    assert result == {"name": "John", "age": 30}


# Test parse_node
@pytest.mark.asyncio
async def test_parse_node():
    """Test parsing node scalar type."""
    graph = create_mock_graph()

    # Node with single label
    value = [
        1,  # node_id
        [0],  # labels indices
        [[0, ResultSetScalarTypes.VALUE_STRING.value, "John"]],  # properties
    ]

    result = await __parse_node(value, graph)
    assert isinstance(result, Node)
    assert result.id == 1
    assert result.labels == ["person"]
    assert result.properties == {"name": "John"}

    # Node with no labels
    value = [
        2,  # node_id
        [],  # no labels
        [],  # no properties
    ]

    result = await __parse_node(value, graph)
    assert isinstance(result, Node)
    assert result.id == 2
    assert result.labels is None
    assert result.properties == {}


# Test parse_edge
@pytest.mark.asyncio
async def test_parse_edge():
    """Test parsing edge scalar type."""
    graph = create_mock_graph()

    value = [
        10,  # edge_id
        0,   # relation index
        1,   # src_node_id
        2,   # dest_node_id
        [[0, ResultSetScalarTypes.VALUE_STRING.value, "pleasure"]],  # properties
    ]

    result = await __parse_edge(value, graph)
    assert isinstance(result, Edge)
    assert result.id == 10
    assert result.relation == "visited"
    assert result.src_node == 1
    assert result.dest_node == 2
    assert result.properties == {"name": "pleasure"}


# Test parse_path
@pytest.mark.asyncio
async def test_parse_path():
    """Test parsing path scalar type."""
    graph = create_mock_graph()

    # Create nodes array
    nodes_array = [
        [ResultSetScalarTypes.VALUE_NODE.value, [0, [0], []]],
        [ResultSetScalarTypes.VALUE_NODE.value, [1, [1], []]],
    ]

    # Create edges array
    edges_array = [
        [ResultSetScalarTypes.VALUE_EDGE.value, [0, 0, 0, 1, []]],
    ]

    # Path value contains two scalar arrays (nodes and edges)
    value = [
        [ResultSetScalarTypes.VALUE_ARRAY.value, nodes_array],
        [ResultSetScalarTypes.VALUE_ARRAY.value, edges_array]
    ]

    result = await __parse_path(value, graph)
    assert isinstance(result, Path)
    assert len(result.nodes()) == 2
    assert len(result.edges()) == 1


# Test parse_map
@pytest.mark.asyncio
async def test_parse_map():
    """Test parsing map scalar type."""
    graph = create_mock_graph()

    # Map with key-value pairs
    value = [
        "name", [ResultSetScalarTypes.VALUE_STRING.value, "John"],
        "age", [ResultSetScalarTypes.VALUE_INTEGER.value, 30],
    ]

    result = await __parse_map(value, graph)
    assert isinstance(result, OrderedDict)
    assert result["name"] == "John"
    assert result["age"] == 30


# Test parse_point
@pytest.mark.asyncio
async def test_parse_point():
    """Test parsing point scalar type."""
    graph = create_mock_graph()

    value = [32.070794860, 34.820751118]
    result = await __parse_point(value, graph)

    assert isinstance(result, dict)
    assert "latitude" in result
    assert "longitude" in result
    assert result["latitude"] == 32.070794860
    assert result["longitude"] == 34.820751118


# Test parse_scalar dispatcher
@pytest.mark.asyncio
async def test_parse_scalar():
    """Test the parse_scalar dispatcher function."""
    graph = create_mock_graph()

    # Test null
    result = await parse_scalar([ResultSetScalarTypes.VALUE_NULL.value, None], graph)
    assert result is None

    # Test string
    result = await parse_scalar([ResultSetScalarTypes.VALUE_STRING.value, "hello"], graph)
    assert result == "hello"

    # Test integer
    result = await parse_scalar([ResultSetScalarTypes.VALUE_INTEGER.value, 42], graph)
    assert result == 42

    # Test boolean
    result = await parse_scalar([ResultSetScalarTypes.VALUE_BOOLEAN.value, "true"], graph)
    assert result is True

    # Test double
    result = await parse_scalar([ResultSetScalarTypes.VALUE_DOUBLE.value, 3.14], graph)
    assert result == 3.14


# Test QueryResult initialization
@pytest.mark.asyncio
async def test_query_result_init():
    """Test QueryResult initialization."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    assert qr.graph == graph
    assert qr.header == []
    assert qr.result_set == []
    assert qr._raw_stats == []


# Test QueryResult with empty response (only statistics)
@pytest.mark.asyncio
async def test_query_result_parse_empty():
    """Test parsing response with only statistics."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    response = [
        [
            "Nodes created: 5",
            "Properties set: 10",
            "internal execution time: 1.234 milliseconds",
        ]
    ]

    await qr.parse(response)

    assert qr.nodes_created == 5
    assert qr.properties_set == 10
    assert qr.run_time_ms == 1.234


# Test QueryResult with full response
@pytest.mark.asyncio
async def test_query_result_parse_full():
    """Test parsing full response with header, results, and statistics."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    response = [
        [["name", 0], ["age", 0]],  # header
        [  # results
            [
                [ResultSetScalarTypes.VALUE_STRING.value, "John"],
                [ResultSetScalarTypes.VALUE_INTEGER.value, 30],
            ],
            [
                [ResultSetScalarTypes.VALUE_STRING.value, "Jane"],
                [ResultSetScalarTypes.VALUE_INTEGER.value, 25],
            ],
        ],
        [  # statistics
            "Cached execution: 1",
            "internal execution time: 0.5 milliseconds",
        ]
    ]

    await qr.parse(response)

    assert len(qr.header) == 2
    assert qr.header[0][0] == "name"
    assert qr.header[1][0] == "age"

    assert len(qr.result_set) == 2
    assert qr.result_set[0] == ["John", 30]
    assert qr.result_set[1] == ["Jane", 25]

    assert qr.cached_execution is True
    assert qr.run_time_ms == 0.5


# Test QueryResult statistics properties
@pytest.mark.asyncio
async def test_query_result_statistics():
    """Test all statistics properties."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    response = [
        [
            "Labels added: 2",
            "Labels removed: 1",
            "Nodes created: 5",
            "Nodes deleted: 3",
            "Properties set: 10",
            "Properties removed: 2",
            "Relationships created: 8",
            "Relationships deleted: 4",
            "Indices created: 1",
            "Indices deleted: 0",
            "Cached execution: 0",
            "internal execution time: 2.5 milliseconds",
        ]
    ]

    await qr.parse(response)

    assert qr.labels_added == 2
    assert qr.labels_removed == 1
    assert qr.nodes_created == 5
    assert qr.nodes_deleted == 3
    assert qr.properties_set == 10
    assert qr.properties_removed == 2
    assert qr.relationships_created == 8
    assert qr.relationships_deleted == 4
    assert qr.indices_created == 1
    assert qr.indices_deleted == 0
    assert qr.cached_execution is False
    assert qr.run_time_ms == 2.5


# Test QueryResult with ResponseError
@pytest.mark.asyncio
async def test_query_result_response_error():
    """Test handling of ResponseError."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    error = ResponseError("Test error")
    response = [error]

    with pytest.raises(ResponseError):
        await qr.parse(response)


# Test QueryResult with SchemaVersionMismatchException
@pytest.mark.asyncio
async def test_query_result_schema_version_mismatch():
    """Test handling of schema version mismatch."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    error = ResponseError("version mismatch")
    response = [error, 5]  # version 5

    with pytest.raises(SchemaVersionMismatchException) as exc_info:
        await qr.parse(response)

    assert exc_info.value.version == 5


# Test QueryResult with error at end of response
@pytest.mark.asyncio
async def test_query_result_error_at_end():
    """Test handling of error at end of response."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    error = ResponseError("Runtime error")
    response = [[], [], error]

    with pytest.raises(ResponseError):
        await qr.parse(response)


# Test QueryResult with empty header
@pytest.mark.asyncio
async def test_query_result_empty_header():
    """Test parsing response with empty header."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    response = [
        [],  # empty header
        [],  # empty results
        ["internal execution time: 0.1 milliseconds"]
    ]

    await qr.parse(response)

    assert qr.header == []
    assert qr.result_set == []
    assert qr.run_time_ms == 0.1


# Test statistics when stat is not found
@pytest.mark.asyncio
async def test_query_result_missing_statistics():
    """Test statistics properties when stat is not found."""
    graph = create_mock_graph()
    qr = QueryResult(graph)

    response = [["internal execution time: 1.0 milliseconds"]]

    await qr.parse(response)

    # All other statistics should return 0
    assert qr.nodes_created == 0
    assert qr.labels_added == 0
    assert qr.properties_set == 0


# Test complex nested structures
@pytest.mark.asyncio
async def test_parse_nested_structures():
    """Test parsing complex nested structures."""
    graph = create_mock_graph()

    # Array containing maps
    value = [
        [ResultSetScalarTypes.VALUE_MAP.value, [
            "key1", [ResultSetScalarTypes.VALUE_STRING.value, "value1"],
        ]],
        [ResultSetScalarTypes.VALUE_MAP.value, [
            "key2", [ResultSetScalarTypes.VALUE_INTEGER.value, 42],
        ]],
    ]

    result = await __parse_array(value, graph)
    assert len(result) == 2
    assert result[0]["key1"] == "value1"
    assert result[1]["key2"] == 42


# Test map with nested arrays
@pytest.mark.asyncio
async def test_parse_map_with_arrays():
    """Test parsing map containing arrays."""
    graph = create_mock_graph()

    value = [
        "numbers", [ResultSetScalarTypes.VALUE_ARRAY.value, [
            [ResultSetScalarTypes.VALUE_INTEGER.value, 1],
            [ResultSetScalarTypes.VALUE_INTEGER.value, 2],
            [ResultSetScalarTypes.VALUE_INTEGER.value, 3],
        ]],
        "name", [ResultSetScalarTypes.VALUE_STRING.value, "test"],
    ]

    result = await __parse_map(value, graph)
    assert result["numbers"] == [1, 2, 3]
    assert result["name"] == "test"


# Test node with multiple labels
@pytest.mark.asyncio
async def test_parse_node_multiple_labels():
    """Test parsing node with multiple labels."""
    graph = create_mock_graph()

    value = [
        5,  # node_id
        [0, 1, 2],  # multiple label indices
        [[0, ResultSetScalarTypes.VALUE_STRING.value, "test"]],
    ]

    result = await __parse_node(value, graph)
    assert isinstance(result, Node)
    assert result.id == 5
    assert len(result.labels) == 3
    assert "person" in result.labels
    assert "country" in result.labels


# Test edge with empty properties
@pytest.mark.asyncio
async def test_parse_edge_empty_properties():
    """Test parsing edge with no properties."""
    graph = create_mock_graph()

    value = [
        20,  # edge_id
        1,   # relation index
        10,  # src_node_id
        11,  # dest_node_id
        [],  # empty properties
    ]

    result = await __parse_edge(value, graph)
    assert isinstance(result, Edge)
    assert result.id == 20
    assert result.properties == {}


# Test array with null values
@pytest.mark.asyncio
async def test_parse_array_with_nulls():
    """Test parsing array containing null values."""
    graph = create_mock_graph()

    value = [
        [ResultSetScalarTypes.VALUE_INTEGER.value, 1],
        [ResultSetScalarTypes.VALUE_NULL.value, None],
        [ResultSetScalarTypes.VALUE_STRING.value, "test"],
        [ResultSetScalarTypes.VALUE_NULL.value, None],
    ]

    result = await __parse_array(value, graph)
    assert result == [1, None, "test", None]


# Test boundary values for numeric types
@pytest.mark.asyncio
async def test_parse_numeric_boundaries():
    """Test parsing boundary values for numeric types."""
    graph = create_mock_graph()

    # Very large integer
    result = await __parse_integer(9223372036854775807, graph)
    assert result == 9223372036854775807

    # Very small integer
    result = await __parse_integer(-9223372036854775808, graph)
    assert result == -9223372036854775808

    # Very small float
    result = await __parse_double(1e-308, graph)
    assert result == 1e-308

    # Very large float
    result = await __parse_double(1e308, graph)
    assert result == 1e308

    # Zero
    result = await __parse_integer(0, graph)
    assert result == 0

    result = await __parse_double(0.0, graph)
    assert result == 0.0


# Test empty map
@pytest.mark.asyncio
async def test_parse_empty_map():
    """Test parsing empty map."""
    graph = create_mock_graph()

    value = []
    result = await __parse_map(value, graph)

    assert isinstance(result, OrderedDict)
    assert len(result) == 0


# Test empty vector
@pytest.mark.asyncio
async def test_parse_empty_vectorf32():
    """Test parsing empty vector."""
    graph = create_mock_graph()

    result = await __parse_vectorf32([], graph)
    assert result == []


# Test ResultSetScalarTypes enum values
def test_result_set_scalar_types_enum():
    """Test ResultSetScalarTypes enum values."""
    assert ResultSetScalarTypes.VALUE_UNKNOWN.value == 0
    assert ResultSetScalarTypes.VALUE_NULL.value == 1
    assert ResultSetScalarTypes.VALUE_STRING.value == 2
    assert ResultSetScalarTypes.VALUE_INTEGER.value == 3
    assert ResultSetScalarTypes.VALUE_BOOLEAN.value == 4
    assert ResultSetScalarTypes.VALUE_DOUBLE.value == 5
    assert ResultSetScalarTypes.VALUE_ARRAY.value == 6
    assert ResultSetScalarTypes.VALUE_EDGE.value == 7
    assert ResultSetScalarTypes.VALUE_NODE.value == 8
    assert ResultSetScalarTypes.VALUE_PATH.value == 9
    assert ResultSetScalarTypes.VALUE_MAP.value == 10
    assert ResultSetScalarTypes.VALUE_POINT.value == 11
    assert ResultSetScalarTypes.VALUE_VECTORF32.value == 12
    assert ResultSetScalarTypes.VALUE_DATETIME.value == 13
    assert ResultSetScalarTypes.VALUE_DATE.value == 14
    assert ResultSetScalarTypes.VALUE_TIME.value == 15
    assert ResultSetScalarTypes.VALUE_DURATION.value == 16