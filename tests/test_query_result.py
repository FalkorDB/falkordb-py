import pytest
from unittest.mock import Mock
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
from redis import ResponseError

from falkordb.query_result import (
    QueryResult,
    ResultSetScalarTypes,
    parse_scalar,
    STATS,
    LABELS_ADDED,
    NODES_CREATED,
    PROPERTIES_SET,
    RELATIONSHIPS_CREATED,
    INTERNAL_EXECUTION_TIME
)
from falkordb.node import Node
from falkordb.edge import Edge
from falkordb.path import Path
from falkordb.exceptions import SchemaVersionMismatchException


class TestResultSetScalarTypes:
    """Tests for ResultSetScalarTypes enum."""
    
    def test_enum_values(self):
        """Test enum values are correctly defined."""
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


class TestParseScalarFunctions:
    """Tests for parse_scalar and related functions."""
    
    def test_parse_null(self):
        """Test parsing null values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_NULL.value, None]
        result = parse_scalar(value, mock_graph)
        assert result is None
    
    def test_parse_string(self):
        """Test parsing string values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_STRING.value, "hello"]
        result = parse_scalar(value, mock_graph)
        assert result == "hello"
    
    def test_parse_string_from_bytes(self):
        """Test parsing string from bytes."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_STRING.value, b"hello"]
        result = parse_scalar(value, mock_graph)
        assert result == "hello"
    
    def test_parse_integer(self):
        """Test parsing integer values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_INTEGER.value, 42]
        result = parse_scalar(value, mock_graph)
        assert result == 42
    
    def test_parse_boolean_true(self):
        """Test parsing boolean true."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_BOOLEAN.value, "true"]
        result = parse_scalar(value, mock_graph)
        assert result is True
    
    def test_parse_boolean_false(self):
        """Test parsing boolean false."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_BOOLEAN.value, "false"]
        result = parse_scalar(value, mock_graph)
        assert result is False
    
    def test_parse_boolean_from_bytes(self):
        """Test parsing boolean from bytes."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_BOOLEAN.value, b"true"]
        result = parse_scalar(value, mock_graph)
        assert result is True
    
    def test_parse_double(self):
        """Test parsing double values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_DOUBLE.value, 3.14]
        result = parse_scalar(value, mock_graph)
        assert result == 3.14
    
    def test_parse_array(self):
        """Test parsing array values."""
        mock_graph = Mock()
        # Array with integers
        value = [ResultSetScalarTypes.VALUE_ARRAY.value, [
            [ResultSetScalarTypes.VALUE_INTEGER.value, 1],
            [ResultSetScalarTypes.VALUE_INTEGER.value, 2],
            [ResultSetScalarTypes.VALUE_INTEGER.value, 3]
        ]]
        result = parse_scalar(value, mock_graph)
        assert result == [1, 2, 3]
    
    def test_parse_empty_array(self):
        """Test parsing empty array."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_ARRAY.value, []]
        result = parse_scalar(value, mock_graph)
        assert result == []
    
    def test_parse_vectorf32(self):
        """Test parsing vectorf32 values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_VECTORF32.value, [1.0, 2.5, 3.7]]
        result = parse_scalar(value, mock_graph)
        assert result == [1.0, 2.5, 3.7]
    
    def test_parse_point(self):
        """Test parsing point values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_POINT.value, [40.7128, -74.0060]]
        result = parse_scalar(value, mock_graph)
        assert result == {"latitude": 40.7128, "longitude": -74.0060}
    
    def test_parse_map(self):
        """Test parsing map values."""
        mock_graph = Mock()
        value = [ResultSetScalarTypes.VALUE_MAP.value, [
            "name",
            [ResultSetScalarTypes.VALUE_STRING.value, "John"],
            "age",
            [ResultSetScalarTypes.VALUE_INTEGER.value, 30]
        ]]
        result = parse_scalar(value, mock_graph)
        assert isinstance(result, OrderedDict)
        assert result["name"] == "John"
        assert result["age"] == 30
    
    def test_parse_node(self):
        """Test parsing node values."""
        mock_graph = Mock()
        mock_graph.schema.get_label.return_value = "Person"
        
        # Create a more explicit mock for get_property
        def get_property(idx):
            properties = {0: "name", 1: "age"}
            return properties.get(idx, f"prop_{idx}")
        mock_graph.schema.get_property.side_effect = get_property
        
        # Node format: [node_id, [label_ids], [[prop_id, scalar_type, scalar_value], ...]]
        value = [ResultSetScalarTypes.VALUE_NODE.value, [
            1,  # node_id
            [0],  # label_ids
            [
                [0, ResultSetScalarTypes.VALUE_STRING.value, "John"],
                [1, ResultSetScalarTypes.VALUE_INTEGER.value, 30]
            ]
        ]]
        
        result = parse_scalar(value, mock_graph)
        assert isinstance(result, Node)
        assert result.id == 1
        assert "Person" in result.labels
        assert result.properties["name"] == "John"
        assert result.properties["age"] == 30
    
    def test_parse_edge(self):
        """Test parsing edge values."""
        mock_graph = Mock()
        mock_graph.schema.get_relation.return_value = "KNOWS"
        mock_graph.schema.get_property.return_value = "since"
        
        # Edge format: [edge_id, relation_id, src_node_id, dest_node_id, [[prop_id, scalar_type, scalar_value], ...]]
        value = [ResultSetScalarTypes.VALUE_EDGE.value, [
            1,  # edge_id
            0,  # relation_id
            2,  # src_node_id
            3,  # dest_node_id
            [
                [0, ResultSetScalarTypes.VALUE_INTEGER.value, 2020]
            ]
        ]]
        
        result = parse_scalar(value, mock_graph)
        assert isinstance(result, Edge)
        assert result.id == 1
        assert result.relation == "KNOWS"
        assert result.src_node == 2
        assert result.dest_node == 3
        assert result.properties["since"] == 2020
    
    def test_parse_path(self):
        """Test parsing path values."""
        mock_graph = Mock()
        mock_graph.schema.get_label.return_value = "Person"
        mock_graph.schema.get_relation.return_value = "KNOWS"
        mock_graph.schema.get_property.return_value = "name"
        
        # Path format: [nodes_array, edges_array]
        value = [ResultSetScalarTypes.VALUE_PATH.value, [
            [ResultSetScalarTypes.VALUE_ARRAY.value, [
                [ResultSetScalarTypes.VALUE_NODE.value, [1, [0], []]],
                [ResultSetScalarTypes.VALUE_NODE.value, [2, [0], []]]
            ]],
            [ResultSetScalarTypes.VALUE_ARRAY.value, [
                [ResultSetScalarTypes.VALUE_EDGE.value, [1, 0, 1, 2, []]]
            ]]
        ]]
        
        result = parse_scalar(value, mock_graph)
        assert isinstance(result, Path)


class TestQueryResult:
    """Tests for QueryResult class."""
    
    def test_query_result_statistics_only(self):
        """Test QueryResult with statistics only (no result set)."""
        mock_graph = Mock()
        response = [
            [
                "Nodes created: 1",
                "Properties set: 2",
                "internal execution time: 1.5 milliseconds"
            ]
        ]
        
        result = QueryResult(mock_graph, response)
        
        assert result.header == []
        assert result.result_set == []
        assert result.nodes_created == 1
        assert result.properties_set == 2
        assert result.run_time_ms == 1.5
    
    def test_query_result_with_results(self):
        """Test QueryResult with results and statistics."""
        mock_graph = Mock()
        response = [
            [["name", "age"]],  # header
            [  # result rows
                [[ResultSetScalarTypes.VALUE_STRING.value, "John"], [ResultSetScalarTypes.VALUE_INTEGER.value, 30]],
                [[ResultSetScalarTypes.VALUE_STRING.value, "Jane"], [ResultSetScalarTypes.VALUE_INTEGER.value, 25]]
            ],
            [  # statistics
                "Nodes created: 2",
                "internal execution time: 2.5 milliseconds"
            ]
        ]
        
        result = QueryResult(mock_graph, response)
        
        assert result.header == [["name", "age"]]
        assert len(result.result_set) == 2
        assert result.result_set[0] == ["John", 30]
        assert result.result_set[1] == ["Jane", 25]
        assert result.nodes_created == 2
        assert result.run_time_ms == 2.5
    
    def test_query_result_empty_header(self):
        """Test QueryResult with empty header."""
        mock_graph = Mock()
        response = [
            [[]],  # empty header
            [],
            ["internal execution time: 1.0 milliseconds"]
        ]
        
        result = QueryResult(mock_graph, response)
        
        assert result.header == [[]]
        assert result.result_set == []
    
    def test_query_result_statistics_properties(self):
        """Test all statistics properties."""
        mock_graph = Mock()
        response = [
            [
                "Labels added: 1",
                "Labels removed: 2",
                "Nodes created: 3",
                "Nodes deleted: 4",
                "Properties set: 5",
                "Properties removed: 6",
                "Relationships created: 7",
                "Relationships deleted: 8",
                "Indices created: 9",
                "Indices deleted: 10",
                "Cached execution: 1",
                "internal execution time: 15.5 milliseconds"
            ]
        ]
        
        result = QueryResult(mock_graph, response)
        
        assert result.labels_added == 1
        assert result.labels_removed == 2
        assert result.nodes_created == 3
        assert result.nodes_deleted == 4
        assert result.properties_set == 5
        assert result.properties_removed == 6
        assert result.relationships_created == 7
        assert result.relationships_deleted == 8
        assert result.indices_created == 9
        assert result.indices_deleted == 10
        assert result.cached_execution is True
        assert result.run_time_ms == 15.5
    
    def test_query_result_cached_execution_false(self):
        """Test cached_execution returns False when not cached."""
        mock_graph = Mock()
        response = [
            ["internal execution time: 1.0 milliseconds"]
        ]
        
        result = QueryResult(mock_graph, response)
        assert result.cached_execution is False
    
    def test_query_result_missing_statistics(self):
        """Test statistics return 0 when not present."""
        mock_graph = Mock()
        response = [
            ["internal execution time: 1.0 milliseconds"]
        ]
        
        result = QueryResult(mock_graph, response)
        
        assert result.labels_added == 0
        assert result.nodes_created == 0
        assert result.properties_set == 0
    
    def test_query_result_error_at_start(self):
        """Test QueryResult raises exception on error at start."""
        mock_graph = Mock()
        error = ResponseError("Test error")
        response = [error]
        
        with pytest.raises(ResponseError, match="Test error"):
            QueryResult(mock_graph, response)
    
    def test_query_result_error_at_end(self):
        """Test QueryResult raises exception on error at end."""
        mock_graph = Mock()
        error = ResponseError("Runtime error")
        response = [
            [["name"]],
            [],
            error
        ]
        
        with pytest.raises(ResponseError, match="Runtime error"):
            QueryResult(mock_graph, response)
    
    def test_query_result_schema_version_mismatch(self):
        """Test QueryResult raises SchemaVersionMismatchException."""
        mock_graph = Mock()
        error = ResponseError("version mismatch")
        response = [error, 5]  # version 5
        
        with pytest.raises(SchemaVersionMismatchException) as exc_info:
            QueryResult(mock_graph, response)
        
        assert exc_info.value.version == 5


class TestStats:
    """Tests for STATS constants."""
    
    def test_stats_list_contains_all_statistics(self):
        """Test STATS list contains all expected statistics."""
        assert LABELS_ADDED in STATS
        assert NODES_CREATED in STATS
        assert PROPERTIES_SET in STATS
        assert RELATIONSHIPS_CREATED in STATS
        assert INTERNAL_EXECUTION_TIME in STATS
