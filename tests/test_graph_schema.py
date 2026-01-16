import pytest
from unittest.mock import Mock, MagicMock
from falkordb.graph_schema import GraphSchema


class TestGraphSchema:
    """Tests for GraphSchema class."""
    
    def test_graph_schema_init(self):
        """Test GraphSchema initialization."""
        mock_graph = Mock()
        schema = GraphSchema(mock_graph)
        
        assert schema.graph == mock_graph
        assert schema.version == 0
        assert schema.labels == []
        assert schema.properties == []
        assert schema.relationships == []
    
    def test_graph_schema_clear(self):
        """Test clear method resets schema."""
        mock_graph = Mock()
        schema = GraphSchema(mock_graph)
        
        # Populate with some data
        schema.version = 5
        schema.labels = ["Person", "Country"]
        schema.properties = ["name", "age"]
        schema.relationships = ["KNOWS", "VISITED"]
        
        # Clear
        schema.clear()
        
        assert schema.version == 0
        assert schema.labels == []
        assert schema.properties == []
        assert schema.relationships == []
    
    def test_refresh_labels(self):
        """Test refresh_labels method."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["Person"], ["Country"], ["City"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.refresh_labels()
        
        assert schema.labels == ["Person", "Country", "City"]
        mock_graph.call_procedure.assert_called_once_with("DB.LABELS")
    
    def test_refresh_relations(self):
        """Test refresh_relations method."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["KNOWS"], ["VISITED"], ["LIKES"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.refresh_relations()
        
        assert schema.relationships == ["KNOWS", "VISITED", "LIKES"]
        mock_graph.call_procedure.assert_called_once_with("DB.RELATIONSHIPTYPES")
    
    def test_refresh_properties(self):
        """Test refresh_properties method."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["name"], ["age"], ["email"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.refresh_properties()
        
        assert schema.properties == ["name", "age", "email"]
        mock_graph.call_procedure.assert_called_once_with("DB.PROPERTYKEYS")
    
    def test_refresh(self):
        """Test refresh method updates all schema data."""
        mock_graph = Mock()
        
        # Setup mock for different procedure calls
        def mock_call_procedure(proc_name):
            result = Mock()
            if proc_name == "DB.LABELS":
                result.result_set = [["Person"], ["Country"]]
            elif proc_name == "DB.RELATIONSHIPTYPES":
                result.result_set = [["KNOWS"], ["VISITED"]]
            elif proc_name == "DB.PROPERTYKEYS":
                result.result_set = [["name"], ["age"]]
            return result
        
        mock_graph.call_procedure.side_effect = mock_call_procedure
        
        schema = GraphSchema(mock_graph)
        schema.refresh(version=10)
        
        assert schema.version == 10
        assert schema.labels == ["Person", "Country"]
        assert schema.relationships == ["KNOWS", "VISITED"]
        assert schema.properties == ["name", "age"]
        
        # Verify all procedures were called
        assert mock_graph.call_procedure.call_count == 3
    
    def test_get_label_by_index(self):
        """Test get_label returns label by index."""
        mock_graph = Mock()
        schema = GraphSchema(mock_graph)
        schema.labels = ["Person", "Country", "City"]
        
        assert schema.get_label(0) == "Person"
        assert schema.get_label(1) == "Country"
        assert schema.get_label(2) == "City"
    
    def test_get_label_out_of_range_refreshes(self):
        """Test get_label refreshes when index is out of range."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["Person"], ["Country"], ["City"], ["NewLabel"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.labels = ["Person", "Country"]
        
        # Try to access index 3 (out of range)
        result = schema.get_label(3)
        
        # Should have refreshed labels
        assert result == "NewLabel"
        assert len(schema.labels) == 4
        mock_graph.call_procedure.assert_called_once_with("DB.LABELS")
    
    def test_get_relation_by_index(self):
        """Test get_relation returns relation by index."""
        mock_graph = Mock()
        schema = GraphSchema(mock_graph)
        schema.relationships = ["KNOWS", "VISITED", "LIKES"]
        
        assert schema.get_relation(0) == "KNOWS"
        assert schema.get_relation(1) == "VISITED"
        assert schema.get_relation(2) == "LIKES"
    
    def test_get_relation_out_of_range_refreshes(self):
        """Test get_relation refreshes when index is out of range."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["KNOWS"], ["VISITED"], ["NewRelation"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.relationships = ["KNOWS"]
        
        # Try to access index 2 (out of range)
        result = schema.get_relation(2)
        
        # Should have refreshed relationships
        assert result == "NewRelation"
        assert len(schema.relationships) == 3
        mock_graph.call_procedure.assert_called_once_with("DB.RELATIONSHIPTYPES")
    
    def test_get_property_by_index(self):
        """Test get_property returns property by index."""
        mock_graph = Mock()
        schema = GraphSchema(mock_graph)
        schema.properties = ["name", "age", "email"]
        
        assert schema.get_property(0) == "name"
        assert schema.get_property(1) == "age"
        assert schema.get_property(2) == "email"
    
    def test_get_property_out_of_range_refreshes(self):
        """Test get_property refreshes when index is out of range."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["name"], ["age"], ["email"], ["newprop"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.properties = ["name", "age"]
        
        # Try to access index 3 (out of range)
        result = schema.get_property(3)
        
        # Should have refreshed properties
        assert result == "newprop"
        assert len(schema.properties) == 4
        mock_graph.call_procedure.assert_called_once_with("DB.PROPERTYKEYS")
    
    def test_schema_procedure_constants(self):
        """Test schema procedure constants are correctly defined."""
        from falkordb.graph_schema import DB_LABELS, DB_PROPERTYKEYS, DB_RELATIONSHIPTYPES
        
        assert DB_LABELS == "DB.LABELS"
        assert DB_PROPERTYKEYS == "DB.PROPERTYKEYS"
        assert DB_RELATIONSHIPTYPES == "DB.RELATIONSHIPTYPES"
    
    def test_get_label_index_error_propagates_after_refresh(self):
        """Test get_label raises IndexError if still out of range after refresh."""
        mock_graph = Mock()
        mock_result = Mock()
        mock_result.result_set = [["Person"]]
        mock_graph.call_procedure.return_value = mock_result
        
        schema = GraphSchema(mock_graph)
        schema.labels = []
        
        # Try to access index 5 which doesn't exist even after refresh
        with pytest.raises(IndexError):
            schema.get_label(5)
    
    def test_multiple_refreshes(self):
        """Test schema can be refreshed multiple times."""
        mock_graph = Mock()
        
        # First refresh
        mock_result1 = Mock()
        mock_result1.result_set = [["Person"]]
        
        # Second refresh
        mock_result2 = Mock()
        mock_result2.result_set = [["Person"], ["Country"]]
        
        mock_graph.call_procedure.side_effect = [
            mock_result1, Mock(result_set=[]), Mock(result_set=[]),  # First refresh
            mock_result2, Mock(result_set=[]), Mock(result_set=[])   # Second refresh
        ]
        
        schema = GraphSchema(mock_graph)
        
        schema.refresh(version=1)
        assert schema.version == 1
        assert schema.labels == ["Person"]
        
        schema.refresh(version=2)
        assert schema.version == 2
        assert schema.labels == ["Person", "Country"]
