import pytest
import tempfile
import os


def _has_falkordblite():
    """Check if falkordblite is installed"""
    try:
        import redislite
        return True
    except ImportError:
        return False


def test_embedded_import_error():
    """Test that we get a helpful error when falkordblite is not installed"""
    from falkordb import FalkorDB
    
    # This will fail if falkordblite is not installed
    with pytest.raises(ImportError) as exc_info:
        db = FalkorDB(embedded=True)
    
    assert "pip install falkordb[embedded]" in str(exc_info.value)


@pytest.mark.skipif(
    not _has_falkordblite(),
    reason="falkordblite not installed"
)
def test_embedded_basic():
    """Test basic embedded FalkorDB functionality"""
    from falkordb import FalkorDB
    
    # Create a temporary database file
    with tempfile.TemporaryDirectory() as tmpdir:
        dbfile = os.path.join(tmpdir, "test.db")
        
        # Create embedded instance
        db = FalkorDB(embedded=True, dbfilename=dbfile)
        
        # Select a graph
        g = db.select_graph("test_graph")
        
        # Execute a simple query
        result = g.query("RETURN 1")
        assert result.result_set[0][0] == 1
        
        # Create a node
        result = g.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
        assert len(result.result_set) == 1
        
        # Query the node back
        result = g.query("MATCH (n:Person) RETURN n.name")
        assert result.result_set[0][0] == 'Alice'
        
        # Test list_graphs
        graphs = db.list_graphs()
        assert 'test_graph' in graphs
        
        # Clean up
        g.delete()


@pytest.mark.skipif(
    not _has_falkordblite(),
    reason="falkordblite not installed"
)
def test_embedded_persistence():
    """Test that embedded database persists data across connections"""
    from falkordb import FalkorDB
    
    with tempfile.TemporaryDirectory() as tmpdir:
        dbfile = os.path.join(tmpdir, "persist.db")
        
        # Create data in first connection
        db1 = FalkorDB(embedded=True, dbfilename=dbfile)
        g1 = db1.select_graph("persist_test")
        g1.query("CREATE (n:Person {name: 'Bob', age: 30})")
        
        # Close by deleting reference
        del db1
        del g1
        
        # Open a new connection to the same database
        db2 = FalkorDB(embedded=True, dbfilename=dbfile)
        g2 = db2.select_graph("persist_test")
        
        # Verify data persisted
        result = g2.query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(result.result_set) == 1
        assert result.result_set[0][0] == 'Bob'
        assert result.result_set[0][1] == 30
        
        # Clean up
        g2.delete()


def test_embedded_not_supported_in_asyncio():
    """Test that asyncio raises NotImplementedError for embedded mode"""
    from falkordb.asyncio import FalkorDB
    
    with pytest.raises(NotImplementedError) as exc_info:
        db = FalkorDB(embedded=True)
    
    assert "not supported with asyncio" in str(exc_info.value)
