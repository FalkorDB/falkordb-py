import pytest
from unittest.mock import MagicMock, patch

class TestFalkorDB:
    @pytest.fixture
    def falkor_db(self):
        # Mocking the FalkorDB instance
        class MockFalkorDB:
            def __init__(self):
                self.connection = MagicMock()
                self.sentinel = None
                self.service_name = "mymaster"
        
        return MockFalkorDB()
    
    def test_get_replica_connections_sentinel(self, falkor_db):
        # Set up sentinel mode
        falkor_db.sentinel = MagicMock()
        falkor_db.sentinel.discover_slaves.return_value = [("127.0.0.1", 6380), ("127.0.0.2", 6381)]
        
        result = falkor_db.get_replica_connections()
        
        assert result == [("127.0.0.1", 6380), ("127.0.0.2", 6381)]
        falkor_db.sentinel.discover_slaves.assert_called_once_with(service_name="mymaster")
    
    def test_get_replica_connections_cluster(self, falkor_db):
        # Set up cluster mode
        falkor_db.connection.execute_command.return_value = {"redis_mode": "cluster"}
        falkor_db.connection.cluster_nodes.return_value = {
            "127.0.0.1:6379": {"hostname": "127.0.0.1", "flags": "master"},
            "127.0.0.2:6380": {"hostname": "127.0.0.2", "flags": "slave"},
            "127.0.0.3:6381": {"hostname": "127.0.0.3", "flags": "slave"},
        }
        
        result = falkor_db.get_replica_connections()
        
        assert result == [("127.0.0.2", 6380), ("127.0.0.3", 6381)]
        falkor_db.connection.cluster_nodes.assert_called_once()
    
    def test_get_replica_connections_unsupported_mode(self, falkor_db):
        # Set up unsupported mode
        falkor_db.connection.execute_command.return_value = {"redis_mode": "unknown"}
        
        with pytest.raises(ValueError, match="Unsupported Redis mode: unknown"):
            falkor_db.get_replica_connections()
    
    def test_get_replica_connections_connection_error(self, falkor_db):
        # Simulate connection error
        falkor_db.connection.execute_command.side_effect = ConnectionError("Connection failed")
        
        with pytest.raises(ConnectionError, match="Connection failed"):
            falkor_db.get_replica_connections()
