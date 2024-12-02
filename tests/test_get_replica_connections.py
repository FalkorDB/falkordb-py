import pytest
import redis
from unittest.mock import MagicMock

class TestFalkorDB:
    @pytest.fixture
    def falkor_db(self):
        class MockFalkorDB:
            def __init__(self):
                self.connection = MagicMock()
                self.sentinel = None
                self.service_name = "mymaster"

            def get_replica_connections(self):
                try:
                    if self.sentinel:
                        replicas = self.sentinel.discover_slaves(service_name=self.service_name)
                        if not replicas:
                            raise ConnectionError("Unable to get replica hostname.")
                        return [(host, int(port)) for host, port in replicas]

                    mode = self.connection.execute_command("info").get('redis_mode', None)
                    if mode == "cluster":
                        nodes = self.connection.cluster_nodes()
                        if not nodes:
                            raise ConnectionError("Unable to get cluster nodes")
                        return [(flag['hostname'], int(ip_port.split(':')[1])) 
                                for ip_port, flag in nodes.items() 
                                if 'slave' in flag['flags']]
                    else:
                        raise ValueError(f"Unsupported Redis mode: {mode}")

                except redis.RedisError as e:
                    raise ConnectionError("Failed to get replica hostnames") from e

        return MockFalkorDB()

    def test_get_replica_connections_connection_error(self, falkor_db):
        """Test that a ConnectionError is raised when Redis is unreachable."""
        falkor_db.connection.execute_command.side_effect = redis.RedisError("Connection failed")

        with pytest.raises(ConnectionError, match="Failed to get replica hostnames") as excinfo:
            falkor_db.get_replica_connections()

        # Ensure the original RedisError is the cause of ConnectionError
        assert isinstance(excinfo.value.__cause__, redis.RedisError)
        assert str(excinfo.value.__cause__) == "Connection failed"

    def test_get_replica_connections_sentinel_error(self, falkor_db):
        """Test Sentinel raises ConnectionError if no replicas are found."""
        falkor_db.sentinel = MagicMock()
        falkor_db.sentinel.discover_slaves.return_value = []

        with pytest.raises(ConnectionError, match="Unable to get replica hostname"):
            falkor_db.get_replica_connections()

    def test_get_replica_connections_cluster_nodes_error(self, falkor_db):
        """Test that ConnectionError is raised if cluster_nodes() returns no nodes."""
        falkor_db.connection.execute_command.return_value = {'redis_mode': 'cluster'}
        falkor_db.connection.cluster_nodes.return_value = {}

        with pytest.raises(ConnectionError, match="Unable to get cluster nodes"):
            falkor_db.get_replica_connections()

    def test_get_replica_connections_unsupported_mode(self, falkor_db):
        """Test ValueError for unsupported Redis mode."""
        falkor_db.connection.execute_command.return_value = {'redis_mode': 'standalone'}

        with pytest.raises(ValueError, match="Unsupported Redis mode: standalone"):
            falkor_db.get_replica_connections()
