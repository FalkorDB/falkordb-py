import pytest
from unittest.mock import Mock, MagicMock, patch
import socket
import redis.exceptions as redis_exceptions
from falkordb.cluster import Is_Cluster, Cluster_Conn


class TestIsCluster:
    """Tests for Is_Cluster function."""
    
    def test_is_cluster_true(self):
        """Test Is_Cluster returns True when connection is a cluster."""
        mock_conn = Mock()
        mock_conn.info.return_value = {
            "redis_mode": "cluster",
            "redis_version": "6.0.0"
        }
        
        result = Is_Cluster(mock_conn)
        
        assert result is True
        mock_conn.info.assert_called_once_with(section="server")
    
    def test_is_cluster_false_not_cluster(self):
        """Test Is_Cluster returns False when redis_mode is not cluster."""
        mock_conn = Mock()
        mock_conn.info.return_value = {
            "redis_mode": "standalone",
            "redis_version": "6.0.0"
        }
        
        result = Is_Cluster(mock_conn)
        
        assert result is False
    
    def test_is_cluster_false_no_redis_mode(self):
        """Test Is_Cluster returns False when redis_mode is not in info."""
        mock_conn = Mock()
        mock_conn.info.return_value = {
            "redis_version": "6.0.0"
        }
        
        result = Is_Cluster(mock_conn)
        
        assert result is False


class TestClusterConn:
    """Tests for Cluster_Conn function."""
    
    @patch('falkordb.cluster.RedisCluster')
    def test_cluster_conn_basic(self, mock_redis_cluster):
        """Test Cluster_Conn with basic configuration."""
        # Setup mock connection
        mock_conn = Mock()
        mock_conn.connection_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'username': 'user',
            'password': 'pass',
            'retry': None,
            'retry_on_timeout': None,
            'retry_on_error': [
                ConnectionRefusedError,
                ConnectionError,
                TimeoutError,
                socket.timeout,
                redis_exceptions.ConnectionError,
            ]
        }
        
        # Setup mock cluster instance
        mock_cluster_instance = Mock()
        mock_redis_cluster.return_value = mock_cluster_instance
        
        # Call function
        result = Cluster_Conn(mock_conn, ssl=False)
        
        # Verify result
        assert result == mock_cluster_instance
        
        # Verify RedisCluster was called with correct arguments
        mock_redis_cluster.assert_called_once()
        call_kwargs = mock_redis_cluster.call_args[1]
        
        assert call_kwargs['host'] == 'localhost'
        assert call_kwargs['port'] == 6379
        assert call_kwargs['username'] == 'user'
        assert call_kwargs['password'] == 'pass'
        assert call_kwargs['decode_responses'] is True
        assert call_kwargs['ssl'] is False
    
    @patch('falkordb.cluster.RedisCluster')
    def test_cluster_conn_with_ssl(self, mock_redis_cluster):
        """Test Cluster_Conn with SSL enabled."""
        # Setup mock connection
        mock_conn = Mock()
        mock_conn.connection_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'username': 'user',
            'password': 'pass',
            'retry': None,
            'retry_on_timeout': None,
            'retry_on_error': []
        }
        
        mock_cluster_instance = Mock()
        mock_redis_cluster.return_value = mock_cluster_instance
        
        # Call function with SSL
        result = Cluster_Conn(mock_conn, ssl=True)
        
        # Verify SSL is passed
        call_kwargs = mock_redis_cluster.call_args[1]
        assert call_kwargs['ssl'] is True
    
    @patch('falkordb.cluster.RedisCluster')
    def test_cluster_conn_with_custom_parameters(self, mock_redis_cluster):
        """Test Cluster_Conn with custom parameters."""
        # Setup mock connection
        mock_conn = Mock()
        mock_conn.connection_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'username': 'user',
            'password': 'pass',
            'retry': Mock(),
            'retry_on_timeout': True,
            'retry_on_error': []
        }
        
        mock_cluster_instance = Mock()
        mock_redis_cluster.return_value = mock_cluster_instance
        
        # Call function with custom parameters
        result = Cluster_Conn(
            mock_conn,
            ssl=False,
            cluster_error_retry_attempts=5,
            require_full_coverage=True,
            reinitialize_steps=10,
            read_from_replicas=True,
            dynamic_startup_nodes=False,
            url="redis://localhost:6379",
            address_remap={"old": "new"},
            startup_nodes=[{"host": "node1", "port": 6379}]
        )
        
        # Verify custom parameters are passed
        call_kwargs = mock_redis_cluster.call_args[1]
        assert call_kwargs['cluster_error_retry_attempts'] == 5
        assert call_kwargs['require_full_coverage'] is True
        assert call_kwargs['reinitialize_steps'] == 10
        assert call_kwargs['read_from_replicas'] is True
        assert call_kwargs['dynamic_startup_nodes'] is False
        assert call_kwargs['url'] == "redis://localhost:6379"
        assert call_kwargs['address_remap'] == {"old": "new"}
        assert call_kwargs['startup_nodes'] == [{"host": "node1", "port": 6379}]
    
    @patch('falkordb.cluster.RedisCluster')
    def test_cluster_conn_extracts_retry_params(self, mock_redis_cluster):
        """Test Cluster_Conn extracts and uses retry parameters."""
        # Setup mock connection with retry parameters
        mock_retry = Mock()
        mock_conn = Mock()
        mock_conn.connection_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'username': 'user',
            'password': 'pass',
            'retry': mock_retry,
            'retry_on_timeout': True,
            'retry_on_error': [ConnectionError, TimeoutError]
        }
        
        mock_cluster_instance = Mock()
        mock_redis_cluster.return_value = mock_cluster_instance
        
        # Call function
        result = Cluster_Conn(mock_conn, ssl=False)
        
        # Verify retry parameters are passed
        call_kwargs = mock_redis_cluster.call_args[1]
        assert call_kwargs['retry'] == mock_retry
        assert call_kwargs['retry_on_timeout'] is True
        assert call_kwargs['retry_on_error'] == [ConnectionError, TimeoutError]
    
    @patch('falkordb.cluster.RedisCluster')
    def test_cluster_conn_default_retry_on_error(self, mock_redis_cluster):
        """Test Cluster_Conn uses default retry_on_error if not provided."""
        # Setup mock connection without retry_on_error
        mock_conn = Mock()
        mock_conn.connection_pool.connection_kwargs = {
            'host': 'localhost',
            'port': 6379,
            'username': 'user',
            'password': 'pass',
            'retry': None,
            'retry_on_timeout': None
        }
        
        mock_cluster_instance = Mock()
        mock_redis_cluster.return_value = mock_cluster_instance
        
        # Call function
        result = Cluster_Conn(mock_conn, ssl=False)
        
        # Verify default retry_on_error is used
        call_kwargs = mock_redis_cluster.call_args[1]
        default_retry_errors = call_kwargs['retry_on_error']
        
        # Check that default errors are included
        assert ConnectionRefusedError in default_retry_errors
        assert ConnectionError in default_retry_errors
        assert TimeoutError in default_retry_errors
        assert socket.timeout in default_retry_errors
        assert redis_exceptions.ConnectionError in default_retry_errors
