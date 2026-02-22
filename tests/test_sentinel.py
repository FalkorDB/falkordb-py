from unittest.mock import Mock, patch

import pytest

from falkordb.sentinel import Is_Sentinel, Sentinel_Conn


class TestIsSentinel:
    """Tests for Is_Sentinel function."""

    def test_is_sentinel_true(self):
        """Test Is_Sentinel returns True when connection is a sentinel."""
        mock_conn = Mock()
        mock_conn.info.return_value = {
            "redis_mode": "sentinel",
            "redis_version": "6.0.0",
        }

        result = Is_Sentinel(mock_conn)

        assert result is True
        mock_conn.info.assert_called_once_with(section="server")

    def test_is_sentinel_false_not_sentinel(self):
        """Test Is_Sentinel returns False when redis_mode is not sentinel."""
        mock_conn = Mock()
        mock_conn.info.return_value = {
            "redis_mode": "standalone",
            "redis_version": "6.0.0",
        }

        result = Is_Sentinel(mock_conn)

        assert result is False

    def test_is_sentinel_false_no_redis_mode(self):
        """Test Is_Sentinel returns False when redis_mode is not in info."""
        mock_conn = Mock()
        mock_conn.info.return_value = {"redis_version": "6.0.0"}

        result = Is_Sentinel(mock_conn)

        assert result is False


class TestSentinelConn:
    """Tests for Sentinel_Conn function."""

    @patch("falkordb.sentinel.Sentinel")
    def test_sentinel_conn_single_master(self, mock_sentinel_class):
        """Test Sentinel_Conn with a single master."""
        # Setup mock connection
        mock_conn = Mock()
        mock_conn.sentinel_masters.return_value = {
            "master1": {"ip": "127.0.0.1", "port": 6379}
        }
        mock_conn.connection_pool.connection_kwargs = {
            "host": "localhost",
            "port": 26379,
            "username": "user",
            "password": "pass",
        }

        # Setup mock sentinel instance
        mock_sentinel_instance = Mock()
        mock_sentinel_class.return_value = mock_sentinel_instance

        # Call function
        result = Sentinel_Conn(mock_conn, ssl=False)

        # Verify results
        assert result == (mock_sentinel_instance, "master1")
        mock_conn.sentinel_masters.assert_called_once()

        # Verify Sentinel was called with correct arguments
        mock_sentinel_class.assert_called_once()
        call_args = mock_sentinel_class.call_args

        # Check sentinels list
        assert call_args[0][0] == [("localhost", 26379)]

        # Check sentinel_kwargs
        assert "username" in call_args[1]["sentinel_kwargs"]
        assert call_args[1]["sentinel_kwargs"]["username"] == "user"
        assert "password" in call_args[1]["sentinel_kwargs"]
        assert call_args[1]["sentinel_kwargs"]["password"] == "pass"

    @patch("falkordb.sentinel.Sentinel")
    def test_sentinel_conn_with_ssl(self, mock_sentinel_class):
        """Test Sentinel_Conn with SSL enabled."""
        # Setup mock connection
        mock_conn = Mock()
        mock_conn.sentinel_masters.return_value = {"master1": {}}
        mock_conn.connection_pool.connection_kwargs = {
            "host": "localhost",
            "port": 26379,
            "username": "user",
            "password": "pass",
        }

        mock_sentinel_instance = Mock()
        mock_sentinel_class.return_value = mock_sentinel_instance

        # Call function with SSL
        Sentinel_Conn(mock_conn, ssl=True)

        # Verify SSL is in sentinel_kwargs
        call_args = mock_sentinel_class.call_args
        assert call_args[1]["sentinel_kwargs"]["ssl"] is True

    @patch("falkordb.sentinel.Sentinel")
    def test_sentinel_conn_no_username_password(self, mock_sentinel_class):
        """Test Sentinel_Conn without username and password."""
        # Setup mock connection without credentials
        mock_conn = Mock()
        mock_conn.sentinel_masters.return_value = {"master1": {}}
        mock_conn.connection_pool.connection_kwargs = {
            "host": "localhost",
            "port": 26379,
        }

        mock_sentinel_instance = Mock()
        mock_sentinel_class.return_value = mock_sentinel_instance

        # Call function
        Sentinel_Conn(mock_conn, ssl=False)

        # Verify sentinel_kwargs does not have username/password
        call_args = mock_sentinel_class.call_args
        assert "username" not in call_args[1]["sentinel_kwargs"]
        assert "password" not in call_args[1]["sentinel_kwargs"]

    def test_sentinel_conn_multiple_masters_raises_exception(self):
        """Test Sentinel_Conn raises exception with multiple masters."""
        # Setup mock connection with multiple masters
        mock_conn = Mock()
        mock_conn.sentinel_masters.return_value = {"master1": {}, "master2": {}}
        mock_conn.connection_pool.connection_kwargs = {
            "host": "localhost",
            "port": 26379,
        }

        # Verify exception is raised
        with pytest.raises(Exception, match="Multiple masters, require service name"):
            Sentinel_Conn(mock_conn, ssl=False)

    def test_sentinel_conn_no_masters_raises_exception(self):
        """Test Sentinel_Conn with no masters."""
        # Setup mock connection with no masters - should fail when
        # trying to get first key
        mock_conn = Mock()
        mock_conn.sentinel_masters.return_value = {}
        mock_conn.connection_pool.connection_kwargs = {
            "host": "localhost",
            "port": 26379,
        }

        # Verify exception is raised (IndexError when getting key from empty dict)
        with pytest.raises(Exception):
            Sentinel_Conn(mock_conn, ssl=False)
