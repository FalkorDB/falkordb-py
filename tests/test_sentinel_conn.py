from unittest.mock import MagicMock, patch

import pytest

from falkordb.asyncio.sentinel import (
    Is_Sentinel as Async_Is_Sentinel,
)
from falkordb.asyncio.sentinel import (
    Sentinel_Conn as Async_Sentinel_Conn,
)
from falkordb.sentinel import (
    Is_Sentinel as Sync_Is_Sentinel,
)
from falkordb.sentinel import (
    Sentinel_Conn as Sync_Sentinel_Conn,
)


def test_sync_is_sentinel_true():
    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "sentinel"}
    assert Sync_Is_Sentinel(mock_conn) is True


def test_sync_is_sentinel_false():
    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "standalone"}
    assert Sync_Is_Sentinel(mock_conn) is False


def test_sync_is_sentinel_generic_exception():
    mock_conn = MagicMock()
    mock_conn.info.side_effect = ValueError("unexpected error")
    assert Sync_Is_Sentinel(mock_conn) is False


def test_sync_is_sentinel_connection_error_raises():
    mock_conn = MagicMock()
    mock_conn.info.side_effect = ConnectionError("failed connect")
    with pytest.raises(ConnectionError):
        Sync_Is_Sentinel(mock_conn)


def test_sync_sentinel_conn_single_master():
    mock_conn = MagicMock()
    mock_conn.sentinel_masters.return_value = {"mymaster": {}}
    mock_conn.connection_pool.connection_kwargs = {
        "host": "localhost",
        "port": 26379,
        "username": "user",
        "password": "pass",
    }
    with patch("falkordb.sentinel.Sentinel") as mock_sentinel_cls:
        sentinel_inst, service_name = Sync_Sentinel_Conn(mock_conn, ssl=True)
        assert service_name == "mymaster"
        mock_sentinel_cls.assert_called_once()


def test_sync_sentinel_conn_multiple_masters_raises():
    mock_conn = MagicMock()
    mock_conn.sentinel_masters.return_value = {"master1": {}, "master2": {}}
    with pytest.raises(Exception, match="Multiple masters"):
        Sync_Sentinel_Conn(mock_conn, ssl=False)


def test_async_is_sentinel_true():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 26379}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "sentinel"}
        assert Async_Is_Sentinel(mock_conn) is True


def test_async_is_sentinel_false():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 26379}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "standalone"}
        assert Async_Is_Sentinel(mock_conn) is False


def test_async_is_sentinel_generic_exception():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 26379}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.side_effect = ValueError("error")
        assert Async_Is_Sentinel(mock_conn) is False


def test_async_sentinel_conn_single_master():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {
        "host": "localhost",
        "port": 26379,
        "username": "user",
        "password": "pass",
    }
    with (
        patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_sync_redis,
        patch("falkordb.asyncio.sentinel.Sentinel") as mock_async_sentinel_cls,
    ):
        mock_sync_redis.return_value.sentinel_masters.return_value = {"mymaster": {}}
        sentinel_inst, service_name = Async_Sentinel_Conn(mock_conn, ssl=True)
        assert service_name == "mymaster"
        mock_async_sentinel_cls.assert_called_once()


def test_async_sentinel_conn_multiple_masters_raises():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 26379}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_sync_redis:
        mock_sync_redis.return_value.sentinel_masters.return_value = {
            "m1": {},
            "m2": {},
        }
        with pytest.raises(Exception, match="Multiple masters"):
            Async_Sentinel_Conn(mock_conn, ssl=False)


def test_async_is_sentinel_unix_socket():
    from redis.asyncio.connection import UnixDomainSocketConnection

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = UnixDomainSocketConnection
    mock_conn.connection_pool.connection_kwargs = {"path": "/tmp/falkordb.sock"}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "sentinel"}
        assert Async_Is_Sentinel(mock_conn) is True


def test_async_is_sentinel_connection_error_raises():
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 26379}
    with patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.side_effect = ConnectionError("failed")
        with pytest.raises(ConnectionError):
            Async_Is_Sentinel(mock_conn)


def test_async_sentinel_conn_unix_socket():
    from redis.asyncio.connection import UnixDomainSocketConnection

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = UnixDomainSocketConnection
    mock_conn.connection_pool.connection_kwargs = {"path": "/tmp/falkordb.sock"}
    with (
        patch("falkordb.asyncio.sentinel.sync_redis.Redis") as mock_sync_redis,
        patch("falkordb.asyncio.sentinel.Sentinel") as mock_async_sentinel_cls,
    ):
        mock_sync_redis.return_value.sentinel_masters.return_value = {"mymaster": {}}
        sentinel_inst, service_name = Async_Sentinel_Conn(mock_conn, ssl=False)
        assert service_name == "mymaster"
        mock_async_sentinel_cls.assert_called_once()
