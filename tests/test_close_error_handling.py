from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from redis.exceptions import RedisError

from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.falkordb import FalkorDB as SyncFalkorDB


def test_sync_close_suppresses_redis_error():
    db = object.__new__(SyncFalkorDB)
    close_mock = Mock(side_effect=RedisError("Connection already closed"))
    db.connection = SimpleNamespace(close=close_mock)
    db.close()
    close_mock.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_aclose_suppresses_redis_error():
    db = object.__new__(AsyncFalkorDB)
    close_mock = AsyncMock(side_effect=RedisError("Connection already closed"))
    db.connection = SimpleNamespace(aclose=close_mock)
    await db.aclose()
    close_mock.assert_awaited_once_with()


def test_sync_context_manager_closes_when_body_raises():
    db = object.__new__(SyncFalkorDB)
    close_mock = Mock()
    db.connection = SimpleNamespace(close=close_mock)
    with pytest.raises(ValueError, match="Application failure"):
        with db:
            raise ValueError("Application failure")
    close_mock.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_context_manager_closes_when_body_raises():
    db = object.__new__(AsyncFalkorDB)
    close_mock = AsyncMock()
    db.connection = SimpleNamespace(aclose=close_mock)

    with pytest.raises(ValueError, match="Application failure"):
        async with db:
            raise ValueError("Application failure")
    close_mock.assert_awaited_once_with()
