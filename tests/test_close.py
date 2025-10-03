from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

import pytest

from falkordb.falkordb import FalkorDB as SyncFalkorDB
from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB


def test_sync_context_manager_calls_close():
    db = object.__new__(SyncFalkorDB)

    mock_conn = SimpleNamespace(close=Mock())
    mock_conn.connection_pool = SimpleNamespace(disconnect=Mock())
    db.connection = mock_conn

    # using context manager should call close on the underlying connection
    with db as d:
        assert d is db

    mock_conn.close.assert_called_once()


def test_sync_close_fallback_disconnect():
    db = object.__new__(SyncFalkorDB)

    # connection has no close(), only a connection_pool.disconnect()
    mock_conn = SimpleNamespace()
    mock_conn.connection_pool = SimpleNamespace(disconnect=Mock())
    db.connection = mock_conn

    db.close()

    mock_conn.connection_pool.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_async_aclose_and_context_manager():
    db = object.__new__(AsyncFalkorDB)

    mock_conn = SimpleNamespace(aclose=AsyncMock())
    db.connection = mock_conn

    # explicit aclose
    await db.aclose()
    mock_conn.aclose.assert_awaited_once()

    # async context manager should also await aclose
    mock_conn.aclose.reset_mock()
    async with db as d:
        assert d is db

    mock_conn.aclose.assert_awaited_once()
