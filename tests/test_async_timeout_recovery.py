from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ResponseError

from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB


@pytest.mark.asyncio
async def test_async_query_timeout_disconnects_unhealthy_connection():
    db = object.__new__(AsyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()

    async def async_exec(*args, **kwargs):
        if mock_conn.execute_command.call_count == 1:
            raise ResponseError("Query timed out")
        return [
            [[1, b"header"]],
            [[[3, b"1"]]],
            [b"Query internal execution time: 0.1 milliseconds"],
        ]

    mock_conn.execute_command = MagicMock(side_effect=async_exec)
    mock_conn.aclose = AsyncMock()

    db.connection = mock_conn
    db.execute_command = mock_conn.execute_command

    graph = db.select_graph("async_test_timeout_graph")

    # First query triggers query timeout
    with pytest.raises(ResponseError) as exc_info:
        await graph.query("MATCH (n) RETURN n")
    assert "Query timed out" in str(exc_info.value)

    # Connection aclose should have been called
    mock_conn.aclose.assert_called_once()

    # Second query succeeds on fresh connection
    res = await graph.query("RETURN 1")
    assert res is not None
