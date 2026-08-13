from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from falkordb.asyncio.graph import AsyncGraph
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb.graph import Graph


@pytest.mark.asyncio
async def test_async_query_awaits_schema_refresh_on_version_mismatch():
    client = SimpleNamespace(
        execute_command=AsyncMock(side_effect=SchemaVersionMismatchException(7))
    )
    graph = AsyncGraph(client, "g")
    graph.schema.refresh = AsyncMock()

    with pytest.raises(SchemaVersionMismatchException) as excinfo:
        await graph._query("MATCH (n) RETURN n")

    # refresh must be awaited, not merely called: without the await the
    # coroutine is dropped and the client keeps a stale schema
    graph.schema.refresh.assert_awaited_once_with(7)
    assert excinfo.value.version == 7


def test_sync_query_refreshes_schema_on_version_mismatch():
    client = SimpleNamespace(
        execute_command=Mock(side_effect=SchemaVersionMismatchException(7))
    )
    graph = Graph(client, "g")
    graph.schema.refresh = Mock()

    with pytest.raises(SchemaVersionMismatchException) as excinfo:
        graph._query("MATCH (n) RETURN n")

    graph.schema.refresh.assert_called_once_with(7)
    assert excinfo.value.version == 7
