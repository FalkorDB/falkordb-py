from unittest.mock import MagicMock

import pytest
from redis.exceptions import ResponseError

from falkordb.falkordb import FalkorDB as SyncFalkorDB


def test_sync_query_timeout_disconnects_unhealthy_connection():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    mock_conn.execute_command.side_effect = [
        ResponseError("Query timed out"),
        [["1"]],
    ]
    db.connection = mock_conn
    db.execute_command = mock_conn.execute_command

    graph = db.select_graph("test_timeout_graph")

    # First query triggers query timeout
    with pytest.raises(ResponseError) as exc_info:
        graph.query("MATCH (n) RETURN n")
    assert "Query timed out" in str(exc_info.value)

    # Connection close should have been called to purge dirty socket state
    mock_conn.close.assert_called_once()

    # Second query succeeds on fresh/reconnected connection without silent C-level crash
    res = graph.query("RETURN 1")
    assert res is not None
