
"""Regression test for stale schema after graph recreation.

Related issue:
https://github.com/FalkorDB/falkordb-py/issues/243
"""

import os
from uuid import uuid4
import pytest

from falkordb import FalkorDB
def test_schema_refresh_after_external_graph_recreation():
    port = os.getenv("FALKORDB_SCHEMA_TEST_PORT")

    if port is None:
        pytest.skip("Dedicated FalkorDB test server not configured")

    graph_name = f"issue243_{uuid4().hex[:12]}"

    client_a = FalkorDB(host="localhost", port=int(port))
    client_b = FalkorDB(host="localhost", port=int(port))

    graph_a = client_a.select_graph(graph_name)
    graph_b = client_b.select_graph(graph_name)

    try:
        graph_a.query("CREATE (:Item {old_name: 'first'})")

        original = graph_a.query(
            "MATCH (n:Item) RETURN n"
        ).result_set[0][0]

        assert original.properties == {"old_name": "first"}

        # External deletion does not clear graph_a's local cache.
        graph_b.delete()

        # Recreate the graph with a different property name.
        graph_b.query("CREATE (:Item {new_name: 'second'})")

        recreated = graph_a.query(
            "MATCH (n:Item) RETURN n"
        ).result_set[0][0]

        assert recreated.properties == {"new_name": "second"}, (
            "Stale schema mapping after graph deletion and recreation: "
            f"{recreated.properties}"
        )

    finally:
        try:
            graph_b.delete()
        except Exception:
            pass
