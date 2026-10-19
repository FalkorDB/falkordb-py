
"""Regression coverage for query recovery after server-side timeout.

Related issue: https://github.com/FalkorDB/falkordb-py/issues/205
"""

import os
import subprocess
import sys
import textwrap

import pytest


def test_query_recovery_after_timeout():
    """A timed-out graph query must not crash the next client operation."""
    port = os.getenv("FALKORDB_TIMEOUT_TEST_PORT")

    if not port:
        pytest.skip("Dedicated timeout test server not configured")

    worker = textwrap.dedent(
        """
        import sys

        from falkordb import FalkorDB
        from redis.exceptions import ResponseError

        port = int(sys.argv[1])
        db = FalkorDB(host="localhost", port=port)
        graph = db.select_graph("issue205_regression")

        assert graph.query("RETURN 1").result_set == [[1]]

        expensive_query = (
            "UNWIND range(1, 1000) AS x "
            "UNWIND range(1, 1000) AS y "
            "RETURN sum(x * y)"
        )

        try:
            graph.query(expensive_query, timeout=1)
        except ResponseError as error:
            if "timed out" not in str(error).lower():
                raise
            print("QUERY_TIMEOUT_CONFIRMED", flush=True)
        else:
            print("QUERY_DID_NOT_TIMEOUT", flush=True)
            sys.exit(0)

        result = graph.query("RETURN 2")
        assert result.result_set == [[2]]

        print("RECOVERY_SUCCESS", flush=True)
        """
    )

    result = subprocess.run(
        [sys.executable, "-u", "-c", worker, port],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    output = result.stdout + "\n" + result.stderr

    
    if "QUERY_DID_NOT_TIMEOUT" in output and result.returncode == 0:
        pytest.fail("Expected the expensive query to time out, but it completed")


    assert result.returncode == 0, (
        f"Worker failed after timeout. Exit: {result.returncode}\n{output}"
    )
    assert "QUERY_TIMEOUT_CONFIRMED" in output, output
    assert "RECOVERY_SUCCESS" in output, output
