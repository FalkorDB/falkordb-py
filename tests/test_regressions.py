"""Regression tests for bugs that need no server connection."""

import asyncio
import contextlib
import warnings

import pytest
from redis import ResponseError

from falkordb import Edge, Node, Path
from falkordb.asyncio.graph import AsyncGraph
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb.execution_plan import ExecutionPlan, Operation
from falkordb.graph import Graph


class SyncStubClient:
    """Records the commands issued by Graph without touching a server."""

    def __init__(self, responses=None):
        self.commands = []
        self.responses = responses or []

    def execute_command(self, *args):
        self.commands.append(args)
        if self.responses:
            return self.responses.pop(0)
        return [[], [], []]


def test_call_procedure_does_not_mutate_caller_args():
    client = SyncStubClient()
    g = Graph(client, "g")

    args = ["Label", "hello"]
    g.call_procedure("proc", args=args)

    # the caller's list must be untouched, it used to be rewritten in place
    assert args == ["Label", "hello"]

    # so a second identical call produces an identical command
    g.call_procedure("proc", args=args)
    assert client.commands[0][2] == client.commands[1][2]
    assert "$param0" in client.commands[0][2]


def test_async_schema_refresh_is_awaited():
    class AsyncStubClient:
        def __init__(self):
            self.calls = 0

        async def execute_command(self, *args):
            self.calls += 1
            if self.calls == 1:
                return [ResponseError("version mismatch"), 7]
            return [["label"]]

    async def scenario():
        g = AsyncGraph(AsyncStubClient(), "g")
        assert g.schema.version == 0

        with pytest.raises(SchemaVersionMismatchException):
            await g.query("RETURN 1")

        # the refresh coroutine used to be dropped, leaving the cache stale
        assert g.schema.version == 7

    with warnings.catch_warnings():
        # an un-awaited coroutine must fail the test rather than warn
        warnings.simplefilter("error", RuntimeWarning)
        asyncio.run(scenario())


def test_parse_scalar_tolerates_unknown_type():
    from falkordb.query_result import parse_scalar

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        # a newer server may return a scalar type this client does not know
        assert parse_scalar([99, "x"], None) is None

    assert any("Unknown scalar type" in str(w.message) for w in caught)


def test_parse_scalar_known_type_still_works():
    from falkordb.query_result import parse_scalar

    assert parse_scalar([3, "7"], None) == 7


def test_execution_plan_rejects_empty_plan():
    with pytest.raises(ValueError, match="at least one operation"):
        ExecutionPlan([])


def test_execution_plan_indentation_uses_leading_spaces_only():
    # a label containing four consecutive spaces must not be read as an indent
    plan = ExecutionPlan(["Project", "    Node By Label Scan | (n:a    b)"])

    root = plan.structured_plan
    assert root.name == "Project"
    assert len(root.children) == 1
    assert root.children[0].name == "Node By Label Scan"
    assert root.children[0].args == "(n:a    b)"


def test_execution_plan_tolerates_integer_execution_time():
    plan = ExecutionPlan(
        ["Results", "    Project | Records produced: 1, Execution time: 0 ms"]
    )
    project = plan.structured_plan.children[0]
    assert project.records_produced == 1
    assert project.execution_time == 0.0


def test_operation_without_profile_stats_raises_clearly():
    op = Operation("Project")
    with pytest.raises(ValueError, match="profile statistics"):
        _ = op.execution_time
    with pytest.raises(ValueError, match="profile statistics"):
        _ = op.records_produced


def test_models_are_hashable():
    # deduplicating query results via a set is an obvious operation
    node = Node(node_id=1, labels="L")
    edge = Edge(node, "R", Node(node_id=2), edge_id=1)
    path = Path([node], [])
    op = Operation("Project")

    assert len({node, Node(node_id=1, labels="L")}) == 1
    assert len({edge}) == 1
    assert len({path}) == 1
    assert len({op}) == 1
    assert {node: "value"}[node] == "value"


def test_models_have_useful_repr():
    assert "Node(id=1" in repr(Node(node_id=1))
    assert "Edge(" in repr(Edge(Node(node_id=1), "R", Node(node_id=2)))
    assert "Path(" in repr(Path([], []))
    assert "Operation(name='Project'" in repr(Operation("Project"))


def test_empty_path_str_does_not_raise():
    assert str(Path([], [])) == "<>"


def test_path_str_direction_follows_edge_source():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)

    forward = Path([node_1, node_2], [Edge(node_1, "R", node_2, edge_id=0)])
    assert str(forward) == "<(1)-[0]->(2)>"

    backward = Path([node_1, node_2], [Edge(node_2, "R", node_1, edge_id=0)])
    assert str(backward) == "<(1)<-[0]-(2)>"


def test_cluster_conn_does_not_mutate_pool_kwargs():
    import redis

    from falkordb.cluster import Cluster_Conn

    pool = redis.ConnectionPool(
        host="127.0.0.1", port=6379, username=None, password=None
    )
    conn = redis.Redis(connection_pool=pool)
    before = dict(pool.connection_kwargs)

    # connecting to a non-cluster server fails, the kwargs check is what
    # matters here
    with contextlib.suppress(Exception):
        Cluster_Conn(conn, False)

    # the pool used to be emptied, leaving later connections unauthenticated
    assert dict(pool.connection_kwargs) == before
