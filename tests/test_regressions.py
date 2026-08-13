"""Regression tests for bugs that need no server connection."""

import asyncio
import contextlib
import warnings

import pytest
from redis import ResponseError

from falkordb import Edge, Node, Path
from falkordb.asyncio.graph import AsyncGraph
from falkordb.asyncio.query_result import QueryResult as AsyncQueryResult
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb.execution_plan import ExecutionPlan, Operation
from falkordb.graph import Graph, ignore_existing_index
from falkordb.query_result import QueryResult


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
        assert parse_scalar([99, "secret-value"], None) is None

    assert any("Unknown scalar type" in str(w.message) for w in caught)
    # warnings reach stderr, and stderr is commonly shipped to a log
    # aggregator, so the value itself must not appear in the message
    assert not any("secret-value" in str(w.message) for w in caught)
    assert any("type id 99" in str(w.message) for w in caught)


def test_async_parse_scalar_tolerates_unknown_type():
    from falkordb.asyncio.query_result import parse_scalar

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert asyncio.run(parse_scalar([99, "secret-value"], None)) is None

    assert any("Unknown scalar type" in str(w.message) for w in caught)
    assert not any("secret-value" in str(w.message) for w in caught)


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


def _stats_result(*stats):
    """Build a QueryResult from a statistics-only response."""
    return QueryResult(None, [list(stats)])


def test_count_statistics_are_ints():
    """Statistics annotated -> int used to return float.

    __get_statistics parses every value with float(), so metrics documented and
    annotated as counts came back as e.g. 1.0 rather than 1.
    """
    result = _stats_result(
        "Nodes created: 3",
        "Nodes deleted: 1",
        "Labels added: 2",
        "Labels removed: 1",
        "Properties set: 5",
        "Properties removed: 4",
        "Relationships created: 6",
        "Relationships deleted: 2",
        "Indices created: 1",
        "Indices deleted: 1",
    )

    counts = {
        "nodes_created": 3,
        "nodes_deleted": 1,
        "labels_added": 2,
        "labels_removed": 1,
        "properties_set": 5,
        "properties_removed": 4,
        "relationships_created": 6,
        "relationships_deleted": 2,
        "indices_created": 1,
        "indices_deleted": 1,
    }

    for name, expected in counts.items():
        value = getattr(result, name)
        assert value == expected, name
        assert isinstance(value, int), f"{name} returned {type(value).__name__}"
        assert not isinstance(value, float), name


def test_run_time_ms_stays_a_float():
    """Execution time is genuinely fractional and must not be truncated."""
    result = _stats_result("internal execution time: 1.75")

    assert isinstance(result.run_time_ms, float)
    assert result.run_time_ms == 1.75


def test_missing_statistic_is_zero():
    result = _stats_result("Nodes created: 1")

    assert result.nodes_created == 1
    assert result.nodes_deleted == 0
    assert isinstance(result.nodes_deleted, int)


def test_query_result_is_iterable_and_sized():
    """QueryResult had no __iter__/__len__, so results could not be iterated."""
    result = _stats_result("Nodes created: 0")
    result._result_set = [["a", 1], ["b", 2]]

    assert len(result) == 2
    assert list(result) == [["a", 1], ["b", 2]]
    assert [row[0] for row in result] == ["a", "b"]


async def _async_stats_result(*stats):
    """Build an async QueryResult from a statistics-only response."""
    result = AsyncQueryResult(None)
    await result.parse([list(stats)])
    return result


def test_async_count_statistics_are_ints():
    """The async result set must report counts as ints, like the sync one."""
    result = asyncio.run(
        _async_stats_result(
            "Nodes created: 3",
            "Relationships created: 6",
            "Indices created: 1",
            "internal execution time: 1.75",
        )
    )

    assert result.nodes_created == 3
    assert isinstance(result.nodes_created, int)
    assert result.relationships_created == 6
    assert isinstance(result.relationships_created, int)
    assert result.indices_created == 1
    assert isinstance(result.indices_created, int)
    assert result.run_time_ms == 1.75
    assert isinstance(result.run_time_ms, float)


def test_node_hash_matches_equality():
    """Equal objects must hash equally, or sets and dicts miss them.

    Node.__eq__ treats a node with an unset id as equal to an otherwise
    identical node that has one, so the id cannot take part in the hash.
    """
    with_id = Node(node_id=1, alias="a", labels="A")
    without_id = Node(alias="a", labels="A")

    assert with_id == without_id
    assert hash(with_id) == hash(without_id)
    assert len({with_id, without_id}) == 1
    assert {with_id: "v"}[without_id] == "v"


def test_edge_hash_matches_equality():
    """The same contract, for edges."""
    src = Node(node_id=1, labels="P")
    dest = Node(node_id=2, labels="P")

    with_id = Edge(src, "KNOWS", dest, edge_id=7)
    without_id = Edge(src, "KNOWS", dest)

    assert with_id == without_id
    assert hash(with_id) == hash(without_id)
    assert len({with_id, without_id}) == 1
    assert {with_id: "v"}[without_id] == "v"


def test_edges_sharing_an_id_but_not_a_relation_differ():
    """A shared id alone must not make two edges equal.

    Equality short-circuits on a matching id, so without also comparing the
    relation there would be no invariant left for __hash__ to use.
    """
    src = Node(node_id=1, labels="P")
    dest = Node(node_id=2, labels="P")

    assert Edge(src, "KNOWS", dest, edge_id=7) != Edge(src, "LIKES", dest, edge_id=7)
    assert Edge(src, "KNOWS", dest, edge_id=7) == Edge(src, "KNOWS", dest, edge_id=7)


def test_path_hash_matches_equality():
    """Path hashing inherits the contract from the models it contains."""
    src = Node(node_id=1, labels="P")
    dest = Node(node_id=2, labels="P")
    left = Path([src, dest], [Edge(src, "KNOWS", dest, edge_id=7)])
    right = Path([src, dest], [Edge(src, "KNOWS", dest)])

    assert left == right
    assert hash(left) == hash(right)
    assert len({left, right}) == 1


def test_existing_index_error_is_ignored():
    """A unique constraint tolerates the range index it needs already existing."""
    with ignore_existing_index():
        raise ResponseError("Attribute 'age' is already indexed")


def test_unrelated_response_error_is_not_ignored():
    """Only the already-indexed case may be swallowed.

    Suppressing every ResponseError would let a rejected label or an
    unsupported command masquerade as an index that was already in place.
    """
    with pytest.raises(ResponseError, match="Unknown command"), ignore_existing_index():
        raise ResponseError("Unknown command 'GRAPH.INDEX'")


def test_call_procedure_rejects_injected_procedure_name():
    """The procedure name is query text, nothing downstream quotes it."""
    g = Graph(SyncStubClient(), "g")

    with pytest.raises(ValueError, match="invalid procedure name"):
        g.call_procedure(
            "db.labels() YIELD label WITH label CREATE (:PWNED) RETURN label //",
            read_only=False,
        )


def test_call_procedure_rejects_injected_yield_name():
    g = Graph(SyncStubClient(), "g")

    with pytest.raises(ValueError, match="invalid YIELD name"):
        g.call_procedure(
            "db.labels",
            read_only=False,
            emit=["label WITH label CREATE (:PWNED) RETURN label //"],
        )


def test_call_procedure_still_accepts_ordinary_names():
    client = SyncStubClient()
    g = Graph(client, "g")

    g.call_procedure("DB.LABELS", emit=["label"])
    g.call_procedure("algo.pageRank", emit=["node AS n", "score"])
    g.call_procedure("db.idx.fulltext.queryNodes", emit=["*"])

    assert "CALL DB.LABELS()YIELD label" in client.commands[0][2]
    assert "YIELD node AS n,score" in client.commands[1][2]


def test_index_options_are_serialized_not_pasted():
    """Index options reach the query as literals and need the same quoting.

    The map used to be built with str() and unescaped single quotes, which is
    the pattern the parameter serializer was hardened against.
    """
    client = SyncStubClient()
    g = Graph(client, "g")

    g.create_node_vector_index("Doc", "embedding", dim=4, similarity_function="cosine")
    query = client.commands[0][2]
    assert 'OPTIONS {`dimension`:4,`similarityFunction`:"cosine"}' in query

    class Sneaky:
        def __str__(self):
            return "4, foo:1"

    with pytest.raises(TypeError, match="unsupported Cypher parameter type"):
        g.create_node_vector_index("L", "v", dim=Sneaky())


def test_index_option_string_cannot_escape_its_quotes():
    client = SyncStubClient()
    g = Graph(client, "g")

    g.create_node_vector_index("L", "v", dim=4, similarity_function='a" , foo:"b')
    query = client.commands[0][2]
    # one option value, the quote is escaped rather than closing the literal
    assert '`similarityFunction`:"a\\" , foo:\\"b"' in query


def test_procedure_name_subclass_cannot_change_what_is_emitted():
    """The validated value must be the value that reaches the query.

    Returning the caller's object and interpolating it means f-string
    formatting calls __format__, which a str subclass controls, so the name
    that was checked and the name that runs can differ.
    """
    client = SyncStubClient()
    g = Graph(client, "g")

    class EvilProc(str):
        def __format__(self, spec):
            return "db.labels() YIELD label WITH label CREATE (:PWNED) //"

    g.call_procedure(EvilProc("db.labels"), read_only=False)
    assert "CALL db.labels()" in client.commands[0][2]
    assert "PWNED" not in client.commands[0][2]


def test_yield_name_subclass_cannot_change_what_is_emitted():
    """The names were validated after .strip(), which the caller controls."""
    g = Graph(SyncStubClient(), "g")

    class EvilYield(str):
        def strip(self, *args):
            return "label"

    with pytest.raises(ValueError, match="invalid YIELD name"):
        g.call_procedure(
            "db.labels",
            read_only=False,
            emit=[EvilYield("label WITH label CREATE (:PWNED) //")],
        )


def test_index_identifiers_are_backticked():
    """Labels and property names are query text, not parameters.

    Without backticks a label could close the pattern and redirect the
    statement, and a property could widen the index.
    """
    client = SyncStubClient()
    g = Graph(client, "g")

    g.create_node_range_index("Person", "age")
    assert "CREATE  INDEX FOR (e:`Person`) ON (e.`age`)" in client.commands[0][2]

    g.drop_node_range_index("Person", "age")
    assert "DROP INDEX FOR (e:`Person`) ON (e.`age`)" in client.commands[1][2]

    # a property that used to expand into two indexed properties
    g.create_node_range_index("L", "age, e.secret")
    assert "ON (e.`age, e.secret`)" in client.commands[2][2]

    with pytest.raises(ValueError, match="backtick"):
        g.create_node_range_index("L`", "age")
