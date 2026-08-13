import pytest
from redis import ResponseError

from falkordb import FalkorDB

from .plan_helpers import assert_parsed_plan, assert_plan_shape


@pytest.fixture
def client(request):
    db = FalkorDB(host="localhost", port=6379)
    return db


def test_explain(client):
    db = client
    g = db.select_graph("explain")

    # run a single query to create the graph
    g.query("RETURN 1")

    plan = g.explain("UNWIND range(0, 3) AS x RETURN x")

    assert_plan_shape(
        plan,
        """
        Project
            Unwind
        """,
    )


def test_cartesian_product_explain(client):
    db = client
    g = db.select_graph("explain")
    plan = g.explain("MATCH (a), (b) RETURN *")

    assert_plan_shape(
        plan,
        """
        Project
            Cartesian Product
                All Node Scan | (a)
                All Node Scan | (b)
        """,
    )


def test_cartesian_product_explain_three_way(client):
    db = client
    g = db.select_graph("explain")
    plan = g.explain("MATCH (a), (b), (c) RETURN *")

    # three operations share a nesting level here, which is what the parser
    # used to get wrong: it attached the third scan to the second instead of
    # to the cartesian product. The engines scan the three in whichever order
    # they like, so the arguments are left out.
    assert_plan_shape(
        plan,
        """
        Project
            Cartesian Product
                All Node Scan
                All Node Scan
                All Node Scan
        """,
    )


def test_merge(client):
    db = client
    g = db.select_graph("explain")

    try:
        g.create_node_range_index("person", "age")
    except ResponseError as e:
        # an earlier run of this test already created the index, any other
        # failure is a real one and must not be swallowed
        assert "already indexed" in str(e), e
    plan = g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    # the two engines compile MERGE differently — Rust matches through an
    # "Include Pending" operation, C emits a "MergeCreate" — so there is no
    # single tree to assert. Check the client parsed whatever came back.
    assert_parsed_plan(plan, min_operations=4, expect_args=True)
