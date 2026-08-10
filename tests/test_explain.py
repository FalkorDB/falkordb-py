import pytest

from falkordb import FalkorDB

from .plan_helpers import assert_parsed_plan


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

    # which operations the query compiles into is up to the engine, the client
    # is responsible for parsing whatever plan comes back
    assert_parsed_plan(plan, min_operations=2)


def test_cartesian_product_explain(client):
    db = client
    g = db.select_graph("explain")
    plan = g.explain("MATCH (a), (b) RETURN *")

    assert_parsed_plan(plan, min_operations=4, expect_args=True)


def test_merge(client):
    db = client
    g = db.select_graph("explain")

    try:
        g.create_node_range_index("person", "age")
    except Exception:
        pass
    plan = g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    assert_parsed_plan(plan, min_operations=4, expect_args=True)
