import pytest

from falkordb import FalkorDB

from .plan_helpers import assert_parsed_profile


@pytest.fixture
def client(request):
    db = FalkorDB(host="localhost", port=6379)
    return db.select_graph("profile")


def test_profile(client):
    g = client
    plan = g.profile("UNWIND range(0, 3) AS x RETURN x")

    # which operations the query compiles into is up to the engine, the client
    # is responsible for parsing the plan and its statistics
    assert_parsed_profile(plan, min_operations=2, records_produced=4)


def test_cartesian_product_profile(client):
    g = client
    plan = g.profile("MATCH (a), (b) RETURN *")

    assert_parsed_profile(plan, min_operations=4, expect_args=True, records_produced=0)
