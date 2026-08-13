import pytest

from falkordb import FalkorDB

from .plan_helpers import assert_profile_shape


@pytest.fixture
def client(request):
    db = FalkorDB(host="localhost", port=6379)
    return db.select_graph("profile")


def test_profile(client):
    g = client
    plan = g.profile("UNWIND range(0, 3) AS x RETURN x")

    assert_profile_shape(
        plan,
        """
        Project
            Unwind
        """,
        records_produced=4,
    )


def test_cartesian_product_profile(client):
    g = client
    plan = g.profile("MATCH (a), (b) RETURN *")

    assert_profile_shape(
        plan,
        """
        Project
            Cartesian Product
                All Node Scan | (a)
                All Node Scan | (b)
        """,
        records_produced=0,
    )
