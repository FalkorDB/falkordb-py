import pytest

from falkordb import FalkorDB

from .plan_utils import plan_root


@pytest.fixture
def client(request):
    db = FalkorDB(host="localhost", port=6379)
    return db.select_graph("profile")


def test_profile(client):
    g = client
    plan = g.profile("UNWIND range(0, 3) AS x RETURN x")

    project_op = plan_root(plan)
    assert project_op.name == "Project"
    assert len(project_op.children) == 1
    assert project_op.profile_stats.records_produced == 4

    unwind_op = project_op.children[0]
    assert unwind_op.name == "Unwind"
    assert len(unwind_op.children) == 0
    assert unwind_op.profile_stats.records_produced == 4


def test_cartesian_product_profile(client):
    g = client
    plan = g.profile("MATCH (a), (b) RETURN *")

    project_op = plan_root(plan)
    assert project_op.name == "Project"
    assert len(project_op.children) == 1
    assert project_op.profile_stats.records_produced == 0

    cp_op = project_op.children[0]
    assert cp_op.name == "Cartesian Product"
    assert len(cp_op.children) == 2
    assert cp_op.profile_stats.records_produced == 0

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert scan_a_op.name == "All Node Scan"
    assert len(scan_a_op.children) == 0
    assert scan_a_op.profile_stats.records_produced == 0

    assert scan_b_op.name == "All Node Scan"
    assert len(scan_b_op.children) == 0
    assert scan_b_op.profile_stats.records_produced == 0
