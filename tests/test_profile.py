import pytest

from falkordb import FalkorDB


@pytest.fixture
def client(request):
    db = FalkorDB(host='localhost', port=6379)
    return db.select_graph("profile")


def test_profile(client):
    g = client
    plan = g.profile("UNWIND range(0, 3) AS x RETURN x")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)
    assert(results_op.profile_stats.records_produced == 4)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)
    assert(project_op.profile_stats.records_produced == 4)

    unwind_op = project_op.children[0]
    assert(unwind_op.name == 'Unwind')
    assert(len(unwind_op.children) == 0)
    assert(unwind_op.profile_stats.records_produced == 4)

def test_cartesian_product_profile(client):
    g = client
    plan = g.profile("MATCH (a), (b) RETURN *")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)
    assert(results_op.profile_stats.records_produced == 0)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)
    assert(project_op.profile_stats.records_produced == 0)

    cp_op = project_op.children[0]
    assert(cp_op.name == 'Cartesian Product')
    assert(len(cp_op.children) == 2)
    assert(cp_op.profile_stats.records_produced == 0)

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert(scan_a_op.name == 'All Node Scan')
    assert(len(scan_a_op.children) == 0)
    assert(scan_a_op.profile_stats.records_produced == 0)

    assert(scan_b_op.name == 'All Node Scan')
    assert(len(scan_b_op.children) == 0)
    assert(scan_b_op.profile_stats.records_produced == 0)
