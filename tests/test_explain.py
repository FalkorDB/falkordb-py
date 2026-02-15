import pytest

from falkordb import FalkorDB


@pytest.fixture
def client(request):
    db = FalkorDB(host='localhost', port=6379)
    return db


def test_explain(client):
    db = client
    g = db.select_graph("explain")

    # run a single query to create the graph
    g.query("RETURN 1")

    plan = g.explain("UNWIND range(0, 3) AS x RETURN x")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)

    unwind_op = project_op.children[0]
    assert(unwind_op.name == 'Unwind')
    assert(len(unwind_op.children) == 0)

def test_cartesian_product_explain(client):
    db = client
    g = db.select_graph("explain")
    plan = g.explain("MATCH (a), (b) RETURN *")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)

    cp_op = project_op.children[0]
    assert(cp_op.name == 'Cartesian Product')
    assert(len(cp_op.children) == 2)

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert(scan_a_op.name == 'All Node Scan')
    assert(len(scan_a_op.children) == 0)

    assert(scan_b_op.name == 'All Node Scan')
    assert(len(scan_b_op.children) == 0)

def test_merge(client):
    db = client
    g = db.select_graph("explain")

    try:
        g.create_node_range_index("person", "age")
    except Exception:
        pass
    plan = g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    root = plan.structured_plan
    assert(root.name == 'Merge')
    assert(len(root.children) == 3)

    merge_op = root.children[0]
    assert(merge_op.name == 'Merge')
    assert(len(merge_op.children) == 2)

    index_scan_op = merge_op.children[0]
    assert(index_scan_op.name == 'Node By Index Scan')
    assert(len(index_scan_op.children) == 0)

    merge_create_op = merge_op.children[1]
    assert(merge_create_op.name == 'MergeCreate')
    assert(len(merge_create_op.children) == 0)

    index_scan_op = root.children[1]
    assert(index_scan_op.name == 'Node By Index Scan')
    assert(len(index_scan_op.children) == 1)

    arg_op = index_scan_op.children[0]
    assert(arg_op.name == 'Argument')
    assert(len(arg_op.children) == 0)

    merge_create_op = root.children[2]
    assert(merge_create_op.name == 'MergeCreate')
    assert(len(merge_create_op.children) == 1)

    arg_op = merge_create_op.children[0]
    assert(arg_op.name == 'Argument')
    assert(len(arg_op.children) == 0)
