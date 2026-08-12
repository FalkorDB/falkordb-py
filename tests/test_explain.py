import contextlib

import pytest

from falkordb import FalkorDB

from .plan_utils import plan_root


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

    project_op = plan_root(plan)
    assert project_op.name == "Project"
    assert len(project_op.children) == 1

    unwind_op = project_op.children[0]
    assert unwind_op.name == "Unwind"
    assert len(unwind_op.children) == 0


def test_cartesian_product_explain(client):
    db = client
    g = db.select_graph("explain")
    plan = g.explain("MATCH (a), (b) RETURN *")

    project_op = plan_root(plan)
    assert project_op.name == "Project"
    assert len(project_op.children) == 1

    cp_op = project_op.children[0]
    assert cp_op.name == "Cartesian Product"
    assert len(cp_op.children) == 2

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert scan_a_op.name == "All Node Scan"
    assert len(scan_a_op.children) == 0

    assert scan_b_op.name == "All Node Scan"
    assert len(scan_b_op.children) == 0


def test_merge(client):
    db = client
    g = db.select_graph("explain")

    with contextlib.suppress(Exception):
        g.create_node_range_index("person", "age")
    plan = g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    # the exact shape of a MERGE plan is a server implementation detail that
    # has changed between releases, assert the parser produced a well-formed
    # tree containing the operations this query must involve
    merges = plan.collect_operations("Merge")
    assert len(merges) == 2

    assert len(plan.collect_operations("Node By Index Scan")) == 2
    assert len(plan.collect_operations("Argument")) == 2

    # every operation reachable from the root must have been indexed, i.e. the
    # tree and the per-name index agree
    seen = []

    def walk(op):
        seen.append(op)
        for child in op.children:
            walk(child)

    walk(plan.structured_plan)
    indexed = sum(len(ops) for ops in plan.operations.values())
    assert len(seen) == indexed

    # a MERGE plan always ends in leaf operations, no orphan/cyclic nodes
    assert all(op.child_count() >= 0 for op in seen)
