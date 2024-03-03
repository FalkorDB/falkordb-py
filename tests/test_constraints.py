import pytest
from redis import ResponseError
from falkordb import FalkorDB

def test_constraints():
    db = FalkorDB(host='localhost', port=6379)
    g = db.select_graph("constraints")

    # create node constraints
    g.create_node_unique_constraint("Person", "name")
    g.create_node_mandatory_constraint("Person", "name")
    g.create_node_unique_constraint("Person", "v1", "v2")

    # create edge constraints
    g.create_edge_unique_constraint("KNOWS", "since")
    g.create_edge_mandatory_constraint("KNOWS", "since")
    g.create_edge_unique_constraint("KNOWS", "v1", "v2")

    constraints = g.list_constraints()
    assert(len(constraints) == 6)

    # drop constraints
    g.drop_node_unique_constraint("Person", "name")
    g.drop_node_mandatory_constraint("Person", "name")
    g.drop_node_unique_constraint("Person", "v1", "v2")

    g.drop_edge_unique_constraint("KNOWS", "since")
    g.drop_edge_mandatory_constraint("KNOWS", "since")
    g.drop_edge_unique_constraint("KNOWS", "v1", "v2")

    constraints = g.list_constraints()
    assert(len(constraints) == 0)

def test_create_existing_constraint():
    # trying to create an existing constraint
    db = FalkorDB(host='localhost', port=6379)
    g = db.select_graph("constraints")

    # create node constraints
    g.create_node_unique_constraint("Person", "name")
    try:
        g.create_node_unique_constraint("Person", "name")
        assert(False)
    except Exception as e:
        assert("Constraint already exists" == str(e))

