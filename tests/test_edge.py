import pytest

from falkordb import Edge, Node


def test_init():
    with pytest.raises(AssertionError):
        Edge(None, None, None)
        Edge(Node(), None, None)
        Edge(None, None, Node())

    assert isinstance(Edge(Node(node_id=1), None, Node(node_id=2)), Edge)


def test_to_string():
    props_result = Edge(
        Node(), None, Node(), properties={"a": "a", "b": 10}
    ).to_string()
    assert props_result == '{a:"a",b:10}'

    no_props_result = Edge(Node(), None, Node(), properties={}).to_string()
    assert no_props_result == ""


def test_stringify():
    john = Node(
        alias="a",
        labels="person",
        properties={"name": "John Doe", "age": 33, "someArray": [1, 2, 3]},
    )

    japan = Node(alias="b", labels="country", properties={"name": "Japan"})

    edge_with_relation = Edge(
        john, "visited", japan, properties={"purpose": "pleasure"}
    )
    assert '(a)-[:visited{purpose:"pleasure"}]->(b)' == str(edge_with_relation)

    edge_no_relation_no_props = Edge(japan, "", john)
    assert "(b)-[]->(a)" == str(edge_no_relation_no_props)

    edge_only_props = Edge(john, "", japan, properties={"a": "b", "c": 3})
    assert '(a)-[{a:"b",c:3}]->(b)' == str(edge_only_props)


def test_comparision():
    node1 = Node(node_id=1)
    node2 = Node(node_id=2)
    node3 = Node(node_id=3)

    edge1 = Edge(node1, None, node2)
    assert edge1 == Edge(node1, None, node2)
    assert edge1 != Edge(node1, "bla", node2)
    assert edge1 != Edge(node1, None, node3)
    assert edge1 != Edge(node3, None, node2)
    assert edge1 != Edge(node2, None, node1)
    assert edge1 != Edge(node1, None, node2, properties={"a": 10})
