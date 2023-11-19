import pytest
from falkordb_py import Node, Edge, Path


def test_init():
    with pytest.raises(TypeError):
        Path(None, None)
        Path([], None)
        Path(None, [])

    assert isinstance(Path([], []), Path)


def test_new_empty_path():
    new_empty_path = Path.new_empty_path()
    assert isinstance(new_empty_path, Path)
    assert new_empty_path._nodes == []
    assert new_empty_path._edges == []


def test_wrong_flows():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    node_3 = Node(node_id=3)

    edge_1 = Edge(node_1, None, node_2)
    edge_2 = Edge(node_1, None, node_3)

    p = Path.new_empty_path()
    with pytest.raises(AssertionError):
        p.add_edge(edge_1)

    p.add_node(node_1)
    with pytest.raises(AssertionError):
        p.add_node(node_2)

    p.add_edge(edge_1)
    with pytest.raises(AssertionError):
        p.add_edge(edge_2)


def test_nodes_and_edges():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2)

    p = Path.new_empty_path()
    assert p.nodes() == []

    p.add_node(node_1)
    assert [] == p.edges()
    assert 0 == p.edge_count()
    assert [node_1] == p.nodes()
    assert node_1 == p.get_node(0)
    assert node_1 == p.first_node()
    assert node_1 == p.last_node()
    assert 1 == p.node_count()

    p.add_edge(edge_1)
    assert [edge_1] == p.edges()
    assert 1 == p.edge_count()
    assert edge_1 == p.get_edge(0)

    p.add_node(node_2)
    assert [node_1, node_2] == p.nodes()
    assert node_1 == p.first_node()
    assert node_2 == p.last_node()
    assert 2 == p.node_count()


def test_compare():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2)

    assert Path.new_empty_path() == Path.new_empty_path()
    assert Path(nodes=[node_1, node_2], edges=[edge_1]) == Path(
        nodes=[node_1, node_2], edges=[edge_1]
    )
    assert Path(nodes=[node_1], edges=[]) != Path(nodes=[], edges=[])
    assert Path(nodes=[node_1], edges=[]) != Path(nodes=[], edges=[])
    assert Path(nodes=[node_1], edges=[]) != Path(nodes=[node_2], edges=[])
    assert Path(nodes=[node_1], edges=[edge_1]) != Path(
        nodes=[node_1], edges=[]
    )
    assert Path(nodes=[node_1], edges=[edge_1]) != Path(
        nodes=[node_2], edges=[edge_1]
    )
