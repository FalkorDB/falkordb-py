import pytest
from falkordb import Node, Edge, Path


def test_init():
    with pytest.raises(TypeError):
        Path(None, None)
        Path([], None)
        Path(None, [])

    assert isinstance(Path([], []), Path)


def test_new_empty_path():
    nodes = []
    edges = []
    path = Path(nodes, edges)
    assert isinstance(path, Path)
    assert path._nodes == []
    assert path._edges == []

def test_wrong_flows():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    node_3 = Node(node_id=3)

    edge_1 = Edge(node_1, None, node_2)
    edge_2 = Edge(node_1, None, node_3)

    nodes = [node_1, node_2, node_3]
    edges = [edge_1, edge_2]

def test_nodes_and_edges():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2)

    nodes = [node_1, node_2]
    edges = [edge_1]

    p = Path(nodes, edges)
    assert nodes == p.nodes()
    assert node_1 == p.get_node(0)
    assert node_2 == p.get_node(1)
    assert node_1 == p.first_node()
    assert node_2 == p.last_node()
    assert 2 == p.node_count()

    assert edges == p.edges()
    assert 1 == p.edge_count()
    assert edge_1 == p.get_edge(0)

    assert p.get_node(-1) is None
    assert p.get_edge(49) is None

    path_str = str(p)
    assert path_str == "<(1)-[]->(2)>"


def test_compare():
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2)
    nodes = [node_1, node_2]
    edges = [edge_1]

    assert Path([], [])             == Path([], [])
    assert Path(nodes, edges)       == Path(nodes, edges)
    assert Path(nodes, [])          != Path([], [])
    assert Path([node_1], [])       != Path([], [])
    assert Path([node_1], edges=[]) != Path([node_2], [])
    assert Path([node_1], [edge_1]) != Path( [node_1], [])
    assert Path([node_1], [edge_1]) != Path([node_2], [edge_1])

    assert not (Path(nodes, edges) == "this is not a path")
    
def test_str_with_none_edge_id():
    """Test that Path.__str__() works when edge.id is None"""
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2)
    
    nodes = [node_1, node_2]
    edges = [edge_1]
    
    p = Path(nodes, edges)
    # Should not raise an exception
    path_str = str(p)
    assert isinstance(path_str, str)
    # The edge should be represented with empty brackets since id is None
    assert ('<-[]-' in path_str) or ('-[]->' in path_str)

def test_str_with_edge_id():
    """Test that Path.__str__() works when edge.id is provided"""
    node_1 = Node(node_id=1)
    node_2 = Node(node_id=2)
    edge_1 = Edge(node_1, None, node_2, edge_id=10)
    
    nodes = [node_1, node_2]
    edges = [edge_1]
    
    p = Path(nodes, edges)
    path_str = str(p)
    assert isinstance(path_str, str)
    # The edge should be represented with the ID
    assert "10" in path_str
