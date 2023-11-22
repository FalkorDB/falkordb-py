import pytest
from falkordb import Node


@pytest.fixture
def fixture():
    no_args     = Node(alias="n")
    no_props    = Node(node_id=1, alias="n", labels="l")
    no_label    = Node(node_id=1, alias="n", properties={"a": "a"})
    props_only  = Node(alias="n", properties={"a": "a", "b": 10})
    multi_label = Node(node_id=1, alias="n", labels=["l", "ll"])

    return no_args, no_props, props_only, no_label, multi_label


def test_to_string(fixture):
    no_args, no_props, props_only, no_label, multi_label = fixture

    assert no_args.to_string()     == ""
    assert no_props.to_string()    == ""
    assert no_label.to_string()    == '{a:"a"}'
    assert props_only.to_string()  == '{a:"a",b:10}'
    assert multi_label.to_string() == ""


def test_stringify(fixture):
    no_args, no_props, props_only, no_label, multi_label = fixture

    assert str(no_args)     == "(n)"
    assert str(no_props)    == "(n:l)"
    assert str(no_label)    == '(n{a:"a"})'
    assert str(props_only)  == '(n{a:"a",b:10})'
    assert str(multi_label) == "(n:l:ll)"


def test_comparision():
    assert Node()                                 != Node(properties={"a": 10})
    assert Node()                                 == Node()
    assert Node(node_id=1)                        == Node(node_id=1)
    assert Node(node_id=1)                        != Node(node_id=2)
    assert Node(node_id=1, alias="a")             == Node(node_id=1, alias="b")
    assert Node(node_id=1, alias="a")             == Node(node_id=1, alias="a")
    assert Node(node_id=1, labels="a")            == Node(node_id=1, labels="a")
    assert Node(node_id=1, labels="a")            != Node(node_id=1, labels="b")
    assert Node(alias="a", labels="l")            != Node(alias="a", labels="l1")
    assert Node(properties={"a": 10})             == Node(properties={"a": 10})
    assert Node(node_id=1, alias="a", labels="l") == Node(node_id=1, alias="a", labels="l")
