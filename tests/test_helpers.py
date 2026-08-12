"""Unit tests for Cypher parameter serialization.

These are pure functions, no server is required.
"""

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from falkordb.helpers import quote_string, stringify_param_value


def test_quote_string():
    assert quote_string("") == '""'
    assert quote_string("hello") == '"hello"'
    assert quote_string(b"hello") == '"hello"'
    assert quote_string('say "hi"') == '"say \\"hi\\""'
    assert quote_string("back\\slash") == '"back\\\\slash"'

    # non textual values pass through untouched
    assert quote_string(5) == 5
    assert quote_string(None) is None


def test_quote_string_rejects_nul():
    # a NUL byte crashes the server's query-header parser, reject it here
    with pytest.raises(ValueError, match="NUL byte"):
        quote_string("a\x00b")

    with pytest.raises(ValueError, match="NUL byte"):
        stringify_param_value("a\x00b")


def test_scalars():
    assert stringify_param_value(None) == "null"
    assert stringify_param_value(True) == "true"
    assert stringify_param_value(False) == "false"
    assert stringify_param_value(42) == "42"
    assert stringify_param_value(-7) == "-7"
    assert stringify_param_value(3.5) == "3.5"
    assert stringify_param_value(Decimal("1.5")) == "1.5"
    assert stringify_param_value("hi") == '"hi"'
    assert stringify_param_value(b"hi") == '"hi"'


def test_bool_is_not_serialized_as_int():
    # bool subclasses int, the bool branch must be checked first
    assert stringify_param_value(True) == "true"
    assert stringify_param_value([True, 1]) == "[true,1]"


def test_temporal_values():
    assert stringify_param_value(datetime(2024, 1, 1, 12, 30)) == (
        '"2024-01-01T12:30:00"'
    )
    assert stringify_param_value(date(2024, 1, 1)) == '"2024-01-01"'
    assert stringify_param_value(time(12, 30)) == '"12:30:00"'


def test_collections():
    assert stringify_param_value([1, 2]) == "[1,2]"
    assert stringify_param_value((1, "a")) == '[1,"a"]'
    assert stringify_param_value([]) == "[]"
    assert stringify_param_value({"a": 1}) == "{`a`:1}"
    assert stringify_param_value({"a": [1, None]}) == "{`a`:[1,null]}"


def test_map_keys_are_backtick_quoted():
    assert stringify_param_value({"@type": 1}) == "{`@type`:1}"
    assert stringify_param_value({b"k": 1}) == "{`k`:1}"

    with pytest.raises(ValueError, match="empty"):
        stringify_param_value({"": 1})

    with pytest.raises(ValueError, match="backtick"):
        stringify_param_value({"a`b": 1})


def test_non_finite_floats_rejected():
    for value in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError, match="Cypher literal representation"):
            stringify_param_value(value)


def test_unsupported_types_rejected():
    # falling back to str() would let arbitrary Cypher be injected
    class Sneaky:
        def __str__(self):
            return "1 CREATE (:PWNED) //"

    with pytest.raises(TypeError, match="unsupported Cypher parameter type"):
        stringify_param_value(Sneaky())

    with pytest.raises(TypeError, match="unsupported Cypher parameter type"):
        stringify_param_value({1, 2})

    with pytest.raises(TypeError, match="unsupported Cypher parameter type"):
        stringify_param_value(object())


def test_injection_attempt_nested_in_collection():
    class Sneaky:
        def __str__(self):
            return "1 CREATE (:PWNED) //"

    with pytest.raises(TypeError):
        stringify_param_value([Sneaky()])

    with pytest.raises(TypeError):
        stringify_param_value({"k": Sneaky()})
