"""Unit tests for Cypher parameter serialization.

These are pure functions, no server is required.
"""

from datetime import date, datetime, time
from decimal import Decimal
from enum import IntEnum

import pytest

from falkordb.helpers import quote_identifier, quote_string, stringify_param_value


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


def test_non_finite_decimals_rejected():
    for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
        with pytest.raises(ValueError, match="Cypher literal representation"):
            stringify_param_value(value)


def test_decimal_keeps_its_own_precision():
    # going through float() would silently round these to a double
    assert (
        stringify_param_value(Decimal("1.2345678901234567890123"))
        == "1.2345678901234567890123"
    )
    assert (
        stringify_param_value(Decimal("123456789012345678901234567890"))
        == "123456789012345678901234567890"
    )


def test_large_finite_decimal_is_not_rejected():
    # float(Decimal("1E+400")) overflows to inf, the Decimal itself is finite
    # and the server is the one that decides whether it can hold the value
    assert stringify_param_value(Decimal("1E+400")) == "1E+400"


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


def test_numeric_subclasses_cannot_inject_cypher():
    """repr()/str() of a subclass is attacker-controlled.

    The strict type whitelist is not enough on its own: isinstance() accepts
    subclasses, so a subclass overriding __repr__ or __str__ would have its
    output spliced straight into the query. Every numeric branch normalizes
    to the exact base type first.
    """

    class EvilInt(int):
        def __repr__(self):
            return "1 CREATE (:PWNED) //"

    class EvilFloat(float):
        def __repr__(self):
            return "1.0 CREATE (:PWNED) //"

    class EvilDecimal(Decimal):
        def __str__(self):
            return "1 CREATE (:PWNED) //"

    assert stringify_param_value(EvilInt(1)) == "1"
    assert stringify_param_value(EvilFloat(1.0)) == "1.0"
    assert stringify_param_value(EvilDecimal("1")) == "1"


def test_int_enum_renders_as_its_value():
    """IntEnum is an int subclass whose repr() is "<Color.RED: 1>".

    Passing one used to produce a query the server could not parse.
    """

    class Color(IntEnum):
        RED = 1

    assert stringify_param_value(Color.RED) == "1"
    assert stringify_param_value([Color.RED]) == "[1]"


def test_decimal_subclass_cannot_lie_about_being_finite():
    class LyingDecimal(Decimal):
        def is_finite(self):
            return True

    with pytest.raises(ValueError, match="Cypher literal representation"):
        stringify_param_value(LyingDecimal("NaN"))


def test_temporal_subclass_returning_non_string_is_quoted():
    """quote_string passes non-textual values through unquoted."""

    class OddDateTime(datetime):
        def isoformat(self, *args, **kwargs):
            return 12345

    assert stringify_param_value(OddDateTime(2024, 1, 1)) == '"12345"'


def test_string_subclasses_cannot_disable_escaping():
    """quote_string escapes by calling methods on the value itself.

    isinstance() accepts subclasses, so overriding replace() would leave the
    quotes and backslashes unescaped and let the value close its own string
    literal. The value is normalized to an exact str first.
    """

    class EvilStr(str):
        def replace(self, *args, **kwargs):
            return self

    assert quote_string(EvilStr('x" CREATE (:PWNED) //')) == '"x\\" CREATE (:PWNED) //"'


def test_string_subclass_cannot_hide_a_nul_byte():
    """The NUL guard is an __contains__ call, which a subclass can override.

    A NUL byte reaching the query header terminates the FalkorDB process.
    """

    class NulStr(str):
        def __contains__(self, item):
            return False

    with pytest.raises(ValueError, match="NUL byte"):
        quote_string(NulStr("a\x00b"))


def test_bytes_subclass_cannot_smuggle_an_unescaped_string():
    class EvilBytes(bytes):
        def decode(self, *args, **kwargs):
            class EvilStr(str):
                def replace(self, *a, **k):
                    return self

            return EvilStr('x" CREATE (:PWNED) //')

    assert quote_string(EvilBytes(b"ok")) == '"ok"'


def test_identifier_guards_cannot_be_bypassed_by_a_subclass():
    """Identifiers are interpolated between backticks with no other quoting.

    Both the str-subclass path and the str() fallback have to be normalized,
    since str() returns whatever __str__ hands back, subclass included.
    """

    class Lying(str):
        def __contains__(self, item):
            return False

    class StrKey(str):
        def __str__(self):
            return Lying("k` , n:PWNED {x:1}) //")

    class ObjectKey:
        def __str__(self):
            return Lying("k` , n:PWNED {x:1}) //")

    # the real string data is used, the lying __str__ is ignored
    assert quote_identifier(StrKey("k")) == "k"
    assert stringify_param_value({StrKey("k"): 1}) == "{`k`:1}"

    with pytest.raises(ValueError, match="backtick"):
        quote_identifier(ObjectKey())
