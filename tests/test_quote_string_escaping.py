
import pytest

from falkordb.helpers import quote_string, stringify_param_value


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("hello\nworld", '"hello\\nworld"'),
        ("hello\rworld", '"hello\\rworld"'),
        ("hello\tworld", '"hello\\tworld"'),
        ('say "hello"', '"say \\"hello\\""'),
        ("hello\\world", '"hello\\\\world"'),
        ("", '""'),
        (b"hello\nworld", '"hello\\nworld"'),
    ],
)
def test_quote_string_escapes_special_characters(value, expected):
    assert quote_string(value) == expected


def test_nested_parameter_values_escape_control_characters():
    value = {
        "items": [
            {
                "text": "first\nsecond",
                "description": "one\ttwo\rthree",
            }
        ]
    }

    serialized = stringify_param_value(value)

    assert "\\n" in serialized
    assert "\\t" in serialized
    assert "\\r" in serialized

    assert "\n" not in serialized
    assert "\t" not in serialized
    assert "\r" not in serialized


def test_quote_string_preserves_non_string_values():
    assert quote_string(123) == 123
    assert quote_string(None) is None
