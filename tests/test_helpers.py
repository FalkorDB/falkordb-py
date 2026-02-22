from falkordb.helpers import quote_string, stringify_param_value


class TestQuoteString:
    """Tests for the quote_string function."""

    def test_quote_string_with_regular_string(self):
        """Test quote_string with a regular string."""
        result = quote_string("hello")
        assert result == '"hello"'

    def test_quote_string_with_empty_string(self):
        """Test quote_string with an empty string."""
        result = quote_string("")
        assert result == '""'

    def test_quote_string_with_bytes(self):
        """Test quote_string with bytes."""
        result = quote_string(b"hello")
        assert result == '"hello"'

    def test_quote_string_with_non_string(self):
        """Test quote_string with non-string (should return as-is)."""
        assert quote_string(123) == 123
        assert quote_string(45.67) == 45.67
        assert quote_string(None) is None
        assert quote_string([1, 2, 3]) == [1, 2, 3]

    def test_quote_string_with_quotes(self):
        """Test quote_string with quotes inside the string."""
        result = quote_string('say "hello"')
        assert result == '"say \\"hello\\""'

    def test_quote_string_with_backslashes(self):
        """Test quote_string with backslashes."""
        result = quote_string("path\\to\\file")
        assert result == '"path\\\\to\\\\file"'

    def test_quote_string_with_mixed_special_chars(self):
        """Test quote_string with mixed special characters."""
        result = quote_string('path\\to\\"file"')
        assert result == '"path\\\\to\\\\\\"file\\""'


class TestStringifyParamValue:
    """Tests for the stringify_param_value function."""

    def test_stringify_string(self):
        """Test stringify_param_value with a string."""
        result = stringify_param_value("hello")
        assert result == '"hello"'

    def test_stringify_none(self):
        """Test stringify_param_value with None."""
        result = stringify_param_value(None)
        assert result == "null"

    def test_stringify_integer(self):
        """Test stringify_param_value with an integer."""
        result = stringify_param_value(42)
        assert result == "42"

    def test_stringify_float(self):
        """Test stringify_param_value with a float."""
        result = stringify_param_value(3.14)
        assert result == "3.14"

    def test_stringify_boolean(self):
        """Test stringify_param_value with booleans."""
        assert stringify_param_value(True) == "True"
        assert stringify_param_value(False) == "False"

    def test_stringify_list(self):
        """Test stringify_param_value with a list."""
        result = stringify_param_value([1, 2, 3])
        assert result == "[1,2,3]"

    def test_stringify_tuple(self):
        """Test stringify_param_value with a tuple."""
        result = stringify_param_value((1, 2, 3))
        assert result == "[1,2,3]"

    def test_stringify_mixed_list(self):
        """Test stringify_param_value with a mixed list."""
        result = stringify_param_value([1, "hello", None, 3.14])
        assert result == '[1,"hello",null,3.14]'

    def test_stringify_dict(self):
        """Test stringify_param_value with a dictionary."""
        result = stringify_param_value({"name": "John", "age": 30})
        # Dictionary iteration order is preserved in Python 3.7+
        assert result == '{name:"John",age:30}' or result == '{age:30,name:"John"}'

    def test_stringify_nested_dict(self):
        """Test stringify_param_value with nested dictionary."""
        result = stringify_param_value({"user": {"name": "John", "age": 30}})
        assert "user:{" in result
        assert "John" in result
        assert "age:30" in result

    def test_stringify_nested_list(self):
        """Test stringify_param_value with nested list."""
        result = stringify_param_value([1, [2, 3], 4])
        assert result == "[1,[2,3],4]"

    def test_stringify_empty_list(self):
        """Test stringify_param_value with an empty list."""
        result = stringify_param_value([])
        assert result == "[]"

    def test_stringify_empty_dict(self):
        """Test stringify_param_value with an empty dictionary."""
        result = stringify_param_value({})
        assert result == "{}"

    def test_stringify_complex_nested_structure(self):
        """Test stringify_param_value with complex nested structure."""
        data = {
            "users": [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}],
            "count": 2,
        }
        result = stringify_param_value(data)
        assert "users:[" in result
        assert "count:2" in result
        assert "Alice" in result
        assert "Bob" in result
