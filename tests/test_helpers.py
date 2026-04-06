"""Tests for helpers.quote_string and stringify_param_value."""

from falkordb.helpers import quote_string, stringify_param_value


class TestQuoteString:
    def test_simple_string(self):
        assert quote_string("hello") == '"hello"'

    def test_empty_string(self):
        assert quote_string("") == '""'

    def test_escapes_backslash(self):
        assert quote_string("a\\b") == '"a\\\\b"'

    def test_escapes_double_quote(self):
        assert quote_string('a"b') == '"a\\"b"'

    def test_escapes_newline(self):
        result = quote_string("line1\nline2")
        assert "\n" not in result
        assert result == '"line1\\nline2"'

    def test_escapes_carriage_return(self):
        result = quote_string("line1\rline2")
        assert "\r" not in result
        assert result == '"line1\\rline2"'

    def test_escapes_tab(self):
        result = quote_string("col1\tcol2")
        assert "\t" not in result
        assert result == '"col1\\tcol2"'

    def test_non_string_passthrough(self):
        assert quote_string(42) == 42
        assert quote_string(3.14) == 3.14

    def test_bytes_decoded(self):
        assert quote_string(b"hello") == '"hello"'

    def test_combined_escapes(self):
        result = quote_string('a"b\nc\\d')
        assert result == '"a\\"b\\nc\\\\d"'


class TestStringifyParamValue:
    def test_string(self):
        assert stringify_param_value("hello") == '"hello"'

    def test_none(self):
        assert stringify_param_value(None) == "null"

    def test_list(self):
        assert stringify_param_value([1, 2]) == "[1,2]"

    def test_dict(self):
        result = stringify_param_value({"key": "val"})
        assert result == '{key:"val"}'

    def test_nested_dict_with_newlines(self):
        """Newlines in dict values must be escaped for CYPHER params header."""
        result = stringify_param_value({"summary": "line1\nline2"})
        assert "\n" not in result
        assert "\\n" in result
