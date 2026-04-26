def quote_identifier(key) -> str:
    """
    Wrap a Cypher identifier (parameter name or map key) in backticks.

    FalkorDB's CYPHER parameter-header parser accepts backtick-quoted
    identifiers but does not support escaping an embedded backtick by
    doubling it. Rather than emit a query the server cannot parse, keys
    containing a literal backtick are rejected here with a clear error.

    Empty keys are also rejected — an empty identifier is not valid Cypher.
    """
    s = key.decode() if isinstance(key, bytes) else str(key)
    if s == "":
        raise ValueError(
            "Cypher identifier (parameter name or map key) cannot be empty"
        )
    if "`" in s:
        raise ValueError(
            "Cypher identifier cannot contain a backtick: "
            f"{s!r} (FalkorDB does not support escaped backticks in identifiers)"
        )
    return f"`{s}`"


def quote_string(v):
    """
    FalkorDB strings must be quoted,
    quote_string wraps given v with quotes incase
    v is a string.
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    v = v.replace("\\", "\\\\")
    v = v.replace('"', '\\"')

    return f'"{v}"'


def stringify_param_value(value):
    """
    turn a parameter value into a string suitable for the params header of
    a Cypher command
    you may pass any value that would be accepted by `json.dumps()`

    ways in which output differs from that of `str()`:
    * strings are quoted
    * None --> "null"
    * in dictionaries, keys are _not_ quoted

    :param value: the parameter value to be turned into a string
    :return: string
    """

    if isinstance(value, str):
        return quote_string(value)

    if value is None:
        return "null"

    if isinstance(value, (list, tuple)):
        return f"[{','.join(map(stringify_param_value, value))}]"

    if isinstance(value, dict):
        return f"{{{','.join(f'{quote_identifier(k)}:{stringify_param_value(v)}' for k, v in value.items())}}}"  # noqa

    return str(value)
