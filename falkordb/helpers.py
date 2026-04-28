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
    * in dictionaries, keys are wrapped in backticks so that non-bare-
      identifier keys (e.g. ``@type``, hyphenated UUIDs) are accepted by
      the Cypher parser. Empty keys and keys containing a literal
      backtick raise ``ValueError`` because FalkorDB's CYPHER header
      parser does not support escaped backticks inside identifiers.

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
        parts = []
        for k, v in value.items():
            key_str = k.decode() if isinstance(k, bytes) else str(k)
            if key_str == "":
                raise ValueError("Cypher map key cannot be empty")
            if "`" in key_str:
                raise ValueError(
                    "Cypher map key cannot contain a backtick: "
                    f"{key_str!r} (FalkorDB does not support escaped "
                    "backticks in identifiers)"
                )
            parts.append(f"`{key_str}`:{stringify_param_value(v)}")
        return "{" + ",".join(parts) + "}"

    return str(value)
