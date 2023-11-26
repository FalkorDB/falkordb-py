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
        return f'[{",".join(map(stringify_param_value, value))}]'

    if isinstance(value, dict):
        return f'{{{",".join(f"{k}:{stringify_param_value(v)}" for k, v in value.items())}}}'  # noqa

    return str(value)
