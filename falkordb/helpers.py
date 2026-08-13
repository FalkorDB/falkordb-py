"""Helpers for serializing Python values into Cypher parameter literals."""

from __future__ import annotations

import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def quote_string(v: Any) -> Any:
    """
    FalkorDB strings must be quoted,
    quote_string wraps given v with quotes incase
    v is a string.

    Args:
        v: The value to quote. Non-textual values are returned unchanged.

    Returns:
        The quoted string, or ``v`` unchanged when it is not textual.

    Raises:
        ValueError: If the string contains a NUL byte, which FalkorDB's query
            header cannot represent and which crashes the server.
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v

    if "\x00" in v:
        raise ValueError("Cypher string parameters cannot contain a NUL byte")

    if len(v) == 0:
        return '""'

    v = v.replace("\\", "\\\\")
    v = v.replace('"', '\\"')

    return f'"{v}"'


def quote_identifier(name: Any, kind: str = "Cypher map key") -> str:
    """
    Normalize and validate a name used as a Cypher identifier.

    Identifiers are interpolated into the query header inside backticks, so
    they bypass the quoting applied to values and must be validated
    separately.

    Args:
        name: The identifier to normalize. ``bytes`` are decoded.
        kind: How to describe the identifier in error messages.

    Returns:
        The normalized identifier, without the surrounding backticks.

    Raises:
        ValueError: If the identifier is empty, contains a backtick, or
            contains a NUL byte. FalkorDB does not support escaped backticks
            in identifiers, and a NUL byte in the header crashes the server.
    """

    name_str = name.decode() if isinstance(name, bytes) else str(name)

    if name_str == "":
        raise ValueError(f"{kind} cannot be empty")

    if "`" in name_str:
        raise ValueError(
            f"{kind} cannot contain a backtick: {name_str!r} "
            "(FalkorDB does not support escaped backticks in identifiers)"
        )

    if "\x00" in name_str:
        raise ValueError(f"{kind} cannot contain a NUL byte: {name_str!r}")

    return name_str


def stringify_param_value(value: Any) -> str:
    """
    turn a parameter value into a string suitable for the params header of
    a Cypher command

    Supported types are ``str``, ``bytes``, ``bool``, ``int``, ``float``,
    ``Decimal``, ``None``, ``list``/``tuple``, ``dict``, and
    ``datetime``/``date``/``time``.

    ways in which output differs from that of `str()`:
    * strings are quoted
    * None --> "null"
    * booleans are lower-cased
    * datetimes, dates and times become quoted ISO-8601 strings
    * in dictionaries, keys are wrapped in backticks so that non-bare-
      identifier keys (e.g. ``@type``, hyphenated UUIDs) are accepted by
      the Cypher parser. Empty keys and keys containing a literal
      backtick raise ``ValueError`` because FalkorDB's CYPHER header
      parser does not support escaped backticks inside identifiers.

    :param value: the parameter value to be turned into a string
    :return: string

    Raises:
        TypeError: If ``value`` has a type that cannot be safely rendered as a
            Cypher literal. Falling back to ``str()`` would let arbitrary
            Cypher be injected through the parameters API.
        ValueError: If ``value`` is a non-finite float, contains a NUL byte, or
            is a mapping with an empty or backtick-containing key.
    """

    if isinstance(value, (str, bytes)):
        return quote_string(value)

    if value is None:
        return "null"

    # bool must be checked before int, bool is a subclass of int
    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, int):
        return repr(value)

    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(
                f"{value!r} is not a valid Cypher parameter: NaN and Infinity "
                "have no Cypher literal representation"
            )
        # render the decimal itself rather than going through float(), which
        # would drop digits beyond a double's precision and turn a large but
        # finite Decimal into inf. The server reports a value it cannot hold
        # as an overflow, which is a better answer than silent rounding.
        return str(value)

    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                f"{value!r} is not a valid Cypher parameter: NaN and Infinity "
                "have no Cypher literal representation"
            )
        return repr(value)

    if isinstance(value, (datetime, date, time)):
        return quote_string(value.isoformat())

    if isinstance(value, (list, tuple)):
        return f"[{','.join(map(stringify_param_value, value))}]"

    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            key_str = quote_identifier(k)
            parts.append(f"`{key_str}`:{stringify_param_value(v)}")
        return "{" + ",".join(parts) + "}"

    raise TypeError(
        f"unsupported Cypher parameter type: {type(value).__name__}. supported "
        "types are str, bytes, bool, int, float, Decimal, None, list, tuple, "
        "dict, datetime, date and time"
    )
