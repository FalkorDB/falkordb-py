"""Assertions for execution plans.

Two levels are available.

``assert_plan_shape`` pins the exact operation tree — every operation name and
how deep it sits. Use it wherever the C and Rust engines compile a query to the
same plan, which is the common case.

``assert_parsed_plan`` only checks that the client turned the reply into a tree
faithfully: every line became one operation, nested as deep as it was indented.
Use it for the few queries the two engines genuinely compile differently, and
name the difference in the test.

The two engines put a different driver operation at the root — C wraps a read
plan in ``Results``, Rust wraps a write plan in ``Commit`` — while the plan
below it is identical. ``assert_plan_shape`` skips that root, so the expected
tree is the part of the plan the query itself describes.
"""

from typing import Iterator, List, Optional, Tuple

from falkordb.execution_plan import ExecutionPlan, Operation

INDENT = "    "

# top level driver operations, emitted by one engine and not the other
ENGINE_ROOT_OPS = ("Results", "Commit")


def iter_operations(op: Operation, depth: int = 0) -> Iterator[Tuple[int, Operation]]:
    """Yields (depth, operation) for the whole tree, depth first."""

    yield depth, op
    for child in op.children:
        yield from iter_operations(child, depth + 1)


def _render(op: Operation) -> str:
    """Renders an operation the way the reply spells it."""

    return f"{op.name} | {op.args}" if op.args else op.name


def _parse_expected(expected: str) -> List[Tuple[int, str]]:
    """Turns an indented expected plan into (depth, text) pairs."""

    lines = [line for line in expected.split("\n") if line.strip()]
    assert lines, "expected plan is empty"

    base = min(len(line) - len(line.lstrip()) for line in lines)
    parsed = []
    for line in lines:
        indent = len(line) - len(line.lstrip()) - base
        assert indent % len(INDENT) == 0, f"expected plan misindented: {line!r}"
        parsed.append((indent // len(INDENT), line.strip()))
    return parsed


def assert_parsed_plan(
    plan: ExecutionPlan,
    min_operations: int = 1,
    expect_args: bool = False,
) -> None:
    """Asserts the client parsed an execution plan reply correctly."""

    root = plan.structured_plan
    assert isinstance(root, Operation)

    parsed = list(iter_operations(root))
    lines = [line for line in plan.plan if line.strip()]

    # every line of the raw reply became exactly one operation
    assert len(parsed) == len(lines)
    assert len(parsed) >= min_operations

    for (depth, op), line in zip(parsed, lines):
        # the operation sits as deep in the tree as its line was indented,
        # which is what makes this a test of the parser rather than the engine
        assert depth == (len(line) - len(line.lstrip())) // len(INDENT)

        # indentation and the argument separator were stripped off the name
        assert isinstance(op.name, str)
        assert op.name == op.name.strip()
        assert op.name != ""
        assert "|" not in op.name
        assert op.name == line.split("|")[0].strip()

        assert op.args is None or isinstance(op.args, str)
        assert isinstance(op.children, list)

    if expect_args:
        assert any(op.args for _, op in parsed)


def assert_plan_shape(plan: ExecutionPlan, expected: str) -> None:
    """Asserts the plan is exactly ``expected``, engine root operation aside.

    ``expected`` is the operation tree, indented four spaces per level::

        Project
            Cartesian Product
                All Node Scan | (a)
                All Node Scan | (b)

    A line may name the operation on its own, or spell out its arguments after
    a ``|`` to pin those too. Give arguments only where both engines render
    them identically — the traverse direction, and the order of sibling scans,
    are two that are not guaranteed to match.
    """

    # the reply was turned into a tree faithfully in the first place
    assert_parsed_plan(plan)

    expected_ops = _parse_expected(expected)
    actual = list(iter_operations(plan.structured_plan))

    # drop the engine's root operation, unless the plan is expected to have it
    root_name = actual[0][1].name
    expected_root = expected_ops[0][1].split("|")[0].strip()
    if root_name in ENGINE_ROOT_OPS and expected_root != root_name:
        actual = [(depth - 1, op) for depth, op in actual[1:]]

    rendered = [
        (depth, _render(op) if "|" in text else op.name)
        for (depth, op), (_, text) in zip(actual, expected_ops)
    ]

    assert len(actual) == len(expected_ops) and rendered == expected_ops, (
        "unexpected execution plan\n\nexpected:\n%s\n\ngot:\n%s"
        % (
            "\n".join(INDENT * d + t for d, t in expected_ops),
            "\n".join(INDENT * d + _render(op) for d, op in actual),
        )
    )


def assert_parsed_profile(
    plan: ExecutionPlan,
    min_operations: int = 1,
    expect_args: bool = False,
    records_produced: Optional[int] = None,
) -> None:
    """Asserts the client parsed a profile reply, statistics included."""

    assert_parsed_plan(plan, min_operations, expect_args)
    _assert_profile_stats(plan, records_produced)


def assert_profile_shape(
    plan: ExecutionPlan,
    expected: str,
    records_produced: Optional[int] = None,
) -> None:
    """Asserts an exact profile plan, statistics included."""

    assert_plan_shape(plan, expected)
    _assert_profile_stats(plan, records_produced)


def _assert_profile_stats(plan: ExecutionPlan, records_produced: Optional[int]) -> None:
    parsed = list(iter_operations(plan.structured_plan))
    for _, op in parsed:
        assert op.profile_stats is not None
        assert isinstance(op.records_produced, int)
        assert op.records_produced >= 0
        assert isinstance(op.execution_time, float)
        assert op.execution_time >= 0

    if records_produced is not None:
        # how many rows the query yields is a property of the query, not of the
        # engine, so the client must report it whichever engine answered
        assert max(op.records_produced for _, op in parsed) == records_produced
