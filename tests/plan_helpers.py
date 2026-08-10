"""Engine-agnostic assertions for execution plans.

Which operations a query compiles into is the engine's business and it changes
between engine versions. The client's job is to issue GRAPH.EXPLAIN /
GRAPH.PROFILE and turn the reply into an operation tree, so that is what these
helpers check: every line of the raw reply became exactly one operation, nested
exactly as deep as that line was indented.
"""

from typing import Iterator, Optional, Tuple

from falkordb.execution_plan import ExecutionPlan, Operation

INDENT = "    "


def iter_operations(op: Operation, depth: int = 0) -> Iterator[Tuple[int, Operation]]:
    """Yields (depth, operation) for the whole tree, depth first."""

    yield depth, op
    for child in op.children:
        yield from iter_operations(child, depth + 1)


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


def assert_parsed_profile(
    plan: ExecutionPlan,
    min_operations: int = 1,
    expect_args: bool = False,
    records_produced: Optional[int] = None,
) -> None:
    """Asserts the client parsed a profile reply, statistics included."""

    assert_parsed_plan(plan, min_operations, expect_args)

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
