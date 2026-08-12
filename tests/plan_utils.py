"""Shared helpers for tests.

FalkorDB used to emit a ``Results`` root operation at the top of every
``GRAPH.EXPLAIN`` / ``GRAPH.PROFILE`` plan. Newer servers omit it. The helpers
here normalize plans so the assertions hold against either server version.
"""

from falkordb.execution_plan import ExecutionPlan, Operation

RESULTS_OP = "Results"


def plan_root(plan: ExecutionPlan) -> Operation:
    """Return the first meaningful operation of a plan.

    Args:
        plan: The execution plan to inspect.

    Returns:
        Operation: ``plan.structured_plan``, or its only child when the server
        wrapped the plan in a legacy ``Results`` operation.
    """
    root = plan.structured_plan
    if root.name == RESULTS_OP and len(root.children) == 1:
        return root.children[0]
    return root


def strip_results_op(tree: Operation) -> Operation:
    """Drop a leading ``Results`` operation from an expected operation tree.

    Args:
        tree: The expected operation tree, rooted at ``Results``.

    Returns:
        Operation: The subtree below ``Results``.
    """
    if tree.name == RESULTS_OP and len(tree.children) == 1:
        return tree.children[0]
    return tree


def canonical_plan_str(text: str) -> str:
    """Normalize a plan's string form by removing a legacy ``Results`` root.

    Args:
        text: The plan rendered via ``str(plan)``.

    Returns:
        str: The plan text without a leading ``Results`` line, dedented by one
        level when that line actually nested the rest of the plan.
    """
    lines = text.splitlines()
    if lines and lines[0].strip() == RESULTS_OP:
        lines = lines[1:]
        if lines and all(not line.strip() or line.startswith("    ") for line in lines):
            lines = [line[4:] if line.startswith("    ") else line for line in lines]
    return "\n".join(lines)


def assert_same_plan(actual: str, expected: str) -> None:
    """Assert two plan strings match, ignoring whitespace and a ``Results`` root.

    Args:
        actual: The plan produced by the server.
        expected: The plan the test expects.
    """

    def squash(text: str) -> str:
        return canonical_plan_str(text).replace(" ", "").replace("\n", "")

    assert squash(actual) == squash(expected)


# operations FalkorDB has renamed across releases, mapped to a canonical name
_OP_ALIASES = {"Join": "Union"}


def op_shape(op: Operation) -> tuple:
    """Reduce an operation tree to a comparable (name, children) shape.

    Operation arguments are deliberately ignored: they are opaque server text
    whose formatting (e.g. traversal arrow direction) has changed between
    releases, while the tree shape is the part the client parser produces.

    Args:
        op: The root operation.

    Returns:
        tuple: Nested ``(name, (children...))`` tuples.
    """
    name = _OP_ALIASES.get(op.name, op.name)
    return (name, tuple(op_shape(child) for child in op.children))


def plan_shape(plan: ExecutionPlan) -> tuple:
    """Return the shape of a plan, ignoring a legacy ``Results`` root.

    Args:
        plan: The execution plan.

    Returns:
        tuple: Nested ``(name, (children...))`` tuples.
    """
    return op_shape(plan_root(plan))


def parse_plan_shape(text: str) -> tuple:
    """Build the expected shape from an indented plan listing.

    Args:
        text: A plan listing, one operation per line, indented by 4 spaces per
            level. Arguments after a ``|`` are ignored.

    Returns:
        tuple: Nested ``(name, (children...))`` tuples.
    """
    root: tuple = ("", [])
    stack: list = [(-1, root)]

    for line in canonical_plan_str(text).splitlines():
        if not line.strip():
            continue
        level = (len(line) - len(line.lstrip(" "))) // 4
        name = line.strip().split("|")[0].strip()
        name = _OP_ALIASES.get(name, name)
        node: tuple = (name, [])
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack[-1][1][1].append(node)
        stack.append((level, node))

    def freeze(node):
        return (node[0], tuple(freeze(child) for child in node[1]))

    children = root[1]
    assert len(children) == 1, "expected exactly one root operation"
    return freeze(children[0])


def assert_plan_shape(plan: ExecutionPlan, expected: str) -> None:
    """Assert a plan's tree shape matches an indented listing.

    Args:
        plan: The plan returned by the server.
        expected: The expected plan listing.
    """
    assert plan_shape(plan) == parse_plan_shape(expected)
