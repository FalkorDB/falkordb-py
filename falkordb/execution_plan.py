import re


class ProfileStats:
    """
    ProfileStats Class for representing runtime execution statistics of an operation.

    Attributes:
        records_produced (int): The number of records produced.
        execution_time (float): The execution time in milliseconds.
    """

    def __init__(self, records_produced: int, execution_time: float):
        """
        Initializes a new ProfileStats instance with the given records_produced and execution_time.

        Args:
            records_produced (int): The number of records produced.
            execution_time (float): The execution time in milliseconds.
        """
        self.execution_time   = execution_time
        self.records_produced = records_produced


class Operation:
    """
    Operation Class for representing a single operation within an execution plan.

    Attributes:
        name (str): The name of the operation.
        args (str): Operation arguments.
        children (list): List of child operations.
        profile_stats (ProfileStats): Profile statistics for the operation.
    """

    def __init__(self, name: str, args=None, profile_stats: bool = None):
        """
        Creates a new Operation instance.

        Args:
            name (str): The name of the operation.
            args (str, optional): Operation arguments.
            profile_stats (ProfileStats, optional): Profile statistics for the operation.
        """
        self.name          = name
        self.args          = args
        self.children      = []
        self.profile_stats = profile_stats

    @property
    def execution_time(self) -> int:
        """
        returns operation's execution time in ms
        """
        return self.profile_stats.execution_time

    @property
    def records_produced(self) -> int:
        """
        returns number of records produced by operation.
        """
        return self.profile_stats.records_produced

    def append_child(self, child):
        """
        Appends a child operation to the current operation.

        Args:
            child (Operation): The child operation to append.

        Returns:
            Operation: The updated operation instance.
        """
        if not isinstance(child, Operation):
            raise Exception("child must be Operation")

        self.children.append(child)
        return self

    def child_count(self) -> int:
        """
        Returns the number of child operations.

        Returns:
            int: Number of child operations.
        """
        return len(self.children)

    def __eq__(self, o: object) -> bool:
        """
        Compares two Operation instances for equality based on their name and arguments.

        Args:
            o (object): Another Operation instance for comparison.

        Returns:
            bool: True if the operations are equal, False otherwise.
        """
        if not isinstance(o, Operation):
            return False

        return self.name == o.name and self.args == o.args

    def __str__(self) -> str:
        """
        Returns a string representation of the operation.

        Returns:
            str: String representation of the operation.
        """
        args_str = "" if self.args is None else " | " + self.args
        return f"{self.name}{args_str}"


class ExecutionPlan:
    """
    ExecutionPlan Class for representing a collection of operations.

    Attributes:
        plan (list): List of strings representing the collection of operations.
        structured_plan (Operation): Root of the structured operation tree.
    """

    def __init__(self, plan):
        """
        Creates a new ExecutionPlan instance.

        Args:
            plan (list): List of strings representing the collection of operations.
        """
        if not isinstance(plan, list):
            raise Exception("plan must be an array")

        if isinstance(plan[0], bytes):
            plan = [b.decode() for b in plan]

        self.plan = plan
        self.operations = {}
        self.structured_plan = self._operation_tree()
        for key in self.operations:
            self.operations[key].reverse()

    def collect_operations(self, op_name):
        """
        Collects all operations with specified name from plan

        Args:
            op_name (string): Name of operation to collect

        Returns:
            List[Operation]: All operations with the specified name
        """
        if op_name in self.operations:
            return self.operations[op_name]
        return []

        ops = []

        for op in self.operations:
            if op.name == op_name:
                ops.append(op)

        return ops

    def __compare_operations(self, root_a, root_b) -> bool:
        """
        Compares execution plan operation trees.

        Returns:
            bool: True if operation trees are equal, False otherwise.
        """
        # compare current root
        if root_a != root_b:
            return False

        # make sure root have the same number of children
        if root_a.child_count() != root_b.child_count():
            return False

        # recursively compare children
        for i in range(root_a.child_count()):
            if not self.__compare_operations(root_a.children[i], root_b.children[i]):
                return False

        return True

    def __str__(self) -> str:
        """
        Returns a string representation of the execution plan.

        Returns:
            str: String representation of the execution plan.
        """
        def aggregate_str(str_children):
            return "\n".join(
                [
                    "    " + line
                    for str_child in str_children
                    for line in str_child.splitlines()
                ]
            )

        def combine_str(x, y):
            return f"{x}\n{y}"

        return self._operation_traverse(
            self.structured_plan, str, aggregate_str, combine_str
        )

    def __eq__(self, o: object) -> bool:
        """
        Compares two execution plans.

        Returns:
            bool: True if the two plans are equal, False otherwise.
        """
        # make sure 'o' is an execution-plan
        if not isinstance(o, ExecutionPlan):
            return False

        # get root for both plans
        root_a = self.structured_plan
        root_b = o.structured_plan

        # compare execution trees
        return self.__compare_operations(root_a, root_b)

    def __iter__(self):
        return iter(self.operations)

    def _operation_traverse(self, op, op_f, aggregate_f, combine_f):
        """
        Traverses the operation tree recursively applying functions.

        Args:
            op: Operation to traverse.
            op_f: Function applied for each operation.
            aggregate_f: Aggregation function applied for all children of a single operation.
            combine_f: Combine function applied for the operation result and the children result.
        """
        # apply op_f for each operation
        op_res = op_f(op)
        if len(op.children) == 0:
            return op_res  # no children return

        # apply _operation_traverse recursively
        children = [
            self._operation_traverse(child, op_f, aggregate_f, combine_f)
            for child in op.children
        ]
        # combine the operation result with the children aggregated result
        return combine_f(op_res, aggregate_f(children))

    def _operation_tree(self):
        """
        Builds the operation tree from the string representation.

        Returns:
            Operation: Root of the structured operation tree.
        """
        # initial state
        i       = 0
        level   = 0
        stack   = []
        current = None

        def create_operation(args):
            profile_stats = None
            name = args[0].strip()
            args.pop(0)
            if len(args) > 0 and "Records produced" in args[-1]:
                records_produced = int(
                    re.search("Records produced: (\\d+)", args[-1]).group(1)
                )
                execution_time = float(
                    re.search("Execution time: (\\d+.\\d+) ms", args[-1]).group(1)
                )
                profile_stats = ProfileStats(records_produced, execution_time)
                args.pop(-1)
            return Operation(
                name, None if len(args) == 0 else args[0].strip(), profile_stats
            )

        # iterate plan operations
        while i < len(self.plan):
            current_op = self.plan[i]
            op_level = current_op.count("    ")
            if op_level == level:
                # if the operation level equal to the current level
                # set the current operation and move next
                child = create_operation(current_op.split("|"))
                if child.name not in self.operations:
                    self.operations[child.name] = []
                self.operations[child.name].append(child)

                if current:
                    current = stack.pop()
                    current.append_child(child)
                current = child
                i += 1
                stack.append(child)
            elif op_level == level + 1:
                # if the operation is child of the current operation
                # add it as child and set as current operation
                child = create_operation(current_op.split("|"))
                if child.name not in self.operations:
                    self.operations[child.name] = []
                self.operations[child.name].append(child)

                current.append_child(child)
                stack.append(current)
                current = child
                level += 1
                i += 1
            elif op_level < level:
                # if the operation is not child of current operation
                # go back to it's parent operation
                levels_back = level - op_level + 1
                for _ in range(levels_back):
                    current = stack.pop()
                level -= levels_back
            else:
                raise Exception("corrupted plan")
        return stack[0]
