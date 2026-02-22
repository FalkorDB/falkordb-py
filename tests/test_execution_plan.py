import pytest

from falkordb.execution_plan import ExecutionPlan, Operation, ProfileStats


class TestProfileStats:
    """Tests for ProfileStats class."""

    def test_profile_stats_init(self):
        """Test ProfileStats initialization."""
        stats = ProfileStats(100, 5.5)
        assert stats.records_produced == 100
        assert stats.execution_time == 5.5

    def test_profile_stats_different_values(self):
        """Test ProfileStats with different values."""
        stats1 = ProfileStats(0, 0.0)
        assert stats1.records_produced == 0
        assert stats1.execution_time == 0.0

        stats2 = ProfileStats(1000000, 999.99)
        assert stats2.records_produced == 1000000
        assert stats2.execution_time == 999.99


class TestOperation:
    """Tests for Operation class."""

    def test_operation_init_basic(self):
        """Test Operation initialization with basic parameters."""
        op = Operation("Scan", "Label: Person")
        assert op.name == "Scan"
        assert op.args == "Label: Person"
        assert op.children == []
        assert op.profile_stats is None

    def test_operation_init_with_profile_stats(self):
        """Test Operation initialization with profile stats."""
        stats = ProfileStats(50, 2.5)
        op = Operation("Filter", "age > 30", stats)
        assert op.name == "Filter"
        assert op.args == "age > 30"
        assert op.profile_stats == stats

    def test_operation_execution_time_property(self):
        """Test Operation execution_time property."""
        stats = ProfileStats(100, 10.5)
        op = Operation("Project", None, stats)
        assert op.execution_time == 10.5

    def test_operation_records_produced_property(self):
        """Test Operation records_produced property."""
        stats = ProfileStats(200, 5.0)
        op = Operation("Sort", None, stats)
        assert op.records_produced == 200

    def test_operation_append_child(self):
        """Test appending a child operation."""
        parent = Operation("Parent")
        child = Operation("Child")
        result = parent.append_child(child)

        assert parent.child_count() == 1
        assert parent.children[0] == child
        assert result == parent  # Returns self

    def test_operation_append_multiple_children(self):
        """Test appending multiple children."""
        parent = Operation("Parent")
        child1 = Operation("Child1")
        child2 = Operation("Child2")
        child3 = Operation("Child3")

        parent.append_child(child1).append_child(child2).append_child(child3)

        assert parent.child_count() == 3
        assert parent.children[0] == child1
        assert parent.children[1] == child2
        assert parent.children[2] == child3

    def test_operation_append_child_invalid_type(self):
        """Test appending invalid child type raises exception."""
        parent = Operation("Parent")
        with pytest.raises(Exception, match="child must be Operation"):
            parent.append_child("not an operation")

    def test_operation_child_count(self):
        """Test child_count method."""
        op = Operation("Root")
        assert op.child_count() == 0

        op.append_child(Operation("Child1"))
        assert op.child_count() == 1

        op.append_child(Operation("Child2"))
        assert op.child_count() == 2

    def test_operation_equality(self):
        """Test Operation equality comparison."""
        op1 = Operation("Scan", "Label: Person")
        op2 = Operation("Scan", "Label: Person")
        op3 = Operation("Scan", "Label: Country")
        op4 = Operation("Filter", "Label: Person")

        assert op1 == op2
        assert op1 != op3
        assert op1 != op4
        assert op1 != "not an operation"

    def test_operation_str_with_args(self):
        """Test Operation string representation with args."""
        op = Operation("Scan", "Label: Person")
        assert str(op) == "Scan | Label: Person"

    def test_operation_str_without_args(self):
        """Test Operation string representation without args."""
        op = Operation("Results")
        assert str(op) == "Results"


class TestExecutionPlan:
    """Tests for ExecutionPlan class."""

    def test_execution_plan_init_invalid_type(self):
        """Test ExecutionPlan initialization with invalid type."""
        with pytest.raises(Exception, match="plan must be an array"):
            ExecutionPlan("not a list")

    def test_execution_plan_init_simple(self):
        """Test ExecutionPlan initialization with simple plan."""
        plan = ["Results", "    Project | Name: n.name", "        All Node Scan | (n)"]
        exec_plan = ExecutionPlan(plan)

        assert exec_plan.plan == plan
        assert exec_plan.structured_plan is not None
        assert exec_plan.structured_plan.name == "Results"

    def test_execution_plan_init_with_bytes(self):
        """Test ExecutionPlan initialization with bytes."""
        plan = [b"Results", b"    Project"]
        exec_plan = ExecutionPlan(plan)

        assert exec_plan.plan == ["Results", "    Project"]

    def test_execution_plan_collect_operations(self):
        """Test collect_operations method."""
        plan = [
            "Results",
            "    Project",
            "        Filter",
            "            Project",
            "                Scan",
        ]
        exec_plan = ExecutionPlan(plan)

        project_ops = exec_plan.collect_operations("Project")
        assert len(project_ops) == 2
        assert all(op.name == "Project" for op in project_ops)

        filter_ops = exec_plan.collect_operations("Filter")
        assert len(filter_ops) == 1
        assert filter_ops[0].name == "Filter"

        nonexistent = exec_plan.collect_operations("NonExistent")
        assert nonexistent == []

    def test_execution_plan_equality(self):
        """Test ExecutionPlan equality comparison."""
        plan1 = ["Results", "    Scan"]
        plan2 = ["Results", "    Scan"]
        plan3 = ["Results", "    Filter"]

        exec_plan1 = ExecutionPlan(plan1)
        exec_plan2 = ExecutionPlan(plan2)
        exec_plan3 = ExecutionPlan(plan3)

        assert exec_plan1 == exec_plan2
        assert exec_plan1 != exec_plan3
        assert exec_plan1 != "not an execution plan"

    def test_execution_plan_str(self):
        """Test ExecutionPlan string representation."""
        plan = ["Results", "    Project", "        Scan"]
        exec_plan = ExecutionPlan(plan)
        result_str = str(exec_plan)

        assert "Results" in result_str
        assert "Project" in result_str
        assert "Scan" in result_str

    def test_execution_plan_iter(self):
        """Test ExecutionPlan iteration."""
        plan = ["Results", "    Project", "        Scan"]
        exec_plan = ExecutionPlan(plan)

        operation_names = list(exec_plan)
        assert "Results" in operation_names
        assert "Project" in operation_names
        assert "Scan" in operation_names

    def test_execution_plan_with_profile_stats(self):
        """Test ExecutionPlan with profile statistics."""
        plan = [
            "Results | Records produced: 10, Execution time: 0.5 ms",
            "    Project | Records produced: 20, Execution time: 1.5 ms",
        ]
        exec_plan = ExecutionPlan(plan)

        assert exec_plan.structured_plan.profile_stats is not None
        assert exec_plan.structured_plan.execution_time == 0.5
        assert exec_plan.structured_plan.records_produced == 10

    def test_execution_plan_complex_tree(self):
        """Test ExecutionPlan with complex tree structure."""
        plan = [
            "Results",
            "    Project",
            "        Merge",
            "            Scan",
            "            Scan",
            "        Filter",
            "            Scan",
        ]
        exec_plan = ExecutionPlan(plan)

        assert exec_plan.structured_plan.name == "Results"
        assert exec_plan.structured_plan.child_count() == 1

        project_op = exec_plan.structured_plan.children[0]
        assert project_op.name == "Project"
        assert project_op.child_count() == 2

        merge_op = project_op.children[0]
        assert merge_op.name == "Merge"
        assert merge_op.child_count() == 2

        filter_op = project_op.children[1]
        assert filter_op.name == "Filter"
        assert filter_op.child_count() == 1

    def test_execution_plan_operation_with_args(self):
        """Test ExecutionPlan with operation arguments."""
        plan = [
            "Results",
            "    Project | Name: n.name, Age: n.age",
            "        Filter | n.age > 30",
            "            All Node Scan | (n:Person)",
        ]
        exec_plan = ExecutionPlan(plan)

        project_op = exec_plan.structured_plan.children[0]
        assert project_op.name == "Project"
        assert project_op.args == "Name: n.name, Age: n.age"

        filter_op = project_op.children[0]
        assert filter_op.name == "Filter"
        assert filter_op.args == "n.age > 30"
