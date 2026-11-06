"""
Test for bundled domain design task display in Cato.

This test verifies that Cato correctly displays bundled design tasks created
by Marcus's advanced PRD parser (GH-108, GH-127).

Bundled design tasks have special characteristics:
- No parent_task_id (they're top-level tasks)
- No children tasks (they're standalone design tasks)
- domain_name field indicates the domain they cover
- feature_ids list shows which features they encompass
- type = "design"

Bug: These tasks are not showing up in Cato's "subtasks" view mode.
"""

import json
from pathlib import Path

import pytest


@pytest.fixture
def sample_bundled_design_tasks():
    """
    Create sample bundled design tasks as they appear from Marcus.

    Simulates output from AdvancedPRDParser._create_bundled_design_tasks()
    """
    return [
        {
            "id": "design_authentication",
            "name": "Design Authentication",
            "description": "Design the architecture for the Authentication domain...",
            "type": "design",
            "domain_name": "Authentication",
            "feature_ids": ["feature_user_login", "feature_user_registration"],
            "priority": "high",
            "estimated_hours": 12.0,
            "labels": ["design", "architecture", "authentication"],
            "status": "completed",
            "parent_task_id": None,  # No parent - it's top-level
            "is_subtask": False,
            "dependency_ids": [],
            "dependent_task_ids": ["feature_user_login", "feature_user_registration"],
        },
        {
            "id": "design_shopping",
            "name": "Design Shopping",
            "description": "Design the architecture for the Shopping domain...",
            "type": "design",
            "domain_name": "Shopping",
            "feature_ids": [
                "feature_product_catalog",
                "feature_shopping_cart",
                "feature_checkout",
            ],
            "priority": "high",
            "estimated_hours": 18.0,
            "labels": ["design", "architecture", "shopping"],
            "status": "in-progress",
            "parent_task_id": None,
            "is_subtask": False,
            "dependency_ids": [],
            "dependent_task_ids": [
                "feature_product_catalog",
                "feature_shopping_cart",
                "feature_checkout",
            ],
        },
    ]


@pytest.fixture
def sample_regular_tasks():
    """Create sample regular tasks (with and without children)."""
    return [
        # Parent task with children
        {
            "id": "epic_backend",
            "name": "Backend Development",
            "parent_task_id": None,
            "is_subtask": False,
            "status": "in-progress",
        },
        # Subtasks
        {
            "id": "task_api_001",
            "name": "Create API endpoints",
            "parent_task_id": "epic_backend",
            "is_subtask": True,
            "status": "completed",
        },
        {
            "id": "task_db_001",
            "name": "Set up database",
            "parent_task_id": "epic_backend",
            "is_subtask": True,
            "status": "completed",
        },
        # Standalone parent with NO children
        {
            "id": "task_standalone",
            "name": "Standalone Task",
            "parent_task_id": None,
            "is_subtask": False,
            "status": "pending",
        },
    ]


def test_filter_tasks_includes_bundled_design_tasks(
    sample_bundled_design_tasks, sample_regular_tasks
):
    """
    Test that _filter_tasks_by_view includes bundled design tasks in 'subtasks' mode.

    Bundled design tasks should be visible because:
    - They are NOT subtasks (is_subtask = False)
    - They have NO children (has_children = False)
    - Logic: "not has_children" should include them

    Expected: Both design tasks appear in filtered results
    """
    from cato_src.core.aggregator import Aggregator

    # Explicit root: these tests exercise pure filter logic and never read
    # the filesystem, so any existing dir works — and passing it keeps the
    # test independent of whether Marcus is installed.
    aggregator = Aggregator(marcus_root=Path(__file__).parent)
    all_tasks = sample_bundled_design_tasks + sample_regular_tasks

    # Test subtasks view mode
    filtered = aggregator._filter_tasks_by_view(all_tasks, view_mode="subtasks")

    # Should include:
    # - design_authentication (no children)
    # - design_shopping (no children)
    # - task_api_001 (is subtask)
    # - task_db_001 (is subtask)
    # - task_standalone (no children)
    # Should NOT include:
    # - epic_backend (has children)

    filtered_ids = {t["id"] for t in filtered}

    assert (
        "design_authentication" in filtered_ids
    ), "Bundled design task 'design_authentication' should be visible"
    assert (
        "design_shopping" in filtered_ids
    ), "Bundled design task 'design_shopping' should be visible"
    assert "task_api_001" in filtered_ids, "Subtask should be visible"
    assert "task_db_001" in filtered_ids, "Subtask should be visible"
    assert (
        "task_standalone" in filtered_ids
    ), "Standalone parent without children should be visible"
    assert (
        "epic_backend" not in filtered_ids
    ), "Parent with children should NOT be visible in subtasks mode"


def test_filter_tasks_includes_parents_without_children(sample_regular_tasks):
    """
    Test that parents without children are shown in subtasks view.

    Bug report: "tasks in which the parent doesn't have subtasks" are not displayed.

    Expected: task_standalone should appear since it has no children.
    """
    from cato_src.core.aggregator import Aggregator

    # Explicit root: these tests exercise pure filter logic and never read
    # the filesystem, so any existing dir works — and passing it keeps the
    # test independent of whether Marcus is installed.
    aggregator = Aggregator(marcus_root=Path(__file__).parent)

    filtered = aggregator._filter_tasks_by_view(
        sample_regular_tasks, view_mode="subtasks"
    )
    filtered_ids = {t["id"] for t in filtered}

    assert (
        "task_standalone" in filtered_ids
    ), "Parent task without children should be visible"
    assert "task_api_001" in filtered_ids, "Subtask should be visible"
    assert (
        "epic_backend" not in filtered_ids
    ), "Parent with children should NOT be visible"


def test_filter_tasks_includes_design_tasks_in_dependency_chain(
    sample_bundled_design_tasks,
):
    """
    Test that design tasks appear when referenced in dependency chains.

    Bundled design tasks have dependent_task_ids pointing to their features.
    Even if logic fails for "not has_children", they should still appear
    if they're in a dependency chain.
    """
    from cato_src.core.aggregator import Aggregator

    # Explicit root: these tests exercise pure filter logic and never read
    # the filesystem, so any existing dir works — and passing it keeps the
    # test independent of whether Marcus is installed.
    aggregator = Aggregator(marcus_root=Path(__file__).parent)

    # Add a task that depends on the design task
    tasks = sample_bundled_design_tasks + [
        {
            "id": "feature_user_login",
            "name": "User Login Feature",
            "dependency_ids": ["design_authentication"],  # Depends on design
            "dependent_task_ids": [],
            "is_subtask": False,
            "parent_task_id": None,
        }
    ]

    filtered = aggregator._filter_tasks_by_view(tasks, view_mode="subtasks")
    filtered_ids = {t["id"] for t in filtered}

    assert (
        "design_authentication" in filtered_ids
    ), "Design task should be visible (in dependency chain)"
    assert (
        "feature_user_login" in filtered_ids
    ), "Task depending on design should be visible"


def test_all_view_mode_shows_everything():
    """Test that 'all' view mode shows all tasks."""
    from cato_src.core.aggregator import Aggregator

    # Explicit root: these tests exercise pure filter logic and never read
    # the filesystem, so any existing dir works — and passing it keeps the
    # test independent of whether Marcus is installed.
    aggregator = Aggregator(marcus_root=Path(__file__).parent)

    tasks = [
        {"id": "task1", "is_subtask": True, "parent_task_id": "parent1"},
        {"id": "parent1", "is_subtask": False, "parent_task_id": None},
        {
            "id": "design_auth",
            "is_subtask": False,
            "parent_task_id": None,
            "type": "design",
        },
    ]

    filtered = aggregator._filter_tasks_by_view(tasks, view_mode="all")

    assert len(filtered) == 3, "All tasks should be visible in 'all' mode"


def test_parents_view_mode_excludes_subtasks():
    """Test that 'parents' view mode only shows parent tasks."""
    from cato_src.core.aggregator import Aggregator

    # Explicit root: these tests exercise pure filter logic and never read
    # the filesystem, so any existing dir works — and passing it keeps the
    # test independent of whether Marcus is installed.
    aggregator = Aggregator(marcus_root=Path(__file__).parent)

    tasks = [
        {"id": "task1", "is_subtask": True, "parent_task_id": "parent1"},
        {"id": "parent1", "is_subtask": False, "parent_task_id": None},
        {
            "id": "design_auth",
            "is_subtask": False,
            "parent_task_id": None,
            "type": "design",
        },
    ]

    filtered = aggregator._filter_tasks_by_view(tasks, view_mode="parents")
    filtered_ids = {t["id"] for t in filtered}

    assert "parent1" in filtered_ids
    assert "design_auth" in filtered_ids
    assert "task1" not in filtered_ids, "Subtasks should not appear in parents mode"
