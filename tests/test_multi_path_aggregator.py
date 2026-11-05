"""Unit tests for multi-path Aggregator support.

Verifies that Aggregator can load projects from multiple marcus_roots
simultaneously (for parallel experiments) while keeping backward compat
with the single marcus_root constructor.
"""

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from cato_src.core.aggregator import Aggregator


def _make_persistence_db(db_path: Path, tasks: list[dict[str, Any]]) -> None:
    """Create a marcus.db with the real persistence table schema."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE persistence (collection TEXT, key TEXT, data TEXT)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS events "
        "(id INTEGER PRIMARY KEY, collection TEXT, key TEXT, data TEXT)"
    )
    for t in tasks:
        conn.execute(
            "INSERT INTO persistence VALUES (?, ?, ?)",
            ("task_metadata", t["id"], json.dumps(t)),
        )
    conn.commit()
    conn.close()


def _make_subtasks_json(root: Path, tasks: list[dict[str, Any]]) -> None:
    """Write subtasks.json to the marcus_state dir of a root."""
    f = root / "data" / "marcus_state" / "subtasks.json"
    with open(f, "w") as fp:
        json.dump({t["id"]: t for t in tasks}, fp)


def _make_conversation_log(
    root: Path, filename: str, entries: list[dict[str, Any]]
) -> None:
    """Write a JSONL conversation log file."""
    log_dir = root / "logs" / "conversations"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / filename
    with open(log_file, "w") as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")


def _make_marcus_root(tmp_path: Path, suffix: str) -> Path:
    """Create a minimal Marcus-like root with projects.json."""
    root = tmp_path / f"marcus_{suffix}"
    data_dir = root / "data" / "marcus_state"
    data_dir.mkdir(parents=True)
    logs_dir = root / "logs" / "conversations"
    logs_dir.mkdir(parents=True)
    return root


def _write_projects(root: Path, projects: dict[str, Any]) -> None:
    """Write projects.json to the persistence_dir of a marcus root."""
    projects_file = root / "data" / "marcus_state" / "projects.json"
    with open(projects_file, "w") as f:
        json.dump(projects, f)


@pytest.fixture
def two_marcus_roots(tmp_path: Path) -> tuple[Path, Path]:
    """Create two independent Marcus roots with distinct projects."""
    root_a = _make_marcus_root(tmp_path, "a")
    root_b = _make_marcus_root(tmp_path, "b")

    _write_projects(
        root_a,
        {
            "proj_a1": {
                "id": "proj_a1",
                "name": "Alpha One",
                "created_at": "2026-01-01",
            },
            "proj_a2": {
                "id": "proj_a2",
                "name": "Alpha Two",
                "created_at": "2026-01-02",
            },
        },
    )
    _write_projects(
        root_b,
        {
            "proj_b1": {
                "id": "proj_b1",
                "name": "Beta One",
                "created_at": "2026-01-03",
            },
        },
    )
    return root_a, root_b


class TestMultiRootInit:
    """Aggregator accepts both singular and plural root forms."""

    def test_single_root_backward_compat(self, tmp_path: Path) -> None:
        """Old-style marcus_root= kwarg still works."""
        root = _make_marcus_root(tmp_path, "single")
        agg = Aggregator(marcus_root=root)
        assert agg.marcus_root == root

    def test_plural_roots_accepted(self, two_marcus_roots: tuple[Path, Path]) -> None:
        """New marcus_roots= list is accepted without error."""
        root_a, root_b = two_marcus_roots
        agg = Aggregator(marcus_roots=[root_a, root_b])
        assert agg.marcus_roots == [root_a, root_b]

    def test_plural_roots_sets_primary(
        self, two_marcus_roots: tuple[Path, Path]
    ) -> None:
        """First entry in marcus_roots becomes marcus_root for backward compat."""
        root_a, root_b = two_marcus_roots
        agg = Aggregator(marcus_roots=[root_a, root_b])
        assert agg.marcus_root == root_a

    def test_single_root_wrapped_in_list(self, tmp_path: Path) -> None:
        """Single marcus_root is also available as marcus_roots[0]."""
        root = _make_marcus_root(tmp_path, "wrap")
        agg = Aggregator(marcus_root=root)
        assert agg.marcus_roots == [root]


class TestMultiRootProjectLoading:
    """_load_projects() merges from all roots."""

    def test_loads_projects_from_all_roots(
        self, two_marcus_roots: tuple[Path, Path]
    ) -> None:
        """All projects from all roots appear in _load_projects() result."""
        root_a, root_b = two_marcus_roots
        agg = Aggregator(marcus_roots=[root_a, root_b])
        projects = agg._load_projects()
        ids = {p["id"] for p in projects}
        assert ids == {"proj_a1", "proj_a2", "proj_b1"}

    def test_total_count_is_sum_of_all_roots(
        self, two_marcus_roots: tuple[Path, Path]
    ) -> None:
        """Project count equals sum across all roots (2 + 1 = 3)."""
        root_a, root_b = two_marcus_roots
        agg = Aggregator(marcus_roots=[root_a, root_b])
        projects = agg._load_projects()
        assert len(projects) == 3

    def test_project_root_map_populated(
        self, two_marcus_roots: tuple[Path, Path]
    ) -> None:
        """_project_root maps each project_id to its source root."""
        root_a, root_b = two_marcus_roots
        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()
        assert agg._project_root["proj_a1"] == root_a
        assert agg._project_root["proj_a2"] == root_a
        assert agg._project_root["proj_b1"] == root_b

    def test_missing_projects_file_in_one_root_skipped(self, tmp_path: Path) -> None:
        """A root with no projects.json is silently skipped; others still load."""
        good_root = _make_marcus_root(tmp_path, "good")
        empty_root = _make_marcus_root(tmp_path, "empty")
        # No projects.json in empty_root
        _write_projects(
            good_root,
            {
                "proj_good": {
                    "id": "proj_good",
                    "name": "Good",
                    "created_at": "2026-01-01",
                }
            },
        )
        agg = Aggregator(marcus_roots=[good_root, empty_root])
        projects = agg._load_projects()
        assert len(projects) == 1
        assert projects[0]["id"] == "proj_good"

    def test_single_root_still_loads_normally(self, tmp_path: Path) -> None:
        """Single-root mode loads as before."""
        root = _make_marcus_root(tmp_path, "solo")
        _write_projects(
            root,
            {
                "solo_proj": {
                    "id": "solo_proj",
                    "name": "Solo",
                    "created_at": "2026-01-01",
                }
            },
        )
        agg = Aggregator(marcus_root=root)
        projects = agg._load_projects()
        assert len(projects) == 1
        assert projects[0]["id"] == "solo_proj"


class TestParallelKanbanDbEnrichment:
    """All kanban*.db files must contribute to task status enrichment.

    SQLite parallel experiments write to kanban_parallel_N.db rather than
    kanban.db. load_parent_tasks_from_db() must read all matching databases
    so Cato shows live task status during parallel runs.
    """

    def _make_kanban_db(self, path: Path, tasks: list[tuple[str, str, str]]) -> None:
        """Create a minimal kanban SQLite db with a tasks table."""
        import sqlite3

        conn = sqlite3.connect(str(path))
        conn.execute("CREATE TABLE tasks (id TEXT, status TEXT, assigned_to TEXT)")
        conn.executemany("INSERT INTO tasks VALUES (?, ?, ?)", tasks)
        conn.commit()
        conn.close()

    def test_reads_parallel_kanban_dbs(self, tmp_path: Path) -> None:
        """Tasks in kanban_parallel_N.db must be status-enriched."""
        root = _make_marcus_root(tmp_path, "parallel")
        data_dir = root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        # No kanban.db — only parallel dbs
        self._make_kanban_db(
            data_dir / "kanban_parallel_0.db",
            [("task-a", "in_progress", "agent-0")],
        )
        self._make_kanban_db(
            data_dir / "kanban_parallel_1.db",
            [("task-b", "done", "agent-1")],
        )

        agg = Aggregator(marcus_root=root)
        parent_tasks = [
            {"id": "task-a", "task_id": "task-a", "status": "todo"},
            {"id": "task-b", "task_id": "task-b", "status": "todo"},
        ]
        import json
        import sqlite3

        marcus_db = data_dir / "marcus.db"
        conn = sqlite3.connect(str(marcus_db))
        conn.execute("CREATE TABLE persistence (collection TEXT, key TEXT, data TEXT)")
        for t in parent_tasks:
            conn.execute(
                "INSERT INTO persistence VALUES (?, ?, ?)",
                ("task_metadata", t["id"], json.dumps(t)),
            )
        conn.commit()
        conn.close()

        tasks = agg.load_parent_tasks_from_db()
        status_map = {t.get("task_id", t.get("id")): t["status"] for t in tasks}

        assert (
            status_map.get("task-a") == "in_progress"
        ), "task-a from kanban_parallel_0.db not enriched"
        assert (
            status_map.get("task-b") == "done"
        ), "task-b from kanban_parallel_1.db not enriched"

    def test_main_kanban_db_still_read(self, tmp_path: Path) -> None:
        """kanban.db must still be included in the glob."""
        root = _make_marcus_root(tmp_path, "main")
        data_dir = root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        self._make_kanban_db(
            data_dir / "kanban.db",
            [("task-c", "done", "agent-main")],
        )

        agg = Aggregator(marcus_root=root)
        import json
        import sqlite3

        marcus_db = data_dir / "marcus.db"
        conn = sqlite3.connect(str(marcus_db))
        conn.execute("CREATE TABLE persistence (collection TEXT, key TEXT, data TEXT)")
        conn.execute(
            "INSERT INTO persistence VALUES (?, ?, ?)",
            (
                "task_metadata",
                "task-c",
                json.dumps({"id": "task-c", "task_id": "task-c", "status": "todo"}),
            ),
        )
        conn.commit()
        conn.close()

        tasks = agg.load_parent_tasks_from_db()
        status_map = {t.get("task_id", t.get("id")): t["status"] for t in tasks}
        assert status_map.get("task-c") == "done", "kanban.db tasks no longer enriched"


class TestParallelTaskLoading:
    """Tasks from non-primary roots must load when a project is selected from that root."""

    def test_tasks_loaded_from_correct_root(self, tmp_path: Path) -> None:
        """_load_tasks must read from the root that owns the selected project."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")

        _write_projects(
            root_a,
            {"proj_a": {"id": "proj_a", "name": "Alpha", "created_at": "2026-01-01"}},
        )
        _write_projects(
            root_b,
            {"proj_b": {"id": "proj_b", "name": "Beta", "created_at": "2026-01-02"}},
        )

        # Root B has tasks in marcus.db
        _make_persistence_db(
            root_b / "data" / "marcus.db",
            [
                {
                    "id": "task-b1",
                    "task_id": "task-b1",
                    "name": "Beta Task",
                    "status": "in_progress",
                }
            ],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()  # populate _project_root

        tasks = agg.load_parent_tasks_from_db(root=root_b)
        ids = {t.get("task_id", t.get("id")) for t in tasks}
        assert "task-b1" in ids, "Task from root_b not loaded when root_b is specified"

    def test_load_parent_tasks_defaults_to_primary_root(self, tmp_path: Path) -> None:
        """load_parent_tasks_from_db() with no root arg uses self.marcus_root."""
        root = _make_marcus_root(tmp_path, "primary")
        _write_projects(
            root,
            {"proj_p": {"id": "proj_p", "name": "Primary", "created_at": "2026-01-01"}},
        )
        _make_persistence_db(
            root / "data" / "marcus.db",
            [
                {
                    "id": "task-p1",
                    "task_id": "task-p1",
                    "name": "Primary Task",
                    "status": "todo",
                }
            ],
        )

        agg = Aggregator(marcus_root=root)
        tasks = agg.load_parent_tasks_from_db()
        ids = {t.get("task_id", t.get("id")) for t in tasks}
        assert "task-p1" in ids

    def test_tasks_not_cross_contaminated_between_roots(self, tmp_path: Path) -> None:
        """Tasks from root_a must not appear when loading root_b's project."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")

        _make_persistence_db(
            root_a / "data" / "marcus.db",
            [
                {
                    "id": "task-a1",
                    "task_id": "task-a1",
                    "name": "Alpha Task",
                    "status": "todo",
                }
            ],
        )
        _make_persistence_db(
            root_b / "data" / "marcus.db",
            [
                {
                    "id": "task-b1",
                    "task_id": "task-b1",
                    "name": "Beta Task",
                    "status": "todo",
                }
            ],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])

        tasks_b = agg.load_parent_tasks_from_db(root=root_b)
        ids = {t.get("task_id", t.get("id")) for t in tasks_b}
        assert "task-b1" in ids
        assert "task-a1" not in ids, "Root A task leaked into root B result"


class TestParallelMessageLoading:
    """Conversation logs from all roots must be merged in _load_messages."""

    def test_messages_merged_from_all_roots(self, tmp_path: Path) -> None:
        """_load_messages must include logs from every root, not just the primary."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")

        _make_conversation_log(
            root_a,
            "conversations_20260101_120000.jsonl",
            [
                {
                    "from_agent_id": "agent_a",
                    "to_agent_id": "marcus",
                    "message_type": "request",
                }
            ],
        )
        _make_conversation_log(
            root_b,
            "conversations_20260101_130000.jsonl",
            [
                {
                    "from_agent_id": "agent_b",
                    "to_agent_id": "marcus",
                    "message_type": "request",
                }
            ],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        messages = agg._load_messages()

        agent_ids = {m.get("from_agent_id") for m in messages}
        assert "agent_a" in agent_ids, "Messages from root_a missing"
        assert "agent_b" in agent_ids, "Messages from root_b missing"

    def test_single_root_messages_unaffected(self, tmp_path: Path) -> None:
        """Single-root mode still loads messages from that root."""
        root = _make_marcus_root(tmp_path, "solo")
        _make_conversation_log(
            root,
            "conversations_20260101_120000.jsonl",
            [
                {
                    "from_agent_id": "agent_solo",
                    "to_agent_id": "marcus",
                    "message_type": "request",
                }
            ],
        )

        agg = Aggregator(marcus_root=root)
        messages = agg._load_messages()
        assert any(m.get("from_agent_id") == "agent_solo" for m in messages)


def _write_decisions_json(
    root: Path, project_id: str, decisions: list[dict[str, Any]]
) -> None:
    """Write a decisions.json file as Marcus's JSON-fallback export would."""
    history_dir = root / "data" / "project_history" / project_id
    history_dir.mkdir(parents=True, exist_ok=True)
    with open(history_dir / "decisions.json", "w") as f:
        json.dump({"decisions": decisions}, f)


def _write_artifacts_json(
    root: Path, project_id: str, artifacts: list[dict[str, Any]]
) -> None:
    """Write an artifacts.json file as Marcus's JSON-fallback export would."""
    history_dir = root / "data" / "project_history" / project_id
    history_dir.mkdir(parents=True, exist_ok=True)
    with open(history_dir / "artifacts.json", "w") as f:
        json.dump({"artifacts": artifacts}, f)


class TestParallelDecisionArtifactLoading:
    """_load_decisions/_load_artifacts must be root-aware, like task loading
    already is (cato#50).

    Reproduces the live bug found 2026-11-05: a project living in a
    non-primary root returned 0 decisions/artifacts even though the data
    genuinely existed, because the persistence lookup only ever looked at
    marcus_root (the first root), never the root that actually owns the
    project being queried.
    """

    def test_decisions_loaded_from_non_primary_root(self, tmp_path: Path) -> None:
        """A project living only in root_b's history must still load its decisions."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")
        _write_projects(
            root_a,
            {"proj_a": {"id": "proj_a", "name": "Alpha", "created_at": "2026-01-01"}},
        )
        _write_projects(
            root_b,
            {"proj_b": {"id": "proj_b", "name": "Beta", "created_at": "2026-01-02"}},
        )
        _write_decisions_json(
            root_b, "proj_b", [{"id": "d1", "project_id": "proj_b", "what": "chose X"}]
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()  # populate _project_root

        decisions = agg._load_decisions(project_id="proj_b")
        ids = {d["id"] for d in decisions}
        assert "d1" in ids, "Decision from root_b not loaded when proj_b is queried"

    def test_artifacts_loaded_from_non_primary_root(self, tmp_path: Path) -> None:
        """A project living only in root_b's history must still load its artifacts."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")
        _write_projects(
            root_a,
            {"proj_a": {"id": "proj_a", "name": "Alpha", "created_at": "2026-01-01"}},
        )
        _write_projects(
            root_b,
            {"proj_b": {"id": "proj_b", "name": "Beta", "created_at": "2026-01-02"}},
        )
        _write_artifacts_json(
            root_b,
            "proj_b",
            [{"id": "a1", "project_id": "proj_b", "name": "report.md"}],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()

        artifacts = agg._load_artifacts(project_id="proj_b")
        ids = {a["id"] for a in artifacts}
        assert "a1" in ids, "Artifact from root_b not loaded when proj_b is queried"

    def test_decisions_still_load_from_primary_root_first(self, tmp_path: Path) -> None:
        """Querying root_a's own project (the primary root) must keep working."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")
        _write_projects(
            root_a,
            {"proj_a": {"id": "proj_a", "name": "Alpha", "created_at": "2026-01-01"}},
        )
        _write_projects(
            root_b,
            {"proj_b": {"id": "proj_b", "name": "Beta", "created_at": "2026-01-02"}},
        )
        _write_decisions_json(
            root_a,
            "proj_a",
            [{"id": "d_a1", "project_id": "proj_a", "what": "chose Y"}],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()

        decisions = agg._load_decisions(project_id="proj_a")
        ids = {d["id"] for d in decisions}
        assert "d_a1" in ids

    def test_querying_root_a_then_root_b_does_not_lock_onto_root_a(
        self, tmp_path: Path
    ) -> None:
        """Resolving one project from root_a must not poison later lookups for
        a project in root_b -- the exact caching failure mode from cato#50."""
        root_a = _make_marcus_root(tmp_path, "a")
        root_b = _make_marcus_root(tmp_path, "b")
        _write_projects(
            root_a,
            {"proj_a": {"id": "proj_a", "name": "Alpha", "created_at": "2026-01-01"}},
        )
        _write_projects(
            root_b,
            {"proj_b": {"id": "proj_b", "name": "Beta", "created_at": "2026-01-02"}},
        )
        _write_decisions_json(
            root_a,
            "proj_a",
            [{"id": "d_a1", "project_id": "proj_a", "what": "chose Y"}],
        )
        _write_decisions_json(
            root_b,
            "proj_b",
            [{"id": "d_b1", "project_id": "proj_b", "what": "chose Z"}],
        )

        agg = Aggregator(marcus_roots=[root_a, root_b])
        agg._load_projects()

        # Query root_a's project first (this is what resolves/caches persistence today).
        first = agg._load_decisions(project_id="proj_a")
        assert {d["id"] for d in first} == {"d_a1"}

        # Now query root_b's project -- must not come back empty or return proj_a's data.
        second = agg._load_decisions(project_id="proj_b")
        assert {d["id"] for d in second} == {"d_b1"}
