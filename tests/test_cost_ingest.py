"""
Unit tests for backend.cost_ingest.

Covers the cwd→AgentBinding resolver and the run_ingest sweep against
synthetic experiment directories laid out the way Marcus's
spawn_agents.py creates them. No real ``~/.claude/projects/`` access.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

try:
    from backend.cost_ingest import (
        clear_project_info_cache,
        resolve_binding_from_cwd,
        run_ingest,
    )
    from backend.cost_routes import COST_TRACKING_AVAILABLE
except ImportError:
    COST_TRACKING_AVAILABLE = False

requires_marcus = pytest.mark.skipif(
    not COST_TRACKING_AVAILABLE,
    reason="Marcus cost_tracking modules unavailable",
)


# ---------------------------------------------------------------------------
# Fixtures: synthetic experiment layout matching spawn_agents.py
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    """Drop the lru_cache between tests so each one re-reads project_info."""
    clear_project_info_cache()


@pytest.fixture
def experiment_dir(tmp_path: Path) -> Path:
    """Mimic the layout spawn_agents.py produces under tmp_path."""
    exp = tmp_path / "my-experiment"
    (exp / "worktrees" / "agent_1").mkdir(parents=True)
    (exp / "worktrees" / "agent_2").mkdir(parents=True)
    (exp / "project_info.json").write_text(
        json.dumps({"project_id": "proj_42", "board_id": "b_1"})
    )
    return exp


# ---------------------------------------------------------------------------
# resolve_binding_from_cwd
# ---------------------------------------------------------------------------


@requires_marcus
class TestResolveBinding:
    """cwd-based agent → project resolution."""

    def test_resolves_agent_in_worktree(self, experiment_dir: Path) -> None:
        """Standard worker layout maps cwd → (agent_id, project_id)."""
        binding = resolve_binding_from_cwd(
            {"cwd": str(experiment_dir / "worktrees" / "agent_1")}
        )
        assert binding is not None
        assert binding.agent_id == "agent_1"
        assert binding.project_id == "proj_42"

    def test_returns_none_when_cwd_missing(self) -> None:
        """No cwd field at all → drop the record."""
        assert resolve_binding_from_cwd({}) is None

    def test_returns_none_when_cwd_not_in_worktree(self, experiment_dir: Path) -> None:
        """A project-creator working in the experiment root has no agent_id.

        Their cwd is the experiment dir, not <exp>/worktrees/<agent>, so
        the resolver returns None and their events are skipped.
        """
        assert resolve_binding_from_cwd({"cwd": str(experiment_dir)}) is None

    def test_returns_none_when_project_info_missing(self, tmp_path: Path) -> None:
        """Experiment dir without project_info.json → cannot bind."""
        broken = tmp_path / "no-info"
        (broken / "worktrees" / "agent_1").mkdir(parents=True)
        # No project_info.json written.
        assert (
            resolve_binding_from_cwd({"cwd": str(broken / "worktrees" / "agent_1")})
            is None
        )

    def test_returns_none_when_project_info_lacks_project_id(
        self, tmp_path: Path
    ) -> None:
        """project_info.json missing project_id field → drop."""
        broken = tmp_path / "bad-info"
        (broken / "worktrees" / "agent_1").mkdir(parents=True)
        (broken / "project_info.json").write_text(json.dumps({"board_id": "b"}))
        assert (
            resolve_binding_from_cwd({"cwd": str(broken / "worktrees" / "agent_1")})
            is None
        )


# ---------------------------------------------------------------------------
# run_ingest sweep
# ---------------------------------------------------------------------------


@requires_marcus
class TestRunIngest:
    """End-to-end: synthetic JSONL → ingest sweep → token_events row."""

    @pytest.fixture
    def store(self, tmp_path: Path) -> Any:
        """Tmp CostStore with default seed prices."""
        from src.cost_tracking.cost_store import CostStore

        s = CostStore(db_path=tmp_path / "costs.db")
        s.load_seed_prices()
        return s

    def test_ingests_worker_session(
        self,
        store: Any,
        experiment_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A session file in the synthetic ~/.claude/projects dir is ingested."""
        # Lay out the synthetic Claude Code session log.
        claude_root = tmp_path / "claude_projects"
        sess_dir = claude_root / "fake-project"
        sess_dir.mkdir(parents=True)
        record = {
            "type": "assistant",
            "uuid": "u1",
            "sessionId": "s1",
            "requestId": "req1",
            "timestamp": "2026-05-10T14:00:00.000Z",
            "cwd": str(experiment_dir / "worktrees" / "agent_1"),
            "message": {
                "model": "claude-sonnet-4-6",
                "usage": {
                    "input_tokens": 100,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 50,
                },
            },
        }
        (sess_dir / "s1.jsonl").write_text(json.dumps(record) + "\n")

        # Redirect Path.home() so run_ingest looks at our tmp tree.
        monkeypatch.setenv("HOME", str(tmp_path))
        # _read_project_info is cached; clear it.
        clear_project_info_cache()

        # ``Path.home()`` reads from $HOME on POSIX. Sanity-check.
        assert Path.home() == tmp_path

        # Move the session dir to where run_ingest expects it.
        target = tmp_path / ".claude" / "projects" / "fake-project"
        target.parent.mkdir(parents=True)
        sess_dir.rename(target)

        result = run_ingest([("default", store)])
        assert result["ingested"] == 1
        assert result["files"] == 1
        assert result["skipped_unbound"] == 0

        row = store.conn.execute(
            "SELECT project_id, agent_id, input_tokens, output_tokens "
            "FROM token_events"
        ).fetchone()
        # run_ingest's per-project ownership check (cato#52) instantiates a
        # CostAggregator, which sets store.conn.row_factory = sqlite3.Row as
        # a side effect -- compare via tuple() so this doesn't depend on
        # which row type the shared connection happens to be using.
        assert tuple(row) == ("proj_42", "agent_1", 100, 50)

    def test_run_ingest_is_idempotent_across_calls(
        self,
        store: Any,
        experiment_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Sweeping the same JSONL twice must not duplicate token_events.

        Cato's dashboard polls run_ingest every 30s with a fresh
        WorkerJSONLIngester whose in-memory dedup set is empty. The
        Marcus side enforces dedup at the DB layer (partial UNIQUE
        INDEX on request_id + INSERT OR IGNORE), so calling run_ingest
        repeatedly on the same files inserts the row only once.
        """
        sess_dir = tmp_path / ".claude" / "projects" / "fake-project"
        sess_dir.mkdir(parents=True)
        record = {
            "type": "assistant",
            "uuid": "u1",
            "sessionId": "s1",
            "requestId": "req_idem_1",
            "timestamp": "2026-05-10T14:00:00.000Z",
            "cwd": str(experiment_dir / "worktrees" / "agent_1"),
            "message": {
                "model": "claude-sonnet-4-6",
                "usage": {
                    "input_tokens": 100,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 50,
                },
            },
        }
        (sess_dir / "s1.jsonl").write_text(json.dumps(record) + "\n")
        monkeypatch.setenv("HOME", str(tmp_path))

        first = run_ingest([("default", store)])
        second = run_ingest([("default", store)])

        assert first["ingested"] == 1
        # Second sweep may report ingested>0 from the library's perspective,
        # but the DB-level UNIQUE constraint guarantees no duplicate rows.
        count = store.conn.execute(
            "SELECT COUNT(*) FROM token_events WHERE request_id = 'req_idem_1'"
        ).fetchone()[0]
        assert count == 1, f"expected 1 row after 2 sweeps, got {count}"
        assert second["files"] == 1

    def test_returns_zeros_when_claude_projects_missing(
        self,
        store: Any,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No ~/.claude/projects/ dir → graceful zero return, no error."""
        monkeypatch.setenv("HOME", str(tmp_path))
        result = run_ingest([("default", store)])
        assert result == {"ingested": 0, "files": 0, "skipped_unbound": 0}
