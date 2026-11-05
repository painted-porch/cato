"""
Integration tests for Cato's multi-instance cost support (cato#52).

Mirrors the fixture style of ``test_cost_api.py``, but overrides
``get_stores``/``get_aggregators`` with TWO tmp :class:`CostStore`
instances instead of one, so every merge/search-across-sources code
path in ``backend/cost_routes.py`` gets exercised against real data,
not mocks.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Tuple

import pytest
from fastapi.testclient import TestClient

try:
    from backend.cost_routes import COST_TRACKING_AVAILABLE
except ImportError:
    COST_TRACKING_AVAILABLE = False

requires_marcus = pytest.mark.skipif(
    not COST_TRACKING_AVAILABLE,
    reason="Marcus cost_tracking modules unavailable",
)


def _seed_store(db_path: Path, run_id: str, project_id: str, project_name: str) -> Any:
    """Build one tmp CostStore with one run/project/event, as a real instance would."""
    from src.cost_tracking.cost_store import CostStore, ModelPrice, Run, TokenEvent

    s = CostStore(db_path=db_path)
    s.record_price(
        ModelPrice(
            model="claude-sonnet-4-6",
            provider="anthropic",
            effective_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            input_per_million=3.0,
            cache_creation_per_million=3.75,
            cache_read_per_million=0.30,
            output_per_million=15.0,
            source="default",
        )
    )
    s.record_run(
        Run(
            run_id=run_id,
            project_id=project_id,
            project_name=project_name,
            started_at=datetime(2026, 5, 10, tzinfo=timezone.utc),
            num_agents=1,
            total_tasks=1,
            completed_tasks=1,
        )
    )
    s.record_event(
        TokenEvent(
            run_id=run_id,
            project_id=project_id,
            agent_id="agent_1",
            agent_role="worker",
            operation="turn",
            provider="anthropic",
            model="claude-sonnet-4-6",
            session_id=f"session_{run_id}",
            turn_index=0,
            input_tokens=1000,
            output_tokens=200,
            timestamp=datetime(2026, 5, 10, 1, tzinfo=timezone.utc),
        )
    )
    return s


@pytest.fixture
def two_sources(tmp_path: Path) -> Tuple[Any, Any]:
    """Two independent CostStore instances, as two isolated Marcus instances would have."""
    store_a = _seed_store(
        tmp_path / "a" / "costs.db",
        run_id="run_a1",
        project_id="proj_a",
        project_name="Alpha",
    )
    store_b = _seed_store(
        tmp_path / "b" / "costs.db",
        run_id="run_b1",
        project_id="proj_b",
        project_name="Beta",
    )
    return store_a, store_b


@pytest.fixture
def client(two_sources: Tuple[Any, Any]) -> TestClient:
    """TestClient wired to two cost sources via get_stores/get_aggregators."""
    from src.cost_tracking.cost_aggregator import CostAggregator

    from backend.api import app
    from backend.cost_routes import get_aggregators, get_stores

    store_a, store_b = two_sources
    app.dependency_overrides[get_stores] = lambda: [
        ("instance_a", store_a),
        ("instance_b", store_b),
    ]
    app.dependency_overrides[get_aggregators] = lambda: [
        ("instance_a", CostAggregator(store=store_a)),
        ("instance_b", CostAggregator(store=store_b)),
    ]
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@requires_marcus
class TestConfiguredSources:
    """_cost_sources() / _configured_cost_db_paths() read config.json correctly."""

    def test_no_config_file_falls_back_to_single_default(self, tmp_path: Path) -> None:
        """A missing config.json is not an error -- falls back to one source."""
        from backend.cost_routes import _cost_sources

        sources = _cost_sources(config_path=tmp_path / "does_not_exist.json")
        assert len(sources) == 1

    def test_absent_cost_db_paths_key_falls_back_to_single_default(
        self, tmp_path: Path
    ) -> None:
        """A config.json with marcus_data_paths but no cost_db_paths still falls back."""
        from backend.cost_routes import _cost_sources

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps({"marcus_data_paths": ["/some/path"]}))
        sources = _cost_sources(config_path=config_path)
        assert len(sources) == 1

    def test_configured_cost_db_paths_returns_one_source_per_entry(
        self, tmp_path: Path
    ) -> None:
        """Two entries in cost_db_paths become two sources, labeled by their path."""
        from backend.cost_routes import _cost_sources

        config_path = tmp_path / "config.json"
        config_path.write_text(
            json.dumps(
                {"cost_db_paths": ["/instance_a/costs.db", "/instance_b/costs.db"]}
            )
        )
        sources = _cost_sources(config_path=config_path)
        labels = [label for label, _ in sources]
        assert labels == ["/instance_a/costs.db", "/instance_b/costs.db"]


@requires_marcus
class TestListProjectsMultiSource:
    def test_merges_projects_from_both_sources(self, client: TestClient) -> None:
        """Both instances' projects appear in one combined list."""
        resp = client.get("/api/cost/projects")
        assert resp.status_code == 200
        ids = {p["project_id"] for p in resp.json()["projects"]}
        assert ids == {"proj_a", "proj_b"}

    def test_each_row_tagged_with_its_source(self, client: TestClient) -> None:
        """Every row carries which instance it came from."""
        resp = client.get("/api/cost/projects")
        by_id = {p["project_id"]: p["source"] for p in resp.json()["projects"]}
        assert by_id["proj_a"] == "instance_a"
        assert by_id["proj_b"] == "instance_b"


@requires_marcus
class TestListRunsMultiSource:
    def test_merges_runs_from_both_sources(self, client: TestClient) -> None:
        resp = client.get("/api/cost/runs")
        ids = {r["run_id"] for r in resp.json()["runs"]}
        assert ids == {"run_a1", "run_b1"}

    def test_filter_by_project_only_queries_that_projects_source(
        self, client: TestClient
    ) -> None:
        """Filtering by proj_b's project_id must not also pull proj_a's run."""
        resp = client.get("/api/cost/runs", params={"project_id": "proj_b"})
        ids = {r["run_id"] for r in resp.json()["runs"]}
        assert ids == {"run_b1"}


@requires_marcus
class TestRunSummaryMultiSource:
    def test_finds_run_on_second_source(self, client: TestClient) -> None:
        """A run that only exists on instance_b must still be found, not 404."""
        resp = client.get("/api/cost/runs/run_b1")
        assert resp.status_code == 200
        assert resp.json()["run_id"] == "run_b1"

    def test_unknown_run_is_404_across_all_sources(self, client: TestClient) -> None:
        resp = client.get("/api/cost/runs/does_not_exist")
        assert resp.status_code == 404


@requires_marcus
class TestUnassignedTotalsMultiSource:
    def test_sums_zero_across_both_sources_when_nothing_unassigned(
        self, client: TestClient
    ) -> None:
        """Both fixture stores only have attributed events; combined total is zero."""
        resp = client.get("/api/cost/projects/unassigned")
        totals = resp.json()
        assert totals["events"] == 0
        assert totals["total_cost_usd"] == 0.0


@requires_marcus
class TestProjectSummaryMultiSource:
    def test_finds_project_owned_by_second_source(self, client: TestClient) -> None:
        resp = client.get("/api/cost/projects/proj_b")
        body = resp.json()
        assert body["project_id"] == "proj_b"
        assert body["totals"]["events"] == 1

    def test_unknown_project_returns_zero_shape_not_error(
        self, client: TestClient
    ) -> None:
        resp = client.get("/api/cost/projects/does_not_exist")
        assert resp.status_code == 200
        assert resp.json()["totals"]["events"] == 0


@requires_marcus
class TestProjectFullSummaryMultiSource:
    def test_finds_project_owned_by_second_source(self, client: TestClient) -> None:
        """proj_b only exists on instance_b; must be found, not 404."""
        resp = client.get("/api/cost/projects/proj_b/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["project_id"] == "proj_b"
        assert body["summary"]["total_events"] == 1

    def test_unknown_project_is_404(self, client: TestClient) -> None:
        resp = client.get("/api/cost/projects/does_not_exist/summary")
        assert resp.status_code == 404


@requires_marcus
class TestSessionTurnsMultiSource:
    def test_finds_session_on_second_source(self, client: TestClient) -> None:
        resp = client.get("/api/cost/sessions/session_run_b1/turns")
        assert resp.status_code == 200
        assert len(resp.json()["turns"]) == 1


@requires_marcus
class TestProjectBudgetMultiSource:
    def test_budget_set_on_second_sources_project_round_trips(
        self, client: TestClient
    ) -> None:
        """Setting a budget for proj_b (owned by instance_b) must persist there,
        not silently land on instance_a's store."""
        put_resp = client.put(
            "/api/cost/projects/proj_b/budget",
            json={"budget_usd": 25.0, "note": "test"},
        )
        assert put_resp.status_code == 200
        assert put_resp.json()["budget"]["budget_usd"] == 25.0

        get_resp = client.get("/api/cost/projects/proj_b/budget")
        assert get_resp.json()["budget"]["budget_usd"] == 25.0

        # proj_a's budget must be unaffected.
        other_resp = client.get("/api/cost/projects/proj_a/budget")
        assert other_resp.json()["budget"] is None


@requires_marcus
class TestExportCsvMultiSource:
    def test_exports_run_from_second_source(self, client: TestClient) -> None:
        resp = client.get("/api/cost/export/run_b1")
        assert resp.status_code == 200
        assert (
            "run_b1" not in resp.text.splitlines()[0]
        )  # header row has no run_id column
        assert len(resp.text.splitlines()) == 2  # header + 1 event row


@requires_marcus
class TestIngestRoutesToOwningSource:
    """run_ingest must route each worker session to the source that
    already owns its project's events, not always to the first source
    (Codex finding on PR #53: a global sweep into one fixed store would
    silently split one project's cost data across two databases)."""

    def test_worker_session_for_proj_b_is_ingested_into_instance_b(
        self,
        client: TestClient,
        two_sources: Tuple[Any, Any],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from backend.cost_ingest import clear_project_info_cache

        store_a, store_b = two_sources
        clear_project_info_cache()

        home = tmp_path / "home"
        exp_dir = home / "experiment_proj_b"
        worktree = exp_dir / "worktrees" / "agent_9"
        worktree.mkdir(parents=True)
        (exp_dir / "project_info.json").write_text(json.dumps({"project_id": "proj_b"}))

        sess_dir = home / ".claude" / "projects" / "fake"
        sess_dir.mkdir(parents=True)
        record = {
            "type": "assistant",
            "uuid": "u_routing_1",
            "sessionId": "s_routing_1",
            "requestId": "req_routing_1",
            "timestamp": "2026-05-10T14:00:00.000Z",
            "cwd": str(worktree),
            "message": {
                "model": "claude-sonnet-4-6",
                "usage": {
                    "input_tokens": 10,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0,
                    "output_tokens": 5,
                },
            },
        }
        (sess_dir / "s.jsonl").write_text(json.dumps(record) + "\n")
        monkeypatch.setenv("HOME", str(home))

        resp = client.post("/api/cost/ingest")
        assert resp.status_code == 200
        assert resp.json()["ingested"] == 1

        in_b = store_b.conn.execute(
            "SELECT COUNT(*) FROM token_events WHERE request_id = 'req_routing_1'"
        ).fetchone()[0]
        in_a = store_a.conn.execute(
            "SELECT COUNT(*) FROM token_events WHERE request_id = 'req_routing_1'"
        ).fetchone()[0]
        assert (
            in_b == 1
        ), "worker event for proj_b (owned by instance_b) must land in instance_b"
        assert in_a == 0, "worker event for proj_b must NOT land in instance_a"


@requires_marcus
class TestBudgetSurvivesSourceMigration:
    """A budget set before a project has events must not be orphaned once
    that project's events land in a different configured source (Codex
    finding on PR #53)."""

    def test_budget_set_before_events_exist_stays_visible_after_events_appear(
        self, client: TestClient, two_sources: Tuple[Any, Any]
    ) -> None:
        store_a, store_b = two_sources

        # proj_new has no events anywhere yet -- budget lands on the first
        # configured source (instance_a) per _find_owning_store's fallback.
        put_resp = client.put(
            "/api/cost/projects/proj_new/budget",
            json={"budget_usd": 50.0, "note": "set before any events"},
        )
        assert put_resp.status_code == 200
        assert put_resp.json()["budget"]["budget_usd"] == 50.0

        # Now proj_new's first event lands on instance_b (a different
        # source than the one the budget was set on).
        from src.cost_tracking.cost_store import TokenEvent

        store_b.record_event(
            TokenEvent(
                run_id="run_new1",
                project_id="proj_new",
                agent_id="agent_1",
                agent_role="worker",
                operation="turn",
                provider="anthropic",
                model="claude-sonnet-4-6",
                input_tokens=10,
                output_tokens=5,
            )
        )

        get_resp = client.get("/api/cost/projects/proj_new/budget")
        assert get_resp.json()["budget"]["budget_usd"] == 50.0, (
            "budget set on instance_a before proj_new had events must still be "
            "visible after proj_new's events land on instance_b"
        )

    def test_updating_an_existing_budget_edits_it_in_place_not_a_new_copy(
        self, client: TestClient, two_sources: Tuple[Any, Any]
    ) -> None:
        """proj_b already has events on instance_b; set its budget, then
        update it, and confirm there is exactly one row, on instance_b."""
        store_a, store_b = two_sources

        client.put("/api/cost/projects/proj_b/budget", json={"budget_usd": 10.0})
        client.put("/api/cost/projects/proj_b/budget", json={"budget_usd": 20.0})

        get_resp = client.get("/api/cost/projects/proj_b/budget")
        assert get_resp.json()["budget"]["budget_usd"] == 20.0

        assert store_a.get_project_budget("proj_b") is None
        assert store_b.get_project_budget("proj_b") is not None


@requires_marcus
class TestProjectNameResolvedFromOwningSourceOnly:
    """A conflicting/stale name snapshot on an unrelated, LATER-iterated
    source must not overwrite the owning (earlier) source's correct name
    (Codex finding on PR #53: merging every source's name map with
    dict.update() makes the last-iterated source always win, regardless
    of which source actually owns the project -- proj_a is owned by
    instance_a, the FIRST configured source, so a stale override on
    instance_b (iterated after it) only catches the bug if instance_b's
    value is wrongly allowed to win)."""

    def test_legacy_summary_uses_owning_sources_name_not_another_sources(
        self, client: TestClient, two_sources: Tuple[Any, Any]
    ) -> None:
        store_a, store_b = two_sources
        store_a.upsert_project_name("proj_a", "Correct Alpha Name")
        # instance_b, iterated after instance_a, has a stale/conflicting
        # snapshot for proj_a's id even though it owns no events for it.
        store_b.upsert_project_name("proj_a", "Wrong Name From Instance B")

        resp = client.get("/api/cost/projects/proj_a")
        assert resp.json()["project_name"] == "Correct Alpha Name"

    def test_full_summary_uses_owning_sources_name_not_another_sources(
        self, client: TestClient, two_sources: Tuple[Any, Any]
    ) -> None:
        store_a, store_b = two_sources
        store_a.upsert_project_name("proj_a", "Correct Alpha Name")
        store_b.upsert_project_name("proj_a", "Wrong Name From Instance B")

        resp = client.get("/api/cost/projects/proj_a/summary")
        assert resp.json()["project_name"] == "Correct Alpha Name"
