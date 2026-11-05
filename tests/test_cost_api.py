"""
Integration tests for Cato's ``/api/cost/*`` endpoints.

These tests use FastAPI's TestClient + a tmp SQLite store seeded with
deterministic data. The store dependency is overridden via
``app.dependency_overrides`` so the tests never touch
``~/.marcus/costs.db``.

Tests are skipped if Marcus cost_tracking modules aren't importable.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path) -> Any:
    """Tmp CostStore seeded with one Anthropic price and a few events."""
    from src.cost_tracking.cost_store import (
        CostStore,
        ModelPrice,
        Run,
        TokenEvent,
    )

    s = CostStore(db_path=tmp_path / "costs.db")
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
            run_id="exp_1",
            project_id="proj_1",
            project_name="hangman",
            started_at=datetime(2026, 5, 10, tzinfo=timezone.utc),
            num_agents=2,
            total_tasks=10,
            completed_tasks=4,
        )
    )
    base = dict(
        run_id="exp_1",
        project_id="proj_1",
        provider="anthropic",
        model="claude-sonnet-4-6",
    )
    s.record_event(
        TokenEvent(
            agent_id="planner",
            agent_role="planner",
            operation="parse_prd",
            input_tokens=1000,
            output_tokens=500,
            **base,
        )
    )
    s.record_event(
        TokenEvent(
            agent_id="agent_1",
            agent_role="worker",
            operation="turn",
            task_id="t_1",
            session_id="s_1",
            turn_index=1,
            input_tokens=2000,
            cache_read_tokens=500,
            output_tokens=100,
            **base,
        )
    )
    s.record_event(
        TokenEvent(
            agent_id="agent_1",
            agent_role="worker",
            operation="turn",
            task_id="t_1",
            session_id="s_1",
            turn_index=2,
            input_tokens=300,
            output_tokens=50,
            **base,
        )
    )
    return s


@pytest.fixture
def client(store: Any) -> TestClient:
    """TestClient with store / aggregator dependencies overridden.

    Overrides both the single-source (``get_store``/``get_aggregator``,
    still used by write endpoints) and multi-source (``get_stores``/
    ``get_aggregators``, used by read endpoints since cato#52)
    dependencies with the same one-store single-source setup, so every
    existing single-instance test keeps exercising exactly one source.
    """
    from src.cost_tracking.cost_aggregator import CostAggregator

    from backend.api import app
    from backend.cost_routes import (
        get_aggregator,
        get_aggregators,
        get_store,
        get_stores,
    )

    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_aggregator] = lambda: CostAggregator(store=store)
    app.dependency_overrides[get_stores] = lambda: [("default", store)]
    app.dependency_overrides[get_aggregators] = lambda: [
        ("default", CostAggregator(store=store))
    ]
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# /api/cost/runs
# ---------------------------------------------------------------------------


@requires_marcus
class TestProjectsList:
    """/api/cost/projects — primary entry point for the dashboard."""

    def test_lists_projects_with_totals(self, client: TestClient) -> None:
        """One row per distinct project_id, derived from token_events."""
        resp = client.get("/api/cost/projects")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["projects"][0]["project_id"] == "proj_1"
        assert (
            body["projects"][0]["total_tokens"]
            == 1000 + 500 + 2000 + 500 + 100 + 300 + 50
        )

    def test_excludes_unassigned_from_project_list(
        self, store: Any, client: TestClient
    ) -> None:
        """Events in the 'unassigned' bucket do not appear as a project."""
        from src.cost_tracking.cost_store import TokenEvent

        store.record_event(
            TokenEvent(
                run_id="unassigned",
                project_id="unassigned",
                agent_id="planner",
                agent_role="planner",
                operation="parse_prd",
                provider="anthropic",
                model="claude-sonnet-4-6",
                input_tokens=10,
                output_tokens=10,
            )
        )
        resp = client.get("/api/cost/projects")
        ids = {p["project_id"] for p in resp.json()["projects"]}
        assert "unassigned" not in ids


@requires_marcus
class TestUnassignedTotals:
    """/api/cost/projects/unassigned — gap visibility."""

    def test_returns_zeros_when_empty(self, client: TestClient) -> None:
        """No unassigned events → zero totals (200, not 404)."""
        resp = client.get("/api/cost/projects/unassigned")
        assert resp.status_code == 200
        body = resp.json()
        assert body["events"] == 0
        assert body["total_cost_usd"] == 0.0

    def test_sums_unassigned_events(self, store: Any, client: TestClient) -> None:
        """An event tagged 'unassigned' shows up in the totals."""
        from src.cost_tracking.cost_store import TokenEvent

        store.record_event(
            TokenEvent(
                run_id="unassigned",
                project_id="unassigned",
                agent_id="planner",
                agent_role="planner",
                operation="parse_prd",
                provider="anthropic",
                model="claude-sonnet-4-6",
                input_tokens=1_000_000,
                output_tokens=0,
            )
        )
        resp = client.get("/api/cost/projects/unassigned")
        body = resp.json()
        assert body["events"] == 1
        # 1M input * $3/M = $3
        assert body["total_cost_usd"] == pytest.approx(3.0, rel=1e-6)


@requires_marcus
class TestRunsList:
    def test_lists_runs(self, client: TestClient) -> None:
        """Endpoint returns the seeded run with totals."""
        resp = client.get("/api/cost/runs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["runs"][0]["run_id"] == "exp_1"
        assert (
            body["runs"][0]["total_tokens"] == 1000 + 500 + 2000 + 500 + 100 + 300 + 50
        )

    def test_filter_by_project(self, client: TestClient) -> None:
        """Unknown project returns empty list, not 404."""
        resp = client.get("/api/cost/runs?project_id=nope")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0


@requires_marcus
class TestRunSummary:
    def test_returns_full_breakdown(self, client: TestClient) -> None:
        """Endpoint returns the same shape as CostAggregator.run_summary."""
        resp = client.get("/api/cost/runs/exp_1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["run_id"] == "exp_1"
        assert {
            "summary",
            "by_role",
            "by_agent",
            "by_task",
            "by_operation",
            "by_model",
        } <= set(body.keys())

    def test_unknown_returns_404(self, client: TestClient) -> None:
        """Missing run_id yields a 404."""
        resp = client.get("/api/cost/runs/nope")
        assert resp.status_code == 404


@requires_marcus
class TestProjectSummary:
    def test_returns_project_totals(self, client: TestClient) -> None:
        """Project endpoint returns totals + runs list."""
        resp = client.get("/api/cost/projects/proj_1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["project_id"] == "proj_1"
        assert body["totals"]["events"] == 3
        assert len(body["runs"]) == 1


@requires_marcus
class TestProjectBudget:
    """``GET/PUT /api/cost/projects/{id}/budget`` — project-level cap."""

    def test_get_returns_null_when_unset(self, client: TestClient) -> None:
        """A project with no cap returns ``budget: null``."""
        resp = client.get("/api/cost/projects/proj_1/budget")
        assert resp.status_code == 200
        body = resp.json()
        assert body["project_id"] == "proj_1"
        assert body["budget"] is None

    def test_put_then_get_roundtrips(self, client: TestClient) -> None:
        """Setting a cap persists across a fresh GET."""
        resp = client.put(
            "/api/cost/projects/proj_1/budget",
            json={"budget_usd": 25.5, "note": "poc"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["budget"]["budget_usd"] == 25.5
        assert body["budget"]["note"] == "poc"

        resp2 = client.get("/api/cost/projects/proj_1/budget")
        assert resp2.json()["budget"]["budget_usd"] == 25.5

    def test_put_zero_clears_the_cap(self, client: TestClient) -> None:
        """PUT with budget_usd=0 removes the row (no cap)."""
        client.put(
            "/api/cost/projects/proj_1/budget",
            json={"budget_usd": 25.0},
        )
        resp = client.put(
            "/api/cost/projects/proj_1/budget",
            json={"budget_usd": 0},
        )
        assert resp.json()["budget"] is None


@requires_marcus
class TestProjectFullSummary:
    """``GET /api/cost/projects/{id}/summary`` — drives project-first tabs."""

    def test_returns_full_breakdown(self, client: TestClient) -> None:
        """Same shape as /experiments/{id} but scoped to project_id."""
        resp = client.get("/api/cost/projects/proj_1/summary")
        assert resp.status_code == 200
        body = resp.json()
        assert body["project_id"] == "proj_1"
        # fixture has 3 events: planner(1500) + worker_turn1(2600) + worker_turn2(350)
        assert body["summary"]["total_events"] == 3
        assert body["summary"]["total_tokens"] == 4450
        roles = {r["role"]: r for r in body["by_role"]}
        assert "planner" in roles and "worker" in roles
        assert any(a["agent_id"] == "agent_1" for a in body["by_agent"])

    def test_unknown_returns_404(self, client: TestClient) -> None:
        """Project with no events → 404, not empty payload."""
        resp = client.get("/api/cost/projects/no_such_project/summary")
        assert resp.status_code == 404


@requires_marcus
class TestProjectNameEnrichment:
    """Picker names come from Marcus's projects.json, not MLflow.

    Marcus's main code path never opens an MLflow experiment, so the
    ``experiments`` table is usually empty. The project list endpoint
    must still surface human-readable names by reading Marcus's
    project registry (same source as Cato's main projects panel).
    """

    def test_list_projects_overlays_registry_name(
        self,
        store: Any,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A project_id with no MLflow row gets its name from projects.json.

        Simulates production: Marcus's main code path emits token events
        for a project without ever calling start_experiment. The
        ``experiments`` table has no row, so list_projects.project_name
        is NULL — the registry must fill it.
        """
        from src.cost_tracking.cost_store import TokenEvent

        from backend import cost_routes

        store.record_event(
            TokenEvent(
                run_id="exp_orphan",
                project_id="proj_no_mlflow",
                agent_id="planner",
                agent_role="planner",
                operation="parse_prd",
                provider="anthropic",
                model="claude-sonnet-4-6",
                input_tokens=100,
                output_tokens=50,
                request_id="req_orphan",
            )
        )

        fake_root = tmp_path / "marcus"
        state_dir = fake_root / "data" / "marcus_state"
        state_dir.mkdir(parents=True)
        (state_dir / "projects.json").write_text(
            json.dumps(
                {
                    "proj_no_mlflow": {
                        "id": "proj_no_mlflow",
                        "name": "registry-named-project",
                    },
                    "active_project": {"id": "proj_no_mlflow"},
                }
            )
        )
        monkeypatch.setattr(cost_routes, "_marcus_root", fake_root)
        cost_routes.clear_project_names_cache()

        resp = client.get("/api/cost/projects")
        assert resp.status_code == 200
        projects = resp.json()["projects"]
        orphan = next(p for p in projects if p["project_id"] == "proj_no_mlflow")
        assert orphan["project_name"] == "registry-named-project"

    def test_project_names_table_overrides_registry(
        self,
        store: Any,
        client: TestClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Snapshotted name (project_names table) wins over registry.

        After Marcus PR #515 lands, project_names captures the name as
        of when work was attributed. The dashboard should prefer that
        snapshot over the registry so renames / deletions don't break
        the display.
        """
        from src.cost_tracking.cost_store import TokenEvent

        from backend import cost_routes

        # Seed an event + a snapshotted name for a project not in the
        # registry.
        store.record_event(
            TokenEvent(
                run_id="exp_snap",
                project_id="snap_proj",
                agent_id="planner",
                agent_role="planner",
                operation="parse_prd",
                provider="anthropic",
                model="claude-sonnet-4-6",
                input_tokens=10,
                output_tokens=5,
                request_id="req_snap",
            )
        )
        store.upsert_project_name("snap_proj", "snapshotted-name")

        fake_root = tmp_path / "marcus"
        (fake_root / "data" / "marcus_state").mkdir(parents=True)
        (fake_root / "data" / "marcus_state" / "projects.json").write_text("{}")
        monkeypatch.setattr(cost_routes, "_marcus_root", fake_root)
        cost_routes.clear_project_names_cache()

        resp = client.get("/api/cost/projects")
        assert resp.status_code == 200
        names = {p["project_id"]: p["project_name"] for p in resp.json()["projects"]}
        assert names["snap_proj"] == "snapshotted-name"

    def test_cache_is_keyed_by_store_identity(
        self,
        store: Any,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Different stores don't share cache entries (Kaia review on #36).

        The module-level cache used to be a single dict; tests using
        ``app.dependency_overrides[get_store]`` with tmp stores could
        leak names across tests. Keying by ``id(store)`` isolates them.
        """
        from src.cost_tracking.cost_store import CostStore

        from backend import cost_routes

        # First store sees one name.
        store.upsert_project_name("p1", "from-store-1")
        cost_routes.clear_project_names_cache()
        names_a = cost_routes._load_project_names(store=store)
        assert names_a.get("p1") == "from-store-1"

        # Second store has a different (or no) name for the same id.
        # Without per-store keying, the cache would return store-1's
        # entry here.
        other_store = CostStore(db_path=tmp_path / "other.db")
        other_store.upsert_project_name("p1", "from-store-2")
        names_b = cost_routes._load_project_names(store=other_store)
        assert names_b.get("p1") == "from-store-2"
        # And the first store's cache still resolves to its own value.
        names_a_again = cost_routes._load_project_names(store=store)
        assert names_a_again.get("p1") == "from-store-1"

    def test_existing_mlflow_name_takes_precedence(
        self, client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An explicit project_name from start_experiment beats the registry."""
        from backend import cost_routes

        # The fixture seeds 'hangman' via the experiments table for proj_1.
        # If registry says something different, MLflow wins (user-explicit).
        fake_root = tmp_path / "marcus"
        state_dir = fake_root / "data" / "marcus_state"
        state_dir.mkdir(parents=True)
        (state_dir / "projects.json").write_text(
            json.dumps({"proj_1": {"id": "proj_1", "name": "different-name"}})
        )
        monkeypatch.setattr(cost_routes, "_marcus_root", fake_root)
        cost_routes.clear_project_names_cache()

        resp = client.get("/api/cost/projects")
        projects = resp.json()["projects"]
        proj_1 = next(p for p in projects if p["project_id"] == "proj_1")
        assert proj_1["project_name"] == "hangman"  # from experiments table


@requires_marcus
class TestSessionTurns:
    def test_returns_ordered_turns(self, client: TestClient) -> None:
        """Per-session turn trajectory is ordered by turn_index."""
        resp = client.get("/api/cost/sessions/s_1/turns")
        assert resp.status_code == 200
        turns = resp.json()["turns"]
        assert [t["turn_index"] for t in turns] == [1, 2]


# ---------------------------------------------------------------------------
# /api/cost/operations
# ---------------------------------------------------------------------------


@requires_marcus
class TestOperationsTaxonomy:
    """``GET /api/cost/operations`` exposes the Marcus operation catalog.

    The dashboard reads this once on load to populate per-event
    tooltips that explain what each LLM call does.
    """

    def test_returns_known_operations(self, client: TestClient) -> None:
        """Catalog includes the well-known keys used by call sites."""
        resp = client.get("/api/cost/operations")
        assert resp.status_code == 200
        ops = resp.json()["operations"]
        # Spot-check a handful of high-traffic operations
        for key in ("decompose_prd", "analyze_blocker", "extract_outcomes"):
            assert key in ops, f"missing {key} from taxonomy"
            entry = ops[key]
            assert entry["label"]
            assert entry["description"]
            assert entry["category"] in {
                "decomposition",
                "runtime",
                "monitoring",
                "other",
            }


# ---------------------------------------------------------------------------
# /api/cost/prices  (GET + POST)
# ---------------------------------------------------------------------------


@requires_marcus
class TestPrices:
    def test_get_current_prices(self, client: TestClient) -> None:
        """GET /prices returns the latest row per (model, provider)."""
        resp = client.get("/api/cost/prices")
        assert resp.status_code == 200
        prices = resp.json()["prices"]
        assert any(p["model"] == "claude-sonnet-4-6" for p in prices)

    def test_post_inserts_new_price_version(self, client: TestClient) -> None:
        """POST /prices inserts a versioned row that wins for new events."""
        new = {
            "model": "claude-sonnet-4-6",
            "provider": "anthropic",
            "effective_from": "2026-06-01T00:00:00+00:00",
            "input_per_million": 2.5,
            "output_per_million": 14.0,
            "cache_creation_per_million": 3.0,
            "cache_read_per_million": 0.25,
            "source": "cato_user",
        }
        resp = client.post("/api/cost/prices", json=new)
        assert resp.status_code == 200

        # New row should appear in /prices/history
        resp2 = client.get("/api/cost/prices/history?model=claude-sonnet-4-6")
        assert resp2.status_code == 200
        rows = resp2.json()["prices"]
        assert any(r["source"] == "cato_user" for r in rows)

    def test_duplicate_post_returns_409(self, client: TestClient) -> None:
        """Re-POSTing the same (model, provider, effective_from) returns 409."""
        new = {
            "model": "x",
            "provider": "y",
            "effective_from": "2026-06-02T00:00:00+00:00",
            "input_per_million": 1.0,
            "output_per_million": 1.0,
            "source": "cato_user",
        }
        first = client.post("/api/cost/prices", json=new)
        assert first.status_code == 200
        dup = client.post("/api/cost/prices", json=new)
        assert dup.status_code == 409


# ---------------------------------------------------------------------------
# /api/cost/export
# ---------------------------------------------------------------------------


@requires_marcus
class TestExport:
    def test_csv_export(self, client: TestClient) -> None:
        """CSV export returns all events for an experiment."""
        resp = client.get("/api/cost/export/exp_1")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/csv")
        rows = resp.text.strip().split("\n")
        # Header + 3 events
        assert len(rows) == 4
        assert "agent_id" in rows[0]
