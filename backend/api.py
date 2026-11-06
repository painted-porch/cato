"""
FastAPI backend for Cato Visualization Dashboard.

Provides unified snapshot API endpoint to serve Marcus data to the dashboard frontend.
Supports CORS for local development and production deployment.
"""

import asyncio
import csv
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

# Ensure we import from the local Cato cato_src directory, not elsewhere
cato_root = Path(__file__).parent.parent
if str(cato_root) not in sys.path:
    sys.path.insert(0, str(cato_root))

from fastapi import FastAPI, HTTPException, Query  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import Response, StreamingResponse  # noqa: E402

from cato_src.core.aggregator import Aggregator  # noqa: E402

# Configure logging to show INFO level from all loggers
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

# Phase 3: Historical analysis imports
# Marcus is installed as 'marcus-ai' package, allowing clean imports
# without namespace conflicts.

HISTORICAL_MODE_AVAILABLE = False
marcus_root = None
ProjectHistoryAggregator = None
ProjectHistoryQuery = None
PostProjectAnalyzer = None

try:
    # Find Marcus root directory (MARCUS_ROOT / config / auto-detect)
    from cato_src.core.marcus_paths import discover_marcus_root  # noqa: E402

    marcus_root = discover_marcus_root("src/analysis")
    if not marcus_root:
        raise ImportError("Marcus root directory not found")

    logger.info(f"Found Marcus at: {marcus_root}")

    # Add Marcus root to sys.path so "from src." imports work
    # No namespace conflict since Cato uses "cato_src" not "src"
    import sys

    if str(marcus_root) not in sys.path:
        sys.path.insert(0, str(marcus_root))

    logger.info("Importing Phase 1 & 2 analysis modules from Marcus...")

    # Import directly from Marcus's src directory
    from src.analysis.aggregator import (  # type: ignore[no-redef]
        ProjectHistoryAggregator,
    )
    from src.analysis.post_project_analyzer import (  # type: ignore[no-redef]
        PostProjectAnalyzer,
    )
    from src.analysis.query_api import ProjectHistoryQuery  # type: ignore[no-redef]

    HISTORICAL_MODE_AVAILABLE = True
    logger.info("✅ Historical analysis mode ENABLED")

except Exception as e:
    logger.error(
        f"❌ Historical analysis mode disabled: {type(e).__name__}: {e}", exc_info=True
    )

# Initialize historical analysis components if available
history_aggregator = None
history_query = None

if HISTORICAL_MODE_AVAILABLE and ProjectHistoryAggregator and ProjectHistoryQuery:
    try:
        history_aggregator = ProjectHistoryAggregator()
        history_query = ProjectHistoryQuery(history_aggregator)
        logger.info("✅ Historical analysis components initialized")
    except Exception as e:
        logger.error(f"Failed to initialize historical analysis components: {e}")
        HISTORICAL_MODE_AVAILABLE = False

# Load Marcus data path(s) and settings from config (for live mode aggregator).
# config.local.json (gitignored, written by the `cato` CLI) overrides config.json
# for the Marcus-path keys only; load_merged_config() ignores anything else in
# it. config_path points at the shared config.json — the /api/settings endpoints
# read and write history_cutoff_date there, and that is the only place it is
# honored, so the cutoff shown in the UI is the cutoff filtering the data.
config_path = Path(__file__).parent.parent / "config.json"
marcus_data_path_root = None
_extra_marcus_roots: list[Path] = []
_history_cutoff_date: Optional[str] = None
try:
    from cato_src.core.marcus_paths import (  # noqa: E402
        live_marcus_roots,
        load_merged_config,
        marcus_root_from_env,
    )

    config = load_merged_config()
    # MARCUS_ROOT overrides the configured path(s); otherwise the plural
    # marcus_data_paths list overrides the single marcus_data_path. The same
    # precedence the launcher and discover_marcus_root() use, so the live
    # snapshot and the historical modules always read the same install.
    _extra_marcus_roots = live_marcus_roots()
    if _extra_marcus_roots:
        marcus_data_path_root = _extra_marcus_roots[0]
        if marcus_root_from_env() is not None:
            logger.info(
                f"MARCUS_ROOT set: live mode uses {marcus_data_path_root} "
                "and ignores any configured Marcus data paths"
            )
        else:
            logger.info(
                f"Using {len(_extra_marcus_roots)} Marcus root(s) from config: "
                + ", ".join(str(r) for r in _extra_marcus_roots)
            )
    else:
        logger.info("No Marcus data path in config, using auto-detection")
    if os.environ.get("MARCUS_ROOT") and marcus_root_from_env() is None:
        logger.warning(
            f"MARCUS_ROOT='{os.environ['MARCUS_ROOT']}' has no data/ directory; "
            "ignoring it"
        )
    _history_cutoff_date = config.get("history_cutoff_date")
    if _history_cutoff_date:
        logger.info(f"History cutoff date: {_history_cutoff_date}")
except Exception as e:
    logger.warning(f"Could not load config: {e}, using auto-detection")


def _project_history_dir() -> Optional[Path]:
    """Resolve Marcus's ``data/project_history`` directory.

    Uses the discovered Marcus root, falling back to the config-derived
    data path root. Returns None when Marcus's location is unknown — no
    machine-specific path is assumed.
    """
    root = marcus_root or marcus_data_path_root
    return (root / "data" / "project_history") if root else None


# Initialize FastAPI app
app = FastAPI(
    title="Cato API",
    description="Backend API for Cato - Marcus parallelization visualization dashboard",
    version="2.0.0",
)

# Configure CORS - allow all localhost origins in development
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cost-tracking router (Marcus issue #409). Imports Marcus's CostStore /
# CostAggregator directly via cost_routes; available iff Marcus is on
# sys.path with src/cost_tracking present.
from backend.cost_routes import router as cost_router  # noqa: E402

app.include_router(cost_router)

# Initialize aggregator for snapshot API — multi-root if configured.
# Resolve a concrete Marcus root up front so a missing/misconfigured
# install fails loudly *at server startup* (see the startup event) rather
# than silently serving an empty dashboard.
#
# Import must not raise: the test suite imports this module without Marcus
# present. When resolution fails we record the reason in _aggregator_error
# and bind `aggregator` to a placeholder pointed at a non-existent root —
# the startup event refuses to serve before any request can use it, so the
# placeholder is never read. This keeps `aggregator` a plain Aggregator for
# every endpoint instead of an Optional that each would have to None-check.
_aggregator_error: Optional[str] = None

if _extra_marcus_roots:
    aggregator = Aggregator(
        marcus_roots=_extra_marcus_roots, history_cutoff_date=_history_cutoff_date
    )
else:
    from cato_src.core.marcus_paths import (  # noqa: E402
        discover_marcus_root as _discover,
    )

    _live_root = marcus_root or marcus_data_path_root or _discover("data")
    if _live_root is None:
        _aggregator_error = (
            "Cato could not locate the Marcus installation. Set the "
            "MARCUS_ROOT environment variable, or run `./cato` to configure "
            "it (this writes config.local.json)."
        )
        logger.error(_aggregator_error)
        aggregator = Aggregator(marcus_root=Path("/nonexistent/marcus-not-configured"))
    else:
        logger.info(f"Live-mode aggregator using Marcus root: {_live_root}")
        aggregator = Aggregator(
            marcus_root=_live_root, history_cutoff_date=_history_cutoff_date
        )

# Simple in-memory cache for snapshots (60s TTL for better performance)
snapshot_cache: Dict[str, tuple[Dict[str, Any], datetime]] = {}
CACHE_TTL_SECONDS = 60  # Increased from 30s to reduce cold loads


def prewarm_recent_projects() -> None:
    """
    Pre-warm cache for active project only.

    This runs in the background on startup to make the active project load instantly.
    Only pre-warms the currently active project to avoid loading old/archived projects
    which was causing slow startup with many historical projects.
    """
    try:
        logger.info("Starting cache pre-warming for active project...")

        # Get active project ID only
        active_project_id = aggregator.get_active_project_id()

        if not active_project_id:
            logger.info("No active project found, skipping pre-warming")
            return

        logger.info(f"Pre-warming active project: {active_project_id}")

        now = datetime.now(timezone.utc)

        try:
            # Create snapshot and cache it for active project only
            snapshot = aggregator.create_snapshot(
                project_id=active_project_id,
                view_mode="subtasks",
                timeline_scale_exponent=1.0,
            )
            snapshot_dict = snapshot.to_dict()

            # Cache with default view settings
            cache_key = f"{active_project_id}_subtasks_1.0"
            snapshot_cache[cache_key] = (snapshot_dict, now)

            logger.info(
                f"Pre-warmed cache for active project: {active_project_id[:40]}"
            )
        except Exception as e:
            logger.warning(
                f"Failed to pre-warm active project {active_project_id}: {e}"
            )

        logger.info("Cache pre-warming complete")
    except Exception as e:
        logger.error(f"Error pre-warming cache: {e}", exc_info=True)


def background_cache_refresh() -> None:
    """
    Background task to refresh cache for active projects.

    Runs every 45 seconds to keep cache warm before 60s TTL expires.
    """
    import time

    # Wait 10 seconds for initial pre-warming to complete
    time.sleep(10)

    while True:
        try:
            # Refresh cache for projects that are in cache and accessed recently
            now = datetime.now(timezone.utc)
            cache_keys_to_refresh = []

            for cache_key, (snapshot_dict, cache_time) in list(snapshot_cache.items()):
                age = (now - cache_time).total_seconds()
                # Refresh if cache is 45+ seconds old (before 60s expiry)
                if 45 <= age < CACHE_TTL_SECONDS:
                    cache_keys_to_refresh.append(cache_key)

            if cache_keys_to_refresh:
                logger.info(
                    "Background refresh: refreshing "
                    f"{len(cache_keys_to_refresh)} cached snapshots"
                )

                for cache_key in cache_keys_to_refresh:
                    try:
                        # Parse cache key: "{project_id}_subtasks_0.4"
                        parts = cache_key.rsplit("_", 2)
                        if len(parts) == 3:
                            project_id = parts[0]
                            view_mode = parts[1]
                            timeline_exp = float(parts[2])

                            # Refresh snapshot
                            snapshot = aggregator.create_snapshot(
                                project_id=project_id if project_id else None,
                                view_mode=view_mode,  # type: ignore[arg-type]
                                timeline_scale_exponent=timeline_exp,
                            )
                            snapshot_dict = snapshot.to_dict()
                            snapshot_cache[cache_key] = (snapshot_dict, now)

                            logger.debug(f"Refreshed cache for: {cache_key}")
                    except Exception as e:
                        logger.warning(f"Failed to refresh cache for {cache_key}: {e}")

            # Sleep for 15 seconds before next check
            time.sleep(15)

        except Exception as e:
            logger.error(f"Error in background cache refresh: {e}", exc_info=True)
            time.sleep(15)


@app.on_event("startup")  # type: ignore[misc]
async def startup_event() -> None:
    """Run background tasks on startup.

    Fails loudly if the Marcus installation could not be located: an
    observability dashboard with no data source should refuse to start,
    not serve an empty UI. Import-time resolution stays non-fatal so the
    test suite can import this module without Marcus present.
    """
    if _aggregator_error is not None:
        raise RuntimeError(_aggregator_error)

    import threading

    # DISABLED: Pre-warming was causing slow startup with many projects
    # Cache will be populated on first request instead
    # prewarm_thread = threading.Thread(target=prewarm_recent_projects, daemon=True)
    # prewarm_thread.start()
    logger.info("Pre-warming disabled - cache will populate on demand")

    # Start background cache refresh
    refresh_thread = threading.Thread(target=background_cache_refresh, daemon=True)
    refresh_thread.start()
    logger.info("Started background cache refresh")


@app.get("/")  # type: ignore[misc]
async def root() -> Dict[str, Any]:
    """
    Root endpoint with API information.

    Returns
    -------
    dict
        API information and status
    """
    return {
        "name": "Cato API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "/api/snapshot": "Get unified denormalized snapshot",
            "/api/projects": "Get list of all projects",
            "/api/export": "Export snapshot data (JSON or CSV bundle)",
            "/health": "Health check",
        },
    }


@app.get("/health")  # type: ignore[misc]
async def health() -> Dict[str, str]:
    """
    Health check endpoint.

    Returns
    -------
    dict
        Health status
    """
    return {"status": "healthy"}


def count_tasks_for_project(
    all_tasks: List[Dict[str, Any]], project_info: Dict[str, Any]
) -> int:
    """
    Count tasks that match a project using Planka fuzzy ID matching.

    This replicates the logic from aggregator._load_tasks() but operates on
    pre-loaded tasks for efficiency.
    """
    if "provider_config" not in project_info:
        return 0

    planka_project_id = project_info["provider_config"].get("project_id", "")
    planka_board_id = project_info["provider_config"].get("board_id", "")

    if not planka_project_id and not planka_board_id:
        return 0

    # Extract target prefixes (first 8 digits of Planka IDs)
    target_prefixes = []
    for id_to_check in [planka_board_id, planka_project_id]:
        if id_to_check and len(id_to_check) >= 8:
            try:
                target_prefixes.append(int(id_to_check[:8]))
            except ValueError:
                pass

    if not target_prefixes:
        return 0

    # Count matching tasks using fuzzy matching (±20 range)
    match_count = 0
    for task in all_tasks:
        task_id = str(task.get("id", ""))

        # Skip non-Planka IDs
        if not task_id or len(task_id) < 8 or not task_id[0].isdigit():
            continue

        try:
            id_prefix = int(task_id[:8])
            # Check if within ±20 of any target prefix
            for target_prefix in target_prefixes:
                distance = abs(id_prefix - target_prefix)
                if distance <= 20:
                    match_count += 1
                    break  # Found match, no need to check other prefixes
        except ValueError:
            # Fallback: try string prefix match
            for planka_id in [planka_board_id, planka_project_id]:
                if planka_id and task_id.startswith(planka_id[:8]):
                    match_count += 1
                    break

    return match_count


@app.get("/api/projects")  # type: ignore[misc]
async def get_projects() -> Dict[str, Any]:
    """
    Get list of projects that have tasks.

    Always includes the active project (even if it has no tasks yet) to ensure
    users can see what Marcus is currently working on. Other projects are only
    shown if they have at least one task to avoid showing empty projects.

    Returns
    -------
    dict
        List of projects with metadata, with active project always included
    """
    try:
        logger.info("Loading projects list")
        projects_data = aggregator._load_projects()
        active_project_id = aggregator.get_active_project_id()

        logger.info(f"Active project ID: {active_project_id}")

        # Load all tasks ONCE for efficiency
        all_tasks = aggregator._load_tasks(project_id=None)
        logger.info(f"Loaded {len(all_tasks)} tasks total for project filtering")

        # Show only "discovered" projects (currently on Planka board) + active project
        # "discovered" tag is set by Marcus when it finds projects on the Planka board
        projects_with_tasks = []
        active_project_included = False

        for p in projects_data:
            if "id" not in p:
                continue

            project_id = p.get("id", "")
            is_active = project_id == active_project_id

            # Check if project should appear in the list
            tags = p.get("tags", [])
            is_discovered = "discovered" in tags
            is_auto_created = "auto-created" in tags

            # Include if: active, discovered (Planka), or auto-created (SQLite)
            if is_active or is_discovered or is_auto_created:
                # Count tasks for this project using fuzzy matching
                # (Only checking ~8-9 projects, so this is fast)
                task_count = count_tasks_for_project(all_tasks, p)
                projects_with_tasks.append(
                    {
                        "id": project_id,
                        "name": p.get("name", project_id),
                        "created_at": p.get("created_at", ""),
                        "last_used": p.get("last_used"),
                        "description": p.get("description", ""),
                        "task_count": task_count,
                        "is_active": is_active,  # Flag for frontend to highlight
                    }
                )

                if is_active:
                    active_project_included = True

        active_status = "included" if active_project_included else "not found"
        logger.info(
            f"Filtered to {len(projects_with_tasks)}/"
            f"{len(projects_data)} projects "
            f"(active={active_status})"
        )

        # Sort: active project first, then by creation date (most recent first)
        projects_with_tasks.sort(
            key=lambda p: (not p.get("is_active", False), p.get("created_at", "")),
            reverse=True,
        )
        logger.info("Sorted projects (active first, then by creation date)")

        return {"projects": projects_with_tasks}
    except Exception as e:
        logger.error(f"Error loading projects: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error loading projects: {str(e)}")


@app.get("/api/settings")  # type: ignore[misc]
async def get_settings() -> Dict[str, Any]:
    """Return current Cato settings from config.json."""
    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)
        return {
            "history_cutoff_date": cfg.get("history_cutoff_date"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/settings")  # type: ignore[misc]
async def update_settings(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update Cato settings in config.json and reinitialize the aggregator.

    Accepts: {"history_cutoff_date": "YYYY-MM-DD" | null}
    """
    global aggregator

    try:
        with open(config_path, "r") as f:
            cfg = json.load(f)

        if "history_cutoff_date" in body:
            cfg["history_cutoff_date"] = body["history_cutoff_date"]

        with open(config_path, "w") as f:
            json.dump(cfg, f, indent=2)

        # Reinitialize aggregator with new cutoff, reusing the Marcus
        # root(s) already resolved at startup so we don't re-run discovery.
        if _aggregator_error is not None:
            raise HTTPException(status_code=503, detail=_aggregator_error)

        new_cutoff: Optional[str] = cfg.get("history_cutoff_date")
        if _extra_marcus_roots:
            aggregator = Aggregator(
                marcus_roots=_extra_marcus_roots, history_cutoff_date=new_cutoff
            )
        else:
            aggregator = Aggregator(
                marcus_root=aggregator.marcus_root, history_cutoff_date=new_cutoff
            )

        # Invalidate snapshot cache so next request re-reads with new cutoff
        snapshot_cache.clear()

        logger.info(f"Settings updated: history_cutoff_date={new_cutoff}")
        return {"history_cutoff_date": new_cutoff}

    except HTTPException:
        raise  # propagate 503 (Marcus missing) without masking it as 500
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/snapshot")  # type: ignore[misc]
async def get_snapshot(
    project_id: Optional[str] = Query(None, description="Project ID to filter by"),
    view: Literal["subtasks", "parents", "all"] = Query(
        "subtasks", description="View mode: 'subtasks', 'parents', or 'all'"
    ),
    timeline_scale_exponent: float = Query(
        0.4, description="Power scale exponent for timeline (default 0.4)"
    ),
    use_cache: bool = Query(True, description="Use cached snapshot if available"),
) -> Dict[str, Any]:
    """
    Get unified denormalized snapshot.

    This endpoint replaces the multi-layer transformation pipeline with a single
    aggregation that returns all data pre-joined and pre-calculated.

    Parameters
    ----------
    project_id : Optional[str]
        Specific project to snapshot (None = all projects)
    view : str
        View mode: 'subtasks' (default), 'parents', or 'all'
    timeline_scale_exponent : float
        Power scale exponent for timeline transformation (default 0.4)
    use_cache : bool
        Whether to use cached snapshot if available (default True)

    Returns
    -------
    dict
        Complete denormalized snapshot with:
        - snapshot_id: Unique snapshot identifier
        - snapshot_version: Version number
        - timestamp: When snapshot was created
        - project_id: Filtered project (or None for all)
        - project_name: Project name
        - view_mode: Active view mode
        - tasks: List of denormalized tasks with embedded relationships
        - agents: List of agents with pre-calculated metrics
        - messages: List of messages with embedded context
        - timeline_events: List of events with embedded context
        - metrics: Pre-calculated project metrics
        - start_time: Timeline start
        - end_time: Timeline end
        - duration_minutes: Total duration
        - task_dependency_graph: Pre-built dependency graph
        - agent_communication_graph: Pre-built communication graph
    """
    try:
        # Create cache key
        cache_key = f"{project_id or 'all'}_{view}_{timeline_scale_exponent}"

        # Check cache
        if use_cache and cache_key in snapshot_cache:
            cached_snapshot, cache_time = snapshot_cache[cache_key]
            age = (datetime.now(timezone.utc) - cache_time).total_seconds()

            if age < CACHE_TTL_SECONDS:
                logger.info(f"Returning cached snapshot (age: {age:.1f}s): {cache_key}")
                return cached_snapshot

        # Create new snapshot
        logger.info(f"Creating new snapshot: {cache_key}")
        snapshot = aggregator.create_snapshot(
            project_id=project_id,
            view_mode=view,
            timeline_scale_exponent=timeline_scale_exponent,
        )

        # Convert to dict for JSON serialization
        snapshot_dict = snapshot.to_dict()

        # Cache the result
        snapshot_cache[cache_key] = (snapshot_dict, datetime.now(timezone.utc))

        # Clean up old cache entries (simple cleanup)
        if len(snapshot_cache) > 100:
            # Remove oldest 50% of entries
            sorted_keys = sorted(
                snapshot_cache.keys(),
                key=lambda k: snapshot_cache[k][1],
            )
            for key in sorted_keys[: len(sorted_keys) // 2]:
                del snapshot_cache[key]

        return snapshot_dict

    except Exception as e:
        logger.error(f"Error creating snapshot: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error creating snapshot: {str(e)}"
        )


@app.get("/api/export")  # type: ignore[misc]
async def export_snapshot(
    project_id: Optional[str] = Query(None, description="Project ID to export"),
    format: Literal["json", "csv"] = Query(
        "json", description="Export format: 'json' or 'csv'"
    ),
) -> Response:
    """
    Export snapshot data in JSON or CSV format.

    Parameters
    ----------
    project_id : Optional[str]
        Specific project to export (None = all projects)
    format : str
        Export format: 'json' (default) or 'csv' (bundle)

    Returns
    -------
    Response
        File download response (JSON or ZIP)
    """
    try:
        logger.info(f"Creating export: project_id={project_id}, format={format}")

        # Create snapshot with full data
        snapshot = aggregator.create_snapshot(
            project_id=project_id,
            view_mode="subtasks",
            timeline_scale_exponent=1.0,
        )

        # Get project name for filename
        project_name = snapshot.project_name.replace(" ", "_").replace("/", "_")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        if format == "json":
            # Export as pretty-printed JSON
            snapshot_dict = snapshot.to_dict()
            json_content = json.dumps(snapshot_dict, indent=2, ensure_ascii=False)

            filename = f"cato_export_{project_name}_{timestamp}.json"

            return Response(
                content=json_content,
                media_type="application/json",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        elif format == "csv":
            # Export as CSV bundle (ZIP file)
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                # 1. Export conversations.csv
                if snapshot.messages:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(
                        [
                            "id",
                            "timestamp",
                            "type",
                            "from_agent_id",
                            "from_agent_name",
                            "to_agent_id",
                            "to_agent_name",
                            "task_id",
                            "task_name",
                            "message",
                            "parent_message_id",
                        ]
                    )
                    for msg in snapshot.messages:
                        writer.writerow(
                            [
                                msg.id,
                                msg.timestamp,
                                msg.type,
                                msg.from_agent_id,
                                msg.from_agent_name,
                                msg.to_agent_id,
                                msg.to_agent_name,
                                msg.task_id or "",
                                msg.task_name or "",
                                msg.message,
                                msg.parent_message_id or "",
                            ]
                        )
                    zip_file.writestr("conversations.csv", csv_buffer.getvalue())

                # 2. Export tasks.csv
                if snapshot.tasks:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(
                        [
                            "id",
                            "name",
                            "description",
                            "status",
                            "progress_percent",
                            "assigned_agent_id",
                            "assigned_agent_name",
                            "parent_task_id",
                            "parent_task_name",
                            "project_id",
                            "project_name",
                            "created_at",
                            "started_at",
                            "completed_at",
                            "updated_at",
                            "priority",
                            "estimated_hours",
                            "actual_hours",
                        ]
                    )
                    for task in snapshot.tasks:
                        writer.writerow(
                            [
                                task.id,
                                task.name,
                                task.description,
                                task.status,
                                task.progress_percent,
                                task.assigned_agent_id or "",
                                task.assigned_agent_name or "",
                                task.parent_task_id or "",
                                task.parent_task_name or "",
                                task.project_id,
                                task.project_name,
                                task.created_at,
                                task.started_at or "",
                                task.completed_at or "",
                                task.updated_at,
                                task.priority,
                                task.estimated_hours,
                                task.actual_hours,
                            ]
                        )
                    zip_file.writestr("tasks.csv", csv_buffer.getvalue())

                # 3. Export agents.csv
                if snapshot.agents:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(
                        [
                            "id",
                            "name",
                            "role",
                            "skills",
                            "completed_tasks_count",
                            "total_hours_worked",
                            "average_task_duration_hours",
                            "performance_score",
                            "capacity_utilization",
                            "messages_sent",
                            "messages_received",
                            "blockers_reported",
                        ]
                    )
                    for agent in snapshot.agents:
                        writer.writerow(
                            [
                                agent.id,
                                agent.name,
                                agent.role,
                                ";".join(agent.skills) if agent.skills else "",
                                agent.completed_tasks_count,
                                agent.total_hours_worked,
                                agent.average_task_duration_hours,
                                agent.performance_score,
                                agent.capacity_utilization,
                                agent.messages_sent,
                                agent.messages_received,
                                agent.blockers_reported,
                            ]
                        )
                    zip_file.writestr("agents.csv", csv_buffer.getvalue())

                # 4. Export task_dependencies.csv
                if snapshot.task_dependency_graph:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(["source_task_id", "target_task_id"])
                    for source_id, targets in snapshot.task_dependency_graph.items():
                        for target_id in targets:
                            writer.writerow([source_id, target_id])
                    zip_file.writestr("task_dependencies.csv", csv_buffer.getvalue())

                # 5. Export agent_communications.csv
                if snapshot.agent_communication_graph:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(["from_agent_id", "to_agent_id", "message_count"])

                    # Count messages between agents
                    comm_counts: Dict[tuple[str, str], int] = {}
                    for msg in snapshot.messages:
                        if msg.from_agent_id and msg.to_agent_id:
                            key = (msg.from_agent_id, msg.to_agent_id)
                            comm_counts[key] = comm_counts.get(key, 0) + 1

                    for (from_id, to_id), count in comm_counts.items():
                        writer.writerow([from_id, to_id, count])
                    zip_file.writestr("agent_communications.csv", csv_buffer.getvalue())

                # 6. Export timeline_events.csv
                if snapshot.timeline_events:
                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(
                        [
                            "id",
                            "timestamp",
                            "event_type",
                            "agent_id",
                            "agent_name",
                            "task_id",
                            "task_name",
                            "data",
                        ]
                    )
                    for event in snapshot.timeline_events:
                        writer.writerow(
                            [
                                event.id,
                                event.timestamp,
                                event.event_type,
                                event.agent_id or "",
                                event.agent_name or "",
                                event.task_id or "",
                                event.task_name or "",
                                json.dumps(event.data) if event.data else "",
                            ]
                        )
                    zip_file.writestr("timeline_events.csv", csv_buffer.getvalue())

            # Reset buffer position
            zip_buffer.seek(0)

            filename = f"cato_export_{project_name}_{timestamp}.zip"

            return Response(
                content=zip_buffer.getvalue(),
                media_type="application/zip",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )

        else:
            raise HTTPException(status_code=400, detail=f"Invalid format: {format}")

    except Exception as e:
        logger.error(f"Error creating export: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error creating export: {str(e)}")


# ============================================================================
# Phase 3: Historical Analysis API Endpoints
# ============================================================================

# Initialize historical analysis components (if available)
if HISTORICAL_MODE_AVAILABLE:
    assert ProjectHistoryAggregator is not None
    assert ProjectHistoryQuery is not None
    history_aggregator = ProjectHistoryAggregator()
    history_query = ProjectHistoryQuery(history_aggregator)

# Import project registry for active project filtering
project_registry = None
try:
    if marcus_root:
        sys.path.insert(0, str(marcus_root))
        from src.core.project_registry import ProjectRegistry

        project_registry = ProjectRegistry()
        logger.info("✅ Project registry loaded for historical filtering")
except Exception as e:
    logger.warning(f"Could not load project registry: {e}")


@app.get("/api/artifacts/{artifact_id}/content")  # type: ignore[misc]
async def get_artifact_content(artifact_id: str) -> Dict[str, Any]:
    """
    Get artifact file content for preview.

    Serves the content of an artifact file identified by artifact_id.
    Validates that the path is safe and within allowed directories.

    PERFORMANCE: Uses lightweight artifact lookup instead of full snapshot.
    """
    try:
        # OPTIMIZED: Only load artifacts, not full snapshot (6,970+ tasks)
        artifact = aggregator.get_artifact_by_id(artifact_id)

        if not artifact:
            raise HTTPException(
                status_code=404, detail=f"Artifact {artifact_id} not found"
            )

        # Security: Validate path is safe
        if not artifact.absolute_path:
            raise HTTPException(
                status_code=404, detail=f"Artifact {artifact_id} has no file path"
            )
        artifact_path = Path(artifact.absolute_path)

        # Check if file exists
        if not artifact_path.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Artifact file not found: {artifact.absolute_path}",
            )

        # Check if it's actually a file (not a directory)
        if not artifact_path.is_file():
            raise HTTPException(status_code=400, detail="Artifact path is not a file")

        # Check file size (limit to 10MB for preview)
        file_size = artifact_path.stat().st_size
        if file_size > 10 * 1024 * 1024:  # 10MB
            raise HTTPException(
                status_code=413, detail="File too large for preview (>10MB)"
            )

        # Read file content
        try:
            content = artifact_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # Binary file, return base64 encoded
            import base64

            content = base64.b64encode(artifact_path.read_bytes()).decode("ascii")
            return {
                "success": True,
                "artifact_id": artifact_id,
                "filename": artifact.filename,
                "artifact_type": artifact.artifact_type,
                "content": content,
                "encoding": "base64",
                "size_bytes": file_size,
            }

        return {
            "success": True,
            "artifact_id": artifact_id,
            "filename": artifact.filename,
            "artifact_type": artifact.artifact_type,
            "content": content,
            "encoding": "utf-8",
            "size_bytes": file_size,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error loading artifact content: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tasks/{task_id}/conversation")  # type: ignore[misc]
async def get_task_conversation(task_id: str) -> Dict[str, Any]:
    """Get the full conversation timeline for a specific task.

    Returns a unified chronological timeline merging:
    - Assignment and instructions
    - Kanban comments (progress, blockers, decisions)
    - Events (from marcus.db)
    - Task outcome (completion data)

    Parameters
    ----------
    task_id : str
        The task ID (UUID from kanban.db or Planka numeric ID)

    Returns
    -------
    dict
        Task metadata and chronological timeline of all interactions.
    """
    import sqlite3

    try:
        marcus_root = aggregator.marcus_root
        timeline: List[Dict[str, Any]] = []

        # 1. Task metadata from marcus.db
        task_meta: Dict[str, Any] = {}
        marcus_db = marcus_root / "data" / "marcus.db"
        if marcus_db.exists():
            conn = sqlite3.connect(str(marcus_db))

            # Task metadata
            row = conn.execute(
                "SELECT data FROM persistence "
                "WHERE collection = 'task_metadata' AND key = ?",
                (task_id,),
            ).fetchone()
            if row:
                task_meta = json.loads(row[0])

            # Events for this task
            rows = conn.execute(
                "SELECT data FROM persistence "
                "WHERE collection = 'events' "
                "AND data LIKE ?",
                (f"%{task_id}%",),
            ).fetchall()
            for r in rows:
                try:
                    event = json.loads(r[0])
                    event_type = event.get("event_type", "event")
                    timestamp = event.get(
                        "timestamp",
                        event.get("created_at", ""),
                    )
                    timeline.append(
                        {
                            "type": event_type,
                            "at": timestamp,
                            "data": event.get("data", {}),
                        }
                    )
                except json.JSONDecodeError:
                    pass

            # Task outcome
            rows = conn.execute(
                "SELECT data FROM persistence "
                "WHERE collection = 'task_outcomes' "
                "AND data LIKE ?",
                (f"%{task_id}%",),
            ).fetchall()
            for r in rows:
                try:
                    outcome = json.loads(r[0])
                    if outcome.get("task_id") == task_id:
                        timeline.append(
                            {
                                "type": "completed",
                                "at": outcome.get("completed_at", ""),
                                "data": {
                                    "agent_id": outcome.get("agent_id"),
                                    "actual_hours": outcome.get("actual_hours", 0),
                                    "success": outcome.get("success"),
                                    "started_at": outcome.get("started_at"),
                                    "completed_at": outcome.get("completed_at"),
                                },
                            }
                        )
                except json.JSONDecodeError:
                    pass

            conn.close()

        # 2. Kanban comments (progress, blockers, decisions)
        kanban_db = marcus_root / "data" / "kanban.db"
        if kanban_db.exists():
            conn = sqlite3.connect(str(kanban_db))
            rows = conn.execute(
                "SELECT content, author, created_at "
                "FROM comments WHERE task_id = ? "
                "ORDER BY created_at",
                (task_id,),
            ).fetchall()
            for r in rows:
                content = r[0]
                # Classify comment type by emoji prefix
                if "assigned" in content.lower():
                    ctype = "assigned"
                elif "BLOCKER" in content:
                    ctype = "blocker"
                elif "Progress:" in content:
                    ctype = "progress"
                elif "ARCHITECTURAL DECISION" in content:
                    ctype = "decision"
                elif "RECOVERY" in content:
                    ctype = "recovery"
                else:
                    ctype = "comment"

                timeline.append(
                    {
                        "type": ctype,
                        "at": r[2],
                        "content": content,
                        "author": r[1],
                    }
                )

            # Also get task status from kanban.db
            task_row = conn.execute(
                "SELECT name, status, assigned_to, priority, "
                "estimated_hours, description "
                "FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if task_row:
                task_meta.update(
                    {
                        "name": task_row[0],
                        "status": task_row[1],
                        "assigned_to": task_row[2],
                        "priority": task_row[3],
                        "estimated_hours": task_row[4],
                        "description": task_row[5],
                    }
                )

            conn.close()

        # 3. Sort timeline chronologically
        timeline.sort(key=lambda x: x.get("at", "") or "")

        return {
            "task_id": task_id,
            "task_name": task_meta.get("name", task_meta.get("task_name", "Unknown")),
            "assigned_to": task_meta.get("assigned_to"),
            "status": task_meta.get("status", "unknown"),
            "priority": task_meta.get("priority"),
            "estimated_hours": task_meta.get("estimated_hours"),
            "description": task_meta.get("description", ""),
            "timeline": timeline,
        }

    except Exception as e:
        logger.error(f"Error loading task conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/historical/projects")  # type: ignore[misc]
async def list_historical_projects() -> Any:
    """
    List historical projects for ACTIVE projects only (default view).

    This endpoint returns only projects that exist in the active project
    registry, providing a clean, focused list of current projects.

    For accessing archived/deleted projects, use /api/historical/projects/all

    Returns
    -------
    {
      "projects": [
        {
          "project_id": "marcus_proj_123",
          "project_name": "Task Management API",
          "total_tasks": 24,
          "completed_tasks": 22,
          "completion_rate": 91.7,
          "blocked_tasks": 1,
          "total_decisions": 15,
          "project_duration_hours": 48.5,
          "is_active": true
        }
      ],
      "count": 2,
      "view": "active"
    }
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Historical analysis mode not available (missing dependencies)",
        )
    assert history_query is not None

    try:
        # Get active project IDs from registry
        active_project_ids = set()
        if project_registry:
            try:
                await project_registry.initialize()
                active_projects = await project_registry.list_projects()
                active_project_ids = {p.id for p in active_projects}
                logger.info(
                    f"Found {len(active_project_ids)} active projects in registry"
                )
            except Exception as e:
                logger.warning(
                    f"Could not load project registry, showing all projects: {e}"
                )

        # Find all project IDs from persistence layer
        history_dir = _project_history_dir()

        active_projects_data = []
        if history_dir and history_dir.exists():
            logger.info(f"Scanning for historical projects in {history_dir}")

            # OPTIMIZATION: Only load summaries for active projects
            # Instead of loading all 26 projects and filtering, we filter FIRST
            if active_project_ids:
                # Load only active projects (fast path)
                for project_id in active_project_ids:
                    project_dir = history_dir / project_id
                    if project_dir.exists() and project_dir.is_dir():
                        try:
                            summary = await history_query.get_project_summary(
                                project_id
                            )
                            summary["is_active"] = True
                            active_projects_data.append(summary)
                        except Exception as e:
                            logger.warning(f"Error loading project {project_id}: {e}")
                logger.info(
                    f"Loaded {len(active_projects_data)} active projects "
                    f"(from {len(active_project_ids)} in registry)"
                )
            else:
                # No registry available - load all projects (slow path)
                logger.warning("No registry available, loading all historical projects")
                for project_dir in history_dir.iterdir():
                    if project_dir.is_dir():
                        project_id = project_dir.name
                        try:
                            summary = await history_query.get_project_summary(
                                project_id
                            )
                            summary["is_active"] = True
                            active_projects_data.append(summary)
                        except Exception as e:
                            logger.warning(f"Error loading project {project_id}: {e}")
        else:
            logger.warning(f"Project history directory not found: {history_dir}")

        logger.info(f"Returning {len(active_projects_data)} active projects")
        return {
            "projects": active_projects_data,
            "count": len(active_projects_data),
            "view": "active",
        }

    except Exception as e:
        logger.error(f"Error listing historical projects: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error listing historical projects: {str(e)}"
        )


@app.get("/api/historical/projects/all")  # type: ignore[misc]
async def list_all_historical_projects(
    search: Optional[str] = Query(
        None, description="Search projects by name (case-insensitive)"
    )
) -> Any:
    """
    List ALL historical projects including archived ones (archive browser).

    Returns all projects with active/archived status, optionally filtered by search.
    Use this endpoint for the archive browser UI.

    Parameters
    ----------
    search : Optional[str]
        Search term to filter project names

    Returns
    -------
    {
      "active": [
        {
          "project_id": "...",
          "project_name": "...",
          "is_active": true,
          "status": "active",
          ...
        }
      ],
      "archived": [
        {
          "project_id": "...",
          "project_name": "...",
          "is_active": false,
          "status": "archived",
          ...
        }
      ],
      "total": 5,
      "view": "all"
    }
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Historical analysis mode not available (missing dependencies)",
        )
    assert history_query is not None

    try:
        # Get active project IDs from registry
        active_project_ids = set()
        if project_registry:
            try:
                await project_registry.initialize()
                active_projects = await project_registry.list_projects()
                active_project_ids = {p.id for p in active_projects}
                logger.info(
                    f"Found {len(active_project_ids)} active projects in registry"
                )
            except Exception as e:
                logger.warning(f"Could not load project registry: {e}")

        # Find all historical projects
        history_dir = _project_history_dir()

        all_projects = []
        if history_dir and history_dir.exists():
            logger.info(f"Scanning for all historical projects in {history_dir}")
            for project_dir in history_dir.iterdir():
                if project_dir.is_dir():
                    project_id = project_dir.name
                    try:
                        summary = await history_query.get_project_summary(project_id)

                        # Apply search filter if provided
                        if search:
                            project_name = summary.get("project_name", "").lower()
                            if search.lower() not in project_name:
                                continue

                        # Add status fields
                        is_active = project_id in active_project_ids
                        summary["is_active"] = is_active
                        summary["status"] = "active" if is_active else "archived"

                        all_projects.append(summary)
                    except Exception as e:
                        logger.warning(f"Error loading project {project_id}: {e}")
        else:
            logger.warning(f"Project history directory not found: {history_dir}")

        # Separate into active and archived
        active = [p for p in all_projects if p["is_active"]]
        archived = [p for p in all_projects if not p["is_active"]]

        logger.info(
            f"Found {len(active)} active and {len(archived)} archived projects"
            + (f" (filtered by '{search}')" if search else "")
        )

        return {
            "active": active,
            "archived": archived,
            "total": len(all_projects),
            "view": "all",
        }

    except Exception as e:
        logger.error(f"Error listing all historical projects: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error listing all historical projects: {str(e)}"
        )


@app.get("/api/historical/projects/stream")  # type: ignore[misc]
async def stream_historical_projects_list() -> StreamingResponse:
    """
    Stream historical projects list with progress updates.

    Uses Server-Sent Events to show progress as each project is loaded.
    Shows "Loading project 1 of 25...", etc.
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Historical analysis mode not available (missing dependencies)",
        )
    assert history_query is not None

    async def event_generator() -> Any:
        """Generate SSE events for project loading progress."""
        try:
            # Step 1: Scan for project directories
            event_data = json.dumps(
                {"type": "log", "message": "📁 Scanning for historical projects..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            history_dir = _project_history_dir()

            if not history_dir or not history_dir.exists():
                event_data = json.dumps(
                    {
                        "type": "error",
                        "message": "Project history directory "
                        f"not found: {history_dir}",
                    }
                )
                yield f"data: {event_data}\n\n"
                return

            # Count project directories
            project_dirs = [d for d in history_dir.iterdir() if d.is_dir()]
            total_projects = len(project_dirs)

            event_data = json.dumps(
                {
                    "type": "log",
                    "message": f"✓ Found {total_projects} project directories",
                }
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            # Step 2: Load each project with progress
            projects = []
            for idx, project_dir in enumerate(project_dirs, 1):
                project_id = project_dir.name

                # Show progress
                event_data = json.dumps(
                    {
                        "type": "progress",
                        "message": f"Loading project {idx} of {total_projects}...",
                        "current": idx,
                        "total": total_projects,
                    }
                )
                yield f"data: {event_data}\n\n"
                await asyncio.sleep(0.05)

                try:
                    summary = await history_query.get_project_summary(project_id)
                    projects.append(summary)

                    # Show success for this project
                    project_name = summary.get("project_name", project_id[:8])
                    event_data = json.dumps(
                        {"type": "log", "message": f"  ✓ Loaded: {project_name}"}
                    )
                    yield f"data: {event_data}\n\n"
                    await asyncio.sleep(0.05)

                except Exception as e:
                    logger.warning(f"Error loading project {project_id}: {e}")
                    event_data = json.dumps(
                        {
                            "type": "log",
                            "message": f"  ⚠️  Skipped: {project_id[:8]} (error)",
                        }
                    )
                    yield f"data: {event_data}\n\n"
                    await asyncio.sleep(0.05)

            # Step 3: Send completion with data
            event_data = json.dumps(
                {
                    "type": "complete",
                    "message": f"✓ Loaded {len(projects)} projects",
                    "data": {"projects": projects},
                }
            )
            yield f"data: {event_data}\n\n"

        except Exception as e:
            logger.error(f"Error streaming historical projects: {e}", exc_info=True)
            event_data = json.dumps(
                {"type": "error", "message": f"Error loading projects: {str(e)}"}
            )
            yield f"data: {event_data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/historical/projects/{project_id}")  # type: ignore[misc]
async def get_project_history(project_id: str) -> Any:
    """
    Get complete project history (raw data only, no LLM analysis).

    Fast endpoint for browsing project data.
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Historical analysis mode not available"
        )
    assert history_query is not None

    try:
        history = await history_query.get_project_history(project_id)

        return {
            "project_id": history.project_id,
            "snapshot": history.snapshot.to_dict() if history.snapshot else None,
            "tasks": [t.to_dict() for t in history.tasks],
            "agents": [a.to_dict() for a in history.agents],
            "decisions": [d.to_dict() for d in history.decisions],
            "artifacts": [a.to_dict() for a in history.artifacts],
            "timeline_count": len(history.timeline),
        }

    except Exception as e:
        logger.error(f"Error loading project history: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error loading project history: {str(e)}"
        )


@app.get("/api/historical/projects/{project_id}/analysis")  # type: ignore[misc]
async def get_project_analysis(project_id: str) -> Any:
    """
    Run complete LLM-powered post-project analysis.

    This is the heavy endpoint - may take 10-30 seconds.
    Includes all Phase 2 analyzers.
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Historical analysis mode not available"
        )
    assert history_query is not None
    assert PostProjectAnalyzer is not None

    try:
        logger.info(f"Starting analysis for project {project_id}")

        # Load project history (Phase 1)
        history = await history_query.get_project_history(project_id)

        # Get summary stats from Phase 1 (always available)
        summary = await history_query.get_project_summary(project_id)

        # Try Phase 2 analysis - may fail if project has no data
        phase2_success = False
        analysis = None
        phase2_error = None

        try:
            analyzer = PostProjectAnalyzer()
            analysis = await analyzer.analyze_project(
                project_id=project_id,
                tasks=history.tasks,
                decisions=history.decisions,
            )
            phase2_success = True
            logger.info(f"Phase 2 analysis complete for project {project_id}")
        except Exception as e:
            logger.warning(f"Phase 2 analysis failed for {project_id}: {e}")
            phase2_error = str(e)

        # Return Phase 1 data always, Phase 2 data if available
        result = {
            # Phase 1 summary stats (always present)
            "project_id": project_id,
            "project_name": summary["project_name"],
            "total_tasks": summary["total_tasks"],
            "completed_tasks": summary["completed_tasks"],
            "completion_rate": summary["completion_rate"],
            "blocked_tasks": summary["blocked_tasks"],
            "total_decisions": summary["total_decisions"],
            "total_artifacts": summary.get("total_artifacts", 0),
            "active_agents": summary.get("active_agents", 0),
            "project_duration_hours": summary["project_duration_hours"],
        }

        # Add Phase 2 data if analysis succeeded
        if phase2_success and analysis:
            result.update(
                {
                    # Phase 2 analysis results
                    "analysis_timestamp": analysis.analysis_timestamp.isoformat(),
                    "summary": analysis.summary,
                    "requirement_divergences": [
                        {
                            "task_id": rd.task_id,
                            "fidelity_score": rd.fidelity_score,
                            "divergences": [
                                {
                                    "requirement": d.requirement,
                                    "implementation": d.implementation,
                                    "severity": d.severity,
                                    "impact": d.impact,
                                    "citation": d.citation,
                                }
                                for d in rd.divergences
                            ],
                            "recommendations": rd.recommendations,
                        }
                        for rd in analysis.requirement_divergences
                    ],
                    "decision_impacts": [
                        {
                            "decision_id": di.decision_id,
                            "impact_chains": [
                                {
                                    "decision_summary": ic.decision_summary,
                                    "direct_impacts": ic.direct_impacts,
                                    "indirect_impacts": ic.indirect_impacts,
                                    "depth": ic.depth,
                                    "citation": ic.citation,
                                }
                                for ic in di.impact_chains
                            ],
                            "unexpected_impacts": [
                                {
                                    "affected_task": ui.affected_task_name,
                                    "anticipated": ui.anticipated,
                                    "actual_impact": ui.actual_impact,
                                    "severity": ui.severity,
                                }
                                for ui in di.unexpected_impacts
                            ],
                            "recommendations": di.recommendations,
                        }
                        for di in analysis.decision_impacts
                    ],
                    "instruction_quality_issues": [
                        {
                            "task_id": iq.task_id,
                            "quality_scores": {
                                "clarity": iq.quality_scores.clarity,
                                "completeness": iq.quality_scores.completeness,
                                "specificity": iq.quality_scores.specificity,
                                "overall": iq.quality_scores.overall,
                            },
                            "ambiguity_issues": [
                                {
                                    "aspect": ai.ambiguous_aspect,
                                    "evidence": ai.evidence,
                                    "consequence": ai.consequence,
                                    "severity": ai.severity,
                                }
                                for ai in iq.ambiguity_issues
                            ],
                            "recommendations": iq.recommendations,
                        }
                        for iq in analysis.instruction_quality_issues
                    ],
                    "failure_diagnoses": [
                        {
                            "task_id": fd.task_id,
                            "failure_causes": [
                                {
                                    "category": fc.category,
                                    "root_cause": fc.root_cause,
                                    "contributing_factors": fc.contributing_factors,
                                    "evidence": fc.evidence,
                                }
                                for fc in fd.failure_causes
                            ],
                            "prevention_strategies": [
                                {
                                    "strategy": ps.strategy,
                                    "rationale": ps.rationale,
                                    "effort": ps.effort,
                                    "priority": ps.priority,
                                }
                                for ps in fd.prevention_strategies
                            ],
                            "lessons_learned": fd.lessons_learned,
                        }
                        for fd in analysis.failure_diagnoses
                    ],
                    "task_redundancy": (
                        {
                            "project_id": analysis.task_redundancy.project_id,
                            "redundant_pairs": [
                                {
                                    "task_1_id": rp.task_1_id,
                                    "task_1_name": rp.task_1_name,
                                    "task_2_id": rp.task_2_id,
                                    "task_2_name": rp.task_2_name,
                                    "overlap_score": rp.overlap_score,
                                    "evidence": rp.evidence,
                                    "time_wasted": rp.time_wasted,
                                }
                                for rp in analysis.task_redundancy.redundant_pairs
                            ],
                            "redundancy_score": (
                                analysis.task_redundancy.redundancy_score
                            ),
                            "total_time_wasted": (
                                analysis.task_redundancy.total_time_wasted
                            ),
                            "over_decomposition_detected": (
                                analysis.task_redundancy.over_decomposition_detected
                            ),
                            "recommended_complexity": (
                                analysis.task_redundancy.recommended_complexity
                            ),
                            "raw_data": (analysis.task_redundancy.raw_data),
                            "llm_interpretation": (
                                analysis.task_redundancy.llm_interpretation
                            ),
                            "recommendations": (
                                analysis.task_redundancy.recommendations
                            ),
                        }
                        if analysis.task_redundancy
                        else None
                    ),
                    "metadata": analysis.metadata,
                }
            )
        else:
            # Phase 2 analysis failed - add error info
            result.update(
                {
                    "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
                    "summary": None,
                    "phase2_error": phase2_error,
                    "requirement_divergences": [],
                    "decision_impacts": [],
                    "instruction_quality_issues": [],
                    "failure_diagnoses": [],
                    "task_redundancy": None,
                    "metadata": {"phase2_available": False, "error": phase2_error},
                }
            )

        return result

    except Exception as e:
        logger.error(f"Error analyzing project: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error analyzing project: {str(e)}"
        )


@app.get("/api/historical/projects/{project_id}/analysis/stream")  # type: ignore[misc]
async def stream_historical_analysis(project_id: str) -> StreamingResponse:
    """
    Stream analysis progress to frontend using Server-Sent Events.

    Returns real-time progress updates as analysis is performed, showing
    each step with counts and status messages.
    """
    if not HISTORICAL_MODE_AVAILABLE:
        raise HTTPException(
            status_code=503, detail="Historical analysis mode not available"
        )
    assert history_query is not None
    assert PostProjectAnalyzer is not None

    async def event_generator() -> Any:
        """Generate SSE events for analysis progress."""
        try:
            # Step 1: Load project data
            event_data = json.dumps(
                {"type": "log", "message": "📂 Loading project data..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)  # Allow UI to update

            summary = await history_query.get_project_summary(project_id)
            project_name = summary["project_name"]

            event_data = json.dumps(
                {"type": "log", "message": f"✓ Loaded project: {project_name}"}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            # Step 2: Load project history (tasks, decisions, artifacts)
            event_data = json.dumps(
                {"type": "log", "message": "📋 Loading project history..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            history = await history_query.get_project_history(project_id)

            # Step 3: Report counts
            task_count = len(history.tasks)
            decision_count = len(history.decisions)
            artifact_count = len(history.artifacts)

            event_data = json.dumps(
                {"type": "log", "message": f"✓ Found {task_count} tasks"}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            event_data = json.dumps(
                {"type": "log", "message": f"✓ Found {decision_count} decisions"}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            event_data = json.dumps(
                {"type": "log", "message": f"✓ Found {artifact_count} artifacts"}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            # Step 4: Phase 1 summary
            event_data = json.dumps(
                {"type": "log", "message": "📊 Calculating Phase 1 metrics..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            completion_rate = summary["completion_rate"]
            event_data = json.dumps(
                {"type": "log", "message": f"✓ Completion rate: {completion_rate:.1f}%"}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            # Step 5: Run Phase 2 analysis with real-time progress
            event_data = json.dumps(
                {"type": "log", "message": "🔍 Starting Phase 2 AI analysis..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            phase2_success = False
            analysis = None
            phase2_error = None

            try:
                # Create a queue for passing progress events from callback to generator
                progress_queue: asyncio.Queue = asyncio.Queue()

                async def run_analysis_with_progress() -> Any:
                    """Run analysis and put progress events in queue."""
                    nonlocal analysis, phase2_success

                    async def progress_callback(event: Any) -> None:
                        """Progress callback that puts events in queue."""
                        if event.total and event.current > 0:
                            pct = (event.current / event.total) * 100
                            msg = (
                                f"  ⟳ {event.message} "
                                f"({event.current}/{event.total}"
                                f" - {pct:.0f}%)"
                            )
                        else:
                            msg = f"  ⟳ {event.message}"

                        # Put formatted message in queue
                        await progress_queue.put(msg)

                    analyzer = PostProjectAnalyzer()
                    analysis = await analyzer.analyze_project(
                        project_id=project_id,
                        tasks=history.tasks,
                        decisions=history.decisions,
                        progress_callback=progress_callback,
                    )
                    phase2_success = True
                    # Signal completion
                    await progress_queue.put(None)

                # Start analysis task
                analysis_task = asyncio.create_task(run_analysis_with_progress())

                # Yield progress events as they arrive in the queue
                # Track time since last message to send keep-alive updates
                import time

                last_message_time = time.time()
                keepalive_interval = 3.0  # Send keep-alive every 3 seconds

                while True:
                    try:
                        # Wait for progress event with timeout
                        msg = await asyncio.wait_for(progress_queue.get(), timeout=0.1)
                        if msg is None:  # Completion signal
                            break
                        # Yield progress event
                        progress_data = json.dumps({"type": "log", "message": msg})
                        yield f"data: {progress_data}\n\n"
                        last_message_time = time.time()
                        await asyncio.sleep(0.05)
                    except asyncio.TimeoutError:
                        # No progress event yet - check if we need a keep-alive
                        current_time = time.time()
                        time_since_last = current_time - last_message_time

                        if time_since_last >= keepalive_interval:
                            # Send keep-alive message
                            keepalive_data = json.dumps(
                                {"type": "log", "message": "  ⟳ Processing..."}
                            )
                            yield f"data: {keepalive_data}\n\n"
                            last_message_time = current_time
                            await asyncio.sleep(0.05)

                        # Check if task is done
                        if analysis_task.done():
                            # Task completed, check for any remaining events
                            while not progress_queue.empty():
                                msg = await progress_queue.get()
                                if msg is not None:
                                    progress_data = json.dumps(
                                        {"type": "log", "message": msg}
                                    )
                                    yield f"data: {progress_data}\n\n"
                                    await asyncio.sleep(0.05)
                            break
                        continue

                # Wait for analysis to complete (should already be done)
                await analysis_task

                event_data = json.dumps(
                    {"type": "log", "message": "✓ Phase 2 analysis complete"}
                )
                yield f"data: {event_data}\n\n"
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.warning(f"Phase 2 analysis failed: {e}")
                phase2_error = str(e)
                event_data = json.dumps(
                    {
                        "type": "log",
                        "message": f"⚠️  Phase 2 analysis failed: {phase2_error}",
                    }
                )
                yield f"data: {event_data}\n\n"
                await asyncio.sleep(0.1)

            # Step 6: Build result
            event_data = json.dumps(
                {"type": "log", "message": "📦 Assembling results..."}
            )
            yield f"data: {event_data}\n\n"
            await asyncio.sleep(0.1)

            result = {
                "project_id": project_id,
                "project_name": summary["project_name"],
                "total_tasks": summary["total_tasks"],
                "completed_tasks": summary["completed_tasks"],
                "completion_rate": summary["completion_rate"],
                "blocked_tasks": summary["blocked_tasks"],
                "total_decisions": summary["total_decisions"],
                "total_artifacts": summary.get("total_artifacts", 0),
                "active_agents": summary.get("active_agents", 0),
                "project_duration_hours": summary["project_duration_hours"],
            }

            if phase2_success and analysis:
                result.update(
                    {
                        "analysis_timestamp": analysis.analysis_timestamp.isoformat(),
                        "summary": analysis.summary,
                        "requirement_divergences": [
                            {
                                "task_id": rd.task_id,
                                "fidelity_score": rd.fidelity_score,
                                "divergences": [
                                    {
                                        "requirement": d.requirement,
                                        "implementation": d.implementation,
                                        "severity": d.severity,
                                        "impact": d.impact,
                                        "citation": d.citation,
                                    }
                                    for d in rd.divergences
                                ],
                                "recommendations": rd.recommendations,
                            }
                            for rd in analysis.requirement_divergences
                        ],
                        "decision_impacts": [
                            {
                                "decision_id": di.decision_id,
                                "impact_chains": [
                                    {
                                        "decision_summary": ic.decision_summary,
                                        "direct_impacts": ic.direct_impacts,
                                        "indirect_impacts": ic.indirect_impacts,
                                        "depth": ic.depth,
                                        "citation": ic.citation,
                                    }
                                    for ic in di.impact_chains
                                ],
                                "unexpected_impacts": [
                                    {
                                        "affected_task": ui.affected_task_name,
                                        "anticipated": ui.anticipated,
                                        "actual_impact": ui.actual_impact,
                                        "severity": ui.severity,
                                    }
                                    for ui in di.unexpected_impacts
                                ],
                                "recommendations": di.recommendations,
                            }
                            for di in analysis.decision_impacts
                        ],
                        "instruction_quality_issues": [
                            {
                                "task_id": iq.task_id,
                                "quality_scores": {
                                    "clarity": iq.quality_scores.clarity,
                                    "completeness": iq.quality_scores.completeness,
                                    "specificity": iq.quality_scores.specificity,
                                    "overall": iq.quality_scores.overall,
                                },
                                "ambiguity_issues": [
                                    {
                                        "task_id": issue.task_id,
                                        "task_name": issue.task_name,
                                        "ambiguous_aspect": issue.ambiguous_aspect,
                                        "evidence": issue.evidence,
                                        "consequence": issue.consequence,
                                        "severity": issue.severity,
                                        "citation": issue.citation,
                                    }
                                    for issue in iq.ambiguity_issues
                                ],
                                "recommendations": iq.recommendations,
                            }
                            for iq in analysis.instruction_quality_issues
                        ],
                        "failure_diagnoses": [
                            {
                                "task_id": fd.task_id,
                                "root_causes": [
                                    {
                                        "category": rc.category,
                                        "description": rc.description,
                                        "evidence": rc.evidence,
                                        "likelihood": rc.likelihood,
                                    }
                                    for rc in fd.root_causes
                                ],
                                "recommendations": fd.recommendations,
                            }
                            for fd in analysis.failure_diagnoses
                        ],
                        "metadata": {
                            "phase2_available": True,
                        },
                    }
                )
            else:
                result.update(
                    {
                        "summary": None,
                        "phase2_error": phase2_error,
                        "requirement_divergences": [],
                        "decision_impacts": [],
                        "instruction_quality_issues": [],
                        "failure_diagnoses": [],
                        "metadata": {"phase2_available": False, "error": phase2_error},
                    }
                )

            # Step 7: Complete
            event_data = json.dumps({"type": "complete", "data": result})
            yield f"data: {event_data}\n\n"

        except Exception as e:
            logger.error(f"Error during streaming analysis: {e}", exc_info=True)
            error_msg = str(e)
            event_data = json.dumps({"type": "error", "message": f"Error: {error_msg}"})
            yield f"data: {event_data}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    # Load port from config.json
    config_path_str = os.path.join(os.path.dirname(__file__), "..", "config.json")
    try:
        with open(config_path_str, "r") as f:
            config = json.load(f)
            port = config.get("backend", {}).get("port", 4301)
    except Exception as e:
        logger.warning(f"Could not load config.json, using default port 4301: {e}")
        port = 4301

    logger.info(f"Starting Cato API on http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")  # nosec B104
