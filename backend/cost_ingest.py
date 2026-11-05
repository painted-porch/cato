"""
Worker JSONL ingestion driver for the Cato cost dashboard.

The Marcus side (``src/cost_tracking/worker_ingester.py``) provides the
:class:`WorkerJSONLIngester` library — it knows how to read a Claude
Code session JSONL file and write ``token_events`` rows. What it doesn't
know is **how to map a session JSONL record to a ``project_id``** —
that's experiment-specific glue. This module supplies that glue.

Binding resolution
------------------
Each Claude Code session record carries a ``cwd`` field — the working
directory of the worker subprocess. Marcus's spawn_agents.py creates a
worktree per agent at::

    <experiment_dir>/worktrees/<agent_id>

…and writes the project's metadata to ``<experiment_dir>/project_info.json``
before spawning workers. The resolver here walks up two levels from
``cwd``, reads ``project_info.json``, and emits an
:class:`AgentBinding`.

Sessions whose cwd doesn't match this layout (project-creator agents,
monitor agents, ad-hoc Claude Code sessions in unrelated dirs) return
``None`` from the resolver and are correctly dropped — they aren't part
of any experiment Cato should attribute cost to.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from src.cost_tracking.worker_ingester import AgentBinding


# ---------------------------------------------------------------------------
# Binding resolution
# ---------------------------------------------------------------------------


def _read_project_info(experiment_dir: str) -> Optional[Dict[str, Any]]:
    """Read ``project_info.json`` from an experiment directory.

    Reads from disk every call. An earlier ``lru_cache`` here returned
    stale data when a user re-ran an experiment in the same directory
    with a different ``project_id``, since the cache had process
    lifetime. Disk hits are cheap (small JSON, OS page cache) and
    correctness matters more than the saved syscalls.

    Returns
    -------
    dict or None
        Parsed JSON when the file exists and is valid, else None.
    """
    path = Path(experiment_dir) / "project_info.json"
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return data
        return None
    except (OSError, json.JSONDecodeError) as exc:
        logger.debug("project_info.json unreadable at %s: %s", path, exc)
        return None


def resolve_binding_from_cwd(record: Dict[str, Any]) -> Optional["AgentBinding"]:
    """Map a Claude Code JSONL record to an :class:`AgentBinding`.

    Walks the ``cwd`` field two levels up (past ``worktrees/<agent_id>``)
    to find the experiment directory, reads ``project_info.json`` from
    there to get the ``project_id``, and uses the cwd's basename as the
    ``agent_id``.

    Returns
    -------
    AgentBinding or None
        ``None`` when the cwd doesn't match the expected layout (e.g.
        project-creator agents working in the experiment root, monitor
        agents, or unrelated Claude Code sessions). The caller drops
        such records.
    """
    from src.cost_tracking.worker_ingester import AgentBinding

    cwd_raw = record.get("cwd")
    if not isinstance(cwd_raw, str) or not cwd_raw:
        return None

    cwd = Path(cwd_raw)
    # Expected layout: <experiment_dir>/worktrees/<agent_id>
    # So parent should be named 'worktrees' and grandparent is the exp dir.
    if cwd.parent.name != "worktrees":
        return None

    experiment_dir = cwd.parent.parent
    info = _read_project_info(str(experiment_dir))
    if info is None:
        return None

    project_id = info.get("project_id")
    if not project_id:
        return None

    return AgentBinding(
        agent_id=cwd.name,
        # run_id isn't recorded in project_info.json, so leave it
        # 'unassigned'. The project_id is what the dashboard joins
        # on. Renamed from experiment_id alongside Marcus's
        # ``runs`` table rename (Simon ``7ed3074d``).
        run_id="unassigned",
        project_id=str(project_id),
    )


# ---------------------------------------------------------------------------
# Ingestion driver
# ---------------------------------------------------------------------------


def _peek_project_id(jsonl: Path) -> Optional[str]:
    """Read just enough of one session file to learn its project_id.

    A worker session's ``cwd`` (and therefore its resolved project_id) is
    the same for every record in one JSONL file in practice -- one
    process, one worktree. Reading ahead like this, separately from the
    real ingest pass, lets the caller pick the right target store
    *before* ingesting, without guessing.

    Returns
    -------
    str or None
        The resolved project_id, or None if no record in the file
        resolves (the caller then falls back to a default store; the
        real ingest pass will still correctly skip every record there).
    """
    try:
        with jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                binding = resolve_binding_from_cwd(record)
                if binding is not None:
                    return str(binding.project_id)
    except OSError:
        return None
    return None


def run_ingest(stores: List[Tuple[str, Any]]) -> Dict[str, Any]:
    """Sweep ``~/.claude/projects/`` and ingest every session JSONL.

    Each session is routed to whichever configured cost source already
    owns its project's events (falling back to the first configured
    source for a project with no events anywhere yet), instead of
    always writing to one fixed store. Without this, a project whose
    planner events live in a second configured database would get its
    worker events written into the first database instead on every
    sweep -- silently splitting one project's cost data across two
    databases (cato#52, Codex finding on PR #53).

    Idempotent — :class:`WorkerJSONLIngester` dedupes by record UUID
    within a single process. Re-running the sweep on the same files
    inserts only records added since the last sweep.

    Parameters
    ----------
    stores : List[Tuple[str, Any]]
        ``(source_label, store)`` pairs for every configured cost
        source, as returned by ``backend.cost_routes.get_stores()``.

    Returns
    -------
    dict
        ``{ingested: int, files: int, skipped_unbound: int}`` where
        ``ingested`` counts new ``token_events`` rows, ``files`` the
        number of JSONL files scanned, and ``skipped_unbound`` is the
        number of sessions whose cwd didn't resolve (project-creators,
        monitors, ad-hoc sessions outside the experiment layout).
    """
    from src.cost_tracking.cost_aggregator import CostAggregator
    from src.cost_tracking.worker_ingester import WorkerJSONLIngester

    base = Path.home() / ".claude" / "projects"
    if not base.exists():
        return {"ingested": 0, "files": 0, "skipped_unbound": 0}

    aggregators = [CostAggregator(store=store) for _, store in stores]

    # Memoized per project_id for this sweep only: many session files
    # belong to the same project, and re-running project_totals against
    # every configured source (including a large production DB) for
    # every file would turn one sweep into files × sources queries
    # instead of distinct_projects × sources (claude-review finding on
    # PR #53). The cache does not persist across sweeps -- a project
    # can only move source by actually accumulating events there, which
    # changes what the next sweep's fresh cache would find anyway.
    _owning_store_cache: Dict[str, Any] = {}

    def _owning_store(project_id: str) -> Any:
        if project_id in _owning_store_cache:
            return _owning_store_cache[project_id]
        result = stores[0][1]
        for agg, (_, store) in zip(aggregators, stores):
            totals = agg.project_totals(project_id)
            if (totals.get("events") or 0) > 0:
                result = store
                break
        _owning_store_cache[project_id] = result
        return result

    skipped_unbound = 0

    def _wrap_resolver(record: Dict[str, Any]) -> Optional["AgentBinding"]:
        nonlocal skipped_unbound
        b = resolve_binding_from_cwd(record)
        if b is None:
            skipped_unbound += 1
        return b

    # One ingester per configured store, reused across every file routed
    # to it, so the per-process dedupe cache (keyed by record UUID) still
    # works across repeated sweeps against the same source.
    ingesters = {
        id(store): WorkerJSONLIngester(store=store, resolve_binding=_wrap_resolver)
        for _, store in stores
    }

    ingested = 0
    files = 0
    for jsonl in base.rglob("*.jsonl"):
        files += 1
        try:
            project_id = _peek_project_id(jsonl)
            target_store = _owning_store(project_id) if project_id else stores[0][1]
            ingester = ingesters[id(target_store)]
            ingested += ingester.ingest_file(jsonl)
        except Exception:
            # One malformed/corrupt file (e.g. invalid UTF-8 from
            # _peek_project_id, which only guards OSError/JSON errors
            # itself) must not abort the whole sweep -- every other
            # session still needs to be ingested on this and every
            # later poll (Codex finding on PR #53's fix commit).
            logger.exception("ingest failed for %s", jsonl)
            continue

    return {
        "ingested": ingested,
        "files": files,
        "skipped_unbound": skipped_unbound,
    }


def clear_project_info_cache() -> None:
    """No-op kept for API compatibility.

    Earlier versions cached :func:`_read_project_info` via ``lru_cache``;
    tests called this between cases to drop the cache. The cache has
    been removed (it caused stale reads on experiment re-runs), but
    callers may still invoke this helper — keep the symbol so they
    don't break.
    """
    return None
