"""
Unified Data Aggregator for Cato Visualization.

This module replaces the multi-layered transformation pipeline in data_loader.py
with a single aggregation function that creates denormalized snapshots.

The aggregator:
1. Loads data from all sources (projects, tasks, logs, events)
2. Denormalizes relationships (embeds parent/project/agent info)
3. Calculates all metrics once
4. Pre-calculates timeline positions
5. Returns immutable Snapshot

Performance:
- Target: < 100ms for typical project (vs 500-2000ms current)
- Single pass over data (vs 7+ transforms)
- All calculations done once (vs recalculated on every render)
"""

import json
import logging
import sqlite3
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Literal, Optional, Set, Tuple

from cato_src.core.store import (
    Agent,
    Artifact,
    Decision,
    Event,
    Message,
    Metrics,
    QualityAssessment,
    Snapshot,
    Task,
)

logger = logging.getLogger(__name__)


class ProjectMatcher:
    """
    Handle task-to-project assignment with multiple matching strategies.

    Supports:
    - exact: Task ID must exactly match project ID
    - fuzzy: Task ID within ±tolerance of project ID (handles Planka quirks)
    - timeframe: Task created within project timeframe
    """

    def __init__(self, tolerance: int = 20):
        """
        Initialize project matcher.

        Parameters
        ----------
        tolerance : int
            Fuzzy matching tolerance (default ±20)
        """
        self.tolerance = tolerance

    def match_task_to_project(
        self,
        task_id: str,
        project_id: str,
        strategy: Literal["exact", "fuzzy", "timeframe"] = "fuzzy",
        project_created: Optional[datetime] = None,
        project_last_used: Optional[datetime] = None,
        task_created: Optional[datetime] = None,
    ) -> bool:
        """
        Determine if task belongs to project.

        Parameters
        ----------
        task_id : str
            Task ID to match
        project_id : str
            Project ID to match against
        strategy : str
            Matching strategy ('exact', 'fuzzy', 'timeframe')
        project_created : Optional[datetime]
            When project was created (for timeframe strategy)
        project_last_used : Optional[datetime]
            When project was last used (for timeframe strategy)
        task_created : Optional[datetime]
            When task was created (for timeframe strategy)

        Returns
        -------
        bool
            True if task matches project
        """
        if strategy == "exact":
            return task_id == project_id or task_id.startswith(f"{project_id}_")

        elif strategy == "fuzzy":
            # Extract numeric portion of IDs
            try:
                task_num = int(task_id.split("_")[0])
                project_num = int(project_id)
                return abs(task_num - project_num) <= self.tolerance
            except (ValueError, IndexError):
                # Fallback to exact matching if numeric extraction fails
                return task_id == project_id or task_id.startswith(f"{project_id}_")

        else:  # strategy == "timeframe"
            # Match if task created within project timeframe
            if not all([project_created, project_last_used, task_created]):
                return False

            # All are confirmed not None by the check above
            assert project_created is not None
            assert project_last_used is not None
            assert task_created is not None
            return project_created <= task_created <= project_last_used


class Aggregator:
    """
    Unified aggregator that creates denormalized snapshots.

    This replaces the 7+ transformation layers in data_loader.py with a
    single aggregation function.
    """

    def __init__(
        self,
        marcus_root: Optional[Path] = None,
        marcus_roots: Optional[List[Path]] = None,
        history_cutoff_date: Optional[str] = None,
    ):
        """
        Initialize the aggregator.

        Parameters
        ----------
        marcus_root : Optional[Path]
            Single Marcus root directory (backward-compatible form).
        marcus_roots : Optional[List[Path]]
            Multiple Marcus root directories for parallel experiments.
            When provided, takes precedence over marcus_root. The first
            entry becomes self.marcus_root for backward compatibility.
        history_cutoff_date : Optional[str]
            ISO date string (YYYY-MM-DD). Projects created before this date
            are excluded from _load_projects(). Log files whose filename
            timestamp predates this cutoff are skipped in _load_messages()
            and _load_events(). None means load everything.
        """
        # Resolve the list of roots: plural arg wins, else wrap singular
        if marcus_roots is not None:
            self.marcus_roots: List[Path] = [Path(r) for r in marcus_roots]
        elif marcus_root is not None:
            self.marcus_roots = [Path(marcus_root)]
        else:
            # Auto-detect: assumes viz is a subdirectory of Marcus
            auto_root = Path(__file__).parent.parent.parent
            self.marcus_roots = [auto_root]

        # Primary root (backward compat attribute)
        self.marcus_root: Path = self.marcus_roots[0]

        self.persistence_dir = self.marcus_root / "data" / "marcus_state"
        self.conversation_logs_dir = self.marcus_root / "logs" / "conversations"
        self.agent_events_dir = self.marcus_root / "logs" / "agent_events"

        self.project_matcher = ProjectMatcher(tolerance=20)
        self.snapshot_version_counter = 0

        # History window — projects and logs older than this are excluded.
        # Stored in two forms: ISO (for project created_at comparison) and
        # compact YYYYMMDD (for log filename comparison).
        self.history_cutoff_date: Optional[str] = history_cutoff_date
        self._cutoff_compact: Optional[str] = (
            history_cutoff_date.replace("-", "") if history_cutoff_date else None
        )

        # Maps project_id → its source marcus_root (populated by _load_projects)
        self._project_root: Dict[str, Path] = {}

        # Cache for projects data to avoid repeated file I/O
        self._projects_cache: Optional[List[Dict[str, Any]]] = None
        self._projects_cache_time: Optional[datetime] = None
        self._projects_cache_ttl = 60  # Cache for 60 seconds

        # Caches for parsed log files (keyed by directory signature: tuple of
        # (path, mtime_ns, size) for each file). Invalidated when any source
        # file appears, disappears, or is modified.
        self._messages_cache: Optional[List[Dict[str, Any]]] = None
        self._messages_cache_signature: Optional[Tuple] = None
        self._events_cache: Optional[List[Dict[str, Any]]] = None
        self._events_cache_signature: Optional[Tuple] = None

        # Lazy-initialized Marcus persistence handles, keyed by root. Each
        # root gets its own ProjectHistoryPersistence instance so a project
        # living in a non-primary root is not served by the primary root's
        # data (cato#50). Reused across calls within a root so decisions and
        # artifacts share one instance instead of paying the init cost twice
        # per snapshot.
        self._persistence: Dict[str, Optional[Any]] = {}
        self._persistence_init_attempted: Dict[str, bool] = {}

        # mtime-validated cache for subtasks.json (per marcus_root). Reading
        # the full ~5k-entry file is ~150ms; the file is rewritten atomically
        # by Marcus, so an mtime+size match guarantees identical content.
        self._subtasks_cache: Dict[
            str, Tuple[Tuple[float, int], List[Dict[str, Any]]]
        ] = {}

        logger.info(f"Initialized Aggregator with roots: {self.marcus_roots}")

    def _get_marcus_persistence(self, root: Optional[Path] = None) -> Optional[Any]:
        """Lazy-load and cache a ProjectHistoryPersistence instance for a given root.

        Cached per root (not globally): a project living in a non-primary
        root must be served by that root's own persistence instance, not
        whichever root resolved first (cato#50).

        Returns None if Marcus is not importable (e.g. running standalone).
        """
        root = root if root is not None else self.marcus_root
        key = str(root)
        if key in self._persistence or self._persistence_init_attempted.get(key):
            return self._persistence.get(key)
        self._persistence_init_attempted[key] = True
        try:
            import sys

            if key not in sys.path:
                sys.path.insert(0, key)
            from src.core.project_history import ProjectHistoryPersistence

            self._persistence[key] = ProjectHistoryPersistence(marcus_root=root)
        except Exception as e:
            logger.debug(f"Marcus persistence unavailable for {root}: {e}")
            self._persistence[key] = None
        return self._persistence[key]

    def _load_subtasks_json_cached(self, root: Path) -> List[Dict[str, Any]]:
        """Read subtasks.json with mtime-validated caching.

        The file is rewritten atomically by Marcus (temp + rename), so an
        mtime+size match means the content is identical. This avoids paying
        ~150ms of JSON parse on every snapshot when nothing changed.
        """
        subtasks_file = root / "data" / "marcus_state" / "subtasks.json"
        if not subtasks_file.exists():
            return []
        try:
            st = subtasks_file.stat()
        except OSError:
            return []

        signature = (st.st_mtime, st.st_size)
        cached = self._subtasks_cache.get(str(subtasks_file))
        if cached and cached[0] == signature:
            return cached[1]

        try:
            with open(subtasks_file, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading {subtasks_file}: {e}")
            return []

        if isinstance(data, dict) and "subtasks" in data:
            subtasks: List[Dict[str, Any]] = list(data["subtasks"].values())
        elif isinstance(data, dict):
            subtasks = list(data.values())
        else:
            subtasks = data

        self._subtasks_cache[str(subtasks_file)] = (signature, subtasks)
        return subtasks

    def _query_parent_ids_by_project_metadata(
        self, project_id: str, root: Path
    ) -> Set[str]:
        """SQL-filter parent task IDs whose metadata.project_id matches.

        Returns the set of task_metadata keys (the parent task IDs) where the
        embedded JSON ``project_id`` field equals ``project_id``. Catches the
        ~50% of parents that have project_id explicitly set; the rest must
        come from conversation logs or Planka prefix matching.
        """
        db_path = root / "data" / "marcus.db"
        if not db_path.exists():
            return set()
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("PRAGMA query_only = 1")
                rows = conn.execute(
                    "SELECT key FROM persistence "
                    "WHERE collection = 'task_metadata' "
                    "AND json_extract(data, '$.project_id') = ?",
                    (project_id,),
                ).fetchall()
            finally:
                conn.close()
            return {row[0] for row in rows}
        except Exception as e:
            logger.debug(f"project_id metadata query failed: {e}")
            return set()

    def _query_parent_ids_by_planka_prefix(
        self,
        planka_board_id: Optional[str],
        planka_project_id: Optional[str],
        root: Path,
    ) -> Set[str]:
        """Match parent task IDs whose 8-char prefix is within ±20 of a Planka ID.

        Replicates the in-memory fuzzy match in the original ``_load_tasks``
        without iterating all 11k tasks. Uses SQL substring + numeric prefix
        comparison. Cheap: indexed scan over ~5k task_metadata rows is fast.
        """
        prefixes: List[int] = []
        for pid in (planka_board_id, planka_project_id):
            if pid and len(pid) >= 8:
                try:
                    prefixes.append(int(pid[:8]))
                except ValueError:
                    pass
        if not prefixes:
            return set()

        db_path = root / "data" / "marcus.db"
        if not db_path.exists():
            return set()
        try:
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("PRAGMA query_only = 1")
                rows = conn.execute(
                    "SELECT key FROM persistence WHERE collection = 'task_metadata'"
                ).fetchall()
            finally:
                conn.close()
        except Exception as e:
            logger.debug(f"Planka prefix scan failed: {e}")
            return set()

        matches: Set[str] = set()
        for (key,) in rows:
            if not key or len(key) < 8 or not key[0].isdigit():
                continue
            try:
                key_prefix = int(key[:8])
            except ValueError:
                continue
            for prefix in prefixes:
                if abs(key_prefix - prefix) <= 20:
                    matches.add(key)
                    break
        return matches

    def _load_parent_tasks_by_ids(
        self, task_ids: Set[str], root: Path
    ) -> List[Dict[str, Any]]:
        """SQL-fetch parent task metadata+outcomes for an explicit ID set.

        Project-scoped equivalent of ``load_parent_tasks_from_db``. Avoids
        loading all 5,344 parent rows when we only need ~10. Uses an indexed
        ``key IN (...)`` lookup against the persistence table.
        """
        if not task_ids:
            return []

        db_path = root / "data" / "marcus.db"
        if not db_path.exists():
            return []

        ids_list = list(task_ids)
        placeholders = ",".join("?" * len(ids_list))

        try:
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("PRAGMA query_only = 1")
                # Metadata
                meta_rows = conn.execute(
                    "SELECT key, data FROM persistence "  # nosec B608
                    f"WHERE collection = 'task_metadata' AND key IN ({placeholders})",
                    ids_list,
                ).fetchall()
                # Outcomes — match by task_id IN (...) against the JSON field,
                # since outcome keys are suffixed with agent+timestamp.
                outcome_rows = conn.execute(
                    "SELECT key, data FROM persistence "  # nosec B608
                    "WHERE collection = 'task_outcomes' "
                    f"AND json_extract(data, '$.task_id') IN ({placeholders})",
                    ids_list,
                ).fetchall()
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error loading filtered parent tasks: {e}")
            return []

        outcomes_by_task: Dict[str, Dict[str, Any]] = {}
        for _, data_json in outcome_rows:
            try:
                outcome = json.loads(data_json)
                actual_id = outcome.get("task_id")
                if actual_id:
                    outcomes_by_task[actual_id] = outcome
            except json.JSONDecodeError:
                continue

        parent_tasks: List[Dict[str, Any]] = []
        for key, data_json in meta_rows:
            try:
                task = json.loads(data_json)
            except json.JSONDecodeError:
                continue
            if "task_id" in task and "id" not in task:
                task["id"] = task["task_id"]
            actual_id = task.get("task_id", key)
            outcome = outcomes_by_task.get(actual_id, {})
            if outcome:
                if outcome.get("completed_at"):
                    task["status"] = "done"
                elif outcome.get("started_at"):
                    task["status"] = "in_progress"
                else:
                    task["status"] = "todo"
                task["updated_at"] = (
                    outcome.get("completed_at")
                    or outcome.get("started_at")
                    or task.get("created_at")
                )
                task["started_at"] = outcome.get("started_at")
                task["completed_at"] = outcome.get("completed_at")
                task["actual_hours"] = outcome.get("actual_hours", 0.0)
                task["progress_percent"] = 100 if outcome.get("completed_at") else 0
                task["assigned_agent_id"] = outcome.get("agent_id")
            if "dependencies" in task:
                task["dependency_ids"] = task["dependencies"]
            # Default unstarted parents to "todo". Without this the DAG/Board
            # treats missing status as filtered/done and the node only shows
            # up after Marcus writes an outcome. Matches load_parent_tasks_from_db.
            task.setdefault("status", "todo")
            task.setdefault("parent_task_id", None)
            task.setdefault("is_subtask", False)
            task.setdefault("assigned_agent_name", None)
            task.setdefault("project_id", None)
            task.setdefault("project_name", None)
            parent_tasks.append(task)

        # Apply kanban status (filtered to our IDs only)
        kanban_status = self._load_kanban_status_for_ids(task_ids, root)
        if kanban_status:
            for task in parent_tasks:
                tid = task.get("task_id", task.get("id"))
                if tid and tid in kanban_status:
                    task["status"] = kanban_status[tid]["status"]
                    if kanban_status[tid].get("assigned_to"):
                        task["assigned_agent_id"] = kanban_status[tid]["assigned_to"]
                    task["blocker_ai_suggestions"] = kanban_status[tid].get(
                        "blocker_ai_suggestions"
                    )

        return parent_tasks

    def _load_kanban_status_for_ids(
        self, task_ids: Set[str], root: Path
    ) -> Dict[str, Dict[str, Any]]:
        """Read kanban*.db status for an explicit ID set.

        Project-scoped equivalent of the kanban scan inside
        ``load_parent_tasks_from_db``.
        """
        if not task_ids:
            return {}
        ids_list = list(task_ids)
        placeholders = ",".join("?" * len(ids_list))
        kanban_status: Dict[str, Dict[str, Any]] = {}
        data_dir = root / "data"
        kanban_dbs = sorted(data_dir.glob("kanban*.db")) if data_dir.exists() else []
        for kanban_db in kanban_dbs:
            try:
                conn = sqlite3.connect(str(kanban_db))
                try:
                    conn.execute("PRAGMA query_only = 1")
                    rows = conn.execute(
                        "SELECT id, status, assigned_to FROM tasks "  # nosec B608
                        f"WHERE id IN ({placeholders})",
                        ids_list,
                    ).fetchall()
                    try:
                        blocker_rows = conn.execute(
                            "SELECT task_id, content FROM comments "  # nosec B608
                            f"WHERE task_id IN ({placeholders}) "
                            "AND content LIKE '%AI Suggestions%' "
                            "ORDER BY created_at ASC",
                            ids_list,
                        ).fetchall()
                    except Exception:
                        blocker_rows = []
                finally:
                    conn.close()
                for tid, status, assigned in rows:
                    kanban_status[tid] = {
                        "status": status,
                        "assigned_to": assigned,
                        "blocker_ai_suggestions": None,
                    }
                for tid, content in blocker_rows:
                    if tid in kanban_status:
                        kanban_status[tid]["blocker_ai_suggestions"] = (
                            self._parse_ai_suggestions(content)
                        )
            except Exception as e:
                logger.warning(
                    f"Could not read {kanban_db.name} for filtered status: {e}"
                )
        return kanban_status

    def _load_outcomes_and_timings_for_ids(
        self, task_ids: Set[str], root: Path
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """SQL-fetch outcomes and timings filtered to a task ID set.

        Returns ``(outcomes_by_task, timings_by_task)`` already keyed by
        the canonical task_id (matching what ``enrich_tasks_with_timing``
        would build via its longest-prefix index). No prefix walk needed
        because the SQL filter already narrowed the result.
        """
        if not task_ids:
            return {}, {}

        db_path = root / "data" / "marcus.db"
        if not db_path.exists():
            return {}, {}

        ids_list = list(task_ids)
        placeholders = ",".join("?" * len(ids_list))
        outcomes: Dict[str, Dict[str, Any]] = {}
        timings: Dict[str, Dict[str, Any]] = {}

        try:
            conn = sqlite3.connect(str(db_path))
            try:
                conn.execute("PRAGMA query_only = 1")
                outcome_rows = conn.execute(
                    "SELECT data FROM persistence "  # nosec B608
                    "WHERE collection = 'task_outcomes' "
                    f"AND json_extract(data, '$.task_id') IN ({placeholders})",
                    ids_list,
                ).fetchall()
                event_rows = conn.execute(
                    "SELECT data FROM persistence "  # nosec B608
                    "WHERE collection = 'events' "
                    "AND json_extract(data, '$.event_type') = 'task_completed' "
                    f"AND json_extract(data, '$.data.task_id') IN ({placeholders})",
                    ids_list,
                ).fetchall()
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error loading filtered outcomes/timings: {e}")
            return {}, {}

        for (data_json,) in outcome_rows:
            try:
                outcome = json.loads(data_json)
            except json.JSONDecodeError:
                continue
            tid = outcome.get("task_id")
            if not tid:
                continue
            outcomes[tid] = {
                "task_id": tid,
                "task_name": outcome.get("task_name"),
                "actual_hours": outcome.get("actual_hours", 0.0),
                "estimated_hours": outcome.get("estimated_hours", 0.0),
                "created_at": outcome.get("created_at"),
                "started_at": outcome.get("started_at"),
                "completed_at": outcome.get("completed_at"),
                "status": "done",
            }

        for (data_json,) in event_rows:
            try:
                event = json.loads(data_json)
            except json.JSONDecodeError:
                continue
            event_data = event.get("data", {})
            tid = event_data.get("task_id")
            started_at = event_data.get("started_at")
            completed_at = event_data.get("completed_at")
            if not (tid and started_at and completed_at):
                continue
            start_with_tz = (
                started_at
                if started_at.endswith(("Z", "+00:00"))
                else started_at + "+00:00"
            )
            end_with_tz = (
                completed_at
                if completed_at.endswith(("Z", "+00:00"))
                else completed_at + "+00:00"
            )
            try:
                start_dt = datetime.fromisoformat(start_with_tz)
                end_dt = datetime.fromisoformat(end_with_tz)
                duration_s = (end_dt - start_dt).total_seconds()
            except (ValueError, TypeError):
                # Malformed timestamp in a single event row — skip it and
                # keep loading the rest. Other event rows are independent.
                continue  # nosec B112
            timings[tid] = {
                "start_time": start_with_tz,
                "end_time": end_with_tz,
                "task_name": event_data.get("task_name"),
                "duration_seconds": duration_s,
                "duration_minutes": duration_s / 60,
                "duration_hours": duration_s / 3600,
            }

        return outcomes, timings

    @contextmanager
    def _override_task_ids(
        self,
        persistence: Any,
        project_id: str,
        task_ids: Set[str],
    ) -> Iterator[None]:
        """Temporarily replace ``persistence._get_task_ids_from_conversations``.

        Marcus's implementation re-globs every conversation file (no cutoff)
        on each call. When Cato has already loaded messages for the project,
        we can supply the task_id set directly and skip that scan entirely.

        Restores the original method on exit so the persistence instance is
        safe to reuse.
        """
        original = persistence._get_task_ids_from_conversations

        async def _stub(pid: str) -> Set[str]:
            if pid == project_id:
                return task_ids
            result: Set[str] = await original(pid)
            return result

        persistence._get_task_ids_from_conversations = _stub
        try:
            yield
        finally:
            persistence._get_task_ids_from_conversations = original

    @contextmanager
    def _timed(self, label: str) -> Iterator[None]:
        """Log wall-clock duration of a block at INFO level."""
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            logger.info(f"[timing] {label}: {elapsed_ms:.1f}ms")

    def _iter_log_files_post_cutoff(self, logs_dir: Path) -> Iterator[Path]:
        """
        Yield .jsonl files in logs_dir, filtering out pre-cutoff files cheaply.

        Filename format: ``{prefix}_{YYYYMMDD}_{HHMMSS}.jsonl``. When a cutoff
        is set, this uses year-prefixed globs to skip pre-cutoff files at the
        OS level (avoids stat'ing 49k irrelevant files in a 50k-file dir),
        then verifies via the filename date component.

        Files are NOT stat'd here — callers that need size should stat after
        this filter to keep the hot path cheap.
        """
        if not logs_dir.exists():
            return

        if not self._cutoff_compact:
            yield from logs_dir.glob("*.jsonl")
            return

        cutoff_year = int(self._cutoff_compact[:4])
        current_year = datetime.now(timezone.utc).year
        seen: Set[Path] = set()
        for year in range(cutoff_year, current_year + 1):
            for log_file in logs_dir.glob(f"*_{year}*.jsonl"):
                if log_file in seen:
                    continue
                seen.add(log_file)
                parts = log_file.stem.split("_")
                if (
                    len(parts) >= 2
                    and parts[1].isdigit()
                    and len(parts[1]) == 8
                    and parts[1] < self._cutoff_compact
                ):
                    continue
                yield log_file

    @staticmethod
    def _dir_signature(files: List[Path]) -> Tuple:
        """Build a cache signature from a list of files using path + mtime + size.

        Stat is the only way to detect content changes without re-reading;
        accept the cost (one stat per surviving file) in exchange for skipping
        the JSON parse on cache hit.
        """
        sig = []
        for f in sorted(files):
            try:
                st = f.stat()
                sig.append((str(f), st.st_mtime_ns, st.st_size))
            except OSError:
                continue
        return tuple(sig)

    def create_snapshot(
        self,
        project_id: Optional[str] = None,
        view_mode: Literal["subtasks", "parents", "all"] = "subtasks",
        timeline_scale_exponent: float = 0.4,
    ) -> Snapshot:
        """
        Create a complete denormalized snapshot.

        This is the main entry point that replaces the entire data_loader pipeline.

        Parameters
        ----------
        project_id : Optional[str]
            Specific project to snapshot (None = all projects)
        view_mode : str
            View mode: 'subtasks' (default), 'parents', or 'all'
        timeline_scale_exponent : float
            Power scale exponent for timeline (default 0.4)

        Returns
        -------
        Snapshot
            Complete denormalized snapshot with all metrics pre-calculated
        """
        snapshot_start = datetime.now(timezone.utc)
        self.snapshot_version_counter += 1

        logger.info(
            f"Creating snapshot v{self.snapshot_version_counter}: "
            f"project_id={project_id}, view={view_mode}"
        )

        # Step 1: Load raw data from all sources
        snapshot_t0 = time.perf_counter()
        with self._timed("_load_projects"):
            projects_data = self._load_projects()
        with self._timed("_load_tasks"):
            raw_tasks = self._load_tasks(project_id)
        with self._timed("_load_messages"):
            raw_messages = self._load_messages()
        with self._timed("_load_events"):
            raw_events = self._load_events()

        # Pre-compute project task_ids from already-loaded messages so
        # _load_decisions/_load_artifacts can skip Marcus's per-call
        # conversation re-glob (`_get_task_ids_from_conversations`).
        project_task_ids: Optional[Set[str]] = None
        if project_id:
            ids: Set[str] = set()
            for msg in raw_messages:
                meta = msg.get("metadata") or {}
                if meta.get("project_id") == project_id and meta.get("task_id"):
                    ids.add(str(meta["task_id"]))
            project_task_ids = ids

        with self._timed("_load_decisions"):
            raw_decisions = self._load_decisions(
                project_id, project_task_ids=project_task_ids
            )
        with self._timed("_load_artifacts"):
            raw_artifacts = self._load_artifacts(
                project_id, project_task_ids=project_task_ids
            )
        logger.info(
            f"[timing] load_phase_total: "
            f"{(time.perf_counter() - snapshot_t0) * 1000:.1f}ms"
        )

        # Step 2: Inherit parent dependencies to first subtasks BEFORE filtering
        # (must happen before parent tasks are filtered out)
        raw_tasks = self._inherit_parent_dependencies_to_first_subtask(raw_tasks)

        # Step 2.5: Stash a parent→subtask-rollup snapshot BEFORE the filter
        # removes subtasks. Applied AFTER message-enrichment below so the
        # rollup wins over message-derived started_at (which is the late
        # auto_complete event timestamp, not the real work span).
        subtask_rollup = self._build_subtask_rollup(raw_tasks)

        # Step 3: Filter tasks by view mode
        filtered_tasks = self._filter_tasks_by_view(raw_tasks, view_mode)

        # Step 3: Pre-filter messages by project tasks (two-pass approach)
        # Create task_ids set early for message filtering
        task_ids_set = {t["id"] for t in filtered_tasks}

        # In parents view, agents do work on subtasks — their status_update
        # messages reference subtask IDs. Rewrite those to the parent ID so
        # they surface in the parent's conversation pane.
        subtask_to_parent: Dict[str, str] = {}
        if view_mode == "parents":
            for t in raw_tasks:
                tid = t.get("id")
                pid = t.get("parent_task_id")
                if tid and pid and str(pid) in task_ids_set:
                    subtask_to_parent[str(tid)] = str(pid)

        def _resolve_task_id(msg: Dict[str, Any]) -> Optional[str]:
            tid = msg.get("task_id") or msg.get("metadata", {}).get("task_id")
            if tid and tid in subtask_to_parent:
                rewritten = subtask_to_parent[tid]
                msg["task_id"] = rewritten
                return rewritten
            return str(tid) if tid else None

        # PASS 1: Filter messages directly related to project tasks
        task_related_messages = []
        for msg in raw_messages:
            task_id = _resolve_task_id(msg)
            if task_id and task_id in task_ids_set:
                task_related_messages.append(msg)

        # Infer project agents from tasks and task-related messages only.
        # Exclude the central coordinator ("marcus"/"system"); it talks to every
        # agent in every project, so treating it as a project agent would pull
        # in cross-project messages in PASS 2.
        project_agent_ids = set()
        coordinator_ids = {"marcus", "system"}

        def _is_coordinator(agent_id: Optional[str]) -> bool:
            if agent_id is None:
                return False
            return agent_id.lower() in coordinator_ids

        # From tasks
        for task in filtered_tasks:
            agent_id = (
                task.get("assigned_agent_id")
                or task.get("agent_id")
                or task.get("assigned_to")
            )
            if agent_id and not _is_coordinator(agent_id):
                project_agent_ids.add(agent_id)

        # From task-related messages
        for msg in task_related_messages:
            from_agent = msg.get("from_agent_id")
            to_agent = msg.get("to_agent_id")
            if from_agent and not _is_coordinator(from_agent):
                project_agent_ids.add(from_agent)
            if to_agent and not _is_coordinator(to_agent):
                project_agent_ids.add(to_agent)

        logger.info(
            f"Identified {len(project_agent_ids)} project agents"
            " from tasks and messages"
        )

        # Compute project active time window from task-related messages.
        # Used to gate task_id=None messages so cross-project task requests by
        # agents who happen to also work on this project don't leak in.
        project_window_start: Optional[datetime] = None
        project_window_end: Optional[datetime] = None
        if project_id and task_related_messages:
            window_ts: List[datetime] = []
            for msg in task_related_messages:
                ts = self._parse_timestamp(msg.get("timestamp"))
                if ts:
                    window_ts.append(ts)
            if window_ts:
                project_window_start = min(window_ts)
                project_window_end = max(window_ts)

        # PASS 2: Include all messages involving project agents
        filtered_messages = []
        for msg in raw_messages:
            task_id = _resolve_task_id(msg)
            from_agent = msg.get("from_agent_id")
            to_agent = msg.get("to_agent_id")

            task_match = bool(task_id) and task_id in task_ids_set
            agent_match = (
                from_agent in project_agent_ids or to_agent in project_agent_ids
            )

            if not (task_match or agent_match):
                continue

            # When a project filter is active, gate non-task-tied agent matches
            # by the project's active time window. Cross-project agents (same
            # worker_id reused across projects) generate task_request messages
            # with no task_id; without this gate they leak in.
            if (
                project_id
                and not task_match
                and project_window_start is not None
                and project_window_end is not None
            ):
                ts = self._parse_timestamp(msg.get("timestamp"))
                if ts is None:
                    continue
                if ts < project_window_start or ts > project_window_end:
                    continue

            filtered_messages.append(msg)

        logger.info(
            f"Pre-filtered messages: {len(filtered_messages)}"
            f"/{len(raw_messages)} related to project"
        )

        # Step 4: Build lookup tables for denormalization
        projects_by_id = {p["id"]: p for p in projects_data if "id" in p}
        # Use ALL tasks for lookup (including parents) so subtasks can find parent names
        all_tasks_by_id = {t["id"]: t for t in raw_tasks}
        agents_by_id = self._infer_agents(filtered_tasks, filtered_messages)

        # Step 5: Enrich tasks with started_at and completed_at from messages
        self._enrich_tasks_with_message_timestamps(filtered_tasks, filtered_messages)

        # Step 5.5: Calculate synthetic start times based on dependencies
        # (for tasks without started_at but with dependencies)
        self._calculate_synthetic_start_times(filtered_tasks)

        # Step 5.6: Apply parent timing rollup LAST. Overrides both message-
        # derived started_at (auto_complete event timestamp) and the
        # dep-completion clamp from synthesize_start_times — the rollup
        # spans the real work window (first subtask start → last subtask
        # end), and a parent's subtasks legitimately start before sibling
        # parents finish.
        self._apply_subtask_rollup(filtered_tasks, subtask_rollup)

        # Step 6: Calculate timeline boundaries
        timeline_start, timeline_end, duration_minutes = self._calculate_timeline(
            filtered_tasks, filtered_messages
        )

        # Step 7: Build denormalized tasks with timeline positions
        tasks = self._build_tasks(
            filtered_tasks,
            projects_by_id,
            all_tasks_by_id,  # Use ALL tasks for parent lookup
            agents_by_id,
            timeline_start,
            timeline_end,
            timeline_scale_exponent,
        )

        # Step 8: Build denormalized agents with metrics
        agents = self._build_agents(agents_by_id, tasks, filtered_messages)

        # Step 9: Build denormalized messages (already filtered above)
        final_task_ids_set = {t.id for t in tasks}
        messages = self._build_messages(
            filtered_messages, final_task_ids_set, all_tasks_by_id, agents_by_id
        )

        # Step 9: Build denormalized events
        events = self._build_events(
            raw_events, final_task_ids_set, all_tasks_by_id, agents_by_id
        )

        # Step 9a: Build denormalized decisions
        decisions = self._build_decisions(
            raw_decisions, final_task_ids_set, all_tasks_by_id, agents_by_id
        )

        # Step 9b: Build denormalized artifacts
        artifacts = self._build_artifacts(
            raw_artifacts, final_task_ids_set, all_tasks_by_id, agents_by_id
        )

        # Step 8a: Generate diagnostic events for timeline
        diagnostic_events = self._build_diagnostic_events(
            tasks, agents, timeline_start, timeline_end
        )
        # Merge diagnostic events with regular events
        all_events = events + diagnostic_events

        # Step 9: Calculate pre-computed metrics
        metrics = self._calculate_metrics(tasks, agents, messages)

        # Step 10: Build dependency graphs
        task_dependency_graph = self._build_dependency_graph(tasks)
        agent_communication_graph = self._build_communication_graph(messages)

        # Step 11: Get project metadata
        project_name = ""
        included_project_ids = []
        if project_id and project_id in projects_by_id:
            project_name = projects_by_id[project_id].get("name", "")
            included_project_ids = [project_id]
        else:
            # All projects
            included_project_ids = list(projects_by_id.keys())

        # Step 12: Create snapshot
        snapshot = Snapshot(
            snapshot_id=str(uuid.uuid4()),
            snapshot_version=self.snapshot_version_counter,
            timestamp=snapshot_start,
            project_id=project_id,
            project_name=project_name,
            project_filter_applied=project_id is not None,
            included_project_ids=included_project_ids,
            view_mode=view_mode,
            tasks=tasks,
            agents=agents,
            messages=messages,
            timeline_events=all_events,
            decisions=decisions,
            artifacts=artifacts,
            metrics=metrics,
            start_time=timeline_start,
            end_time=timeline_end,
            duration_minutes=duration_minutes,
            task_dependency_graph=task_dependency_graph,
            quality_assessment=(
                self.load_quality_assessment(project_id) if project_id else None
            ),
            agent_communication_graph=agent_communication_graph,
            timezone="UTC",
        )

        snapshot_end = datetime.now(timezone.utc)
        elapsed_ms = (snapshot_end - snapshot_start).total_seconds() * 1000
        logger.info(
            f"Snapshot v{self.snapshot_version_counter} created in {elapsed_ms:.1f}ms: "
            f"{len(tasks)} tasks, {len(agents)} agents, "
            f"{len(messages)} messages"
        )

        return snapshot

    def _load_projects(self) -> List[Dict[str, Any]]:
        """Load projects from all marcus_roots with caching.

        Merges projects.json from every root in self.marcus_roots. Roots
        with no projects.json are silently skipped. Each project's source
        root is recorded in self._project_root for downstream use.
        """
        # Check cache first
        now = datetime.now(timezone.utc)
        if (
            self._projects_cache is not None
            and self._projects_cache_time is not None
            and (now - self._projects_cache_time).total_seconds()
            < self._projects_cache_ttl
        ):
            return self._projects_cache

        all_projects: List[Dict[str, Any]] = []
        project_root_map: Dict[str, Path] = {}

        for root in self.marcus_roots:
            projects_file = root / "data" / "marcus_state" / "projects.json"
            if not projects_file.exists():
                logger.debug(f"Projects file not found (skipping): {projects_file}")
                continue
            try:
                with open(projects_file, "r") as f:
                    data = json.load(f)
                for key, value in data.items():
                    if key != "active_project" and isinstance(value, dict):
                        if "id" in value:
                            if (
                                self.history_cutoff_date
                                and value.get("created_at", "")
                                < self.history_cutoff_date
                            ):
                                continue
                            all_projects.append(value)
                            project_root_map[value["id"]] = root
            except Exception as e:
                logger.error(f"Error loading projects from {projects_file}: {e}")

        self._project_root = project_root_map
        self._projects_cache = all_projects
        self._projects_cache_time = now

        logger.info(
            f"Loaded {len(all_projects)} projects from {len(self.marcus_roots)} root(s)"
        )
        return all_projects

    def get_active_project_id(self) -> Optional[str]:
        """
        Get the ID of the currently active project from projects.json.

        Returns
        -------
        Optional[str]
            Active project ID if set, None otherwise
        """
        projects_file = self.persistence_dir / "projects.json"
        if not projects_file.exists():
            return None

        try:
            with open(projects_file, "r") as f:
                data = json.load(f)
                active_project = data.get("active_project", {})
                return active_project.get("project_id")  # type: ignore[no-any-return]
        except Exception as e:
            logger.error(f"Error loading active project: {e}")
            return None

    def get_parent_task_ids_from_conversations(
        self, project_id: str, board_id: str
    ) -> set[str]:
        """
        Query conversation logs (events) to find parent task IDs for a project.

        Parent tasks are logged in the events collection with
        project_id and board_id context. This method queries the
        marcus.db events to find all parent tasks (tasks without
        '_sub_' in ID) associated with the given project.

        Parameters
        ----------
        project_id : str
            The Marcus project UUID
        board_id : str
            The Planka board ID

        Returns
        -------
        set[str]
            Set of parent task IDs associated with this project
        """
        parent_task_ids: set[str] = set()

        db_path = self.marcus_root / "data" / "marcus.db"
        if not db_path.exists():
            logger.warning(f"marcus.db not found at {db_path}")
            return parent_task_ids

        try:
            with sqlite3.connect(str(db_path)) as conn:
                cursor = conn.cursor()

                # Query events with task_assignment or task_request
                cursor.execute("""
                    SELECT data
                    FROM persistence
                    WHERE collection = 'events'
                    """)

                for (event_data_str,) in cursor.fetchall():
                    try:
                        event_data = json.loads(event_data_str)

                        # Check if event has data with project/board context
                        event_payload = event_data.get("data", {})

                        # Check for project_id or board_id match
                        event_project_id = event_payload.get("project_id")
                        event_board_id = event_payload.get("board_id")

                        if event_project_id == project_id or event_board_id == board_id:
                            # Extract task_id if it's a parent task
                            task_id = event_payload.get("task_id")
                            if task_id and "_sub_" not in str(task_id):
                                parent_task_ids.add(str(task_id))

                    except (json.JSONDecodeError, KeyError):
                        continue

                logger.info(
                    f"Found {len(parent_task_ids)} parent task IDs"
                    f" from conversation logs for project {project_id}"
                )

        except Exception as e:
            logger.error(f"Error querying conversation logs: {e}")

        return parent_task_ids

    def _resolve_slug_dependencies(
        self, tasks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Resolve slug-based dependency IDs to actual task IDs.

        Parent tasks use human-readable slugs (e.g., 'design_time_management')
        for dependencies, but we need numeric Planka IDs for the dependency graph.

        Parameters
        ----------
        tasks : list[dict[str, Any]]
            List of tasks with potentially slug-based dependency_ids

        Returns
        -------
        list[dict[str, Any]]
            Tasks with resolved dependency IDs
        """
        # Build slug-to-ID mapping from all tasks
        slug_to_id: dict[str, str] = {}
        for task in tasks:
            task_name = task.get("name", "")
            task_id = str(task.get("id", ""))
            if task_name and task_id:
                # Generate multiple slug variations to match Marcus's formats

                # Format 1: Simple slug (e.g., "design_productivity_tools")
                simple_slug = task_name.lower().replace(" ", "_")
                slug_to_id[simple_slug] = task_id

                # Format 2: Marcus task slug format
                # Regular tasks: task_{feature-name}_{task-type}
                # NFR tasks: nfr_task_nfr-{requirement-name}
                labels = task.get("labels", [])
                task_type = labels[0].lower() if labels else None

                # Try to extract feature name by removing task type prefix from name
                if task_type and task_name.lower().startswith(task_type):
                    # Remove task type prefix
                    # e.g., "Implement Pomodoro Timer" -> "Pomodoro Timer"
                    feature_part = task_name[len(task_type) :].strip()
                    feature_slug = feature_part.lower().replace(" ", "-")

                    # Generate slug: task_{feature}_{type}
                    marcus_slug = f"task_{feature_slug}_{task_type}"
                    slug_to_id[marcus_slug] = task_id
                    # Also underscore variant (Marcus uses both)
                    underscore_slug = (
                        f"task_{feature_slug.replace('-', '_')}_{task_type}"
                    )
                    slug_to_id[underscore_slug] = task_id
                    # Compact variant (no separators in feature)
                    compact_slug = (
                        f"task_{feature_part.lower().replace(' ', '')}_{task_type}"
                    )
                    slug_to_id[compact_slug] = task_id
                    logger.debug(
                        f"Generated Marcus slug '{marcus_slug}' for task '{task_name}'"
                    )

                    # Also generate NFR slug variant for tasks that look like NFRs
                    # (e.g., "Implement Usability" → "nfr_task_nfr-usability")
                    if feature_part and not any(
                        keyword in feature_part.lower()
                        for keyword in ["timer", "pomodoro", "session", "control"]
                    ):
                        nfr_slug = f"nfr_task_nfr-{feature_slug}"
                        slug_to_id[nfr_slug] = task_id
                        logger.debug(
                            f"Generated NFR slug '{nfr_slug}' for task '{task_name}'"
                        )

        logger.info(f"Built slug-to-ID mapping with {len(slug_to_id)} entries")

        # Resolve dependency IDs
        resolutions = 0
        for task in tasks:
            dependency_ids = task.get("dependencies", [])
            if not dependency_ids:
                continue

            resolved_deps: list[str] = []
            for dep_id in dependency_ids:
                dep_str = str(dep_id)

                # Check if this is a slug (non-numeric)
                if not dep_str.isdigit():
                    # Try to resolve slug to actual ID
                    if dep_str in slug_to_id:
                        resolved_id = slug_to_id[dep_str]
                        resolved_deps.append(resolved_id)
                        resolutions += 1
                        logger.debug(
                            f"Resolved dependency '{dep_str}' → '{resolved_id}' "
                            f"for task {task.get('name', 'Unknown')}"
                        )
                    else:
                        # Slug not found, keep original
                        logger.warning(
                            f"Could not resolve dependency slug '{dep_str}' "
                            f"for task {task.get('name', 'Unknown')}"
                        )
                        resolved_deps.append(dep_str)
                else:
                    # Already numeric ID, keep as-is
                    resolved_deps.append(dep_str)

            # Update task with resolved dependencies
            task["dependencies"] = resolved_deps
            task["dependency_ids"] = resolved_deps

        logger.info(f"Resolved {resolutions} slug-based dependencies to task IDs")
        return tasks

    def _add_parent_dependencies(
        self, tasks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Add parent task IDs to subtask dependency_ids for graph layering.

        Subtasks need to depend on their parent tasks to create proper
        graph hierarchy and dependency arrows.

        Parameters
        ----------
        tasks : list[dict[str, Any]]
            List of task dictionaries

        Returns
        -------
        list[dict[str, Any]]
            Tasks with updated dependency_ids
        """
        enriched_count = 0

        for task in tasks:
            parent_task_id = task.get("parent_task_id")

            # Only process subtasks that have a parent
            if not parent_task_id:
                continue

            # Get or initialize dependency_ids
            dependency_ids = task.get("dependency_ids", [])
            if not isinstance(dependency_ids, list):
                dependency_ids = []

            # Add parent to dependencies if not already present
            parent_id_str = str(parent_task_id)
            if parent_id_str not in [str(d) for d in dependency_ids]:
                dependency_ids.append(parent_id_str)
                task["dependency_ids"] = dependency_ids
                enriched_count += 1

        logger.info(
            f"Added parent dependencies to {enriched_count} subtasks "
            f"for graph layering"
        )
        return tasks

    def _inherit_parent_dependencies_to_first_subtask(
        self, tasks: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Inherit parent dependencies to ONLY the first subtask for clean visualization.

        The first subtask is identified as the one with no inter-subtask dependencies.
        This creates a clean dependency flow:
        Parent Dependencies -> First Subtask -> Other Subtasks.

        Parameters
        ----------
        tasks : list[dict[str, Any]]
            List of task dictionaries

        Returns
        -------
        list[dict[str, Any]]
            Tasks with updated dependencies
        """
        # Build mapping of parent_id → parent task
        parent_tasks = {str(t["id"]): t for t in tasks if not t.get("parent_task_id")}

        # Group subtasks by their parent
        subtasks_by_parent: dict[str, list[dict[str, Any]]] = {}
        for task in tasks:
            parent_id = task.get("parent_task_id")
            if parent_id:
                parent_id_str = str(parent_id)
                if parent_id_str not in subtasks_by_parent:
                    subtasks_by_parent[parent_id_str] = []
                subtasks_by_parent[parent_id_str].append(task)

        inherited_count = 0

        # For each parent with subtasks
        for parent_id_str, subtasks in subtasks_by_parent.items():
            parent_task = parent_tasks.get(parent_id_str)
            if not parent_task:
                continue

            parent_deps = parent_task.get("dependencies", [])
            if not parent_deps:
                continue  # Parent has no dependencies to inherit

            # Find the "first" subtask - the one with NO dependencies
            # This is the subtask that can start immediately after parent deps
            first_subtasks = [t for t in subtasks if not t.get("dependencies", [])]

            if not first_subtasks:
                # No subtask with empty dependencies, skip
                continue

            # Use the first one found (there should typically be only one)
            first_subtask = first_subtasks[0]

            # Add parent dependencies to first subtask
            subtask_deps = list(first_subtask.get("dependencies", []))

            # Add parent's dependencies to first subtask (avoid duplicates)
            for dep in parent_deps:
                dep_str = str(dep)
                if dep_str not in [str(d) for d in subtask_deps]:
                    subtask_deps.append(dep_str)
                    inherited_count += 1

            first_subtask["dependencies"] = subtask_deps

        logger.info(
            "Inherited parent dependencies to "
            f"{len([s for s in subtasks_by_parent.values() if s])} "
            f"first subtasks ({inherited_count} dependencies added)"
        )
        return tasks

    def _load_tasks_for_project_fast(
        self, project_id: str, root: Path
    ) -> Optional[List[Dict[str, Any]]]:
        """Return enriched tasks for one project without scanning every row.

        Strategy:
        1. Resolve candidate parent IDs from three cheap sources (SQL filter
           on metadata.project_id, conversation logs, Planka prefix range).
        2. SQL-fetch only those parents from ``task_metadata``.
        3. Read mtime-cached ``subtasks.json``, filter by ``parent_task_id``.
        4. SQL-fetch outcomes/timings filtered to the candidate task IDs.
        5. Enrich + resolve dependencies, return.

        Returns ``None`` to signal "fall back to the slow global path"
        when no candidates can be resolved (e.g. unfamiliar project layout).
        """
        try:
            # Step 1: Gather candidate parent task IDs from the same sources
            # the slow global path uses, so visible task counts match.
            # NOTE: deliberately NOT querying ``WHERE project_id = ?`` on
            # task_metadata — that catches Marcus-internal tasks (e.g.
            # ``planning_*``, About-cards) that the slow path has always
            # filtered out. Adding them here would change visible counts.
            candidates: Set[str] = set()

            # Resolve planka_board_id if available
            projects_data = self._load_projects()
            project_info = next(
                (p for p in projects_data if p.get("id") == project_id), None
            )
            planka_board_id = ""
            planka_project_id = ""
            if project_info and "provider_config" in project_info:
                planka_board_id = project_info["provider_config"].get("board_id", "")
                planka_project_id = project_info["provider_config"].get(
                    "project_id", ""
                )

            # Source A: conversation logs (cheap; reuses message scan)
            try:
                conv_ids = self.get_parent_task_ids_from_conversations(
                    project_id=project_id, board_id=planka_board_id
                )
                candidates |= {str(i) for i in conv_ids}
            except Exception as e:
                logger.debug(f"Conversation parent-id lookup failed: {e}")

            # Determine whether the slow path would take the Planka prefix
            # match or its no-prefix project_id fallback. This gate matches
            # aggregator.py:1656 — non-numeric Planka IDs fall back.
            # A genuine Planka ID is a fully-numeric string. A SQLite project
            # ID is a 32-char hex UUID. Testing only ``pid[:8]`` misclassified
            # hex UUIDs whose first 8 chars happen to be all decimal digits
            # (e.g. "10519982...") as numeric Planka IDs, routing them down the
            # prefix-match path that silently drops every hex task ID. Require
            # the whole string to be numeric.
            has_numeric_planka = any(
                pid and pid.isdigit() for pid in (planka_board_id, planka_project_id)
            )

            if has_numeric_planka:
                # Source B: Planka prefix fuzzy match (numeric IDs only)
                candidates |= self._query_parent_ids_by_planka_prefix(
                    planka_board_id, planka_project_id, root
                )
            else:
                # Source B': project_id field match — slow-path fallback for
                # hex-Planka projects (board_id like "f3ae1ca0..."). Without
                # this every parent except the few in conversations is missing.
                candidates |= self._query_parent_ids_by_project_metadata(
                    project_id, root
                )
                if planka_project_id and planka_project_id != project_id:
                    candidates |= self._query_parent_ids_by_project_metadata(
                        planka_project_id, root
                    )

            # Source C: parent IDs of subtasks that match Planka prefix.
            # Replicates the slow path's "matched subtask reveals parent"
            # heuristic without iterating all 5k subtasks for hex projects.
            if planka_board_id or planka_project_id:
                prefixes: List[int] = []
                for pid in (planka_board_id, planka_project_id):
                    if pid and len(pid) >= 8:
                        try:
                            prefixes.append(int(pid[:8]))
                        except ValueError:
                            pass
                if prefixes:
                    for sub in self._load_subtasks_json_cached(root):
                        sub_id = str(sub.get("id", ""))
                        if not sub_id or len(sub_id) < 8 or not sub_id[0].isdigit():
                            continue
                        try:
                            sub_prefix = int(sub_id[:8])
                        except ValueError:
                            continue
                        if any(abs(sub_prefix - p) <= 20 for p in prefixes):
                            parent_id = sub.get("parent_task_id")
                            if parent_id:
                                candidates.add(str(parent_id))

            if not candidates:
                logger.info(
                    f"Fast path: no candidate parent IDs for project {project_id}, "
                    "falling back to global load"
                )
                return None

            # Step 2: Fetch parent task rows for the candidate IDs, then
            # follow ``dependencies`` edges to pull in any referenced parents
            # that didn't surface via conversations / Planka prefix. Without
            # this expansion, an unstarted task whose dependent appears first
            # in the logs would be a dangling edge target → orphan-filtered
            # out of the DAG. The slow path does the same walk inline
            # (aggregator.py:1771-1789).
            parent_tasks = self._load_parent_tasks_by_ids(candidates, root)
            seen_ids = set(candidates)
            for _ in range(8):  # bounded BFS over dep chain depth
                new_dep_ids: Set[str] = set()
                for task in parent_tasks:
                    deps = task.get("dependencies") or task.get("dependency_ids") or []
                    for dep in deps:
                        dep_str = str(dep)
                        if dep_str and dep_str not in seen_ids:
                            new_dep_ids.add(dep_str)
                if not new_dep_ids:
                    break
                seen_ids |= new_dep_ids
                extra = self._load_parent_tasks_by_ids(new_dep_ids, root)
                if not extra:
                    break
                parent_tasks.extend(extra)
            candidates = seen_ids

            # Step 3: Read subtasks.json (mtime-cached) and filter by parent
            all_subtasks = self._load_subtasks_json_cached(root)
            candidate_id_strs = {str(c) for c in candidates}
            project_subtasks = [
                t
                for t in all_subtasks
                if str(t.get("parent_task_id") or "") in candidate_id_strs
            ]

            project_tasks = parent_tasks + project_subtasks

            # Step 4: SQL-fetch outcomes+timings filtered to project tasks
            project_task_ids: Set[str] = set()
            for t in project_tasks:
                tid = str(t.get("id") or t.get("task_id") or "")
                if tid:
                    project_task_ids.add(tid)
            outcomes_by_task, timings_by_task = self._load_outcomes_and_timings_for_ids(
                project_task_ids, root
            )

            # Step 5: Apply enrichment in-place (subset of enrich_tasks_with_timing)
            # Populate started_at/completed_at AND created_at/updated_at — the
            # former so getTaskStateAtTime + synth see the real per-task start
            # (without it, every same-dep sibling collapses onto latest_dep_end).
            for task in project_tasks:
                task_id = str(task.get("id") or task.get("task_id") or "")
                outcome = outcomes_by_task.get(task_id)
                if outcome:
                    task["actual_hours"] = outcome["actual_hours"]
                    if outcome.get("started_at"):
                        task["created_at"] = outcome["started_at"]
                        task["started_at"] = outcome["started_at"]
                    if outcome.get("completed_at"):
                        task["updated_at"] = outcome["completed_at"]
                        task["completed_at"] = outcome["completed_at"]
                timing = timings_by_task.get(task_id)
                if timing:
                    if "start_time" in timing:
                        task["created_at"] = timing["start_time"]
                        task["started_at"] = timing["start_time"]
                    if "end_time" in timing:
                        task["updated_at"] = timing["end_time"]
                        task["completed_at"] = timing["end_time"]
                    if "duration_hours" in timing:
                        task["actual_hours"] = timing["duration_hours"]

            # Synthetic timestamps for incomplete tasks (mirrors
            # enrich_tasks_with_timing's tail logic)
            now_iso = datetime.now(timezone.utc).isoformat()
            for task in project_tasks:
                status = (task.get("status") or "").lower()
                if status in (
                    "todo",
                    "pending",
                    "in-progress",
                    "in_progress",
                    "blocked",
                ):
                    if not task.get("created_at"):
                        task["created_at"] = now_iso
                    if not task.get("updated_at"):
                        task["updated_at"] = task["created_at"]

            logger.info(
                f"Fast path: {len(parent_tasks)} parents + "
                f"{len(project_subtasks)} subtasks for project {project_id}"
            )

            # Resolve slug-based dependencies (cheap on the small set)
            return self._resolve_slug_dependencies(project_tasks)

        except Exception as e:
            logger.warning(
                f"Fast path failed for project {project_id}, falling back: {e}"
            )
            return None

    def _load_tasks(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Load tasks from subtasks.json and marcus.db, optionally filtered by project.

        Parent tasks are stored in marcus.db (task_metadata collection).
        Subtasks are stored in subtasks.json.
        This method loads and merges both to provide a complete task list.
        """
        all_tasks = []

        # Resolve which root owns this project (populated by _load_projects).
        # Falls back to primary root when called without a project_id or when
        # _project_root hasn't been populated yet (e.g. direct test calls).
        project_root = (
            self._project_root.get(project_id, self.marcus_root)
            if project_id
            else self.marcus_root
        )

        # Project-scoped fast path: SQL-filter parents and outcomes to the
        # candidate ID set, read subtasks.json from cache, enrich just this
        # project's ~10-100 tasks. Avoids the global 11k-row load+enrich.
        if project_id:
            fast_tasks = self._load_tasks_for_project_fast(
                project_id=project_id, root=project_root
            )
            if fast_tasks is not None:
                return fast_tasks

        # Load parent tasks from database for this project's root
        parent_tasks = self.load_parent_tasks_from_db(root=project_root)
        all_tasks.extend(parent_tasks)
        logger.info(f"Loaded {len(parent_tasks)} parent tasks from database")

        # Load subtasks from JSON file for this project's root
        subtasks_file = project_root / "data" / "marcus_state" / "subtasks.json"
        if subtasks_file.exists():
            try:
                with open(subtasks_file, "r") as f:
                    data = json.load(f)

                    # Handle nested format: {"subtasks": {task_id: task_data}}
                    if isinstance(data, dict) and "subtasks" in data:
                        subtasks = list(data["subtasks"].values())
                    # Handle both formats: {task_id: task_data} or [task1, task2, ...]
                    elif isinstance(data, dict):
                        subtasks = list(data.values())
                    else:
                        subtasks = data

                    all_tasks.extend(subtasks)
                    logger.info(f"Loaded {len(subtasks)} subtasks from subtasks.json")
            except Exception as e:
                logger.error(f"Error loading subtasks.json: {e}")
        else:
            logger.warning(f"Subtasks file not found: {subtasks_file}")

        logger.info(f"Total tasks loaded (before filtering): {len(all_tasks)}")

        try:

            # Enrich tasks with actual timing data from marcus.db before filtering
            all_tasks = self.enrich_tasks_with_timing(all_tasks)

            # Pre-bind project_info so the unfiltered fallback path below can
            # safely reference it when project_id is None.
            project_info: Optional[Dict[str, Any]] = None

            # Filter by project using fuzzy matching (±20 range)
            # Planka creates task IDs that are offset from board IDs
            if project_id:
                # Load projects to get Planka board/project ID mapping (uses cache)
                projects_data = self._load_projects()
                project_info = next(
                    (p for p in projects_data if p.get("id") == project_id), None
                )

                if project_info and "provider_config" in project_info:
                    planka_project_id = project_info["provider_config"].get(
                        "project_id", ""
                    )
                    planka_board_id = project_info["provider_config"].get(
                        "board_id", ""
                    )

                    if planka_project_id or planka_board_id:
                        # Pre-compute target prefixes for efficient comparison
                        target_prefixes = []
                        for id_to_check in [planka_board_id, planka_project_id]:
                            if id_to_check and len(id_to_check) >= 8:
                                try:
                                    target_prefixes.append(int(id_to_check[:8]))
                                except ValueError:
                                    pass

                        if not target_prefixes:
                            logger.warning(
                                "No valid Planka ID prefixes for "
                                f"project {project_id} - "
                                "using fallback project_id field matching"
                            )
                            # Fallback: filter by task's project_id field
                            # Match Marcus registry ID or provider-level ID
                            match_ids = {project_id}
                            if planka_project_id:
                                match_ids.add(planka_project_id)
                            filtered_tasks = [
                                t for t in all_tasks if t.get("project_id") in match_ids
                            ]
                            # Also include subtasks whose parent was matched
                            matched_ids = {str(t.get("id", "")) for t in filtered_tasks}
                            for task in all_tasks:
                                parent_id = str(task.get("parent_task_id") or "")
                                task_id = str(task.get("id", ""))
                                if (
                                    parent_id
                                    and parent_id in matched_ids
                                    and task_id not in matched_ids
                                ):
                                    filtered_tasks.append(task)
                                    matched_ids.add(task_id)
                            logger.info(
                                f"Fallback filtering: "
                                f"{len(filtered_tasks)}"
                                f"/{len(all_tasks)} tasks "
                                f"(match_ids={match_ids})"
                            )
                            if filtered_tasks:
                                filtered_tasks = self._resolve_slug_dependencies(
                                    filtered_tasks
                                )
                            return filtered_tasks if filtered_tasks else []

                        # Get project creation time for filtering
                        if project_info and "created_at" in project_info:
                            try:
                                datetime.fromisoformat(
                                    project_info["created_at"].replace("Z", "+00:00")
                                )
                            except Exception:
                                pass

                        # First pass: Filter subtasks using Planka fuzzy ID matching
                        # and collect their parent IDs
                        filtered_tasks = []
                        parent_ids_to_include = set()

                        for task in all_tasks:
                            parent_task_id = task.get("parent_task_id")
                            is_parent_task = parent_task_id is None and not task.get(
                                "is_subtask", False
                            )

                            # Check if this parent task has subtasks
                            has_subtasks = False
                            if is_parent_task:
                                task_id = str(task.get("id", ""))
                                has_subtasks = any(
                                    str(t.get("parent_task_id")) == task_id
                                    for t in all_tasks
                                )

                            # Skip parent tasks WITH subtasks in first pass
                            # (their subtasks will represent them)
                            # But include parent tasks WITHOUT subtasks via ID matching
                            if is_parent_task and has_subtasks:
                                continue

                            # Check the task's own ID for matching
                            id_to_check = str(task.get("id", ""))

                            # Early exit: Skip non-Planka IDs
                            if (
                                not id_to_check
                                or len(id_to_check) < 8
                                or not id_to_check[0].isdigit()
                            ):
                                continue

                            try:
                                id_prefix = int(id_to_check[:8])

                                # Check distance to any target prefix
                                for target_prefix in target_prefixes:
                                    distance = abs(id_prefix - target_prefix)
                                    if distance <= 20:
                                        filtered_tasks.append(task)
                                        logger.debug(
                                            f"Matched task {id_to_check} "
                                            f"(distance={distance} "
                                            f"from {target_prefix}): "
                                            f"{task.get('name', 'Unknown')}"
                                        )
                                        # Extract parent ID from subtask ID
                                        task_id_str = str(task.get("id", ""))
                                        if "_sub_" in task_id_str:
                                            parent_id = task_id_str.split("_sub_")[0]
                                            parent_ids_to_include.add(parent_id)
                                        break  # Found match
                            except ValueError:
                                # Fallback: try string prefix match
                                for planka_id in [planka_board_id, planka_project_id]:
                                    if planka_id and id_to_check.startswith(
                                        planka_id[:8]
                                    ):
                                        filtered_tasks.append(task)
                                        # Extract parent ID from subtask ID
                                        task_id_str = str(task.get("id", ""))
                                        if "_sub_" in task_id_str:
                                            parent_id = task_id_str.split("_sub_")[0]
                                            parent_ids_to_include.add(parent_id)
                                        break

                        # Include subtasks from subtasks.json whose parent was matched
                        # (hex UUID subtask IDs fail the Planka numeric filter above)
                        matched_ids = {str(t.get("id", "")) for t in filtered_tasks}
                        for task in all_tasks:
                            parent_id = str(task.get("parent_task_id") or "")
                            if not parent_id:
                                continue
                            task_id = str(task.get("id", ""))
                            if parent_id in matched_ids and task_id not in matched_ids:
                                filtered_tasks.append(task)
                                matched_ids.add(task_id)
                                if "_sub_" in task_id:
                                    parent_ids_to_include.add(parent_id)

                        # Collect dependency IDs from matched tasks
                        dependency_ids_to_include: set[str] = set()
                        for task in filtered_tasks:
                            # Check both 'dependencies' and 'dependency_ids' keys
                            deps = task.get("dependencies", []) or task.get(
                                "dependency_ids", []
                            )
                            if deps:
                                dependency_ids_to_include.update(str(d) for d in deps)
                                logger.info(
                                    "Task '%s' has dependencies: %s"
                                    % (task.get("name"), deps)
                                )

                        logger.info(
                            f"Collected {len(dependency_ids_to_include)}"
                            " dependency IDs: "
                            # Show first 10
                            f"{list(dependency_ids_to_include)[:10]}"
                        )

                        # Get parent task IDs from conversation logs
                        # Authoritative source for project->parent mapping
                        conversation_parent_ids = (
                            self.get_parent_task_ids_from_conversations(
                                project_id=project_id, board_id=planka_board_id
                            )
                        )

                        logger.info(
                            f"Collected {len(parent_ids_to_include)}"
                            " parent IDs from subtasks, "
                            f"{len(dependency_ids_to_include)}"
                            " from dependencies, "
                            f"{len(conversation_parent_ids)}"
                            " from conversation logs"
                        )

                        # Second pass: Include parent tasks whose IDs were referenced
                        # Priority: conversations > subtasks > deps
                        # Build set of already included task IDs to avoid duplicates
                        already_included_ids = {
                            str(t.get("id", "")) for t in filtered_tasks
                        }

                        parent_tasks_added = []
                        for task in all_tasks:
                            parent_task_id = task.get("parent_task_id")
                            is_parent_task = parent_task_id is None and not task.get(
                                "is_subtask", False
                            )

                            if is_parent_task:
                                task_id = str(task.get("id", ""))
                                task_name = task.get("name", "Unknown")

                                # Skip if already included in first pass
                                if task_id in already_included_ids:
                                    continue

                                # Include if referenced by logs/subtasks/deps
                                if (
                                    task_id in conversation_parent_ids
                                    or task_id in parent_ids_to_include
                                    or task_id in dependency_ids_to_include
                                ):
                                    filtered_tasks.append(task)
                                    source = []
                                    if task_id in conversation_parent_ids:
                                        source.append("conversations")
                                    if task_id in parent_ids_to_include:
                                        source.append("subtasks")
                                    if task_id in dependency_ids_to_include:
                                        source.append("dependencies")
                                    parent_tasks_added.append(
                                        (task_name, task_id, source)
                                    )
                                    logger.info(
                                        f"Including parent task "
                                        f"'{task_name}' ({task_id})"
                                        f" via {', '.join(source)}"
                                    )
                                else:
                                    logger.debug(
                                        f"Skipping parent task "
                                        f"'{task_name}' ({task_id})"
                                        " - not in any inclusion set"
                                    )

                        logger.info(
                            f"Added {len(parent_tasks_added)} parent"
                            " tasks to filtered list"
                        )

                        logger.info(
                            f"Filtered {len(filtered_tasks)}/{len(all_tasks)} tasks "
                            f"for project {project_id} (Planka ID: {planka_project_id})"
                        )

                        # Resolve slug-based dependencies to actual task IDs
                        filtered_tasks = self._resolve_slug_dependencies(filtered_tasks)

                        return filtered_tasks
                    else:
                        logger.info(
                            f"Project {project_id} has no Planka IDs, "
                            f"trying project_id field match"
                        )
                else:
                    logger.info(
                        f"Project {project_id} no provider_config, "
                        f"trying project_id field match"
                    )

            # Fallback: match by project_id field on tasks
            # For SQLite/non-Planka providers, the task's project_id
            # is the kanban-level ID (from auto_setup_project), not
            # the Marcus registry ID. Check both.
            provider_project_id = None
            if project_info and project_info.get("provider_config"):
                provider_project_id = project_info["provider_config"].get("project_id")

            if project_id:
                match_ids = {project_id}
                if provider_project_id:
                    match_ids.add(provider_project_id)

                matched = [t for t in all_tasks if t.get("project_id") in match_ids]
                if matched:
                    logger.info(
                        f"project_id field matched "
                        f"{len(matched)}/{len(all_tasks)} tasks "
                        f"(match_ids={match_ids})"
                    )
                    matched = self._resolve_slug_dependencies(matched)
                    return matched

                # Project specified, no match — return empty
                logger.info(
                    f"No tasks matched project {project_id} "
                    f"(also tried provider_id={provider_project_id})"
                )
                return []

            logger.info(f"Loaded {len(all_tasks)} tasks (all projects)")
            return all_tasks

        except Exception as e:
            logger.error(f"Error loading tasks: {e}")
            return []

    @staticmethod
    def _normalize_realtime_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a realtime_*.jsonl entry to conversation log format.

        realtime logs use {type, source, target, agent_id, ...} while the
        aggregator expects {from_agent_id, to_agent_id, message_type, ...}.
        This method adds the expected fields without removing originals.

        Parameters
        ----------
        entry : Dict[str, Any]
            Raw entry from a realtime_*.jsonl file.

        Returns
        -------
        Dict[str, Any]
            Entry with added from_agent_id, to_agent_id, message_type fields.
        """
        normalized = dict(entry)
        event_type = entry.get("type", "")

        normalized["message_type"] = event_type

        if event_type == "task_assignment":
            normalized["to_agent_id"] = entry.get("agent_id") or entry.get("target")
            normalized.setdefault("from_agent_id", entry.get("source"))
        elif event_type in {
            "task_request",
            "task_progress",
            "task_completion",
            "task_blocked",
        }:
            normalized["from_agent_id"] = (
                entry.get("agent_id") or entry.get("worker_id") or entry.get("source")
            )
            normalized.setdefault("to_agent_id", entry.get("target"))
        else:
            # Non-agent events (server_startup, ping_*) get None agent IDs
            normalized.setdefault("from_agent_id", None)
            normalized.setdefault("to_agent_id", None)

        return normalized

    def _load_messages(self) -> List[Dict[str, Any]]:
        """Load conversation messages from logs.

        Loads both conversations_*.jsonl and realtime_*.jsonl files from all
        marcus_roots. Realtime entries are normalized to the aggregator's expected
        field names via _normalize_realtime_entry().

        Cached across calls; invalidated when any post-cutoff file's
        path/mtime/size changes.
        """
        # Collect post-cutoff candidate files (cheap filename filter, no stat)
        candidate_files: List[Path] = []
        for root in self.marcus_roots:
            logs_dir = root / "logs" / "conversations"
            if not logs_dir.exists():
                logger.warning(f"Conversation logs dir not found: {logs_dir}")
                continue
            candidate_files.extend(self._iter_log_files_post_cutoff(logs_dir))

        # Cache check: stat surviving files only (e.g. ~1.7k vs 50k total)
        signature = self._dir_signature(candidate_files)
        if (
            self._messages_cache is not None
            and self._messages_cache_signature == signature
        ):
            logger.info(
                f"[timing] _load_messages: cache hit "
                f"({len(self._messages_cache)} messages, "
                f"{len(candidate_files)} files)"
            )
            return self._messages_cache

        messages: List[Dict[str, Any]] = []
        for log_file in candidate_files:
            try:
                if log_file.stat().st_size == 0:
                    continue
            except OSError:
                continue
            is_realtime = log_file.name.startswith("realtime_")
            try:
                with open(log_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            if is_realtime and "from_agent_id" not in entry:
                                entry = self._normalize_realtime_entry(entry)
                            messages.append(entry)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.error(f"Error loading messages from {log_file}: {e}")

        self._messages_cache = messages
        self._messages_cache_signature = signature
        logger.info(
            f"Loaded {len(messages)} messages from "
            f"{len(candidate_files)} files (cache miss)"
        )
        return messages

    def _load_events(self) -> List[Dict[str, Any]]:
        """Load agent events from logs across all marcus_roots.

        Cached across calls; invalidated when any post-cutoff file's
        path/mtime/size changes.
        """
        candidate_files: List[Path] = []
        for root in self.marcus_roots:
            events_dir = root / "logs" / "agent_events"
            if not events_dir.exists():
                logger.warning(f"Agent events dir not found: {events_dir}")
                continue
            candidate_files.extend(self._iter_log_files_post_cutoff(events_dir))

        signature = self._dir_signature(candidate_files)
        if self._events_cache is not None and self._events_cache_signature == signature:
            logger.info(
                f"[timing] _load_events: cache hit "
                f"({len(self._events_cache)} events, "
                f"{len(candidate_files)} files)"
            )
            return self._events_cache

        events: List[Dict[str, Any]] = []
        for log_file in candidate_files:
            try:
                if log_file.stat().st_size == 0:
                    continue
            except OSError:
                continue
            try:
                with open(log_file, "r") as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            events.append(entry)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.error(f"Error loading events from {log_file}: {e}")

        self._events_cache = events
        self._events_cache_signature = signature
        logger.info(
            f"Loaded {len(events)} events from "
            f"{len(candidate_files)} files (cache miss)"
        )
        return events

    def _load_decisions(
        self,
        project_id: Optional[str] = None,
        project_task_ids: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Load decisions from Marcus persistence layer.

        Uses ProjectHistoryPersistence to access decisions from SQLite.
        Falls back to JSON files if unavailable.

        Parameters
        ----------
        project_id : Optional[str]
            Specific project to load decisions for (None = all projects)
        project_task_ids : Optional[Set[str]]
            Pre-computed task_ids for this project. When provided, skips
            Marcus's ``_get_task_ids_from_conversations`` re-glob (which has
            no cutoff filter and re-reads every conversation file).

        Returns
        -------
        List[Dict[str, Any]]
            List of decision dictionaries
        """
        # Resolve which root(s) actually own the data being asked for. A
        # project living in a non-primary root must be read from that root,
        # not from self.marcus_root (cato#50).
        if project_id:
            roots = [self._project_root.get(project_id, self.marcus_root)]
        else:
            roots = self.marcus_roots

        decisions: List[Dict[str, Any]] = []
        for root in roots:
            decisions.extend(
                self._load_decisions_for_root(root, project_id, project_task_ids)
            )

        logger.info(f"Loaded {len(decisions)} decisions")
        return decisions

    def _load_decisions_for_root(
        self,
        root: Path,
        project_id: Optional[str],
        project_task_ids: Optional[Set[str]],
    ) -> List[Dict[str, Any]]:
        """
        Load decisions for a single root, falling back to JSON for that root only.

        Each root's JSON fallback must be evaluated independently (cato#50
        follow-up): testing whether the *combined* result across every root
        is empty before falling back means a root whose persistence
        genuinely has nothing loses its JSON-only decisions the moment any
        other root succeeds via persistence.

        Parameters
        ----------
        root : Path
            The Marcus root to load from.
        project_id : Optional[str]
            Specific project to load decisions for (None = all projects
            whose history lives under this root).
        project_task_ids : Optional[Set[str]]
            Pre-computed task_ids for ``project_id``; see ``_load_decisions``.

        Returns
        -------
        List[Dict[str, Any]]
            Decisions found for this root, from persistence or its JSON fallback.
        """
        decisions: List[Dict[str, Any]] = []

        try:
            import asyncio

            persistence = self._get_marcus_persistence(root)
            if persistence is None:
                raise ImportError("Marcus persistence unavailable")

            def _load_for(pid: str) -> List[Dict[str, Any]]:
                if project_task_ids is not None and pid == project_id:
                    with self._override_task_ids(persistence, pid, project_task_ids):
                        objs = asyncio.run(persistence.load_decisions(pid))
                else:
                    objs = asyncio.run(persistence.load_decisions(pid))
                return [d.to_dict() for d in objs]

            if project_id:
                decisions.extend(_load_for(project_id))
            else:
                project_history_dir = root / "data" / "project_history"
                if project_history_dir.exists():
                    for project_dir in project_history_dir.iterdir():
                        if project_dir.is_dir():
                            try:
                                decisions.extend(_load_for(project_dir.name))
                            except Exception as e:
                                logger.debug(
                                    "Could not load decisions for "
                                    f"{project_dir.name}: {e}"
                                )

        except (ImportError, RuntimeError, Exception) as e:
            logger.debug(f"Marcus persistence not available or failed for {root}: {e}")

        if decisions:
            return decisions

        logger.debug(f"No decisions from persistence for {root}, trying JSON fallback")
        if project_id:
            json_path = (
                root / "data" / "project_history" / project_id / "decisions.json"
            )
            if json_path.exists():
                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)
                        decisions.extend(data.get("decisions", []))
                except Exception as json_error:
                    logger.warning(f"JSON fallback failed for decisions: {json_error}")
        else:
            project_history_dir = root / "data" / "project_history"
            if project_history_dir.exists():
                for project_dir in project_history_dir.iterdir():
                    if project_dir.is_dir():
                        json_path = project_dir / "decisions.json"
                        if json_path.exists():
                            try:
                                with open(json_path, "r") as f:
                                    data = json.load(f)
                                    decisions.extend(data.get("decisions", []))
                            except Exception as json_error:
                                logger.debug(
                                    f"Could not load decisions from "
                                    f"{json_path}: {json_error}"
                                )

        return decisions

    def _load_artifacts(
        self,
        project_id: Optional[str] = None,
        project_task_ids: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Load artifacts from Marcus persistence layer.

        Uses ProjectHistoryPersistence to access artifacts from SQLite.
        Falls back to JSON files if unavailable.

        Parameters
        ----------
        project_id : Optional[str]
            Specific project to load artifacts for (None = all projects)
        project_task_ids : Optional[Set[str]]
            Pre-computed task_ids for this project. Skips Marcus's
            conversation re-glob when supplied.

        Returns
        -------
        List[Dict[str, Any]]
            List of artifact metadata dictionaries
        """
        # Resolve which root(s) actually own the data being asked for (cato#50).
        if project_id:
            roots = [self._project_root.get(project_id, self.marcus_root)]
        else:
            roots = self.marcus_roots

        artifacts: List[Dict[str, Any]] = []
        for root in roots:
            artifacts.extend(
                self._load_artifacts_for_root(root, project_id, project_task_ids)
            )

        logger.info(f"Loaded {len(artifacts)} artifacts")
        return artifacts

    def _load_artifacts_for_root(
        self,
        root: Path,
        project_id: Optional[str],
        project_task_ids: Optional[Set[str]],
    ) -> List[Dict[str, Any]]:
        """
        Load artifacts for a single root, falling back to JSON for that root only.

        Each root's JSON fallback must be evaluated independently (cato#50
        follow-up): testing whether the *combined* result across every root
        is empty before falling back means a root whose persistence
        genuinely has nothing loses its JSON-only artifacts the moment any
        other root succeeds via persistence.

        Parameters
        ----------
        root : Path
            The Marcus root to load from.
        project_id : Optional[str]
            Specific project to load artifacts for (None = all projects
            whose history lives under this root).
        project_task_ids : Optional[Set[str]]
            Pre-computed task_ids for ``project_id``; see ``_load_artifacts``.

        Returns
        -------
        List[Dict[str, Any]]
            Artifacts found for this root, from persistence or its JSON fallback.
        """
        artifacts: List[Dict[str, Any]] = []

        try:
            import asyncio

            persistence = self._get_marcus_persistence(root)
            if persistence is None:
                raise ImportError("Marcus persistence unavailable")

            def _load_for(pid: str) -> List[Dict[str, Any]]:
                if project_task_ids is not None and pid == project_id:
                    with self._override_task_ids(persistence, pid, project_task_ids):
                        objs = asyncio.run(persistence.load_artifacts(pid))
                else:
                    objs = asyncio.run(persistence.load_artifacts(pid))
                return [a.to_dict() for a in objs]

            if project_id:
                artifacts.extend(_load_for(project_id))
            else:
                project_history_dir = root / "data" / "project_history"
                if project_history_dir.exists():
                    for project_dir in project_history_dir.iterdir():
                        if project_dir.is_dir():
                            try:
                                artifacts.extend(_load_for(project_dir.name))
                            except Exception as e:
                                logger.debug(
                                    "Could not load artifacts for "
                                    f"{project_dir.name}: {e}"
                                )

        except (ImportError, RuntimeError, Exception) as e:
            logger.debug(f"Marcus persistence not available or failed for {root}: {e}")

        if artifacts:
            return artifacts

        logger.debug(f"No artifacts from persistence for {root}, trying JSON fallback")
        if project_id:
            json_path = (
                root / "data" / "project_history" / project_id / "artifacts.json"
            )
            if json_path.exists():
                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)
                        artifacts.extend(data.get("artifacts", []))
                except Exception as json_error:
                    logger.warning(f"JSON fallback failed for artifacts: {json_error}")
        else:
            project_history_dir = root / "data" / "project_history"
            if project_history_dir.exists():
                for project_dir in project_history_dir.iterdir():
                    if project_dir.is_dir():
                        json_path = project_dir / "artifacts.json"
                        if json_path.exists():
                            try:
                                with open(json_path, "r") as f:
                                    data = json.load(f)
                                    artifacts.extend(data.get("artifacts", []))
                            except Exception as json_error:
                                logger.debug(
                                    f"Could not load artifacts from "
                                    f"{json_path}: {json_error}"
                                )

        return artifacts

    def _parse_ai_suggestions(self, content: str) -> Optional[Dict[str, Any]]:
        """Extract the AI suggestions dict from a blocker comment string."""
        import ast

        marker = "📋 AI Suggestions:\n"
        idx = content.find(marker)
        if idx == -1:
            return None
        raw = content[idx + len(marker) :].strip()
        # Try JSON first (new format after fence-stripping fix), then Python repr
        for parser in (json.loads, ast.literal_eval):
            try:
                result = parser(raw)
                if isinstance(result, dict):
                    return result
            except Exception:  # nosec B112
                continue
        return None

    def _classify_display_role(
        self, task_data: Dict[str, Any]
    ) -> Literal["work", "structural", "context"]:
        """
        Classify a task's display role for visualization.

        Returns
        -------
        str
            "structural" — design tasks that create fan-out topology
                           (ghost nodes in DAG, shown in Project Info drawer)
            "work" — all other tasks (full display everywhere)

        Note: Design tasks are checked BEFORE auto_completed because
        design tasks often have both labels. They need to be structural
        (ghost nodes) to preserve the DAG's diamond topology.
        """
        # name = task_data.get("name", "")
        labels = task_data.get("labels") or []
        # source_type = task_data.get("source_type", "")

        # Structural: design tasks that create fan-out topology
        # Must be checked BEFORE auto_completed — design tasks often have
        # both "design" and "auto_completed" labels, but they need to stay
        # in the DAG as ghost nodes to preserve the diamond shape.
        if task_data.get("type") == "design" or "design" in labels:
            return "structural"

        return "work"

    def _build_subtask_rollup(
        self, tasks: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Aggregate subtask timing per parent.

        Run before parents-view filtering removes subtasks. The returned
        map is applied by ``_apply_subtask_rollup`` after enrichment.
        """
        subtasks_by_parent: Dict[str, List[Dict[str, Any]]] = {}
        for t in tasks:
            pid = t.get("parent_task_id")
            if pid:
                subtasks_by_parent.setdefault(str(pid), []).append(t)

        # Fast path stores outcome timestamps in created_at/updated_at, not
        # started_at/completed_at — so accept either source per subtask.
        def _start(c: Dict[str, Any]) -> Optional[str]:
            return c.get("started_at") or c.get("created_at")

        def _is_done(c: Dict[str, Any]) -> bool:
            if c.get("completed_at"):
                return True
            return (c.get("status") or "").lower() in ("done", "completed")

        def _end(c: Dict[str, Any]) -> Optional[str]:
            # updated_at is a placeholder on incomplete subtasks — only treat
            # it as an end time once the child has actually completed,
            # otherwise an active parent gets marked completed.
            if not _is_done(c):
                return None
            return c.get("completed_at") or c.get("updated_at")

        rollup: Dict[str, Dict[str, Any]] = {}
        for pid, children in subtasks_by_parent.items():
            starts = [s for c in children if (s := _start(c))]
            # completed_at only rolls up when every child is done — a partial
            # rollup would mark a still-active parent completed.
            all_done = all(_is_done(c) for c in children)
            ends = [e for c in children if (e := _end(c))] if all_done else []
            hours = [
                c["actual_hours"]
                for c in children
                if isinstance(c.get("actual_hours"), (int, float))
            ]
            agent_counts: Dict[str, int] = {}
            for c in children:
                aid = c.get("assigned_agent_id")
                if aid:
                    agent_counts[aid] = agent_counts.get(aid, 0) + 1

            rollup[pid] = {
                "started_at": min(starts) if starts else None,
                "completed_at": max(ends) if ends else None,
                "actual_hours": sum(hours) if hours else None,
                "assigned_agent_id": (
                    max(agent_counts, key=lambda k: agent_counts[k])
                    if agent_counts
                    else None
                ),
            }
        return rollup

    def _apply_subtask_rollup(
        self,
        tasks: List[Dict[str, Any]],
        rollup: Dict[str, Dict[str, Any]],
    ) -> None:
        """Apply pre-computed subtask rollup to parents.

        Marcus intentionally writes NO ``task_outcomes`` row for parents that
        auto-complete via ``check_and_complete_parent_task`` — the work data
        lives on the subtask outcomes, attributed to the real worker who did
        each subtask. Writing a synthetic parent outcome upstream would
        corrupt agent learning (double-counted hours, inflated total_tasks,
        skewed skill_success_rates EMA). See the contract comment in
        ``marcus/src/marcus_mcp/coordinator/subtask_assignment.py``.

        Cato is a denormalized read-side view, so aggregation happens here:
        we synthesize parent display fields by rolling up subtask data, and
        flag the parent with ``work_via_subtasks=True`` so per-view rendering
        decisions stay honest. Overrides message-derived started_at because
        the auto_complete event timestamp is roughly the last-subtask end,
        not the real work span.
        """
        applied = 0
        for parent in tasks:
            pid = str(parent.get("id") or parent.get("task_id") or "")
            r = rollup.get(pid)
            if not r:
                continue
            applied += 1
            if r["started_at"]:
                parent["started_at"] = r["started_at"]
            if r["completed_at"]:
                parent["completed_at"] = r["completed_at"]
                if (
                    not parent.get("updated_at")
                    or parent["updated_at"] < r["completed_at"]
                ):
                    parent["updated_at"] = r["completed_at"]
            if r["actual_hours"] is not None and not parent.get("actual_hours"):
                parent["actual_hours"] = r["actual_hours"]
            if r["assigned_agent_id"] and not parent.get("assigned_agent_id"):
                parent["assigned_agent_id"] = r["assigned_agent_id"]
            # Flag so SwimLane can skip this parent — the rolled-up
            # min→max span isn't a contiguous work bar (subtasks interleave).
            parent["work_via_subtasks"] = True
        if applied:
            logger.info(f"Applied subtask rollup to {applied} parents")

    def _filter_tasks_by_view(
        self, tasks: List[Dict[str, Any]], view_mode: str
    ) -> List[Dict[str, Any]]:
        """
        Filter tasks based on view mode.

        For 'subtasks' view: return subtasks + parent tasks WITHOUT subtasks
        - Subtasks: tasks with parent_task_id set
        - Parent tasks WITHOUT subtasks: bundled design tasks, standalone tasks
        - Parent tasks WITH subtasks: excluded (their subtasks represent them)

        This creates clean parallelization visualization showing task execution flow.
        Display role classification (work/structural/context) is handled separately
        in _classify_display_role() and applied during _build_tasks().
        """
        if view_mode == "subtasks":
            # Build set of parent IDs that have subtasks
            parent_ids_with_subtasks = {
                str(t.get("parent_task_id")) for t in tasks if t.get("parent_task_id")
            }

            # Return: subtasks + parents WITHOUT subtasks
            return [
                t
                for t in tasks
                if t.get("parent_task_id")  # Is a subtask
                or str(t.get("id"))
                not in parent_ids_with_subtasks  # Parent without subtasks
            ]
        elif view_mode == "parents":
            # Infer is_subtask from parent_task_id if not explicitly set
            return [
                t
                for t in tasks
                if not t.get("is_subtask", bool(t.get("parent_task_id")))
            ]
        else:  # "all"
            return tasks

    def _infer_agents(
        self, tasks: List[Dict[str, Any]], messages: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Infer agents from tasks and messages."""
        agents = {}

        def get_agent_name(agent_id: str) -> str:
            """Convert agent ID to display name. System/marcus becomes 'Marcus'."""
            if agent_id.lower() in ["system", "marcus"]:
                return "Marcus"
            return agent_id

        # Always include system agent
        agents["system"] = {
            "id": "system",
            "name": "Marcus",
            "role": "system",
            "skills": [],
        }

        # From tasks
        for task in tasks:
            agent_id = (
                task.get("assigned_agent_id")
                or task.get("agent_id")
                or task.get("assigned_to")
            )
            if agent_id and agent_id not in agents:
                agents[agent_id] = {
                    "id": agent_id,
                    "name": get_agent_name(agent_id),
                    "role": (
                        "system"
                        if agent_id.lower() in ["system", "marcus"]
                        else "agent"
                    ),
                    "skills": [],
                }

        # From messages (extract better names if available)
        for msg in messages:
            for id_field in ["from_agent_id", "to_agent_id", "agent_id"]:
                agent_id = msg.get(id_field)
                if agent_id and agent_id not in agents:
                    agents[agent_id] = {
                        "id": agent_id,
                        "name": get_agent_name(agent_id),
                        "role": (
                            "system"
                            if agent_id.lower() in ["system", "marcus"]
                            else "agent"
                        ),
                        "skills": [],
                    }

        logger.info(f"Inferred {len(agents)} agents")
        return agents

    def _calculate_progress(self, task_data: Dict[str, Any]) -> int:
        """
        Calculate task progress percentage based on status and hours.

        Matches viz worktree implementation for accurate progress display.

        Parameters
        ----------
        task_data : Dict[str, Any]
            Raw task data from persistence

        Returns
        -------
        int
            Progress percentage (0-100)
        """
        status = task_data.get("status", "todo")

        if status == "done":
            return 100
        elif status == "in_progress":
            # Try to calculate from actual vs estimated hours
            estimated = task_data.get("estimated_hours", 0.0)
            actual = task_data.get("actual_hours", 0.0)
            if estimated > 0 and actual > 0:
                # Cap at 90% to leave room for review/completion
                return min(int((actual / estimated) * 100), 90)
            # Default to 50% if no hours tracked yet
            return 50
        elif status == "blocked":
            return 0
        else:  # todo or any other status
            return 0

    def load_parent_tasks_from_db(
        self, root: Optional[Path] = None
    ) -> List[Dict[str, Any]]:
        """
        Load parent tasks from marcus.db (task_metadata collection).

        Parent tasks are stored in the database, while subtasks are in subtasks.json.
        This method loads the parent tasks and enriches them with outcome data.

        Parameters
        ----------
        root : Optional[Path]
            Marcus root to read from. Defaults to self.marcus_root (primary root).
            Pass a non-primary root when loading tasks for a project that lives in
            a parallel experiment directory.

        Returns
        -------
        List[Dict[str, Any]]
            List of parent task dictionaries with task metadata and outcomes.
        """
        import sqlite3

        effective_root = root if root is not None else self.marcus_root
        db_path = effective_root / "data" / "marcus.db"
        if not db_path.exists():
            logger.warning(f"marcus.db not found at {db_path}")
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Load task_metadata
            cursor.execute("""
                SELECT key, data FROM persistence
                WHERE collection = 'task_metadata'
                """)

            task_metadata_map = {}
            for task_id, data_json in cursor.fetchall():
                try:
                    task_data = json.loads(data_json)
                    task_metadata_map[task_id] = task_data
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse task_metadata for {task_id}: {e}")

            # Load task_outcomes to get completion data
            cursor.execute("""
                SELECT key, data FROM persistence
                WHERE collection = 'task_outcomes'
                """)

            task_outcomes_map = {}
            for outcome_key, data_json in cursor.fetchall():
                try:
                    outcome_data = json.loads(data_json)
                    # Outcome keys are like: "task_id_agent_id_timestamp"
                    # Extract the task_id part
                    actual_task_id = outcome_data.get(
                        "task_id", outcome_key.split("_")[0]
                    )
                    task_outcomes_map[actual_task_id] = outcome_data
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Failed to parse task_outcomes for {outcome_key}: {e}"
                    )

            conn.close()

            # Merge metadata with outcomes and normalize fields
            parent_tasks = []
            for task_id, metadata in task_metadata_map.items():
                # Start with metadata
                task = metadata.copy()

                # Ensure id field
                if "task_id" in task and "id" not in task:
                    task["id"] = task["task_id"]

                # Get outcome data if available (match by task_id from metadata)
                actual_task_id = task.get("task_id", task_id)
                outcome = task_outcomes_map.get(actual_task_id, {})

                # Derive status from outcome data
                # task_outcomes schema:
                #   {success: bool, completed_at: str, started_at: str}
                # Use "done" for completed tasks to match
                # _calculate_progress() expectations
                if outcome:
                    if outcome.get("completed_at"):
                        derived_status = "done"
                    elif outcome.get("started_at"):
                        derived_status = "in_progress"
                    else:
                        derived_status = "pending"
                else:
                    derived_status = "todo"

                # Add/normalize required fields
                task["status"] = derived_status
                task["updated_at"] = (
                    outcome.get("completed_at")
                    or outcome.get("started_at")
                    or task.get("created_at")
                )
                task["started_at"] = outcome.get("started_at")
                task["completed_at"] = outcome.get("completed_at")
                task["actual_hours"] = outcome.get("actual_hours", 0.0)
                task["progress_percent"] = 100 if outcome.get("completed_at") else 0
                task["assigned_agent_id"] = outcome.get("agent_id")

                # Normalize dependencies field
                if "dependencies" in task:
                    task["dependency_ids"] = task["dependencies"]

                # Add missing fields with defaults
                task.setdefault("parent_task_id", None)
                task.setdefault("is_subtask", False)
                task.setdefault("assigned_agent_name", None)
                task.setdefault("project_id", None)
                task.setdefault("project_name", None)

                parent_tasks.append(task)

            # Enrich with authoritative status from all kanban*.db files.
            # Parallel experiments (introduced with SQLite parallel support)
            # write to kanban_parallel_N.db rather than kanban.db, so we
            # must read all matching databases and merge. Later files in the
            # glob win on conflict — each experiment owns its own task IDs
            # so collisions should not occur in practice.
            kanban_status: Dict[str, Dict[str, Any]] = {}
            data_dir = effective_root / "data"
            kanban_dbs = (
                sorted(data_dir.glob("kanban*.db")) if data_dir.exists() else []
            )
            for kanban_db in kanban_dbs:
                try:
                    kanban_conn = sqlite3.connect(str(kanban_db))
                    kanban_rows = kanban_conn.execute(
                        "SELECT id, status, assigned_to FROM tasks"
                    ).fetchall()
                    # Read latest blocker comment with AI suggestions per task
                    # (comments table may not exist in older/test databases)
                    try:
                        blocker_rows = kanban_conn.execute(
                            "SELECT task_id, content FROM comments "
                            "WHERE content LIKE '%AI Suggestions%' "
                            "ORDER BY created_at ASC"
                        ).fetchall()
                    except Exception:
                        blocker_rows = []
                    kanban_conn.close()
                    for row in kanban_rows:
                        kanban_status[row[0]] = {
                            "status": row[1],
                            "assigned_to": row[2],
                            "blocker_ai_suggestions": None,
                        }
                    for task_id, content in blocker_rows:
                        if task_id in kanban_status:
                            kanban_status[task_id]["blocker_ai_suggestions"] = (
                                self._parse_ai_suggestions(content)
                            )
                    logger.debug(
                        f"Read {len(kanban_rows)} task statuses from {kanban_db.name}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Could not read {kanban_db.name} for status enrichment: {e}"
                    )

            if kanban_status:
                enriched = 0
                for task in parent_tasks:
                    tid = task.get("task_id", task.get("id"))
                    if tid and tid in kanban_status:
                        task["status"] = kanban_status[tid]["status"]
                        if kanban_status[tid]["assigned_to"]:
                            task["assigned_agent_id"] = kanban_status[tid][
                                "assigned_to"
                            ]
                        task["blocker_ai_suggestions"] = kanban_status[tid].get(
                            "blocker_ai_suggestions"
                        )
                        enriched += 1
                if enriched:
                    logger.info(
                        f"Enriched {enriched} tasks with status from "
                        f"{len(kanban_dbs)} kanban database(s)"
                    )

            logger.info(f"Loaded {len(parent_tasks)} parent tasks from marcus.db")
            return parent_tasks

        except Exception as e:
            logger.error(f"Error loading parent tasks from database: {e}")
            return []

    def load_quality_assessment(self, project_id: str) -> Optional[QualityAssessment]:
        """
        Load Epictetus audit report for a project from marcus.db.

        Parameters
        ----------
        project_id : str
            Project ID to look up.

        Returns
        -------
        Optional[QualityAssessment]
            Quality assessment data, or None if not found.
        """
        db_path = self.marcus_root / "data" / "marcus.db"
        if not db_path.exists():
            logger.warning(f"marcus.db not found at {db_path}")
            return None

        # project_ids in projects.json may use dashed UUID format
        # (e.g. "abc-def-...") while Epictetus stores keys without dashes.
        # Normalize to hex-without-dashes so both formats match.
        normalized_id = project_id.replace("-", "")

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute(
                "SELECT data FROM persistence "
                "WHERE collection IN ('quality_assessments', 'epictetus_audits') "
                "AND key=? ORDER BY rowid DESC LIMIT 1",
                (normalized_id,),
            )
            row = cursor.fetchone()
            conn.close()

            if not row:
                logger.debug(f"No quality assessment found for project {project_id}")
                return None

            data = json.loads(row[0])
            # Map Epictetus report field names to QualityAssessment
            metadata = data.get("metadata", {})
            scores = data.get("scores", {})
            weighted = scores.get("weighted_total", {})
            return QualityAssessment(
                project_id=metadata.get("project_id", project_id),
                audit_date=metadata.get("audit_date", ""),
                weighted_score=float(weighted.get("score", 0.0)),
                weighted_grade=weighted.get("grade", ""),
                scores=scores,
                agent_grades=data.get("agent_grades", []),
                coordination=data.get("coordination_effectiveness", {}),
                contribution=data.get("contribution_distribution", {}),
                issues=data.get("issues", {}),
                recommendations=data.get("recommendations", []),
                smoke_test=data.get("runtime_smoke_test", {}),
                cohesiveness=data.get("authorship_cohesiveness", {}),
                metadata={
                    **metadata,
                    "process_evidence": data.get("process_evidence", {}),
                },
            )
        except Exception as e:
            logger.error(f"Error loading quality assessment for {project_id}: {e}")
            return None

    def load_task_outcomes_from_db(self) -> Dict[str, Dict[str, Any]]:
        """
        Load task outcomes from marcus.db (Memory system).

        Returns actual task durations and completion data from completed tasks.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            Dictionary mapping task_id to outcome data with actual_hours,
            started_at, completed_at, etc.
        """
        import sqlite3

        db_path = self.marcus_root / "data" / "marcus.db"
        if not db_path.exists():
            logger.warning(f"marcus.db not found at {db_path}")
            return {}

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Query task_outcomes from persistence table
            cursor.execute("""
                SELECT key, data FROM persistence
                WHERE collection = 'task_outcomes'
            """)

            outcomes = {}
            for row in cursor.fetchall():
                task_id, data_json = row
                data = json.loads(data_json)

                # Extract key fields
                outcomes[task_id] = {
                    "task_id": task_id,
                    "task_name": data.get("task_name"),
                    "actual_hours": data.get("actual_hours", 0.0),
                    "estimated_hours": data.get("estimated_hours", 0.0),
                    "created_at": data.get("created_at"),
                    "started_at": data.get("started_at"),
                    "completed_at": data.get("completed_at"),
                    "status": "done",  # Outcomes are for completed tasks
                }

            conn.close()
            logger.info(f"Loaded {len(outcomes)} task outcomes from marcus.db")
            return outcomes

        except Exception as e:
            logger.error(f"Error loading task outcomes from db: {e}")
            return {}

    def load_task_timing_from_agent_events(self) -> Dict[str, Dict[str, Any]]:
        """
        Load task start/end times from events in marcus.db.

        Extracts timing data from task_completed events which contain
        both started_at and completed_at timestamps.

        Returns
        -------
        Dict[str, Dict[str, Any]]
            Dictionary mapping task_id to timing data with
            start_time, end_time, duration
        """
        import sqlite3

        timings: Dict[str, Any] = {}
        db_path = self.marcus_root / "data" / "marcus.db"

        if not db_path.exists():
            logger.warning(f"marcus.db not found at {db_path}")
            return timings

        try:
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()

            # Get task_completed events which contain started_at and completed_at
            cursor.execute("""
                SELECT data FROM persistence
                WHERE collection = 'events'
                  AND json_extract(data, '$.event_type') = 'task_completed'
            """)

            for row in cursor.fetchall():
                try:
                    event = json.loads(row[0])
                    event_data = event.get("data", {})

                    task_id = event_data.get("task_id")
                    started_at = event_data.get("started_at")
                    completed_at = event_data.get("completed_at")
                    task_name = event_data.get("task_name")

                    if task_id and started_at and completed_at:
                        # Ensure timestamps have timezone info
                        # for JavaScript compatibility
                        # Marcus events store timestamps without timezone, so add UTC
                        start_with_tz = (
                            started_at
                            if started_at.endswith(("Z", "+00:00"))
                            else started_at + "+00:00"
                        )
                        end_with_tz = (
                            completed_at
                            if completed_at.endswith(("Z", "+00:00"))
                            else completed_at + "+00:00"
                        )

                        timings[task_id] = {
                            "start_time": start_with_tz,
                            "end_time": end_with_tz,
                            "task_name": task_name,
                        }

                        # Calculate duration
                        try:
                            start = datetime.fromisoformat(
                                start_with_tz.replace("Z", "+00:00")
                            )
                            end = datetime.fromisoformat(
                                end_with_tz.replace("Z", "+00:00")
                            )
                            duration_seconds = (end - start).total_seconds()
                            timings[task_id]["duration_seconds"] = duration_seconds
                            timings[task_id]["duration_minutes"] = duration_seconds / 60
                            timings[task_id]["duration_hours"] = duration_seconds / 3600
                        except Exception as e:
                            logger.warning(
                                f"Error calculating duration for {task_id}: {e}"
                            )

                except json.JSONDecodeError as e:
                    logger.warning(f"Error parsing event JSON: {e}")
                    continue

            conn.close()
            logger.info(f"Loaded timing for {len(timings)} tasks from marcus.db events")
            if timings:
                # Log first timing for debugging
                first_task_id = list(timings.keys())[0]
                logger.info(
                    f"Example timing: {first_task_id} -> {timings[first_task_id]}"
                )
            return timings

        except Exception as e:
            logger.error(f"Error loading task timing from marcus.db events: {e}")
            return {}

    def enrich_tasks_with_timing(
        self, tasks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich task data with actual timing from marcus.db.

        Loads timing data from task_outcomes and task_completed events,
        then updates task dictionaries with actual start/end times and durations.
        This enables smooth timeline animation.

        Parameters
        ----------
        tasks : List[Dict[str, Any]]
            Tasks from subtasks.json with potentially zero durations

        Returns
        -------
        List[Dict[str, Any]]
            Tasks enriched with actual start/end times and durations
        """
        # Load timing data from database
        outcomes = self.load_task_outcomes_from_db()
        timings = self.load_task_timing_from_agent_events()

        logger.info(
            f"Enriching {len(tasks)} tasks with {len(outcomes)} outcomes "
            f"and {len(timings)} timings"
        )

        # Build prefix→entry indexes once. Marcus stores outcomes/timings keyed
        # by ``{task_id}_{actor}_{timestamp}``, so we match by reducing the key
        # to the longest underscore-delimited prefix that exists in the task
        # set. This replaces the prior O(n×m) startswith() scan (~40M
        # comparisons on real data) with O(m × avg_underscores).
        task_id_set = {str(t.get("id", "")) for t in tasks if t.get("id")}

        def _index_by_task_id(
            entries: Dict[str, Dict[str, Any]],
        ) -> Dict[str, Dict[str, Any]]:
            indexed: Dict[str, Dict[str, Any]] = {}
            for entry_key, entry in entries.items():
                # Exact match wins
                if entry_key in task_id_set:
                    indexed.setdefault(entry_key, entry)
                    continue
                # Walk longest→shortest prefix; first hit on a known task wins
                parts = entry_key.split("_")
                for i in range(len(parts) - 1, 0, -1):
                    candidate = "_".join(parts[:i])
                    if candidate in task_id_set:
                        indexed.setdefault(candidate, entry)
                        break
            return indexed

        outcomes_by_task = _index_by_task_id(outcomes)
        timings_by_task = _index_by_task_id(timings)

        # Enrich each task with O(1) lookups
        enriched_count = 0
        for task in tasks:
            task_id = str(task.get("id", ""))

            outcome = outcomes_by_task.get(task_id)
            if outcome:
                task["actual_hours"] = outcome["actual_hours"]
                if outcome["started_at"]:
                    task["created_at"] = outcome["started_at"]
                if outcome["completed_at"]:
                    task["updated_at"] = outcome["completed_at"]

            matched_timing = timings_by_task.get(task_id)
            if matched_timing:
                if "start_time" in matched_timing:
                    task["created_at"] = matched_timing["start_time"]
                if "end_time" in matched_timing:
                    task["updated_at"] = matched_timing["end_time"]
                if "duration_hours" in matched_timing:
                    task["actual_hours"] = matched_timing["duration_hours"]
                enriched_count += 1

        logger.info(f"Enriched {enriched_count}/{len(tasks)} tasks with timing data")

        # Filter and prepare tasks for display
        # Keep all tasks including planned, pending, in-progress ones
        filtered_tasks = []
        for task in tasks:
            task_status = task.get("status", "").lower()
            created = task.get("created_at")
            updated = task.get("updated_at")

            # For incomplete tasks (todo, pending, in-progress, blocked),
            # add synthetic timestamps if missing
            incomplete_statuses = [
                "todo",
                "pending",
                "in-progress",
                "in_progress",
                "blocked",
            ]
            if task_status in incomplete_statuses:
                # Keep incomplete tasks even without timing data
                if not created:
                    # Use current time as placeholder
                    task["created_at"] = datetime.now(timezone.utc).isoformat()
                if not updated:
                    # Set updated_at = created_at for zero-duration bar
                    task["updated_at"] = task.get("created_at")
                filtered_tasks.append(task)
                continue

            # For completed/failed tasks, try to keep them if they have timing
            if created:
                if not updated:
                    # If no updated_at, use created_at (zero-duration)
                    task["updated_at"] = created

                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    updated_dt = datetime.fromisoformat(
                        task["updated_at"].replace("Z", "+00:00")
                    )
                    duration = (updated_dt - created_dt).total_seconds()

                    # Keep tasks with any duration OR that have actual_hours tracked
                    if duration >= 0 or task.get("actual_hours", 0.0) > 0:
                        filtered_tasks.append(task)
                except Exception as e:
                    logger.debug(
                        f"Skipping task {task.get('id')} due to timestamp error: {e}"
                    )
                    continue

        removed_count = len(tasks) - len(filtered_tasks)
        logger.info(
            f"Filtered to {len(filtered_tasks)} tasks with valid timing "
            f"(removed {removed_count} zero-duration tasks, kept planned tasks)"
        )
        return filtered_tasks

    def _calculate_timeline(
        self,
        tasks: List[Dict[str, Any]],
        messages: List[Dict[str, Any]],
    ) -> tuple[datetime, datetime, int]:
        """
        Calculate timeline boundaries from filtered tasks only.

        Collects all available timestamps from tasks (created_at, updated_at,
        started_at, completed_at) and uses min/max to determine timeline.
        Excludes messages to ensure timeline matches the filtered task data.

        Returns
        -------
        tuple
            (start_time, end_time, duration_minutes)
        """
        # Collect all available timestamps from displayed tasks
        timestamps = []

        logger.info(f"Calculating timeline from {len(tasks)} tasks")

        for task in tasks:
            # Include started_at/completed_at so message-derived timestamps
            # (set by _enrich_tasks_with_message_timestamps) extend the
            # timeline. Without this, tasks whose assignment messages
            # arrive after the last completed task's updated_at render
            # as todo 0% because currentAbsTime never reaches their start.
            for field in ["created_at", "updated_at", "started_at", "completed_at"]:
                if task.get(field):
                    try:
                        ts_str = task[field]
                        # Handle both ISO format with and without 'Z'
                        if isinstance(ts_str, str):
                            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                            if ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)
                            timestamps.append(ts)
                    except (ValueError, AttributeError, TypeError) as e:
                        logger.warning(
                            f"Failed to parse {field}: {task.get(field)}: {e}"
                        )
                        continue

        logger.info(f"Collected {len(timestamps)} timestamps from tasks")

        if not timestamps:
            # No data, use current time
            logger.warning("No valid timestamps found, using current time")
            now = datetime.now(timezone.utc)
            return now, now, 0

        # Use earliest timestamp as start, latest as end
        start_time = min(timestamps)
        end_time = max(timestamps)
        duration_minutes = int((end_time - start_time).total_seconds() / 60)

        logger.info(
            f"Timeline: {start_time} to {end_time}, duration={duration_minutes} minutes"
        )

        return start_time, end_time, duration_minutes

    def _calculate_power_scale_position(
        self, linear_pos: float, exponent: float = 0.4
    ) -> float:
        """
        Calculate power-scaled position.

        Parameters
        ----------
        linear_pos : float
            Linear position 0.0-1.0
        exponent : float
            Power scale exponent (< 1 expands early timeline)

        Returns
        -------
        float
            Scaled position 0.0-1.0
        """
        if linear_pos <= 0:
            return 0.0
        if linear_pos >= 1.0:
            return 1.0
        return float(linear_pos**exponent)

    def _enrich_tasks_with_message_timestamps(
        self, tasks: List[Dict[str, Any]], messages: List[Dict[str, Any]]
    ) -> None:
        """
        Enrich tasks with started_at and completed_at timestamps from messages.

        Parameters
        ----------
        tasks : List[Dict[str, Any]]
            Task dictionaries to enrich (modified in place)
        messages : List[Dict[str, Any]]
            Messages to extract timestamps from
        """
        # Build message index by task_id for faster lookup
        messages_by_task: dict[str, list[dict[str, Any]]] = {}
        for msg in messages:
            task_id = msg.get("task_id") or msg.get("metadata", {}).get("task_id")
            if task_id:
                if task_id not in messages_by_task:
                    messages_by_task[task_id] = []
                messages_by_task[task_id].append(msg)

        # Enrich each task
        for task in tasks:
            task_id = task.get("id")
            if not task_id or task_id not in messages_by_task:
                continue

            task_messages = messages_by_task[task_id]

            # Find started_at from task_assignment message
            assignment_msgs = [
                m for m in task_messages if m.get("type") == "task_assignment"
            ]
            if assignment_msgs:
                # Use earliest assignment
                earliest = min(assignment_msgs, key=lambda m: m.get("timestamp", ""))
                task["started_at"] = earliest.get("timestamp")
                # Backfill assigned_agent_id from the assignment recipient
                # when neither outcome nor kanban supplied one. Without this
                # the SwimLane drops the task (its filter requires the agent
                # match), so e.g. auto_complete assignments to unicorn_N
                # never surface.
                if not task.get("assigned_agent_id"):
                    to_agent = earliest.get("to_agent_id") or earliest.get(
                        "to_agent_name"
                    )
                    if to_agent:
                        task["assigned_agent_id"] = to_agent

            # Find completed_at from worker progress messages
            progress_msgs = [
                m for m in task_messages if m.get("conversation_type") == "worker_to_pm"
            ]
            for msg in progress_msgs:
                content = msg.get("message", "")
                metadata = msg.get("metadata", {})
                # Check for 100% in message or status=completed in metadata
                if (
                    "100%" in content
                    or "COMPLETED" in content.upper()
                    or metadata.get("progress") == 100
                    or metadata.get("status") == "completed"
                ):
                    task["completed_at"] = msg.get("timestamp")
                    break

    def _calculate_synthetic_start_times(self, tasks: list[dict[str, Any]]) -> None:
        """
        Calculate and correct start times for tasks based on dependencies.

        For tasks with dependencies:
        1. If no started_at: use max dependency completion time
        2. If started_at is BEFORE dependency completion: override with
           dependency completion time (fixes historical parallel execution bugs)

        This ensures the timeline visualization shows correct dependency ordering
        even when Marcus incorrectly ran tasks in parallel.

        Parameters
        ----------
        tasks : list[dict[str, Any]]
            Task dictionaries to enrich (modified in place)
        """
        # Build task lookup by ID
        tasks_by_id = {str(t["id"]): t for t in tasks}

        # Track synthetic timestamps for logging
        synthetic_count = 0

        def get_task_end_time(task: dict[str, Any]) -> datetime | None:
            """Get the end time of a task (completed_at or updated_at)."""
            # Prefer completed_at if available
            if task.get("completed_at"):
                return self._parse_timestamp(task["completed_at"])
            # Fall back to updated_at
            if task.get("updated_at"):
                return self._parse_timestamp(task["updated_at"])
            # If neither, use created_at as fallback
            if task.get("created_at"):
                return self._parse_timestamp(task["created_at"])
            return None

        # Track corrected timestamps for logging
        corrected_count = 0

        # Process all tasks with dependencies
        for task in tasks:
            # Get task dependencies
            dep_ids = task.get("dependencies", [])
            if not dep_ids:
                continue

            # Find latest completion time among dependencies
            latest_dep_end = None
            for dep_id in dep_ids:
                dep_id_str = str(dep_id)
                if dep_id_str not in tasks_by_id:
                    continue

                dep_task = tasks_by_id[dep_id_str]
                dep_end = get_task_end_time(dep_task)

                if dep_end and (latest_dep_end is None or dep_end > latest_dep_end):
                    latest_dep_end = dep_end

            if not latest_dep_end:
                continue

            # Case 1: No started_at - set synthetic start time
            if not task.get("started_at"):
                task["started_at"] = latest_dep_end.isoformat()
                synthetic_count += 1
                logger.debug(
                    f"Synthetic start time for task {task['id']}: "
                    f"{latest_dep_end.isoformat()} (no started_at, using dependency)"
                )
                continue

            # Case 2: started_at exists but is BEFORE dependency completed
            # This happens when Marcus incorrectly ran tasks in parallel
            task_start = self._parse_timestamp(task["started_at"])
            if task_start and task_start < latest_dep_end:
                logger.warning(
                    f"Task {task['id']} started at {task_start.isoformat()} "
                    f"BEFORE dependency completed at {latest_dep_end.isoformat()}. "
                    f"Correcting start time to respect dependency."
                )
                task["started_at"] = latest_dep_end.isoformat()
                corrected_count += 1

        if synthetic_count > 0 or corrected_count > 0:
            logger.info(
                f"Calculated {synthetic_count} synthetic start times, "
                f"corrected {corrected_count} invalid start times "
                f"(started before dependencies completed)"
            )

    def _build_tasks(
        self,
        task_dicts: List[Dict[str, Any]],
        projects_by_id: Dict[str, Dict[str, Any]],
        tasks_by_id: Dict[str, Dict[str, Any]],
        agents_by_id: Dict[str, Dict[str, Any]],
        timeline_start: datetime,
        timeline_end: datetime,
        timeline_scale_exponent: float,
    ) -> List[Task]:
        """Build denormalized Task objects with all relationships embedded."""
        tasks: List[Task] = []
        timeline_duration = (timeline_end - timeline_start).total_seconds()

        for task_data in task_dicts:
            # Parse timestamps
            created_at = self._parse_timestamp(task_data.get("created_at"))
            updated_at = self._parse_timestamp(
                task_data.get("updated_at", task_data.get("created_at"))
            )
            started_at = self._parse_timestamp(task_data.get("started_at"))
            completed_at = self._parse_timestamp(task_data.get("completed_at"))

            # Calculate timeline positions
            # Use started_at if available (corrected by synthetic calculation),
            # otherwise fall back to created_at. This matches frontend logic.
            position_timestamp = started_at if started_at else created_at

            if timeline_duration > 0 and position_timestamp:
                linear_pos = (
                    position_timestamp - timeline_start
                ).total_seconds() / timeline_duration
                scaled_pos = self._calculate_power_scale_position(
                    linear_pos, timeline_scale_exponent
                )
            else:
                linear_pos = 0.0
                scaled_pos = 0.0

            # Embed parent info
            parent_id = task_data.get("parent_task_id")
            parent_name = None
            if parent_id and parent_id in tasks_by_id:
                parent_name = tasks_by_id[parent_id].get("name")

            # Embed project info
            project_id = task_data.get("project_id", "")
            project_name = ""
            if project_id and project_id in projects_by_id:
                project_name = projects_by_id[project_id].get("name", "")

            # Embed agent info
            agent_id = (
                task_data.get("assigned_agent_id")
                or task_data.get("agent_id")
                or task_data.get("assigned_to")
            )
            agent_name = None
            agent_role = None
            if agent_id and agent_id in agents_by_id:
                agent_name = agents_by_id[agent_id]["name"]
                agent_role = agents_by_id[agent_id]["role"]

            task = Task(
                id=task_data.get("id", ""),
                name=task_data.get("name", ""),
                description=task_data.get("description", ""),
                status=task_data.get("status", "todo"),
                priority=task_data.get("priority", "medium"),
                progress_percent=self._calculate_progress(task_data),
                created_at=created_at or datetime.now(timezone.utc),
                started_at=started_at,
                completed_at=completed_at,
                updated_at=updated_at or datetime.now(timezone.utc),
                estimated_hours=task_data.get("estimated_hours", 0.0),
                actual_hours=task_data.get("actual_hours", 0.0),
                parent_task_id=parent_id,
                parent_task_name=parent_name,
                is_subtask=task_data.get("is_subtask", bool(parent_id)),
                subtask_index=task_data.get("subtask_index"),
                project_id=project_id,
                project_name=project_name,
                assigned_agent_id=agent_id,
                assigned_agent_name=agent_name,
                assigned_agent_role=agent_role,
                dependency_ids=task_data.get("dependencies", []),
                dependent_task_ids=[],  # Will be filled by reverse lookup
                timeline_linear_position=linear_pos,
                timeline_scaled_position=scaled_pos,
                timeline_scale_exponent=timeline_scale_exponent,
                labels=task_data.get("labels", []),
                metadata=task_data.get("metadata", {}),
                display_role=self._classify_display_role(task_data),
                blocker_ai_suggestions=task_data.get("blocker_ai_suggestions"),
                work_via_subtasks=bool(task_data.get("work_via_subtasks")),
            )
            tasks.append(task)

        # Fill reverse dependencies
        task_map = {t.id: t for t in tasks}
        for task in tasks:
            for dep_id in task.dependency_ids:
                if dep_id in task_map:
                    task_map[dep_id].dependent_task_ids.append(task.id)

        return tasks

    def _build_agents(
        self,
        agents_by_id: Dict[str, Dict[str, Any]],
        tasks: List[Task],
        messages: List[Dict[str, Any]],
    ) -> List[Agent]:
        """Build denormalized Agent objects with metrics."""
        agents: List[Agent] = []

        # Calculate per-agent metrics
        for agent_id, agent_data in agents_by_id.items():
            agent_tasks = [t for t in tasks if t.assigned_agent_id == agent_id]
            current_tasks = [
                t for t in agent_tasks if t.status in ["in_progress", "todo"]
            ]
            completed_tasks = [t for t in agent_tasks if t.status == "done"]

            total_hours = sum(t.actual_hours for t in completed_tasks)
            avg_hours = total_hours / len(completed_tasks) if completed_tasks else 0.0

            # Count messages
            messages_sent = sum(
                1 for m in messages if m.get("from_agent_id") == agent_id
            )
            messages_received = sum(
                1 for m in messages if m.get("to_agent_id") == agent_id
            )
            blockers_reported = sum(
                1
                for m in messages
                if m.get("from_agent_id") == agent_id and m.get("type") == "blocker"
            )

            agent = Agent(
                id=agent_id,
                name=agent_data["name"],
                role=agent_data["role"],
                skills=agent_data["skills"],
                current_task_ids=[t.id for t in current_tasks],
                current_task_names=[t.name for t in current_tasks],
                completed_task_ids=[t.id for t in completed_tasks],
                completed_tasks_count=len(completed_tasks),
                total_hours_worked=total_hours,
                average_task_duration_hours=avg_hours,
                performance_score=0.0,  # TODO: Calculate based on actual metrics
                capacity_utilization=0.0,  # TODO: Calculate based on actual metrics
                messages_sent=messages_sent,
                messages_received=messages_received,
                blockers_reported=blockers_reported,
            )
            agents.append(agent)

        return agents

    def _build_messages(
        self,
        message_dicts: List[Dict[str, Any]],
        task_ids: Set[str],
        tasks_by_id: Dict[str, Dict[str, Any]],
        agents_by_id: Dict[str, Dict[str, Any]],
    ) -> List[Message]:
        """Build denormalized Message objects."""
        messages: List[Message] = []

        for msg_data in message_dicts:
            # Skip if not related to our filtered tasks
            task_id = msg_data.get("task_id") or msg_data.get("metadata", {}).get(
                "task_id"
            )
            if task_id and task_id not in task_ids:
                continue

            timestamp = self._parse_timestamp(msg_data.get("timestamp"))
            if not timestamp:
                continue

            # Map conversation logger fields to expected format
            # Conversation logger uses: source/target, worker_id, conversation_type
            # We need: from_agent_id, to_agent_id
            from_agent_id = msg_data.get("from_agent_id")
            to_agent_id = msg_data.get("to_agent_id")

            # Fallback: try conversation logger format
            if not from_agent_id:
                from_agent_id = msg_data.get("source", "system")
            if not to_agent_id:
                to_agent_id = msg_data.get("target", "system")

            # Handle worker messages (worker_id + conversation_type)
            worker_id = msg_data.get("worker_id")
            conv_type = msg_data.get("conversation_type")
            if worker_id and conv_type:
                if conv_type == "worker_to_pm":
                    from_agent_id = worker_id
                    to_agent_id = "system"
                elif conv_type == "pm_to_worker":
                    from_agent_id = "system"
                    to_agent_id = worker_id

            # Normalize "marcus" to "system" for agent lookup
            if from_agent_id and from_agent_id.lower() == "marcus":
                from_agent_id = "system"
            if to_agent_id and to_agent_id.lower() == "marcus":
                to_agent_id = "system"

            from_agent_name = agents_by_id.get(from_agent_id, {}).get(
                "name", from_agent_id
            )
            to_agent_name = agents_by_id.get(to_agent_id, {}).get("name", to_agent_id)

            task_name = None
            if task_id and task_id in tasks_by_id:
                task_name = tasks_by_id[task_id].get("name")

            msg = Message(
                id=msg_data.get("id", str(uuid.uuid4())),
                timestamp=timestamp,
                message=msg_data.get("message", msg_data.get("content", "")),
                type=msg_data.get("type", "status_update"),
                from_agent_id=from_agent_id,
                from_agent_name=from_agent_name,
                to_agent_id=to_agent_id,
                to_agent_name=to_agent_name,
                task_id=task_id,
                task_name=task_name,
                parent_message_id=msg_data.get("parent_message_id"),
                metadata=msg_data.get("metadata", {}),
            )
            messages.append(msg)

        # Detect and mark duplicates
        messages = self._detect_duplicates(messages)

        # Filter out duplicates - keep only originals
        messages = [msg for msg in messages if not msg.is_duplicate]

        return messages

    def _detect_duplicates(
        self, messages: List[Message], time_threshold_seconds: float = 2.0
    ) -> List[Message]:
        """
        Detect and mark duplicate messages.

        Duplicates are identified as messages with:
        - Same content
        - Same timestamp (within threshold)
        - Same from/to agents
        - Same task_id
        - Same message type

        Parameters
        ----------
        messages : List[Message]
            Messages to analyze
        time_threshold_seconds : float
            Time window for duplicate detection (default 2 seconds)

        Returns
        -------
        List[Message]
            Messages with duplicate flags set
        """
        # Group potential duplicates by a similarity key
        groups = defaultdict(list)

        for msg in messages:
            # Create a key that groups similar messages
            # (content hash + agent pair + task + type)
            key = (
                hash(msg.message),  # Content
                msg.from_agent_id,
                msg.to_agent_id,
                msg.task_id or "",
                msg.type,
            )
            groups[key].append(msg)

        # Within each group, find duplicates by timestamp proximity
        group_counter = 0

        for key, group_messages in groups.items():
            if len(group_messages) < 2:
                continue  # No duplicates possible

            # Sort by timestamp
            sorted_messages = sorted(group_messages, key=lambda m: m.timestamp)

            # Find duplicates within time threshold
            i = 0
            while i < len(sorted_messages):
                current_msg = sorted_messages[i]
                duplicate_set = [current_msg]

                # Find all messages within time threshold
                j = i + 1
                while j < len(sorted_messages):
                    next_msg = sorted_messages[j]
                    time_diff = abs(
                        (next_msg.timestamp - current_msg.timestamp).total_seconds()
                    )

                    if time_diff <= time_threshold_seconds:
                        duplicate_set.append(next_msg)
                        j += 1
                    else:
                        break

                # If we found duplicates, mark them
                if len(duplicate_set) > 1:
                    group_counter += 1
                    group_id = f"dup_group_{group_counter}"

                    # First message is the original, rest are duplicates
                    for idx, msg in enumerate(duplicate_set):
                        msg.is_duplicate = idx > 0
                        msg.duplicate_group_id = group_id
                        msg.duplicate_count = len(duplicate_set)

                i = j if j > i + 1 else i + 1

        logger.info(f"Detected {group_counter} duplicate message groups")
        return messages

    def _build_events(
        self,
        event_dicts: List[Dict[str, Any]],
        task_ids: Set[str],
        tasks_by_id: Dict[str, Dict[str, Any]],
        agents_by_id: Dict[str, Dict[str, Any]],
    ) -> List[Event]:
        """Build denormalized Event objects."""
        events: List[Event] = []

        for event_data in event_dicts:
            # Skip if not related to our filtered tasks
            task_id = event_data.get("task_id")
            if task_id and task_id not in task_ids:
                continue

            timestamp = self._parse_timestamp(event_data.get("timestamp"))
            if not timestamp:
                continue

            agent_id = event_data.get("agent_id")
            agent_name = None
            if agent_id and agent_id in agents_by_id:
                agent_name = agents_by_id[agent_id]["name"]

            task_name = None
            if task_id and task_id in tasks_by_id:
                task_name = tasks_by_id[task_id].get("name")

            event = Event(
                id=event_data.get("id", str(uuid.uuid4())),
                timestamp=timestamp,
                event_type=event_data.get("event_type", "unknown"),
                agent_id=agent_id,
                agent_name=agent_name,
                task_id=task_id,
                task_name=task_name,
                data=event_data.get("data", {}),
            )
            events.append(event)

        return events

    def _build_decisions(
        self,
        raw_decisions: List[Dict[str, Any]],
        task_ids_set: Set[str],
        tasks_by_id: Dict[str, Dict[str, Any]],
        agents_by_id: Dict[str, Dict[str, Any]],
    ) -> List[Decision]:
        """
        Build denormalized Decision objects with embedded context.

        Parameters
        ----------
        raw_decisions : List[Dict[str, Any]]
            Raw decision data from persistence layer
        task_ids_set : Set[str]
            Set of task IDs in this snapshot
        tasks_by_id : Dict[str, Dict[str, Any]]
            Task lookup dictionary
        agents_by_id : Dict[str, Dict[str, Any]]
            Agent lookup dictionary

        Returns
        -------
        List[Decision]
            Denormalized decisions with embedded task/agent names
        """
        decisions: List[Decision] = []

        for dec_data in raw_decisions:
            task_id: str = dec_data.get("task_id") or ""

            # Filter by all tasks in project, not just view-filtered tasks
            # Decisions on parent tasks should be visible even in subtasks view
            if task_id and task_id not in tasks_by_id:
                continue

            try:
                # Parse timestamp
                ts_str = dec_data.get("timestamp")
                ts: Optional[datetime] = None
                if isinstance(ts_str, str):
                    ts = datetime.fromisoformat(ts_str)
                elif isinstance(ts_str, datetime):
                    ts = ts_str

                if ts is None:
                    continue

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                # Embed task and agent names
                task = tasks_by_id.get(task_id, {})
                agent_id = dec_data.get("agent_id", "")
                agent = agents_by_id.get(agent_id, {})

                decision = Decision(
                    decision_id=dec_data.get("decision_id", ""),
                    task_id=task_id,
                    agent_id=agent_id,
                    timestamp=ts,
                    what=dec_data.get("what", ""),
                    why=dec_data.get("why", ""),
                    impact=dec_data.get("impact", ""),
                    affected_tasks=dec_data.get("affected_tasks", []),
                    confidence=dec_data.get("confidence", 0.8),
                    task_name=task.get("name", task_id),
                    agent_name=agent.get("name", agent_id),
                )

                decisions.append(decision)

            except Exception as e:
                logger.warning(f"Error building decision: {e}")
                continue

        logger.info(f"Built {len(decisions)} denormalized decisions")
        return decisions

    def _build_artifacts(
        self,
        raw_artifacts: List[Dict[str, Any]],
        task_ids_set: Set[str],
        tasks_by_id: Dict[str, Dict[str, Any]],
        agents_by_id: Dict[str, Dict[str, Any]],
    ) -> List[Artifact]:
        """
        Build denormalized Artifact objects with embedded context.

        Parameters
        ----------
        raw_artifacts : List[Dict[str, Any]]
            Raw artifact data from persistence layer
        task_ids_set : Set[str]
            Set of task IDs in this snapshot
        tasks_by_id : Dict[str, Dict[str, Any]]
            Task lookup dictionary
        agents_by_id : Dict[str, Dict[str, Any]]
            Agent lookup dictionary

        Returns
        -------
        List[Artifact]
            Denormalized artifacts with embedded task/agent names
        """
        artifacts: List[Artifact] = []

        for art_data in raw_artifacts:
            task_id: str = art_data.get("task_id") or ""

            # Filter by all tasks in project, not just view-filtered tasks
            # Artifacts from parent tasks should be visible even in subtasks view
            if task_id and task_id not in tasks_by_id:
                continue

            try:
                ts_str = art_data.get("timestamp")
                ts: Optional[datetime] = None
                if isinstance(ts_str, str):
                    ts = datetime.fromisoformat(ts_str)
                elif isinstance(ts_str, datetime):
                    ts = ts_str

                if ts is None:
                    continue

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                task = tasks_by_id.get(task_id, {})
                agent_id = art_data.get("agent_id", "")
                agent = agents_by_id.get(agent_id, {})

                artifact = Artifact(
                    artifact_id=art_data.get("artifact_id", ""),
                    task_id=task_id,
                    agent_id=agent_id,
                    timestamp=ts,
                    filename=art_data.get("filename", ""),
                    artifact_type=art_data.get("artifact_type", ""),
                    description=art_data.get("description", ""),
                    file_size_bytes=art_data.get("file_size_bytes", 0),
                    referenced_by_tasks=art_data.get("referenced_by_tasks", []),
                    task_name=task.get("name", task_id),
                    agent_name=agent.get("name", agent_id),
                    relative_path=art_data.get("relative_path"),
                    absolute_path=art_data.get("absolute_path"),
                )

                artifacts.append(artifact)

            except Exception as e:
                logger.warning(f"Error building artifact: {e}")
                continue

        logger.info(f"Built {len(artifacts)} denormalized artifacts")
        return artifacts

    def _build_diagnostic_events(
        self,
        tasks: List[Task],
        agents: List[Agent],
        timeline_start: Optional[datetime],
        timeline_end: Optional[datetime],
    ) -> List[Event]:
        """
        Build diagnostic events for timeline visualization.

        Detects issues like zombie tasks, circular dependencies, bottlenecks,
        and generates timeline events so they can be visualized during playback.

        Parameters
        ----------
        tasks : List[Task]
            Denormalized tasks
        agents : List[Agent]
            Denormalized agents
        timeline_start : Optional[datetime]
            Timeline start time
        timeline_end : Optional[datetime]
            Timeline end time

        Returns
        -------
        List[Event]
            Diagnostic events with timestamps
        """
        diagnostic_events: List[Event] = []

        if not timeline_start or not timeline_end:
            return diagnostic_events

        # Build task lookup for dependency checking
        tasks_by_id = {t.id: t for t in tasks}
        # 1. Detect zombie tasks (IN_PROGRESS with no agent assigned)
        for task in tasks:
            if task.status == "in_progress" and task.assigned_agent_id is None:
                # Use task's updated time as when the issue was detected
                # updated_at is already a datetime object, no parsing needed
                event_time = task.updated_at or timeline_end

                diagnostic_events.append(
                    Event(
                        id=f"diagnostic_zombie_{task.id}",
                        timestamp=event_time,
                        event_type="diagnostic:zombie_task",
                        agent_id=None,
                        agent_name=None,
                        task_id=task.id,
                        task_name=task.name,
                        data={
                            "severity": "high",
                            "description": (
                                f"Task '{task.name}' is marked "
                                "IN_PROGRESS but has no "
                                "assigned agent"
                            ),
                            "recommendation": (
                                "Reset to TODO status or "
                                "assign to an available agent"
                            ),
                        },
                    )
                )

        # 2. Detect bottleneck tasks (blocking 3+ other tasks)
        dependent_count: defaultdict[str, int] = defaultdict(int)
        for task in tasks:
            for dep_id in task.dependency_ids:
                dependent_count[dep_id] += 1

        for task_id, count in dependent_count.items():
            if count >= 3:
                blocking_task = tasks_by_id.get(task_id)
                if blocking_task and blocking_task.status != "done":
                    event_time = blocking_task.updated_at or timeline_end

                    diagnostic_events.append(
                        Event(
                            id=f"diagnostic_bottleneck_{blocking_task.id}",
                            timestamp=event_time,
                            event_type="diagnostic:bottleneck",
                            agent_id=blocking_task.assigned_agent_id,
                            agent_name=blocking_task.assigned_agent_name,
                            task_id=blocking_task.id,
                            task_name=blocking_task.name,
                            data={
                                "severity": "medium",
                                "description": (
                                    f"Task '{blocking_task.name}' is "
                                    f"blocking {count} other tasks"
                                ),
                                "recommendation": (
                                    "Prioritize completing this"
                                    f" task to unblock {count}"
                                    " tasks"
                                ),
                                "blocks_count": count,
                            },
                        )
                    )

        # 3. Detect circular dependencies (basic check)
        visited: Set[str] = set()
        rec_stack: Set[str] = set()

        def has_cycle(task_id: str, path: List[str]) -> Optional[List[str]]:
            """DFS to detect cycles."""
            if task_id in rec_stack:
                # Found a cycle
                cycle_start_idx = path.index(task_id)
                return path[cycle_start_idx:] + [task_id]

            if task_id in visited or task_id not in tasks_by_id:
                return None

            visited.add(task_id)
            rec_stack.add(task_id)
            path.append(task_id)

            task = tasks_by_id[task_id]
            for dep_id in task.dependency_ids:
                cycle = has_cycle(dep_id, path[:])
                if cycle:
                    return cycle

            rec_stack.remove(task_id)
            return None

        detected_cycles: List[List[str]] = []
        for task in tasks:
            if task.id not in visited:
                cycle = has_cycle(task.id, [])
                if cycle:
                    # Avoid duplicate cycles
                    cycle_set = frozenset(cycle)
                    if not any(frozenset(c) == cycle_set for c in detected_cycles):
                        detected_cycles.append(cycle)

        for cycle in detected_cycles:
            # Use the latest updated time from tasks in the cycle
            cycle_tasks = [tasks_by_id[tid] for tid in cycle if tid in tasks_by_id]
            if cycle_tasks:
                latest_time = max(
                    (t.updated_at for t in cycle_tasks if t.updated_at),
                    default=timeline_end,
                )

                cycle_names = [
                    tasks_by_id[tid].name for tid in cycle[:3] if tid in tasks_by_id
                ]

                diagnostic_events.append(
                    Event(
                        id=f"diagnostic_circular_{'_'.join(cycle[:2])}",
                        timestamp=latest_time or timeline_end,
                        event_type="diagnostic:circular_dependency",
                        agent_id=None,
                        agent_name=None,
                        task_id=cycle[0] if cycle else None,
                        task_name=None,
                        data={
                            "severity": "critical",
                            "description": (
                                "Circular dependency detected"
                                f": {' -> '.join(cycle_names)}"
                                "..."
                            ),
                            "recommendation": (
                                "Break the cycle by removing" " one dependency link"
                            ),
                            "cycle": cycle,
                            "cycle_length": len(cycle),
                        },
                    )
                )

        # 4. Detect redundant dependencies (transitive)
        for task in tasks:
            if len(task.dependency_ids) < 2:
                continue

            # Find what's reachable through dependencies
            reachable: Set[str] = set()

            def find_reachable(dep_id: str, visited_deps: Set[str]) -> None:
                """Find all tasks reachable from dep_id."""
                if dep_id in visited_deps or dep_id not in tasks_by_id:
                    return
                visited_deps.add(dep_id)
                dep_task = tasks_by_id[dep_id]
                for next_dep in dep_task.dependency_ids:
                    reachable.add(next_dep)
                    find_reachable(next_dep, visited_deps)

            for dep_id in task.dependency_ids:
                find_reachable(dep_id, set())

            # Check if any direct dependency is also reachable transitively
            redundant = set(task.dependency_ids) & reachable

            if redundant:
                event_time = task.updated_at or timeline_end
                for redundant_dep in redundant:
                    redundant_task = tasks_by_id.get(redundant_dep)
                    if redundant_task:
                        diagnostic_events.append(
                            Event(
                                id=f"diagnostic_redundant_{task.id}_{redundant_dep}",
                                timestamp=event_time,
                                event_type="diagnostic:redundant_dependency",
                                agent_id=None,
                                agent_name=None,
                                task_id=task.id,
                                task_name=task.name,
                                data={
                                    "severity": "low",
                                    "description": (
                                        f"Task '{task.name}' has "
                                        "redundant dependency on "
                                        f"'{redundant_task.name}'"
                                    ),
                                    "recommendation": (
                                        "Remove redundant dependency"
                                        " to simplify graph"
                                    ),
                                    "redundant_dependency_id": redundant_dep,
                                    "redundant_dependency_name": redundant_task.name,
                                },
                            )
                        )

        logger.info(f"Generated {len(diagnostic_events)} diagnostic timeline events")
        return diagnostic_events

    def _calculate_parallelization_metrics(
        self, tasks: List[Task]
    ) -> tuple[int, float, float]:
        """
        Calculate parallelization metrics by analyzing task timeline overlap.

        Parameters
        ----------
        tasks : List[Task]
            Tasks to analyze

        Returns
        -------
        tuple[int, float, float]
            (peak_parallel_tasks, average_parallel_tasks, parallelization_efficiency)
        """
        if not tasks:
            return 0, 0.0, 0.0

        # Collect all time events (task starts and ends)
        events = []
        for task in tasks:
            if task.created_at and task.updated_at:
                start_time = task.created_at.timestamp()
                end_time = task.updated_at.timestamp()
                if end_time > start_time:  # Only valid duration tasks
                    events.append((start_time, 1))  # Task starts (+1)
                    events.append((end_time, -1))  # Task ends (-1)

        if not events:
            return 0, 0.0, 0.0

        # Sort events by time
        events.sort()

        # Calculate concurrent tasks at each event
        concurrent_counts = []
        current_count = 0
        last_time = events[0][0]
        total_task_time = 0.0

        for ts, delta in events:
            if ts > last_time and current_count > 0:
                duration = ts - last_time
                total_task_time += duration * current_count
                concurrent_counts.append((duration, current_count))

            current_count += delta
            last_time = ts

        if not concurrent_counts:
            return 0, 0.0, 0.0

        # Calculate metrics
        peak_parallel = max(count for _, count in concurrent_counts)

        # Average parallel = total task-time / total duration
        total_duration = events[-1][0] - events[0][0]
        average_parallel = (
            total_task_time / total_duration if total_duration > 0 else 0.0
        )

        # Efficiency = (actual parallel work) / (ideal serial time)
        # Ideal serial time = sum of all task durations
        total_task_duration = sum(
            (t.updated_at.timestamp() - t.created_at.timestamp())
            for t in tasks
            if t.created_at is not None
            and t.updated_at is not None
            and t.updated_at.timestamp() > t.created_at.timestamp()
        )

        # Efficiency = how much we compressed the work through parallelization
        # If we did all tasks in parallel perfectly, efficiency = 1.0
        # If we did all tasks serially, efficiency = 1/n where n is task count
        if total_duration > 0 and total_task_duration > 0:
            parallelization_efficiency = total_duration / total_task_duration
        else:
            parallelization_efficiency = 0.0

        return peak_parallel, average_parallel, min(parallelization_efficiency, 1.0)

    def _calculate_metrics(
        self, tasks: List[Task], agents: List[Agent], messages: List[Message]
    ) -> Metrics:
        """Calculate all project metrics.

        Only counts work tasks — structural and context tasks are excluded
        from metrics to avoid inflating completion rates and task counts.
        """
        tasks = [t for t in tasks if t.display_role == "work"]
        total_tasks = len(tasks)
        completed_tasks = len([t for t in tasks if t.status == "done"])
        in_progress_tasks = len([t for t in tasks if t.status == "in_progress"])
        blocked_tasks = len([t for t in tasks if t.status == "blocked"])
        completion_rate = completed_tasks / total_tasks if total_tasks > 0 else 0.0

        # Time metrics
        completed_task_durations = []
        total_duration_seconds = 0.0

        # Find actual timeline boundaries from tasks
        task_times = []
        for task in tasks:
            if task.created_at:
                task_times.append(task.created_at.timestamp())
            if task.updated_at:
                task_times.append(task.updated_at.timestamp())

            # Calculate individual task durations
            # (use created_at/updated_at, not started/completed)
            if task.created_at and task.updated_at:
                duration_seconds = (
                    task.updated_at.timestamp() - task.created_at.timestamp()
                )
                if duration_seconds > 0:
                    duration_minutes = duration_seconds / 60.0  # Convert to minutes
                    completed_task_durations.append(duration_minutes)

        # Calculate total project duration
        if task_times:
            total_duration_seconds = max(task_times) - min(task_times)
        total_duration_minutes = round(
            total_duration_seconds / 60.0
        )  # Round to whole number

        # Average task duration in minutes
        # (field name says 'hours' but we use minutes)
        avg_duration_minutes = (
            sum(completed_task_durations) / len(completed_task_durations)
            if completed_task_durations
            else 0.0
        )

        # Parallelization metrics - analyze timeline overlap
        peak_parallel, average_parallel, parallelization_efficiency = (
            self._calculate_parallelization_metrics(tasks)
        )

        # Agent metrics
        total_agents = len(agents)
        active_agents = len([a for a in agents if a.current_task_ids])
        tasks_per_agent = total_tasks / total_agents if total_agents > 0 else 0.0

        # Blockers
        total_blockers = sum(a.blockers_reported for a in agents)
        blocked_percentage = blocked_tasks / total_tasks if total_tasks > 0 else 0.0

        return Metrics(
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            in_progress_tasks=in_progress_tasks,
            blocked_tasks=blocked_tasks,
            completion_rate=completion_rate,
            total_duration_minutes=total_duration_minutes,
            # Actually minutes, field name is misleading
            average_task_duration_hours=avg_duration_minutes,
            peak_parallel_tasks=peak_parallel,
            average_parallel_tasks=average_parallel,
            parallelization_efficiency=parallelization_efficiency,
            total_agents=total_agents,
            active_agents=active_agents,
            tasks_per_agent=tasks_per_agent,
            total_blockers=total_blockers,
            blocked_task_percentage=blocked_percentage,
        )

    def _build_dependency_graph(self, tasks: List[Task]) -> Dict[str, List[str]]:
        """Build task dependency graph.

        Uses display_role to control graph inclusion:
        - "context" tasks: excluded entirely (no node, no edges)
        - "structural" tasks: included with full edges (preserves DAG topology)
        - "work" tasks: included with full edges
        """
        graph: dict[str, list[str]] = {}
        for task in tasks:
            if task.display_role == "context":
                continue
            graph[task.id] = task.dependency_ids
        return graph

    def _build_communication_graph(
        self, messages: List[Message]
    ) -> Dict[str, List[str]]:
        """Build agent communication graph."""
        graph = defaultdict(set)
        for msg in messages:
            if msg.from_agent_id and msg.to_agent_id:
                graph[msg.from_agent_id].add(msg.to_agent_id)
        return {k: list(v) for k, v in graph.items()}

    def get_artifact_by_id(self, artifact_id: str) -> Optional[Artifact]:
        """
        Lightweight artifact lookup by ID without loading full snapshot.

        This is optimized for artifact preview - only loads artifacts,
        not the full snapshot with 6,970+ tasks.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to look up

        Returns
        -------
        Optional[Artifact]
            The artifact if found, None otherwise
        """
        # Load just artifacts (fast - typically < 100 items)
        artifact_dicts = self._load_artifacts()

        # Find the artifact by ID
        for art_data in artifact_dicts:
            if art_data.get("artifact_id") == artifact_id:
                # Build minimal Artifact object
                return Artifact(
                    artifact_id=art_data["artifact_id"],
                    task_id=art_data.get("task_id", ""),
                    agent_id=art_data.get("agent_id", ""),
                    filename=art_data.get("filename", ""),
                    artifact_type=art_data.get("artifact_type", ""),
                    description=art_data.get("description", ""),
                    file_size_bytes=art_data.get("file_size_bytes", 0),
                    timestamp=self._parse_timestamp(art_data.get("timestamp"))
                    or datetime.now(timezone.utc),
                    # Embedded context
                    task_name=None,
                    agent_name=None,
                    relative_path=art_data.get("relative_path"),
                    absolute_path=art_data.get("absolute_path"),
                    referenced_by_tasks=[],
                )

        return None

    def _parse_timestamp(self, ts_str: Optional[str]) -> Optional[datetime]:
        """Parse timestamp string to timezone-aware datetime."""
        if not ts_str:
            return None

        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                # Naive timestamps are treated as UTC
                # (Marcus stores all timestamps in UTC)
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        except (ValueError, AttributeError):
            return None
