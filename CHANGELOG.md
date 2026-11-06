# Changelog

All notable changes to Cato will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Compatibility:** Cato versions independently from Marcus. See README for
minimum compatible Marcus version.

## [Unreleased]

### Changed
- **The Marcus install path is now configurable per machine.** Cato previously
  hardcoded `/Users/lwgray/dev/marcus` in several places, making it unusable by
  anyone else. The Marcus location is now resolved dynamically: the `MARCUS_ROOT`
  environment variable, then `config.local.json` / `config.json`, then
  auto-detection (Marcus as a sibling of Cato, or `~/dev/marcus`). On first
  `./cato start`, the CLI prompts for the Marcus root, validates that it
  contains both `data/` and `src/`, and saves it to `config.local.json`
  (gitignored); subsequent starts reuse it silently.
- **`Aggregator` auto-detection now uses the shared resolver.** Its previous
  fallback assumed Cato was a subdirectory of Marcus (a leftover from when
  Cato lived in the Marcus repo) and silently pointed at the Cato root.

### Fixed
- **Cato now fails loudly when Marcus cannot be located** instead of serving
  an empty dashboard. A no-arg `Aggregator()` and backend startup raise a
  clear error directing the user to set `MARCUS_ROOT` or run `./cato`.

## [0.3.4] - 2026-05-17

Bugfix release.

### Fixed
- **Hex project IDs with all-digit prefixes loaded only a few tasks.** Marcus
  projects use 32-character hexadecimal IDs. The fast task-loading path checked
  whether a project was a numeric Planka project by inspecting only the first
  8 characters of its ID. A hex ID whose first 8 characters were coincidentally
  all decimal digits (e.g. `10519982...`) was misclassified as a Planka
  project, routing it down a prefix-matching path that silently dropped every
  hex task ID. Affected projects showed only the 2-4 tasks recovered from
  conversation logs in the DAG, Swim Lane, and Board views instead of the full
  task list. The check now requires the entire ID to be numeric.

## [0.3.3] - 2026-05-17

Adds the cost dashboard — a full per-project / per-task / per-tool view of
Marcus token spend.

### Added
- **Cost dashboard.** New `/api/cost/*` endpoints backed by Marcus, with a
  real-time cost tab and Historical / Budget / Pricing tabs.
- Project-first dashboard layout with project as the primary axis across the
  API and UI; unified project picker; Historical and Pricing surfaced in
  Settings.
- Per-task and per-tool cost audit views; operation-taxonomy drill-down in the
  Real-time tab.
- Subtask timing rolls up onto auto-completed parent tasks.
- Show Subtasks / Parents / All toggle on graph views.
- Persistent project names and global CSS theme tokens.

### Changed
- Aggregator mirrors Marcus's runs/path rename.
- Worker JSONL ingestion wired through; pricing UX cleanup.
- Dashboard dev server moved to port 5847 with auto-bump.

## [0.3.2] - 2026-05-07

Three regressions from the v0.3.1 project-scoped fast path. All showed up as
DAG/Board nodes being absent at project start and only "appearing" once an
agent worked on them.

### Fixed
- **Hex-Planka projects load all parents from creation time.**
  The slow path has two candidate-resolution strategies depending on whether
  the Planka board_id has a numeric 8-char prefix. For numeric prefixes it
  fuzzy-matches task IDs; for non-numeric (hex) prefixes it falls back to
  filtering by ``metadata.project_id`` directly. The fast path only
  implemented the prefix-match branch, so for hex-Planka projects (e.g.
  board_id starts with "05c39bc6") only tasks already mentioned in
  conversation logs surfaced — typically just the design tasks. Other parent
  tasks (Implement X, Compose Y, About:, Marcus Planning:) were absent until
  agents picked them up. Adds the project_id metadata fallback when no
  Planka ID has a numeric prefix.
- **Unstarted parent tasks render at creation time.**
  `_load_parent_tasks_by_ids` only set `task["status"]` when an outcome row
  existed, so tasks created in `marcus.db` but not yet started had no status
  field and were treated as filtered/missing downstream. Restores the prior
  `task.setdefault("status", "todo")` default.
- **Dependency-only parents are pulled into the project's task set.**
  The slow path also collected dependency-implied parents (if matched task B
  depends on parent A, A is included even if A hadn't surfaced via
  conversations or Planka prefix matching). The fast path skipped that walk.
  Adds a bounded BFS (max 8 levels) over `dependencies` after the initial
  parent fetch.

## [0.3.1] - 2026-05-06

**The performance & polish release.** Cuts cold snapshot load from 8s to ~1s on
large Marcus deployments, scopes conversation logs correctly to project, and
cleans up several DAG/Board UI rough edges. No API changes.

### Performance
- Cold snapshot load **8.1s → 1.1s** on real data (~7×, no staleness window).
  - New project-scoped fast path in `_load_tasks` uses SQL `WHERE key IN (...)`
    against `marcus.db` plus mtime-validated `subtasks.json`; only the project's
    ~10 rows are enriched instead of all 11k.
  - `enrich_tasks_with_timing` replaces O(n×m) prefix scan with longest-prefix
    index (also fixes a latent bug where parents matched subtask outcomes).
  - `_load_decisions` / `_load_artifacts` skip Marcus's per-call 50k-file
    conversation re-glob via a scoped contextmanager that supplies pre-computed
    task IDs; `ProjectHistoryPersistence` reused across both calls.
  - Log-file scans pre-filter via year-prefix glob and filename date check
    before `stat()`, avoiding 49k unnecessary syscalls in 50k-file dirs.
  - mtime/size-based caches for parsed messages and events.
- All SQL queries on Marcus's database open with `PRAGMA query_only=1`.
- `[timing]` log lines added around each load phase for ongoing visibility.

### Added
- Multi-path aggregator support for parallel Marcus instances
- Subtask grouping context surfaced across DAG, Swimlane, and Board views
- Header pill strip in DAG view showing all design tasks
- Hover-highlight in DAG and max-3 expand in Board replace cluttered grouping marks
- Ghost-group column breakdown in DAG layout
- History cutoff date setting filters projects and log files
- Parallel kanban enrichment for multi-experiment Marcus setups
- AI blocker suggestions surfaced in Board view
- Project Info / About Tasks pane cleanup
- Opt-in "Reset timeline on tab switch" toggle in header settings (off by default)
- Header timestamp suffix disambiguates duplicate project names

### Fixed
- Board column scroll restored via absolute positioning and fixed flex height chain
- Aggregator normalizes `realtime_*.jsonl` entries for Conversation view
- Conversations now scoped to the active project; "general" group always last
- `UnboundLocalError` on `project_info` in the all-projects code path

### Tooling
- `cato` launcher renames the backend process to `cato-backend` so `pkill`
  targets only this project; frontend kill scoped to the project's path
- `.claude/` added to `.gitignore`

## [0.3.0] - 2026-04-07

**The Quality & Observability release.** Adds Epictetus quality dashboard,
display_role task classification, design phase visibility in Swim Lanes,
and Project Info drawer. Requires Marcus >= 0.3.0 for quality_assessments.

### Added
- **Quality dashboard tab** — visualizes Epictetus code audit reports with sub-tabs: Agents (default, effectiveness bars, root cause attributions), Scores (dimension bars, smoke tests, coordination), Issues (ghost code, cross-agent, findings), Recommendations (instruction quality, project/global scope filtering)
- **Display role system** — three-tier task classification (work/structural/context) computed by the aggregator, controlling which tasks appear in each visualization view
- **Design phase in Swim Lane** — dedicated "Marcus / Planning" lane with dashed ghost-node styling
- **Ghost nodes in DAG** — design/structural tasks rendered as hollow rings with dashed stroke
- **Project Info drawer** — context tasks surfaced in a dedicated drawer
- **QualityAssessment dataclass** and aggregator method to load audit reports from marcus.db

### Changed
- Quality tab hides metrics sidebar and timeline scrubber for full viewport width
- Metrics exclude structural and context tasks from completion counts
- Dependency graph excludes context tasks; structural tasks preserve DAG topology

### Fixed
- `auto_completed` label no longer misclassifies work tasks as context
- Context tasks filtered from agent swim lanes

## [0.2.1] - 2026-03-29
### Added
- Pre-commit hooks: mypy, black, isort, flake8, pydocstyle, bandit, detect-secrets
- GitHub Actions CI/CD workflows: quality checks, test suite, Claude code review, version gate
- Configuration files: `.flake8`, `.coveragerc`, `pytest.ini`, `.secrets.baseline`
- Expanded dev dependencies (pytest-asyncio, pytest-cov, isort, flake8, bandit, httpx)
- Custom pre-commit hooks for naive datetime detection and changelog reminders

### Fixed
- All existing lint, type annotation, and formatting issues across the codebase
- Setuptools flat-layout package discovery error in CI
- Deprecated license format in pyproject.toml
- Historical API integration tests now skip gracefully when Marcus is not installed

## [0.1.1] - 2026-03-09
### Added
- Decisions/artifacts support in aggregator
- Data export functionality for multi-agent analysis
- Project filtering with efficient fuzzy matching
- Background refresh for projects

### Fixed
- JSON fallback and view-mode filtering for decisions/artifacts
- Duplicate client-side project filtering
- Network view filtering by selected project
- Progress tracking and analysis caching issues
- Power scale exponent set to 1.0 for linear timeline consistency

### Changed
- Removed historical analysis UI, keeping only live mode
- Removed mock mode, switched to live-only data
- Optimized /api/projects to load tasks once instead of per-project

## [0.1.0] - 2026-03-01
### Added
- Initial release of Cato visualization dashboard
- Network graph for task dependency visualization
- Agent swim lanes timeline view
- Conversation view for agent message flow
- Real-time metrics panel
- Timeline playback functionality
- Project filtering
- Auto-refresh with configurable intervals
- FastAPI backend serving Marcus data
- React + TypeScript frontend
- Zustand state management
- Docker integration with Marcus

[Unreleased]: https://github.com/lwgray/cato/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/lwgray/cato/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/lwgray/cato/compare/v0.1.1...v0.2.1
[0.1.1]: https://github.com/lwgray/cato/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lwgray/cato/releases/tag/v0.1.0
